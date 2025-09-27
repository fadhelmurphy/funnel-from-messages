import os, re, asyncio, datetime
import asyncpg
import redis.asyncio as redis_lib
from dateutil import parser as dateparser
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()  

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:secret@postgres:5432/sparks")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

BOOKING_KEYWORDS = ["booking", "book", "daftar", "reserve", "registrasi", "registr"]
TRANSACTION_KEYWORDS = ["paid", "transfer", "bayar", "pembayaran", "lunas", "sudah transfer"]

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "3"))

money_re = re.compile(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)")

async def run_funnel_etl():
    redis = await redis_lib.from_url(REDIS_URL)
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    print("START FUNNEL ETL")

    try:
        async with pool.acquire() as conn:

            rooms = await conn.fetch("SELECT id, room_id, channel FROM rooms")

            for r in rooms:
                room_db_id = r["id"]

                msgs = await conn.fetch("""
                    SELECT phone, content, raw_payload, created_at
                    FROM messages
                    WHERE room_id=$1
                    ORDER BY created_at ASC
                """, room_db_id)

                if not msgs:
                    continue
                
                kwset = await redis.smembers("keywords:opening")
                kwset = [k.decode().lower() for k in kwset] if kwset else []
                booking_kwset = await redis.smembers("keywords:booking")
                BOOKING_KEYWORDS = [k.decode().lower() for k in booking_kwset] if booking_kwset else BOOKING_KEYWORDS

                transaction_kwset = await redis.smembers("keywords:transaction")
                TRANSACTION_KEYWORDS = [k.decode().lower() for k in transaction_kwset] if transaction_kwset else TRANSACTION_KEYWORDS


                leads_date = None
                opening_keyword = None
                booking_date = None
                transaction_date = None
                transaction_value = None
                phone = None

                for m in msgs:
                    text = (m.get("content") or "").lower()
                    created = m.get("created_at")

                    phone = phone or m.get("phone")
                    raw = m.get("raw_payload")
                    if isinstance(raw, dict):
                        phone = phone or raw.get("sender", {}).get("phone") or raw.get("phone")

                    if not leads_date:
                        for kw in kwset:
                            if kw and kw in text:
                                leads_date = created.date() if isinstance(created, datetime.datetime) else created
                                opening_keyword = kw
                                break
                        if not leads_date and isinstance(raw, dict):
                            sender_type = raw.get("sender", {}).get("type") or raw.get("sender_type")
                            if sender_type and sender_type.lower() == "customer":
                                leads_date = created.date() if isinstance(created, datetime.datetime) else created

                    for kw in BOOKING_KEYWORDS:
                        if kw in text:
                            try:
                                import re
                                match = re.search(r"\d{4}-\d{2}-\d{2}", text)
                                if match:
                                    booking_date = datetime.datetime.strptime(match.group(), "%Y-%m-%d").date()
                            except Exception:
                                pass

                    for kw in TRANSACTION_KEYWORDS:
                        if kw in text:
                            transaction_date = created.date() if isinstance(created, datetime.datetime) else created
                            match = money_re.search(text)
                            if match:
                                # clean format 1.500.000 -> 1500000
                                transaction_value = float(match.group().replace(".", "").replace(",", ""))

                await conn.execute("""
                INSERT INTO funnel (room_id, leads_date, channel, phone, booking_date, transaction_date, transaction_value, opening_keyword, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (room_id) DO UPDATE SET
                    leads_date = EXCLUDED.leads_date,
                    channel = EXCLUDED.channel,
                    phone = EXCLUDED.phone,
                    booking_date = EXCLUDED.booking_date,
                    transaction_date = EXCLUDED.transaction_date,
                    transaction_value = EXCLUDED.transaction_value,
                    opening_keyword = EXCLUDED.opening_keyword;
                """,
                room_db_id,
                leads_date,
                r["channel"],
                phone,
                booking_date,
                transaction_date,
                transaction_value,
                opening_keyword,
                datetime.datetime.utcnow()
                )
                logging.info(f"[OK] funnel upserted for room {r['room_id']}")

    finally:
        await redis.aclose()
        await pool.close()


async def main_loop():
    while True:
        try:
            await run_funnel_etl()
        except Exception as e:
            print("etl error", e)
        await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main_loop())
