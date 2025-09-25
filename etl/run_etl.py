# etl/run_etl.py
import os, re, asyncio, datetime
import asyncpg
import redis.asyncio as redis_lib
from dateutil import parser as dateparser

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

# keywords for booking and transaction (can extend)
BOOKING_KEYWORDS = ["booking", "book", "daftar", "reserve", "registrasi", "registr"]
TRANSACTION_KEYWORDS = ["paid", "transfer", "bayar", "pembayaran", "lunas", "sudah transfer"]

money_re = re.compile(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)")  # naive money matcher

async def run_etl():
    redis = await redis_lib.from_url(REDIS_URL)
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    try:
        # fetch rooms
        async with pool.acquire() as conn:
            rooms = await conn.fetch("SELECT id, room_id, channel FROM rooms")
            for r in rooms:
                room_db_id = r["id"]
                # skip if already exists in funnel (simple logic; can upsert instead)
                exists = await conn.fetchval("SELECT 1 FROM funnel WHERE room_id=$1", room_db_id)
                if exists:
                    continue

                # fetch messages for room ordered
                msgs = await conn.fetch("SELECT content, raw_payload, created_at FROM messages WHERE room_id=$1 ORDER BY created_at ASC", room_db_id)
                if not msgs:
                    continue

                # load keywords from redis
                kwset = await redis.smembers("keywords:opening")
                kwset = [k.decode().lower() for k in kwset] if kwset else []

                leads_date = None
                opening_keyword = None
                booking_date = None
                transaction_date = None
                transaction_value = None
                phone = None

                for m in msgs:
                    text = (m["content"] or "").lower()
                    created = m["created_at"]
                    # try set phone from raw payload
                    try:
                        raw = m["raw_payload"]
                        if isinstance(raw, dict):
                            phone = phone or raw.get("sender",{}).get("phone") or raw.get("phone")
                        else:
                            # raw stored as json string sometimes
                            pass
                    except Exception:
                        pass

                    # leads = opening keyword first
                    if not leads_date:
                        # check opening keywords
                        for kw in kwset:
                            if kw and kw in text:
                                leads_date = created.date() if isinstance(created, datetime.date) is False else created
                                opening_keyword = kw
                                break
                        # fallback: first customer message as lead if still empty
                        if not leads_date:
                            # we assume sender type marked in raw_payload
                            try:
                                raw = m["raw_payload"]
                                if isinstance(raw, dict):
                                    sender_type = raw.get("sender",{}).get("type") or raw.get("sender_type")
                                    if sender_type and sender_type.lower() == "customer":
                                        leads_date = created.date() if hasattr(created, "date") else created
                            except Exception:
                                pass

                    # booking detection
                    if not booking_date:
                        for bk in BOOKING_KEYWORDS:
                            if bk in text:
                                # try parse date inside text
                                dt = None
                                # search for date like yyyy-mm-dd
                                mdate = re.search(r"(20\d{2}[-/]\d{1,2}[-/]\d{1,2})", text)
                                if mdate:
                                    try:
                                        dt = dateparser.parse(mdate.group(1)).date()
                                    except:
                                        pass
                                booking_date = dt or (created.date() if hasattr(created, "date") else created)

                    # transaction detection
                    if not transaction_date:
                        for tk in TRANSACTION_KEYWORDS:
                            if tk in text:
                                transaction_date = created.date() if hasattr(created, "date") else created
                                # try extract money value
                                mm = money_re.search(text.replace(" ", ""))
                                if mm:
                                    val = mm.group(1).replace(".", "").replace(",", ".")
                                    try:
                                        transaction_value = float(val)
                                    except:
                                        transaction_value = None
                                break

                    if leads_date and booking_date and transaction_date:
                        break

                # insert into funnel
                await conn.execute(
                    "INSERT INTO funnel (room_id, leads_date, channel, phone, booking_date, transaction_date, transaction_value, opening_keyword, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
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
                print(f"funnel inserted for room {r['room_id']}")
    finally:
        await redis.aclose()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(run_etl())
