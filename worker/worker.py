# worker/worker.py
import os, asyncio, json, datetime
import asyncpg, boto3
import redis.asyncio as redis_lib
from botocore.client import Config

REDIS_URL = os.getenv("REDIS_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw-payloads")

def s3_client():
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

async def process_entry(pool, s3, entry):
    data = {k.decode(): v.decode() for k,v in entry.items()}
    key = data.get("raw_object_key")
    room_key = data.get("room_id")
    provider = data.get("provider")

    try:
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
        raw_bytes = obj["Body"].read()
        payload = json.loads(raw_bytes)
    except Exception:
        payload = {"provider": provider, "room_id": room_key}

    async with pool.acquire() as conn:
        now = datetime.datetime.utcnow()
        # upsert room
        r = await conn.fetchrow("SELECT id FROM rooms WHERE room_id=$1", room_key)
        if r:
            room_id = r["id"]
            await conn.execute("UPDATE rooms SET last_activity_at=$1 WHERE id=$2", now, room_id)
        else:
            row = await conn.fetchrow("INSERT INTO rooms (room_id, channel, raw_meta, created_at, last_activity_at) VALUES ($1,$2,$3,$4,$5) RETURNING id",
                                      room_key, payload.get("channel","unknown"), json.dumps(payload.get("meta") or {}), now, now)
            room_id = row["id"]

        msg_id = payload.get("message",{}).get("id") or payload.get("msg_id")
        sender = payload.get("sender",{}) or {}
        content = payload.get("message",{}).get("text") or json.dumps(payload.get("message") or {})
        created_at = payload.get("timestamp") or now.isoformat()
        # insert message with idempotency (msg_id)
        try:
            await conn.execute(
                "INSERT INTO messages (room_id, msg_id, sender_type, sender_id, phone, content, raw_payload, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                room_id, msg_id, sender.get("type") or payload.get("sender_type"), sender.get("id"),
                sender.get("phone") or payload.get("phone"), content, json.dumps(payload), created_at
            )
        except Exception:
            pass

        # Note: do NOT create funnel here. ETL job handles funnel computation (to keep separation of concerns).

async def consumer():
    s3 = s3_client()
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    redis = await redis_lib.from_url(REDIS_URL)
    stream = "incoming:messages"
    group = "workers"
    consumer_name = f"worker-{os.getenv('HOSTNAME','1')}"
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception:
        pass
    while True:
        try:
            resp = await redis.xreadgroup(group, consumer_name, streams={stream: ">"}, count=10, block=5000)
            if not resp:
                await asyncio.sleep(0.2)
                continue
            for stream_name, messages in resp:
                for msg_id, fields in messages:
                    await process_entry(pool, s3, fields)
                    await redis.xack(stream, group, msg_id)
        except Exception as e:
            print("worker error:", e)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(consumer())
