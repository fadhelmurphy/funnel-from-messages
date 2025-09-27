import os, json, uuid, datetime
from fastapi import FastAPI, Request, HTTPException
import redis.asyncio as redis
from .utils_s3 import get_s3_client, ensure_bucket
from .db import get_pool

REDIS_URL = os.getenv("REDIS_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw-payloads")

app = FastAPI(title="Sparks Ingest API")

@app.on_event("startup")
async def startup():
    app.state.redis = await redis.from_url(REDIS_URL)
    app.state.s3 = get_s3_client(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    ensure_bucket(app.state.s3, RAW_BUCKET)
    app.state.db = await get_pool()

@app.on_event("shutdown")
async def shutdown():
    try:
        await app.state.redis.close()
    except:
        pass
    pool = getattr(app.state, "db", None)
    if pool:
        await pool.close()

@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    channel = data.get("channel") or data.get("source") or "unknown"
    room_id = data.get("room_id") or data.get("room", {}).get("id") or data.get("roomId")
    if not room_id:
        room_id = f"room_unknown_{uuid.uuid4().hex[:8]}"

    ts = datetime.datetime.utcnow().isoformat()
    key_json = f"raw/{channel}/{room_id}/{ts}.json"
    raw_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")

    try:
        app.state.s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key_json,
            Body=raw_bytes,
            ContentType="application/json"
        )
        print(f"Uploaded raw JSON to MinIO: {key_json}")
    except Exception as e:
        print("S3 put error:", e)

    stream_key = "incoming:messages"
    entry = {
        "provider": channel,
        "room_id": room_id,
        "raw_object_key": key_json,
        "received_at": ts
    }
    await app.state.redis.xadd(stream_key, entry)

    return {"ok": True, "queued": True, "room_id": room_id}

@app.post("/sync-keywords")
async def manual_sync_keywords(body: dict):
    """
    Upload keywords per kategori.
    Body example:
    {
        "opening": ["halo","hai"],
        "booking": ["booking","reserve"],
        "transaction": ["bayar","lunas"]
    }
    """
    redis = app.state.redis

    for category in ["opening", "booking", "transaction"]:
        kws = body.get(category, [])
        if kws:
            await redis.delete(f"keywords:{category}")
            for k in kws:
                await redis.sadd(f"keywords:{category}", k)

    return {"ok": True, "counts": {c: len(body.get(c, [])) for c in ["opening","booking","transaction"]}}

# @app.get("/funnel-report")
# async def funnel_report(start: str = None, end: str = None):
#     """
#     Simple report aggregated from funnel table.
#     """
#     pool = app.state.db
#     sql = "SELECT channel, COUNT(*) as leads_count, COUNT(booking_date) FILTER (WHERE booking_date IS NOT NULL) as bookings_count, SUM(transaction_value) as transaction_value_sum FROM funnel WHERE 1=1"
#     params = []
#     if start:
#         sql += " AND leads_date >= $1"
#         params.append(start)
#     if end:
#         if not params:
#             sql += " AND leads_date <= $1"
#             params.append(end)
#         else:
#             sql += " AND leads_date <= ${}".format(len(params)+1)
#             params.append(end)
#     sql += " GROUP BY channel"
#     async with pool.acquire() as conn:
#         rows = await conn.fetch(sql, *params) if params else await conn.fetch(sql)
#     return [dict(r) for r in rows]
