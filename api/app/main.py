# api/app/main.py
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
    # Generic webhook entrypoint. Accepts provider payloads.
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    provider = data.get("provider") or data.get("source") or "unknown"
    room_id = data.get("room_id") or data.get("room", {}).get("id") or data.get("roomId")
    if not room_id:
        room_id = f"room_unknown_{uuid.uuid4().hex[:8]}"

    ts = datetime.datetime.utcnow().isoformat()
    key = f"raw/{provider}/{room_id}/{ts}.json"
    raw_json = json.dumps(data)

    # upload to minio
    try:
        app.state.s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=raw_json.encode("utf-8"))
    except Exception as e:
        print("s3 put error", e)

    # push to redis stream
    stream_key = "incoming:messages"
    entry = {
        "provider": provider,
        "room_id": room_id,
        "raw_object_key": key,
        "received_at": ts
    }
    await app.state.redis.xadd(stream_key, entry)

    return {"ok": True, "queued": True, "room_id": room_id}

@app.post("/sync-keywords")
async def manual_sync_keywords(body: dict):
    """
    Manual upload of keywords (for testing). Body: {"keywords": ["booking","daftar"]}
    In production, keyword-sync service will push keywords to Redis set.
    """
    keywords = body.get("keywords", [])
    if not isinstance(keywords, list):
        raise HTTPException(status_code=400, detail="keywords must be a list")
    redis = app.state.redis
    if keywords:
        await redis.delete("keywords:opening")
        for k in keywords:
            await redis.sadd("keywords:opening", k)
    return {"ok": True, "count": len(keywords)}

@app.get("/funnel-report")
async def funnel_report(start: str = None, end: str = None):
    """
    Simple report aggregated from funnel table.
    """
    pool = app.state.db
    sql = "SELECT channel, COUNT(*) as leads_count, COUNT(booking_date) FILTER (WHERE booking_date IS NOT NULL) as bookings_count, SUM(transaction_value) as transaction_value_sum FROM funnel WHERE 1=1"
    params = []
    if start:
        sql += " AND leads_date >= $1"
        params.append(start)
    if end:
        if not params:
            sql += " AND leads_date <= $1"
            params.append(end)
        else:
            sql += " AND leads_date <= ${}".format(len(params)+1)
            params.append(end)
    sql += " GROUP BY channel"
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params) if params else await conn.fetch(sql)
    return [dict(r) for r in rows]
