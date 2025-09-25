# backend/main.py
from fastapi import FastAPI, Query
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "sparks",
    "user": "sparks",
    "password": "sparks",
    "host": "postgres",
    "port": 5432,
}

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/funnel-report")
def funnel_report(
    start_date: Optional[str] = Query(None, description="Filter from this date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Filter until this date (YYYY-MM-DD)"),
    channel: Optional[str] = Query(None, description="Filter by channel")
):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    query = "SELECT * FROM funnel_table WHERE 1=1"
    params = {}

    if start_date:
        query += " AND leads_date >= %(start_date)s"
        params["start_date"] = start_date
    if end_date:
        query += " AND leads_date <= %(end_date)s"
        params["end_date"] = end_date
    if channel:
        query += " AND channel = %(channel)s"
        params["channel"] = channel

    query += " ORDER BY leads_date DESC"

    cur.execute(query, params)
    results = cur.fetchall()

    cur.close()
    conn.close()
    return {"count": len(results), "data": results}
