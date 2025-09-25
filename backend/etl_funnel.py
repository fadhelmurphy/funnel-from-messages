import psycopg2
from psycopg2.extras import DictCursor

DB_CONFIG = {
    "dbname": "sparks",
    "user": "app",
    "password": "secret",
    "host": "postgres",
    "port": 5432,
}

def run_funnel_etl():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=DictCursor)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS funnel_table (
        room_id TEXT,
        leads_date DATE,
        channel TEXT,
        phone_number TEXT,
        booking_date DATE,
        transaction_date DATE,
        transaction_value NUMERIC,
        PRIMARY KEY (room_id)
    );
    """)

    # Ambil rooms + messages (asumsi message_type bisa customer, agent, system)
    cur.execute("""
    SELECT r.id as room_id, r.channel, r.phone_number, m.created_at, m.message
    FROM rooms r
    JOIN messages m ON r.id = m.room_id
    ORDER BY r.id, m.created_at;
    """)
    rows = cur.fetchall()

    funnel_data = {}
    for row in rows:
        room_id = row["room_id"]
        if room_id not in funnel_data:
            funnel_data[room_id] = {
                "room_id": room_id,
                "channel": row["channel"],
                "phone_number": row["phone_number"],
                "leads_date": None,
                "booking_date": None,
                "transaction_date": None,
                "transaction_value": None,
            }

        msg_text = row["message"].lower()
        ts = row["created_at"].date()

        # Rules sederhana
        if funnel_data[room_id]["leads_date"] is None:
            funnel_data[room_id]["leads_date"] = ts

        if "booking" in msg_text and funnel_data[room_id]["booking_date"] is None:
            funnel_data[room_id]["booking_date"] = ts

        if "transaction" in msg_text and funnel_data[room_id]["transaction_date"] is None:
            funnel_data[room_id]["transaction_date"] = ts

        if "value=" in msg_text:
            try:
                val = float(msg_text.split("value=")[1].split()[0])
                funnel_data[room_id]["transaction_value"] = val
            except Exception:
                pass

    # Upsert ke funnel_table
    for record in funnel_data.values():
        cur.execute("""
        INSERT INTO funnel_table (room_id, leads_date, channel, phone_number, booking_date, transaction_date, transaction_value)
        VALUES (%(room_id)s, %(leads_date)s, %(channel)s, %(phone_number)s, %(booking_date)s, %(transaction_date)s, %(transaction_value)s)
        ON CONFLICT (room_id) DO UPDATE SET
            leads_date = EXCLUDED.leads_date,
            channel = EXCLUDED.channel,
            phone_number = EXCLUDED.phone_number,
            booking_date = EXCLUDED.booking_date,
            transaction_date = EXCLUDED.transaction_date,
            transaction_value = EXCLUDED.transaction_value;
        """, record)

    conn.commit()
    cur.close()
    conn.close()
    print("Funnel ETL completed")

if __name__ == "__main__":
    run_funnel_etl()
