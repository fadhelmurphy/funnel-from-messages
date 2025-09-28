import psycopg2
import csv
import os

DB_CONFIG = {
    "dbname": "sparks",
    "user": "app",
    "password": "secret",
    "host": "localhost",
    "port": 5432,
}

FUNNEL_CSV = os.getenv("FUNNEL_CSV", "funnel_report.csv")
REJECT_CSV = os.getenv("REJECT_CSV", "funnel_reject.csv")

def export_funnel_to_csv():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, room_id, leads_date, channel, phone, booking_date, transaction_date, transaction_value, opening_keyword
        FROM funnel
        ORDER BY leads_date;
    """)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]

    with open(FUNNEL_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Funnel report exported to {FUNNEL_CSV} ({len(rows)} rows)")

    cur.execute("""
        SELECT id, room_id, leads_date, channel, phone, booking_date, transaction_date, transaction_value, opening_keyword
        FROM funnel
        WHERE leads_date IS NULL
           OR phone IS NULL
           OR booking_date IS NULL
           OR transaction_date IS NULL
           OR transaction_value IS NULL
           OR opening_keyword IS NULL
        ORDER BY id;
    """)
    reject_rows = cur.fetchall()

    if reject_rows:
        with open(REJECT_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(reject_rows)
        print(f"Reject report exported to {REJECT_CSV} ({len(reject_rows)} rows)")
    else:
        print("No rejected rows found.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    export_funnel_to_csv()
