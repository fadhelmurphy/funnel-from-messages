import psycopg2
import csv

DB_CONFIG = {
    "dbname": "sparks",
    "user": "app",
    "password": "secret",
    "host": "localhost",
    "port": 5432,
}

def export_funnel_to_csv(filename="funnel_report.csv"):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT id, room_id, leads_date, channel, phone, booking_date, transaction_date, transaction_value, opening_keyword FROM funnel ORDER BY leads_date;")
    rows = cur.fetchall()

    headers = [desc[0] for desc in cur.description]

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Funnel report exported to {filename}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    export_funnel_to_csv()
