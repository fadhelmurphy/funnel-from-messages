import requests
import random
import datetime

API_URL = "http://localhost:8000/webhook"

MOCK_MESSAGES = [
    {"phone": "08123456789", "content": "halo kak"},                     # opening (leads)
    {"phone": "08123456789", "content": "saya mau booking 2025-10-01"}, # booking
    {"phone": "08123456789", "content": "sudah transfer 1.500.000"}     # transaction
]

def send_mock_room():
    room_id = f"room-{random.randint(1000,9999)}"
    for msg in MOCK_MESSAGES:
        payload = {
            "room_id": room_id,
            "channel": "whatsapp",
            "sender": {"type": "customer", "phone": msg["phone"]},
            "message": {"id": f"msg-{random.randint(1000,9999)}", "text": msg["content"]},
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
        r = requests.post(API_URL, json=payload)
        try:
            response = r.json()
        except Exception:
            response = r.text
        print(f"Sent: {payload} → {r.status_code} -> {response}")

if __name__ == "__main__":
    send_mock_room()
