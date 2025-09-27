import os, time, json, asyncio
import gspread
from google.oauth2.service_account import Credentials
import redis.asyncio as redis_lib
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()  

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
SHEET_KEY = os.getenv("GSHEET_KEY")
SA_JSON = os.getenv("GOOGLE_SA_JSON", "/secrets/google-service-account.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

# fallback hardcoded keywords per category
HARDCODED_KEYWORDS = {
    "opening": ["halo", "hai", "selamat pagi"],
    "booking": ["booking", "reserve", "daftar"],
    "transaction": ["bayar", "transfer", "lunas"]
}

def get_keywords_from_sheet():
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file(SA_JSON, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_KEY)
        ws = sh.sheet1
        data = ws.get_all_records()

        result = {}
        for category in HARDCODED_KEYWORDS.keys():
            kws = [row[category].strip() for row in data if row.get(category) and row[category].strip()]
            result[category] = kws if kws else HARDCODED_KEYWORDS[category]

        return result
    except Exception as e:
        print("Error ambil keywords dari GSheet:", e)
        return HARDCODED_KEYWORDS

async def push_to_redis(keywords_by_category):
    redis = await redis_lib.from_url(REDIS_URL)
    for category, kws in keywords_by_category.items():
        await redis.delete(f"keywords:{category}")
        for k in kws:
            await redis.sadd(f"keywords:{category}", k)
        print(f"Pushed {len(kws)} keywords to Redis category '{category}'")
    await redis.aclose()

async def main_loop():
    while True:
        try:
            kws_by_cat = get_keywords_from_sheet()
            await push_to_redis(kws_by_cat)
        except Exception as e:
            print("sync error", e)
        await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main_loop())
