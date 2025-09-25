# keyword_sync/sync_keywords.py
import os, time, json
import gspread
from google.oauth2.service_account import Credentials
import asyncio
import redis.asyncio as redis_lib

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
SHEET_KEY = os.getenv("GSHEET_KEY")  # spreadsheet id
SA_JSON = os.getenv("GOOGLE_SA_JSON", "/secrets/google-service-account.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

def get_keywords_from_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(SA_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_KEY)
    ws = sh.sheet1
    vals = ws.col_values(1)
    # normalize
    kws = [v.strip() for v in vals if v and v.strip()]
    return kws

async def push_to_redis(kws):
    redis = await redis_lib.from_url(REDIS_URL)
    await redis.delete("keywords:opening")
    if kws:
        for k in kws:
            await redis.sadd("keywords:opening", k)
    await redis.aclose()

async def main_loop():
    while True:
        try:
            kws = get_keywords_from_sheet()
            await push_to_redis(kws)
            print(f"synced {len(kws)} keywords")
        except Exception as e:
            print("sync error", e)
        await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main_loop())
