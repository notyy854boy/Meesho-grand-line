import os
import asyncio
import io
import ccxt.async_support as ccxt_async
import pandas as pd
from supabase import create_client, Client
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from fastapi import FastAPI
import uvicorn
from contextlib import asynccontextmanager

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# 🔥 YAHAN APNI SUPABASE MASTER KEY (service_role) DAALNA MAT BHOOLNA 🔥
SUPABASE_URL = "https://sdlfggybitpoxczdeihq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkbGZnZ3liaXRwb3hjemRlaWhxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc5MTEyMjAxOSwiZXhwIjoyMTA2Njk4MDE5fQ.-qa5c60tZf1viwGhQpYqiGq0vv0Fy9IfIbK7quID838"
SUPABASE_BUCKET = "quant-lake"

# --- INITIALIZATION ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

is_harvesting = False
harvest_status = {"status": "IDLE", "last_batch": "None", "errors": 0}

class MasterDataPipeline:
    def __init__(self):
        self.exchange = ccxt_async.binance({'enableRateLimit': True})
    
    async def upload_archive_async(self, df: pd.DataFrame, filename: str):
        def _upload():
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False, compression='gzip')
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=filename,
                file=csv_buffer.getvalue(),
                file_options={"content-type": "application/gzip", "upsert": "true"}
            )
        await asyncio.to_thread(_upload)
        
    async def update_live_db(self, df: pd.DataFrame, symbol: str, timeframe: str):
        def _update():
            records = []
            for _, row in df.iterrows():
                records.append({
                    "id": f"{symbol}_{timeframe}_{int(row['timestamp'])}",
                    "symbol": symbol, "timeframe": timeframe, "timestamp": int(row['timestamp']),
                    "open": float(row['open']), "high": float(row['high']),
                    "low": float(row['low']), "close": float(row['close']), "volume": float(row['volume'])
                })
            supabase.table("live_ohlcv").upsert(records).execute()
        await asyncio.to_thread(_update)

    async def run_pipeline(self, bot_instance, chat_id, symbol="BTC/USDT", timeframe="15m"):
        global is_harvesting, harvest_status
        is_harvesting = True
        harvest_status["status"] = "RUNNING"
        
        try:
            checkpoint_id = f"{symbol}_{timeframe}"
            res = supabase.table("harvest_checkpoints").select("last_timestamp").eq("id", checkpoint_id).execute()
            
            if len(res.data) > 0:
                since = res.data[0]["last_timestamp"] + 1
                await bot_instance.send_message(chat_id, f"🔄 Resuming from Checkpoint: {pd.to_datetime(since, unit='ms')}")
            else:
                since = self.exchange.parse8601('2025-01-01T00:00:00Z')
                await bot_instance.send_message(chat_id, f"🚀 Fresh Start for {symbol}")

            retry_count = 0
            
            while True:
                try:
                    ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                    if not ohlcv or len(ohlcv) < 2:
                        await bot_instance.send_message(chat_id, f"✅ Pipeline Synced to LIVE Market!")
                        harvest_status["status"] = "SYNCED"
                        break
                    
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    last_ts = int(df['timestamp'].iloc[-1])
                    
                    filename = f"{symbol.replace('/','')}/{timeframe}/batch_{last_ts}.csv.gz"
                    
                    await asyncio.gather(
                        self.upload_archive_async(df, filename),
                        self.update_live_db(df, symbol, timeframe)
                    )
                    
                    supabase.table("harvest_checkpoints").upsert({"id": checkpoint_id, "last_timestamp": last_ts}).execute()
                    
                    since = last_ts + 1
                    retry_count = 0
                    harvest_status["last_batch"] = str(pd.to_datetime(last_ts, unit='ms'))
                    await asyncio.sleep(2)
                    
                except Exception as batch_err:
                    retry_count += 1
                    harvest_status["errors"] += 1
                    if retry_count > 5: raise Exception(f"Failed after 5 retries. Error: {batch_err}")
                    await asyncio.sleep(5 * retry_count)
                    
        except Exception as e:
            harvest_status["status"] = f"ERROR: {e}"
            await bot_instance.send_message(chat_id, f"⚠️ Pipeline Paused (Auto-resume ready). Error: {str(e)}")
        finally:
            await self.exchange.close()
            is_harvesting = False

@dp.message(Command("harvest"))
async def cmd_harvest(message: types.Message):
    global is_harvesting
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID: return
    if is_harvesting: return await message.answer("⚠️ Pipeline is already running!")
    await message.answer("⚙ Starting Data Pipeline (Binance → Live DB + Storage Archive)...")
    asyncio.create_task(MasterDataPipeline().run_pipeline(bot, message.chat.id, "BTC/USDT", "15m"))

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID: return
    res = supabase.table("harvest_checkpoints").select("*").execute()
    chk_text = "\n".join([f"• {r['id']}: {pd.to_datetime(r['last_timestamp'], unit='ms')}" for r in res.data]) if res.data else "No checkpoints yet."
    
    text = (f"📊 **God Engine Pipeline Status**\n\n"
            f"**Engine State:** `{harvest_status['status']}`\n"
            f"**Last Sync:** `{harvest_status['last_batch']}`\n"
            f"**Errors:** `{harvest_status['errors']}`\n\n"
            f"**Checkpoints:**\n{chk_text}")
    await message.answer(text, parse_mode="Markdown")

# --- FASTAPI & BOT LIFECYCLE (Error Free) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Bada Bhai God Engine Started...")
    await bot.delete_webhook(drop_pending_updates=True)
    task = asyncio.create_task(dp.start_polling(bot))
    yield
    task.cancel()

api = FastAPI(lifespan=lifespan)

@api.get("/")
def root():
    return {"status": "Bada Bhai God Engine is LIVE!"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(api, host="0.0.0.0", port=port)
    
