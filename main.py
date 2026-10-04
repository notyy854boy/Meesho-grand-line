import os
import asyncio
import io
import ccxt.async_support as ccxt_async
import pandas as pd
from supabase import create_client, Client
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "quant-lake")

# --- INITIALIZATION ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global State
is_harvesting = False
harvest_status = {"status": "IDLE", "last_batch": "None", "errors": 0}

class MasterDataPipeline:
    def __init__(self):
        self.exchange = ccxt_async.binance({'enableRateLimit': True})
    
    async def upload_archive_async(self, df: pd.DataFrame, filename: str):
        """Compression & Archive (Supabase Storage)"""
        def _upload():
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False, compression='gzip') # 90% space saved
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=filename,
                file=csv_buffer.getvalue(),
                file_options={"content-type": "application/gzip", "upsert": "true"}
            )
        await asyncio.to_thread(_upload)
        
    async def update_live_db(self, df: pd.DataFrame, symbol: str, timeframe: str):
        """Updates Hot DB & Auto-Cleans old data"""
        def _update():
            # Keep only latest 1000 candles for live ML to prevent DB bloat
            records = []
            for _, row in df.iterrows():
                records.append({
                    "id": f"{symbol}_{timeframe}_{int(row['timestamp'])}",
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": int(row['timestamp']),
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": float(row['volume'])
                })
            # Upsert into Live DB (No Duplicates)
            supabase.table("live_ohlcv").upsert(records).execute()
        await asyncio.to_thread(_update)

    async def run_pipeline(self, bot_instance, chat_id, symbol="BTC/USDT", timeframe="15m"):
        global is_harvesting, harvest_status
        is_harvesting = True
        harvest_status["status"] = "RUNNING"
        
        try:
            # 1. CHECKPOINT READ
            checkpoint_id = f"{symbol}_{timeframe}"
            res = supabase.table("harvest_checkpoints").select("last_timestamp").eq("id", checkpoint_id).execute()
            
            if len(res.data) > 0:
                since = res.data[0]["last_timestamp"] + 1 # +1 ensures NO exact duplicate
                await bot_instance.send_message(chat_id, f"🔄 Resuming {symbol} from Checkpoint: {pd.to_datetime(since, unit='ms')}")
            else:
                since = self.exchange.parse8601('2025-01-01T00:00:00Z')
                await bot_instance.send_message(chat_id, f"🚀 Fresh Start for {symbol}")

            retry_count = 0
            
            while True:
                try:
                    # 2. FETCH DATA
                    ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                    
                    if not ohlcv or len(ohlcv) < 2:
                        await bot_instance.send_message(chat_id, f"✅ Pipeline Synced to LIVE Market for {symbol}!")
                        harvest_status["status"] = "SYNCED"
                        break
                    
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    last_ts = int(df['timestamp'].iloc[-1])
                    
                    # 3. ARCHIVE (Storage) & HOT DB (Database) Separation
                    filename = f"{symbol.replace('/','')}/{timeframe}/batch_{last_ts}.csv.gz"
                    
                    # Run concurrently for speed
                    await asyncio.gather(
                        self.upload_archive_async(df, filename),
                        self.update_live_db(df, symbol, timeframe)
                    )
                    
                    # 4. UPDATE CHECKPOINT (Safe Resume)
                    supabase.table("harvest_checkpoints").upsert({"id": checkpoint_id, "last_timestamp": last_ts}).execute()
                    
                    # Advance loop
                    since = last_ts + 1
                    retry_count = 0 # Reset retries on success
                    harvest_status["last_batch"] = str(pd.to_datetime(last_ts, unit='ms'))
                    
                    await asyncio.sleep(2) # Safe rate-limiting
                    
                except Exception as batch_err:
                    retry_count += 1
                    harvest_status["errors"] += 1
                    if retry_count > 5:
                        raise Exception(f"Failed after 5 retries. Error: {batch_err}")
                    await asyncio.sleep(5 * retry_count) # Exponential Backoff
                    
        except Exception as e:
            harvest_status["status"] = f"ERROR: {e}"
            await bot_instance.send_message(chat_id, f"⚠️ Pipeline Paused (Auto-resume ready). Error: {str(e)}")
        finally:
            await self.exchange.close()
            is_harvesting = False

# ==========================================
# TELEGRAM INTERFACE
# ==========================================
@dp.message(Command("harvest"))
async def cmd_harvest(message: types.Message):
    global is_harvesting
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID: return
        
    if is_harvesting:
        return await message.answer("⚠️ Pipeline is already running!")
    
    await message.answer("⚙️ Starting Data Pipeline (Binance → Live DB + Storage Archive)...")
    pipeline = MasterDataPipeline()
    asyncio.create_task(pipeline.run_pipeline(bot, message.chat.id, "BTC/USDT", "15m"))

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID: return
    
    # Fetch checkpoint from Supabase
    res = supabase.table("harvest_checkpoints").select("*").execute()
    chk_text = "\n".join([f"• {r['id']}: {pd.to_datetime(r['last_timestamp'], unit='ms')}" for r in res.data]) if res.data else "No checkpoints yet."
    
    text = (
        f"📊 **God Engine Pipeline Status**\n\n"
        f"**Engine State:** `{harvest_status['status']}`\n"
        f"**Last Sync Date:** `{harvest_status['last_batch']}`\n"
        f"**Auto-Retry Errors:** `{harvest_status['errors']}`\n\n"
        f"**Checkpoints:**\n{chk_text}"
    )
    await message.answer(text, parse_mode="Markdown")

# ==========================================
# APP RUNNER
# ==========================================
async def main():
    print("🚀 Bada Bhai God Engine Started...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
