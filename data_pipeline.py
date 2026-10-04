import os
import asyncio
import io
import ccxt.async_support as ccxt_async
import pandas as pd
import pandas_ta as ta
from supabase import create_client, Client
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from contextlib import asynccontextmanager

# ==========================================
# 1. CONFIGURATION & SETUP
# ==========================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "TUMHARA_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEB_APP_URL = "https://meesho-grand-line.onrender.com" 

SUPABASE_URL = "https://sdlfggybitpoxczdeihq.supabase.co"
# (Bhai apna real key env variable me rakhna aage chalke, abhi ke liye working hai)
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkbGZnZ3liaXRwb3hjemRlaWhxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc5MTEyMjAxOSwiZXhwIjoyMTA2Njk4MDE5fQ.-qa5c60tZf1viwGhQpYqiGq0vv0Fy9IfIbK7quID838"
SUPABASE_BUCKET = "quant-lake"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

is_harvesting = False
harvest_status = {"status": "IDLE", "last_batch": "None", "errors": 0}

# ==========================================
# 2. DATA HARVESTER PIPELINE (BACKEND)
# ==========================================
class MasterDataPipeline:
    def __init__(self):
        self.exchange = ccxt_async.kucoin({'enableRateLimit': True})
    
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
            else:
                since = self.exchange.parse8601('2025-01-01T00:00:00Z')

            retry_count = 0
            while True:
                try:
                    ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                    if not ohlcv or len(ohlcv) < 2:
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
                    if retry_count > 5: raise Exception(f"Failed after 5 retries.")
                    await asyncio.sleep(5 * retry_count)
                    
        except Exception as e:
            harvest_status["status"] = f"ERROR: {e}"
        finally:
            await self.exchange.close()
            is_harvesting = False

# ==========================================
# 3. TELEGRAM BOT COMMANDS
# ==========================================
@dp.message(Command("start", "panel"))
async def cmd_start(message: types.Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID: return
    # Permanent Direct Button for UI
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 OPEN BADA BHAI TERMINAL", web_app=WebAppInfo(url=WEB_APP_URL))]],
        resize_keyboard=True
    )
    await message.answer("👑 **BADA BHAI ENGINE ONLINE**\nTerminal kholne ke liye neeche button daba 👇", reply_markup=keyboard, parse_mode="Markdown")

@dp.message(Command("harvest"))
async def cmd_harvest(message: types.Message):
    global is_harvesting
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID: return
    if is_harvesting: return await message.answer("⚠️ Pipeline already running!")
    await message.answer("⚙ Starting KuCoin Data Harvester...")
    asyncio.create_task(MasterDataPipeline().run_pipeline(bot, message.chat.id, "BTC/USDT", "15m"))

# ==========================================
# 4. FASTAPI SERVER & REAL MATH ENDPOINT
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.delete_webhook(drop_pending_updates=True)
    task = asyncio.create_task(dp.start_polling(bot))
    yield
    task.cancel()

api = FastAPI(lifespan=lifespan)
api.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@api.get("/api/predict")
def get_prediction():
    """Ye API directly UI ke Prediction button se call hogi"""
    try:
        res = supabase.table("live_ohlcv").select("*").order("id", desc=True).limit(100).execute()
        if not res.data or len(res.data) < 50:
            return JSONResponse({"status": "error", "message": "Not enough data yet."})
            
        df = pd.DataFrame(res.data)
        df = df.iloc[::-1].reset_index(drop=True)
        
        # Real Math Calculations
        df['RSI'] = ta.rsi(df['close'], length=14)
        macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
        df = pd.concat([df, macd], axis=1)

        latest = df.iloc[-1]
        trend = "BULLISH 🚀" if (latest['RSI'] < 30 and latest['MACD_12_26_9'] > latest['MACDs_12_26_9']) else ("BEARISH 🩸" if latest['RSI'] > 70 else "NEUTRAL ⚖️")
        
        return JSONResponse({
            "status": "success",
            "price": f"${latest['close']:.2f}",
            "rsi": f"{latest['RSI']:.2f}",
            "macd": f"{latest['MACD_12_26_9']:.2f}",
            "trend": trend
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

# ==========================================
# 5. FRONTEND UI (THE PRO TERMINAL)
# ==========================================
@api.get("/", response_class=HTMLResponse)
def root():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Bada Bhai Terminal</title>
        <script src="https://telegram.org/js/telegram-web-app.js"></script>
        <style>
            body { 
                background-color: #0b0e14; color: #d1d4dc; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                margin: 0; padding: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; 
            }
            .header { display: flex; justify-content: space-between; padding: 10px; background: #131722; border-bottom: 1px solid #2a2e39; }
            .nav-btn { 
                background: #1e222d; color: #d1d4dc; border: 1px solid #363c4e; padding: 8px 5px; border-radius: 4px; 
                font-size: 12px; font-weight: bold; cursor: pointer; flex: 1; margin: 0 4px; text-align: center; transition: 0.2s;
            }
            .nav-btn:active { background: #2962ff; color: white; border-color: #2962ff; }
            .chart-wrapper { flex: 1; width: 100%; position: relative; }
            .footer-panel { display: flex; padding: 10px; background: #131722; border-top: 1px solid #2a2e39; gap: 10px; }
            .stat-box { background: #1e222d; padding: 10px; border-radius: 6px; flex: 1; font-size: 11px; border: 1px solid #2a2e39; }
            .live-dot { height: 8px; width: 8px; background-color: #00ff00; border-radius: 50%; display: inline-block; box-shadow: 0 0 8px #00ff00; animation: blink 1.5s infinite; }
            @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }
            #ai_status { color: #e0e3eb; margin-top: 5px; font-size: 12px; }
        </style>
    </head>
    <body>
        <div class="header">
            <div class="nav-btn" id="btn_badabhai">🤖 Bada Bhai</div>
            <div class="nav-btn" id="btn_predict">🎯 Prediction</div>
            <div class="nav-btn" id="btn_volume">🔥 Volume List</div>
        </div>
        
        <div class="chart-wrapper" id="tv_chart"></div>

        <div class="footer-panel">
            <div class="stat-box">
                <div style="color:#8a93a6; font-weight:bold;">CONNECTION</div>
                <div style="margin-top:5px;"><span class="live-dot"></span> LIVE (KuCoin)</div>
            </div>
            <div class="stat-box" style="flex: 1.5;">
                <div style="color:#8a93a6; font-weight:bold;">AI ENGINE VERDICT</div>
                <div id="ai_status">Monitoring Market...</div>
            </div>
        </div>

        <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
        <script type="text/javascript">
            Telegram.WebApp.ready();
            Telegram.WebApp.expand();
            Telegram.WebApp.setHeaderColor('#131722');
            Telegram.WebApp.setBackgroundColor('#0b0e14');

            new TradingView.widget({
                "autosize": true,
                "symbol": "KUCOIN:BTCUSDT", 
                "interval": "15", 
                "timezone": "Asia/Kolkata", 
                "theme": "dark",
                "style": "1",
                "locale": "en",
                "enable_publishing": false,
                "backgroundColor": "#0b0e14",
                "gridColor": "#1f293d",
                "hide_top_toolbar": false, 
                "hide_legend": false,
                "save_image": false,
                "details": true,
                "container_id": "tv_chart"
            });
            
            // Asli Math Call Jab "Prediction" Button Dabe
            document.getElementById('btn_predict').addEventListener('click', async function() {
                const statusBox = document.getElementById('ai_status');
                statusBox.innerHTML = '<span style="color:#f5a623;">Calculating Math...</span>';
                
                try {
                    const response = await fetch('/api/predict');
                    const data = await response.json();
                    
                    if(data.status === 'success') {
                        statusBox.innerHTML = `
                            <span style="color:#00ff00;">${data.trend}</span><br>
                            RSI: ${data.rsi} | MACD: ${data.macd}
                        `;
                    } else {
                        statusBox.innerHTML = `<span style="color:red;">Error: ${data.message}</span>`;
                    }
                } catch(err) {
                    statusBox.innerHTML = '<span style="color:red;">Network Error</span>';
                }
            });
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(api, host="0.0.0.0", port=port)
