import os
import asyncio
import io
import ccxt.async_support as ccxt_async
import pandas as pd
import pandas_ta as ta
import httpx
from supabase import create_client, Client
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from contextlib import asynccontextmanager

# ==========================================
# 1. VIP CONFIGURATION & DATABASES
# ==========================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEB_APP_URL = "https://meesho-grand-line.onrender.com"

# 🗄️ SUPABASE: The Core Data Lake (Live OHLCV & Zip Archives)
SUPABASE_URL = "https://sdlfggybitpoxczdeihq.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "YOUR_SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
SUPABASE_BUCKET = "quant-lake"

# 🧠 MONGODB: The AI Memory (For Future Logs, User Settings, Learning States)
MONGO_URI = os.getenv("MONGO_URI", "YOUR_MONGO_URI")
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["bada_bhai_ai_memory"]
ai_logs_col = db["system_logs"]

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==========================================
# 2. ALADDIN SCANNER (Supabase Powered)
# ==========================================
class AladdinScanner:
    def __init__(self):
        self.exchange = ccxt_async.kucoin({'enableRateLimit': True})
        self.coins = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "BNB/USDT", "XRP/USDT"]

    async def scan_market_24x7(self):
        while True:
            try:
                for symbol in self.coins:
                    ohlcv = await self.exchange.fetch_ohlcv(symbol, '15m', limit=100)
                    if not ohlcv: continue
                    
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    
                    # 1. LIVE DATA TO SUPABASE (Upsert)
                    records = []
                    for _, row in df.iterrows():
                        records.append({
                            "id": f"{symbol}_15m_{int(row['timestamp'])}",
                            "symbol": symbol, "timeframe": "15m", "timestamp": int(row['timestamp']),
                            "open": float(row['open']), "high": float(row['high']),
                            "low": float(row['low']), "close": float(row['close']), "volume": float(row['volume'])
                        })
                    # Background task to not block scanner
                    asyncio.create_task(asyncio.to_thread(supabase.table("live_ohlcv").upsert(records).execute))

                    # 2. ARCHIVE TO SUPABASE BUCKET (Lifetime Storage)
                    csv_buffer = io.BytesIO()
                    df.to_csv(csv_buffer, index=False, compression='gzip')
                    filename = f"archive/{symbol.replace('/','')}_latest.csv.gz"
                    asyncio.create_task(asyncio.to_thread(
                        supabase.storage.from_(SUPABASE_BUCKET).upload, filename, csv_buffer.getvalue(), {"upsert": "true"}
                    ))
                
                await asyncio.sleep(60 * 5) # 5 Minute cycle
            except Exception as e:
                print(f"Scanner Error: {e}")
                await asyncio.sleep(60)

# ==========================================
# 3. FASTAPI SERVER & PRO API ENDPOINTS
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(dp.start_polling(bot))
    asyncio.create_task(AladdinScanner().scan_market_24x7())
    # Log boot event to MongoDB
    await ai_logs_col.insert_one({"event": "God Engine Boot", "status": "Online"})
    yield

api = FastAPI(lifespan=lifespan)
api.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@api.get("/")
def serve_terminal():
    return FileResponse("static/index.html")

# --- API 1: HOT VOLUME & FEAR/GREED MACRO ---
@api.get("/api/macro-status")
async def get_macro_status():
    """Volume aur Global Emotion check karega"""
    try:
        # Fear & Greed Index
        async with httpx.AsyncClient() as client:
            fg_res = await client.get("https://api.alternative.me/fng/")
            fg_data = fg_res.json()['data'][0]
            
        exchange = ccxt_async.kucoin()
        tickers = await exchange.fetch_tickers()
        await exchange.close()
        
        usdt_pairs = {k: v for k, v in tickers.items() if '/USDT' in k and v['quoteVolume']}
        sorted_pairs = sorted(usdt_pairs.items(), key=lambda x: x[1]['quoteVolume'], reverse=True)
        hot_coins = [{"symbol": k.replace("/",""), "vol": v['quoteVolume']} for k,v in sorted_pairs[:10]]
        
        return JSONResponse({
            "status": "success", 
            "fear_greed": {"value": fg_data['value'], "emotion": fg_data['value_classification']},
            "hot_volume": hot_coins
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

# --- API 2: NEWS SCANNER WITH SENTIMENT ---
@api.get("/api/news")
async def get_global_news():
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get("https://api.coingecko.com/api/v3/news")
            news_list = res.json().get('data', [])[:10]
            
            # Simple AI Sentiment Tagger
            for news in news_list:
                title = news['title'].lower()
                if any(word in title for word in ['surge', 'bull', 'adopt', 'launch', 'high']):
                    news['sentiment'] = "BULLISH 🟢"
                elif any(word in title for word in ['hack', 'ban', 'drop', 'crash', 'sec']):
                    news['sentiment'] = "BEARISH 🔴"
                else:
                    news['sentiment'] = "NEUTRAL ⚪"
                    
            return JSONResponse({"status": "success", "data": news_list})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

# --- API 3: THE SUPABASE MATH BRAIN ---
@api.get("/api/predict")
async def get_prediction(symbol: str = "BTCUSDT"):
    try:
        db_symbol = symbol.replace("USDT", "/USDT")
        
        # 🟢 FETCHING STRICTLY FROM SUPABASE NOW
        res = supabase.table("live_ohlcv").select("*").eq("symbol", db_symbol).order("id", desc=True).limit(100).execute()
        
        if not res.data or len(res.data) < 50:
            return JSONResponse({"status": "error", "message": "Supabase scanning data..."})
            
        df = pd.DataFrame(res.data)
        df = df.iloc[::-1].reset_index(drop=True)
        
        # 🟢 HARDCORE MATH (RSI, MACD, BOLLINGER BANDS, ATR)
        df['RSI'] = ta.rsi(df['close'], length=14)
        macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
        df = pd.concat([df, macd], axis=1)
        
        # Bollinger Bands (Squeeze Detection)
        bbands = ta.bbands(df['close'], length=20, std=2)
        df = pd.concat([df, bbands], axis=1)
        
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
        latest = df.iloc[-1]
        current_price = latest['close']
        recent_high = df['high'].rolling(window=20).max().iloc[-1]
        recent_low = df['low'].rolling(window=20).min().iloc[-1]
        
        bb_width = (latest['BBU_20_2.0'] - latest['BBL_20_2.0']) / latest['BBM_20_2.0']
        is_squeeze = bb_width < 0.05 # Volatility contraction (Big move coming)

        # 🟢 THE AI VERDICT
        trend = "NEUTRAL ⚖"
        accuracy = 50
        reason = "Market sideways hai, order block form ho raha hai."
        
        if latest['RSI'] < 35 and current_price <= (recent_low * 1.02):
            trend = "BULLISH 🚀"
            accuracy = 85
            reason = "RSI oversold zone mein hai aur price key support se bounce le raha hai."
        elif latest['RSI'] > 65 and current_price >= (recent_high * 0.98):
            trend = "BEARISH 🩸"
            accuracy = 82
            reason = "RSI overbought hai, liquidity grab complete hua hai, rejection ke chances hain."
            
        if is_squeeze:
            reason += " ⚠️ BOLLINGER SQUEEZE DETECTED: Bada breakout/breakdown aane wala hai!"

        return JSONResponse({
            "status": "success",
            "price": current_price,
            "rsi": round(latest['RSI'], 2),
            "macd": round(latest['MACD_12_26_9'], 2),
            "support": recent_low,
            "resistance": recent_high,
            "atr": latest['ATR'],
            "trend": trend,
            "reason": reason,
            "accuracy": f"{accuracy}%"
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(api, host="0.0.0.0", port=port)
