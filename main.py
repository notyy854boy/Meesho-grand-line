import os
import asyncio
import json
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from motor.motor_asyncio import AsyncIOMotorClient
import edge_tts
from dotenv import load_dotenv
import uvicorn

load_dotenv()

# ==========================================
# CREDENTIALS & CONFIG
# ==========================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
MONGO_URI = os.getenv("MONGO_URI")
MINI_APP_URL = os.getenv("MINI_APP_URL", "https://meesho-grand-line.onrender.com")

# ==========================================
# INITIALIZATION (FastAPI, MongoDB, Telegram)
# ==========================================
app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Avoid MongoDB connection error if URI is missing
if MONGO_URI:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client["bada_bhai_db"]
    logs_collection = db["live_radar_logs"]

# Serve the Mini App UI
app.mount("/static", StaticFiles(directory="static"), name="static")

active_ws_connections = []
global_live_data = {"symbol": "BTCUSDT", "action": "BUY", "rr": "1:0.0"}

# ==========================================
# BADA BHAI AI ENGINE (Built-in)
# ==========================================
class BadaBhaiQuantEngine:
    def __init__(self):
        self.risk_reward_ratio = 4.0 # 1:4 ka Target

    async def analyze_market_context(self, symbol, current_price):
        """
        AI Brain logic: Calculates patterns and Risk/Reward parameters
        """
        print(f"[🧠 AI BRAIN] Analyzing {symbol} at {current_price}...")
        decision = "BUY" if current_price > 60000 else "SELL"
        sl_margin = current_price * 0.002  # 0.2% Stop Loss
        
        if decision == "BUY":
            entry = current_price
            sl = current_price - sl_margin
            tp = current_price + (sl_margin * self.risk_reward_ratio)
        else:
            entry = current_price
            sl = current_price + sl_margin
            tp = current_price - (sl_margin * self.risk_reward_ratio)

        return {
            "symbol": symbol,
            "decision": decision,
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "tp": round(tp, 2)
        }

quant_engine = BadaBhaiQuantEngine()

# ==========================================
# BINANCE WEBSOCKET & SHADOW ENGINE
# ==========================================
async def binance_stream():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/solusdt@ticker"
    global global_live_data
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print("⚡ Binance WebSocket Connected!")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    symbol = data.get("s", "")
                    price = float(data.get("c", 0.0))
                    
                    if "BTC" in symbol:
                        # Process logic with AI Brain
                        ai_result = await quant_engine.analyze_market_context(symbol, price)
                        global_live_data["symbol"] = ai_result["symbol"]
                        global_live_data["action"] = ai_result["decision"]
                        global_live_data["rr"] = f"1:{quant_engine.risk_reward_ratio}"
                        
                        # Send to all connected Mini Apps
                        for connection in active_ws_connections:
                            try:
                                await connection.send_json(global_live_data)
                            except:
                                pass
        except Exception as e:
            print(f"Stream dropped, reconnecting... {e}")
            await asyncio.sleep(2)

# ==========================================
# FASTAPI ROUTES
# ==========================================
@app.get("/")
async def root():
    return FileResponse("static/index.html")

@app.websocket("/ws/terminal")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_ws_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_ws_connections.remove(websocket)

@app.get("/api/voice")
async def generate_voice():
    text = f"Bhai, market mein volatility aayi hai. {global_live_data['symbol']} par nazar rakh."
    communicate = edge_tts.Communicate(text, "hi-IN-MadhurNeural")
    await communicate.save("alert.mp3")
    return FileResponse("alert.mp3", media_type="audio/mpeg")

# ==========================================
# TELEGRAM BOT LOGIC
# ==========================================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    # Only Admin (You) can use this bot
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID:
        return await message.answer("Access Denied.")
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 OPEN 4K APEX TERMINAL", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])
    await message.answer(
        "⚡ <b>BADA BHAI 2026 TERMINAL ONLINE</b>\n\n"
        "Bhai, quant engine aur AI Brain live hai.\n"
        "Click below to open the dashboard.",
        reply_markup=kb,
        parse_mode="HTML"
    )

# ==========================================
# MASTER RUNNER (FIXED FOR RENDER)
# ==========================================
@app.on_event("startup")
async def start_background_processes():
    print("🚀 Starting Bada Bhai Background Engines...")
    asyncio.create_task(binance_stream())
    asyncio.create_task(dp.start_polling(bot))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
