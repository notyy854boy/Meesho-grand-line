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
MINI_APP_URL = os.getenv("MINI_APP_URL", "https://main-py-owl7.onrender.com")

# ==========================================
# INITIALIZATION (FastAPI, MongoDB, Telegram)
# ==========================================
app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["bada_bhai_db"]
logs_collection = db["live_radar_logs"]

# Serve the Mini App UI
app.mount("/static", StaticFiles(directory="static"), name="static")

active_ws_connections = []
global_live_data = {"symbol": "BTCUSDT", "action": "BUY", "rr": "1:0.0"}

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
                    
                    # Basic Shadow Logic: Update active state based on volume/price
                    if "BTC" in symbol:
                        global_live_data["symbol"] = symbol
                        global_live_data["action"] = "BUY" if price > 60000 else "SELL"
                        global_live_data["rr"] = "1:4.2"
                        
                        # Send to all connected Mini Apps
                        for connection in active_ws_connections:
                            await connection.send_json(global_live_data)
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
    if message.from_user.id != ADMIN_ID:
        return await message.answer("Access Denied.")
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 OPEN 4K APEX TERMINAL", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])
    await message.answer(
        "⚡ <b>BADA BHAI 2026 TERMINAL ONLINE</b>\n\n"
        "Bhai, database (MongoDB) connected hai. Render backend live hai.\n"
        "Click below to open the dashboard.",
        reply_markup=kb,
        parse_mode="HTML"
    )

# ==========================================
# MASTER RUNNER
# ==========================================
async def main():
    asyncio.create_task(binance_stream())
    config = uvicorn.Config(app=app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
    server = uvicorn.Server(config)
    await asyncio.gather(
        server.serve(),
        dp.start_polling(bot)
    )

if __name__ == "__main__":
    asyncio.run(main())
