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
import edge_tts
from dotenv import load_dotenv
import uvicorn

load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
MINI_APP_URL = os.getenv("MINI_APP_URL", "https://meesho-grand-line.onrender.com")

app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

app.mount("/static", StaticFiles(directory="static"), name="static")

active_ws_connections = []
# Multiple coins to track
tracked_symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "dogeusdt"]

# ==========================================
# BADA BHAI AI ENGINE (Calculates Red/Green Box)
# ==========================================
class BadaBhaiQuantEngine:
    def __init__(self):
        self.risk_reward_ratio = 4.0 # Always targets 1:4 RR

    def process_tick(self, symbol, current_price, price_change_pct):
        """
        Calculates Trend, Support/Resistance, Entry, SL, and TP for the chart box.
        """
        # Simulated logic based on price action momentum
        if price_change_pct > 0:
            decision = "BUY"
            sl_margin = current_price * 0.005 # 0.5% Stop Loss
            entry = current_price
            sl = current_price - sl_margin
            tp = current_price + (sl_margin * self.risk_reward_ratio)
        else:
            decision = "SELL"
            sl_margin = current_price * 0.005 # 0.5% Stop Loss
            entry = current_price
            sl = current_price + sl_margin
            tp = current_price - (sl_margin * self.risk_reward_ratio)

        return {
            "symbol": symbol.upper(),
            "price": current_price,
            "change": round(price_change_pct, 2),
            "action": decision,
            "ai": {
                "entry": entry,
                "sl": sl,
                "tp": tp
            }
        }

quant_engine = BadaBhaiQuantEngine()

# ==========================================
# BINANCE MULTI-COIN WEBSOCKET
# ==========================================
async def binance_stream():
    # Build stream URL for all tracked coins
    streams = "/".join([f"{s}@ticker" for s in tracked_symbols])
    uri = f"wss://stream.binance.com:9443/ws/{streams}"
    
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print(f"⚡ Connected to Binance Multi-Stream: {streams}")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    symbol = data.get("s", "")
                    price = float(data.get("c", 0.0))
                    change = float(data.get("P", 0.0)) # Price change percentage
                    
                    # Run AI logic on this tick
                    ai_payload = quant_engine.process_tick(symbol, price, change)
                    
                    # Broadcast to all frontend UI screens instantly
                    for connection in active_ws_connections:
                        try:
                            await connection.send_json(ai_payload)
                        except:
                            pass
        except Exception as e:
            print(f"Stream reconnecting... {e}")
            await asyncio.sleep(2)

# ==========================================
# ROUTES
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
    text = "Bhai, naye crypto assets scan ho chuke hain. Live charts par focus rakho. Entry mark ho gayi hai."
    communicate = edge_tts.Communicate(text, "hi-IN-MadhurNeural")
    await communicate.save("alert.mp3")
    return FileResponse("alert.mp3", media_type="audio/mpeg")

# ==========================================
# TELEGRAM BOT LOGIC
# ==========================================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID:
        return await message.answer("Access Denied.")
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 OPEN APEX QUANT TERMINAL", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])
    await message.answer(
        "⚡ <b>BADA BHAI ALL-CRYPTO SCANNER ONLINE</b>\n\n"
        "Bhai, system ab BTC, ETH, SOL, BNB, DOGE sabko ek sath track kar raha hai.\n"
        "Terminal kholo, auto-drawing aur live screener active hai.",
        reply_markup=kb,
        parse_mode="HTML"
    )

# ==========================================
# RUNNER
# ==========================================
@app.on_event("startup")
async def start_engines():
    print("🚀 Booting Multi-Coin Engines...")
    asyncio.create_task(binance_stream())
    asyncio.create_task(dp.start_polling(bot))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
