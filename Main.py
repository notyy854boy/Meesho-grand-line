import certifi
import asyncio
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from motor.motor_asyncio import AsyncIOMotorClient
import uvicorn

# --- CONFIGURATION (Bas yahan apna naya token dalna hai) ---
BOT_TOKEN = "8995479806:AAHW047HqtIdYrAq3UU8rgYHrN_tILkdBUo" 
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzny9o.mongodb.net/?appName=Cluster0"
WEBAPP_URL = "https://meesho-grand-line.onrender.com"

# --- INITIALIZATION ---
app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]

os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")

# --- WEBHOOK & ROUTES ---
@app.on_event("startup")
async def on_startup():
    # Yeh line Telegram ko batayegi ki message kahan bhejne hain
    webhook_url = f"{WEBAPP_URL}/webhook"
    await bot.set_webhook(url=webhook_url, drop_pending_updates=True)

@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is 100% awake and running!"}

@app.post("/webhook")
async def bot_webhook(request: Request):
    try:
        update_data = await request.json()
        update = types.Update(**update_data)
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        print("Error in webhook:", e)
    return {"status": "ok"}

# --- TELEGRAM BOT LOGIC ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = str(message.from_user.id)
    if not await users_collection.find_one({"user_id": user_id}):
        await users_collection.insert_one({"user_id": user_id, "wallet": 0, "accounts": []})

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Open Shop", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    
    await message.answer("🛍 **Grand Line Store**\nYour system is now completely ONLINE!\nClick below to open your app.", reply_markup=keyboard, parse_mode="Markdown")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
