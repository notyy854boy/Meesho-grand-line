import certifi
import asyncio
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from motor.motor_asyncio import AsyncIOMotorClient
import uvicorn

# --- CONFIGURATION ---
BOT_TOKEN = "8995479806:AAHW047HqtIdYrAq3UU8rgYHrN_tILkdBUo"
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzny9o.mongodb.net/?appName=Cluster0"
WEBAPP_URL = "https://main-py-owl7.onrender.com"

# --- INITIALIZATION ---
app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]

os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")

# --- FASTAPI ROUTES (Frontend) ---
@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is 100% awake and running!"}

# --- TELEGRAM BOT LOGIC ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = str(message.from_user.id)
    if not await users_collection.find_one({"user_id": user_id}):
        await users_collection.insert_one({"user_id": user_id, "wallet": 0, "accounts": []})

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Open Shop", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    
    await message.answer(
        "🛍 **Grand Line Store**\nYour system is now completely ONLINE!\nClick below to open your app.", 
        reply_markup=keyboard, 
        parse_mode="Markdown"
    )

# Background task to run bot polling alongside FastAPI without Webhook conflicts
@app.on_event("startup")
async def startup_event():
    # Purana koi bhi webhook ho toh usko hata do taaki polling smoothly chale
    await bot.delete_webhook(drop_pending_updates=True)
    # Background mein bot ka polling shuru kar do
    asyncio.create_task(dp.start_polling(bot))

if __name__ == "__main__":
    uvicorn.run("Main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
