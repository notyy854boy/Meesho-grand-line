import certifi
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

# --- 100% PRE-CONFIGURED SETTINGS (Tujhe kuch nahi badalna) ---
BOT_TOKEN = "8995479806:AAHW047HqtIdYrAq3UU8rgYHrN_tILkdBUo"
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzny9o.mongodb.net/?appName=Cluster0"
WEBAPP_URL = "https://main-py-owl7.onrender.com"

# --- SYSTEM INITIALIZATION ---
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

# --- WEBHOOK & LIFECYCLE MANAGEMENT ---
@app.on_event("startup")
async def startup_event():
    # Purana webhook hata kar naya set kiya taaki bot turant active ho jaye
    await bot.delete_webhook(drop_pending_updates=True)
    webhook_url = f"{WEBAPP_URL}/webhook"
    await bot.set_webhook(url=webhook_url)

@app.on_event("shutdown")
async def shutdown_event():
    await bot.session.close()

# --- SERVER ROUTES ---
@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is fully online and ready!"}

@app.post("/webhook")
async def bot_webhook(request: Request):
    try:
        data = await request.json()
        update = types.Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        print(f"Webhook error: {e}")
    return {"ok": True}

# --- TELEGRAM BOT LOGIC ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = str(message.from_user.id)
    
    # MongoDB mein user check aur save karna
    user_exists = await users_collection.find_one({"user_id": user_id})
    if not user_exists:
        await users_collection.insert_one({
            "user_id": user_id, 
            "wallet": 0, 
            "accounts": []
        })

    # Mini App open karne ke liye clean keyboard button
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Open Shop", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    
    await message.answer(
        "🛍 **Grand Line Store**\n\nWelcome! Your system is completely online and linked.\nClick the button below to open your store.", 
        reply_markup=keyboard, 
        parse_mode="Markdown"
    )

if __name__ == "__main__":
    uvicorn.run("Main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
    
