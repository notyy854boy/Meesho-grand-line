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
from pydantic import BaseModel

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
    allow_headers=["*"],
)

# --- MONGODB SETUP ---
client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]
orders_collection = db["orders"]
accounts_collection = db["accounts"]

# --- STATIC FILES SETUP ---
os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is fully active and running!"}

# --- TELEGRAM WEBHOOK SETUP & DEBUGGING ---
@app.on_event("startup")
async def startup_event():
    try:
        webhook_url = f"{WEBAPP_URL}/webhook"
        await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
        
        # Yeh line Render logs mein batayegi ki Telegram webhook status kya hai
        webhook_info = await bot.get_webhook_info()
        print(f"=== TELEGRAM WEBHOOK INFO ===")
        print(webhook_info)
    except Exception as e:
        print(f"Webhook setup error: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    await bot.session.close()

@app.post("/webhook")
async def bot_webhook(request: Request):
    try:
        data = await request.json()
        print(f"Incoming Telegram Update: {data}") # Yeh print batayega ki Telegram se message aa raha hai ya nahi
        update = types.Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        print(f"Webhook processing error: {e}")
    return {"ok": True}

# --- TELEGRAM BOT HANDLERS ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = str(message.from_user.id)
    
    user = await users_collection.find_one({"user_id": user_id})
    if not user:
        await users_collection.insert_one({
            "user_id": user_id,
            "wallet": 0,
            "accounts": []
        })

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Open Shop", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    
    await message.answer(
        "🛍 **Grand Line Store**\n\nYour account is linked successfully.\nClick below to open your shopping dashboard:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# --- API ROUTES ---
@app.get("/api/accounts")
async def get_accounts(phone: str = None):
    if phone:
        account = await accounts_collection.find_one({"phone": phone})
        return {"ok": True, "account": account}
    accounts = await accounts_collection.find().to_list(100)
    return {"ok": True, "accounts": accounts}

@app.get("/api/suggest")
async def suggest_products(q: str = ""):
    suggestions = ["short kurtis", "shirt for men", "shoes", "t-shirts", "kurti combo"]
    filtered = [s for s in suggestions if q.lower() in s.lower()] if q else suggestions
    return {"ok": True, "suggestions": filtered}

class CheckoutRequest(BaseModel):
    phone: str
    items: list
    address_id: str = None

@app.post("/api/checkout")
async def checkout_order(data: CheckoutRequest):
    order_doc = {
        "phone": data.phone,
        "items": data.items,
        "address_id": data.address_id,
        "status": "Pending"
    }
    result = await orders_collection.insert_one(order_doc)
    return {
        "ok": True,
        "message": "Order placed successfully",
        "order_id": str(result.inserted_id)
    }

if __name__ == "__main__":
    uvicorn.run("Main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
