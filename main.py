import os
import certifi
from fastapi import FastAPI, Request, HTTPException
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
ADMIN_ID = 6571947272

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

# --- DATABASE SETUP ---
client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]
orders_collection = db["orders"]
accounts_collection = db["accounts"]

# --- STATIC FILES ---
os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")

@app.on_event("startup")
async def on_startup():
    try:
        webhook_url = f"{WEBAPP_URL}/webhook"
        await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
        print(f"Webhook automatically set to: {webhook_url}")
    except Exception as e:
        print(f"Webhook startup error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()

# --- ROUTES ---
@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Meesho Aalu Server is 100% active!"}

@app.api_route("/webhook", methods=["GET", "POST", "HEAD"])
async def webhook(request: Request):
    if request.method in ["GET", "HEAD"]:
        return {"status": "Webhook active"}
    try:
        json_data = await request.json()
        await dp.feed_webhook_update(bot, json_data)
    except Exception as e:
        print(f"Webhook Execution Error: {e}")
    return {"ok": True}

# --- TELEGRAM BOT LOGIC ---
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    user_id_str = str(user_id)

    user = await users_collection.find_one({"user_id": user_id_str})
    if not user:
        await users_collection.insert_one({
            "user_id": user_id_str,
            "wallet": 0.0,
            "role": "admin" if user_id == ADMIN_ID else "client"
        })

    if user_id == ADMIN_ID:
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠️ Open Admin Panel", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton(text="🛍️ Open Store", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("👑 **Boss Admin Panel**\nManage orders and user accounts seamlessly.", reply_markup=admin_keyboard, parse_mode="Markdown")
    else:
        client_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍️ Open Meesho Aalu Store", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("🎉 **Welcome to Meesho Aalu!**\n\nRegister now to unlock guaranteed **₹180 New User Max Discount** on your orders!", reply_markup=client_keyboard, parse_mode="Markdown")

# --- API ENDPOINTS ---
class OrderModel(BaseModel):
    user_id: str
    phone: str
    product_name: str
    price: float

@app.post("/api/order/place")
async def place_real_order(data: OrderModel):
    # Enforce official ₹180 max new user discount
    discounted_price = max(0, data.price - 180.0)
    
    order_doc = {
        "user_id": data.user_id,
        "phone": data.phone,
        "product_name": data.product_name,
        "original_price": data.price,
        "final_price": discounted_price,
        "discount_applied": 180.0,
        "status": "Placed"
    }
    res = await orders_collection.insert_one(order_doc)
    return {
        "ok": True,
        "message": f"Real order placed successfully with ₹180 New User Discount applied!",
        "order_id": str(res.inserted_id),
        "final_price": discounted_price
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
