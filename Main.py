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
from pydantic import BaseModel

# --- CONFIGURATION ---
# YAHAN APNA NAYA FRESH TOKEN DAALNA (Kyunki purana unauthorized ho gaya tha)
BOT_TOKEN = "8995479806:AAHW047HqtIdYrAq3UU8rgYHrN_tILkdBUo" 
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzny9o.mongodb.net/?appName=Cluster0"
WEBAPP_URL = "https://meesho-grand-line.onrender.com"

# --- INITIALIZATION ---
app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# CORS Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB Client Setup with SSL fix
client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]
orders_collection = db["orders"]

# Public folder setup for Mini App frontend
os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

# --- WEBHOOK SETUP (THE GAME CHANGER) ---
@app.on_event("startup")
async def on_startup():
    # Server start hote hi Telegram ko batayega ki messages is URL par bhejo
    webhook_url = f"{WEBAPP_URL}/webhook"
    await bot.set_webhook(url=webhook_url, drop_pending_updates=True)

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()

@app.post("/webhook")
async def bot_webhook(request: Request):
    # Telegram se aane wale messages ko FastAPI yahan receive karega
    update_data = await request.json()
    update = types.Update(**update_data)
    await dp.feed_update(bot=bot, update=update)
    return {"status": "ok"}

# --- FASTAPI ROUTES (UI to Backend) ---
class OrderRequest(BaseModel):
    user_id: str
    product_id: str
    price: int

@app.post("/api/v1/place-order")
async def place_order(order: OrderRequest):
    user = await users_collection.find_one({"user_id": order.user_id})
    if not user or user.get("wallet", 0) < order.price:
        return {"status": "error", "message": "Insufficient funds in wallet"}
    
    await users_collection.update_one({"user_id": order.user_id}, {"$inc": {"wallet": -order.price}})
    await orders_collection.insert_one({"user_id": order.user_id, "product_id": order.product_id, "status": "Placed"})
    
    await bot.send_message(order.user_id, f"✅ Order Placed Successfully!\nProduct: {order.product_id}\nDeducted: ₹{order.price}")
    return {"status": "success", "message": "Order placed successfully!"}

@app.get("/api/v1/wallet/{user_id}")
async def get_wallet(user_id: str):
    user = await users_collection.find_one({"user_id": user_id})
    return {"status": "success", "wallet": user.get("wallet", 0) if user else 0}

# --- TELEGRAM BOT HANDLERS ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = str(message.from_user.id)
    # New user register
    if not await users_collection.find_one({"user_id": user_id}):
        await users_collection.insert_one({"user_id": user_id, "wallet": 0, "accounts": []})

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Open Shop", web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text="➕ Add Account", callback_data="add_account"),
         InlineKeyboardButton(text="👥 My Accounts", callback_data="my_accounts")],
        [InlineKeyboardButton(text="💳 Add Funds", callback_data="add_funds"),
         InlineKeyboardButton(text="📦 History", callback_data="history")]
    ])
    
    text = (
        "🛍 **Meesho Order Bot**\n"
        "Your personal Meesho shopping concierge\n\n"
        "💰 Wallet: ₹0.00\n"
        "👤 Accounts: 0 linked\n\n"
        "✨ Service fee — ₹10.00 per order\n\n"
        "Pick an option below to get started 👇"
    )
    await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")

@dp.callback_query()
async def handle_callbacks(callback: types.CallbackQuery):
    if callback.data == "add_account":
        await callback.message.answer("Please send your phone number to login and receive OTP.")
    elif callback.data == "add_funds":
        await callback.message.answer("Scan the QR code or send UPI ID to add funds.")
    await callback.answer()

# --- SERVER RUNNER ---
if __name__ == "__main__":
    # Webhook wale system mein hum sirf FastAPI ko normal tareeke se run karte hain
    uvicorn.run("Main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
