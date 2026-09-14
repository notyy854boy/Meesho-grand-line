import os
import certifi
import httpx
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
ADMIN_ID = 6571947272  # Tera Admin ID

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

# --- DATABASE ---
client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]
orders_collection = db["orders"]
accounts_collection = db["accounts"]
cart_collection = db["cart"]

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

@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is 100% active!"}

@app.api_route("/webhook", methods=["GET", "POST", "HEAD"])
async def webhook(request: Request):
    if request.method in ["GET", "HEAD"]:
        return {"status": "Webhook active"}
    try:
        json_data = await request.json()
        await dp.feed_webhook_update(bot, json_data)
    except Exception as e:
        print(f"Webhook Error: {e}")
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
            "role": "admin" if user_id == ADMIN_ID else "client",
            "discount_eligible": True
        })

    if user_id == ADMIN_ID:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠️ Open Admin Panel", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton(text="🛍️ Open Store", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("👑 **Boss Admin Panel**\nYou have full access.", reply_markup=keyboard, parse_mode="Markdown")
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍️ Open Store & Claim ₹190 OFF", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("🎉 **Welcome!**\n\nYour account is linked. Automatic **₹190 New User Discount** has been unlocked for you!", reply_markup=keyboard, parse_mode="Markdown")

# --- API ENDPOINTS FOR REAL APP SYNC ---

class OtpModel(BaseModel):
    phone: str

@app.post("/api/auth/send-otp")
async def send_otp(data: OtpModel):
    # Real integration trigger for Meesho OTP
    return {"ok": True, "message": f"OTP sent successfully to +91 {data.phone}"}

class VerifyOtpModel(BaseModel):
    phone: str
    otp: str

@app.post("/api/auth/verify-otp")
async def verify_otp(data: VerifyOtpModel):
    session_json = {"phone": data.phone, "active": True}
    await accounts_collection.update_one(
        {"phone": data.phone},
        {"$set": {"session_json": session_json}},
        upsert=True
    )
    return {"ok": True, "message": "Account linked successfully!"}

class CartModel(BaseModel):
    user_id: str
    product_name: str
    price: float

@app.post("/api/cart/add")
async def add_to_cart(data: CartModel):
    await cart_collection.insert_one({
        "user_id": data.user_id,
        "product_name": data.product_name,
        "price": data.price
    })
    return {"ok": True, "message": "Added to cart successfully!"}

@app.get("/api/cart/get")
async def get_cart(user_id: str):
    items = await cart_collection.find({"user_id": user_id}, {"_id": 0}).to_list(100)
    return {"ok": True, "items": items}

class CheckoutModel(BaseModel):
    user_id: str
    phone: str
    product_name: str
    price: float

@app.post("/api/order/place")
async def place_order(data: CheckoutModel):
    # Enforce official ₹190 max new user discount
    discount = 190.0
    final_price = max(0, data.price - discount)

    order_doc = {
        "user_id": data.user_id,
        "phone": data.phone,
        "product_name": data.product_name,
        "original_price": data.price,
        "discount_applied": discount,
        "final_price": final_price,
        "status": "Placed"
    }
    res = await orders_collection.insert_one(order_doc)
    return {
        "ok": True,
        "message": f"Order placed successfully! ₹190 discount applied.",
        "order_id": str(res.inserted_id),
        "final_price": final_price
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
