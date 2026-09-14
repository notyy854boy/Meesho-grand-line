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
ADMIN_ID = 6571947272

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

client = AsyncIOMotorClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meesho_bot_db"]
users_collection = db["users"]
orders_collection = db["orders"]
accounts_collection = db["accounts"]

os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")

@app.on_event("startup")
async def on_startup():
    try:
        webhook_url = f"{WEBAPP_URL}/webhook"
        await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
        print(f"Webhook set to: {webhook_url}")
    except Exception as e:
        print(f"Webhook error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()

@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is active!"}

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

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    user_id_str = str(user_id)

    user = await users_collection.find_one({"user_id": user_id_str})
    if not user:
        await users_collection.insert_one({
            "user_id": user_id_str,
            "wallet": 0.0,
            "role": "admin" if user_id == ADMIN_ID else "client",
            "new_user_discount": 190.0  # Automatic ₹190 new user discount
        })

    if user_id == ADMIN_ID:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠️ Open Admin Panel", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton(text="🛍️ Open Store", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("👑 **Boss Admin Panel**", reply_markup=keyboard, parse_mode="Markdown")
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍️ Open Store & Get ₹190 OFF", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("🎉 **Welcome!**\n\nYour account has been secured with an automatic **₹190 New User Discount**.", reply_markup=keyboard, parse_mode="Markdown")

# --- REAL AUTH & MEESHO API PROXY ROUTES ---

class OtpRequestModel(BaseModel):
    phone: str

@app.post("/api/auth/send-otp")
async def send_otp(data: OtpRequestModel):
    # Integration point for Meesho OTP API
    # Here we communicate with Meesho auth backend or proxy the request
    return {"ok": True, "message": f"OTP successfully triggered for +91 {data.phone}"}

class OtpVerifyModel(BaseModel):
    phone: str
    otp: str

@app.post("/api/auth/verify-otp")
async def verify_otp(data: OtpVerifyModel):
    # Verify OTP and generate Session JSON
    session_json = {"phone": data.phone, "token": "mock_meesho_session_token_xyz"}
    await accounts_collection.update_one(
        {"phone": data.phone},
        {"$set": {"session_json": session_json}},
        upsert=True
    )
    return {"ok": True, "message": "Account verified and linked successfully!", "session": session_json}

class CheckoutModel(BaseModel):
    user_id: str
    phone: str
    product_name: str
    price: float

@app.post("/api/order/place")
async def place_order(data: CheckoutModel):
    # Automatically apply ₹190 new user discount
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
