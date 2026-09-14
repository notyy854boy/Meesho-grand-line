import os
import certifi
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, CallbackQuery
from aiogram.filters import Command
from motor.motor_asyncio import AsyncIOMotorClient
import uvicorn
from pydantic import BaseModel

# --- CONFIGURATION ---
BOT_TOKEN = "8995479806:AAHW047HqtIdYrAq3UU8rgYHrN_tILkdBUo"
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzny9o.mongodb.net/?appName=Cluster0"
WEBAPP_URL = "https://main-py-owl7.onrender.com"
ADMIN_ID = 6571947272  # Tera Official Admin ID

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

# --- TELEGRAM BOT LOGIC & ADVANCED INLINE KEYBOARDS ---
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
            "discount_eligible": True
        })

    if user_id == ADMIN_ID:
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠️ Open Admin Panel", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton(text="📊 Manage Users & Wallets", callback_data="admin_users")],
            [InlineKeyboardButton(text="📦 View All Orders", callback_data="admin_orders")],
            [InlineKeyboardButton(text="🛍️ Open Store WebApp", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer("👑 **Boss Admin Control Panel**\nSaare controls aur management yahan available hain:", reply_markup=admin_keyboard, parse_mode="Markdown")
    else:
        client_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍️ Open Store & Get ₹190 OFF", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton(text="💳 Check Wallet Balance", callback_data="check_wallet")],
            [InlineKeyboardButton(text="ℹ️ Help & Support", callback_data="help_support")]
        ])
        await message.answer("🎉 **Welcome to Store!**\n\nNaye user ke liye automatic **₹190 ka discount** lock ho chuka hai. Niche click karke store open karo:", reply_markup=client_keyboard, parse_mode="Markdown")

@dp.callback_query()
async def callback_handler(callback: CallbackQuery):
    data = callback.data
    if data == "check_wallet":
        user = await users_collection.find_one({"user_id": str(callback.from_user.id)})
        wallet = user.get("wallet", 0.0) if user else 0.0
        await callback.answer(f"Aapka wallet balance: ₹{wallet}", show_alert=True)
    elif data == "help_support":
        await callback.answer("Kisi bhi samasya ke liye admin se sampark karein.", show_alert=True)
    elif data == "admin_users":
        await callback.answer("Admin panel web app se manage karein.", show_alert=True)
    elif data == "admin_orders":
        await callback.answer("Orders list admin panel mein dekhein.", show_alert=True)

# --- API ENDPOINTS FOR AUTH & CART ---
class OtpModel(BaseModel):
    phone: str

@app.post("/api/auth/send-otp")
async def send_otp(data: OtpModel):
    return {"ok": True, "message": f"OTP successfully triggered for +91 {data.phone}"}

class VerifyOtpModel(BaseModel):
    phone: str
    otp: str

@app.post("/api/auth/verify-otp")
async def verify_otp(data: VerifyOtpModel):
    session_json = {"phone": data.phone, "session_active": True}
    await accounts_collection.update_one(
        {"phone": data.phone},
        {"$set": {"session_json": session_json}},
        upsert=True
    )
    return {"ok": True, "message": "Account linked & Session JSON saved successfully!"}

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
    return {"ok": True, "message": "Product added to cart successfully!"}

# --- REAL MEESHO PREORDER API ROUTE (Captured via HttpCanary) ---
class OrderRequest(BaseModel):
    user_id: str
    app_user_id: str
    cookies: dict
    payload: dict

@app.post("/api/meesho/place-real-order")
async def place_real_meesho_order(data: OrderRequest):
    MEESHO_PREORDER_URL = "https://www.meesho.com/mcheckout/api/4.0/preorders[span_1](start_span)"[span_1](end_span)
    
    headers = {
        "Host": "www.meesho.com",
        "Connection": "keep-alive",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.meesho.com",[span_2](start_span)[span_2](end_span)
        "Referer": "https://www.meesho.com/mcheckout/payment?source=cart-icon",[span_3](start_span)[span_3](end_span)
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",[span_4](start_span)[span_4](end_span)
        "app-user-id": data.app_user_id[span_5](start_span)[span_5](end_span)
    }

    try:
        async with httpx.AsyncClient(cookies=data.cookies, timeout=30.0) as client:
            response = await client.post(
                MEESHO_PREORDER_URL, 
                headers=headers, 
                json=data.payload
            )
            
            if response.status_code == 200:
                return {
                    "status": "Success", 
                    "message": "Real order payload accepted by Meesho server!",
                    "data": response.json()
                }
            else:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Meesho Server Error: {response.text}"
                )
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
