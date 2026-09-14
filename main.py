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
ADMIN_ID = 6571947272  # Tera Official Admin Telegram ID

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
admin_logs_collection = db["admin_logs"]

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
    return {"status": "Success", "message": "Meesho Automation Server is 100% active!"}

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

# --- TELEGRAM BOT LOGIC (ADMIN vs CLIENT) ---
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    user_id_str = str(user_id)

    # Check if user exists in DB
    user = await users_collection.find_one({"user_id": user_id_str})
    if not user:
        await users_collection.insert_one({
            "user_id": user_id_str,
            "wallet": 0.0,
            "role": "admin" if user_id == ADMIN_ID else "client"
        })

    if user_id == ADMIN_ID:
        # --- ADMIN PANEL INTERFACE ---
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠️ Open Admin Panel", web_app=WebAppInfo(url=f"{WEBAPP_URL}/admin-panel"))],
            [InlineKeyboardButton(text="🛒 Open Shop App", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer(
            "👑 **Welcome Boss (Admin Panel)**\nYou have full control over wallets, accounts, and orders.",
            reply_markup=admin_keyboard,
            parse_mode="Markdown"
        )
    else:
        # --- CLIENT PANEL INTERFACE ---
        client_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍️ Open Shop & Apply Discount", web_app=WebAppInfo(url=WEBAPP_URL))]
        ])
        await message.answer(
            "🛍️ **Welcome to Grand Line Store!**\n\nClick below to open the shop app, log in with your accounts, and grab official discounts up to ₹190!",
            reply_markup=client_keyboard,
            parse_mode="Markdown"
        )

# --- ADMIN PANEL ROUTE ---
@app.get("/admin-panel")
async def serve_admin_panel():
    # Dedicated admin panel view file
    return FileResponse("public/index.html")

# --- API ENDPOINTS FOR FRONTEND & AUTOMATION ---

@app.get("/api/user/info")
async def get_user_info(user_id: str):
    user = await users_collection.find_one({"user_id": user_id})
    if not user:
        return {"ok": False, "message": "User not found"}
    return {"ok": True, "wallet": user.get("wallet", 0), "role": user.get("role", "client")}

# Admin feature: Add/Deduct Funds from Client Wallet
class FundUpdateModel(BaseModel):
    admin_id: int
    target_user_id: str
    amount: float

@app.post("/api/admin/update-wallet")
async def update_wallet(data: FundUpdateModel):
    if data.admin_id != ADMIN_ID:
        raise HTTPException(status_code=403, detail="Unauthorized Action!")
    
    result = await users_collection.update_one(
        {"user_id": data.target_user_id},
        {"$inc": {"wallet": data.amount}}
    )
    if result.matched_count == 0:
        return {"ok": False, "message": "Target client user not found!"}
    
    return {"ok": True, "message": f"Successfully updated wallet by ₹{data.amount}"}

# Multiple Accounts & JSON Login Management
class AccountModel(BaseModel):
    phone: str
    json_session: dict

@app.post("/api/accounts/add")
async def add_account(data: AccountModel):
    await accounts_collection.update_one(
        {"phone": data.phone},
        {"$set": {"json_session": data.json_session}},
        upsert=True
    )
    return {"ok": True, "message": "Account & JSON Session saved successfully!"}

@app.get("/api/accounts")
async def get_accounts():
    accounts = await accounts_collection.find({}, {"_id": 0, "json_session": 0}).to_list(100)
    return {"ok": True, "accounts": accounts}

@app.get("/api/suggest")
async def suggest_products(q: str = ""):
    suggestions = ["kurti", "t-shirt", "shoes", "shirt", "watch"]
    filtered = [s for s in suggestions if q.lower() in s.lower()] if q else suggestions
    return {"ok": True, "suggestions": filtered}

# Checkout with Official Max Discount Limit (₹190)
class CheckoutModel(BaseModel):
    user_id: str
    phone: str
    items: list
    discount_applied: float

@app.post("/api/checkout")
async def checkout(data: CheckoutModel):
    # Enforce maximum discount limit of ₹190 officially
    max_allowed_discount = 190.0
    final_discount = min(data.discount_applied, max_allowed_discount)

    doc = {
        "user_id": data.user_id,
        "phone": data.phone,
        "items": data.items,
        "discount_applied": final_discount,
        "status": "Pending"
    }
    res = await orders_collection.insert_one(doc)
    return {
        "ok": True, 
        "message": f"Order placed with official max discount of ₹{final_discount}!", 
        "order_id": str(res.inserted_id)
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
