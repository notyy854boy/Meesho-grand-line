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

BOT_TOKEN = "8995479806:AAHW047HqtIdYrAq3UU8rgYHrN_tILkdBUo"
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzny9o.mongodb.net/?appName=Cluster0"
WEBAPP_URL = "https://main-py-owl7.onrender.com"

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
        print(f"Webhook successfully set to: {webhook_url}")
    except Exception as e:
        print(f"Startup webhook error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.session.close()

@app.get("/")
async def serve_frontend():
    return FileResponse("public/index.html")

@app.get("/ping")
async def ping_server():
    return {"status": "Success", "message": "Server is 100% active and running!"}

@app.post("/webhook")
async def webhook(request: Request):
    try:
        json_data = await request.json()
        print("Incoming Telegram Update:", json_data)
        update = types.Update.model_validate(json_data)
        await dp.feed_update(bot, update)
    except Exception as e:
        print(f"Webhook Execution Error: {e}")
    return {"ok": True}

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = str(message.from_user.id)
    user = await users_collection.find_one({"user_id": user_id})
    if not user:
        await users_collection.insert_one({
            "user_id": user_id,
            "wallet": 0,
            "accounts": []
        })

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Open Shop", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    
    await message.answer(
        "Grand Line Store\n\nYour system is completely online!\nClick below to open your store app.",
        reply_markup=keyboard
    )

@app.get("/api/accounts")
async def get_accounts(phone: str = None):
    if phone:
        account = await accounts_collection.find_one({"phone": phone})
        return {"ok": True, "account": account}
    accounts = await accounts_collection.find().to_list(100)
    return {"ok": True, "accounts": accounts}

@app.get("/api/suggest")
async def suggest_products(q: str = ""):
    suggestions = ["kurti", "t-shirt", "shoes", "shirt", "watch"]
    filtered = [s for s in suggestions if q.lower() in s.lower()] if q else suggestions
    return {"ok": True, "suggestions": filtered}

class CheckoutModel(BaseModel):
    phone: str
    items: list
    address_id: str = None

@app.post("/api/checkout")
async def checkout(data: CheckoutModel):
    doc = {
        "phone": data.phone,
        "items": data.items,
        "address_id": data.address_id,
        "status": "Pending"
    }
    res = await orders_collection.insert_one(doc)
    return {"ok": True, "message": "Order placed successfully", "order_id": str(res.inserted_id)}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
    
