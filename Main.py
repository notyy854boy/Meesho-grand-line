import asyncio
import os
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import requests

# Direct Configuration (Aapke credentials set hain)
BOT_TOKEN = "8995479806:AAEWU0T7ClnI5KHlm9IRRC3zhcGPu82OMOk"
MONGO_URI = "mongodb+srv://rakib8802:rakib8802@cluster0.4kzmy9o.mongodb.net/?appName=Cluster0"
MINI_APP_URL = "https://your-render-url.onrender.com"

app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# MongoDB Client Setup
client = AsyncIOMotorClient(MONGO_URI)
db = client["meesho_bot_db"]

# Public folder setup for Mini App frontend
os.makedirs("public", exist_ok=True)
app.mount("/static", StaticFiles(directory="public"), name="static")


class OrderRequest(BaseModel):
  telegram_id: int
  user_cookie: str
  order_payload: dict
  order_total: float


# --- Telegram Bot /start Command ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
  text = (
      "🛍️ *Meesho Order Bot*\n_Your personal Meesho shopping concierge_\n\n💰"
      " Wallet · ₹0.00\n👤 Accounts · 5 linked\n\n✨ Service fee — ₹10.00 per"
      " order\n\nPick an option below to get started 👇"
  )

  builder = InlineKeyboardBuilder()
  builder.row(
      types.InlineKeyboardButton(
          text="🛍️ Open Shop", web_app=types.WebAppInfo(url=MINI_APP_URL)
      )
  )
  builder.row(
      types.InlineKeyboardButton(
          text="➕ Add Account", callback_data="add_account"
      ),
      types.InlineKeyboardButton(
          text="👥 My Accounts", callback_data="my_accounts"
      ),
  )
  builder.row(
      types.InlineKeyboardButton(text="💳 Add Funds", callback_data="add_funds"),
      types.InlineKeyboardButton(text="📜 History", callback_data="history"),
  )

  await message.answer(
      text, reply_markup=builder.as_markup(), parse_mode="Markdown"
  )


# --- Meesho Preorder API Execution Function ---
def trigger_meesho_preorder(user_cookie: str, order_payload: dict):
  url = "https://www.meesho.com/mcheckout/api/4.0/preorders"

  headers = {
      "content-type": "application/json",
      "cookie": user_cookie,
      "origin": "https://www.meesho.com",
      "referer": "https://www.meesho.com/mcheckout/payment?source=cart-icon",
      "user-agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
          " like Gecko) Chrome/152.0.0.0 Safari/537.36"
      ),
  }

  try:
    response = requests.post(url, json=order_payload, headers=headers)
    if response.status_code == 200:
      return {"success": True, "data": response.json()}
    else:
      return {
          "success": False,
          "status_code": response.status_code,
          "error": response.text,
      }
  except Exception as e:
    return {"success": False, "error": str(e)}


# --- Backend API for Complete Wallet & Order Processing ---
@app.post("/api/v1/place-order")
async def place_order(data: OrderRequest):
  try:
    users_col = db["users"]
    orders_col = db["orders"]

    # 1. Check User Wallet Balance (Order Total + ₹10 Service Fee)
    user = await users_col.find_one({"telegram_id": data.telegram_id})
    service_fee = 10.0
    total_required = data.order_total + service_fee
    user_balance = user.get("wallet_balance", 0.0) if user else 0.0

    if user_balance < total_required:
      return {
          "success": False,
          "message": f"₹{total_required - user_balance} needed in wallet",
      }

    # 2. Trigger Real Meesho Preorder API
    meesho_response = trigger_meesho_preorder(
        data.user_cookie, data.order_payload
    )

    if not meesho_response.get("success"):
      return {
          "success": False,
          "message": "Meesho API rejected order",
          "details": meesho_response,
      }

    # 3. Deduct Wallet Balance after successful order hit
    new_balance = user_balance - total_required
    await users_col.update_one(
        {"telegram_id": data.telegram_id},
        {"$set": {"wallet_balance": new_balance}},
        upsert=True,
    )

    # 4. Save Order & Tracking Details in MongoDB
    order_doc = {
        "order_id": "OD" + str(datetime.now().strftime("%d%H%M%S")),
        "telegram_id": data.telegram_id,
        "amount_paid": data.order_total,
        "service_fee": service_fee,
        "status": "Dispatched",
        "courier_partner": "Valmo",
        "tracking_id": "VL" + str(datetime.now().strftime("%Y%m%d%H%M")),
        "meesho_response": meesho_response.get("data"),
        "created_at": datetime.utcnow(),
    }

    await orders_col.insert_one(order_doc)

    return {
        "success": True,
        "message": "Order placed successfully!",
        "order_id": order_doc["order_id"],
        "tracking_id": order_doc["tracking_id"],
    }
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))


# Background task to run Telegram Bot polling live
async def run_telegram_bot():
  print("Starting Telegram Bot Polling Live...")
  await dp.start_polling(bot)


@app.on_event("startup")
async def startup_event():
  await client.admin.command("ping")
  print("Connected to MongoDB Atlas successfully!")
  asyncio.create_task(run_telegram_bot())


@app.on_event("shutdown")
async def shutdown_event():
  client.close()
  await bot.session.close()
