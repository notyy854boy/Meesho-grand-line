import os
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel

# Configuration from Environment Variables
TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "YOUR_MONGODB_ATLAS_URI")

app = FastAPI()
bot = Bot(token=TOKEN)
dp = Dispatcher()

# MongoDB Client Setup
client = AsyncIOMotorClient(MONGO_URI)
db = client["meesho_bot_db"]

# Mount Frontend Static Files
app.mount("/static", StaticFiles(directory="public"), name="static")


class OrderRequest(BaseModel):
  telegram_id: int
  product_id: int
  product_name: str
  order_total: float


# --- Telegram Bot Commands ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
  text = (
      "🛍️ **Meesho Order Bot**\n"
      "_Your personal Meesho shopping concierge_\n\n"
      "💰 Wallet · ₹0.00\n"
      "👤 Accounts · 5 linked\n\n"
      "✨ Service fee — ₹10.00 per order\n\n"
      "Pick an option below to get started 👇"
  )

  builder = InlineKeyboardBuilder()
  builder.row(
      types.InlineKeyboardButton(
          text="🛍️ Open Shop",
          web_app=types.WebAppInfo(url="https://your-render-url.onrender.com/"),
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


# --- Backend API Endpoints ---
@app.post("/api/v1/place-order")
async def place_order(data: OrderRequest):
  user = await db.users.find_one({"telegram_id": data.telegram_id})

  service_fee = 10.0
  total_required = data.order_total + service_fee
  user_balance = user.get("wallet_balance", 0.0) if user else 0.0

  if user_balance < total_required:
    return {
        "success": False,
        "message": f"₹{total_required - user_balance} needed in wallet",
    }

  # Deduct balance & Save Order
  new_balance = user_balance - total_required
  await db.users.update_one(
      {"telegram_id": data.telegram_id},
      {"$set": {"wallet_balance": new_balance}},
      upsert=True,
  )

  order_doc = {
      "order_id": "OD" + datetime.now().strftime("%d%H%M%S"),
      "telegram_id": data.telegram_id,
      "product_name": data.product_name,
      "amount_paid": data.order_total,
      "service_fee": service_fee,
      "status": "Dispatched",
      "courier_partner": "Valmo",
      "tracking_id": "VL" + datetime.now().strftime("%Y%m%d%H%M"),
      "created_at": datetime.utcnow(),
  }

  await db.orders.insert_one(order_doc)

  return {
      "success": True,
      "message": "Order placed successfully!",
      "order_id": order_doc["order_id"],
      "tracking_id": order_doc["tracking_id"],
  }


@app.on_event("startup")
async def on_startup():
  # Start bot polling in background or use webhooks
  print("Bot and FastAPI server started successfully!")
