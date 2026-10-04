import os
import asyncio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
import edge_tts
from dotenv import load_dotenv
import uvicorn

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
MINI_APP_URL = os.getenv("MINI_APP_URL", "https://meesho-grand-line.onrender.com")

app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse("static/index.html")

@app.get("/api/voice")
async def generate_voice():
    text = "Bhai, market structure change ho gaya hai. Prediction screen par long position draw kar di hai, stop loss follow karna."
    communicate = edge_tts.Communicate(text, "hi-IN-MadhurNeural")
    await communicate.save("alert.mp3")
    return FileResponse("alert.mp3", media_type="audio/mpeg")

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID:
        return await message.answer("Access Denied.")
        
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 OPEN GOD ENGINE", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])
    await message.answer(
        "⚡ <b>GOD ENGINE V4 (DRAWING MATCHED)</b>\n\n"
        "Bhai, tumhari drawing ke hisaab se First Screen (Chart), Second Screen (AI Draw) aur Third Screen (News) set hai.\n"
        "Live scanner seedha phone se connected hai, koi lag nahi.\n\n"
        "Open karo aur check karo.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@app.on_event("startup")
async def start_engines():
    print("🚀 Booting God Engine...")
    asyncio.create_task(dp.start_polling(bot))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
