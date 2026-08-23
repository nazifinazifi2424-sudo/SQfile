import asyncio
import threading
import time
import math
import os
import sys
import traceback
import inspect
import socket
import platform
import shutil
from http.server import BaseHTTPRequestHandler, HTTPServer

import telebot
from telebot import types

from pyrogram import Client
from pyrogram.errors import FloodWait


# =============================================================
# 1. ENVIRONMENT VARIABLES
# =============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()

# Render Web Service yana ba da PORT da kansa
PORT_RAW = os.getenv("PORT", "10000").strip()


# -------------------------------------------------------------
# CHECK REQUIRED VARIABLES
# -------------------------------------------------------------

if not BOT_TOKEN:
    print("❌ ERROR: BOT_TOKEN bai samu ba.")
    sys.exit(1)

if not ADMIN_ID_RAW:
    print("❌ ERROR: ADMIN_ID bai samu ba.")
    sys.exit(1)

if not API_ID_RAW:
    print("❌ ERROR: API_ID bai samu ba.")
    sys.exit(1)

if not API_HASH:
    print("❌ ERROR: API_HASH bai samu ba.")
    sys.exit(1)


# -------------------------------------------------------------
# CONVERT VARIABLES
# -------------------------------------------------------------

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except Exception:
    print("❌ ERROR: ADMIN_ID dole ya zama number.")
    sys.exit(1)


try:
    API_ID = int(API_ID_RAW)
except Exception:
    print("❌ ERROR: API_ID dole ya zama number.")
    sys.exit(1)


try:
    PORT = int(PORT_RAW)
except Exception:
    PORT = 10000


# =============================================================
# 2. GLOBAL STATE
# =============================================================

USER_STATES = {}
PENDING_DATA = {}
PROGRESS_STATE = {}

PYRO_READY = threading.Event()
PYRO_FAILED = threading.Event()
BOT_READY = threading.Event()

pyro_loop = asyncio.new_event_loop()


# =============================================================
# 3. STARTUP INFORMATION
# =============================================================

print("\n" + "=" * 70)
print("🚀 SABON CONVERTER BOT - STARTING")
print("=" * 70)

print(f"ADMIN_ID: {ADMIN_ID}")
print(f"API_ID: {API_ID}")
print(f"API_HASH: {'SET' if API_HASH else 'NOT SET'}")
print(f"BOT_TOKEN: {'SET' if BOT_TOKEN else 'NOT SET'}")
print(f"PORT: {PORT}")
print(f"Python: {sys.version}")
print(f"Platform: {platform.platform()}")
print("=" * 70)


# =============================================================
# 4. TELEBOT
# =============================================================

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="Markdown"
)


# =============================================================
# 5. PYROGRAM
# =============================================================

pyro_bot = Client(
    "pyro_converter_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)


# =============================================================
# 6. TIME
# =============================================================

def now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")


# =============================================================
# 7. ADMIN DEBUG
# =============================================================

def send_admin_debug(text, level="DEBUG"):

    full_text = (
        f"🛠️ *{level}*\n"
        f"🕒 `{now_time()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{text}"
    )

    print("\n" + "=" * 70)
    print(f"[{level}] {now_time()}")
    print(text)
    print("=" * 70)

    try:
        bot.send_message(
            ADMIN_ID,
            full_text,
            parse_mode="Markdown"
        )

    except Exception as e:
        print(
            f"[DEBUG SEND ERROR] {e}"
        )


# =============================================================
# 8. ADMIN EXCEPTION
# =============================================================

def send_admin_exception(title, exc):

    tb = traceback.format_exc()

    text = (
        f"🚨 *{title}*\n\n"
        f"*Exception Type:*\n"
        f"`{type(exc).__name__}`\n\n"
        f"*Exception:*\n"
        f"`{str(exc)}`\n\n"
        f"*TRACEBACK:*\n"
        f"```text\n{tb[-3500:]}\n```"
    )

    print("\n" + "#" * 80)
    print(f"🚨 {title}")
    print(tb)
    print("#" * 80)

    try:

        bot.send_message(
            ADMIN_ID,
            text,
            parse_mode="Markdown"
        )

    except Exception as send_error:

        print(
            f"[EXCEPTION DEBUG SEND ERROR] "
            f"{send_error}"
        )


# =============================================================
# 9. HUMAN BYTES
# =============================================================

def humanbytes(size):

    if not size:
        return "0 B"

    power = 1024
    n = 0

    units = {
        0: "B",
        1: "KiB",
        2: "MiB",
        3: "GiB",
        4: "TiB"
    }

    while size >= power and n < 4:

        size /= power
        n += 1

    return f"{round(size, 2)} {units[n]}"


# =============================================================
# 10. TIME FORMATTER
# =============================================================

def TimeFormatter(milliseconds: int) -> str:

    seconds, milliseconds = divmod(
        int(milliseconds),
        1000
    )

    minutes, seconds = divmod(
        seconds,
        60
    )

    hours, minutes = divmod(
        minutes,
        60
    )

    days, hours = divmod(
        hours,
        24
    )

    result = ""

    if days:
        result += f"{days}d, "

    if hours:
        result += f"{hours}h, "

    if minutes:
        result += f"{minutes}m, "

    if seconds:
        result += f"{seconds}s, "

    return result[:-2] if result else "0s"


# =============================================================
# 11. SYSTEM INFORMATION
# =============================================================

def system_info():

    try:

        disk = shutil.disk_usage("/")

        disk_text = (
            f"💾 *DISK*\n"
            f"Total: `{humanbytes(disk.total)}`\n"
            f"Used: `{humanbytes(disk.used)}`\n"
            f"Free: `{humanbytes(disk.free)}`"
        )

    except Exception as e:

        disk_text = (
            f"💾 Disk check failed: `{e}`"
        )


    try:

        import psutil

        ram = psutil.virtual_memory()

        ram_text = (
            f"🧠 *RAM*\n"
            f"Total: `{humanbytes(ram.total)}`\n"
            f"Available: `{humanbytes(ram.available)}`"
        )

        cpu_text = (
            f"⚙️ *CPU*\n"
            f"Usage: `{psutil.cpu_percent(interval=0.2)}%`"
        )

    except Exception:

        ram_text = "🧠 RAM: psutil unavailable"
        cpu_text = ""


    return (
        f"🖥️ *SYSTEM INFORMATION*\n\n"
        f"Python: `{platform.python_version()}`\n"
        f"Hostname: `{socket.gethostname()}`\n"
        f"PID: `{os.getpid()}`\n"
        f"PORT: `{PORT}`\n\n"
        f"{disk_text}\n\n"
        f"{ram_text}\n"
        f"{cpu_text}"
    )


# =============================================================
# 12. STATUS EDITOR
# =============================================================

async def edit_status(
    chat_id,
    message_id,
    text
):

    try:

        await pyro_bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text
        )

        return True

    except Exception as e:

        print(
            f"[STATUS EDIT ERROR] "
            f"chat={chat_id} "
            f"message={message_id} "
            f"error={e}"
        )

        return False


# =============================================================
# 13. PROGRESS CALLBACK
# =============================================================

async def progress_args(
    current,
    total,
    text_type,
    chat_id,
    message_id,
    start_time,
    stage
):

    try:

        now = time.time()

        diff = now - start_time

        if diff <= 0:
            diff = 0.001


        percentage = (
            current * 100 / total
            if total > 0
            else 0
        )


        speed = current / diff


        remaining = (
            (total - current) / speed
            if speed > 0
            else 0
        )


        eta = TimeFormatter(
            remaining * 1000
        )


        blocks = max(
            0,
            min(
                10,
                math.floor(
                    percentage / 10
                )
            )
        )


        progress = (
            "["
            + ("▰" * blocks)
            + ("▱" * (10 - blocks))
            + "]"
        )


        text = (
            f"{text_type}\n\n"
            f"{progress} `{percentage:.2f}%`\n\n"
            f"📊 *Adadi:* "
            f"`{humanbytes(current)} / "
            f"{humanbytes(total)}`\n"
            f"⚡ *Speed:* "
            f"`{humanbytes(speed)}/s`\n"
            f"⏳ *Lokacin da ya rage:* "
            f"`{eta}`"
        )


        key = (
            chat_id,
            message_id,
            stage
        )


        last_time = PROGRESS_STATE.get(
            key,
            0
        )


        # Kada a yi Telegram edit fiye da kima
        if (
            now - last_time >= 3
            or current >= total
        ):

            PROGRESS_STATE[key] = now

            await edit_status(
                chat_id,
                message_id,
                text
            )


    except Exception as e:

        print(
            f"[PROGRESS ERROR] "
            f"stage={stage} "
            f"error={e}"
        )


# =============================================================
# 14. PYROGRAM MAIN
# =============================================================

async def pyro_main():

    send_admin_debug(
        "🚀 *PYROGRAM THREAD YA FARA*\n\n"
        f"Thread: `{threading.current_thread().name}`\n"
        f"Loop: `{id(asyncio.get_running_loop())}`\n\n"
        "Ana kokarin haɗa Pyrogram...",
        "PYROGRAM START"
    )


    try:

        send_admin_debug(
            "🔌 Ana kiran `pyro_bot.start()`...\n\n"
            "Idan ya tsaya a nan, matsalar tana "
            "bangaren Pyrogram/API credentials.",
            "PYROGRAM CONNECT"
        )


        result = pyro_bot.start()


        if inspect.isawaitable(result):

            await result


        PYRO_READY.set()
        PYRO_FAILED.clear()


        send_admin_debug(
            "🟢🟢🟢 *PYROGRAM READY* 🟢🟢🟢\n\n"
            "Pyrogram ya haɗu da Telegram lafiya.\n\n"
            f"Ready: `{PYRO_READY.is_set()}`\n"
            f"Loop: `{id(asyncio.get_running_loop())}`",
            "PYROGRAM READY"
        )


        # Keep alive
        await asyncio.Event().wait()


    except Exception as e:

        PYRO_FAILED.set()
        PYRO_READY.clear()

        send_admin_exception(
            "💥 PYROGRAM YA CRASH",
            e
        )


    finally:

        try:

            if pyro_bot.is_connected:

                result = pyro_bot.stop()

                if inspect.isawaitable(result):
                    await result

        except Exception as e:

            print(
                f"[PYRO STOP ERROR] {e}"
            )


# =============================================================
# 15. PYROGRAM THREAD
# =============================================================

def start_pyro_loop():

    try:

        asyncio.set_event_loop(
            pyro_loop
        )


        send_admin_debug(
            "🧵 Ana fara Pyrogram asyncio loop...\n\n"
            f"Thread: `{threading.current_thread().name}`\n"
            f"PID: `{os.getpid()}`",
            "PYRO LOOP"
        )


        pyro_loop.run_until_complete(
            pyro_main()
        )


    except Exception as e:

        PYRO_FAILED.set()
        PYRO_READY.clear()

        send_admin_exception(
            "🚨 PYROGRAM LOOP YA CRASH",
            e
        )


    finally:

        try:
            pyro_loop.close()
        except Exception:
            pass


# =============================================================
# 16. START PYROGRAM THREAD
# =============================================================

pyro_thread = threading.Thread(
    target=start_pyro_loop,
    name="PyrogramThread",
    daemon=True
)

pyro_thread.start()


# =============================================================
# 17. /START
# =============================================================

@bot.message_handler(
    commands=["start"]
)
def start_handler(message):

    user_id = message.from_user.id


    if user_id != ADMIN_ID:

        bot.reply_to(
            message,
            "❌ Wannan bot na admin ne kawai."
        )

        return


    bot.reply_to(
        message,
        "🟢 *Bot yana aiki lafiya.*\n\n"
        "Aika `/video` domin fara converter."
    )


    send_admin_debug(
        "👋 `/start` ya iso.\n\n"
        f"User ID: `{user_id}`\n"
        f"Chat ID: `{message.chat.id}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Pyro Failed: `{PYRO_FAILED.is_set()}`",
        "START RECEIVED"
    )


# =============================================================
# 18. /VIDEO
# =============================================================

@bot.message_handler(
    commands=["video"]
)
def start_video_process(message):

    user_id = message.from_user.id


    send_admin_debug(
        "📥 `/video` RECEIVED\n\n"
        f"User ID: `{user_id}`\n"
        f"Chat ID: `{message.chat.id}`\n"
        f"Message ID: `{message.message_id}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Pyro Failed: `{PYRO_FAILED.is_set()}`",
        "VIDEO COMMAND"
    )


    if user_id != ADMIN_ID:

        send_admin_debug(
            f"❌ User `{user_id}` "
            "ba ADMIN_ID ba.",
            "ACCESS DENIED"
        )

        return


    USER_STATES[user_id] = True


    bot.reply_to(
        message,
        "✅ *An kunna tsarin karɓar aiki!*\n\n"
        "Yanzu aiko min da *Video* ko *File* "
        "din da kake son sarrafawa."
    )


    send_admin_debug(
        "🟢 *USER STATE AN KUNNA*\n\n"
        f"USER_STATES[{user_id}] = `True`\n\n"
        "Ana jiran Video/Document.",
        "WAITING FILE"
    )


# =============================================================
# 19. RECEIVE VIDEO / DOCUMENT
# =============================================================

@bot.message_handler(
    content_types=[
        "video",
        "document"
    ]
)
def handle_incoming_file(message):

    user_id = message.from_user.id


    send_admin_debug(
        "📦 *FILE UPDATE YA ISO*\n\n"
        f"User ID: `{user_id}`\n"
        f"Chat ID: `{message.chat.id}`\n"
        f"Message ID: `{message.message_id}`\n"
        f"Content Type: `{message.content_type}`\n"
        f"State: `{USER_STATES.get(user_id, False)}`",
        "FILE RECEIVED"
    )


    if user_id != ADMIN_ID:
        return


    if not USER_STATES.get(
        user_id,
        False
    ):

        send_admin_debug(
            "ℹ️ File ya iso amma babu "
            "active `/video` state.",
            "FILE IGNORED"
        )

        return


    USER_STATES[user_id] = False


    file_name = "Video/File"
    file_size = 0
    file_id = None


    try:

        if message.video:

            file_name = (
                getattr(
                    message.video,
                    "file_name",
                    None
                )
                or "video.mp4"
            )

            file_size = (
                getattr(
                    message.video,
                    "file_size",
                    0
                )
                or 0
            )

            file_id = getattr(
                message.video,
                "file_id",
                None
            )


        elif message.document:

            file_name = (
                getattr(
                    message.document,
                    "file_name",
                    None
                )
                or "file"
            )

            file_size = (
                getattr(
                    message.document,
                    "file_size",
                    0
                )
                or 0
            )

            file_id = getattr(
                message.document,
                "file_id",
                None
            )


        send_admin_debug(
            "✅ *FILE DETAILS*\n\n"
            f"Name: `{file_name}`\n"
            f"Size: `{humanbytes(file_size)}`\n"
            f"File ID: `{file_id}`\n"
            f"Message ID: `{message.message_id}`",
            "FILE DETAILS"
        )


    except Exception as e:

        send_admin_exception(
            "FILE DETAILS ERROR",
            e
        )


    markup = types.InlineKeyboardMarkup()


    btn1 = types.InlineKeyboardButton(
        "🎬 Video",
        callback_data="convert_video"
    )


    btn2 = types.InlineKeyboardButton(
        "📁 File",
        callback_data="convert_file"
    )


    markup.add(
        btn1,
        btn2
    )


    try:

        sent = bot.reply_to(
            message,
            f"✅ *An karɓi fayil:* `{file_name}`\n\n"
            "Shin a wanne tsari kake son dawo da shi?",
            reply_markup=markup
        )


        PENDING_DATA[
            sent.message_id
        ] = {

            "msg_id": message.message_id,

            "chat_id": message.chat.id,

            "file_name": file_name,

            "file_size": file_size,

            "created_at": time.time()
        }


        send_admin_debug(
            "🟢 *PENDING DATA AN AJIYE*\n\n"
            f"Status Message ID: `{sent.message_id}`\n"
            f"Original Message ID: `{message.message_id}`\n"
            f"Chat ID: `{message.chat.id}`\n"
            f"File: `{file_name}`\n"
            f"Size: `{humanbytes(file_size)}`",
            "PENDING DATA"
        )


    except Exception as e:

        send_admin_exception(
            "FILE REPLY ERROR",
            e
        )


# =============================================================
# 20. CALLBACK
# =============================================================

@bot.callback_query_handler(
    func=lambda call:
    call.data.startswith("convert_")
)
def process_conversion_callback(call):

    try:

        send_admin_debug(
            "🔘 *CALLBACK YA ISO*\n\n"
            f"Callback ID: `{call.id}`\n"
            f"Data: `{call.data}`\n"
            f"User ID: `{call.from_user.id}`\n"
            f"Chat ID: `{call.message.chat.id}`\n"
            f"Message ID: `{call.message.message_id}`",
            "CALLBACK RECEIVED"
        )


        bot.answer_callback_query(
            call.id
        )


        if call.from_user.id != ADMIN_ID:
            return


        msg_id = call.message.message_id


        if msg_id not in PENDING_DATA:

            bot.edit_message_text(
                "❌ *Aikin ya fita daga tsarin lokaci.*\n\n"
                "Sake fara `/video`.",
                call.message.chat.id,
                msg_id
            )

            send_admin_debug(
                f"❌ PENDING DATA babu key `{msg_id}`.",
                "PENDING MISSING"
            )

            return


        task_info = PENDING_DATA.pop(
            msg_id
        )


        as_video = (
            call.data == "convert_video"
        )


        chat_id = task_info[
            "chat_id"
        ]

        target_msg_id = task_info[
            "msg_id"
        ]

        file_name = task_info.get(
            "file_name",
            "Unknown"
        )


        send_admin_debug(
            "🟢 *TASK ACCEPTED*\n\n"
            f"Mode: `{'VIDEO' if as_video else 'FILE'}`\n"
            f"Chat ID: `{chat_id}`\n"
            f"Target Message ID: `{target_msg_id}`\n"
            f"Status Message ID: `{msg_id}`\n"
            f"File: `{file_name}`\n"
            f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
            f"Loop Running: `{pyro_loop.is_running()}`",
            "TASK ACCEPTED"
        )


        if not PYRO_READY.is_set():

            bot.edit_message_text(
                "❌ *Pyrogram bai shirya ba.*\n\n"
                "Duba debug na admin.",
                chat_id,
                msg_id
            )

            send_admin_debug(
                "🔴 PYROGRAM NOT READY.\n\n"
                f"Ready: `{PYRO_READY.is_set()}`\n"
                f"Failed: `{PYRO_FAILED.is_set()}`",
                "PYRO NOT READY"
            )

            return


        bot.edit_message_text(
            "🔄 *Shirin gudanar da aiki...*\n\n"
            f"File: `{file_name}`",
            chat_id,
            msg_id
        )


        if not pyro_loop.is_running():

            raise Exception(
                "Pyrogram asyncio loop baya running."
            )


        future = asyncio.run_coroutine_threadsafe(
            run_pyrogram_task(
                chat_id,
                target_msg_id,
                msg_id,
                as_video
            ),
            pyro_loop
        )


        send_admin_debug(
            "🚀 *TASK AN TURAWA PYROGRAM LOOP*\n\n"
            f"Future Done: `{future.done()}`\n"
            f"Cancelled: `{future.cancelled()}`\n"
            f"Loop Running: `{pyro_loop.is_running()}`",
            "TASK QUEUED"
        )


    except Exception as e:

        send_admin_exception(
            "💥 CALLBACK CRASH",
            e
        )


# =============================================================
# 21. PYROGRAM TASK
# =============================================================

async def run_pyrogram_task(
    chat_id,
    target_msg_id,
    status_msg_id,
    as_video
):

    task_started = time.time()

    file_path = ""


    send_admin_debug(
        "🚀🚀🚀 *TASK YA FARA* 🚀🚀🚀\n\n"
        f"Chat ID: `{chat_id}`\n"
        f"Target Message: `{target_msg_id}`\n"
        f"Status Message: `{status_msg_id}`\n"
        f"Mode: `{'VIDEO' if as_video else 'FILE'}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`",
        "TASK START"
    )


    try:

        # =====================================================
        # STEP 0
        # =====================================================

        send_admin_debug(
            "🔎 STEP 0\n\n"
            "Ana gwada Pyrogram connection...",
            "PYRO CHECK"
        )


        me = await pyro_bot.get_me()


        send_admin_debug(
            "🟢 *PYROGRAM CONNECTION OK*\n\n"
            f"ID: `{me.id}`\n"
            f"Username: `@{me.username}`\n"
            f"Name: `{me.first_name}`",
            "PYRO OK"
        )


        # =====================================================
        # STEP 1
        # =====================================================

        status_msg = await pyro_bot.get_messages(
            chat_id,
            status_msg_id
        )


        if not status_msg:

            raise Exception(
                "Ba a samu status message ba."
            )


        # =====================================================
        # STEP 2
        # =====================================================

        await edit_status(
            chat_id,
            status_msg_id,
            "🔄 *Ana shirya fayil...*"
        )


        send_admin_debug(
            "🔎 STEP 2\n\n"
            "Ana samun original message...",
            "GET TARGET"
        )


        msg = await pyro_bot.get_messages(
            chat_id,
            target_msg_id
        )


        if not msg:

            raise Exception(
                "Original message bai samu ba."
            )


        send_admin_debug(
            "🟢 *TARGET MESSAGE OK*\n\n"
            f"Message ID: `{msg.id}`\n"
            f"Video: `{bool(msg.video)}`\n"
            f"Document: `{bool(msg.document)}`\n"
            f"Animation: `{bool(msg.animation)}`",
            "TARGET OK"
        )


        # =====================================================
        # STEP 3 MEDIA
        # =====================================================

        media_type = "UNKNOWN"
        media_size = 0
        media_file_name = "unknown"


        if msg.video:

            media_type = "VIDEO"

            media_size = (
                msg.video.file_size or 0
            )

            media_file_name = (
                msg.video.file_name
                or "video.mp4"
            )


        elif msg.document:

            media_type = "DOCUMENT"

            media_size = (
                msg.document.file_size or 0
            )

            media_file_name = (
                msg.document.file_name
                or "file"
            )


        elif msg.animation:

            media_type = "ANIMATION"

            media_size = (
                msg.animation.file_size or 0
            )

            media_file_name = (
                msg.animation.file_name
                or "animation"
            )


        else:

            raise Exception(
                "Message ba ya dauke da "
                "video/document/animation."
            )


        send_admin_debug(
            "🎬 *MEDIA IDENTIFIED*\n\n"
            f"Type: `{media_type}`\n"
            f"Name: `{media_file_name}`\n"
            f"Telegram Size: `{humanbytes(media_size)}`",
            "MEDIA CHECK"
        )


        # =====================================================
        # STEP 4 DOWNLOAD
        # =====================================================

        await edit_status(
            chat_id,
            status_msg_id,
            "⏬ *Ana Saukewa...*"
        )


        download_start = time.time()


        send_admin_debug(
            "⬇️⬇️⬇️ *DOWNLOAD YA FARA* ⬇️⬇️⬇️\n\n"
            f"File: `{media_file_name}`\n"
            f"Expected Size: `{humanbytes(media_size)}`\n\n"
            "Idan ya tsaya a nan, download/Telegram "
            "connection ne ake bincika.",
            "DOWNLOAD START"
        )


        file_path = await pyro_bot.download_media(
            message=msg,
            progress=progress_args,
            progress_args=(
                "⏬ *Ana Saukewa...*",
                chat_id,
                status_msg_id,
                download_start,
                "DOWNLOAD"
            )
        )


        # =====================================================
        # STEP 5 DOWNLOAD RETURN
        # =====================================================

        send_admin_debug(
            "📥 *DOWNLOAD FUNCTION YA KOMA*\n\n"
            f"Path: `{file_path}`\n"
            f"Type: `{type(file_path).__name__}`",
            "DOWNLOAD RETURN"
        )


        if not file_path:

            raise Exception(
                "download_media() ya dawo empty."
            )


        if not os.path.exists(file_path):

            raise Exception(
                f"File bai wanzu ba:\n{file_path}"
            )


        # =====================================================
        # STEP 6 DISK
        # =====================================================

        disk = shutil.disk_usage(
            os.path.dirname(file_path)
            or "/"
        )


        local_size = os.path.getsize(
            file_path
        )


        send_admin_debug(
            "💾 *LOCAL FILE CHECK*\n\n"
            f"Path: `{file_path}`\n"
            f"Size: `{humanbytes(local_size)}`\n"
            f"Readable: `{os.access(file_path, os.R_OK)}`\n"
            f"Disk Free: `{humanbytes(disk.free)}`",
            "LOCAL FILE OK"
        )


        download_time = (
            time.time() - download_start
        )


        # =====================================================
        # STEP 7 DOWNLOAD COMPLETE
        # =====================================================

        await edit_status(
            chat_id,
            status_msg_id,
            "✅ *Download Ya Kammala!*\n\n"
            f"📂 `{humanbytes(local_size)}`\n\n"
            "⏳ Ana shirin upload..."
        )


        send_admin_debug(
            "✅✅✅ *DOWNLOAD COMPLETE* ✅✅✅\n\n"
            f"File: `{media_file_name}`\n"
            f"Size: `{humanbytes(local_size)}`\n"
            f"Time: `{download_time:.2f}s`\n\n"
            "🔥 DOWNLOAD YA WUCE.\n"
            "Yanzu upload ne zai fara.",
            "DOWNLOAD COMPLETE"
        )


        await asyncio.sleep(2)


        # =====================================================
        # STEP 8 UPLOAD START
        # =====================================================

        upload_start = time.time()


        send_admin_debug(
            "⬆️⬆️⬆️ *UPLOAD YA FARA* ⬆️⬆️⬆️\n\n"
            f"Path: `{file_path}`\n"
            f"Size: `{humanbytes(local_size)}`\n"
            f"Destination: `{chat_id}`\n"
            f"Mode: `{'VIDEO' if as_video else 'DOCUMENT'}`\n\n"
            "🔥 Idan ya tsaya a nan, yanzu muna "
            "binciken upload.",
            "UPLOAD START"
        )


        # =====================================================
        # STEP 9 SEND
        # =====================================================

        if as_video:

            send_admin_debug(
                "🎬 Ana kiran `send_video()` yanzu...\n\n"
                f"File: `{file_path}`\n"
                f"Size: `{humanbytes(local_size)}`",
                "SEND VIDEO"
            )


            result = await pyro_bot.send_video(
                chat_id=chat_id,
                video=file_path,
                caption="🎬 An kammala sarrafa bidiyon ku lafiya!",
                supports_streaming=True,
                progress=progress_args,
                progress_args=(
                    "⬆️ *Ana Turawa (Video)...*",
                    chat_id,
                    status_msg_id,
                    upload_start,
                    "UPLOAD_VIDEO"
                )
            )


        else:

            send_admin_debug(
                "📁 Ana kiran `send_document()` yanzu...\n\n"
                f"File: `{file_path}`\n"
                f"Size: `{humanbytes(local_size)}`",
                "SEND DOCUMENT"
            )


            result = await pyro_bot.send_document(
                chat_id=chat_id,
                document=file_path,
                caption="📁 An kammala sarrafa fayil ɗin ku lafiya!",
                progress=progress_args,
                progress_args=(
                    "⬆️ *Ana Turawa (File)...*",
                    chat_id,
                    status_msg_id,
                    upload_start,
                    "UPLOAD_DOCUMENT"
                )
            )


        # =====================================================
        # STEP 10 UPLOAD COMPLETE
        # =====================================================

        upload_time = (
            time.time() - upload_start
        )


        send_admin_debug(
            "🎉🎉🎉 *UPLOAD YA KAMMALA* 🎉🎉🎉\n\n"
            f"Result: `{bool(result)}`\n"
            f"Type: `{type(result).__name__}`\n"
            f"Upload Time: `{upload_time:.2f}s`\n"
            f"Size: `{humanbytes(local_size)}`",
            "UPLOAD COMPLETE"
        )


        await edit_status(
            chat_id,
            status_msg_id,
            "🎉 *An gama aikin lafiya!*\n\n"
            "🟢 Download: OK\n"
            "🟢 Upload: OK\n"
            "🟢 Telegram Delivery: OK"
        )


        # =====================================================
        # STEP 11 TOTAL
        # =====================================================

        total_time = (
            time.time() - task_started
        )


        send_admin_debug(
            "🏆🏆🏆 *TASK SUCCESS* 🏆🏆🏆\n\n"
            f"File: `{media_file_name}`\n"
            f"Size: `{humanbytes(local_size)}`\n"
            f"Download: `{download_time:.2f}s`\n"
            f"Upload: `{upload_time:.2f}s`\n"
            f"Total: `{total_time:.2f}s`\n\n"
            "🟢 DOWNLOAD OK\n"
            "🟢 LOCAL FILE OK\n"
            "🟢 UPLOAD OK\n"
            "🟢 DELIVERY OK",
            "TASK SUCCESS"
        )


    except FloodWait as e:

        send_admin_debug(
            "⚠️ *TELEGRAM FLOOD WAIT*\n\n"
            f"Jira: `{e.value}` seconds",
            "FLOOD WAIT"
        )


        try:

            await edit_status(
                chat_id,
                status_msg_id,
                f"⚠️ Telegram Limit.\n\n"
                f"Jira daƙiƙa `{e.value}`."
            )

        except Exception:
            pass


    except Exception as e:

        send_admin_exception(
            "💥💥💥 PYROGRAM TASK YA CRASH",
            e
        )


        try:

            await edit_status(
                chat_id,
                status_msg_id,
                "❌ *Aiki ya samu kuskure.*\n\n"
                "Duba DEBUG na admin."
            )

        except Exception:
            pass


    finally:

        # =====================================================
        # CLEANUP
        # =====================================================

        send_admin_debug(
            "🧹 *CLEANUP*\n\n"
            f"Path: `{file_path}`\n"
            f"Exists: `{os.path.exists(file_path) if file_path else False}`",
            "CLEANUP"
        )


        if file_path and os.path.exists(file_path):

            try:

                os.remove(
                    file_path
                )


                send_admin_debug(
                    "🟢 Temporary file an goge.",
                    "CLEANUP SUCCESS"
                )


            except Exception as e:

                send_admin_exception(
                    "CLEANUP ERROR",
                    e
                )


# =============================================================
# 22. RENDER HTTP SERVER
# =============================================================
#
# WANNAN SHI NE MUHIMMIN GYARAN RENDER WEB SERVICE.
#
# Render yana son service ɗin ya saurari PORT.
# Telebot polling ba ya sauraron HTTP port.
#
# Saboda haka muna buɗe ƙaramin HTTP server a background.
# Wannan BA WEBHOOK BA NE.
#
# =============================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path == "/":

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/plain; charset=utf-8"
            )
            self.end_headers()

            self.wfile.write(
                b"Converter Bot is running."
            )

            return


        if self.path == "/health":

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "application/json"
            )
            self.end_headers()

            status = (
                b'{"status":"ok"}'
            )

            self.wfile.write(
                status
            )

            return


        self.send_response(404)
        self.end_headers()


    def log_message(
        self,
        format,
        *args
    ):

        # Kar a cika Render logs da HTTP requests
        return


def start_http_server():

    try:

        server = HTTPServer(
            (
                "0.0.0.0",
                PORT
            ),
            HealthHandler
        )


        send_admin_debug(
            "🌐 *HTTP SERVER YA TASHI*\n\n"
            f"Host: `0.0.0.0`\n"
            f"Port: `{PORT}`\n\n"
            "Render Web Service yanzu zai ga open port.\n\n"
            "⚠️ Wannan HTTP server ba webhook ba ne.",
            "HTTP SERVER READY"
        )


        server.serve_forever()


    except Exception as e:

        send_admin_exception(
            "💥 HTTP SERVER YA KASA",
            e
        )


http_thread = threading.Thread(
    target=start_http_server,
    name="HTTPServerThread",
    daemon=True
)

http_thread.start()


# =============================================================
# 23. STARTUP MONITOR
# =============================================================

def startup_monitor():

    time.sleep(5)


    send_admin_debug(
        "🟢🟢🟢 *FULL STARTUP STATUS* 🟢🟢🟢\n\n"
        f"PID: `{os.getpid()}`\n"
        f"PORT: `{PORT}`\n"
        f"HTTP Thread: `{http_thread.name}`\n"
        f"Pyro Thread: `{pyro_thread.name}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Pyro Failed: `{PYRO_FAILED.is_set()}`\n\n"
        + system_info(),
        "STARTUP STATUS"
    )


threading.Thread(
    target=startup_monitor,
    name="StartupMonitor",
    daemon=True
).start()


# =============================================================
# 24. TELEBOT STARTUP
# =============================================================

if __name__ == "__main__":

    print("\n")
    print("=" * 70)
    print("🟢 TELEBOT MAIN PROCESS")
    print("=" * 70)


    # ---------------------------------------------------------
    # REMOVE OLD WEBHOOK
    # ---------------------------------------------------------

    try:

        bot.delete_webhook(
            drop_pending_updates=True
        )


        print(
            "🧹 An cire tsohon webhook."
        )


        send_admin_debug(
            "🧹 *WEBHOOK AN CIRE*\n\n"
            "Bot zai yi POLLING kawai.\n\n"
            "Ba webhook ake amfani da shi ba.",
            "WEBHOOK REMOVED"
        )


    except Exception as e:

        print(
            f"⚠️ Webhook removal error: {e}"
        )


        send_admin_debug(
            f"⚠️ Webhook removal ya samu kuskure:\n"
            f"`{e}`",
            "WEBHOOK WARNING"
        )


    BOT_READY.set()


    send_admin_debug(
        "🟢🟢🟢 *TELEBOT READY* 🟢🟢🟢\n\n"
        "HTTP server yana aiki.\n"
        "Pyrogram yana cikin background.\n"
        "Yanzu Telebot zai fara polling.\n\n"
        f"PORT: `{PORT}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Pyro Failed: `{PYRO_FAILED.is_set()}`",
        "TELEBOT READY"
    )


    # ---------------------------------------------------------
    # POLLING AUTO RETRY
    # ---------------------------------------------------------

    retry_count = 0


    while True:

        try:

            print(
                "\n🚀 Starting Telebot polling..."
            )


            send_admin_debug(
                "🚀 *POLLING YA FARA*\n\n"
                f"Retry Count: `{retry_count}`\n"
                f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
                f"HTTP Port: `{PORT}`",
                "POLLING START"
            )


            bot.infinity_polling(
                skip_pending=True,
                timeout=60,
                long_polling_timeout=60,
                allowed_updates=None
            )


            # Idan ya tsaya ba tare da exception ba

            retry_count += 1


            send_admin_debug(
                "⚠️ *POLLING YA TSAYA*\n\n"
                f"Retry: `{retry_count}`\n\n"
                "Ana jira seconds 5 sannan a sake kunnawa.",
                "POLLING STOPPED"
            )


            time.sleep(5)


        except KeyboardInterrupt:

            print(
                "🛑 Bot an dakatar da shi."
            )


            send_admin_debug(
                "🛑 Bot an dakatar da shi da hannu.",
                "BOT STOPPED"
            )


            break


        except Exception as e:

            retry_count += 1


            send_admin_exception(
                f"❌ POLLING CRASH - RETRY #{retry_count}",
                e
            )


            print(
                "\n❌ POLLING ERROR\n"
                f"{e}\n"
                f"Retry #{retry_count}\n"
                "⏳ Ana jira seconds 10...\n"
            )


            time.sleep(10)


            # -------------------------------------------------
            # REMOVE WEBHOOK AGAIN
            # -------------------------------------------------

            try:

                bot.delete_webhook(
                    drop_pending_updates=True
                )

                print(
                    "🧹 Webhook safety cleanup done."
                )

            except Exception as webhook_error:

                print(
                    f"⚠️ Webhook cleanup error: "
                    f"{webhook_error}"
                )


            # -------------------------------------------------
            # CHECK PYROGRAM
            # -------------------------------------------------

            if PYRO_FAILED.is_set():

                send_admin_debug(
                    "🔴 *WARNING: PYROGRAM YA MUTU*\n\n"
                    "Telebot polling yana ci gaba.\n\n"
                    "Za mu iya sake gyara Pyrogram "
                    "separately idan ya cancanta.",
                    "PYROGRAM WARNING"
                )


            print(
                "🔄 Ana sake fara polling..."
            )