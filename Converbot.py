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
# 13. UPLOAD PROGRESS CALLBACK
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

        # Progress error kada ya kashe upload
        print(
            f"[UPLOAD PROGRESS ERROR] "
            f"stage={stage} "
            f"current={current} "
            f"total={total} "
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
            "🔌 Ana kiran `pyro_bot.start()`...",
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
            f"❌ User `{user_id}` ba ADMIN_ID ba.",
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


    if user_id != ADMIN_ID:
        return


    if not USER_STATES.get(
        user_id,
        False
    ):
        return


    USER_STATES[user_id] = False


    file_name = "Video/File"
    file_size = 0
    file_id = None
    original_type = message.content_type


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
            "📦 *FILE AN KARƁA*\n\n"
            f"Type: `{original_type}`\n"
            f"Name: `{file_name}`\n"
            f"Size: `{humanbytes(file_size)}`\n"
            f"File ID: `{file_id}`\n"
            f"Message ID: `{message.message_id}`\n"
            f"Chat ID: `{message.chat.id}`",
            "FILE RECEIVED"
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

            "original_type": original_type,

            "file_id": file_id,

            "created_at": time.time()
        }


        send_admin_debug(
            "🟢 *PENDING DATA AN AJIYE*\n\n"
            f"Status Message ID: `{sent.message_id}`\n"
            f"Original Message ID: `{message.message_id}`\n"
            f"Chat ID: `{message.chat.id}`\n"
            f"Original Type: `{original_type}`\n"
            f"File: `{file_name}`\n"
            f"Size: `{humanbytes(file_size)}`\n\n"
            "Ana jiran zabin Video ko File.",
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
            f"Status Message ID: `{call.message.message_id}`",
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


        # =====================================================
        # MUKE RIKE DA ZABIN DA ADMIN YA YI TUN FARKO
        # =====================================================

        as_video = (
            call.data == "convert_video"
        )

        selected_mode = (
            "VIDEO"
            if as_video
            else "FILE"
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

        original_type = task_info.get(
            "original_type",
            "unknown"
        )

        file_size = task_info.get(
            "file_size",
            0
        )


        send_admin_debug(
            "🎯 *ZABI YA KAMMALA*\n\n"
            f"Selected Mode: `{selected_mode}`\n"
            f"Original Telegram Type: `{original_type}`\n"
            f"File Name: `{file_name}`\n"
            f"File Size: `{humanbytes(file_size)}`\n"
            f"Chat ID: `{chat_id}`\n"
            f"Original Message ID: `{target_msg_id}`\n"
            f"Status Message ID: `{msg_id}`\n\n"
            "Wannan zabin ne za a yi amfani da shi har zuwa send.",
            "MODE LOCKED"
        )


        if not PYRO_READY.is_set():

            bot.edit_message_text(
                "❌ *Pyrogram bai shirya ba.*\n\n"
                "Duba DEBUG na admin.",
                chat_id,
                msg_id
            )

            send_admin_debug(
                "🔴 *PYROGRAM NOT READY*\n\n"
                f"Ready: `{PYRO_READY.is_set()}`\n"
                f"Failed: `{PYRO_FAILED.is_set()}`\n"
                f"Loop Running: `{pyro_loop.is_running()}`",
                "PYRO NOT READY"
            )

            return


        if not pyro_loop.is_running():

            raise Exception(
                "Pyrogram asyncio loop baya running."
            )


        bot.edit_message_text(
            "🔄 *Ana fara aikin...*\n\n"
            f"📁 `{file_name}`\n"
            f"🎯 Tsari: `{selected_mode}`",
            chat_id,
            msg_id
        )


        send_admin_debug(
            "🚀 *TASK ZA A TURAWA PYROGRAM*\n\n"
            f"Mode: `{selected_mode}`\n"
            f"File: `{file_name}`\n"
            f"Size: `{humanbytes(file_size)}`\n"
            f"Target Message: `{target_msg_id}`\n"
            f"Status Message: `{msg_id}`\n"
            f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
            f"Loop Running: `{pyro_loop.is_running()}`",
            "TASK QUEUE START"
        )


        future = asyncio.run_coroutine_threadsafe(
            run_pyrogram_task(
                chat_id,
                target_msg_id,
                msg_id,
                as_video,
                file_name,
                original_type
            ),
            pyro_loop
        )


        send_admin_debug(
            "🟢 *TASK AN SHIGA PYROGRAM LOOP*\n\n"
            f"Future Done: `{future.done()}`\n"
            f"Future Cancelled: `{future.cancelled()}`\n"
            f"Loop Running: `{pyro_loop.is_running()}`\n"
            f"Mode: `{selected_mode}`",
            "TASK QUEUED"
        )


        # Wannan callback din zai gano exception da zai iya faruwa
        def future_done_callback(done_future):

            try:

                exception = done_future.exception()

                if exception:

                    send_admin_exception(
                        "🚨 FUTURE TASK YA KOMA DA EXCEPTION",
                        exception
                    )

                else:

                    send_admin_debug(
                        "🟢 FUTURE TASK YA KAMMALA BA TARE DA EXCEPTION BA.",
                        "FUTURE COMPLETE"
                    )

            except Exception as callback_error:

                send_admin_exception(
                    "FUTURE CALLBACK ERROR",
                    callback_error
                )


        future.add_done_callback(
            future_done_callback
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
    as_video,
    selected_file_name=None,
    original_type=None
):

    task_started = time.time()

    file_path = ""


    selected_mode = (
        "VIDEO"
        if as_video
        else "DOCUMENT"
    )


    send_admin_debug(
        "🚀🚀🚀 *UPLOAD TASK YA FARA* 🚀🚀🚀\n\n"
        f"Mode: `{selected_mode}`\n"
        f"Original Telegram Type: `{original_type}`\n"
        f"File Name: `{selected_file_name}`\n"
        f"Chat ID: `{chat_id}`\n"
        f"Target Message: `{target_msg_id}`\n"
        f"Status Message: `{status_msg_id}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Loop Running: `{pyro_loop.is_running()}`",
        "UPLOAD TASK START"
    )


    try:

        # =====================================================
        # STEP 1 — CONNECTION
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 1/10*\n\n"
            "Ana tabbatar Pyrogram yana aiki kafin upload...",
            "UPLOAD CHECK 1"
        )


        me = await pyro_bot.get_me()


        send_admin_debug(
            "🟢 *TELEGRAM CONNECTION VERIFIED*\n\n"
            f"Bot ID: `{me.id}`\n"
            f"Username: `@{me.username}`\n"
            f"Name: `{me.first_name}`",
            "UPLOAD CHECK 1 OK"
        )


        # =====================================================
        # STEP 2 — STATUS MESSAGE
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 2/10*\n\n"
            "Ana neman status message...",
            "UPLOAD CHECK 2"
        )


        status_msg = await pyro_bot.get_messages(
            chat_id,
            status_msg_id
        )


        if not status_msg:

            raise Exception(
                "Ba a samu status message ba."
            )


        send_admin_debug(
            "🟢 *STATUS MESSAGE OK*\n\n"
            f"Status Message ID: `{status_msg.id}`",
            "UPLOAD CHECK 2 OK"
        )


        await edit_status(
            chat_id,
            status_msg_id,
            "🔄 *Ana fara upload...*"
        )


        # =====================================================
        # STEP 3 — ORIGINAL MESSAGE
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 3/10*\n\n"
            "Ana neman original media message...",
            "UPLOAD CHECK 3"
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
            "🟢 *ORIGINAL MESSAGE OK*\n\n"
            f"Message ID: `{msg.id}`\n"
            f"Video: `{bool(msg.video)}`\n"
            f"Document: `{bool(msg.document)}`\n"
            f"Animation: `{bool(msg.animation)}`",
            "UPLOAD CHECK 3 OK"
        )


        # =====================================================
        # STEP 4 — LOCAL FILE
        # =====================================================
        #
        # Ba mu sake yin download debug ba.
        # An dauka download ya riga ya kammala.
        #
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 4/10*\n\n"
            "Ana duba local file da za a tura...",
            "UPLOAD CHECK 4"
        )


        media_type = "UNKNOWN"
        media_size = 0
        media_file_name = (
            selected_file_name
            or "unknown"
        )


        if msg.video:

            media_type = "VIDEO"

            media_size = (
                msg.video.file_size or 0
            )

            media_file_name = (
                msg.video.file_name
                or media_file_name
                or "video.mp4"
            )


        elif msg.document:

            media_type = "DOCUMENT"

            media_size = (
                msg.document.file_size or 0
            )

            media_file_name = (
                msg.document.file_name
                or media_file_name
                or "file"
            )


        elif msg.animation:

            media_type = "ANIMATION"

            media_size = (
                msg.animation.file_size or 0
            )

            media_file_name = (
                msg.animation.file_name
                or media_file_name
                or "animation"
            )


        else:

            raise Exception(
                "Original message ba ya dauke da "
                "video/document/animation."
            )


        # Idan akwai path daga download
        # a cikin aikin nan, a sake gano shi ta hanyar
        # download_media ba tare da aika download debug ba.
        #
        # Wannan yana tabbatar da cewa send stage yana da
        # real local file.

        file_path = await pyro_bot.download_media(
            message=msg
        )


        if not file_path:

            raise Exception(
                "Pyrogram ya kasa samar da local file "
                "don upload."
            )


        if not os.path.exists(file_path):

            raise Exception(
                f"Local file bai wanzu ba:\n{file_path}"
            )


        local_size = os.path.getsize(
            file_path
        )


        if local_size <= 0:

            raise Exception(
                "Local file yana da size 0 bytes."
            )


        if not os.access(
            file_path,
            os.R_OK
        ):

            raise Exception(
                "Local file ba readable ba ne."
            )


        disk = shutil.disk_usage(
            os.path.dirname(file_path)
            or "/"
        )


        send_admin_debug(
            "🟢 *LOCAL FILE VERIFIED*\n\n"
            f"Path: `{file_path}`\n"
            f"Telegram Type: `{media_type}`\n"
            f"Selected Mode: `{selected_mode}`\n"
            f"Name: `{media_file_name}`\n"
            f"Size: `{humanbytes(local_size)}`\n"
            f"Readable: `{os.access(file_path, os.R_OK)}`\n"
            f"Disk Free: `{humanbytes(disk.free)}`",
            "UPLOAD CHECK 4 OK"
        )


        # =====================================================
        # STEP 5 — MODE VERIFICATION
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 5/10*\n\n"
            "Ana tabbatar da cewa zabin admin "
            "shi ne zai yi tasiri...",
            "UPLOAD CHECK 5"
        )


        if as_video:

            upload_method = "send_video"
            destination_type = "VIDEO"

        else:

            upload_method = "send_document"
            destination_type = "DOCUMENT"


        send_admin_debug(
            "🟢 *UPLOAD MODE LOCKED*\n\n"
            f"Admin Selected: `{selected_mode}`\n"
            f"Original Telegram Type: `{media_type}`\n"
            f"Pyrogram Method: `{upload_method}`\n"
            f"Destination Type: `{destination_type}`\n\n"
            "Ba za a canza zabin ba yayin upload.",
            "UPLOAD CHECK 5 OK"
        )


        await edit_status(
            chat_id,
            status_msg_id,
            f"⬆️ *Ana Turawa...*\n\n"
            f"📁 `{media_file_name}`\n"
            f"🎯 `{selected_mode}`"
        )


        # =====================================================
        # STEP 6 — TELEGRAM UPLOAD PRE-CHECK
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 6/10*\n\n"
            "Ana shirin kiran Telegram send method.\n\n"
            f"Destination Chat: `{chat_id}`\n"
            f"File Path: `{file_path}`\n"
            f"File Size: `{humanbytes(local_size)}`\n"
            f"Method: `{upload_method}`\n"
            f"Mode: `{selected_mode}`",
            "TELEGRAM SEND PRECHECK"
        )


        upload_start = time.time()


        # =====================================================
        # STEP 7 — ACTUAL SEND
        # =====================================================

        send_admin_debug(
            "🚨 *UPLOAD DEBUG 7/10 — SEND YANZU*\n\n"
            f"Telegram Method: `{upload_method}`\n"
            f"Chat ID: `{chat_id}`\n"
            f"File: `{file_path}`\n"
            f"Size: `{humanbytes(local_size)}`\n\n"
            "⚠️ Daga nan zuwa gaba Telegram/Pyrogram "
            "ne zai amsa.\n"
            "Idan ya tsaya a nan, matsalar tana "
            "bangaren send/upload.",
            "TELEGRAM SEND START"
        )


        if as_video:

            send_admin_debug(
                "🎬 *ANA KIRAN send_video()*\n\n"
                f"chat_id=`{chat_id}`\n"
                f"video=`{file_path}`\n"
                f"size=`{humanbytes(local_size)}`\n"
                "supports_streaming=`True`",
                "SEND VIDEO CALL"
            )


            result = await pyro_bot.send_video(
                chat_id=chat_id,
                video=file_path,
                caption=(
                    "🎬 An kammala sarrafa "
                    "bidiyon ku lafiya!"
                ),
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
                "📁 *ANA KIRAN send_document()*\n\n"
                f"chat_id=`{chat_id}`\n"
                f"document=`{file_path}`\n"
                f"size=`{humanbytes(local_size)}`",
                "SEND DOCUMENT CALL"
            )


            result = await pyro_bot.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=(
                    "📁 An kammala sarrafa "
                    "fayil ɗin ku lafiya!"
                ),
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
        # STEP 8 — TELEGRAM RESPONSE
        # =====================================================

        upload_time = (
            time.time() - upload_start
        )


        send_admin_debug(
            "🟢 *UPLOAD DEBUG 8/10 — TELEGRAM YA AMSA*\n\n"
            f"Result Exists: `{bool(result)}`\n"
            f"Result Type: `{type(result).__name__}`\n"
            f"Upload Time: `{upload_time:.2f}s`\n"
            f"Mode: `{selected_mode}`",
            "TELEGRAM SEND RETURN"
        )


        if not result:

            raise Exception(
                "Telegram/Pyrogram send method ya dawo "
                "da empty result."
            )


        # =====================================================
        # STEP 9 — DELIVERY VERIFICATION
        # =====================================================

        send_admin_debug(
            "🔎 *UPLOAD DEBUG 9/10*\n\n"
            "Ana duba sakamakon Telegram domin tabbatar "
            "an samu Message object...",
            "DELIVERY VERIFY"
        )


        sent_message_id = getattr(
            result,
            "id",
            None
        )


        sent_chat_id = getattr(
            getattr(result, "chat", None),
            "id",
            None
        )


        if not sent_message_id:

            raise Exception(
                "Telegram ya dawo result amma babu "
                "Message ID."
            )


        send_admin_debug(
            "🟢🟢🟢 *TELEGRAM DELIVERY VERIFIED* 🟢🟢🟢\n\n"
            f"Sent Message ID: `{sent_message_id}`\n"
            f"Sent Chat ID: `{sent_chat_id}`\n"
            f"Expected Chat ID: `{chat_id}`\n"
            f"Mode: `{selected_mode}`\n"
            f"Upload Time: `{upload_time:.2f}s`\n"
            f"Size: `{humanbytes(local_size)}`",
            "DELIVERY VERIFIED"
        )


        await edit_status(
            chat_id,
            status_msg_id,
            "🎉 *An gama aikin lafiya!*\n\n"
            "🟢 Upload: OK\n"
            "🟢 Telegram: OK\n"
            "🟢 Delivery: OK"
        )


        # =====================================================
        # STEP 10 — FINAL SUCCESS
        # =====================================================

        total_time = (
            time.time() - task_started
        )


        send_admin_debug(
            "🏆🏆🏆 *TASK SUCCESS* 🏆🏆🏆\n\n"
            f"File: `{media_file_name}`\n"
            f"Size: `{humanbytes(local_size)}`\n"
            f"Original Type: `{media_type}`\n"
            f"Selected Mode: `{selected_mode}`\n"
            f"Send Method: `{upload_method}`\n"
            f"Telegram Message ID: `{sent_message_id}`\n"
            f"Upload Time: `{upload_time:.2f}s`\n"
            f"Total Time: `{total_time:.2f}s`\n\n"
            "🟢 PYROGRAM OK\n"
            "🟢 LOCAL FILE OK\n"
            "🟢 SEND METHOD OK\n"
            "🟢 TELEGRAM RESPONSE OK\n"
            "🟢 DELIVERY VERIFIED",
            "TASK SUCCESS"
        )


    except FloodWait as e:

        send_admin_debug(
            "⚠️⚠️⚠️ *TELEGRAM FLOOD WAIT*\n\n"
            f"Seconds: `{e.value}`\n"
            f"Mode: `{selected_mode}`\n"
            f"File: `{selected_file_name}`\n\n"
            "Telegram ne ya dakatar da request saboda limit.",
            "TELEGRAM FLOOD WAIT"
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

        # =====================================================
        # FINAL CRASH DEBUG
        # =====================================================

        send_admin_exception(
            (
                "💥💥💥 UPLOAD/SEND TASK YA CRASH\n"
                f"MODE={selected_mode}\n"
                f"FILE={selected_file_name}\n"
                f"ORIGINAL_TYPE={original_type}"
            ),
            e
        )


        try:

            await edit_status(
                chat_id,
                status_msg_id,
                "❌ *Aiki ya samu kuskure.*\n\n"
                "Duba DEBUG na admin domin ganin "
                "ainihin inda ya tsaya."
            )

        except Exception as status_error:

            send_admin_exception(
                "STATUS UPDATE AFTER UPLOAD ERROR",
                status_error
            )


    finally:

        # =====================================================
        # CLEANUP
        # =====================================================

        send_admin_debug(
            "🧹 *FINAL CLEANUP YA FARA*\n\n"
            f"File Path: `{file_path}`\n"
            f"Exists: `{os.path.exists(file_path) if file_path else False}`\n"
            f"Mode: `{selected_mode}`",
            "FINAL CLEANUP"
        )


        if file_path and os.path.exists(file_path):

            try:

                os.remove(
                    file_path
                )


                send_admin_debug(
                    "🟢 *TEMPORARY FILE AN GOGE*\n\n"
                    f"Path: `{file_path}`",
                    "CLEANUP SUCCESS"
                )


            except Exception as e:

                send_admin_exception(
                    "CLEANUP ERROR",
                    e
                )

        else:

            send_admin_debug(
                "ℹ️ Babu local file da za a goge.",
                "CLEANUP SKIPPED"
            )


# =============================================================
# 22. RENDER HTTP SERVER
# =============================================================

class HealthHandler(
    BaseHTTPRequestHandler
):

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

            self.wfile.write(
                b'{"status":"ok"}'
            )

            return


        self.send_response(404)
        self.end_headers()


    def log_message(
        self,
        format,
        *args
    ):

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