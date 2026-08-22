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


# =============================================================
# 2. TELEGRAM CLIENTS
# =============================================================

print("\n" + "=" * 70)
print("🚀 SABON CONVERTER BOT - STARTING")
print("=" * 70)

print(f"ADMIN_ID: {ADMIN_ID}")
print(f"API_ID: {API_ID}")
print(f"API_HASH: {'SET' if API_HASH else 'NOT SET'}")
print(f"BOT_TOKEN: {'SET' if BOT_TOKEN else 'NOT SET'}")
print(f"Python: {sys.version}")
print(f"Platform: {platform.platform()}")
print("=" * 70)


bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="Markdown"
)


# =============================================================
# 3. PYROGRAM CLIENT
# =============================================================

pyro_bot = Client(
    "pyro_converter_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)


# =============================================================
# 4. ASYNCIO LOOP
# =============================================================

pyro_loop = asyncio.new_event_loop()

PYRO_READY = threading.Event()
PYRO_FAILED = threading.Event()
BOT_READY = threading.Event()

USER_STATES = {}
PENDING_DATA = {}

PROGRESS_STATE = {}


# =============================================================
# 5. DEBUG SYSTEM
# =============================================================

def now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def send_admin_debug(text, level="DEBUG"):
    """
    Tura cikakken debug zuwa ADMIN_ID.
    Kuma ya print a Render logs.
    """

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
        print(f"[DEBUG SEND ERROR] {e}")


def send_admin_exception(title, exc):
    """
    Tura cikakken exception + traceback zuwa ADMIN_ID.
    """

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
        print(f"[EXCEPTION DEBUG SEND ERROR] {send_error}")


# =============================================================
# 6. SYSTEM INFORMATION
# =============================================================

def system_info():

    try:
        disk = shutil.disk_usage("/")

        total_disk = disk.total / (1024 ** 3)
        used_disk = disk.used / (1024 ** 3)
        free_disk = disk.free / (1024 ** 3)

        disk_text = (
            f"💾 *DISK*\n"
            f"Total: `{total_disk:.2f} GiB`\n"
            f"Used: `{used_disk:.2f} GiB`\n"
            f"Free: `{free_disk:.2f} GiB`\n"
        )

    except Exception as e:
        disk_text = f"💾 Disk check failed: `{e}`"

    try:
        import psutil

        ram = psutil.virtual_memory()

        ram_total = ram.total / (1024 ** 3)
        ram_available = ram.available / (1024 ** 3)

        ram_text = (
            f"🧠 *RAM*\n"
            f"Total: `{ram_total:.2f} GiB`\n"
            f"Available: `{ram_available:.2f} GiB`\n"
        )

        cpu_text = (
            f"⚙️ *CPU*\n"
            f"Usage: `{psutil.cpu_percent(interval=0.5)}%`\n"
        )

    except Exception as e:
        ram_text = f"🧠 RAM check failed: `{e}`"
        cpu_text = ""

    return (
        f"🖥️ *SYSTEM INFORMATION*\n\n"
        f"Python: `{platform.python_version()}`\n"
        f"Hostname: `{socket.gethostname()}`\n"
        f"PID: `{os.getpid()}`\n\n"
        f"{disk_text}\n"
        f"{ram_text}\n"
        f"{cpu_text}"
    )


# =============================================================
# 7. HELPER FUNCTIONS
# =============================================================

def humanbytes(size):

    if not size:
        return "0 B"

    power = 2 ** 10
    n = 0

    dic_power_ten = {
        0: "B",
        1: "KiB",
        2: "MiB",
        3: "GiB",
        4: "TiB"
    }

    while size >= power and n < 4:
        size /= power
        n += 1

    return f"{round(size, 2)} {dic_power_ten[n]}"


def TimeFormatter(milliseconds: int) -> str:

    seconds, milliseconds = divmod(
        int(milliseconds),
        1000
    )

    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    tmp = (
        (f"{days}d, " if days else "") +
        (f"{hours}h, " if hours else "") +
        (f"{minutes}m, " if minutes else "") +
        (f"{seconds}s, " if seconds else "")
    )

    return tmp[:-2] if tmp else "0s"


# =============================================================
# 8. STATUS MESSAGE EDITOR
# =============================================================

async def edit_status(chat_id, message_id, text):

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
# 9. PROGRESS CALLBACK
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

        eta_str = TimeFormatter(
            remaining * 1000
        )

        blocks = math.floor(
            percentage / 10
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
            f"`{humanbytes(current)} / {humanbytes(total)}`\n"
            f"⚡ *Speed:* `{humanbytes(speed)}/s`\n"
            f"⏳ *Lokacin da ya rage:* `{eta_str}`\n"
        )

        # Kada mu yi edit da yawa a lokaci guda
        last_time = PROGRESS_STATE.get(
            (chat_id, message_id, stage),
            0
        )

        if (
            now - last_time >= 3
            or current >= total
        ):

            PROGRESS_STATE[
                (chat_id, message_id, stage)
            ] = now

            await edit_status(
                chat_id,
                message_id,
                text
            )

    except Exception as e:

        print(
            f"[PROGRESS CALLBACK ERROR] "
            f"stage={stage} "
            f"error={e}"
        )


# =============================================================
# 10. PYROGRAM MAIN LOOP
# =============================================================

async def pyro_main():

    send_admin_debug(
        "🚀 *PYROGRAM THREAD YA FARA*\n\n"
        "Ana kokarin kunna Pyrogram Client...\n\n"
        f"Thread: `{threading.current_thread().name}`\n"
        f"Loop: `{id(asyncio.get_running_loop())}`",
        "PYROGRAM START"
    )

    try:

        send_admin_debug(
            "🔌 Ana kiran `pyro_bot.start()` yanzu...\n\n"
            "Idan ya tsaya a nan, matsalar Pyrogram/API "
            "ce kafin conversion ya fara.",
            "PYROGRAM CONNECT"
        )

        result = pyro_bot.start()

        # Wannan yana kare mu daga versions
        # da start() yake awaitable ko synchronous.
        if inspect.isawaitable(result):

            send_admin_debug(
                "⏳ `pyro_bot.start()` ya dawo awaitable.\n"
                "Ana `await` dinsa yanzu...",
                "PYROGRAM ASYNC START"
            )

            await result

        else:

            send_admin_debug(
                "ℹ️ `pyro_bot.start()` ya dawo ba tare da "
                "awaitable ba.\n"
                f"Return type: `{type(result).__name__}`",
                "PYROGRAM START RETURN"
            )

        PYRO_READY.set()

        send_admin_debug(
            "🟢🟢🟢 *PYROGRAM YA TASHI LAFIYA* 🟢🟢🟢\n\n"
            f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
            f"Thread: `{threading.current_thread().name}`\n"
            f"Loop: `{id(asyncio.get_running_loop())}`\n\n"
            "Yanzu Pyrogram a shirye yake karbar "
            "download/upload task.",
            "PYROGRAM READY"
        )

        # Rayar da loop har abada
        await asyncio.Event().wait()

    except Exception as e:

        PYRO_FAILED.set()

        send_admin_exception(
            "💥 PYROGRAM THREAD YA KARE / CRASH",
            e
        )

    finally:

        if PYRO_READY.is_set():

            try:

                result = pyro_bot.stop()

                if inspect.isawaitable(result):
                    await result

                print(
                    "🛑 Pyrogram Client an dakatar."
                )

            except Exception as e:

                print(
                    f"[PYRO STOP ERROR] {e}"
                )


def start_pyro_loop():

    try:

        asyncio.set_event_loop(
            pyro_loop
        )

        send_admin_debug(
            "🔄 Ana fara Pyrogram asyncio loop...\n\n"
            f"Thread: `{threading.current_thread().name}`\n"
            f"PID: `{os.getpid()}`",
            "ASYNCIO LOOP"
        )

        pyro_loop.run_until_complete(
            pyro_main()
        )

    except Exception as e:

        PYRO_FAILED.set()

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
# 11. START PYROGRAM THREAD
# =============================================================

send_admin_debug(
    "🧵 Ana kirkirar Pyrogram background thread...\n\n"
    "Ba webhook ake amfani da shi ba.\n"
    "Pyrogram zai yi aiki a nasa asyncio loop.",
    "THREAD CREATE"
)

pyro_thread = threading.Thread(
    target=start_pyro_loop,
    name="PyrogramThread",
    daemon=True
)

pyro_thread.start()


# =============================================================
# 12. STARTUP DEBUG
# =============================================================

def startup_debug():

    time.sleep(2)

    send_admin_debug(
        "🟢🟢🟢 *BOT PROCESS YA FARA* 🟢🟢🟢\n\n"
        f"PID: `{os.getpid()}`\n"
        f"Python PID: `{os.getpid()}`\n"
        f"Pyrogram Thread: `{pyro_thread.name}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Pyro Failed: `{PYRO_FAILED.is_set()}`\n\n"
        + system_info(),
        "BOT START"
    )


threading.Thread(
    target=startup_debug,
    daemon=True
).start()


# =============================================================
# 13. /start
# =============================================================

@bot.message_handler(commands=["start"])
def start_handler(message):

    user_id = message.from_user.id

    print(
        f"[START] User={user_id}"
    )

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
        f"Username: `@{message.from_user.username}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`",
        "START RECEIVED"
    )


# =============================================================
# 14. /video
# =============================================================

@bot.message_handler(commands=["video"])
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
            f"❌ An hana user `{user_id}` saboda "
            "ba ADMIN_ID ba ne.",
            "ACCESS DENIED"
        )

        return

    USER_STATES[user_id] = True

    send_admin_debug(
        "🟢 USER STATE AN KUNNA.\n\n"
        f"USER_STATES[{user_id}] = `True`\n\n"
        "Yanzu bot yana jiran Video ko Document.",
        "WAITING FOR FILE"
    )

    bot.reply_to(
        message,
        "✅ *An kunna tsarin karɓar aiki!*\n\n"
        "Yanzu aiko min da *Video* ko *File* "
        "din da kake son sarrafawa."
    )


# =============================================================
# 15. RECEIVE VIDEO / DOCUMENT
# =============================================================

@bot.message_handler(
    content_types=["video", "document"]
)
def handle_incoming_file(message):

    user_id = message.from_user.id

    send_admin_debug(
        "📦 *FILE UPDATE YA ISO*\n\n"
        f"User ID: `{user_id}`\n"
        f"Chat ID: `{message.chat.id}`\n"
        f"Message ID: `{message.message_id}`\n"
        f"Content type: `{message.content_type}`\n"
        f"State: `{USER_STATES.get(user_id, False)}`",
        "FILE RECEIVED"
    )

    if user_id != ADMIN_ID:

        send_admin_debug(
            f"❌ File daga `{user_id}` an yi watsi da shi.\n"
            "Ba ADMIN_ID ba ne.",
            "FILE REJECTED"
        )

        return

    if not USER_STATES.get(
        user_id,
        False
    ):

        send_admin_debug(
            "ℹ️ An karbi file amma `/video` state "
            "bai kunna ba.\n\n"
            "An yi watsi da file din.",
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
            "✅ *FILE DETAILS AN SAMU*\n\n"
            f"Name: `{file_name}`\n"
            f"Size: `{humanbytes(file_size)}`\n"
            f"Telegram File ID: `{file_id}`\n"
            f"Message ID: `{message.message_id}`",
            "FILE DETAILS"
        )

    except Exception as e:

        send_admin_exception(
            "❌ ERROR LOKACIN KARANTA FILE DETAILS",
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
            f"File: `{file_name}`\n\n"
            "Yanzu ana jiran callback:",
            "PENDING DATA"
        )

    except Exception as e:

        send_admin_exception(
            "❌ ERROR LOKACIN AMSA FILE",
            e
        )


# =============================================================
# 16. CALLBACK
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

            send_admin_debug(
                f"❌ Callback daga `{call.from_user.id}` "
                "an hana.",
                "CALLBACK DENIED"
            )

            return

        msg_id = call.message.message_id

        send_admin_debug(
            "🔎 Ana duba PENDING_DATA...\n\n"
            f"Lookup key: `{msg_id}`\n"
            f"Exists: `{msg_id in PENDING_DATA}`\n"
            f"Pending count: `{len(PENDING_DATA)}`",
            "PENDING LOOKUP"
        )

        if msg_id not in PENDING_DATA:

            bot.edit_message_text(
                "❌ *Aikin ya fita daga tsarin lokaci.*\n\n"
                "Sake fara sabo da `/video`.",
                call.message.chat.id,
                msg_id
            )

            send_admin_debug(
                "❌ PENDING DATA BA A SAMU BA.\n\n"
                "An daina task din saboda babu bayanan "
                "fayil.",
                "PENDING DATA MISSING"
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
            "🟢 *TASK AN KARƁA*\n\n"
            f"Mode: `{'VIDEO' if as_video else 'FILE'}`\n"
            f"Chat ID: `{chat_id}`\n"
            f"Target Message ID: `{target_msg_id}`\n"
            f"Status Message ID: `{msg_id}`\n"
            f"File: `{file_name}`\n\n"
            f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
            f"Pyro Failed: `{PYRO_FAILED.is_set()}`",
            "TASK ACCEPTED"
        )

        if not PYRO_READY.is_set():

            send_admin_debug(
                "🔴 *PYROGRAM BAI READY BA!*\n\n"
                "Ba za a tura task zuwa Pyrogram ba.\n\n"
                f"PYRO_READY: `{PYRO_READY.is_set()}`\n"
                f"PYRO_FAILED: `{PYRO_FAILED.is_set()}`",
                "PYRO NOT READY"
            )

            bot.edit_message_text(
                "❌ *Pyrogram bai shirya ba.*\n\n"
                "Duba DEBUG na admin.",
                chat_id,
                msg_id
            )

            return

        bot.edit_message_text(
            "🔄 *Shirin gudanar da aiki...*\n\n"
            f"Chat ID: `{chat_id}`\n"
            f"File: `{file_name}`",
            chat_id,
            msg_id
        )

        send_admin_debug(
            "🚀 *ANA TURA TASK ZUWA PYROGRAM LOOP*\n\n"
            f"Loop ID: `{id(pyro_loop)}`\n"
            f"Loop running: `{pyro_loop.is_running()}`\n"
            f"Chat ID: `{chat_id}`\n"
            f"Target Message ID: `{target_msg_id}`\n"
            f"Status Message ID: `{msg_id}`\n"
            f"As Video: `{as_video}`",
            "TASK SUBMIT"
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
            "🟢 *TASK AN SHIGA PYROGRAM LOOP*\n\n"
            f"Future: `{future}`\n"
            f"Future Done: `{future.done()}`\n"
            f"Future Cancelled: `{future.cancelled()}`",
            "TASK QUEUED"
        )

    except Exception as e:

        send_admin_exception(
            "💥 CALLBACK PROCESS YA CRASH",
            e
        )


# =============================================================
# 17. PYROGRAM TASK
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
        "🚀🚀🚀 *PYROGRAM TASK YA FARA* 🚀🚀🚀\n\n"
        f"Chat ID: `{chat_id}`\n"
        f"Target Message ID: `{target_msg_id}`\n"
        f"Status Message ID: `{status_msg_id}`\n"
        f"As Video: `{as_video}`\n"
        f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
        f"Loop: `{id(asyncio.get_running_loop())}`",
        "TASK START"
    )

    try:

        # -----------------------------------------------------
        # STEP 0 - PYROGRAM CONNECTION CHECK
        # -----------------------------------------------------

        send_admin_debug(
            "🔎 STEP 0\n\n"
            "Ana tabbatar Pyrogram Client yana aiki...",
            "PYRO CHECK"
        )

        try:

            me = await pyro_bot.get_me()

            send_admin_debug(
                "🟢 *PYROGRAM GET_ME SUCCESS*\n\n"
                f"Bot ID: `{me.id}`\n"
                f"Username: `@{me.username}`\n"
                f"Name: `{me.first_name}`",
                "PYRO CONNECTION OK"
            )

        except Exception as e:

            send_admin_exception(
                "❌ PYROGRAM GET_ME YA KASA",
                e
            )

            raise


        # -----------------------------------------------------
        # STEP 1 - GET STATUS MESSAGE
        # -----------------------------------------------------

        send_admin_debug(
            "🔎 STEP 1\n\n"
            "Ana samun status message daga Telegram...\n\n"
            f"Chat ID: `{chat_id}`\n"
            f"Status Message ID: `{status_msg_id}`",
            "GET STATUS MESSAGE"
        )

        status_msg = await pyro_bot.get_messages(
            chat_id,
            status_msg_id
        )

        if not status_msg:

            raise Exception(
                "Pyrogram bai iya samun status message ba."
            )

        send_admin_debug(
            "🟢 STATUS MESSAGE AN SAMU.\n\n"
            f"ID: `{status_msg.id}`\n"
            f"Chat: `{status_msg.chat.id if status_msg.chat else 'NONE'}`",
            "STATUS MESSAGE OK"
        )


        # -----------------------------------------------------
        # STEP 2 - GET TARGET MESSAGE
        # -----------------------------------------------------

        await edit_status(
            chat_id,
            status_msg_id,
            "🔄 *Ana shirya fayil...*"
        )

        send_admin_debug(
            "🔎 STEP 2\n\n"
            "Ana neman original file/message "
            "a Telegram...\n\n"
            f"Chat ID: `{chat_id}`\n"
            f"Target Message ID: `{target_msg_id}`",
            "GET TARGET MESSAGE"
        )

        msg = await pyro_bot.get_messages(
            chat_id,
            target_msg_id
        )

        if not msg:

            raise Exception(
                "Pyrogram bai samu original message ba."
            )

        send_admin_debug(
            "🟢 *TARGET MESSAGE AN SAMU*\n\n"
            f"Message ID: `{msg.id}`\n"
            f"Date: `{msg.date}`\n"
            f"Has video: `{bool(msg.video)}`\n"
            f"Has document: `{bool(msg.document)}`\n"
            f"Has animation: `{bool(msg.animation)}`\n"
            f"Text: `{str(msg.text)[:300] if msg.text else 'NONE'}`",
            "TARGET MESSAGE OK"
        )


        # -----------------------------------------------------
        # STEP 3 - IDENTIFY MEDIA
        # -----------------------------------------------------

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
                "Original message ba ya dauke da "
                "Video/Document/Animation media."
            )

        send_admin_debug(
            "🎬 *MEDIA IDENTIFIED*\n\n"
            f"Type: `{media_type}`\n"
            f"File name: `{media_file_name}`\n"
            f"Telegram size: `{humanbytes(media_size)}`\n"
            f"Raw size: `{media_size}` bytes",
            "MEDIA CHECK"
        )


        # -----------------------------------------------------
        # STEP 4 - DOWNLOAD START
        # -----------------------------------------------------

        await edit_status(
            chat_id,
            status_msg_id,
            "⏬ *Ana Saukewa (Downloading)...*\n\n"
            "Ana fara karɓar fayil daga Telegram..."
        )

        download_start = time.time()

        send_admin_debug(
            "⬇️⬇️⬇️ *STEP 4 - DOWNLOAD START* ⬇️⬇️⬇️\n\n"
            f"File: `{media_file_name}`\n"
            f"Expected Size: `{humanbytes(media_size)}`\n"
            f"Start Time: `{now_time()}`\n\n"
            "Idan debug ya tsaya a nan, matsalar tana "
            "bangaren DOWNLOAD/Telegram/connection.",
            "DOWNLOAD START"
        )

        file_path = await pyro_bot.download_media(
            message=msg,
            progress=progress_args,
            progress_args=(
                "⏬ *Ana Saukewa (Downloading)...*",
                chat_id,
                status_msg_id,
                download_start,
                "DOWNLOAD"
            )
        )


        # -----------------------------------------------------
        # STEP 5 - DOWNLOAD RETURN
        # -----------------------------------------------------

        send_admin_debug(
            "📥 *DOWNLOAD FUNCTION YA KOMA*\n\n"
            f"Returned path: `{file_path}`\n"
            f"Return type: `{type(file_path).__name__}`",
            "DOWNLOAD RETURN"
        )

        if not file_path:

            raise Exception(
                "download_media() ya dawo da empty path."
            )

        if not os.path.exists(file_path):

            raise Exception(
                f"File path ya dawo amma file bai wanzu ba: "
                f"{file_path}"
            )

        send_admin_debug(
            "🟢 *DOWNLOAD FILE EXISTS*\n\n"
            f"Path: `{file_path}`\n"
            f"Exists: `{os.path.exists(file_path)}`\n"
            f"Readable: `{os.access(file_path, os.R_OK)}`",
            "DOWNLOAD FILE OK"
        )


        # -----------------------------------------------------
        # STEP 6 - DISK CHECK
        # -----------------------------------------------------

        try:

            disk = shutil.disk_usage(
                os.path.dirname(file_path)
                or "/"
            )

            send_admin_debug(
                "💾 *DISK CHECK BAYAN DOWNLOAD*\n\n"
                f"Free: `{humanbytes(disk.free)}`\n"
                f"Used: `{humanbytes(disk.used)}`\n"
                f"Total: `{humanbytes(disk.total)}`",
                "DISK CHECK"
            )

        except Exception as e:

            send_admin_debug(
                f"⚠️ Disk check ya kasa: `{e}`",
                "DISK WARNING"
            )


        # -----------------------------------------------------
        # STEP 7 - LOCAL FILE SIZE
        # -----------------------------------------------------

        file_size = os.path.getsize(
            file_path
        )

        file_size_readable = humanbytes(
            file_size
        )

        download_time = (
            time.time() - download_start
        )

        send_admin_debug(
            "✅✅ *DOWNLOAD YA KAMMALA* ✅✅\n\n"
            f"Local Path: `{file_path}`\n"
            f"Local Size: `{file_size_readable}`\n"
            f"Raw Size: `{file_size}` bytes\n"
            f"Download Time: `{download_time:.2f}s`\n\n"
            "🔥 YANZU MUN WUCE DOWNLOAD.\n"
            "Idan upload ya tsaya, matsalar tana "
            "daga nan gaba.",
            "DOWNLOAD COMPLETE"
        )

        await edit_status(
            chat_id,
            status_msg_id,
            f"✅ *Download Ya Kammala!*\n\n"
            f"📂 Girman Fayil: `{file_size_readable}`\n\n"
            f"⏳ Ana shirin tura shi zuwa Telegram..."
        )

        await asyncio.sleep(2)


        # -----------------------------------------------------
        # STEP 8 - UPLOAD START
        # -----------------------------------------------------

        upload_start = time.time()

        send_admin_debug(
            "⬆️⬆️⬆️ *STEP 8 - UPLOAD START* ⬆️⬆️⬆️\n\n"
            f"Path: `{file_path}`\n"
            f"Size: `{file_size_readable}`\n"
            f"Destination Chat: `{chat_id}`\n"
            f"Mode: `{'VIDEO' if as_video else 'DOCUMENT'}`\n"
            f"Start Time: `{now_time()}`\n\n"
            "🔥🔥🔥 YANZU AINIHIN UPLOAD YA FARA.\n"
            "Idan debug ya tsaya a nan, "
            "matsalar tana upload/Telegram/file/connection.",
            "UPLOAD START"
        )


        # -----------------------------------------------------
        # STEP 9 - SEND VIDEO
        # -----------------------------------------------------

        if as_video:

            send_admin_debug(
                "🎬 *ANA KIRAN send_video()*\n\n"
                f"File: `{file_path}`\n"
                f"Size: `{file_size_readable}`\n"
                f"Chat: `{chat_id}`",
                "SEND VIDEO CALL"
            )

            result = await pyro_bot.send_video(
                chat_id=chat_id,
                video=file_path,
                caption="🎬 *An kammala sarrafa bidiyon ku lafiya!*",
                supports_streaming=True,
                progress=progress_args,
                progress_args=(
                    "⬆️ *Ana Turawa (Uploading Video)...*",
                    chat_id,
                    status_msg_id,
                    upload_start,
                    "UPLOAD_VIDEO"
                )
            )

        # -----------------------------------------------------
        # STEP 10 - SEND DOCUMENT
        # -----------------------------------------------------

        else:

            send_admin_debug(
                "📁 *ANA KIRAN send_document()*\n\n"
                f"File: `{file_path}`\n"
                f"Size: `{file_size_readable}`\n"
                f"Chat: `{chat_id}`",
                "SEND DOCUMENT CALL"
            )

            result = await pyro_bot.send_document(
                chat_id=chat_id,
                document=file_path,
                caption="📁 *An kammala sarrafa fayil ɗin ku lafiya!*",
                progress=progress_args,
                progress_args=(
                    "⬆️ *Ana Turawa (Uploading File)...*",
                    chat_id,
                    status_msg_id,
                    upload_start,
                    "UPLOAD_DOCUMENT"
                )
            )


        # -----------------------------------------------------
        # STEP 11 - UPLOAD RETURN
        # -----------------------------------------------------

        upload_time = (
            time.time() - upload_start
        )

        send_admin_debug(
            "🎉🎉🎉 *UPLOAD FUNCTION YA KOMA* 🎉🎉🎉\n\n"
            f"Result: `{bool(result)}`\n"
            f"Result Type: `{type(result).__name__}`\n"
            f"Upload Time: `{upload_time:.2f}s`\n"
            f"Size: `{file_size_readable}`\n\n"
            "🔥🔥🔥 UPLOAD YA KAMMALA.",
            "UPLOAD COMPLETE"
        )

        await edit_status(
            chat_id,
            status_msg_id,
            "✅ *Download Ya Kammala!*\n\n"
            "🎬 *Upload Ya Kammala!*\n\n"
            "🎉 *An gama aikin sarrafa fayil lafiya!*"
        )


        # -----------------------------------------------------
        # STEP 12 - TOTAL TIME
        # -----------------------------------------------------

        total_time = (
            time.time() - task_started
        )

        send_admin_debug(
            "🏆🏆🏆 *TASK YA KAMMALA* 🏆🏆🏆\n\n"
            f"File: `{media_file_name}`\n"
            f"Size: `{file_size_readable}`\n"
            f"Download: `{download_time:.2f}s`\n"
            f"Upload: `{upload_time:.2f}s`\n"
            f"Total: `{total_time:.2f}s`\n\n"
            "🟢 DOWNLOAD: OK\n"
            "🟢 LOCAL FILE: OK\n"
            "🟢 UPLOAD: OK\n"
            "🟢 TELEGRAM DELIVERY: OK",
            "TASK SUCCESS"
        )


    except FloodWait as e:

        send_admin_debug(
            "⚠️⚠️⚠️ *TELEGRAM FLOOD WAIT* ⚠️⚠️⚠️\n\n"
            f"Seconds: `{e.value}`\n"
            f"File: `{file_path}`",
            "FLOOD WAIT"
        )

        try:

            await edit_status(
                chat_id,
                status_msg_id,
                f"⚠️ *Telegram Limit!*\n\n"
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
                "❌ *Kuskure A Aiki!*\n\n"
                "Duba DEBUG na admin domin ganin "
                "ainihin matakin da ya fadi."
            )

        except Exception as status_error:

            send_admin_debug(
                f"❌ Status edit ma ya kasa:\n"
                f"`{status_error}`",
                "STATUS ERROR"
            )


    finally:

        # -----------------------------------------------------
        # STEP 13 - CLEANUP
        # -----------------------------------------------------

        send_admin_debug(
            "🧹 *CLEANUP STEP*\n\n"
            f"File path: `{file_path}`\n"
            f"Exists before cleanup: "
            f"`{os.path.exists(file_path) if file_path else False}`",
            "CLEANUP START"
        )

        if file_path and os.path.exists(
            file_path
        ):

            try:

                os.remove(
                    file_path
                )

                send_admin_debug(
                    "🟢 *TEMP FILE AN GOGE*\n\n"
                    f"Path: `{file_path}`\n"
                    "Cleanup: SUCCESS",
                    "CLEANUP SUCCESS"
                )

            except Exception as e:

                send_admin_exception(
                    "❌ CLEANUP YA KASA",
                    e
                )

        else:

            send_admin_debug(
                "ℹ️ Babu file da za a goge.\n\n"
                f"Path: `{file_path}`",
                "CLEANUP SKIPPED"
            )


# =============================================================
# 18. TELEBOT STARTUP
# =============================================================

if __name__ == "__main__":

    try:

        send_admin_debug(
            "🚀 *MAIN PROCESS YA FARA*\n\n"
            "Ana shirin fara Telebot polling.\n\n"
            f"BOT TOKEN: `SET`\n"
            f"ADMIN ID: `{ADMIN_ID}`\n"
            f"API ID: `{API_ID}`\n"
            f"API HASH: `SET`\n\n"
            "⚠️ Wannan code din BA YA SET webhook.\n"
            "Polling kawai ake amfani da shi.",
            "MAIN START"
        )

        # Wannan ba setup webhook ba ne.
        # Safety ne kawai idan Telegram yana da tsohon
        # webhook a kan wannan token.
        try:

            bot.delete_webhook(
                drop_pending_updates=True
            )

            send_admin_debug(
                "🧹 *WEBHOOK SAFETY CHECK*\n\n"
                "An tabbatar babu webhook da zai hana "
                "polling aiki.\n\n"
                "Wannan bot ba ya saita webhook.",
                "WEBHOOK CHECK"
            )

        except Exception as e:

            send_admin_debug(
                f"⚠️ Webhook safety check ya kasa:\n"
                f"`{e}`",
                "WEBHOOK WARNING"
            )

        BOT_READY.set()

        send_admin_debug(
            "🟢🟢🟢 *TELEBOT READY* 🟢🟢🟢\n\n"
            "Ana fara `infinity_polling()` yanzu.\n\n"
            f"Pyro Ready: `{PYRO_READY.is_set()}`\n"
            f"Pyro Failed: `{PYRO_FAILED.is_set()}`",
            "TELEBOT READY"
        )

        print(
            "\n🟢 BOT DIN YA FARA POLLING...\n"
        )

        bot.infinity_polling(
            skip_pending=True,
            timeout=60,
            long_polling_timeout=60
        )

    except Exception as e:

        send_admin_exception(
            "💥💥💥 BOT MAIN PROCESS YA CRASH",
            e
        )

        raise