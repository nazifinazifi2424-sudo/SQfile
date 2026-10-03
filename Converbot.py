import os
import time
import shutil
import logging
import tempfile
import threading
from pathlib import Path

import telebot
from telebot import types

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()

# Optional:
# Idan kana son admin ya rika samun debug messages,
# saka ADMIN_ID a Render.
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()


# ============================================================
# VALIDATION
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from Render Environment Variables.")

if not API_ID_RAW:
    raise RuntimeError("API_ID is missing from Render Environment Variables.")

if not API_HASH:
    raise RuntimeError("API_HASH is missing from Render Environment Variables.")

try:
    API_ID = int(API_ID_RAW)
except ValueError:
    raise RuntimeError("API_ID must be a number.")

ADMIN_ID = None

if ADMIN_ID_RAW:
    try:
        ADMIN_ID = int(ADMIN_ID_RAW)
    except ValueError:
        ADMIN_ID = None


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("VD-BOT")


# ============================================================
# TELEBOT
# ============================================================

bot = telebot.TeleBot(
    BOT_TOKEN,
    parse_mode="HTML",
    threaded=True
)


# ============================================================
# /VIDEOCON — LARGE VIDEO / FILE CONVERTER
# ============================================================
#
# FULL DEBUG EDITION
#
# FLOW:
#
# /videocon
#      ↓
# Send VIDEO or DOCUMENT
#      ↓
# Choose:
#      🎬 Video
#      📁 File
#      ↓
# Pyrogram MTProto
#      ↓
# Telegram → Render DOWNLOAD
#      ↓
# Render → Telegram UPLOAD
#
# ============================================================
# DEBUG SYSTEM
# ============================================================
#
# EVERYTHING IS LOGGED TO:
#
# 1. RENDER CONSOLE
# 2. TELEGRAM ADMIN CHAT
#
# DEBUG COVERS:
#
# - Bot command received
# - User ID
# - Message ID
# - File ID
# - File name
# - File type
# - File size
# - Button callback
# - Job creation
# - Worker creation
# - Pyrogram thread
# - Pyrogram event loop
# - Pyrogram startup
# - Pyrogram get_me
# - Pyrogram get_messages
# - Telegram API errors
# - Download start
# - Download progress
# - Download finish
# - Download verification
# - Upload start
# - Upload progress
# - Upload finish
# - Disk total
# - Disk used
# - Disk free
# - RAM
# - CPU
# - Python version
# - OS
# - Render environment
# - Temp directory
# - File existence
# - File size after download
# - Cleanup
# - Thread exceptions
# - Asyncio exceptions
# - Unexpected exceptions
# - Traceback
#
# ============================================================
#
# IMPORTANT:
#
# This code DOES NOT convert/compress the file.
#
# It only:
#
# Telegram
#    ↓
# Download original file
#    ↓
# Upload same file
#    ↓
# Video OR Document
#
# ============================================================


# ============================================================
# IMPORTS
# ============================================================

import os
import sys
import asyncio
import tempfile
import shutil
import threading
import time
import traceback
import queue
import platform
import socket
import json

import shutil as diskutil

from datetime import datetime

from telebot import types
from pyrogram import Client


# ============================================================
# CONFIG
# ============================================================

VIDEOCON_MAX_GB = 1.65

VIDEOCON_MAX_BYTES = int(
    VIDEOCON_MAX_GB
    * 1024
    * 1024
    * 1024
)


# ============================================================
# ENVIRONMENT
# ============================================================

try:

    API_ID = int(
        os.getenv(
            "API_ID",
            "0"
        )
    )

except Exception:

    API_ID = 0


API_HASH = os.getenv(
    "API_HASH",
    ""
)


BOT_TOKEN = os.getenv(
    "BOT_TOKEN",
    ""
)


try:

    ADMIN_ID = int(
        os.getenv(
            "ADMIN_ID",
            str(globals().get("ADMIN_ID", "0"))
        )
    )

except Exception:

    ADMIN_ID = 0


# ============================================================
# PYROGRAM SESSION
# ============================================================

VIDEOCON_SESSION_NAME = (
    "videocon_mtproto"
)


# ============================================================
# SAFETY MARGIN
# ============================================================

VIDEOCON_MIN_FREE_BYTES = (
    300
    * 1024
    * 1024
)


# ============================================================
# TELEGRAM PROGRESS UPDATE
# ============================================================

VIDEOCON_PROGRESS_INTERVAL = 5


# ============================================================
# DEBUG TELEGRAM LIMIT
# ============================================================

# Telegram message maximum is around 4096 characters.
# We keep lower than that for safety.

VIDEOCON_DEBUG_MESSAGE_LIMIT = 3500


# ============================================================
# SESSION STATE
# ============================================================

_videocon_waiting = set()

_videocon_jobs = {}

_videocon_lock = threading.Lock()


# ============================================================
# PYROGRAM ENGINE STATE
# ============================================================

_videocon_loop = None

_videocon_loop_thread = None

_videocon_pyro = None

_videocon_ready = threading.Event()

_videocon_start_error = None

_videocon_engine_lock = threading.Lock()


# ============================================================
# TELEGRAM DEBUG QUEUE
# ============================================================

_videocon_debug_queue = queue.Queue(
    maxsize=3000
)

_videocon_debug_sender_started = False

_videocon_debug_sender_lock = (
    threading.Lock()
)


# ============================================================
# DEBUG FORMATTER
# ============================================================

def _videocon_debug_format(
    level,
    *args
):

    try:

        timestamp = (
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    except Exception:

        timestamp = "UNKNOWN_TIME"


    try:

        thread_name = (
            threading.current_thread().name
        )

    except Exception:

        thread_name = "UNKNOWN_THREAD"


    try:

        message = " ".join(
            str(x)
            for x in args
        )

    except Exception:

        message = repr(args)


    return (
        f"[{timestamp}] "
        f"[VIDEOCON] "
        f"[{level}] "
        f"[{thread_name}] "
        f"{message}"
    )


# ============================================================
# TELEGRAM DEBUG SENDER
# ============================================================

def _videocon_debug_sender():

    """
    Dedicated thread for sending debug logs
    to ADMIN_ID.

    Important:
    Debug sending is separated from the main
    bot/Pyrogram worker so Telegram debug failure
    does NOT stop the converter.
    """

    global _videocon_debug_sender_started

    while True:

        try:

            first = (
                _videocon_debug_queue.get()
            )

            lines = [
                first
            ]

            # Collect a few more logs immediately.
            # This prevents Telegram flood.
            deadline = (
                time.time()
                + 0.7
            )

            while (
                time.time()
                <
                deadline
            ):

                try:

                    item = (
                        _videocon_debug_queue.get(
                            timeout=0.05
                        )
                    )

                    lines.append(item)

                except queue.Empty:

                    pass


            # ------------------------------------------------
            # JOIN
            # ------------------------------------------------

            text = "\n".join(
                lines
            )


            # ------------------------------------------------
            # SPLIT
            # ------------------------------------------------

            chunks = []

            while len(text) > (
                VIDEOCON_DEBUG_MESSAGE_LIMIT
            ):

                chunks.append(
                    text[
                        :VIDEOCON_DEBUG_MESSAGE_LIMIT
                    ]
                )

                text = text[
                    VIDEOCON_DEBUG_MESSAGE_LIMIT:
                ]


            if text:

                chunks.append(
                    text
                )


            # ------------------------------------------------
            # SEND
            # ------------------------------------------------

            if not ADMIN_ID:

                # Cannot send without ADMIN_ID.
                continue


            for chunk in chunks:

                try:

                    # bot must already exist.
                    telegram_bot = (
                        globals().get(
                            "bot"
                        )
                    )

                    if not telegram_bot:

                        # Main bot is not ready yet.
                        # Put back once, but don't loop forever.
                        continue


                    telegram_bot.send_message(

                        ADMIN_ID,

                        (
                            "🛠 <b>VIDEOCON DEBUG</b>\n\n"
                            "<code>"
                            + chunk.replace(
                                "&",
                                "&amp;"
                            ).replace(
                                "<",
                                "&lt;"
                            ).replace(
                                ">",
                                "&gt;"
                            )
                            + "</code>"
                        ),

                        parse_mode="HTML"

                    )

                except Exception as telegram_error:

                    # VERY IMPORTANT:
                    # DO NOT call videocon_debug() here.
                    # Otherwise we create an infinite debug loop.

                    try:

                        print(
                            "❌ [VIDEOCON DEBUG TELEGRAM SEND ERROR]",
                            repr(
                                telegram_error
                            ),
                            flush=True
                        )

                    except Exception:

                        pass


        except Exception as sender_error:

            try:

                print(
                    "❌ [VIDEOCON DEBUG SENDER ERROR]",
                    repr(sender_error),
                    flush=True
                )

            except Exception:

                pass

            time.sleep(1)


# ============================================================
# START DEBUG SENDER
# ============================================================

def start_videocon_debug_sender():

    global _videocon_debug_sender_started

    with _videocon_debug_sender_lock:

        if _videocon_debug_sender_started:

            return

        thread = threading.Thread(

            target=(
                _videocon_debug_sender
            ),

            daemon=True,

            name="videocon-debug-sender"

        )

        thread.start()

        _videocon_debug_sender_started = True


# ============================================================
# MAIN DEBUG FUNCTION
# ============================================================

def videocon_debug(
    *args,
    level="INFO",
    telegram=True
):

    """
    Every debug goes to:

    1. Render print
    2. Telegram ADMIN_ID

    This function NEVER raises an exception.
    """

    try:

        line = _videocon_debug_format(
            level,
            *args
        )

    except Exception:

        line = repr(args)


    # ========================================================
    # RENDER
    # ========================================================

    try:

        print(
            line,
            flush=True
        )

    except Exception:

        pass


    # ========================================================
    # TELEGRAM
    # ========================================================

    if not telegram:

        return


    try:

        start_videocon_debug_sender()

    except Exception:

        pass


    try:

        if _videocon_debug_queue.full():

            # Remove oldest item to make room.
            try:

                _videocon_debug_queue.get_nowait()

            except Exception:

                pass


        _videocon_debug_queue.put_nowait(
            line
        )

    except Exception:

        pass


# ============================================================
# EXCEPTION DEBUG
# ============================================================

def videocon_debug_exception(
    title,
    exception
):

    try:

        videocon_debug(
            "================================",
            level="ERROR"
        )

        videocon_debug(
            title,
            level="ERROR"
        )

        videocon_debug(
            "ERROR TYPE =",
            type(exception).__name__,
            level="ERROR"
        )

        videocon_debug(
            "ERROR =",
            repr(exception),
            level="ERROR"
        )

        videocon_debug(
            "TRACEBACK:",
            level="ERROR"
        )

        videocon_debug(
            traceback.format_exc(),
            level="ERROR"
        )

        videocon_debug(
            "================================",
            level="ERROR"
        )

    except Exception:

        pass


# ============================================================
# SIZE FORMAT
# ============================================================

def videocon_size(
    value
):

    try:

        value = float(
            value
        )

    except Exception:

        return "0 B"


    if value >= 1024 ** 3:

        return (
            f"{value / (1024 ** 3):.2f} GB"
        )


    if value >= 1024 ** 2:

        return (
            f"{value / (1024 ** 2):.2f} MB"
        )


    if value >= 1024:

        return (
            f"{value / 1024:.2f} KB"
        )


    return (
        f"{int(value)} B"
    )


# ============================================================
# PERCENT
# ============================================================

def videocon_progress_percent(
    current,
    total
):

    try:

        if not total:

            return 0.0

        return (
            float(current)
            * 100
        ) / float(total)

    except Exception:

        return 0.0


# ============================================================
# SYSTEM DEBUG
# ============================================================

def videocon_system_debug():

    videocon_debug(
        "===== SYSTEM DEBUG ====="
    )

    # --------------------------------------------------------
    # PYTHON
    # --------------------------------------------------------

    try:

        videocon_debug(
            "PYTHON VERSION =",
            sys.version
        )

    except Exception as e:

        videocon_debug(
            "PYTHON VERSION ERROR =",
            repr(e),
            level="ERROR"
        )


    # --------------------------------------------------------
    # PLATFORM
    # --------------------------------------------------------

    try:

        videocon_debug(
            "PLATFORM =",
            platform.platform()
        )

        videocon_debug(
            "SYSTEM =",
            platform.system()
        )

        videocon_debug(
            "RELEASE =",
            platform.release()
        )

        videocon_debug(
            "MACHINE =",
            platform.machine()
        )

    except Exception as e:

        videocon_debug(
            "PLATFORM DEBUG ERROR =",
            repr(e),
            level="ERROR"
        )


    # --------------------------------------------------------
    # HOSTNAME
    # --------------------------------------------------------

    try:

        videocon_debug(
            "HOSTNAME =",
            socket.gethostname()
        )

    except Exception as e:

        videocon_debug(
            "HOSTNAME ERROR =",
            repr(e),
            level="ERROR"
        )


    # --------------------------------------------------------
    # CURRENT DIRECTORY
    # --------------------------------------------------------

    try:

        videocon_debug(
            "CURRENT DIRECTORY =",
            os.getcwd()
        )

    except Exception as e:

        videocon_debug(
            "CWD ERROR =",
            repr(e),
            level="ERROR"
        )


    # --------------------------------------------------------
    # DISK
    # --------------------------------------------------------

    try:

        total, used, free = (
            diskutil.disk_usage(
                os.getcwd()
            )
        )

        videocon_debug(
            "DISK TOTAL =",
            videocon_size(total)
        )

        videocon_debug(
            "DISK USED =",
            videocon_size(used)
        )

        videocon_debug(
            "DISK FREE =",
            videocon_size(free)
        )

    except Exception as e:

        videocon_debug(
            "DISK CHECK ERROR =",
            repr(e),
            level="ERROR"
        )


    # --------------------------------------------------------
    # RAM
    # --------------------------------------------------------

    try:

        import psutil

        memory = (
            psutil.virtual_memory()
        )

        videocon_debug(
            "RAM TOTAL =",
            videocon_size(
                memory.total
            )
        )

        videocon_debug(
            "RAM USED =",
            videocon_size(
                memory.used
            )
        )

        videocon_debug(
            "RAM AVAILABLE =",
            videocon_size(
                memory.available
            )
        )

        videocon_debug(
            "RAM PERCENT =",
            memory.percent
        )

        # CPU
        videocon_debug(
            "CPU COUNT =",
            psutil.cpu_count()
        )

        videocon_debug(
            "CPU USAGE =",
            psutil.cpu_percent(
                interval=0.1
            ),
            "%"
        )

        # PROCESS RAM
        try:

            process = (
                psutil.Process(
                    os.getpid()
                )
            )

            process_memory = (
                process.memory_info()
            )

            videocon_debug(
                "PROCESS RAM =",
                videocon_size(
                    process_memory.rss
                )
            )

        except Exception as e:

            videocon_debug(
                "PROCESS RAM ERROR =",
                repr(e),
                level="ERROR"
            )

    except Exception as e:

        videocon_debug(
            "RAM/CPU CHECK ERROR =",
            repr(e),
            level="ERROR"
        )


    # --------------------------------------------------------
    # ENVIRONMENT STATUS
    # --------------------------------------------------------

    try:

        videocon_debug(
            "API_ID PRESENT =",
            bool(API_ID)
        )

        videocon_debug(
            "API_HASH PRESENT =",
            bool(API_HASH)
        )

        videocon_debug(
            "BOT_TOKEN PRESENT =",
            bool(BOT_TOKEN)
        )

        videocon_debug(
            "ADMIN_ID =",
            ADMIN_ID
        )

        videocon_debug(
            "PORT =",
            os.getenv(
                "PORT",
                "NOT_SET"
            )
        )

    except Exception as e:

        videocon_debug(
            "ENV DEBUG ERROR =",
            repr(e),
            level="ERROR"
        )


    videocon_debug(
        "===== END SYSTEM DEBUG ====="
    )


# ============================================================
# SAFE TELEGRAM STATUS EDIT
# ============================================================

def videocon_edit_status(
    status_message,
    text
):

    if not status_message:

        videocon_debug(
            "STATUS EDIT SKIPPED: status_message=None",
            level="WARNING"
        )

        return False


    try:

        telegram_bot = (
            globals().get(
                "bot"
            )
        )

        if not telegram_bot:

            videocon_debug(
                "STATUS EDIT FAILED: bot object unavailable",
                level="ERROR"
            )

            return False


        telegram_bot.edit_message_text(

            chat_id=(
                status_message.chat.id
            ),

            message_id=(
                status_message.message_id
            ),

            text=text,

            parse_mode="HTML"

        )

        return True

    except Exception as e:

        videocon_debug(
            "STATUS EDIT ERROR =",
            repr(e),
            level="WARNING"
        )

        return False


# ============================================================
# PROGRESS STATUS
# ============================================================

def videocon_build_progress_text(
    mode,
    current,
    total
):

    percent = (
        videocon_progress_percent(
            current,
            total
        )
    )


    if mode == "download":

        title = (
            "⬇️ <b>DOWNLOADING...</b>"
        )

        direction = (
            "Telegram → Render"
        )

    else:

        title = (
            "📤 <b>UPLOADING...</b>"
        )

        direction = (
            "Render → Telegram"
        )


    return (

        f"{title}\n\n"

        f"📊 Progress: "
        f"<b>{percent:.1f}%</b>\n"

        f"📦 "
        f"<b>{videocon_size(current)}</b>"
        f" / "
        f"<b>{videocon_size(total)}</b>\n\n"

        f"🔄 {direction}\n\n"

        "⏳ Please wait..."
    )


# ============================================================
# TRANSFER PROGRESS
# ============================================================

async def videocon_transfer_progress(

    current,

    total,

    status_message,

    mode,

    progress_state

):

    try:

        if not total:

            return


        percent = (
            videocon_progress_percent(
                current,
                total
            )
        )


        now = time.time()

        last_time = (
            progress_state.get(
                "time",
                0
            )
        )


        # ----------------------------------------------------
        # Telegram status every X seconds
        # ----------------------------------------------------

        if (

            percent < 100

            and

            now - last_time
            <
            VIDEOCON_PROGRESS_INTERVAL

        ):

            return


        progress_state["time"] = now


        # ----------------------------------------------------
        # DEBUG
        # ----------------------------------------------------

        videocon_debug(

            mode.upper(),

            f"{percent:.1f}%",

            videocon_size(current),

            "/",

            videocon_size(total)

        )


        # ----------------------------------------------------
        # SYSTEM CHECK EVERY UPDATE
        # ----------------------------------------------------

        try:

            total_disk, used_disk, free_disk = (
                diskutil.disk_usage(
                    os.getcwd()
                )
            )

            videocon_debug(
                mode.upper(),
                "DISK FREE =",
                videocon_size(
                    free_disk
                )
            )

        except Exception:

            pass


        # ----------------------------------------------------
        # USER STATUS
        # ----------------------------------------------------

        text = (
            videocon_build_progress_text(
                mode,
                current,
                total
            )
        )


        videocon_edit_status(
            status_message,
            text
        )


    except Exception as e:

        videocon_debug_exception(
            "PROGRESS CALLBACK ERROR",
            e
        )


# ============================================================
# PYROGRAM THREAD
# ============================================================

def _videocon_pyrogram_thread():

    global _videocon_loop
    global _videocon_pyro
    global _videocon_start_error


    try:

        videocon_debug(
            "================================"
        )

        videocon_debug(
            "PYROGRAM THREAD STARTING"
        )

        videocon_debug(
            "THREAD NAME =",
            threading.current_thread().name
        )

        videocon_debug(
            "THREAD IDENT =",
            threading.get_ident()
        )


        videocon_system_debug()


        # ====================================================
        # VALIDATE ENV
        # ====================================================

        if not API_ID:

            raise RuntimeError(
                "API_ID baya nan ko invalid ne."
            )


        if not API_HASH:

            raise RuntimeError(
                "API_HASH baya nan."
            )


        if not BOT_TOKEN:

            raise RuntimeError(
                "BOT_TOKEN baya nan."
            )


        # ====================================================
        # EVENT LOOP
        # ====================================================

        videocon_debug(
            "Creating asyncio event loop..."
        )

        _videocon_loop = (
            asyncio.new_event_loop()
        )


        asyncio.set_event_loop(
            _videocon_loop
        )


        videocon_debug(
            "Asyncio event loop created."
        )


        # ====================================================
        # ASYNCIO EXCEPTION HANDLER
        # ====================================================

        def async_exception_handler(
            loop,
            context
        ):

            try:

                message = context.get(
                    "message"
                )

                exception = context.get(
                    "exception"
                )

                videocon_debug(
                    "ASYNCIO UNHANDLED ERROR",
                    message,
                    level="ERROR"
                )

                if exception:

                    videocon_debug_exception(
                        "ASYNCIO EXCEPTION",
                        exception
                    )

            except Exception:

                pass


        _videocon_loop.set_exception_handler(
            async_exception_handler
        )


        # ====================================================
        # PYROGRAM CLIENT
        # ====================================================

        videocon_debug(
            "Creating Pyrogram Client..."
        )


        _videocon_pyro = Client(

            VIDEOCON_SESSION_NAME,

            api_id=API_ID,

            api_hash=API_HASH,

            bot_token=BOT_TOKEN,

            no_updates=True,

            max_concurrent_transmissions=1,

            sleep_threshold=30

        )


        videocon_debug(
            "Pyrogram Client object created."
        )


        # ====================================================
        # START CLIENT
        # ====================================================

        async def start_client():

            videocon_debug(
                "Calling Pyrogram start()..."
            )

            await _videocon_pyro.start()

            videocon_debug(
                "✅ Pyrogram start() SUCCESS."
            )


            # ------------------------------------------------
            # GET ME
            # ------------------------------------------------

            try:

                videocon_debug(
                    "Calling Pyrogram get_me()..."
                )

                me = (
                    await
                    _videocon_pyro.get_me()
                )


                videocon_debug(
                    "Pyrogram username =",
                    getattr(
                        me,
                        "username",
                        None
                    )
                )

                videocon_debug(
                    "Pyrogram first_name =",
                    getattr(
                        me,
                        "first_name",
                        None
                    )
                )

                videocon_debug(
                    "Pyrogram ID =",
                    getattr(
                        me,
                        "id",
                        None
                    )
                )


            except Exception as e:

                videocon_debug_exception(
                    "Pyrogram get_me() ERROR",
                    e
                )


        # ====================================================
        # RUN START
        # ====================================================

        _videocon_loop.run_until_complete(
            start_client()
        )


        # ====================================================
        # READY
        # ====================================================

        _videocon_ready.set()


        videocon_debug(
            "================================"
        )

        videocon_debug(
            "✅ VIDEOCON MTProto ENGINE READY"
        )

        videocon_debug(
            "Pyrogram loop is now running."
        )

        videocon_debug(
            "================================"
        )


        # ====================================================
        # KEEP ALIVE
        # ====================================================

        _videocon_loop.run_forever()


    except Exception as e:

        _videocon_start_error = e


        videocon_debug_exception(
            "❌ PYROGRAM START ERROR",
            e
        )


        # Very important:
        # worker waiting on Event must wake up.

        _videocon_ready.set()


    finally:

        videocon_debug(
            "PYROGRAM THREAD EXITED.",
            level="WARNING"
        )


# ============================================================
# START PYROGRAM ENGINE
# ============================================================

def start_videocon_engine():

    global _videocon_loop_thread


    with _videocon_engine_lock:

        # ----------------------------------------------------
        # Already running
        # ----------------------------------------------------

        if (

            _videocon_loop_thread

            and

            _videocon_loop_thread.is_alive()

        ):

            videocon_debug(
                "Pyrogram engine already running."
            )

            return


        # ----------------------------------------------------
        # Reset startup state
        # ----------------------------------------------------

        _videocon_ready.clear()


        # ----------------------------------------------------
        # New thread
        # ----------------------------------------------------

        videocon_debug(
            "Starting new Pyrogram engine thread..."
        )


        _videocon_loop_thread = (
            threading.Thread(

                target=(
                    _videocon_pyrogram_thread
                ),

                daemon=True,

                name="videocon-mtproto"

            )
        )


        _videocon_loop_thread.start()


        videocon_debug(
            "Pyrogram engine thread STARTED."
        )


# ============================================================
# RUN COROUTINE
# ============================================================

def videocon_run_async(
    coro
):

    if not _videocon_loop:

        raise RuntimeError(
            "Pyrogram event loop bai fara ba."
        )


    if _videocon_start_error:

        raise RuntimeError(

            "Pyrogram startup failed: "
            f"{_videocon_start_error}"

        )


    if (
        not _videocon_loop_thread
        or
        not _videocon_loop_thread.is_alive()
    ):

        raise RuntimeError(
            "Pyrogram thread baya aiki."
        )


    try:

        future = (
            asyncio.run_coroutine_threadsafe(

                coro,

                _videocon_loop

            )
        )

        return future.result()

    except Exception as e:

        videocon_debug_exception(
            "RUN COROUTINE ERROR",
            e
        )

        raise


# ============================================================
# /VIDEOCON
# ============================================================

@bot.message_handler(
    commands=["videocon"]
)
def videocon_command(
    message
):

    user_id = (
        message.from_user.id
    )


    videocon_debug(
        "================================"
    )

    videocon_debug(
        "/VIDEOCON COMMAND RECEIVED"
    )

    videocon_debug(
        "USER ID =",
        user_id
    )

    videocon_debug(
        "CHAT ID =",
        message.chat.id
    )

    videocon_debug(
        "MESSAGE ID =",
        message.message_id
    )


    # ========================================================
    # ADMIN ONLY
    # ========================================================

    if user_id != ADMIN_ID:

        videocon_debug(
            "NON-ADMIN ATTEMPTED /videocon",
            user_id,
            level="WARNING"
        )

        try:

            bot.reply_to(

                message,

                "❌ Wannan command ɗin "
                "na Admin ne kawai."

            )

        except Exception as e:

            videocon_debug_exception(
                "NON-ADMIN REPLY ERROR",
                e
            )

        return


    videocon_debug(
        "ADMIN VERIFIED."
    )


    # ========================================================
    # START PYROGRAM
    # ========================================================

    try:

        start_videocon_engine()

    except Exception as e:

        videocon_debug_exception(
            "ENGINE START ERROR",
            e
        )


    # ========================================================
    # SESSION
    # ========================================================

    _videocon_waiting.add(
        user_id
    )


    _videocon_jobs.pop(
        user_id,
        None
    )


    videocon_debug(
        "ADMIN SESSION CREATED."
    )


    # ========================================================
    # ASK
    # ========================================================

    try:

        bot.reply_to(

            message,

            (

                "🎬 <b>VIDEO CONVERTER</b>\n\n"

                "Turo min <b>Video</b> ko "
                "<b>File/Document</b> ɗin da "
                "kake son mu canza.\n\n"

                f"📦 Maximum: "
                f"<b>{VIDEOCON_MAX_GB} GB</b>\n\n"

                "⏳ Bayan ka turo shi zan tambaye ka "
                "irin yadda kake son na dawo maka da shi."

            ),

            parse_mode="HTML"

        )


        videocon_debug(
            "COMMAND REPLY SENT TO ADMIN."
        )


    except Exception as e:

        videocon_debug_exception(
            "COMMAND REPLY ERROR",
            e
        )


    videocon_debug(
        "================================"
    )


# ============================================================
# RECEIVE VIDEO
# ============================================================

@bot.message_handler(

    content_types=["video"],

    func=lambda message:
        message.from_user.id
        in _videocon_waiting

)
def videocon_receive_video(
    message
):

    user_id = (
        message.from_user.id
    )


    videocon_debug(
        "================================"
    )

    videocon_debug(
        "VIDEO MESSAGE RECEIVED"
    )

    videocon_debug(
        "USER =",
        user_id
    )

    videocon_debug(
        "MESSAGE ID =",
        message.message_id
    )


    if user_id != ADMIN_ID:

        videocon_debug(
            "VIDEO FROM NON-ADMIN REJECTED",
            level="WARNING"
        )

        _videocon_waiting.discard(
            user_id
        )

        return


    try:

        file_size = (

            getattr(
                message.video,
                "file_size",
                None
            )

            or 0

        )


        file_id = (
            message.video.file_id
        )


        videocon_debug(
            "VIDEO FILE ID =",
            file_id
        )

        videocon_debug(
            "VIDEO SIZE =",
            videocon_size(file_size)
        )


        # ====================================================
        # VALIDATE SIZE
        # ====================================================

        if file_size <= 0:

            videocon_debug(
                "VIDEO SIZE INVALID",
                level="ERROR"
            )

            bot.reply_to(
                message,
                "❌ An kasa gano girman video."
            )

            return


        if (
            file_size
            >
            VIDEOCON_MAX_BYTES
        ):

            videocon_debug(
                "VIDEO EXCEEDS MAXIMUM",
                level="WARNING"
            )

            bot.reply_to(

                message,

                (
                    "❌ <b>Video ya yi girma.</b>\n\n"

                    f"📦 Size: "
                    f"<b>{videocon_size(file_size)}</b>\n"

                    f"📦 Maximum: "
                    f"<b>{VIDEOCON_MAX_GB} GB</b>"
                ),

                parse_mode="HTML"

            )

            return


        # ====================================================
        # SAVE JOB
        # ====================================================

        _videocon_waiting.discard(
            user_id
        )


        _videocon_jobs[user_id] = {

            "file_id":
                file_id,

            "file_type":
                "video",

            "file_name":
                "converted_video.mp4",

            "file_size":
                file_size,

            "message_id":
                message.message_id,

            "chat_id":
                message.chat.id

        }


        videocon_debug(
            "VIDEO JOB SAVED."
        )

        videocon_debug(
            "JOB DATA =",
            repr(
                _videocon_jobs[user_id]
            )
        )


        # ====================================================
        # BUTTONS
        # ====================================================

        keyboard = (
            types.InlineKeyboardMarkup(
                row_width=2
            )
        )


        keyboard.add(

            types.InlineKeyboardButton(

                "🎬 Video",

                callback_data=(
                    "videocon:video"
                )

            ),

            types.InlineKeyboardButton(

                "📁 File",

                callback_data=(
                    "videocon:file"
                )

            )

        )


        # ====================================================
        # SEND CHOICE
        # ====================================================

        bot.reply_to(

            message,

            (

                "✅ <b>VIDEO RECEIVED</b>\n\n"

                f"📦 Size: "
                f"<b>{videocon_size(file_size)}</b>\n\n"

                "❓ <b>Me kake so na dawo maka da shi?</b>"

            ),

            reply_markup=keyboard,

            parse_mode="HTML"

        )


        videocon_debug(
            "VIDEO CHOICE BUTTONS SENT."
        )


    except Exception as e:

        videocon_debug_exception(
            "RECEIVE VIDEO ERROR",
            e
        )


# ============================================================
# RECEIVE DOCUMENT
# ============================================================

@bot.message_handler(

    content_types=["document"],

    func=lambda message:
        message.from_user.id
        in _videocon_waiting

)
def videocon_receive_document(
    message
):

    user_id = (
        message.from_user.id
    )


    videocon_debug(
        "================================"
    )

    videocon_debug(
        "DOCUMENT MESSAGE RECEIVED"
    )

    videocon_debug(
        "USER =",
        user_id
    )

    videocon_debug(
        "MESSAGE ID =",
        message.message_id
    )


    if user_id != ADMIN_ID:

        videocon_debug(
            "DOCUMENT FROM NON-ADMIN REJECTED",
            level="WARNING"
        )

        _videocon_waiting.discard(
            user_id
        )

        return


    try:

        file_size = (

            getattr(
                message.document,
                "file_size",
                None
            )

            or 0

        )


        file_name = (

            getattr(
                message.document,
                "file_name",
                None
            )

            or "converted_file"

        )


        file_id = (
            message.document.file_id
        )


        videocon_debug(
            "DOCUMENT FILE ID =",
            file_id
        )

        videocon_debug(
            "DOCUMENT FILE NAME =",
            file_name
        )

        videocon_debug(
            "DOCUMENT SIZE =",
            videocon_size(file_size)
        )


        # ====================================================
        # SIZE
        # ====================================================

        if file_size <= 0:

            videocon_debug(
                "DOCUMENT SIZE INVALID",
                level="ERROR"
            )

            bot.reply_to(
                message,
                "❌ An kasa gano girman file."
            )

            return


        if (
            file_size
            >
            VIDEOCON_MAX_BYTES
        ):

            videocon_debug(
                "DOCUMENT EXCEEDS MAXIMUM",
                level="WARNING"
            )

            bot.reply_to(

                message,

                (
                    "❌ <b>File ya yi girma.</b>\n\n"

                    f"📦 Size: "
                    f"<b>{videocon_size(file_size)}</b>\n"

                    f"📦 Maximum: "
                    f"<b>{VIDEOCON_MAX_GB} GB</b>"
                ),

                parse_mode="HTML"

            )

            return


        # ====================================================
        # SAVE JOB
        # ====================================================

        _videocon_waiting.discard(
            user_id
        )


        _videocon_jobs[user_id] = {

            "file_id":
                file_id,

            "file_type":
                "document",

            "file_name":
                file_name,

            "file_size":
                file_size,

            "message_id":
                message.message_id,

            "chat_id":
                message.chat.id

        }


        videocon_debug(
            "DOCUMENT JOB SAVED."
        )

        videocon_debug(
            "JOB DATA =",
            repr(
                _videocon_jobs[user_id]
            )
        )


        # ====================================================
        # BUTTONS
        # ====================================================

        keyboard = (
            types.InlineKeyboardMarkup(
                row_width=2
            )
        )


        keyboard.add(

            types.InlineKeyboardButton(

                "🎬 Video",

                callback_data=(
                    "videocon:video"
                )

            ),

            types.InlineKeyboardButton(

                "📁 File",

                callback_data=(
                    "videocon:file"
                )

            )

        )


        bot.reply_to(

            message,

            (

                "✅ <b>FILE RECEIVED</b>\n\n"

                f"📄 Name: "
                f"<b>{file_name}</b>\n\n"

                f"📦 Size: "
                f"<b>{videocon_size(file_size)}</b>\n\n"

                "❓ <b>Me kake so na dawo maka da shi?</b>"

            ),

            reply_markup=keyboard,

            parse_mode="HTML"

        )


        videocon_debug(
            "DOCUMENT CHOICE BUTTONS SENT."
        )


    except Exception as e:

        videocon_debug_exception(
            "RECEIVE DOCUMENT ERROR",
            e
        )


# ============================================================
# CALLBACK
# ============================================================

@bot.callback_query_handler(

    func=lambda call:
        call.data.startswith(
            "videocon:"
        )

)
def videocon_callback(
    call
):

    user_id = (
        call.from_user.id
    )


    videocon_debug(
        "================================"
    )

    videocon_debug(
        "VIDEOCON CALLBACK RECEIVED"
    )

    videocon_debug(
        "CALL ID =",
        call.id
    )

    videocon_debug(
        "USER =",
        user_id
    )

    videocon_debug(
        "CALL DATA =",
        call.data
    )


    # ========================================================
    # ADMIN
    # ========================================================

    if user_id != ADMIN_ID:

        videocon_debug(
            "NON-ADMIN CALLBACK REJECTED",
            level="WARNING"
        )

        try:

            bot.answer_callback_query(

                call.id,

                "❌ Admin kawai."

            )

        except Exception as e:

            videocon_debug_exception(
                "CALLBACK ANSWER ERROR",
                e
            )

        return


    # ========================================================
    # CHOICE
    # ========================================================

    try:

        choice = (
            call.data.split(
                ":",
                1
            )[1]
        )

    except Exception as e:

        videocon_debug_exception(
            "CALLBACK DATA PARSE ERROR",
            e
        )

        return


    videocon_debug(
        "SELECTED CHOICE =",
        choice
    )


    if choice not in (
        "video",
        "file"
    ):

        videocon_debug(
            "INVALID CALLBACK OPTION",
            level="ERROR"
        )

        try:

            bot.answer_callback_query(

                call.id,

                "❌ Invalid option."

            )

        except Exception:

            pass

        return


    # ========================================================
    # JOB
    # ========================================================

    job = _videocon_jobs.get(
        user_id
    )


    if not job:

        videocon_debug(
            "NO JOB FOUND FOR USER",
            user_id,
            level="ERROR"
        )

        try:

            bot.answer_callback_query(

                call.id,

                "❌ Session ta ƙare. "
                "Ka sake amfani da /videocon."

            )

        except Exception:

            pass

        return


    videocon_debug(
        "JOB FOUND."
    )

    videocon_debug(
        "INPUT TYPE =",
        job.get("file_type")
    )

    videocon_debug(
        "OUTPUT TYPE =",
        choice
    )

    videocon_debug(
        "FILE SIZE =",
        videocon_size(
            job.get(
                "file_size",
                0
            )
        )
    )


    # ========================================================
    # REMOVE BUTTON
    # ========================================================

    try:

        bot.edit_message_reply_markup(

            chat_id=(
                call.message.chat.id
            ),

            message_id=(
                call.message.message_id
            ),

            reply_markup=None

        )


        videocon_debug(
            "CHOICE BUTTONS REMOVED."
        )


    except Exception as e:

        videocon_debug(
            "REMOVE BUTTON ERROR =",
            repr(e),
            level="WARNING"
        )


    # ========================================================
    # ANSWER CALLBACK
    # ========================================================

    try:

        bot.answer_callback_query(

            call.id,

            "🚀 An fara aiki..."

        )


        videocon_debug(
            "CALLBACK ANSWERED."
        )


    except Exception as e:

        videocon_debug(
            "CALLBACK ANSWER ERROR =",
            repr(e),
            level="WARNING"
        )


    # ========================================================
    # STATUS MESSAGE
    # ========================================================

    status_message = None


    try:

        status_message = (
            bot.send_message(

                user_id,

                (

                    "🚀 <b>AN FARA AIKI...</b>\n\n"

                    f"📥 Input: "
                    f"<b>{job.get('file_type')}</b>\n"

                    f"📤 Output: "
                    f"<b>{choice}</b>\n"

                    f"📦 Size: "
                    f"<b>"
                    f"{videocon_size(job.get('file_size', 0))}"
                    f"</b>\n\n"

                    "🔧 Ana shirya Pyrogram..."

                ),

                parse_mode="HTML"

            )
        )


        videocon_debug(
            "STATUS MESSAGE CREATED."
        )

        videocon_debug(
            "STATUS MESSAGE ID =",
            status_message.message_id
        )


    except Exception as e:

        videocon_debug_exception(
            "STATUS MESSAGE CREATE ERROR",
            e
        )


    # ========================================================
    # START WORKER
    # ========================================================

    try:

        worker = threading.Thread(

            target=_videocon_process,

            args=(

                user_id,

                job,

                choice,

                status_message

            ),

            daemon=True,

            name="videocon-worker"

        )


        worker.start()


        videocon_debug(
            "WORKER THREAD STARTED."
        )

        videocon_debug(
            "WORKER THREAD IDENT =",
            worker.ident
        )


    except Exception as e:

        videocon_debug_exception(
            "WORKER START ERROR",
            e
        )


# ============================================================
# MAIN PROCESS
# ============================================================

def _videocon_process(

    user_id,

    job,

    choice,

    status_message

):

    temp_dir = None

    input_file = None

    downloaded_size = 0


    try:

        # ====================================================
        # JOB START
        # ====================================================

        videocon_debug(
            "================================"
        )

        videocon_debug(
            "🚀 NEW VIDEOCON JOB STARTED"
        )

        videocon_debug(
            "USER =",
            user_id
        )

        videocon_debug(
            "INPUT =",
            job.get("file_type")
        )

        videocon_debug(
            "OUTPUT =",
            choice
        )

        videocon_debug(
            "EXPECTED SIZE =",
            videocon_size(
                job.get(
                    "file_size",
                    0
                )
            )
        )

        videocon_debug(
            "ORIGINAL MESSAGE ID =",
            job.get(
                "message_id"
            )
        )

        videocon_debug(
            "ORIGINAL CHAT ID =",
            job.get(
                "chat_id"
            )
        )

        videocon_debug(
            "ORIGINAL FILE ID =",
            job.get(
                "file_id"
            )
        )


        # ====================================================
        # SYSTEM
        # ====================================================

        videocon_system_debug()


        # ====================================================
        # STATUS
        # ====================================================

        videocon_edit_status(

            status_message,

            (
                "🚀 <b>AN FARA AIKI...</b>\n\n"

                "🔧 Ana haɗa Pyrogram...\n\n"

                "⏳ Please wait..."
            )

        )


        # ====================================================
        # WAIT PYROGRAM
        # ====================================================

        videocon_debug(
            "WAITING FOR PYROGRAM ENGINE..."
        )


        if not _videocon_ready.wait(
            timeout=90
        ):

            raise RuntimeError(

                "Pyrogram engine bai fara "
                "cikin seconds 90 ba."

            )


        videocon_debug(
            "Pyrogram READY EVENT received."
        )


        # ====================================================
        # START ERROR
        # ====================================================

        if _videocon_start_error:

            raise RuntimeError(

                "Pyrogram startup failed: "
                f"{_videocon_start_error}"

            )


        # ====================================================
        # CLIENT
        # ====================================================

        if not _videocon_pyro:

            raise RuntimeError(
                "Pyrogram client baya nan."
            )


        videocon_debug(
            "✅ PYROGRAM CLIENT AVAILABLE."
        )


        # ====================================================
        # TEMP DIRECTORY
        # ====================================================

        videocon_debug(
            "Creating temporary directory..."
        )


        temp_dir = (
            tempfile.mkdtemp(
                prefix="videocon_"
            )
        )


        videocon_debug(
            "TEMP DIR =",
            temp_dir
        )


        # ====================================================
        # DISK CHECK
        # ====================================================

        total, used, free = (
            diskutil.disk_usage(
                temp_dir
            )
        )


        videocon_debug(
            "DISK BEFORE DOWNLOAD:"
        )

        videocon_debug(
            "TOTAL =",
            videocon_size(total)
        )

        videocon_debug(
            "USED =",
            videocon_size(used)
        )

        videocon_debug(
            "FREE =",
            videocon_size(free)
        )


        required_space = (

            int(
                job["file_size"]
            )

            +

            VIDEOCON_MIN_FREE_BYTES

        )


        videocon_debug(
            "REQUIRED SPACE =",
            videocon_size(
                required_space
            )
        )


        if free < required_space:

            raise RuntimeError(

                "Render disk bai da isasshen "
                "wuri ba.\n\n"

                f"Free: "
                f"{videocon_size(free)}\n"

                f"Required: "
                f"{videocon_size(required_space)}"

            )


        videocon_debug(
            "✅ DISK SPACE CHECK PASSED."
        )


        # ====================================================
        # FILE NAME
        # ====================================================

        original_name = (

            job.get(
                "file_name"
            )

            or "converted_file"

        )


        original_name = os.path.basename(
            original_name
        )


        input_file = os.path.join(

            temp_dir,

            original_name

        )


        videocon_debug(
            "INPUT FILE PATH =",
            input_file
        )


        # ====================================================
        # CHECK TELEGRAM ORIGINAL
        # ====================================================

        videocon_edit_status(

            status_message,

            (
                "🔎 <b>CHECKING TELEGRAM...</b>\n\n"

                "Ana neman original message...\n\n"

                f"📦 "
                f"<b>{videocon_size(job['file_size'])}</b>"
            )

        )


        videocon_debug(
            "================================"
        )

        videocon_debug(
            "GETTING ORIGINAL TELEGRAM MESSAGE"
        )

        videocon_debug(
            "CHAT ID =",
            job["chat_id"]
        )

        videocon_debug(
            "MESSAGE ID =",
            job["message_id"]
        )


        pyro_message = (
            videocon_run_async(

                _videocon_get_message(

                    job["chat_id"],

                    job["message_id"]

                )

            )
        )


        if not pyro_message:

            raise RuntimeError(

                "Pyrogram bai iya samun "
                "original message ba."

            )


        videocon_debug(
            "✅ ORIGINAL TELEGRAM MESSAGE FOUND."
        )


        videocon_debug(
            "PYROGRAM MESSAGE ID =",
            getattr(
                pyro_message,
                "id",
                None
            )
        )


        videocon_debug(
            "PYROGRAM MESSAGE TYPE =",
            getattr(
                pyro_message,
                "media",
                None
            )
        )


        # ====================================================
        # DOWNLOAD
        # ====================================================

        videocon_edit_status(

            status_message,

            (
                "⬇️ <b>DOWNLOADING...</b>\n\n"

                f"📦 Expected: "
                f"<b>"
                f"{videocon_size(job['file_size'])}"
                f"</b>\n\n"

                "🔄 Telegram → Render\n\n"

                "⏳ Please wait..."
            )

        )


        videocon_debug(
            "================================"
        )

        videocon_debug(
            "⬇️ DOWNLOAD STARTED"
        )

        videocon_debug(
            "TARGET PATH =",
            input_file
        )


        download_state = {
            "time": 0
        }


        downloaded_path = (
            videocon_run_async(

                _videocon_download(

                    pyro_message,

                    input_file,

                    status_message,

                    download_state

                )

            )
        )


        videocon_debug(
            "download_media() RETURNED =",
            downloaded_path
        )


        if not downloaded_path:

            raise RuntimeError(

                "Pyrogram download ya dawo "
                "babu file path."

            )


        input_file = downloaded_path


        videocon_debug(
            "DOWNLOAD FUNCTION FINISHED."
        )


        # ====================================================
        # VERIFY FILE
        # ====================================================

        videocon_debug(
            "VERIFYING DOWNLOADED FILE..."
        )


        if not os.path.exists(
            input_file
        ):

            raise RuntimeError(

                "Downloaded file bai "
                "bayyana a disk ba."

            )


        downloaded_size = (
            os.path.getsize(
                input_file
            )
        )


        videocon_debug(
            "DOWNLOADED FILE EXISTS."
        )

        videocon_debug(
            "DOWNLOADED PATH =",
            input_file
        )

        videocon_debug(
            "DOWNLOADED SIZE =",
            videocon_size(
                downloaded_size
            )
        )


        if downloaded_size <= 0:

            raise RuntimeError(
                "Downloaded file empty ne."
            )


        # ====================================================
        # COMPARE SIZE
        # ====================================================

        expected_size = int(
            job.get(
                "file_size",
                0
            )
        )


        videocon_debug(
            "EXPECTED SIZE =",
            videocon_size(
                expected_size
            )
        )

        videocon_debug(
            "ACTUAL SIZE =",
            videocon_size(
                downloaded_size
            )
        )


        if expected_size:

            difference = (
                downloaded_size
                -
                expected_size
            )

            videocon_debug(
                "SIZE DIFFERENCE =",
                videocon_size(
                    abs(difference)
                )
            )


        # ====================================================
        # SYSTEM AFTER DOWNLOAD
        # ====================================================

        videocon_system_debug()


        # ====================================================
        # DOWNLOAD COMPLETE
        # ====================================================

        videocon_edit_status(

            status_message,

            (
                "✅ <b>DOWNLOAD COMPLETE</b>\n\n"

                f"📦 File: "
                f"<b>"
                f"{videocon_size(downloaded_size)}"
                f"</b>\n\n"

                "📤 Yanzu ana aikawa "
                "Telegram...\n\n"

                "⏳ Please wait..."
            )

        )


        # ====================================================
        # UPLOAD AS FILE
        # ====================================================

        if choice == "file":

            videocon_debug(
                "================================"
            )

            videocon_debug(
                "📤 DOCUMENT UPLOAD PREPARING"
            )


            videocon_edit_status(

                status_message,

                (
                    "📤 <b>UPLOADING AS FILE...</b>\n\n"

                    f"📦 Size: "
                    f"<b>"
                    f"{videocon_size(downloaded_size)}"
                    f"</b>\n\n"

                    "🔄 Render → Telegram\n\n"

                    "⏳ Please wait..."
                )

            )


            upload_state = {
                "time": 0
            }


            videocon_debug(
                "Calling send_document()..."
            )


            result = (
                videocon_run_async(

                    _videocon_upload_document(

                        user_id,

                        input_file,

                        original_name,

                        downloaded_size,

                        status_message,

                        upload_state

                    )

                )
            )


            videocon_debug(
                "send_document() RETURNED."
            )


            if not result:

                raise RuntimeError(

                    "Telegram bai dawo da "
                    "document upload result ba."

                )


            videocon_debug(
                "DOCUMENT MESSAGE ID =",
                getattr(
                    result,
                    "id",
                    None
                )
            )

            videocon_debug(
                "📤 DOCUMENT UPLOAD FINISHED."
            )


        # ====================================================
        # UPLOAD AS VIDEO
        # ====================================================

        elif choice == "video":

            videocon_debug(
                "================================"
            )

            videocon_debug(
                "🎬 VIDEO UPLOAD PREPARING"
            )


            videocon_edit_status(

                status_message,

                (
                    "📤 <b>UPLOADING AS VIDEO...</b>\n\n"

                    f"📦 Size: "
                    f"<b>"
                    f"{videocon_size(downloaded_size)}"
                    f"</b>\n\n"

                    "🔄 Render → Telegram\n\n"

                    "⏳ Please wait..."
                )

            )


            upload_state = {
                "time": 0
            }


            videocon_debug(
                "Calling send_video()..."
            )


            result = (
                videocon_run_async(

                    _videocon_upload_video(

                        user_id,

                        input_file,

                        original_name,

                        downloaded_size,

                        status_message,

                        upload_state

                    )

                )
            )


            videocon_debug(
                "send_video() RETURNED."
            )


            if not result:

                raise RuntimeError(

                    "Telegram bai dawo da "
                    "video upload result ba."

                )


            videocon_debug(
                "VIDEO MESSAGE ID =",
                getattr(
                    result,
                    "id",
                    None
                )
            )


            videocon_debug(
                "🎬 VIDEO UPLOAD FINISHED."
            )


        # ====================================================
        # COMPLETE
        # ====================================================

        videocon_debug(
            "Preparing COMPLETE status..."
        )


        returned_as = (

            "VIDEO"
            if choice == "video"
            else
            "FILE"

        )


        videocon_edit_status(

            status_message,

            (
                "✅ <b>CONVERSION COMPLETE</b>\n\n"

                f"📦 Size: "
                f"<b>"
                f"{videocon_size(downloaded_size)}"
                f"</b>\n\n"

                f"📤 Returned as: "
                f"<b>{returned_as}</b>\n\n"

                "🎉 An gama successfully."
            )

        )


        # ====================================================
        # COMPLETE DEBUG
        # ====================================================

        videocon_debug(
            "================================"
        )

        videocon_debug(
            "✅ JOB COMPLETE"
        )

        videocon_debug(
            "USER =",
            user_id
        )

        videocon_debug(
            "INPUT =",
            job.get("file_type")
        )

        videocon_debug(
            "OUTPUT =",
            choice
        )

        videocon_debug(
            "SIZE =",
            videocon_size(
                downloaded_size
            )
        )

        videocon_debug(
            "STATUS = SUCCESS"
        )

        videocon_debug(
            "================================"
        )


    # ========================================================
    # ERROR
    # ========================================================

    except Exception as e:

        videocon_debug(
            "================================",
            level="ERROR"
        )

        videocon_debug(
            "❌ VIDEOCON JOB FAILED",
            level="ERROR"
        )

        videocon_debug(
            "USER =",
            user_id,
            level="ERROR"
        )

        videocon_debug(
            "INPUT =",
            job.get("file_type"),
            level="ERROR"
        )

        videocon_debug(
            "OUTPUT =",
            choice,
            level="ERROR"
        )

        videocon_debug(
            "MESSAGE ID =",
            job.get("message_id"),
            level="ERROR"
        )

        videocon_debug(
            "CHAT ID =",
            job.get("chat_id"),
            level="ERROR"
        )

        videocon_debug(
            "FILE ID =",
            job.get("file_id"),
            level="ERROR"
        )

        videocon_debug(
            "ERROR TYPE =",
            type(e).__name__,
            level="ERROR"
        )

        videocon_debug(
            "ERROR =",
            repr(e),
            level="ERROR"
        )

        videocon_debug(
            "TRACEBACK:",
            level="ERROR"
        )

        videocon_debug(
            traceback.format_exc(),
            level="ERROR"
        )


        # ----------------------------------------------------
        # SYSTEM AT FAILURE
        # ----------------------------------------------------

        videocon_debug(
            "SYSTEM STATE AT FAILURE:",
            level="ERROR"
        )

        videocon_system_debug()


        # ----------------------------------------------------
        # FILE STATE AT FAILURE
        # ----------------------------------------------------

        try:

            videocon_debug(
                "TEMP DIR =",
                temp_dir,
                level="ERROR"
            )

            videocon_debug(
                "INPUT FILE =",
                input_file,
                level="ERROR"
            )


            if input_file:

                videocon_debug(
                    "INPUT EXISTS =",
                    os.path.exists(
                        input_file
                    ),
                    level="ERROR"
                )


                if os.path.exists(
                    input_file
                ):

                    videocon_debug(
                        "INPUT SIZE =",
                        videocon_size(
                            os.path.getsize(
                                input_file
                            )
                        ),
                        level="ERROR"
                    )

        except Exception as file_debug_error:

            videocon_debug(
                "FAILURE FILE DEBUG ERROR =",
                repr(file_debug_error),
                level="ERROR"
            )


        # ----------------------------------------------------
        # USER ERROR
        # ----------------------------------------------------

        try:

            safe_error = (
                str(e)
            )


            if len(safe_error) > 1500:

                safe_error = (
                    safe_error[:1500]
                    +
                    "..."
                )


            if status_message:

                videocon_edit_status(

                    status_message,

                    (
                        "❌ <b>CONVERSION ERROR</b>\n\n"

                        f"<code>"
                        f"{safe_error}"
                        f"</code>\n\n"

                        "🔎 An aika cikakken "
                        "error zuwa Admin Telegram "
                        "da Render Logs."
                    )

                )

            else:

                bot.send_message(

                    user_id,

                    (
                        "❌ <b>Conversion Error</b>\n\n"

                        f"<code>"
                        f"{safe_error}"
                        f"</code>"
                    ),

                    parse_mode="HTML"

                )


        except Exception as notify_error:

            videocon_debug(
                "ERROR NOTIFICATION FAILED =",
                repr(notify_error),
                level="ERROR"
            )


        videocon_debug(
            "================================",
            level="ERROR"
        )


    # ========================================================
    # CLEANUP
    # ========================================================

    finally:

        videocon_debug(
            "================================"
        )

        videocon_debug(
            "CLEANUP STARTED"
        )


        try:

            if (

                temp_dir

                and

                os.path.exists(
                    temp_dir
                )

            ):

                videocon_debug(
                    "Deleting temp directory...",
                    temp_dir
                )


                shutil.rmtree(

                    temp_dir,

                    ignore_errors=True

                )


                videocon_debug(
                    "🧹 TEMP FILES DELETED."
                )


            else:

                videocon_debug(
                    "NO TEMP DIRECTORY TO DELETE."
                )


        except Exception as cleanup_error:

            videocon_debug_exception(
                "CLEANUP ERROR",
                cleanup_error
            )


        # ----------------------------------------------------
        # JOB STATE
        # ----------------------------------------------------

        try:

            _videocon_jobs.pop(
                user_id,
                None
            )

            _videocon_waiting.discard(
                user_id
            )


            videocon_debug(
                "JOB STATE REMOVED."
            )


        except Exception as state_error:

            videocon_debug_exception(
                "JOB STATE CLEANUP ERROR",
                state_error
            )


        # ----------------------------------------------------
        # FINAL DISK
        # ----------------------------------------------------

        try:

            total, used, free = (
                diskutil.disk_usage(
                    os.getcwd()
                )
            )


            videocon_debug(
                "FINAL DISK FREE =",
                videocon_size(free)
            )

        except Exception as e:

            videocon_debug(
                "FINAL DISK CHECK ERROR =",
                repr(e),
                level="WARNING"
            )


        videocon_debug(
            "VIDEOCON SESSION CLOSED."
        )

        videocon_debug(
            "================================"
        )


# ============================================================
# PYROGRAM: GET MESSAGE
# ============================================================

async def _videocon_get_message(

    chat_id,

    message_id

):

    videocon_debug(
        "================================"
    )

    videocon_debug(
        "PYROGRAM get_messages() START"
    )

    videocon_debug(
        "CHAT ID =",
        chat_id
    )

    videocon_debug(
        "MESSAGE ID =",
        message_id
    )


    try:

        message = (
            await
            _videocon_pyro.get_messages(

                chat_id,

                message_id

            )
        )


        videocon_debug(
            "PYROGRAM get_messages() SUCCESS"
        )


        if message:

            videocon_debug(
                "RETURNED MESSAGE ID =",
                getattr(
                    message,
                    "id",
                    None
                )
            )

        else:

            videocon_debug(
                "get_messages() returned NONE",
                level="ERROR"
            )


        return message


    except Exception as e:

        videocon_debug_exception(
            "PYROGRAM get_messages() ERROR",
            e
        )

        raise


# ============================================================
# PYROGRAM: DOWNLOAD
# ============================================================

async def _videocon_download(

    message,

    output_path,

    status_message,

    progress_state

):

    videocon_debug(
        "================================"
    )

    videocon_debug(
        "PYROGRAM download_media() START"
    )

    videocon_debug(
        "OUTPUT PATH =",
        output_path
    )


    async def progress(
        current,
        total
    ):

        await videocon_transfer_progress(

            current,

            total,

            status_message,

            "download",

            progress_state

        )


    try:

        result = (
            await
            _videocon_pyro.download_media(

                message,

                file_name=output_path,

                progress=progress

            )
        )


        videocon_debug(
            "PYROGRAM download_media() RETURNED =",
            result
        )


        return result


    except Exception as e:

        videocon_debug_exception(
            "PYROGRAM DOWNLOAD ERROR",
            e
        )

        raise


# ============================================================
# PYROGRAM: UPLOAD DOCUMENT
# ============================================================

async def _videocon_upload_document(

    user_id,

    input_file,

    original_name,

    file_size,

    status_message,

    progress_state

):

    videocon_debug(
        "================================"
    )

    videocon_debug(
        "PYROGRAM send_document() START"
    )

    videocon_debug(
        "USER ID =",
        user_id
    )

    videocon_debug(
        "FILE =",
        input_file
    )

    videocon_debug(
        "FILE SIZE =",
        videocon_size(file_size)
    )


    async def progress(
        current,
        total
    ):

        await videocon_transfer_progress(

            current,

            total,

            status_message,

            "upload",

            progress_state

        )


    try:

        result = (
            await
            _videocon_pyro.send_document(

                chat_id=user_id,

                document=input_file,

                caption=(

                    "✅ <b>File Converted</b>\n\n"

                    f"📄 Name: "
                    f"<b>{original_name}</b>\n\n"

                    f"📦 Size: "
                    f"<b>"
                    f"{videocon_size(file_size)}"
                    f"</b>\n\n"

                    "📁 Video Converter"

                ),

                progress=progress

            )
        )


        videocon_debug(
            "PYROGRAM send_document() SUCCESS"
        )


        return result


    except Exception as e:

        videocon_debug_exception(
            "PYROGRAM DOCUMENT UPLOAD ERROR",
            e
        )

        raise


# ============================================================
# PYROGRAM: UPLOAD VIDEO
# ============================================================

async def _videocon_upload_video(

    user_id,

    input_file,

    original_name,

    file_size,

    status_message,

    progress_state

):

    videocon_debug(
        "================================"
    )

    videocon_debug(
        "PYROGRAM send_video() START"
    )

    videocon_debug(
        "USER ID =",
        user_id
    )

    videocon_debug(
        "FILE =",
        input_file
    )

    videocon_debug(
        "FILE SIZE =",
        videocon_size(file_size)
    )


    async def progress(
        current,
        total
    ):

        await videocon_transfer_progress(

            current,

            total,

            status_message,

            "upload",

            progress_state

        )


    try:

        result = (
            await
            _videocon_pyro.send_video(

                chat_id=user_id,

                video=input_file,

                caption=(

                    "✅ <b>Video Converted</b>\n\n"

                    f"📦 Size: "
                    f"<b>"
                    f"{videocon_size(file_size)}"
                    f"</b>\n\n"

                    "🎬 Video Converter"

                ),

                supports_streaming=True,

                progress=progress

            )
        )


        videocon_debug(
            "PYROGRAM send_video() SUCCESS"
        )


        return result


    except Exception as e:

        videocon_debug_exception(
            "PYROGRAM VIDEO UPLOAD ERROR",
            e
        )

        raise


# ============================================================
# GLOBAL THREAD EXCEPTION HOOK
# ============================================================

def _videocon_thread_exception_hook(
    args
):

    try:

        videocon_debug(
            "================================",
            level="ERROR"
        )

        videocon_debug(
            "UNCAUGHT THREAD EXCEPTION",
            level="ERROR"
        )

        videocon_debug(
            "THREAD =",
            getattr(
                args.thread,
                "name",
                None
            ),
            level="ERROR"
        )

        videocon_debug(
            "EXCEPTION TYPE =",
            getattr(
                args.exc_type,
                "__name__",
                None
            ),
            level="ERROR"
        )

        videocon_debug(
            "EXCEPTION =",
            repr(
                args.exc_value
            ),
            level="ERROR"
        )

        videocon_debug(
            "".join(
                traceback.format_exception(
                    args.exc_type,
                    args.exc_value,
                    args.exc_traceback
                )
            ),
            level="ERROR"
        )

    except Exception:

        pass


try:

    threading.excepthook = (
        _videocon_thread_exception_hook
    )

    videocon_debug(
        "Global threading exception hook installed."
    )

except Exception as e:

    videocon_debug(
        "Could not install threading exception hook:",
        repr(e),
        level="WARNING"
    )


# ============================================================
# INITIAL SYSTEM DEBUG
# ============================================================

try:

    start_videocon_debug_sender()

except Exception:

    pass


try:

    videocon_debug(
        "================================"
    )

    videocon_debug(
        "VIDEOCON MODULE LOADED"
    )

    videocon_debug(
        "MAX FILE SIZE =",
        VIDEOCON_MAX_GB,
        "GB"
    )

    videocon_system_debug()

    videocon_debug(
        "================================"
    )

except Exception as e:

    try:

        print(
            "VIDEOCON INITIAL DEBUG ERROR:",
            repr(e),
            flush=True
        )

    except Exception:

        pass


# ============================================================
# START PYROGRAM ENGINE IMMEDIATELY
# ============================================================

try:

    videocon_debug(
        "Starting Videocon Pyrogram engine immediately..."
    )

    start_videocon_engine()

    videocon_debug(
        "Initial Pyrogram engine start command completed."
    )

except Exception as e:

    videocon_debug_exception(
        "INITIAL VIDEOCON ENGINE START FAILED",
        e
    )







#END ============================================================
# PYROGRAM
# ============================================================

# Wannan client din zai yi:
# - Telegram MTProto download
# - Telegram upload
#
# Session din zai kasance a /tmp saboda Render filesystem
# ba persistent bane a wannan setup.

PYROGRAM_SESSION = "/tmp/vd_pyrogram_session"

app = Client(
    name=PYROGRAM_SESSION,
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="/tmp"
)


# ============================================================
# TEMP STORAGE
# ============================================================

BASE_TEMP_DIR = Path(tempfile.gettempdir()) / "telegram_vd"

BASE_TEMP_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# USER STATES
# ============================================================

# user_id -> state
#
# None
# waiting_file
# waiting_format
#
user_states = {}

# user_id -> job information
active_jobs = {}

state_lock = threading.Lock()


# ============================================================
# TEXT
# ============================================================

WELCOME_TEXT = (
    "🎬 <b>Video / File Converter</b>\n\n"
    "Turo min file ko video da kake son mayarwa.\n\n"
    "Bayan na karba zan tambaye ka:\n"
    "📁 File ko 📺 Video."
)

WAITING_TEXT = (
    "📥 <b>Ina jira file ɗinka...</b>\n\n"
    "Turo min:\n"
    "• 📄 Document/File\n"
    "• 🎬 Video"
)

PROCESSING_TEXT = (
    "⏳ <b>Aiki yana tafiya...</b>\n\n"
    "Ana sauke file ɗinka sannan ana sake tura shi.\n"
    "Don Allah ka jira."
)

INVALID_TEXT = (
    "❌ Wannan ba file/video bane.\n\n"
    "Ka turo min Document ko Video."
)

import shutil
import subprocess

print("========== FFMPEG CHECK ==========")

ffmpeg_path = shutil.which("ffmpeg")
ffprobe_path = shutil.which("ffprobe")

print("FFmpeg path:", ffmpeg_path)
print("FFprobe path:", ffprobe_path)

if ffmpeg_path:
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        print(result.stdout.splitlines()[0])
        print("✅ FFmpeg yana nan.")
    except Exception as e:
        print("❌ FFmpeg yana nan amma an samu error:", e)
else:
    print("❌ FFmpeg BA ya nan.")

if ffprobe_path:
    try:
        result = subprocess.run(
            ["ffprobe", "-version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        print(result.stdout.splitlines()[0])
        print("✅ FFprobe yana nan.")
    except Exception as e:
        print("❌ FFprobe yana nan amma an samu error:", e)
else:
    print("❌ FFprobe BA ya nan.")

print("===================================")


# ============================================================
# /COMP — FAST ADMIN VIDEO COMPRESSOR
# ============================================================
#
# FLOW:
#
# /comp
#    ↓
# Admin sends VIDEO
#    ↓
# Bot reads REAL file size
#    ↓
# Bot reads duration
#    ↓
# Target buttons
#    ↓
# Admin chooses target
#    ↓
# Download
#    ↓
# FAST 1-PASS FFMPEG COMPRESSION
#    ↓
# Upload compressed video
#    ↓
# Delete ALL temporary files
#
# FEATURES:
# - ADMIN ONLY
# - 1-PASS = FASTER
# - veryfast preset
# - Telegram status edit every 20 seconds
# - Download status
# - Compression progress
# - Upload status
# - Automatic cleanup
# - Does NOT touch database
# - Does NOT touch movie delivery system
#
# TEST SIZE:
#   Maximum input = 20 MB
#
# RENDER:
#   Works with FFmpeg installed on server.
# ============================================================


import os
import time
import shutil
import tempfile
import subprocess
import threading

from telebot import types
from telebot.apihelper import ApiTelegramException


# ============================================================
# CONFIG
# ============================================================

COMP_MAX_INPUT_MB = 20

COMP_MAX_INPUT_BYTES = (
    COMP_MAX_INPUT_MB * 1024 * 1024
)

# Safety limit for output
COMP_MAX_OUTPUT_MB = 50

COMP_MAX_OUTPUT_BYTES = (
    COMP_MAX_OUTPUT_MB * 1024 * 1024
)

# Telegram message edit interval
COMP_PROGRESS_INTERVAL = 20

# ============================================================
# SPEED SETTINGS
# ============================================================

# IMPORTANT:
# veryfast = much faster than fast
#
# If you want maximum speed later:
# "ultrafast"
#
# For now:
# veryfast
#
COMP_PRESET = "veryfast"

# Threads
COMP_THREADS = "0"

# CRF is NOT used here because we calculate
# bitrate from target size.
#
# Target-size compression uses:
# - video bitrate
# - audio bitrate


# ============================================================
# AUDIO BITRATE
# ============================================================

# Normal audio
COMP_AUDIO_NORMAL = 48_000

# Small target audio
COMP_AUDIO_LOW = 32_000


# ============================================================
# SESSION STATE
# ============================================================

_comp_waiting_for_video = set()

_comp_pending_jobs = {}

_comp_lock = threading.Lock()

_comp_running = False


# ============================================================
# SIZE FORMAT
# ============================================================

def _comp_format_size(size_bytes):

    try:

        size_bytes = float(size_bytes)

        if size_bytes < 1024:

            return f"{size_bytes:.0f} B"

        if size_bytes < 1024 * 1024:

            return (
                f"{size_bytes / 1024:.2f} KB"
            )

        if size_bytes < 1024 * 1024 * 1024:

            return (
                f"{size_bytes / (1024 * 1024):.2f} MB"
            )

        return (
            f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
        )

    except Exception:

        return "Unknown"


# ============================================================
# TIME FORMAT
# ============================================================

def _comp_format_time(seconds):

    try:

        seconds = int(max(0, seconds))

        hours = seconds // 3600

        minutes = (
            (seconds % 3600) // 60
        )

        secs = seconds % 60

        if hours:

            return (
                f"{hours:02d}:"
                f"{minutes:02d}:"
                f"{secs:02d}"
            )

        return (
            f"{minutes:02d}:"
            f"{secs:02d}"
        )

    except Exception:

        return "00:00"


# ============================================================
# FFMPEG CHECK
# ============================================================

def _comp_check_ffmpeg():

    ffmpeg = shutil.which("ffmpeg")

    ffprobe = shutil.which("ffprobe")

    if not ffmpeg:

        raise RuntimeError(
            "FFmpeg ba a samu a server ba."
        )

    if not ffprobe:

        raise RuntimeError(
            "FFprobe ba a samu a server ba."
        )

    return ffmpeg, ffprobe


# ============================================================
# GET VIDEO DURATION
# ============================================================

def _comp_get_duration(
    ffprobe,
    input_file
):

    try:

        result = subprocess.run(

            [
                ffprobe,

                "-v",
                "error",

                "-show_entries",
                "format=duration",

                "-of",
                "default="
                "noprint_wrappers=1:"
                "nokey=1",

                input_file
            ],

            stdout=subprocess.PIPE,

            stderr=subprocess.PIPE,

            text=True,

            timeout=60
        )

        if result.returncode != 0:

            return 0.0

        value = result.stdout.strip()

        if not value:

            return 0.0

        return float(value)

    except Exception as e:

        print(
            "COMP duration error:",
            repr(e)
        )

        return 0.0


# ============================================================
# SAFE TELEGRAM EDIT
#
# Ba zai edit fiye da sau daya cikin 20 seconds ba.
# ============================================================

def _comp_safe_edit(
    chat_id,
    message_id,
    text,
    state
):

    now = time.monotonic()

    with state["edit_lock"]:

        last_edit = state.get(
            "last_edit",
            0.0
        )

        if (
            now - last_edit
            < COMP_PROGRESS_INTERVAL
        ):

            return False

        try:

            bot.edit_message_text(

                chat_id=chat_id,

                message_id=message_id,

                text=text,

                parse_mode="HTML"
            )

            state["last_edit"] = now

            return True

        except ApiTelegramException as e:

            error_text = str(e).lower()

            if (
                "message is not modified"
                in error_text
            ):

                return False

            print(
                "COMP Telegram edit error:",
                repr(e)
            )

            return False

        except Exception as e:

            print(
                "COMP edit error:",
                repr(e)
            )

            return False


# ============================================================
# FORCE EDIT
#
# Ana amfani da shi idan muna son canza stage nan take.
# Misali:
# Download complete
# Compression complete
# Uploading
# Completed
# ============================================================

def _comp_force_edit(
    chat_id,
    message_id,
    text,
    state
):

    try:

        bot.edit_message_text(

            chat_id=chat_id,

            message_id=message_id,

            text=text,

            parse_mode="HTML"
        )

        with state["edit_lock"]:

            state["last_edit"] = (
                time.monotonic()
            )

        return True

    except ApiTelegramException as e:

        if (
            "message is not modified"
            in str(e).lower()
        ):

            return False

        print(
            "COMP force edit Telegram error:",
            repr(e)
        )

        return False

    except Exception as e:

        print(
            "COMP force edit error:",
            repr(e)
        )

        return False


# ============================================================
# HEARTBEAT
#
# Wannan yana nuna cewa aiki yana gudana.
# Edit = maximum once every 20 seconds.
# ============================================================

def _comp_heartbeat(
    chat_id,
    message_id,
    state,
    stop_event,
    stage_text
):

    started = time.monotonic()

    while not stop_event.wait(1):

        elapsed = int(
            time.monotonic() - started
        )

        if (
            elapsed
            < COMP_PROGRESS_INTERVAL
        ):

            continue

        text = (

            f"{stage_text}\n\n"

            f"⏱️ Time: "
            f"<b>{_comp_format_time(elapsed)}</b>\n\n"

            "⏳ Please wait..."
        )

        _comp_safe_edit(

            chat_id,

            message_id,

            text,

            state
        )


# ============================================================
# TARGET OPTIONS
#
# Misali:
#
# Original = 12.05 MB
#
# 15% ≈ 1 MB
# 45% ≈ 5 MB
# 65% ≈ 7 MB
#
# Original = 14 MB
#
# ≈ 2 MB
# ≈ 6 MB
# ≈ 9 MB
# ============================================================

def _comp_target_options(
    original_mb
):

    values = []

    ratios = [
        0.15,
        0.45,
        0.65
    ]

    for ratio in ratios:

        target = max(
            1,
            int(
                original_mb * ratio
            )
        )

        # Target must be smaller
        if target >= original_mb:

            target = max(
                1,
                int(original_mb) - 1
            )

        if target not in values:

            values.append(target)

    # Sort small -> large
    values.sort()

    return values


# ============================================================
# CALCULATE BITRATES
#
# Wannan shine babban bangaren target-size system.
#
# 1-pass ne.
# Ba zai yi pass 1 + pass 2 ba.
#
# Wannan yana sa compression ya fi sauri sosai.
# ============================================================

def _comp_calculate_bitrates(
    target_mb,
    duration
):

    if duration <= 0:

        raise RuntimeError(
            "An kasa gano duration na video."
        )

    target_bytes = (
        target_mb
        * 1024
        * 1024
    )

    target_bits = (
        target_bytes
        * 8
    )

    # Leave room for MP4 overhead
    total_bitrate = (
        target_bits
        / duration
        * 0.90
    )

    # Very small target:
    # use lower audio bitrate.
    if total_bitrate < 160_000:

        audio_bitrate = (
            COMP_AUDIO_LOW
        )

    else:

        audio_bitrate = (
            COMP_AUDIO_NORMAL
        )

    video_bitrate = (
        total_bitrate
        - audio_bitrate
    )

    # Minimum video bitrate.
    if video_bitrate < 24_000:

        # Try very low audio
        audio_bitrate = 24_000

        video_bitrate = (
            total_bitrate
            - audio_bitrate
        )

    if video_bitrate < 16_000:

        raise RuntimeError(

            "Target size ya yi ƙanƙanta "
            "ga tsawon wannan video."
        )

    return (
        int(video_bitrate),
        int(audio_bitrate)
    )


# ============================================================
# RUN FAST 1-PASS FFMPEG
# ============================================================

def _comp_run_ffmpeg(

    ffmpeg,

    input_file,

    output_file,

    duration,

    target_mb,

    status_message,

    state
):

    (
        video_bitrate,
        audio_bitrate
    ) = _comp_calculate_bitrates(

        target_mb,

        duration
    )

    video_kbps = max(
        16,
        int(
            video_bitrate / 1000
        )
    )

    audio_kbps = max(
        24,
        int(
            audio_bitrate / 1000
        )
    )

    # ========================================================
    # FFMPEG COMMAND
    #
    # 1 PASS
    # veryfast
    # threads 0 = FFmpeg chooses available threads
    # ========================================================

    command = [

        ffmpeg,

        "-y",

        "-i",
        input_file,

        # Video stream
        "-map",
        "0:v:0",

        # Audio if available
        "-map",
        "0:a?",

        # H264
        "-c:v",
        "libx264",

        # FAST
        "-preset",
        COMP_PRESET,

        # Target bitrate
        "-b:v",
        f"{video_kbps}k",

        # Prevent huge bitrate spikes
        "-maxrate",
        f"{video_kbps}k",

        "-bufsize",
        f"{video_kbps * 2}k",

        # Audio
        "-c:a",
        "aac",

        "-b:a",
        f"{audio_kbps}k",

        # MP4 compatibility
        "-pix_fmt",
        "yuv420p",

        "-movflags",
        "+faststart",

        # Use all available FFmpeg threads
        "-threads",
        COMP_THREADS,

        # Progress
        "-progress",
        "pipe:1",

        "-nostats",

        output_file
    ]

    print(
        "================================================"
    )

    print(
        "COMP FFMPEG 1-PASS:"
    )

    print(
        " ".join(command)
    )

    print(
        "================================================"
    )

    # ========================================================
    # START PROCESS
    # ========================================================

    process = subprocess.Popen(

        command,

        stdout=subprocess.PIPE,

        stderr=subprocess.PIPE,

        text=True,

        bufsize=1
    )

    current_time = 0.0

    last_edit = time.monotonic()

    try:

        while True:

            line = (
                process.stdout.readline()
            )

            if not line:

                if (
                    process.poll()
                    is not None
                ):

                    break

                time.sleep(0.2)

                continue

            line = line.strip()

            # =================================================
            # READ CURRENT VIDEO TIME
            # =================================================

            if line.startswith(
                "out_time_ms="
            ):

                try:

                    current_time = (

                        int(
                            line.split(
                                "=",
                                1
                            )[1]
                        )

                        / 1_000_000
                    )

                except Exception:

                    pass

            # =================================================
            # TELEGRAM UPDATE EVERY 20 SEC
            # =================================================

            now = time.monotonic()

            if (
                now - last_edit
                >= COMP_PROGRESS_INTERVAL
            ):

                if duration > 0:

                    percent = min(

                        99,

                        int(

                            (
                                current_time
                                / duration
                            )
                            * 100
                        )
                    )

                else:

                    percent = 0

                progress_text = (

                    "⚙️ "
                    "<b>Compressing video...</b>\n\n"

                    f"📊 Progress: "
                    f"<b>{percent}%</b>\n"

                    f"⏱️ "
                    f"<b>"
                    f"{_comp_format_time(current_time)}"
                    f"</b>"
                    " / "
                    f"<b>"
                    f"{_comp_format_time(duration)}"
                    f"</b>\n\n"

                    f"🎯 Target: "
                    f"<b>{target_mb} MB</b>\n"

                    f"🚀 Mode: "
                    f"<b>FAST 1-PASS</b>\n"

                    f"🔧 Preset: "
                    f"<b>{COMP_PRESET}</b>\n\n"

                    "⏳ Please wait..."
                )

                _comp_force_edit(

                    status_message.chat.id,

                    status_message.message_id,

                    progress_text,

                    state
                )

                last_edit = now

        # ====================================================
        # WAIT
        # ====================================================

        return_code = process.wait()

        stderr_output = ""

        try:

            stderr_output = (
                process.stderr.read()
            )

        except Exception:

            pass

        if return_code != 0:

            print(
                "❌ FFmpeg ERROR:"
            )

            print(
                stderr_output[-10000:]
            )

            raise RuntimeError(
                "FFmpeg compression failed."
            )

        return True

    except Exception:

        try:

            process.kill()

        except Exception:

            pass

        raise


# ============================================================
# /COMP COMMAND
# ============================================================

@bot.message_handler(
    commands=["comp"]
)
def comp_command(message):

    global _comp_running

    user_id = (
        message.from_user.id
    )

    # ========================================================
    # ADMIN ONLY
    # ========================================================

    if user_id != ADMIN_ID:

        try:

            bot.reply_to(

                message,

                "❌ Wannan command ɗin "
                "na Admin ne kawai."
            )

        except Exception:

            pass

        return

    # ========================================================
    # CHECK FFMPEG
    # ========================================================

    try:

        _comp_check_ffmpeg()

    except Exception as e:

        bot.reply_to(

            message,

            (
                "❌ "
                "<b>FFmpeg ba a shirya ba.</b>\n\n"

                f"<code>{str(e)}</code>"
            ),

            parse_mode="HTML"
        )

        return

    # ========================================================
    # CHECK RUNNING
    # ========================================================

    with _comp_lock:

        if _comp_running:

            bot.reply_to(

                message,

                (
                    "⚠️ "
                    "<b>Compression yana gudana yanzu.</b>\n\n"

                    "Da fatan ka jira ya gama "
                    "kafin ka fara wani."
                ),

                parse_mode="HTML"
            )

            return

    # ========================================================
    # WAIT FOR VIDEO
    # ========================================================

    _comp_waiting_for_video.add(
        user_id
    )

    bot.reply_to(

        message,

        (
            "🎬 <b>VIDEO COMPRESSOR</b>\n\n"

            "Turo min video ɗin da kake "
            "son mu compress.\n\n"

            f"📦 Maximum: "
            f"<b>{COMP_MAX_INPUT_MB} MB</b>\n\n"

            "⏳ Bayan ka turo shi zan karanta "
            "ainihin size ɗinsa sannan "
            "zan baka zabukan size."
        ),

        parse_mode="HTML"
    )


# ============================================================
# RECEIVE VIDEO
# ============================================================
#
# IMPORTANT:
# Idan kana da wani generic:
#
# @bot.message_handler(content_types=["video"])
#
# wannan handler ɗin ya kamata ya zo
# kafin generic video handler ɗinka.
# ============================================================

@bot.message_handler(
    content_types=["video"],
    func=lambda message:
        message.from_user.id
        in _comp_waiting_for_video
)
def comp_receive_video(message):

    user_id = (
        message.from_user.id
    )

    # ========================================================
    # ADMIN ONLY
    # ========================================================

    if user_id != ADMIN_ID:

        _comp_waiting_for_video.discard(
            user_id
        )

        return

    # ========================================================
    # STOP WAITING
    # ========================================================

    _comp_waiting_for_video.discard(
        user_id
    )

    # ========================================================
    # GET FILE INFO
    # ========================================================

    try:

        file_info = bot.get_file(

            message.video.file_id

        )

    except Exception as e:

        bot.reply_to(

            message,

            (
                "❌ An kasa karɓar bayanan video.\n\n"

                f"<code>{str(e)}</code>"
            ),

            parse_mode="HTML"
        )

        return

    # ========================================================
    # REAL SIZE
    # ========================================================

    file_size = (

        getattr(
            file_info,
            "file_size",
            None
        )

        or

        getattr(
            message.video,
            "file_size",
            None
        )
    )

    if not file_size:

        bot.reply_to(

            message,

            "❌ An kasa gano ainihin "
            "girman video."
        )

        return

    # ========================================================
    # MAX INPUT
    # ========================================================

    if (
        file_size
        > COMP_MAX_INPUT_BYTES
    ):

        bot.reply_to(

            message,

            (
                "❌ <b>Video ya yi girma.</b>\n\n"

                f"📦 Video: "
                f"<b>"
                f"{_comp_format_size(file_size)}"
                f"</b>\n"

                f"📦 Maximum: "
                f"<b>"
                f"{COMP_MAX_INPUT_MB} MB"
                f"</b>"
            ),

            parse_mode="HTML"
        )

        return

    # ========================================================
    # ORIGINAL MB
    # ========================================================

    original_mb = (

        file_size
        / (1024 * 1024)
    )

    # ========================================================
    # TELEGRAM DURATION
    # ========================================================

    duration = (

        getattr(
            message.video,
            "duration",
            0
        )

        or 0
    )

    # ========================================================
    # TARGETS
    # ========================================================

    targets = _comp_target_options(
        original_mb
    )

    if not targets:

        bot.reply_to(

            message,

            "❌ Babu target size "
            "mai kyau."
        )

        return

    # ========================================================
    # SAVE JOB
    # ========================================================

    _comp_pending_jobs[user_id] = {

        "file_id":
            message.video.file_id,

        "file_size":
            file_size,

        "original_mb":
            original_mb,

        "duration":
            duration,

        "message_id":
            message.message_id
    }

    # ========================================================
    # BUTTON KEYBOARD
    # ========================================================

    keyboard = (
        types.InlineKeyboardMarkup(
            row_width=3
        )
    )

    buttons = []

    for target in targets:

        buttons.append(

            types.InlineKeyboardButton(

                f"📦 {target} MB",

                callback_data=(
                    f"comp_target:{target}"
                )
            )
        )

    keyboard.add(
        *buttons
    )

    # ========================================================
    # ASK ADMIN
    # ========================================================

    bot.reply_to(

        message,

        (
            "🎬 <b>VIDEO RECEIVED</b>\n\n"

            f"📦 Original size: "
            f"<b>"
            f"{_comp_format_size(file_size)}"
            f"</b>\n"

            f"⏱️ Duration: "
            f"<b>"
            f"{_comp_format_time(duration)}"
            f"</b>\n\n"

            "❓ <b>MB nawa kake son "
            "video ɗin ya koma?</b>\n\n"

            "Zaɓi ɗaya daga cikin buttons:"
        ),

        reply_markup=keyboard,

        parse_mode="HTML"
    )


# ============================================================
# TARGET BUTTON
# ============================================================

@bot.callback_query_handler(
    func=lambda call:
        call.data.startswith(
            "comp_target:"
        )
)
def comp_target_callback(call):

    global _comp_running

    user_id = (
        call.from_user.id
    )

    # ========================================================
    # ADMIN ONLY
    # ========================================================

    if user_id != ADMIN_ID:

        bot.answer_callback_query(

            call.id,

            "❌ Admin kawai."
        )

        return

    # ========================================================
    # TARGET
    # ========================================================

    try:

        target_mb = int(

            call.data.split(
                ":",
                1
            )[1]
        )

    except Exception:

        bot.answer_callback_query(

            call.id,

            "❌ Invalid target."
        )

        return

    # ========================================================
    # GET JOB
    # ========================================================

    job = _comp_pending_jobs.get(
        user_id
    )

    if not job:

        bot.answer_callback_query(

            call.id,

            "❌ Compression session ta ƙare."
        )

        return

    # ========================================================
    # PREVENT TWO JOBS
    # ========================================================

    with _comp_lock:

        if _comp_running:

            bot.answer_callback_query(

                call.id,

                "Compression yana gudana."
            )

            return

        _comp_running = True

    # ========================================================
    # REMOVE PENDING JOB
    # ========================================================

    _comp_pending_jobs.pop(
        user_id,
        None
    )

    # ========================================================
    # BUTTON RESPONSE
    # ========================================================

    bot.answer_callback_query(

        call.id,

        "🚀 Starting compression..."
    )

    # ========================================================
    # START WORKER
    # ========================================================

    worker = threading.Thread(

        target=_comp_process_job,

        args=(

            call,

            job,

            target_mb
        ),

        daemon=True
    )

    worker.start()


# ============================================================
# MAIN JOB
# ============================================================

def _comp_process_job(

    call,

    job,

    target_mb
):

    global _comp_running

    user_id = (
        call.from_user.id
    )

    temp_dir = None

    input_file = None

    output_file = None

    status_message = None

    heartbeat_stop = None

    heartbeat_thread = None

    # ========================================================
    # EDIT STATE
    # ========================================================

    state = {

        "last_edit":
            0.0,

        "edit_lock":
            threading.Lock()
    }

    try:

        # ====================================================
        # TEMP DIRECTORY
        # ====================================================

        temp_dir = tempfile.mkdtemp(
            prefix="telegram_comp_"
        )

        input_file = os.path.join(

            temp_dir,

            "input.mp4"
        )

        output_file = os.path.join(

            temp_dir,

            "compressed.mp4"
        )

        # ====================================================
        # INITIAL MESSAGE
        # ====================================================

        status_message = bot.send_message(

            user_id,

            (
                "⏳ <b>Preparing compressor...</b>\n\n"

                f"📦 Original: "
                f"<b>"
                f"{job['original_mb']:.2f} MB"
                f"</b>\n"

                f"🎯 Target: "
                f"<b>"
                f"{target_mb} MB"
                f"</b>\n\n"

                "🔧 Checking FFmpeg..."
            ),

            parse_mode="HTML"
        )

        # ====================================================
        # CHECK FFMPEG
        # ====================================================

        ffmpeg, ffprobe = (
            _comp_check_ffmpeg()
        )

        # ====================================================
        # DOWNLOAD START
        # ====================================================

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "⬇️ <b>DOWNLOADING VIDEO...</b>\n\n"

                f"📦 Size: "
                f"<b>"
                f"{job['original_mb']:.2f} MB"
                f"</b>\n\n"

                "⏳ Please wait..."
            ),

            state
        )

        # ====================================================
        # DOWNLOAD HEARTBEAT
        # ====================================================

        heartbeat_stop = (
            threading.Event()
        )

        heartbeat_thread = (
            threading.Thread(

                target=_comp_heartbeat,

                args=(

                    user_id,

                    status_message.message_id,

                    state,

                    heartbeat_stop,

                    "⬇️ "
                    "<b>DOWNLOADING VIDEO...</b>"
                ),

                daemon=True
            )
        )

        heartbeat_thread.start()

        # ====================================================
        # TELEGRAM DOWNLOAD
        # ====================================================

        file_info = bot.get_file(
            job["file_id"]
        )

        file_data = bot.download_file(
            file_info.file_path
        )

        # ====================================================
        # SAVE INPUT
        # ====================================================

        with open(
            input_file,
            "wb"
        ) as f:

            f.write(file_data)

        # ====================================================
        # RELEASE DOWNLOAD MEMORY
        # ====================================================

        del file_data

        # Stop heartbeat
        heartbeat_stop.set()

        # ====================================================
        # VERIFY
        # ====================================================

        downloaded_size = os.path.getsize(
            input_file
        )

        if downloaded_size <= 0:

            raise RuntimeError(
                "Downloaded file empty ne."
            )

        # ====================================================
        # GET REAL DURATION
        # ====================================================

        duration = (
            _comp_get_duration(

                ffprobe,

                input_file
            )
        )

        if duration <= 0:

            duration = float(

                job.get(
                    "duration",
                    0
                )
            )

        if duration <= 0:

            raise RuntimeError(
                "An kasa gano duration."
            )

        # ====================================================
        # DOWNLOAD COMPLETE
        # ====================================================

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "✅ <b>DOWNLOAD COMPLETE</b>\n\n"

                f"📦 Downloaded: "
                f"<b>"
                f"{_comp_format_size(downloaded_size)}"
                f"</b>\n"

                f"⏱️ Duration: "
                f"<b>"
                f"{_comp_format_time(duration)}"
                f"</b>\n\n"

                f"🎯 Target: "
                f"<b>"
                f"{target_mb} MB"
                f"</b>\n\n"

                "⚙️ Preparing fast compression..."
            ),

            state
        )

        time.sleep(1)

        # ====================================================
        # COMPRESS
        # ====================================================

        _comp_run_ffmpeg(

            ffmpeg,

            input_file,

            output_file,

            duration,

            target_mb,

            status_message,

            state
        )

        # ====================================================
        # CHECK OUTPUT
        # ====================================================

        if not os.path.exists(
            output_file
        ):

            raise RuntimeError(
                "FFmpeg bai samar da output ba."
            )

        output_size = os.path.getsize(
            output_file
        )

        if output_size <= 0:

            raise RuntimeError(
                "Compressed file empty ne."
            )

        # ====================================================
        # OUTPUT SAFETY
        # ====================================================

        if (
            output_size
            > COMP_MAX_OUTPUT_BYTES
        ):

            raise RuntimeError(

                "Compressed video ya wuce "
                f"{COMP_MAX_OUTPUT_MB} MB."
            )

        # ====================================================
        # COMPRESSION COMPLETE
        # ====================================================

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "✅ <b>COMPRESSION COMPLETE</b>\n\n"

                f"📦 Original: "
                f"<b>"
                f"{_comp_format_size(downloaded_size)}"
                f"</b>\n"

                f"📦 Compressed: "
                f"<b>"
                f"{_comp_format_size(output_size)}"
                f"</b>\n"

                f"🎯 Target: "
                f"<b>"
                f"{target_mb} MB"
                f"</b>\n\n"

                "📤 <b>UPLOADING VIDEO...</b>\n\n"

                "⏳ Please wait..."
            ),

            state
        )

        # ====================================================
        # UPLOAD HEARTBEAT
        # ====================================================

        heartbeat_stop = (
            threading.Event()
        )

        heartbeat_thread = (
            threading.Thread(

                target=_comp_heartbeat,

                args=(

                    user_id,

                    status_message.message_id,

                    state,

                    heartbeat_stop,

                    "📤 "
                    "<b>UPLOADING VIDEO...</b>"
                ),

                daemon=True
            )
        )

        heartbeat_thread.start()

        # ====================================================
        # UPLOAD
        # ====================================================

        with open(
            output_file,
            "rb"
        ) as video_file:

            bot.send_video(

                user_id,

                video_file,

                caption=(

                    "✅ <b>Compression Completed</b>\n\n"

                    f"📦 Original: "
                    f"<b>"
                    f"{_comp_format_size(downloaded_size)}"
                    f"</b>\n"

                    f"📦 Compressed: "
                    f"<b>"
                    f"{_comp_format_size(output_size)}"
                    f"</b>\n"

                    f"🎯 Target: "
                    f"<b>"
                    f"{target_mb} MB"
                    f"</b>\n\n"

                    "🎬 Video Compressor"
                ),

                parse_mode="HTML",

                supports_streaming=True,

                timeout=600
            )

        # ====================================================
        # UPLOAD COMPLETE
        # ====================================================

        heartbeat_stop.set()

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "✅ <b>COMPRESSION COMPLETED</b>\n\n"

                f"📦 Original: "
                f"<b>"
                f"{_comp_format_size(downloaded_size)}"
                f"</b>\n"

                f"📦 Final: "
                f"<b>"
                f"{_comp_format_size(output_size)}"
                f"</b>\n"

                f"🎯 Target: "
                f"<b>"
                f"{target_mb} MB"
                f"</b>\n\n"

                "📤 Uploaded successfully ✅\n\n"

                "🧹 Temporary files za share."
            ),

            state
        )

        print(
            "✅ COMP COMPLETED:",
            user_id,
            _comp_format_size(
                downloaded_size
            ),
            "→",
            _comp_format_size(
                output_size
            )
        )

    # ========================================================
    # ERROR
    # ========================================================

    except Exception as e:

        print(
            "❌ COMP ERROR:",
            repr(e)
        )

        try:

            if status_message:

                _comp_force_edit(

                    user_id,

                    status_message.message_id,

                    (
                        "❌ "
                        "<b>COMPRESSION ERROR</b>\n\n"

                        f"<code>"
                        f"{str(e)}"
                        f"</code>\n\n"

                        "Da fatan ka sake gwadawa."
                    ),

                    state
                )

            else:

                bot.send_message(

                    user_id,

                    (
                        "❌ "
                        "<b>Compression Error</b>\n\n"

                        f"<code>"
                        f"{str(e)}"
                        f"</code>"
                    ),

                    parse_mode="HTML"
                )

        except Exception as notify_error:

            print(
                "COMP notification error:",
                repr(notify_error)
            )

    # ========================================================
    # ALWAYS CLEANUP
    # ========================================================

    finally:

        # ====================================================
        # STOP HEARTBEAT
        # ====================================================

        try:

            if heartbeat_stop:

                heartbeat_stop.set()

        except Exception:

            pass

        # ====================================================
        # DELETE TEMP DIRECTORY
        # ====================================================
        #
        # Wannan zai share:
        #
        # input.mp4
        # compressed.mp4
        # duk wani temporary file
        #
        # ====================================================

        try:

            if (
                temp_dir
                and os.path.exists(
                    temp_dir
                )
            ):

                shutil.rmtree(

                    temp_dir,

                    ignore_errors=True
                )

                print(
                    "🧹 COMP temp files deleted."
                )

        except Exception as cleanup_error:

            print(
                "COMP cleanup error:",
                repr(cleanup_error)
            )

        # ====================================================
        # RELEASE LOCK
        # ====================================================

        with _comp_lock:

            _comp_running = False

        print(
            "🔓 COMP lock released."
        )





# ============================================================
# HELPERS
# ============================================================

def set_state(user_id, state):
    with state_lock:
        user_states[user_id] = state


def get_state(user_id):
    with state_lock:
        return user_states.get(user_id)


def clear_state(user_id):
    with state_lock:
        user_states.pop(user_id, None)


def is_busy(user_id):
    with state_lock:
        return user_id in active_jobs


def mark_busy(user_id, data=None):
    with state_lock:
        active_jobs[user_id] = data or True


def unmark_busy(user_id):
    with state_lock:
        active_jobs.pop(user_id, None)


def format_size(size):
    if size is None:
        return "Unknown"

    size = float(size)

    if size < 1024:
        return f"{size:.0f} B"

    if size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"

    if size < 1024 ** 3:
        return f"{size / (1024 ** 2):.2f} MB"

    return f"{size / (1024 ** 3):.2f} GB"


def safe_filename(filename):
    if not filename:
        return "converted_file"

    filename = os.path.basename(filename)

    # Kada mu barin characters masu kawo matsala
    bad_chars = '<>:"/\\|?*'

    for char in bad_chars:
        filename = filename.replace(char, "_")

    return filename[:200]


def send_debug(text):
    logger.info(text)

    if ADMIN_ID:
        try:
            bot.send_message(
                ADMIN_ID,
                f"🛠 <b>VD DEBUG</b>\n\n{text}",
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning("Could not send debug to admin: %s", e)


def cleanup_folder(folder):
    try:
        if folder and os.path.exists(folder):
            shutil.rmtree(folder, ignore_errors=True)
    except Exception as e:
        logger.warning("Cleanup error: %s", e)


def get_format_keyboard():
    markup = types.InlineKeyboardMarkup(row_width=2)

    markup.add(
        types.InlineKeyboardButton(
            "📁 File",
            callback_data="vd_format:file"
        ),
        types.InlineKeyboardButton(
            "📺 Video",
            callback_data="vd_format:video"
        )
    )

    return markup


# ============================================================
# /START
# ============================================================

@bot.message_handler(commands=["start"])
def start_handler(message):

    user_id = message.from_user.id

    clear_state(user_id)

    bot.send_message(
        message.chat.id,
        "👋 Sannu!\n\n"
        "Yi amfani da:\n\n"
        "<code>/vd</code>\n\n"
        "domin amfani da File/Video Converter."
    )


# ============================================================
# /VD
# ============================================================

@bot.message_handler(commands=["vd"])
def vd_handler(message):

    user_id = message.from_user.id

    if is_busy(user_id):
        bot.send_message(
            message.chat.id,
            "⏳ Kana da wani file da ake processing yanzu.\n"
            "Da fatan za ka jira ya gama."
        )
        return

    set_state(user_id, "waiting_file")

    bot.send_message(
        message.chat.id,
        WELCOME_TEXT
    )

    bot.send_message(
        message.chat.id,
        WAITING_TEXT
    )


# ============================================================
# DOCUMENT RECEIVER
# ============================================================

@bot.message_handler(
    content_types=["document"],
    func=lambda message: get_state(message.from_user.id) == "waiting_file"
)
def document_handler(message):

    user_id = message.from_user.id

    if is_busy(user_id):
        bot.send_message(
            message.chat.id,
            "⏳ Ana processing wani file ɗinka yanzu."
        )
        return

    document = message.document

    filename = document.file_name or "file"

    file_size = document.file_size or 0

    send_debug(
        f"User: {user_id}\n"
        f"Type: DOCUMENT\n"
        f"Name: {filename}\n"
        f"Size: {format_size(file_size)}\n"
        f"File ID: {document.file_id}"
    )

    # Ajiye bayanin file
    with state_lock:
        user_states[user_id] = {
            "state": "waiting_format",
            "file_type": "document",
            "file_id": document.file_id,
            "file_name": filename,
            "file_size": file_size,
            "message_id": message.message_id,
            "chat_id": message.chat.id
        }

    bot.send_message(
        message.chat.id,
        "✅ <b>Na karɓi file ɗinka.</b>\n\n"
        f"📄 Name: <code>{safe_filename(filename)}</code>\n"
        f"📦 Size: <b>{format_size(file_size)}</b>\n\n"
        "Yanzu zaɓi yadda kake son in dawo maka da shi:",
        reply_markup=get_format_keyboard()
    )


# ============================================================
# VIDEO RECEIVER
# ============================================================

@bot.message_handler(
    content_types=["video"],
    func=lambda message: get_state(message.from_user.id) == "waiting_file"
)
def video_handler(message):

    user_id = message.from_user.id

    if is_busy(user_id):
        bot.send_message(
            message.chat.id,
            "⏳ Ana processing wani file ɗinka yanzu."
        )
        return

    video = message.video

    filename = video.file_name or f"video_{message.message_id}.mp4"

    file_size = video.file_size or 0

    send_debug(
        f"User: {user_id}\n"
        f"Type: VIDEO\n"
        f"Name: {filename}\n"
        f"Size: {format_size(file_size)}\n"
        f"File ID: {video.file_id}"
    )

    with state_lock:
        user_states[user_id] = {
            "state": "waiting_format",
            "file_type": "video",
            "file_id": video.file_id,
            "file_name": filename,
            "file_size": file_size,
            "message_id": message.message_id,
            "chat_id": message.chat.id
        }

    bot.send_message(
        message.chat.id,
        "✅ <b>Na karɓi video ɗinka.</b>\n\n"
        f"🎬 Name: <code>{safe_filename(filename)}</code>\n"
        f"📦 Size: <b>{format_size(file_size)}</b>\n\n"
        "Yanzu zaɓi yadda kake son in dawo maka da shi:",
        reply_markup=get_format_keyboard()
    )


# ============================================================
# INVALID FILE WHILE WAITING
# ============================================================

@bot.message_handler(
    content_types=[
        "photo",
        "audio",
        "voice",
        "video_note",
        "animation",
        "sticker",
        "contact",
        "location"
    ],
    func=lambda message: get_state(message.from_user.id) == "waiting_file"
)
def invalid_file_handler(message):

    bot.send_message(
        message.chat.id,
        INVALID_TEXT
    )


# ============================================================
# FORMAT CALLBACK
# ============================================================

@bot.callback_query_handler(
    func=lambda call: call.data.startswith("vd_format:")
)
def format_callback(call):

    user_id = call.from_user.id
    chat_id = call.message.chat.id

    requested_format = call.data.split(":", 1)[1]

    if requested_format not in ("file", "video"):
        bot.answer_callback_query(
            call.id,
            "Invalid format.",
            show_alert=True
        )
        return

    with state_lock:
        state = user_states.get(user_id)

    if not isinstance(state, dict):
        bot.answer_callback_query(
            call.id,
            "Babu file da zan yi aiki a kai.",
            show_alert=True
        )
        return

    if state.get("state") != "waiting_format":
        bot.answer_callback_query(
            call.id,
            "Wannan request ya ƙare.",
            show_alert=True
        )
        return

    if is_busy(user_id):
        bot.answer_callback_query(
            call.id,
            "Akwai aiki da ke tafiya.",
            show_alert=True
        )
        return

    # Lock user immediately
    mark_busy(
        user_id,
        {
            "format": requested_format,
            "started": time.time()
        }
    )

    clear_state(user_id)

    bot.answer_callback_query(
        call.id,
        "An zaɓa."
    )

    try:
        bot.edit_message_reply_markup(
            chat_id,
            call.message.message_id,
            reply_markup=None
        )
    except Exception:
        pass

    bot.send_message(
        chat_id,
        PROCESSING_TEXT
    )

    # Run heavy work in background
    thread = threading.Thread(
        target=process_file_job,
        args=(
            user_id,
            chat_id,
            state,
            requested_format
        ),
        daemon=True
    )

    thread.start()


# ============================================================
# MAIN PROCESSING
# ============================================================

def process_file_job(
    user_id,
    chat_id,
    state,
    requested_format
):

    job_start = time.time()

    work_dir = None

    try:

        original_name = safe_filename(
            state.get("file_name") or "file"
        )

        file_type = state.get("file_type")
        file_id = state.get("file_id")
        message_id = state.get("message_id")

        send_debug(
            f"START JOB\n"
            f"User: {user_id}\n"
            f"Requested: {requested_format}\n"
            f"Original type: {file_type}\n"
            f"Name: {original_name}\n"
            f"Message ID: {message_id}"
        )

        # ----------------------------------------------------
        # Create private temporary directory
        # ----------------------------------------------------

        work_dir = tempfile.mkdtemp(
            prefix=f"vd_{user_id}_",
            dir=str(BASE_TEMP_DIR)
        )

        # ----------------------------------------------------
        # Get message through Pyrogram
        # ----------------------------------------------------

        send_debug(
            f"Getting Telegram message...\n"
            f"User: {user_id}\n"
            f"Chat: {chat_id}\n"
            f"Message: {message_id}"
        )

        source_message = app.get_messages(
            chat_id,
            message_id
        )

        if not source_message:
            raise RuntimeError(
                "Pyrogram could not retrieve the source message."
            )

        # ----------------------------------------------------
        # Download
        # ----------------------------------------------------

        bot.send_message(
            chat_id,
            "⬇️ <b>Download yana tafiya...</b>"
        )

        download_start = time.time()

        downloaded_path = app.download_media(
            source_message,
            file_name=os.path.join(
                work_dir,
                original_name
            )
        )

        download_time = time.time() - download_start

        if not downloaded_path:
            raise RuntimeError(
                "Telegram download failed."
            )

        downloaded_path = str(downloaded_path)

        if not os.path.exists(downloaded_path):
            raise RuntimeError(
                "Downloaded file does not exist."
            )

        downloaded_size = os.path.getsize(
            downloaded_path
        )

        send_debug(
            f"DOWNLOAD COMPLETE\n"
            f"User: {user_id}\n"
            f"Path: {downloaded_path}\n"
            f"Size: {format_size(downloaded_size)}\n"
            f"Time: {download_time:.2f}s"
        )

        # ----------------------------------------------------
        # FILE OUTPUT
        # ----------------------------------------------------

        if requested_format == "file":

            bot.send_message(
                chat_id,
                "⬆️ <b>Ana sake tura file...</b>"
            )

            upload_start = time.time()

            app.send_document(
                chat_id,
                document=downloaded_path,
                caption=(
                    "📁 <b>Ga file ɗinka.</b>"
                )
            )

            upload_time = time.time() - upload_start

            send_debug(
                f"DOCUMENT UPLOAD COMPLETE\n"
                f"User: {user_id}\n"
                f"Size: {format_size(downloaded_size)}\n"
                f"Upload time: {upload_time:.2f}s"
            )

        # ----------------------------------------------------
        # VIDEO OUTPUT
        # ----------------------------------------------------

        elif requested_format == "video":

            # Telegram/clients generally expect MP4/MPEG4
            # for a normal playable video.
            #
            # Wannan first version ba ya force FFmpeg conversion.
            # Idan source file ya riga ya dace, za a tura shi.
            #
            # Daga baya za mu ƙara FFmpeg auto-conversion
            # idan kana son MKV/AVI -> MP4.

            extension = Path(downloaded_path).suffix.lower()

            if extension not in (
                ".mp4",
                ".m4v",
                ".mov"
            ):
                send_debug(
                    f"WARNING: Requested VIDEO output but "
                    f"source extension is {extension}.\n"
                    f"No FFmpeg conversion is performed in this "
                    f"first test version."
                )

            bot.send_message(
                chat_id,
                "⬆️ <b>Ana sake tura shi a matsayin Video...</b>"
            )

            upload_start = time.time()

            app.send_video(
                chat_id,
                video=downloaded_path,
                caption="📺 <b>Ga video ɗinka.</b>",
                supports_streaming=True
            )

            upload_time = time.time() - upload_start

            send_debug(
                f"VIDEO UPLOAD COMPLETE\n"
                f"User: {user_id}\n"
                f"Size: {format_size(downloaded_size)}\n"
                f"Upload time: {upload_time:.2f}s"
            )

        else:
            raise RuntimeError(
                "Unknown requested format."
            )

        total_time = time.time() - job_start

        bot.send_message(
            chat_id,
            "✅ <b>An gama!</b>\n\n"
            f"📦 Size: <b>{format_size(downloaded_size)}</b>\n"
            f"⏱ Lokaci: <b>{total_time:.1f}s</b>"
        )

        send_debug(
            f"JOB COMPLETE\n"
            f"User: {user_id}\n"
            f"Total time: {total_time:.2f}s"
        )

    except FloodWait as e:

        wait_seconds = getattr(
            e,
            "value",
            0
        )

        logger.exception("FloodWait")

        try:
            bot.send_message(
                chat_id,
                "⚠️ Telegram ta ce mu jira kaɗan saboda rate limit.\n"
                f"Ka sake gwadawa bayan {wait_seconds} seconds."
            )
        except Exception:
            pass

        send_debug(
            f"FLOOD WAIT\n"
            f"User: {user_id}\n"
            f"Wait: {wait_seconds}"
        )

    except RPCError as e:

        logger.exception("Pyrogram RPC Error")

        try:
            bot.send_message(
                chat_id,
                "❌ Telegram ta ƙi yin aikin.\n\n"
                f"<code>{str(e)[:1000]}</code>"
            )
        except Exception:
            pass

        send_debug(
            f"PYROGRAM RPC ERROR\n"
            f"User: {user_id}\n"
            f"Error: {str(e)[:1500]}"
        )

    except Exception as e:

        logger.exception("VD JOB ERROR")

        try:
            bot.send_message(
                chat_id,
                "❌ <b>An samu matsala.</b>\n\n"
                "An kasa kammala aikin.\n"
                "Ka sake gwadawa da wani ƙaramin file."
            )
        except Exception:
            pass

        send_debug(
            f"JOB ERROR\n"
            f"User: {user_id}\n"
            f"Error type: {type(e).__name__}\n"
            f"Error: {str(e)[:2000]}"
        )

    finally:

        # ----------------------------------------------------
        # ALWAYS CLEAN TEMP FILES
        # ----------------------------------------------------

        if work_dir:
            cleanup_folder(work_dir)

        unmark_busy(user_id)

        send_debug(
            f"CLEANUP COMPLETE\n"
            f"User: {user_id}"
        )


# ============================================================
# /CANCEL
# ============================================================

@bot.message_handler(commands=["cancel"])
def cancel_handler(message):

    user_id = message.from_user.id

    if is_busy(user_id):
        bot.send_message(
            message.chat.id,
            "⏳ Aikin yana gudana yanzu.\n"
            "Ba za a iya cancel bayan download ya fara ba."
        )
        return

    clear_state(user_id)

    bot.send_message(
        message.chat.id,
        "❌ An soke request ɗin."
    )


# ============================================================
# GENERAL TEXT WHILE WAITING
# ============================================================

@bot.message_handler(
    content_types=["text"],
    func=lambda message: get_state(message.from_user.id) == "waiting_file"
)
def waiting_text_handler(message):

    bot.send_message(
        message.chat.id,
        "📥 Ina jiran <b>file ko video</b>.\n\n"
        "Ka turo shi yanzu."
    )



# =============================================================
# RENDER WEB SERVICE + TELEBOT + PYROGRAM
# =============================================================

from flask import Flask
import os
import time
import threading


# =============================================================
# FLASK HEALTH SERVER
# =============================================================

server = Flask(__name__)


@server.route("/")
def home():
    return "VD Bot is running successfully.", 200


@server.route("/health")
def health():
    return "OK", 200


@server.route("/status")
def status():
    return {
        "status": "online",
        "bot": "running",
        "pyrogram": "running"
    }, 200


def run_web_server():
    """
    Render Web Service yana bukatar application
    ya saurari PORT.
    """

    port_raw = os.getenv("PORT", "10000").strip()

    try:
        port = int(port_raw)
    except ValueError:
        port = 10000

    logger.info(
        "Starting Render HTTP server on 0.0.0.0:%s",
        port
    )

    try:
        server.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False,
            threaded=True
        )

    except Exception as e:

        logger.exception(
            "HTTP server crashed: %s",
            e
        )

        send_debug(
            "🚨 RENDER HTTP SERVER CRASHED\n\n"
            f"Error: {type(e).__name__}\n"
            f"{str(e)[:1500]}"
        )


# =============================================================
# TELEBOT POLLING
# =============================================================

def polling_loop():

    while True:

        try:

            logger.info(
                "Starting TeleBot polling..."
            )

            send_debug(
                "🔄 TeleBot polling is starting..."
            )

            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
                skip_pending=True,
                allowed_updates=[
                    "message",
                    "callback_query"
                ]
            )

        except Exception as e:

            logger.exception(
                "TeleBot polling crashed: %s",
                e
            )

            send_debug(
                "🚨 TELEBOT POLLING CRASHED\n\n"
                f"Error Type: {type(e).__name__}\n"
                f"Error: {str(e)[:2000]}\n\n"
                "Bot zai sake kokarin tashi bayan seconds 5."
            )

            time.sleep(5)

            logger.info(
                "Restarting TeleBot polling..."
            )


# =============================================================
# MAIN
# =============================================================

def main():

    logger.info(
        "=============================================="
    )

    logger.info(
        "🚀 VD BOT STARTING..."
    )

    logger.info(
        "=============================================="
    )


    # ---------------------------------------------------------
    # CHECK ENVIRONMENT VARIABLES
    # ---------------------------------------------------------

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing."
        )

    if not API_ID:
        raise RuntimeError(
            "API_ID is missing."
        )

    if not API_HASH:
        raise RuntimeError(
            "API_HASH is missing."
        )


    logger.info(
        "BOT_TOKEN: LOADED"
    )

    logger.info(
        "API_ID: %s",
        API_ID
    )

    logger.info(
        "API_HASH: LOADED"
    )


    if ADMIN_ID:

        logger.info(
            "ADMIN_ID: %s",
            ADMIN_ID
        )

    else:

        logger.info(
            "ADMIN_ID: NOT SET"
        )


    # ---------------------------------------------------------
    # START RENDER HTTP SERVER
    # ---------------------------------------------------------

    logger.info(
        "Starting Render Web Service HTTP server..."
    )

    web_thread = threading.Thread(
        target=run_web_server,
        name="RenderHTTP",
        daemon=True
    )

    web_thread.start()


    # ---------------------------------------------------------
    # START PYROGRAM
    # ---------------------------------------------------------

    logger.info(
        "Starting Pyrogram..."
    )

    try:

        app.start()

        logger.info(
            "✅ Pyrogram started successfully."
        )

        send_debug(
            "🟢 PYROGRAM STARTED\n\n"
            "Telegram download/upload system is ready."
        )

    except Exception as e:

        logger.exception(
            "Pyrogram failed to start."
        )

        send_debug(
            "🚨 PYROGRAM START FAILED\n\n"
            f"Error Type: {type(e).__name__}\n"
            f"Error: {str(e)[:2000]}"
        )

        raise


    # ---------------------------------------------------------
    # BOT STARTED MESSAGE
    # ---------------------------------------------------------

    send_debug(
        "🚀 VD BOT STARTED SUCCESSFULLY\n\n"
        "✅ Render HTTP Server\n"
        "✅ TeleBot\n"
        "✅ Pyrogram\n\n"
        "All systems are ready."
    )


    logger.info(
        "=============================================="
    )

    logger.info(
        "🟢 ALL SYSTEMS ARE READY"
    )

    logger.info(
        "=============================================="
    )


    # ---------------------------------------------------------
    # START TELEBOT
    # ---------------------------------------------------------

    polling_loop()


# =============================================================
# START APPLICATION
# =============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        logger.info(
            "Bot stopped manually."
        )

    except Exception as e:

        logger.exception(
            "FATAL APPLICATION ERROR"
        )

        try:

            send_debug(
                "💥 FATAL APPLICATION ERROR\n\n"
                f"Error Type: {type(e).__name__}\n"
                f"Error: {str(e)[:3000]}"
            )

        except Exception:
            pass

        # Kada mu mutu gaba daya nan take.
        # Render zai sake tayar da service din.
        raise

