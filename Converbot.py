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
# /VIDEOCON — LARGE VIDEO / FILE CONVERTER TEST
# ============================================================
#
# TELEBOT:
#   - /videocon
#   - receive Video / Document
#   - buttons
#
# PYROGRAM / MTProto:
#   - LARGE DOWNLOAD
#   - LARGE UPLOAD
#
# TEST:
#   - Maximum: 1.65 GB
#   - NO DATABASE
#   - NO FFMPEG
#   - NO COMPRESSION
#   - TEMP FILE ONLY
#
# DEBUG:
#   - Telegram receive
#   - Telegram download
#   - Render disk
#   - RAM
#   - upload progress
#   - exceptions
# ============================================================

import os
import asyncio
import tempfile
import shutil
import threading
import time
import traceback
import shutil as diskutil

from telebot import types

from pyrogram import Client


# ============================================================
# CONFIG
# ============================================================

VIDEOCON_MAX_GB = 1.65

VIDEOCON_MAX_BYTES = int(
    VIDEOCON_MAX_GB * 1024 * 1024 * 1024
)

# Existing ENV variables
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Pyrogram session
VIDEOCON_SESSION_NAME = "videocon_mtproto"

# Minimum free disk safety margin
VIDEOCON_MIN_FREE_BYTES = 300 * 1024 * 1024


# ============================================================
# SESSION STATE
# ============================================================

_videocon_waiting = set()

_videocon_jobs = {}

_videocon_lock = threading.Lock()


# ============================================================
# PYROGRAM ENGINE
# ============================================================

_videocon_loop = None

_videocon_loop_thread = None

_videocon_pyro = None

_videocon_ready = threading.Event()

_videocon_start_error = None


# ============================================================
# DEBUG
# ============================================================

def videocon_debug(*args):

    try:

        print(
            "🔎 [VIDEOCON DEBUG]",
            *args,
            flush=True
        )

    except Exception:
        pass


# ============================================================
# FORMAT BYTES
# ============================================================

def videocon_size(value):

    try:

        value = float(value)

    except Exception:

        return "0 B"

    if value >= 1024 ** 3:

        return f"{value / (1024 ** 3):.2f} GB"

    if value >= 1024 ** 2:

        return f"{value / (1024 ** 2):.2f} MB"

    if value >= 1024:

        return f"{value / 1024:.2f} KB"

    return f"{int(value)} B"


# ============================================================
# SYSTEM DEBUG
# ============================================================

def videocon_system_debug():

    try:

        total, used, free = diskutil.disk_usage(
            os.getcwd()
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
            repr(e)
        )

    try:

        import psutil

        memory = psutil.virtual_memory()

        videocon_debug(
            "RAM TOTAL =",
            videocon_size(memory.total)
        )

        videocon_debug(
            "RAM USED =",
            videocon_size(memory.used)
        )

        videocon_debug(
            "RAM AVAILABLE =",
            videocon_size(memory.available)
        )

        videocon_debug(
            "RAM PERCENT =",
            memory.percent
        )

    except Exception as e:

        videocon_debug(
            "RAM CHECK ERROR =",
            repr(e)
        )


# ============================================================
# PROGRESS FORMAT
# ============================================================

def videocon_progress_percent(
    current,
    total
):

    if not total:

        return 0

    return (
        current * 100
    ) / total


# ============================================================
# PYROGRAM STARTER
# ============================================================

def _videocon_pyrogram_thread():

    global _videocon_loop
    global _videocon_pyro
    global _videocon_start_error

    try:

        videocon_debug(
            "Starting Pyrogram MTProto engine..."
        )

        videocon_debug(
            "API_ID present =",
            bool(API_ID)
        )

        videocon_debug(
            "API_HASH present =",
            bool(API_HASH)
        )

        videocon_debug(
            "BOT_TOKEN present =",
            bool(BOT_TOKEN)
        )

        _videocon_loop = asyncio.new_event_loop()

        asyncio.set_event_loop(
            _videocon_loop
        )

        # ====================================================
        # IMPORTANT:
        # no_updates=True
        #
        # Pyrogram is used only for file transfer.
        # TeleBot remains responsible for bot updates.
        # ====================================================

        _videocon_pyro = Client(

            VIDEOCON_SESSION_NAME,

            api_id=API_ID,

            api_hash=API_HASH,

            bot_token=BOT_TOKEN,

            no_updates=True,

            max_concurrent_transmissions=1,

            sleep_threshold=30
        )

        async def start_client():

            await _videocon_pyro.start()

            videocon_debug(
                "✅ Pyrogram connected to Telegram."
            )

            try:

                me = await _videocon_pyro.get_me()

                videocon_debug(
                    "Pyrogram bot:",
                    getattr(me, "username", None),
                    getattr(me, "id", None)
                )

            except Exception as e:

                videocon_debug(
                    "get_me debug error:",
                    repr(e)
                )

        _videocon_loop.run_until_complete(
            start_client()
        )

        _videocon_ready.set()

        videocon_debug(
            "✅ VIDEOCON MTProto ENGINE READY."
        )

        # Keep loop alive
        _videocon_loop.run_forever()

    except Exception as e:

        _videocon_start_error = e

        videocon_debug(
            "❌ PYROGRAM START ERROR =",
            repr(e)
        )

        videocon_debug(
            traceback.format_exc()
        )

        _videocon_ready.set()


# ============================================================
# START PYROGRAM ENGINE
# ============================================================

def start_videocon_engine():

    global _videocon_loop_thread

    if _videocon_loop_thread:

        return

    _videocon_loop_thread = threading.Thread(

        target=_videocon_pyrogram_thread,

        daemon=True,

        name="videocon-mtproto"
    )

    _videocon_loop_thread.start()

    videocon_debug(
        "Pyrogram engine thread started."
    )


# ============================================================
# RUN COROUTINE IN PYROGRAM LOOP
# ============================================================

def videocon_run_async(coro):

    if not _videocon_loop:

        raise RuntimeError(
            "Pyrogram event loop bai fara ba."
        )

    future = asyncio.run_coroutine_threadsafe(

        coro,

        _videocon_loop
    )

    return future.result()


# ============================================================
# PROGRESS CALLBACK
# ============================================================

async def videocon_transfer_progress(

    current,

    total,

    status_message,

    mode,

    last_state
):

    try:

        if not total:

            return

        percent = videocon_progress_percent(
            current,
            total
        )

        now = time.time()

        last_time = last_state.get(
            "time",
            0
        )

        # Don't spam Telegram
        if (

            percent < 100

            and now - last_time < 5

        ):

            return

        last_state["time"] = now

        text = (

            "⬇️ <b>DOWNLOADING...</b>"
            if mode == "download"
            else
            "📤 <b>UPLOADING...</b>"
        )

        text += (

            "\n\n"

            f"📊 Progress: "
            f"<b>{percent:.1f}%</b>\n"

            f"📦 {videocon_size(current)}"
            f" / "
            f"{videocon_size(total)}\n\n"

            "⏳ Please wait..."
        )

        videocon_debug(

            mode.upper(),

            f"{percent:.1f}%",

            videocon_size(current),

            "/",

            videocon_size(total)
        )

        try:

            await asyncio.to_thread(

                bot.edit_message_text,

                chat_id=status_message.chat.id,

                message_id=status_message.id,

                text=text,

                parse_mode="HTML"
            )

        except Exception as e:

            videocon_debug(
                "Progress message update failed:",
                repr(e)
            )

    except Exception as e:

        videocon_debug(
            "Progress callback error:",
            repr(e)
        )


# ============================================================
# /VIDEOCON
# ============================================================

@bot.message_handler(
    commands=["videocon"]
)
def videocon_command(message):

    user_id = message.from_user.id

    # ========================================================
    # ADMIN ONLY
    # ========================================================

    if user_id != ADMIN_ID:

        try:

            bot.reply_to(
                message,
                "❌ Wannan command ɗin na Admin ne kawai."
            )

        except Exception:

            pass

        return

    # ========================================================
    # START ENGINE IF NECESSARY
    # ========================================================

    try:

        start_videocon_engine()

    except Exception as e:

        videocon_debug(
            "Engine start error:",
            repr(e)
        )

    # ========================================================
    # START SESSION
    # ========================================================

    _videocon_waiting.add(
        user_id
    )

    _videocon_jobs.pop(
        user_id,
        None
    )

    # ========================================================
    # ASK
    # ========================================================

    bot.reply_to(

        message,

        (
            "🎬 <b>VIDEO CONVERTER</b>\n\n"

            "Turo min <b>Video</b> ko "
            "<b>File/Document</b> ɗin da kake son "
            "mu canza.\n\n"

            f"📦 Maximum: "
            f"<b>{VIDEOCON_MAX_GB} GB</b>\n\n"

            "⏳ Bayan ka turo shi zan tambaye ka "
            "irin yadda kake son na dawo maka da shi."
        ),

        parse_mode="HTML"
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
def videocon_receive_video(message):

    user_id = message.from_user.id

    if user_id != ADMIN_ID:

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

        videocon_debug(
            "VIDEO RECEIVED"
        )

        videocon_debug(
            "message_id =",
            message.message_id
        )

        videocon_debug(
            "file_id =",
            message.video.file_id
        )

        videocon_debug(
            "size =",
            videocon_size(file_size)
        )

        # ====================================================
        # SIZE CHECK
        # ====================================================

        if file_size <= 0:

            bot.reply_to(
                message,
                "❌ An kasa gano girman video."
            )

            return

        if file_size > VIDEOCON_MAX_BYTES:

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
        # SAVE
        # ====================================================

        _videocon_waiting.discard(
            user_id
        )

        _videocon_jobs[user_id] = {

            "file_id":
                message.video.file_id,

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

        # ====================================================
        # BUTTONS
        # ====================================================

        keyboard = types.InlineKeyboardMarkup(
            row_width=2
        )

        keyboard.add(

            types.InlineKeyboardButton(
                "🎬 Video",
                callback_data="videocon:video"
            ),

            types.InlineKeyboardButton(
                "📁 File",
                callback_data="videocon:file"
            )
        )

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

    except Exception as e:

        videocon_debug(
            "RECEIVE VIDEO ERROR:",
            repr(e)
        )

        videocon_debug(
            traceback.format_exc()
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
def videocon_receive_document(message):

    user_id = message.from_user.id

    if user_id != ADMIN_ID:

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

        videocon_debug(
            "DOCUMENT RECEIVED"
        )

        videocon_debug(
            "message_id =",
            message.message_id
        )

        videocon_debug(
            "file_name =",
            file_name
        )

        videocon_debug(
            "file_id =",
            message.document.file_id
        )

        videocon_debug(
            "size =",
            videocon_size(file_size)
        )

        # ====================================================
        # SIZE
        # ====================================================

        if file_size <= 0:

            bot.reply_to(
                message,
                "❌ An kasa gano girman file."
            )

            return

        if file_size > VIDEOCON_MAX_BYTES:

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
        # SAVE
        # ====================================================

        _videocon_waiting.discard(
            user_id
        )

        _videocon_jobs[user_id] = {

            "file_id":
                message.document.file_id,

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

        # ====================================================
        # BUTTONS
        # ====================================================

        keyboard = types.InlineKeyboardMarkup(
            row_width=2
        )

        keyboard.add(

            types.InlineKeyboardButton(
                "🎬 Video",
                callback_data="videocon:video"
            ),

            types.InlineKeyboardButton(
                "📁 File",
                callback_data="videocon:file"
            )
        )

        bot.reply_to(

            message,

            (
                "✅ <b>FILE RECEIVED</b>\n\n"

                f"📄 Name: "
                f"<b>{file_name}</b>\n"

                f"📦 Size: "
                f"<b>{videocon_size(file_size)}</b>\n\n"

                "❓ <b>Me kake so na dawo maka da shi?</b>"
            ),

            reply_markup=keyboard,

            parse_mode="HTML"
        )

    except Exception as e:

        videocon_debug(
            "RECEIVE DOCUMENT ERROR:",
            repr(e)
        )

        videocon_debug(
            traceback.format_exc()
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
def videocon_callback(call):

    user_id = call.from_user.id

    # ========================================================
    # ADMIN
    # ========================================================

    if user_id != ADMIN_ID:

        bot.answer_callback_query(

            call.id,

            "❌ Admin kawai."
        )

        return

    # ========================================================
    # CHOICE
    # ========================================================

    choice = call.data.split(
        ":",
        1
    )[1]

    if choice not in (
        "video",
        "file"
    ):

        bot.answer_callback_query(
            call.id,
            "❌ Invalid option."
        )

        return

    # ========================================================
    # JOB
    # ========================================================

    job = _videocon_jobs.get(
        user_id
    )

    if not job:

        bot.answer_callback_query(

            call.id,

            "❌ Session ta ƙare. "
            "Ka sake amfani da /videocon."
        )

        return

    videocon_debug(
        "BUTTON SELECTED"
    )

    videocon_debug(
        "input type =",
        job.get("file_type")
    )

    videocon_debug(
        "output type =",
        choice
    )

    videocon_debug(
        "size =",
        videocon_size(
            job.get("file_size", 0)
        )
    )

    # ========================================================
    # REMOVE BUTTONS
    # ========================================================

    try:

        bot.edit_message_reply_markup(

            chat_id=call.message.chat.id,

            message_id=call.message.message_id,

            reply_markup=None
        )

    except Exception as e:

        videocon_debug(
            "Remove buttons error:",
            repr(e)
        )

    # ========================================================
    # ANSWER
    # ========================================================

    bot.answer_callback_query(

        call.id,

        "🚀 An fara aiki..."
    )

    # ========================================================
    # START WORKER
    # ========================================================

    worker = threading.Thread(

        target=_videocon_process,

        args=(
            user_id,
            job,
            choice
        ),

        daemon=True,

        name="videocon-worker"
    )

    worker.start()


# ============================================================
# MAIN WORKER
# ============================================================

def _videocon_process(

    user_id,

    job,

    choice
):

    temp_dir = None

    input_file = None

    status_message = None

    try:

        # ====================================================
        # SYSTEM DEBUG
        # ====================================================

        videocon_debug(
            "================================"
        )

        videocon_debug(
            "NEW VIDEOCON JOB"
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
                job.get("file_size", 0)
            )
        )

        videocon_system_debug()

        # ====================================================
        # WAIT FOR PYROGRAM
        # ====================================================

        videocon_debug(
            "Waiting for Pyrogram engine..."
        )

        if not _videocon_ready.wait(
            timeout=60
        ):

            raise RuntimeError(
                "Pyrogram engine bai fara cikin seconds 60 ba."
            )

        if _videocon_start_error:

            raise RuntimeError(
                f"Pyrogram startup failed: "
                f"{_videocon_start_error}"
            )

        videocon_debug(
            "Pyrogram engine ready."
        )

        # ====================================================
        # TEMP DIRECTORY
        # ====================================================

        temp_dir = tempfile.mkdtemp(
            prefix="videocon_"
        )

        videocon_debug(
            "TEMP DIR =",
            temp_dir
        )

        # ====================================================
        # DISK CHECK
        # ====================================================

        total, used, free = diskutil.disk_usage(
            temp_dir
        )

        videocon_debug(
            "FREE DISK BEFORE DOWNLOAD =",
            videocon_size(free)
        )

        required_space = (

            job["file_size"]

            + VIDEOCON_MIN_FREE_BYTES
        )

        if free < required_space:

            raise RuntimeError(

                "Render disk bai da isasshen wuri ba.\n\n"

                f"Free: {videocon_size(free)}\n"

                f"Required: "
                f"{videocon_size(required_space)}"
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

        # Prevent weird paths
        original_name = os.path.basename(
            original_name
        )

        # ====================================================
        # INPUT PATH
        # ====================================================

        input_file = os.path.join(

            temp_dir,

            original_name
        )

        # ====================================================
        # STATUS
        # ====================================================

        status_message = bot.send_message(

            user_id,

            (
                "⏳ <b>Preparing...</b>\n\n"

                "🔧 Ana shirya file ɗinka.\n\n"

                f"📦 Size: "
                f"<b>{videocon_size(job['file_size'])}</b>\n\n"

                "⏳ Please wait..."
            ),

            parse_mode="HTML"
        )

        # ====================================================
        # DOWNLOAD MESSAGE THROUGH PYROGRAM
        # ====================================================

        videocon_debug(
            "Getting original Telegram message..."
        )

        pyro_message = videocon_run_async(

            _videocon_get_message(

                job["chat_id"],

                job["message_id"]
            )
        )

        if not pyro_message:

            raise RuntimeError(
                "Pyrogram bai iya samun original message ba."
            )

        videocon_debug(
            "Original Telegram message found."
        )

        # ====================================================
        # DOWNLOAD
        # ====================================================

        bot.edit_message_text(

            chat_id=user_id,

            message_id=status_message.message_id,

            text=(

                "⬇️ <b>DOWNLOADING...</b>\n\n"

                f"📦 Expected: "
                f"<b>{videocon_size(job['file_size'])}</b>\n\n"

                "⏳ Telegram → Render..."
            ),

            parse_mode="HTML"
        )

        videocon_debug(
            "DOWNLOAD STARTED"
        )

        download_state = {
            "time": 0
        }

        downloaded_path = videocon_run_async(

            _videocon_download(

                pyro_message,

                input_file,

                status_message,

                download_state
            )
        )

        if not downloaded_path:

            raise RuntimeError(
                "Pyrogram download ya dawo babu file."
            )

        input_file = downloaded_path

        videocon_debug(
            "DOWNLOAD FINISHED"
        )

        # ====================================================
        # VERIFY DISK FILE
        # ====================================================

        if not os.path.exists(
            input_file
        ):

            raise RuntimeError(
                "Downloaded file bai bayyana a disk ba."
            )

        downloaded_size = os.path.getsize(
            input_file
        )

        videocon_debug(
            "DOWNLOADED SIZE =",
            videocon_size(downloaded_size)
        )

        if downloaded_size <= 0:

            raise RuntimeError(
                "Downloaded file empty ne."
            )

        videocon_system_debug()

        # ====================================================
        # FILE OUTPUT
        # ====================================================

        if choice == "file":

            bot.edit_message_text(

                chat_id=user_id,

                message_id=status_message.message_id,

                text=(

                    "📤 <b>UPLOADING AS FILE...</b>\n\n"

                    f"📦 Size: "
                    f"<b>{videocon_size(downloaded_size)}</b>\n\n"

                    "⏳ Render → Telegram..."
                ),

                parse_mode="HTML"
            )

            videocon_debug(
                "UPLOAD AS DOCUMENT STARTED"
            )

            upload_state = {
                "time": 0
            }

            result = videocon_run_async(

                _videocon_upload_document(

                    user_id,

                    input_file,

                    original_name,

                    downloaded_size,

                    upload_state
                )
            )

            if not result:

                raise RuntimeError(
                    "Telegram bai dawo da upload result ba."
                )

            videocon_debug(
                "DOCUMENT UPLOAD FINISHED"
            )

        # ====================================================
        # VIDEO OUTPUT
        # ====================================================

        elif choice == "video":

            bot.edit_message_text(

                chat_id=user_id,

                message_id=status_message.message_id,

                text=(

                    "📤 <b>UPLOADING AS VIDEO...</b>\n\n"

                    f"📦 Size: "
                    f"<b>{videocon_size(downloaded_size)}</b>\n\n"

                    "⏳ Render → Telegram..."
                ),

                parse_mode="HTML"
            )

            videocon_debug(
                "UPLOAD AS VIDEO STARTED"
            )

            upload_state = {
                "time": 0
            }

            result = videocon_run_async(

                _videocon_upload_video(

                    user_id,

                    input_file,

                    original_name,

                    downloaded_size,

                    upload_state
                )
            )

            if not result:

                raise RuntimeError(
                    "Telegram bai dawo da video upload result ba."
                )

            videocon_debug(
                "VIDEO UPLOAD FINISHED"
            )

        # ====================================================
        # COMPLETE
        # ====================================================

        bot.edit_message_text(

            chat_id=user_id,

            message_id=status_message.message_id,

            text=(

                "✅ <b>CONVERSION COMPLETE</b>\n\n"

                f"📦 Size: "
                f"<b>{videocon_size(downloaded_size)}</b>\n\n"

                (

                    "🎬 Returned as: "
                    "<b>VIDEO</b>"

                    if choice == "video"

                    else

                    "📁 Returned as: "
                    "<b>FILE</b>"
                )
            ),

            parse_mode="HTML"
        )

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
            videocon_size(downloaded_size)
        )

        videocon_debug(
            "================================"
        )

    # ========================================================
    # ERROR
    # ========================================================

    except Exception as e:

        videocon_debug(
            "================================"
        )

        videocon_debug(
            "❌ VIDEOCON ERROR"
        )

        videocon_debug(
            "ERROR TYPE =",
            type(e).__name__
        )

        videocon_debug(
            "ERROR =",
            repr(e)
        )

        videocon_debug(
            traceback.format_exc()
        )

        videocon_debug(
            "================================"
        )

        try:

            if status_message:

                bot.edit_message_text(

                    chat_id=user_id,

                    message_id=status_message.message_id,

                    text=(

                        "❌ <b>CONVERSION ERROR</b>\n\n"

                        f"<code>{str(e)}</code>\n\n"

                        "🔎 An rubuta cikakken error "
                        "a Render Logs.\n\n"

                        "Da fatan ka sake gwadawa."
                    ),

                    parse_mode="HTML"
                )

            else:

                bot.send_message(

                    user_id,

                    (
                        "❌ <b>Conversion Error</b>\n\n"

                        f"<code>{str(e)}</code>"
                    ),

                    parse_mode="HTML"
                )

        except Exception as notify_error:

            videocon_debug(
                "ERROR NOTIFICATION FAILED:",
                repr(notify_error)
            )

    # ========================================================
    # CLEANUP
    # ========================================================

    finally:

        try:

            if temp_dir and os.path.exists(
                temp_dir
            ):

                shutil.rmtree(

                    temp_dir,

                    ignore_errors=True
                )

                videocon_debug(
                    "🧹 TEMP FILES DELETED."
                )

        except Exception as cleanup_error:

            videocon_debug(
                "CLEANUP ERROR:",
                repr(cleanup_error)
            )

        _videocon_jobs.pop(
            user_id,
            None
        )

        _videocon_waiting.discard(
            user_id
        )

        videocon_debug(
            "VIDEOCON SESSION CLOSED."
        )


# ============================================================
# PYROGRAM: GET MESSAGE
# ============================================================

async def _videocon_get_message(

    chat_id,

    message_id
):

    videocon_debug(
        "Pyrogram get_messages:",
        chat_id,
        message_id
    )

    message = await _videocon_pyro.get_messages(

        chat_id,

        message_id
    )

    return message


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
        "Pyrogram download_media() starting..."
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

    result = await _videocon_pyro.download_media(

        message,

        file_name=output_path,

        progress=progress
    )

    videocon_debug(
        "Pyrogram download_media() returned:",
        result
    )

    return result


# ============================================================
# PYROGRAM: UPLOAD DOCUMENT
# ============================================================

async def _videocon_upload_document(

    user_id,

    input_file,

    original_name,

    file_size,

    progress_state
):

    videocon_debug(
        "Pyrogram send_document() starting..."
    )

    async def progress(
        current,
        total
    ):

        videocon_debug(

            "UPLOAD FILE:",

            f"{videocon_progress_percent(current, total):.1f}%",

            videocon_size(current),

            "/",

            videocon_size(total)
        )

    result = await _videocon_pyro.send_document(

        chat_id=user_id,

        document=input_file,

        caption=(

            "✅ <b>File Converted</b>\n\n"

            f"📄 Name: "
            f"<b>{original_name}</b>\n\n"

            f"📦 Size: "
            f"<b>{videocon_size(file_size)}</b>\n\n"

            "📁 Video Converter"
        ),

        progress=progress
    )

    return result


# ============================================================
# PYROGRAM: UPLOAD VIDEO
# ============================================================

async def _videocon_upload_video(

    user_id,

    input_file,

    original_name,

    file_size,

    progress_state
):

    videocon_debug(
        "Pyrogram send_video() starting..."
    )

    async def progress(
        current,
        total
    ):

        videocon_debug(

            "UPLOAD VIDEO:",

            f"{videocon_progress_percent(current, total):.1f}%",

            videocon_size(current),

            "/",

            videocon_size(total)
        )

    result = await _videocon_pyro.send_video(

        chat_id=user_id,

        video=input_file,

        caption=(

            "✅ <b>Video Converted</b>\n\n"

            f"📦 Size: "
            f"<b>{videocon_size(file_size)}</b>\n\n"

            "🎬 Video Converter"
        ),

        supports_streaming=True,

        progress=progress
    )

    return result


# ============================================================
# START ENGINE IMMEDIATELY
# ============================================================

try:

    start_videocon_engine()

except Exception as e:

    videocon_debug(
        "Initial Videocon engine start failed:",
        repr(e)
    )






# ============================================================
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

