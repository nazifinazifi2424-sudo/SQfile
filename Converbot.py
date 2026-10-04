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
# CLEAN LARGE-FILE EDITION
#
# FLOW:
#
# /videocon
#      ↓
# Admin sends VIDEO or DOCUMENT
#      ↓
# 🎬 Video | 📁 File
#      ↓
# Pyrogram MTProto
#      ↓
# Telegram → Render DOWNLOAD
#      ↓
# Render → Telegram UPLOAD
#      ↓
# CLEANUP
#
# IMPORTANT:
# - NO ARTIFICIAL GB FILE LIMIT
# - Telegram/Pyrogram decides the real Telegram limit
# - Render disk availability is checked before download
# - No conversion/compression is performed
# - File → Video uses metadata detection only
# - ffprobe DOES NOT convert or compress the file
# - Failed jobs are cleaned from Render disk
# - Download/upload are verified
# - Operation timeout protection
# - Pyrogram startup protection
# - Render health server
# ============================================================


# ============================================================
# IMPORTS
# ============================================================

import os
import asyncio
import tempfile
import shutil
import threading
import time
import html
import subprocess
import json

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from telebot import types

from pyrogram import Client
from pyrogram.errors import RPCError


# ============================================================
# CONFIG
# ============================================================

# ------------------------------------------------------------
# IMPORTANT:
#
# BABU VIDEOCON_MAX_GB.
#
# Wannan yana nufin code ba ya cewa:
#
# 1 GB = YES
# 2 GB = YES
# 4 GB = NO
#
# Babu wannan restriction.
#
# Telegram/Pyrogram ne zai yanke hukuncin file limit.
# Render disk kuma shi ne yake bukatar isasshen storage.
# ------------------------------------------------------------


# ============================================================
# MINIMUM FREE DISK
# ============================================================
#
# Wannan BA file-size limit bane.
#
# Ana barin 300 MB reserve domin kada filesystem ya cika.
#
# Misali:
#
# File = 4 GB
# Reserve = 300 MB
#
# Required ≈ 4.3 GB free.
# ============================================================

VIDEOCON_MIN_FREE_BYTES = (
    300 * 1024 * 1024
)


# ============================================================
# PROGRESS
# ============================================================

VIDEOCON_PROGRESS_INTERVAL = 10


# ============================================================
# OPERATION TIMEOUT
# ============================================================
#
# Wannan yana hana download/upload rataye har abada.
#
# Ba file-size limit bane.
# ============================================================

VIDEOCON_OPERATION_TIMEOUT = (
    12 * 60 * 60
)


# ============================================================
# PYROGRAM STARTUP TIMEOUT
# ============================================================

VIDEOCON_ENGINE_START_TIMEOUT = 90


# ============================================================
# HEALTH SERVER
# ============================================================

VIDEOCON_HEALTH_ENABLED = (
    os.getenv(
        "VIDEOCON_HEALTH_ENABLED",
        "true"
    ).lower()
    in (
        "1",
        "true",
        "yes",
        "on"
    )
)


# ============================================================
# ENVIRONMENT VARIABLES
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
            str(
                globals().get(
                    "ADMIN_ID",
                    "0"
                )
            )
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
# SESSION STATE
# ============================================================

_videocon_waiting = set()

_videocon_jobs = {}

_videocon_jobs_lock = (
    threading.RLock()
)


# ============================================================
# PYROGRAM ENGINE STATE
# ============================================================

_videocon_loop = None

_videocon_loop_thread = None

_videocon_pyro = None

_videocon_ready = (
    threading.Event()
)

_videocon_start_error = None

_videocon_engine_lock = (
    threading.Lock()
)


# ============================================================
# HEALTH SERVER STATE
# ============================================================

_videocon_health_server = None

_videocon_health_thread = None

_videocon_health_lock = (
    threading.Lock()
)


# ============================================================
# SIZE FORMAT
# ============================================================

def videocon_size(value):

    try:

        value = int(value)

    except Exception:

        return "0 B"


    if value >= 1024 ** 4:

        return (
            f"{value / (1024 ** 4):.2f} TB"
        )


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
        f"{value} B"
    )


# ============================================================
# PROGRESS PERCENT
# ============================================================

def videocon_progress_percent(
    current,
    total
):

    try:

        current = int(current or 0)

        total = int(total or 0)

    except Exception:

        return None


    if total <= 0:

        return None


    if current < 0:

        current = 0


    if current > total:

        current = total


    return (
        current * 100.0 / total
    )


# ============================================================
# PROGRESS BAR
# ============================================================

def videocon_progress_bar(
    current,
    total,
    length=10
):

    try:

        current = int(current or 0)

        total = int(total or 0)

    except Exception:

        return "░" * length


    if total <= 0:

        return "░" * length


    ratio = (
        current / total
    )


    if ratio < 0:

        ratio = 0


    if ratio > 1:

        ratio = 1


    filled = int(
        ratio * length
    )


    if filled > length:

        filled = length


    return (
        "█" * filled
        +
        "░" * (length - filled)
    )


# ============================================================
# SPEED
# ============================================================

def videocon_speed(
    current,
    progress_state
):

    try:

        current = int(current or 0)

        now = time.time()

        start_time = (
            progress_state.get(
                "start_time"
            )
        )

        start_bytes = (
            progress_state.get(
                "start_bytes",
                current
            )
        )


        if not start_time:

            progress_state[
                "start_time"
            ] = now

            progress_state[
                "start_bytes"
            ] = current

            return "Calculating..."


        elapsed = (
            now - start_time
        )


        if elapsed <= 0:

            return "Calculating..."


        transferred = (
            current - start_bytes
        )


        if transferred <= 0:

            return "Calculating..."


        speed = (
            transferred / elapsed
        )


        if speed <= 0:

            return "Calculating..."


        return (
            f"{videocon_size(speed)}/s"
        )

    except Exception:

        return "Calculating..."


# ============================================================
# ETA
# ============================================================

def videocon_eta(
    current,
    total,
    progress_state
):

    try:

        current = int(current or 0)

        total = int(total or 0)

    except Exception:

        return "--:--"


    if total <= 0:

        return "--:--"


    if current >= total:

        return "00:00"


    try:

        now = time.time()

        start_time = (
            progress_state.get(
                "start_time"
            )
        )

        start_bytes = (
            progress_state.get(
                "start_bytes",
                current
            )
        )


        if not start_time:

            return "--:--"


        elapsed = (
            now - start_time
        )


        if elapsed <= 0:

            return "--:--"


        transferred = (
            current - start_bytes
        )


        if transferred <= 0:

            return "--:--"


        speed = (
            transferred / elapsed
        )


        if speed <= 0:

            return "--:--"


        remaining = (
            total - current
        )


        seconds = int(
            remaining / speed
        )


        if seconds < 0:

            seconds = 0


        hours = (
            seconds // 3600
        )

        minutes = (
            (seconds % 3600) // 60
        )

        secs = (
            seconds % 60
        )


        if hours > 0:

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

        return "--:--"


# ============================================================
# STATUS EDIT
# ============================================================

def videocon_edit_status(
    status_message,
    text
):

    if not status_message:

        return False


    try:

        telegram_bot = (
            globals().get(
                "bot"
            )
        )


        if not telegram_bot:

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


    except Exception:

        return False


# ============================================================
# ASYNC STATUS EDIT
# ============================================================

async def _videocon_async_status_edit(
    status_message,
    text
):

    try:

        await asyncio.to_thread(

            videocon_edit_status,

            status_message,

            text

        )

    except Exception:

        pass


# ============================================================
# PROGRESS TEXT
# ============================================================

def videocon_build_progress_text(

    mode,

    current,

    total,

    progress_state

):

    percent = (
        videocon_progress_percent(
            current,
            total
        )
    )


    bar = (
        videocon_progress_bar(
            current,
            total
        )
    )


    speed = (
        videocon_speed(
            current,
            progress_state
        )
    )


    eta = (
        videocon_eta(
            current,
            total,
            progress_state
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


    if percent is None:

        percent_text = (
            "<b>Preparing...</b>"
        )

    else:

        percent_text = (
            f"<b>{percent:.1f}%</b>"
        )


    return (

        f"{title}\n\n"

        f"<code>{bar}</code> "
        f"{percent_text}\n\n"

        f"📦 "
        f"<b>{videocon_size(current)}</b>"
        f" / "
        f"<b>{videocon_size(total)}</b>\n\n"

        f"⚡ Speed: "
        f"<b>{speed}</b>\n"

        f"⏱ ETA: "
        f"<b>{eta}</b>\n\n"

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

        current = int(
            current or 0
        )

        total = int(
            total or 0
        )


        now = time.time()


        if not progress_state.get(
            "start_time"
        ):

            progress_state[
                "start_time"
            ] = now

            progress_state[
                "start_bytes"
            ] = current


        last_time = (
            progress_state.get(
                "time",
                0
            )
        )


        # ----------------------------------------------------
        # EDIT EVERY 10 SECONDS
        # ----------------------------------------------------

        if (

            last_time

            and

            (
                now - last_time
            )
            <
            VIDEOCON_PROGRESS_INTERVAL

            and

            current < total

        ):

            return


        progress_state[
            "time"
        ] = now


        text = (
            videocon_build_progress_text(

                mode,

                current,

                total,

                progress_state

            )
        )


        await _videocon_async_status_edit(

            status_message,

            text

        )


    except Exception:

        pass


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

class _VideoconHealthHandler(
    BaseHTTPRequestHandler
):

    def do_GET(self):

        try:

            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "text/plain; charset=utf-8"
            )

            self.end_headers()

            self.wfile.write(
                b"VIDEOCON OK\n"
            )

        except Exception:

            pass


    def log_message(
        self,
        format,
        *args
    ):

        return


# ============================================================
# START HEALTH SERVER
# ============================================================

def start_videocon_health_server():

    global _videocon_health_server
    global _videocon_health_thread


    if not VIDEOCON_HEALTH_ENABLED:

        return


    with _videocon_health_lock:

        if (

            _videocon_health_thread

            and

            _videocon_health_thread.is_alive()

        ):

            return


        port_text = os.getenv(
            "PORT",
            "10000"
        )


        try:

            port = int(
                port_text
            )

        except Exception:

            port = 10000


        try:

            _videocon_health_server = (
                ThreadingHTTPServer(

                    (
                        "0.0.0.0",
                        port
                    ),

                    _VideoconHealthHandler

                )
            )


            _videocon_health_thread = (
                threading.Thread(

                    target=(
                        _videocon_health_server.serve_forever
                    ),

                    daemon=True,

                    name="videocon-health-server"

                )
            )


            _videocon_health_thread.start()


        except OSError:

            # Idan babban app dinka ya riga ya mallaki PORT,
            # ba matsala bane.
            pass

        except Exception:

            pass


# ============================================================
# PYROGRAM ENGINE THREAD
# ============================================================

def _videocon_pyrogram_thread():

    global _videocon_loop
    global _videocon_pyro
    global _videocon_start_error


    _videocon_start_error = None

    local_loop = None
    local_client = None


    try:

        # ====================================================
        # VALIDATION
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

        local_loop = (
            asyncio.new_event_loop()
        )

        _videocon_loop = local_loop


        asyncio.set_event_loop(
            local_loop
        )


        # ====================================================
        # ASYNCIO EXCEPTION HANDLER
        # ====================================================

        def async_exception_handler(
            loop,
            context
        ):

            # Ba mu spam admin ba.
            pass


        local_loop.set_exception_handler(
            async_exception_handler
        )


        # ====================================================
        # PYROGRAM CLIENT
        # ====================================================

        local_client = Client(

            VIDEOCON_SESSION_NAME,

            api_id=API_ID,

            api_hash=API_HASH,

            bot_token=BOT_TOKEN,

            no_updates=True,

            max_concurrent_transmissions=1,

            sleep_threshold=30

        )


        _videocon_pyro = local_client


        # ====================================================
        # START
        # ====================================================

        async def start_client():

            await local_client.start()


        local_loop.run_until_complete(
            start_client()
        )


        # ====================================================
        # READY
        # ====================================================

        _videocon_ready.set()


        # ====================================================
        # KEEP LOOP ALIVE
        # ====================================================

        local_loop.run_forever()


    except Exception as e:

        _videocon_start_error = e

        _videocon_ready.set()


    finally:

        # ====================================================
        # SAFE PYROGRAM SHUTDOWN
        # ====================================================

        try:

            if (
                local_client
                and
                local_loop
                and
                not local_loop.is_closed()
            ):

                is_connected = False

                try:

                    is_connected = (
                        local_client.is_connected
                    )

                except Exception:

                    pass


                if is_connected:

                    local_loop.run_until_complete(
                        local_client.stop()
                    )

        except Exception:

            pass


        # ====================================================
        # CLOSE LOOP
        # ====================================================

        try:

            if (
                local_loop
                and
                not local_loop.is_closed()
            ):

                local_loop.close()

        except Exception:

            pass


        if _videocon_loop is local_loop:

            _videocon_loop = None


        if _videocon_pyro is local_client:

            _videocon_pyro = None


# ============================================================
# START / RESTART PYROGRAM
# ============================================================

def start_videocon_engine():

    global _videocon_loop_thread
    global _videocon_start_error


    with _videocon_engine_lock:

        # ----------------------------------------------------
        # IF THREAD IS ALREADY ALIVE
        # ----------------------------------------------------

        if (

            _videocon_loop_thread

            and

            _videocon_loop_thread.is_alive()

        ):

            return


        # ----------------------------------------------------
        # RESET STATE
        # ----------------------------------------------------

        _videocon_ready.clear()

        _videocon_start_error = None


        # ----------------------------------------------------
        # START NEW THREAD
        # ----------------------------------------------------

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


# ============================================================
# ASYNC OPERATION TIMEOUT WRAPPER
# ============================================================

async def _videocon_timeout_wrapper(
    coro,
    timeout
):

    return await asyncio.wait_for(
        coro,
        timeout=timeout
    )


# ============================================================
# RUN COROUTINE SAFELY
# ============================================================

def videocon_run_async(

    coro,

    timeout=VIDEOCON_OPERATION_TIMEOUT

):

    if not _videocon_loop:

        try:

            coro.close()

        except Exception:

            pass

        raise RuntimeError(
            "Pyrogram event loop bai fara ba."
        )


    if _videocon_start_error:

        try:

            coro.close()

        except Exception:

            pass

        raise RuntimeError(

            "Pyrogram startup failed: "
            f"{_videocon_start_error}"

        )


    if (

        not _videocon_loop_thread

        or

        not _videocon_loop_thread.is_alive()

    ):

        try:

            coro.close()

        except Exception:

            pass

        raise RuntimeError(
            "Pyrogram thread baya aiki."
        )


    # --------------------------------------------------------
    # IMPORTANT:
    #
    # wait_for yana cancel coroutine idan timeout ya faru.
    # Wannan ya fi future.cancel() kadai.
    # --------------------------------------------------------

    wrapped = (
        _videocon_timeout_wrapper(
            coro,
            timeout
        )
    )


    future = (
        asyncio.run_coroutine_threadsafe(

            wrapped,

            _videocon_loop

        )
    )


    try:

        return future.result(
            timeout=timeout + 30
        )


    except TimeoutError:

        future.cancel()

        raise RuntimeError(

            "Pyrogram operation ta wuce "
            f"timeout na "
            f"{timeout // 3600} hours."

        )


    except Exception:

        raise


# ============================================================
# TELEGRAM ERROR FORMATTER
# ============================================================

def videocon_format_telegram_error(
    error
):

    try:

        error_text = str(
            error
        ).strip()

    except Exception:

        error_text = (
            "Unknown Telegram error"
        )


    if not error_text:

        error_text = (
            error.__class__.__name__
        )


    lowered = (
        error_text.lower()
    )


    # --------------------------------------------------------
    # LARGE FILE
    # --------------------------------------------------------

    if any(
        keyword in lowered
        for keyword in (
            "file too big",
            "file size",
            "file_size",
            "too large",
            "maximum file",
            "max file",
            "big file",
            "limit"
        )
    ):

        return (

            "❌ <b>TELEGRAM YA ƘI FILE ƊIN</b>\n\n"

            "Telegram/Pyrogram bai yarda da wannan "
            "girman file ba.\n\n"

            "Aikin ya tsaya kuma za a goge "
            "temporary file daga Render.\n\n"

            f"Telegram: <code>"
            f"{html.escape(error_text)}"
            f"</code>"

        )


    # --------------------------------------------------------
    # FLOOD WAIT
    # --------------------------------------------------------

    if "flood" in lowered:

        return (

            "❌ <b>TELEGRAM FLOOD LIMIT</b>\n\n"

            "Telegram ya bukaci a dakata kafin "
            "a ci gaba da upload.\n\n"

            f"<code>"
            f"{html.escape(error_text)}"
            f"</code>"

        )


    # --------------------------------------------------------
    # GENERIC TELEGRAM ERROR
    # --------------------------------------------------------

    return (

        "❌ <b>TELEGRAM YA ƘI AIKIN</b>\n\n"

        "Telegram ya dawo da error yayin "
        "aikin file.\n\n"

        f"<code>"
        f"{html.escape(error_text)}"
        f"</code>\n\n"

        "Temporary file ɗin za a goge."

    )


# ============================================================
# SAFE FILE NAME
# ============================================================

def videocon_safe_filename(
    filename
):

    filename = (
        os.path.basename(
            str(filename or "")
        )
    )


    filename = (
        filename
        .replace(
            "\x00",
            ""
        )
        .strip()
    )


    if not filename:

        filename = (
            "converted_file"
        )


    return filename


# ============================================================
# VIDEO METADATA
# ============================================================
#
# Wannan yana gyara matsalar:
#
# File → Video
#
# inda Telegram zai iya nuna:
#
# 0:00
#
# Muna amfani da ffprobe ne kawai domin karanta:
#
# - duration
# - width
# - height
#
# BA A CONVERTING.
# BA A COMPRESSING.
# BA A SAKE ENCODE.
#
# Idan ffprobe baya nan, za mu tura video ba tare da
# metadata ba. Telegram zai iya karɓa ko ya ƙi shi.
# ============================================================

def videocon_get_video_metadata(
    input_file
):

    metadata = {

        "duration": None,

        "width": None,

        "height": None

    }


    ffprobe = shutil.which(
        "ffprobe"
    )


    if not ffprobe:

        return metadata


    try:

        command = [

            ffprobe,

            "-v",
            "error",

            "-select_streams",
            "v:0",

            "-show_entries",
            "stream=width,height:format=duration",

            "-of",
            "json",

            input_file

        ]


        process = (
            subprocess.run(

                command,

                stdout=subprocess.PIPE,

                stderr=subprocess.PIPE,

                text=True,

                timeout=60

            )
        )


        if process.returncode != 0:

            return metadata


        data = json.loads(
            process.stdout
        )


        streams = (
            data.get(
                "streams",
                []
            )
        )


        formats = (
            data.get(
                "format",
                {}
            )
        )


        # ----------------------------------------------------
        # WIDTH / HEIGHT
        # ----------------------------------------------------

        if streams:

            stream = streams[0]


            try:

                width = int(
                    stream.get(
                        "width"
                    )
                    or 0
                )

            except Exception:

                width = 0


            try:

                height = int(
                    stream.get(
                        "height"
                    )
                    or 0
                )

            except Exception:

                height = 0


            if width > 0:

                metadata[
                    "width"
                ] = width


            if height > 0:

                metadata[
                    "height"
                ] = height


        # ----------------------------------------------------
        # DURATION
        # ----------------------------------------------------

        duration_value = (
            formats.get(
                "duration"
            )
        )


        if duration_value is not None:

            try:

                duration = int(
                    float(
                        duration_value
                    )
                )

                if duration >= 0:

                    metadata[
                        "duration"
                    ] = duration

            except Exception:

                pass


    except Exception:

        pass


    return metadata


# ============================================================
# CHECK IF FILE LOOKS LIKE VIDEO
# ============================================================

def videocon_is_video_file(
    input_file
):

    metadata = (
        videocon_get_video_metadata(
            input_file
        )
    )


    if (

        metadata.get("duration")
        is not None

        or

        (
            metadata.get("width")
            and
            metadata.get("height")
        )

    ):

        return True


    # --------------------------------------------------------
    # Extension fallback
    # --------------------------------------------------------

    extension = (
        os.path.splitext(
            input_file
        )[1].lower()
    )


    video_extensions = {

        ".mp4",
        ".mkv",
        ".mov",
        ".avi",
        ".webm",
        ".m4v",
        ".3gp",
        ".ts",
        ".mpeg",
        ".mpg"

    }


    return (
        extension in video_extensions
    )


# ============================================================
# /VIDEOCON COMMAND
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
    # DO NOT DESTROY AN ACTIVE JOB
    # ========================================================

    with _videocon_jobs_lock:

        existing_job = (
            _videocon_jobs.get(
                user_id
            )
        )


        if (
            existing_job
            and
            existing_job.get(
                "state"
            )
            == "processing"
        ):

            try:

                bot.reply_to(

                    message,

                    (
                        "⏳ Akwai wani aikin "
                        "Video Converter da yake gudana.\n\n"
                        "Ka jira ya gama kafin ka fara wani."
                    )

                )

            except Exception:

                pass

            return


        _videocon_waiting.add(
            user_id
        )


        _videocon_jobs.pop(
            user_id,
            None
        )


    # ========================================================
    # START PYROGRAM
    # ========================================================

    try:

        start_videocon_engine()

    except Exception:

        pass


    # ========================================================
    # REPLY
    # ========================================================

    try:

        bot.reply_to(

            message,

            (
                "🎬 <b>VIDEO CONVERTER</b>\n\n"

                "Turo min <b>Video</b> ko "
                "<b>File/Document</b> ɗin "
                "da kake son mu dawo maka da shi.\n\n"

                "📦 <b>Ba mu saka artificial GB limit ba.</b>\n\n"

                "Telegram/Pyrogram ne zai yanke "
                "ainihin abin da zai iya karɓa.\n\n"

                "⏳ Bayan ka turo shi zan tambaye ka "
                "irin yadda kake son na dawo maka da shi."

            ),

            parse_mode="HTML"

        )

    except Exception:

        pass


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


    if user_id != ADMIN_ID:

        with _videocon_jobs_lock:

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


        file_name = (
            getattr(
                message.video,
                "file_name",
                None
            )
            or "converted_video.mp4"
        )


        file_name = (
            videocon_safe_filename(
                file_name
            )
        )


        with _videocon_jobs_lock:

            _videocon_waiting.discard(
                user_id
            )


            _videocon_jobs[user_id] = {

                "file_id":
                    file_id,

                "file_type":
                    "video",

                "file_name":
                    file_name,

                "file_size":
                    file_size,

                "message_id":
                    message.message_id,

                "chat_id":
                    message.chat.id,

                "state":
                    "waiting_choice"

            }


        keyboard = (
            types.InlineKeyboardMarkup(
                row_width=2
            )
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


        size_text = (
            videocon_size(file_size)
            if file_size > 0
            else
            "Unknown"
        )


        bot.reply_to(

            message,

            (
                "✅ <b>VIDEO RECEIVED</b>\n\n"

                f"📦 Size: "
                f"<b>{size_text}</b>\n\n"

                "❓ <b>Me kake so na dawo maka da shi?</b>"
            ),

            reply_markup=keyboard,

            parse_mode="HTML"

        )


    except Exception:

        try:

            bot.reply_to(
                message,
                "❌ An samu matsala wajen karɓar video."
            )

        except Exception:

            pass


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


    if user_id != ADMIN_ID:

        with _videocon_jobs_lock:

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


        file_name = (
            videocon_safe_filename(
                file_name
            )
        )


        file_id = (
            message.document.file_id
        )


        with _videocon_jobs_lock:

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
                    message.chat.id,

                "state":
                    "waiting_choice"

            }


        keyboard = (
            types.InlineKeyboardMarkup(
                row_width=2
            )
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


        size_text = (
            videocon_size(file_size)
            if file_size > 0
            else
            "Unknown"
        )


        bot.reply_to(

            message,

            (
                "✅ <b>FILE RECEIVED</b>\n\n"

                f"📄 Name: "
                f"<b>{html.escape(file_name)}</b>\n\n"

                f"📦 Size: "
                f"<b>{size_text}</b>\n\n"

                "❓ <b>Me kake so na dawo maka da shi?</b>"
            ),

            reply_markup=keyboard,

            parse_mode="HTML"

        )


    except Exception:

        try:

            bot.reply_to(
                message,
                "❌ An samu matsala wajen karɓar file."
            )

        except Exception:

            pass


# ============================================================
# CALLBACK
# ============================================================

@bot.callback_query_handler(

    func=lambda call:
        bool(call.data)
        and
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


    # ========================================================
    # ADMIN ONLY
    # ========================================================

    if user_id != ADMIN_ID:

        try:

            bot.answer_callback_query(

                call.id,

                "❌ Admin kawai."

            )

        except Exception:

            pass

        return


    try:

        choice = (
            call.data.split(
                ":",
                1
            )[1]
        )

    except Exception:

        return


    if choice not in (
        "video",
        "file"
    ):

        try:

            bot.answer_callback_query(

                call.id,

                "❌ Invalid option."

            )

        except Exception:

            pass

        return


    # ========================================================
    # LOCK JOB
    # ========================================================

    with _videocon_jobs_lock:

        job = (
            _videocon_jobs.get(
                user_id
            )
        )


        if not job:

            job = None

        else:

            current_state = (
                job.get(
                    "state"
                )
            )


            if current_state == "processing":

                job = None

            else:

                job["state"] = (
                    "processing"
                )


    if not job:

        try:

            bot.answer_callback_query(

                call.id,

                (
                    "❌ Session ta ƙare "
                    "ko aikin yana gudana."
                )

            )

        except Exception:

            pass

        return


    # ========================================================
    # REMOVE BUTTONS
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

    except Exception:

        pass


    # ========================================================
    # CALLBACK ANSWER
    # ========================================================

    try:

        bot.answer_callback_query(

            call.id,

            "🚀 An fara aiki..."

        )

    except Exception:

        pass


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
                    f"<b>"
                    f"{html.escape(str(job.get('file_type')))}"
                    f"</b>\n"

                    f"📤 Output: "
                    f"<b>{html.escape(choice)}</b>\n"

                    f"📦 Size: "
                    f"<b>"
                    f"{videocon_size(job.get('file_size', 0))}"
                    f"</b>\n\n"

                    "🔧 Ana shirya Pyrogram..."
                ),

                parse_mode="HTML"

            )
        )

    except Exception:

        pass


    # ========================================================
    # WORKER
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


    except Exception:

        with _videocon_jobs_lock:

            current_job = (
                _videocon_jobs.get(
                    user_id
                )
            )

            if current_job:

                current_job[
                    "state"
                ] = "waiting_choice"


        try:

            bot.send_message(

                user_id,

                "❌ An kasa fara aikin converter."

            )

        except Exception:

            pass


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
        # START
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
        # WAIT FOR PYROGRAM
        # ====================================================

        if not _videocon_ready.wait(
            timeout=VIDEOCON_ENGINE_START_TIMEOUT
        ):

            # Try one restart if engine died before ready.
            try:

                start_videocon_engine()

            except Exception:

                pass


            raise RuntimeError(

                "Pyrogram engine bai fara cikin "
                f"{VIDEOCON_ENGINE_START_TIMEOUT} seconds ba."

            )


        if _videocon_start_error:

            raise RuntimeError(

                "Pyrogram startup failed: "
                f"{_videocon_start_error}"

            )


        if not _videocon_pyro:

            raise RuntimeError(
                "Pyrogram client baya nan."
            )


        # ====================================================
        # TEMP DIRECTORY
        # ====================================================

        temp_dir = (
            tempfile.mkdtemp(
                prefix="videocon_"
            )
        )


        # ====================================================
        # DISK CHECK
        # ====================================================

        total_disk, used_disk, free_disk = (
            shutil.disk_usage(
                temp_dir
            )
        )


        expected_size = int(
            job.get(
                "file_size",
                0
            )
            or 0
        )


        if expected_size > 0:

            required_space = (
                expected_size
                +
                VIDEOCON_MIN_FREE_BYTES
            )


            if free_disk < required_space:

                raise RuntimeError(

                    "❌ Render disk bai da isasshen space.\n\n"

                    f"📦 File: "
                    f"{videocon_size(expected_size)}\n"

                    f"💾 Free: "
                    f"{videocon_size(free_disk)}\n"

                    f"💾 Required: "
                    f"{videocon_size(required_space)}"

                )


        else:

            # ------------------------------------------------
            # Unknown file size.
            #
            # Ba mu saka artificial limit ba.
            #
            # Za mu fara download, sannan mu duba actual
            # downloaded size.
            # ------------------------------------------------

            if free_disk <= (
                VIDEOCON_MIN_FREE_BYTES
            ):

                raise RuntimeError(

                    "❌ Render disk free space ya yi ƙasa sosai.\n\n"

                    f"💾 Free: "
                    f"{videocon_size(free_disk)}"

                )


        # ====================================================
        # FILE NAME
        # ====================================================

        original_name = (
            videocon_safe_filename(
                job.get(
                    "file_name"
                )
            )
        )


        input_file = os.path.join(

            temp_dir,

            original_name

        )


        # ====================================================
        # GET ORIGINAL TELEGRAM MESSAGE
        # ====================================================

        videocon_edit_status(

            status_message,

            (
                "🔎 <b>CHECKING TELEGRAM...</b>\n\n"

                "Ana neman original message...\n\n"

                f"📦 <b>"
                f"{videocon_size(expected_size)}"
                f"</b>"
            )

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


        # ====================================================
        # DOWNLOAD
        # ====================================================

        videocon_edit_status(

            status_message,

            (
                "⬇️ <b>DOWNLOADING...</b>\n\n"

                "<code>░░░░░░░░░░</code> "
                "<b>Preparing...</b>\n\n"

                f"📦 <b>0 B</b>"
                f" / "
                f"<b>"
                f"{videocon_size(expected_size)}"
                f"</b>\n\n"

                "⚡ Speed: <b>Calculating...</b>\n"

                "⏱ ETA: <b>--:--</b>\n\n"

                "🔄 Telegram → Render\n\n"

                "⏳ Please wait..."
            )

        )


        download_state = {

            "time": 0,

            "start_time": 0,

            "start_bytes": 0

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


        if not downloaded_path:

            raise RuntimeError(
                "Pyrogram download ya dawo babu path."
            )


        input_file = (
            downloaded_path
        )


        # ====================================================
        # VERIFY DOWNLOAD PATH
        # ====================================================

        if not os.path.exists(
            input_file
        ):

            raise RuntimeError(
                "Downloaded file bai bayyana a disk ba."
            )


        downloaded_size = (
            os.path.getsize(
                input_file
            )
        )


        if downloaded_size <= 0:

            raise RuntimeError(
                "Downloaded file empty ne."
            )


        # ====================================================
        # VERIFY DOWNLOAD SIZE
        # ====================================================

        if (

            expected_size > 0

            and

            downloaded_size != expected_size

        ):

            raise RuntimeError(

                "Download bai kammala daidai ba.\n\n"

                f"Expected: "
                f"{videocon_size(expected_size)}\n"

                f"Downloaded: "
                f"{videocon_size(downloaded_size)}"

            )


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

                "📤 Yanzu ana aikawa Telegram...\n\n"

                "⏳ Please wait..."
            )

        )


        # ====================================================
        # UPLOAD STATE
        # ====================================================

        upload_state = {

            "time": 0,

            "start_time": 0,

            "start_bytes": 0

        }


        # ====================================================
        # UPLOAD AS FILE
        # ====================================================

        if choice == "file":

            videocon_edit_status(

                status_message,

                (
                    "📤 <b>UPLOADING AS FILE...</b>\n\n"

                    "<code>░░░░░░░░░░</code> "
                    "<b>Preparing...</b>\n\n"

                    f"📦 <b>0 B</b>"
                    f" / "
                    f"<b>"
                    f"{videocon_size(downloaded_size)}"
                    f"</b>\n\n"

                    "⚡ Speed: <b>Calculating...</b>\n"

                    "⏱ ETA: <b>--:--</b>\n\n"

                    "🔄 Render → Telegram\n\n"

                    "⏳ Please wait..."
                )

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


        # ====================================================
        # UPLOAD AS VIDEO
        # ====================================================

        elif choice == "video":

            # ------------------------------------------------
            # Check actual video.
            #
            # Wannan yana hana a tura PDF/ZIP/etc. a matsayin
            # video kawai saboda an danna Video.
            # ------------------------------------------------

            if not videocon_is_video_file(
                input_file
            ):

                raise RuntimeError(

                    "❌ Wannan file ba video bane.\n\n"

                    "Ba a yi conversion ba saboda tsarin "
                    "converter ɗin baya converting/compressing "
                    "file.\n\n"

                    "Idan file ɗin video ne amma extension "
                    "ɗinsa ba a gane shi ba, Telegram na iya "
                    "ƙin karɓarsa a matsayin Video."

                )


            videocon_edit_status(

                status_message,

                (
                    "🎬 <b>UPLOADING AS VIDEO...</b>\n\n"

                    "<code>░░░░░░░░░░</code> "
                    "<b>Preparing...</b>\n\n"

                    f"📦 <b>0 B</b>"
                    f" / "
                    f"<b>"
                    f"{videocon_size(downloaded_size)}"
                    f"</b>\n\n"

                    "⚡ Speed: <b>Calculating...</b>\n"

                    "⏱ ETA: <b>--:--</b>\n\n"

                    "🔄 Render → Telegram\n\n"

                    "⏳ Please wait..."
                )

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


        else:

            raise RuntimeError(
                "Invalid output choice."
            )


        # ====================================================
        # UPLOAD VERIFICATION
        # ====================================================

        if not result:

            raise RuntimeError(
                "Telegram upload bai dawo da successful message ba."
            )


        # ====================================================
        # SUCCESS
        # ====================================================

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


    # ========================================================
    # TELEGRAM RPC ERROR
    # ========================================================

    except RPCError as e:

        try:

            error_text = (
                videocon_format_telegram_error(
                    e
                )
            )


            if status_message:

                videocon_edit_status(

                    status_message,

                    error_text

                )

            else:

                bot.send_message(

                    user_id,

                    error_text,

                    parse_mode="HTML"

                )

        except Exception:

            pass


    # ========================================================
    # GENERAL ERROR
    # ========================================================

    except Exception as e:

        try:

            safe_error = (
                str(e)
            )


            if len(safe_error) > 1800:

                safe_error = (
                    safe_error[:1800]
                    +
                    "..."
                )


            safe_error = (
                html.escape(
                    safe_error
                )
            )


            if status_message:

                videocon_edit_status(

                    status_message,

                    (
                        "❌ <b>CONVERSION ERROR</b>\n\n"

                        f"<code>"
                        f"{safe_error}"
                        f"</code>\n\n"

                        "🧹 Za a goge temporary file.\n\n"

                        "Ka sake gwadawa."

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


        except Exception:

            pass


    finally:

        # ====================================================
        # CLEANUP TEMP DIRECTORY
        # ====================================================

        try:

            if (

                temp_dir

                and

                os.path.exists(
                    temp_dir
                )

            ):

                shutil.rmtree(

                    temp_dir,

                    ignore_errors=True

                )

        except Exception:

            pass


        # ====================================================
        # REMOVE JOB STATE
        # ====================================================

        try:

            with _videocon_jobs_lock:

                _videocon_jobs.pop(
                    user_id,
                    None
                )

                _videocon_waiting.discard(
                    user_id
                )

        except Exception:

            pass


# ============================================================
# PYROGRAM — GET MESSAGE
# ============================================================

async def _videocon_get_message(

    chat_id,

    message_id

):

    message = (
        await
        _videocon_pyro.get_messages(

            chat_id,

            message_id

        )
    )


    if not message:

        return None


    return message


# ============================================================
# PYROGRAM — DOWNLOAD
# ============================================================

async def _videocon_download(

    message,

    output_path,

    status_message,

    progress_state

):

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


    result = (
        await
        _videocon_pyro.download_media(

            message,

            file_name=output_path,

            progress=progress

        )
    )


    # --------------------------------------------------------
    # FINAL DOWNLOAD PROGRESS
    # --------------------------------------------------------

    if result:

        try:

            final_size = (
                os.path.getsize(
                    result
                )
            )

        except Exception:

            final_size = 0


        if final_size > 0:

            await videocon_transfer_progress(

                final_size,

                final_size,

                status_message,

                "download",

                progress_state

            )


    return result


# ============================================================
# PYROGRAM — UPLOAD DOCUMENT
# ============================================================

async def _videocon_upload_document(

    user_id,

    input_file,

    original_name,

    file_size,

    status_message,

    progress_state

):

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


    result = (
        await
        _videocon_pyro.send_document(

            chat_id=user_id,

            document=input_file,

            file_name=original_name,

            caption=(

                "✅ <b>File Converted</b>\n\n"

                f"📄 Name: "
                f"<b>"
                f"{html.escape(original_name)}"
                f"</b>\n\n"

                f"📦 Size: "
                f"<b>"
                f"{videocon_size(file_size)}"
                f"</b>\n\n"

                "📁 Video Converter"

            ),

            progress=progress

        )
    )


    # --------------------------------------------------------
    # FINAL UPLOAD PROGRESS
    # --------------------------------------------------------

    if result:

        await videocon_transfer_progress(

            file_size,

            file_size,

            status_message,

            "upload",

            progress_state

        )


    return result


# ============================================================
# PYROGRAM — UPLOAD VIDEO
# ============================================================

async def _videocon_upload_video(

    user_id,

    input_file,

    original_name,

    file_size,

    status_message,

    progress_state

):

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


    # ========================================================
    # GET VIDEO METADATA
    # ========================================================
    #
    # Wannan shine babban gyaran File → Video.
    #
    # Muna karanta metadata kawai.
    # Ba conversion bane.
    # ========================================================

    metadata = (
        videocon_get_video_metadata(
            input_file
        )
    )


    duration = (
        metadata.get(
            "duration"
        )
    )


    width = (
        metadata.get(
            "width"
        )
    )


    height = (
        metadata.get(
            "height"
        )
    )


    # ========================================================
    # BUILD SEND_VIDEO ARGUMENTS
    # ========================================================

    video_kwargs = {

        "chat_id":
            user_id,

        "video":
            input_file,

        "file_name":
            original_name,

        "caption": (

            "✅ <b>Video Converted</b>\n\n"

            f"📄 Name: "
            f"<b>"
            f"{html.escape(original_name)}"
            f"</b>\n\n"

            f"📦 Size: "
            f"<b>"
            f"{videocon_size(file_size)}"
            f"</b>\n\n"

            "🎬 Video Converter"

        ),

        "supports_streaming":
            True,

        "progress":
            progress

    }


    # --------------------------------------------------------
    # ADD DURATION
    # --------------------------------------------------------

    if (
        duration is not None
        and
        duration >= 0
    ):

        video_kwargs[
            "duration"
        ] = duration


    # --------------------------------------------------------
    # ADD WIDTH / HEIGHT
    # --------------------------------------------------------

    if (
        width
        and
        height
        and
        width > 0
        and
        height > 0
    ):

        video_kwargs[
            "width"
        ] = width

        video_kwargs[
            "height"
        ] = height


    # ========================================================
    # SEND VIDEO
    # ========================================================

    result = (
        await
        _videocon_pyro.send_video(
            **video_kwargs
        )
    )


    # ========================================================
    # FINAL UPLOAD PROGRESS
    # ========================================================

    if result:

        await videocon_transfer_progress(

            file_size,

            file_size,

            status_message,

            "upload",

            progress_state

        )


    return result


# ============================================================
# INITIALIZATION
# ============================================================

try:

    start_videocon_health_server()

except Exception:

    pass


# ============================================================
# START PYROGRAM ENGINE
# ============================================================

try:

    start_videocon_engine()

except Exception:

    pass






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

