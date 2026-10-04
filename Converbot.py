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




# ============================================================
# /START — VIDEO CONVERTER + COMPRESSER
# ============================================================

@bot.message_handler(commands=["start"])
def start_command(message):

    user_id = message.from_user.id

    keyboard = types.InlineKeyboardMarkup(row_width=2)

    keyboard.add(

        types.InlineKeyboardButton(
            "🎬 Converter",
            callback_data="videocon:converter"
        ),

        types.InlineKeyboardButton(
            "🗜️ Compresser",
            callback_data="videocon:compresser"
        )

    )

    bot.send_message(

        user_id,

        (
            "👋 <b>WELCOME</b>\n\n"

            "🎬 <b>Video Converter</b>\n"
            "Convert your Telegram files and videos.\n\n"

            "🗜️ <b>Video Compresser</b>\n"
            "Compress your video to a smaller size.\n\n"

            "👇 <b>Zaɓi abin da kake so:</b>"
        ),

        reply_markup=keyboard,

        parse_mode="HTML"
    )


# ============================================================
# START MENU CALLBACK
# ============================================================

@bot.callback_query_handler(
    func=lambda call:
        bool(call.data)
        and call.data.startswith("videocon:")
        and call.data in (
            "videocon:converter",
            "videocon:compresser"
        )
)
def start_menu_callback(call):

    user_id = call.from_user.id

    choice = call.data.split(":", 1)[1]

    # --------------------------------------------------------
    # CONVERTER
    # --------------------------------------------------------

    if choice == "converter":

        try:
            bot.answer_callback_query(
                call.id,
                "🎬 Converter"
            )
        except Exception:
            pass

        try:
            bot.send_message(
                user_id,

                (
                    "🎬 <b>VIDEO CONVERTER</b>\n\n"

                    "Ka yi amfani da:\n\n"

                    "/videocon\n\n"

                    "domin fara aikin Converter."
                ),

                parse_mode="HTML"
            )
        except Exception:
            pass

        return


    # --------------------------------------------------------
    # COMPRESSER
    # --------------------------------------------------------

    if choice == "compresser":

        try:
            bot.answer_callback_query(
                call.id,
                "🗜️ Compresser"
            )
        except Exception:
            pass

        try:
            bot.send_message(
                user_id,

                (
                    "🗜️ <b>VIDEO COMPRESSER</b>\n\n"

                    "🚧 Wannan system ɗin muna "
                    "gina shi yanzu.\n\n"

                    "Da zarar mun gama, zaka iya turo "
                    "fim/video sannan bot zai karanta "
                    "girman file ɗin ya baka zabin "
                    "compression."
                ),

                parse_mode="HTML"
            )
        except Exception:
            pass

        return







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

