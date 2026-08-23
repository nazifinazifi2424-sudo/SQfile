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
PORT_RAW = os.getenv("PORT", "10000").strip()

if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN bai samu ba.")
    sys.exit(1)

if not ADMIN_ID_RAW:
    print("ERROR: ADMIN_ID bai samu ba.")
    sys.exit(1)

if not API_ID_RAW:
    print("ERROR: API_ID bai samu ba.")
    sys.exit(1)

if not API_HASH:
    print("ERROR: API_HASH bai samu ba.")
    sys.exit(1)

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except Exception:
    print("ERROR: ADMIN_ID dole ya zama number.")
    sys.exit(1)

try:
    API_ID = int(API_ID_RAW)
except Exception:
    print("ERROR: API_ID dole ya zama number.")
    sys.exit(1)

try:
    PORT = int(PORT_RAW)
except Exception:
    PORT = 10000



# =============================================================
# =============================================================
#                  CONVERTER SYSTEM
# =============================================================
#
# IMPORTANT:
# DOWNLOAD SYSTEM DA UPLOAD SYSTEM AN RABA SU GABA DAYA.
#
# Idan kana gyaran UPLOAD:
#    Ka yi aiki ne tsakanin:
#
#    #ANAN FARKON SYSTEM UPLOAD
#    ...
#    #NAN SHINE KARSHEN SYSTEM UPLOAD
#
# Kada ka taba DOWNLOAD idan matsalar Upload ce.
#
# =============================================================


# =============================================================
# 2. SETTINGS
# =============================================================

# -------------------------------------------------------------
# UPLOAD SETTINGS
# -------------------------------------------------------------

# Maximum lokacin attempt guda.
UPLOAD_TIMEOUT = 1800

# 2 retries + first attempt = 3 attempts total.
UPLOAD_RETRIES = 2

# Idan babu progress na tsawon wannan lokaci,
# za mu dauki upload a matsayin STALLED.
UPLOAD_STALL_SECONDS = 45

# Yawan seconds tsakanin admin debug updates.
ADMIN_DEBUG_UPDATE_SECONDS = 10

# User progress update.
PROGRESS_UPDATE_SECONDS = 3


# =============================================================
# 3. GLOBAL STATE
# =============================================================

USER_STATES = {}

PENDING_DATA = {}

PROGRESS_STATE = {}

# Upload-specific state.
UPLOAD_STATE = {}

# Admin debug messages.
ADMIN_DEBUG_MESSAGES = {}

PYRO_READY = threading.Event()
PYRO_FAILED = threading.Event()
BOT_READY = threading.Event()

pyro_loop = asyncio.new_event_loop()


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
# 6. TIME / HELPERS
# =============================================================

def now_time():
    return time.strftime(
        "%Y-%m-%d %H:%M:%S"
    )


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

    try:
        size = float(size)
    except Exception:
        return "0 B"

    while size >= power and n < 4:
        size /= power
        n += 1

    return f"{round(size, 2)} {units[n]}"


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

    return (
        result[:-2]
        if result
        else "0s"
    )


# =============================================================
# 7. ADMIN EXCEPTION
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
        f"```text\n"
        f"{tb[-3500:]}"
        f"\n```"
    )

    print(
        "\n" +
        "#" * 80
    )

    print(
        f"ERROR: {title}"
    )

    print(tb)

    print(
        "#" * 80
    )

    try:

        bot.send_message(
            ADMIN_ID,
            text,
            parse_mode="Markdown"
        )

    except Exception as send_error:

        print(
            "[ADMIN ERROR SEND FAILED]",
            send_error
        )


# =============================================================
# 8. SYSTEM INFORMATION
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

        ram_text = (
            "🧠 RAM: psutil unavailable"
        )

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
# 9. STATUS EDITOR
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
            "[STATUS EDIT ERROR]"
            f" chat={chat_id}"
            f" message={message_id}"
            f" error={e}"
        )

        return False


# =============================================================
# =============================================================
#              SYSTEM DOWNLOAD
# =============================================================
#
# Wannan bangaren DOWNLOAD ne kawai.
#
# Kada a gyara wannan lokacin da matsalar Upload ce.
#
# =============================================================


# =============================================================
# DOWNLOAD PROGRESS
# =============================================================

async def download_progress_args(
    current,
    total,
    text_type,
    chat_id,
    message_id,
    start_time
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
            f"{progress} "
            f"`{percentage:.2f}%`\n\n"
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
            "DOWNLOAD"
        )

        last_time = PROGRESS_STATE.get(
            key,
            0
        )

        if (
            now - last_time
            >= PROGRESS_UPDATE_SECONDS
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
            "[DOWNLOAD PROGRESS ERROR]",
            e
        )


# =============================================================
# =============================================================
# DOWNLOAD FUNCTION
# =============================================================

async def download_original_file(
    pyro_message,
    chat_id,
    status_msg_id,
    media_file_name,
    media_size
):

    download_start = time.time()

    await edit_status(
        chat_id,
        status_msg_id,
        (
            "⬇️ *Ana sauke file...*\n\n"
            f"📁 `{media_file_name}`\n"
            f"📦 `{humanbytes(media_size)}`"
        )
    )

    file_path = await pyro_bot.download_media(
        message=pyro_message,

        progress=download_progress_args,

        progress_args=(
            "⬇️ *Ana Sauke...*",
            chat_id,
            status_msg_id,
            download_start
        )
    )

    if not file_path:
        raise Exception(
            "Pyrogram ya kasa samar da local file."
        )

    if not os.path.exists(file_path):
        raise Exception(
            f"Local file bai wanzu ba:\n"
            f"{file_path}"
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

    download_time = (
        time.time() -
        download_start
    )

    print(
        "\n[DOWNLOAD SUCCESS]"
    )

    print(
        f"File: {media_file_name}"
    )

    print(
        f"Size: {humanbytes(local_size)}"
    )

    print(
        f"Time: {download_time:.1f}s"
    )

    return (
        file_path,
        local_size,
        download_time
    )


# =============================================================
# UPLOAD SYSTEM - FULL STABLE VERSION
# =============================================================

import os
import time
import math
import asyncio
import inspect
import threading
import traceback

from http.server import HTTPServer, BaseHTTPRequestHandler


# =============================================================
# UPLOAD CONFIG
# =============================================================

UPLOAD_RETRIES = 2
UPLOAD_TIMEOUT = 300

# Idan babu sabon progress na wannan lokaci,
# za a dauki upload din a matsayin stalled.
UPLOAD_STALL_SECONDS = 45

# Sau nawa admin debug zai update.
ADMIN_DEBUG_UPDATE_SECONDS = 5

# Sau nawa user progress zai update.
PROGRESS_UPDATE_SECONDS = 3

# Lokacin jira kafin sabon retry.
UPLOAD_RETRY_DELAY = 5

# FloodWait max wait kafin mu ci gaba.
# Idan Telegram ya ce jira fiye da wannan,
# za mu jira shi maimakon karya upload.
MAX_FLOODWAIT = 3600


# =============================================================
# GLOBAL UPLOAD STATES
# =============================================================

UPLOAD_STATE = {}

PROGRESS_STATE = {}


# =============================================================
# SAFE ADMIN DEBUG SEND
# =============================================================

async def admin_debug_send(
    text
):

    try:

        result = await asyncio.to_thread(

            bot.send_message,

            ADMIN_ID,

            text,

            parse_mode="Markdown"
        )

        return result

    except Exception as e:

        print(
            "[ADMIN DEBUG SEND ERROR]",
            repr(e)
        )

        return None


# =============================================================
# SAFE ADMIN DEBUG EDIT
# =============================================================

async def admin_debug_edit(
    debug_message_id,
    text
):

    if not debug_message_id:

        return

    try:

        await asyncio.to_thread(

            bot.edit_message_text,

            text,

            ADMIN_ID,

            debug_message_id,

            parse_mode="Markdown"
        )

    except Exception as e:

        print(
            "[ADMIN DEBUG EDIT ERROR]",
            repr(e)
        )


# =============================================================
# SAFE DEBUG VALUE
# =============================================================

def safe_debug_value(
    value
):

    if value is None:

        return "None"

    try:

        text = str(value)

    except Exception:

        text = repr(value)

    text = text.replace(
        "`",
        "'"
    )

    return text[:800]


# =============================================================
# SAFE FILE SIZE
# =============================================================

def safe_file_size(
    file_path
):

    try:

        if not file_path:

            return 0

        if not os.path.exists(
            file_path
        ):

            return 0

        return os.path.getsize(
            file_path
        )

    except Exception:

        return 0


# =============================================================
# SAFE TASK CANCEL
# =============================================================

async def cancel_task_safely(
    task,
    name="TASK"
):

    if not task:

        return

    if task.done():

        return

    try:

        task.cancel()

    except Exception as e:

        print(
            f"[{name} CANCEL ERROR]",
            repr(e)
        )

        return

    try:

        await asyncio.gather(
            task,
            return_exceptions=True
        )

    except Exception as e:

        print(
            f"[{name} WAIT AFTER CANCEL ERROR]",
            repr(e)
        )


# =============================================================
# UPLOAD PROGRESS CALLBACK
# =============================================================

async def upload_progress_args(
    current,
    total,
    text_type,
    chat_id,
    status_msg_id,
    start_time,
    attempt,
    upload_state
):

    try:

        now = time.time()

        # -----------------------------------------------------
        # PROTECT AGAINST BAD VALUES
        # -----------------------------------------------------

        try:

            current = int(
                current or 0
            )

        except Exception:

            current = 0

        try:

            total = int(
                total or 0
            )

        except Exception:

            total = 0

        if current < 0:

            current = 0

        if total < 0:

            total = 0

        # -----------------------------------------------------
        # TIME
        # -----------------------------------------------------

        elapsed = (
            now -
            start_time
        )

        if elapsed <= 0:

            elapsed = 0.001

        # -----------------------------------------------------
        # PERCENTAGE
        # -----------------------------------------------------

        if total > 0:

            percentage = (
                current *
                100.0 /
                total
            )

        else:

            percentage = 0.0

        percentage = max(
            0.0,
            min(
                100.0,
                percentage
            )
        )

        # -----------------------------------------------------
        # SPEED
        # -----------------------------------------------------

        speed = (
            current /
            elapsed
        )

        if speed < 0:

            speed = 0

        # -----------------------------------------------------
        # ETA
        # -----------------------------------------------------

        if speed > 0 and total > current:

            remaining_seconds = (
                total -
                current
            ) / speed

        else:

            remaining_seconds = 0

        try:

            eta = TimeFormatter(
                remaining_seconds * 1000
            )

        except Exception:

            eta = "N/A"

        # -----------------------------------------------------
        # PROGRESS BAR
        # -----------------------------------------------------

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

            +

            ("▰" * blocks)

            +

            ("▱" * (10 - blocks))

            +

            "]"
        )

        # =====================================================
        # CRITICAL STATE UPDATE
        # =====================================================
        #
        # Wannan yana faruwa kafin edit_status.
        #
        # Saboda idan Telegram message edit ya kasa,
        # watchdog zai ci gaba da ganin cewa upload
        # yana samun progress.
        # =====================================================

        upload_state["current"] = current

        upload_state["total"] = total

        upload_state["percentage"] = percentage

        upload_state["speed"] = speed

        upload_state["last_progress"] = now

        upload_state["last_progress_bytes"] = current

        upload_state["callback_count"] = (

            upload_state.get(
                "callback_count",
                0
            )

            +

            1
        )

        upload_state["last_callback_time"] = now

        # -----------------------------------------------------
        # DETECT REAL PROGRESS
        # -----------------------------------------------------

        previous_bytes = upload_state.get(
            "previous_progress_bytes",
            0
        )

        if current > previous_bytes:

            upload_state[
                "last_real_progress"
            ] = now

            upload_state[
                "last_real_progress_bytes"
            ] = current

            upload_state[
                "previous_progress_bytes"
            ] = current

        # -----------------------------------------------------
        # USER STATUS
        # -----------------------------------------------------

        status_text = (

            f"{text_type}\n\n"

            f"{progress} "
            f"`{percentage:.2f}%`\n\n"

            f"📊 *Adadi:* "
            f"`{humanbytes(current)} / "
            f"{humanbytes(total)}`\n"

            f"⚡ *Speed:* "
            f"`{humanbytes(speed)}/s`\n"

            f"⏳ *Lokacin da ya rage:* "
            f"`{eta}`"
        )

        # -----------------------------------------------------
        # STATUS KEY
        # -----------------------------------------------------

        key = (

            chat_id,

            status_msg_id,

            "UPLOAD",

            attempt
        )

        last_edit = PROGRESS_STATE.get(
            key,
            0
        )

        # -----------------------------------------------------
        # THROTTLE MESSAGE EDIT
        # -----------------------------------------------------

        if (

            now -
            last_edit
            >= PROGRESS_UPDATE_SECONDS

            or

            current >= total

        ):

            PROGRESS_STATE[key] = now

            try:

                await edit_status(

                    chat_id,

                    status_msg_id,

                    status_text
                )

            except Exception as e:

                # IMPORTANT:
                # Kada progress callback ya mutu saboda
                # Telegram message edit error.
                print(
                    "[PROGRESS STATUS EDIT ERROR]",
                    repr(e)
                )

    except Exception as e:

        # -----------------------------------------------------
        # NEVER KILL UPLOAD FROM CALLBACK ERROR
        # -----------------------------------------------------

        print(
            "[UPLOAD PROGRESS ERROR]",
            repr(e)
        )


# =============================================================
# BUILD UPLOAD DEBUG
# =============================================================

def build_upload_debug(
    file_path,
    mode,
    attempt,
    upload_state,
    started,
    event
):

    now = time.time()

    current = upload_state.get(
        "current",
        0
    )

    total = upload_state.get(
        "total",
        0
    )

    percentage = upload_state.get(
        "percentage",
        0
    )

    speed = upload_state.get(
        "speed",
        0
    )

    last_progress = upload_state.get(
        "last_progress",
        started
    )

    last_real_progress = upload_state.get(
        "last_real_progress",
        started
    )

    callback_count = upload_state.get(
        "callback_count",
        0
    )

    elapsed = (
        now -
        started
    )

    no_callback = (
        now -
        last_progress
    )

    no_real_progress = (
        now -
        last_real_progress
    )

    stall_seconds = upload_state.get(
        "stall_seconds",
        0
    )

    timeout = upload_state.get(
        "timeout",
        False
    )

    cancelled = upload_state.get(
        "cancelled",
        False
    )

    return (

        f"🔎 *UPLOAD DEBUG*\n\n"

        f"📌 *Event:*\n"
        f"`{safe_debug_value(event)}`\n\n"

        f"🎯 *Mode:*\n"
        f"`{safe_debug_value(mode)}`\n\n"

        f"🔁 *Attempt:*\n"
        f"`{attempt}/{UPLOAD_RETRIES + 1}`\n\n"

        f"📁 *File:*\n"
        f"`{safe_debug_value(file_path)}`\n\n"

        f"📊 *Progress:*\n"
        f"`{percentage:.2f}%`\n\n"

        f"📦 *Uploaded:*\n"
        f"`{humanbytes(current)} / "
        f"{humanbytes(total)}`\n\n"

        f"⚡ *Speed:*\n"
        f"`{humanbytes(speed)}/s`\n\n"

        f"🧩 *Callbacks:*\n"
        f"`{callback_count}`\n\n"

        f"⏱️ *Elapsed:*\n"
        f"`{elapsed:.1f}s`\n\n"

        f"🕐 *No Callback:*\n"
        f"`{no_callback:.1f}s`\n\n"

        f"🛑 *No Real Progress:*\n"
        f"`{no_real_progress:.1f}s`\n\n"

        f"🚨 *Stall:*\n"
        f"`{stall_seconds:.1f}s`\n\n"

        f"⏰ *Timeout:*\n"
        f"`{timeout}`\n\n"

        f"❌ *Cancelled:*\n"
        f"`{cancelled}`\n\n"

        f"🟢 *Last Progress:*\n"
        f"`{time.strftime('%H:%M:%S', time.localtime(last_progress))}`"
    )


# =============================================================
# UPLOAD WATCHDOG
# =============================================================

async def upload_watchdog(

    upload_task,

    upload_state,

    file_path,

    mode,

    attempt,

    started,

    debug_message_id

):

    last_admin_update = 0

    last_seen_bytes = (
        upload_state.get(
            "current",
            0
        )
    )

    last_real_change = started

    while not upload_task.done():

        try:

            await asyncio.sleep(5)

        except asyncio.CancelledError:

            return

        if upload_task.done():

            break

        now = time.time()

        # -----------------------------------------------------
        # CURRENT PROGRESS
        # -----------------------------------------------------

        current = upload_state.get(
            "current",
            0
        )

        # -----------------------------------------------------
        # REAL PROGRESS CHECK
        # -----------------------------------------------------

        if current > last_seen_bytes:

            last_seen_bytes = current

            last_real_change = now

            upload_state[
                "last_real_progress"
            ] = now

        no_real_progress = (

            now -
            last_real_change
        )

        upload_state[
            "watchdog_no_progress"
        ] = no_real_progress

        # -----------------------------------------------------
        # ADMIN DEBUG
        # -----------------------------------------------------

        if (

            now -
            last_admin_update
            >= ADMIN_DEBUG_UPDATE_SECONDS

        ):

            debug_text = build_upload_debug(

                file_path,

                mode,

                attempt,

                upload_state,

                started,

                "UPLOAD RUNNING"
            )

            await admin_debug_edit(

                debug_message_id,

                debug_text
            )

            last_admin_update = now

        # -----------------------------------------------------
        # STALL DETECTION
        # -----------------------------------------------------

        if (

            no_real_progress
            >= UPLOAD_STALL_SECONDS

        ):

            upload_state["stalled"] = True

            upload_state[
                "stall_seconds"
            ] = no_real_progress

            debug_text = build_upload_debug(

                file_path,

                mode,

                attempt,

                upload_state,

                started,

                "🚨 UPLOAD STALLED"
            )

            await admin_debug_edit(

                debug_message_id,

                debug_text
            )

            print(
                "\n" +
                "=" * 80
            )

            print(
                "🚨 UPLOAD STALLED"
            )

            print(
                f"Attempt: "
                f"{attempt}/{UPLOAD_RETRIES + 1}"
            )

            print(
                f"Progress: "
                f"{upload_state.get('percentage', 0):.2f}%"
            )

            print(
                f"Uploaded: "
                f"{humanbytes(current)} / "
                f"{humanbytes(upload_state.get('total', 0))}"
            )

            print(
                f"No real progress: "
                f"{no_real_progress:.1f}s"
            )

            print(
                "=" * 80
            )

            # -------------------------------------------------
            # CANCEL UPLOAD
            # -------------------------------------------------

            upload_state[
                "cancelled"
            ] = True

            try:

                upload_task.cancel()

            except Exception as e:

                print(
                    "[WATCHDOG CANCEL ERROR]",
                    repr(e)
                )

            # -------------------------------------------------
            # WAIT UNTIL TASK REALLY STOPS
            # -------------------------------------------------

            try:

                await asyncio.gather(

                    upload_task,

                    return_exceptions=True
                )

            except Exception as e:

                print(
                    "[WATCHDOG WAIT ERROR]",
                    repr(e)
                )

            return


# =============================================================
# CUSTOM UPLOAD ERRORS
# =============================================================

class UploadStalledError(
    Exception
):
    pass


class UploadTimeoutError(
    Exception
):
    pass


# =============================================================
# ONE UPLOAD ATTEMPT
# =============================================================

async def run_single_upload_attempt(

    chat_id,

    file_path,

    as_video,

    status_msg_id,

    attempt,

    debug_message_id

):

    mode = (

        "VIDEO"

        if as_video

        else

        "DOCUMENT"
    )

    started = time.time()

    # ---------------------------------------------------------
    # FILE CHECK
    # ---------------------------------------------------------

    if not file_path:

        raise FileNotFoundError(
            "file_path babu value."
        )

    if not os.path.exists(
        file_path
    ):

        raise FileNotFoundError(
            f"File bai wanzu: {file_path}"
        )

    actual_size = os.path.getsize(
        file_path
    )

    if actual_size <= 0:

        raise ValueError(
            "File size = 0 bytes."
        )

    # ---------------------------------------------------------
    # UPLOAD STATE
    # ---------------------------------------------------------

    upload_state = {

        "current": 0,

        "total": actual_size,

        "percentage": 0,

        "speed": 0,

        "last_progress": started,

        "last_real_progress": started,

        "last_progress_bytes": 0,

        "last_real_progress_bytes": 0,

        "previous_progress_bytes": 0,

        "callback_count": 0,

        "stalled": False,

        "stall_seconds": 0,

        "timeout": False,

        "cancelled": False
    }

    # ---------------------------------------------------------
    # SAVE STATE
    # ---------------------------------------------------------

    state_key = (

        chat_id,

        status_msg_id
    )

    UPLOAD_STATE[state_key] = (
        upload_state
    )

    # ---------------------------------------------------------
    # DEBUG START
    # ---------------------------------------------------------

    await admin_debug_edit(

        debug_message_id,

        build_upload_debug(

            file_path,

            mode,

            attempt,

            upload_state,

            started,

            "🚀 UPLOAD ATTEMPT START"
        )
    )

    # =========================================================
    # SEND FUNCTION
    # =========================================================

    async def send_file():

        # -----------------------------------------------------
        # CHECK FILE AGAIN
        # -----------------------------------------------------

        if not os.path.exists(
            file_path
        ):

            raise FileNotFoundError(
                f"File ya bace kafin upload: "
                f"{file_path}"
            )

        # -----------------------------------------------------
        # VIDEO
        # -----------------------------------------------------

        if as_video:

            return await pyro_bot.send_video(

                chat_id=chat_id,

                video=file_path,

                caption=(
                    "🎬 An kammala sarrafa "
                    "bidiyon ku lafiya!"
                ),

                supports_streaming=True,

                progress=upload_progress_args,

                progress_args=(

                    "⬆️ *Ana Turawa (Video)...*",

                    chat_id,

                    status_msg_id,

                    started,

                    attempt,

                    upload_state
                )
            )

        # -----------------------------------------------------
        # DOCUMENT
        # -----------------------------------------------------

        return await pyro_bot.send_document(

            chat_id=chat_id,

            document=file_path,

            caption=(
                "📁 An kammala sarrafa "
                "fayil ɗin ku lafiya!"
            ),

            progress=upload_progress_args,

            progress_args=(

                "⬆️ *Ana Turawa (File)...*",

                chat_id,

                status_msg_id,

                started,

                attempt,

                upload_state
            )
        )

    # =========================================================
    # CREATE UPLOAD TASK
    # =========================================================

    upload_task = asyncio.create_task(
        send_file(),
        name=f"TelegramUpload-{attempt}"
    )

    # =========================================================
    # CREATE WATCHDOG
    # =========================================================

    watchdog_task = asyncio.create_task(

        upload_watchdog(

            upload_task,

            upload_state,

            file_path,

            mode,

            attempt,

            started,

            debug_message_id
        ),

        name=f"UploadWatchdog-{attempt}"
    )

    try:

        # -----------------------------------------------------
        # WAIT FOR UPLOAD
        # -----------------------------------------------------

        result = await asyncio.wait_for(

            upload_task,

            timeout=UPLOAD_TIMEOUT
        )

        # -----------------------------------------------------
        # VERIFY RESULT
        # -----------------------------------------------------

        if not result:

            raise Exception(
                "Telegram send method ya dawo "
                "da empty result."
            )

        sent_message_id = getattr(

            result,

            "id",

            None
        )

        if not sent_message_id:

            raise Exception(
                "Telegram result babu Message ID."
            )

        # -----------------------------------------------------
        # FINAL STATE
        # -----------------------------------------------------

        upload_state[
            "current"
        ] = max(

            upload_state.get(
                "current",
                0
            ),

            upload_state.get(
                "total",
                actual_size
            )
        )

        upload_state[
            "total"
        ] = max(

            upload_state.get(
                "total",
                0
            ),

            actual_size
        )

        upload_state[
            "percentage"
        ] = 100

        upload_state[
            "last_progress"
        ] = time.time()

        upload_state[
            "last_real_progress"
        ] = time.time()

        # -----------------------------------------------------
        # DEBUG SUCCESS
        # -----------------------------------------------------

        await admin_debug_edit(

            debug_message_id,

            build_upload_debug(

                file_path,

                mode,

                attempt,

                upload_state,

                started,

                "✅ UPLOAD SUCCESS"
            )
        )

        return result

    except asyncio.CancelledError:

        # -----------------------------------------------------
        # STALLED CANCELLATION
        # -----------------------------------------------------

        if upload_state.get(
            "stalled",
            False
        ):

            raise UploadStalledError(

                "Upload ya tsaya babu real progress "
                f"na tsawon "
                f"{upload_state.get('stall_seconds', 0):.1f}s"
            )

        # -----------------------------------------------------
        # NORMAL CANCELLATION
        # -----------------------------------------------------

        raise

    except asyncio.TimeoutError:

        upload_state[
            "timeout"
        ] = True

        await admin_debug_edit(

            debug_message_id,

            build_upload_debug(

                file_path,

                mode,

                attempt,

                upload_state,

                started,

                "⏰ UPLOAD TIMEOUT"
            )
        )

        raise UploadTimeoutError(

            f"Upload ya kai timeout "
            f"{UPLOAD_TIMEOUT}s"
        )

    finally:

        # -----------------------------------------------------
        # STOP WATCHDOG
        # -----------------------------------------------------

        await cancel_task_safely(

            watchdog_task,

            "UPLOAD WATCHDOG"
        )

        # -----------------------------------------------------
        # IF UPLOAD TASK IS STILL RUNNING,
        # CANCEL IT
        # -----------------------------------------------------

        if (

            upload_task

            and

            not upload_task.done()

        ):

            upload_state[
                "cancelled"
            ] = True

            await cancel_task_safely(

                upload_task,

                "UPLOAD TASK"
            )

        # -----------------------------------------------------
        # CLEAN PROGRESS STATE
        # -----------------------------------------------------

        UPLOAD_STATE.pop(

            state_key,

            None
        )


# =============================================================
# SAFE TELEGRAM UPLOAD
# =============================================================

async def upload_to_telegram(

    chat_id,

    file_path,

    as_video,

    status_msg_id

):

    mode = (

        "VIDEO"

        if as_video

        else

        "DOCUMENT"
    )

    total_attempts = (
        UPLOAD_RETRIES + 1
    )

    last_error = None

    # ---------------------------------------------------------
    # FILE CHECK
    # ---------------------------------------------------------

    if not os.path.exists(
        file_path
    ):

        raise FileNotFoundError(
            f"Upload file bai wanzu: {file_path}"
        )

    file_size = os.path.getsize(
        file_path
    )

    if file_size <= 0:

        raise ValueError(
            "Upload file yana da 0 bytes."
        )

    # =========================================================
    # ADMIN DEBUG MESSAGE
    # =========================================================

    debug_message = await admin_debug_send(

        (
            "🟡 *UPLOAD SYSTEM START*\n\n"

            f"🎯 Mode: `{mode}`\n"

            f"📁 File: "
            f"`{safe_debug_value(file_path)}`\n"

            f"📦 Size: "
            f"`{humanbytes(file_size)}`\n"

            f"🔁 Attempts: "
            f"`{total_attempts}`\n"

            f"⏰ Timeout: "
            f"`{UPLOAD_TIMEOUT}s`\n"

            f"🚨 Stall: "
            f"`{UPLOAD_STALL_SECONDS}s`\n\n"

            "⏳ Ana jiran Telegram..."
        )
    )

    debug_message_id = getattr(

        debug_message,

        "message_id",

        None
    )

    # =========================================================
    # ATTEMPTS
    # =========================================================

    for attempt in range(

        1,

        total_attempts + 1
    ):

        try:

            print(
                "\n" +
                "=" * 80
            )

            print(
                f"🚀 UPLOAD ATTEMPT "
                f"{attempt}/{total_attempts}"
            )

            print(
                f"Mode: {mode}"
            )

            print(
                f"File: {file_path}"
            )

            print(
                f"Size: {humanbytes(file_size)}"
            )

            print(
                "=" * 80
            )

            # -------------------------------------------------
            # DEBUG
            # -------------------------------------------------

            await admin_debug_edit(

                debug_message_id,

                (
                    f"🚀 *UPLOAD ATTEMPT "
                    f"{attempt}/{total_attempts}*\n\n"

                    f"🎯 Mode: `{mode}`\n"

                    f"📁 `{safe_debug_value(file_path)}`\n"

                    f"📦 `{humanbytes(file_size)}`\n\n"

                    "Telegram send method yana farawa..."
                )
            )

            # -------------------------------------------------
            # USER STATUS
            # -------------------------------------------------

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "⬆️ *Ana Turawa Telegram...*\n\n"

                    f"📁 `{os.path.basename(file_path)}`\n"

                    f"📦 `{humanbytes(file_size)}`\n"

                    f"🎯 `{mode}`\n"

                    f"🔁 Attempt "
                    f"`{attempt}/{total_attempts}`"
                )
            )

            # -------------------------------------------------
            # RUN ATTEMPT
            # -------------------------------------------------

            result = await run_single_upload_attempt(

                chat_id=chat_id,

                file_path=file_path,

                as_video=as_video,

                status_msg_id=status_msg_id,

                attempt=attempt,

                debug_message_id=debug_message_id
            )

            # -------------------------------------------------
            # SUCCESS
            # -------------------------------------------------

            await admin_debug_edit(

                debug_message_id,

                (
                    "🟢 *UPLOAD COMPLETE*\n\n"

                    f"🎯 Mode: `{mode}`\n"

                    f"🔁 Attempt: "
                    f"`{attempt}/{total_attempts}`\n"

                    f"📁 `{safe_debug_value(file_path)}`\n"

                    f"📦 `{humanbytes(file_size)}`\n\n"

                    "Telegram ya karɓi file lafiya."
                )
            )

            return result

        # =====================================================
        # FLOOD WAIT
        # =====================================================

        except FloodWait as e:

            last_error = e

            wait_seconds = int(
                getattr(
                    e,
                    "value",
                    0
                )
                or 0
            )

            print(
                f"⚠️ FLOOD WAIT: {wait_seconds}s"
            )

            # -------------------------------------------------
            # IF WAIT IS TOO LARGE
            # -------------------------------------------------

            if wait_seconds > MAX_FLOODWAIT:

                await admin_debug_edit(

                    debug_message_id,

                    (
                        "❌ *TELEGRAM FLOOD WAIT YA YI GIRMA*\n\n"

                        f"⏳ Wait: `{wait_seconds}s`\n\n"

                        "An dakatar da upload."
                    )
                )

                raise

            # -------------------------------------------------
            # WAIT AND RETRY
            # -------------------------------------------------

            await admin_debug_edit(

                debug_message_id,

                (
                    "⚠️ *TELEGRAM FLOOD WAIT*\n\n"

                    f"🎯 Mode: `{mode}`\n"

                    f"🔁 Attempt: "
                    f"`{attempt}/{total_attempts}`\n"

                    f"⏳ Wait: `{wait_seconds}s`\n\n"

                    "Telegram ya hana request na ɗan lokaci.\n"

                    "⏳ Za mu jira sannan mu sake gwadawa..."
                )
            )

            await asyncio.sleep(
                wait_seconds
            )

            if attempt < total_attempts:

                continue

            raise

        # =====================================================
        # STALLED
        # =====================================================

        except UploadStalledError as e:

            last_error = e

            print(
                f"🚨 STALLED: {e}"
            )

            if attempt < total_attempts:

                next_attempt = (
                    attempt + 1
                )

                await admin_debug_edit(

                    debug_message_id,

                    (
                        "🚨 *UPLOAD STALLED*\n\n"

                        f"🎯 Mode: `{mode}`\n"

                        f"🔁 Attempt: "
                        f"`{attempt}/{total_attempts}`\n"

                        f"❌ `{safe_debug_value(e)}`\n\n"

                        f"🔄 *RETRY "
                        f"{next_attempt}/{total_attempts} "
                        f"ZAI FARA...*"
                    )
                )

                try:

                    await edit_status(

                        chat_id,

                        status_msg_id,

                        (
                            "⚠️ *Upload ya tsaya.*\n\n"

                            "An gano cewa Telegram "
                            "baya samun progress.\n\n"

                            f"🔄 Ana sake gwadawa...\n"

                            f"Attempt "
                            f"`{next_attempt}/{total_attempts}`"
                        )
                    )

                except Exception:

                    pass

                await asyncio.sleep(
                    UPLOAD_RETRY_DELAY
                )

                continue

            # -------------------------------------------------
            # ALL RETRIES FAILED
            # -------------------------------------------------

            await admin_debug_edit(

                debug_message_id,

                (
                    "🔴 *UPLOAD YA KASA — ALL RETRIES FAILED*\n\n"

                    f"🎯 Mode: `{mode}`\n"

                    f"🔁 Attempts: "
                    f"`{total_attempts}`\n"

                    f"❌ `{safe_debug_value(e)}`"
                )
            )

            raise

        # =====================================================
        # TIMEOUT
        # =====================================================

        except UploadTimeoutError as e:

            last_error = e

            print(
                f"⏰ TIMEOUT: {e}"
            )

            if attempt < total_attempts:

                next_attempt = (
                    attempt + 1
                )

                await admin_debug_edit(

                    debug_message_id,

                    (
                        "⏰ *UPLOAD TIMEOUT*\n\n"

                        f"🎯 Mode: `{mode}`\n"

                        f"🔁 Attempt: "
                        f"`{attempt}/{total_attempts}`\n"

                        f"❌ `{safe_debug_value(e)}`\n\n"

                        f"🔄 *RETRY "
                        f"{next_attempt}/{total_attempts} "
                        f"ZAI FARA...*"
                    )
                )

                await asyncio.sleep(
                    UPLOAD_RETRY_DELAY
                )

                continue

            raise

        # =====================================================
        # NORMAL ERROR
        # =====================================================

        except Exception as e:

            last_error = e

            error_text = (

                f"{type(e).__name__}: "
                f"{str(e)}"
            )

            print(
                "\n🚨 UPLOAD ERROR"
            )

            print(
                error_text
            )

            traceback.print_exc()

            if attempt < total_attempts:

                next_attempt = (
                    attempt + 1
                )

                await admin_debug_edit(

                    debug_message_id,

                    (
                        "❌ *UPLOAD ERROR*\n\n"

                        f"🎯 Mode: `{mode}`\n"

                        f"🔁 Attempt: "
                        f"`{attempt}/{total_attempts}`\n\n"

                        f"Exception:\n"
                        f"`{safe_debug_value(error_text)}`\n\n"

                        f"🔄 *RETRY "
                        f"{next_attempt}/{total_attempts} "
                        f"ZAI FARA...*"
                    )
                )

                await asyncio.sleep(
                    UPLOAD_RETRY_DELAY
                )

                continue

            # -------------------------------------------------
            # FINAL ERROR
            # -------------------------------------------------

            await admin_debug_edit(

                debug_message_id,

                (
                    "🔴 *UPLOAD YA KASA*\n\n"

                    f"🎯 Mode: `{mode}`\n"

                    f"🔁 Attempts: "
                    f"`{total_attempts}`\n\n"

                    f"Exception:\n"
                    f"`{safe_debug_value(error_text)}`"
                )
            )

            raise

    # =========================================================
    # FINAL FALLBACK
    # =========================================================

    if last_error:

        raise last_error

    raise Exception(
        "Upload ya kasa ba tare da error ba."
    )


# =============================================================
# PYROGRAM MAIN - AUTO RESTART
# =============================================================

async def pyro_main():

    restart_delay = 5

    while True:

        try:

            print(
                "\n" +
                "=" * 80
            )

            print(
                "🚀 PYROGRAM STARTING..."
            )

            print(
                "=" * 80
            )

            # -------------------------------------------------
            # START
            # -------------------------------------------------

            result = pyro_bot.start()

            if inspect.isawaitable(
                result
            ):

                await result

            # -------------------------------------------------
            # READY
            # -------------------------------------------------

            PYRO_READY.set()

            PYRO_FAILED.clear()

            restart_delay = 5

            print(
                "🟢 Pyrogram ya shirya."
            )

            # -------------------------------------------------
            # KEEP ALIVE
            # -------------------------------------------------

            await asyncio.Event().wait()

        except asyncio.CancelledError:

            PYRO_READY.clear()

            PYRO_FAILED.set()

            print(
                "🛑 Pyrogram task cancelled."
            )

            raise

        except Exception as e:

            PYRO_READY.clear()

            PYRO_FAILED.set()

            print(
                "\n🚨 PYROGRAM YA CRASH"
            )

            traceback.print_exc()

            try:

                send_admin_exception(

                    "PYROGRAM YA CRASH",

                    e
                )

            except Exception:

                pass

        finally:

            PYRO_READY.clear()

            # -------------------------------------------------
            # STOP CLIENT
            # -------------------------------------------------

            try:

                if getattr(
                    pyro_bot,
                    "is_connected",
                    False
                ):

                    result = pyro_bot.stop()

                    if inspect.isawaitable(
                        result
                    ):

                        await result

            except Exception as e:

                print(
                    "[PYRO STOP ERROR]",
                    repr(e)
                )

        # =====================================================
        # AUTO RESTART
        # =====================================================

        print(
            f"🔄 Pyrogram zai sake farawa "
            f"bayan {restart_delay}s..."
        )

        try:

            await asyncio.sleep(
                restart_delay
            )

        except asyncio.CancelledError:

            raise

        restart_delay = min(

            restart_delay * 2,

            60
        )


# =============================================================
# /START
# =============================================================

@bot.message_handler(
    commands=["start"]
)
def start_handler(
    message
):

    user_id = message.from_user.id

    if user_id != ADMIN_ID:

        bot.reply_to(

            message,

            "❌ Wannan bot na admin ne kawai."
        )

        return

    bot.reply_to(

        message,

        (
            "🟢 *Bot yana aiki lafiya.*\n\n"

            "Aika `/video` domin fara converter."
        )
    )


# =============================================================
# /VIDEO
# =============================================================

@bot.message_handler(
    commands=["video"]
)
def start_video_process(
    message
):

    user_id = message.from_user.id

    if user_id != ADMIN_ID:

        return

    USER_STATES[user_id] = True

    bot.reply_to(

        message,

        (
            "✅ *An kunna tsarin karɓar aiki!*\n\n"

            "Yanzu aiko min da *Video* ko *File* "
            "din da kake son sarrafawa."
        )
    )


# =============================================================
# RECEIVE VIDEO / DOCUMENT
# =============================================================

@bot.message_handler(
    content_types=[
        "video",
        "document"
    ]
)
def handle_incoming_file(
    message
):

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

    original_type = (
        message.content_type
    )

    try:

        # -----------------------------------------------------
        # VIDEO
        # -----------------------------------------------------

        if message.video:

            file_name = (

                getattr(

                    message.video,

                    "file_name",

                    None
                )

                or

                "video.mp4"
            )

            file_size = (

                getattr(

                    message.video,

                    "file_size",

                    0
                )

                or

                0
            )

            file_id = getattr(

                message.video,

                "file_id",

                None
            )

        # -----------------------------------------------------
        # DOCUMENT
        # -----------------------------------------------------

        elif message.document:

            file_name = (

                getattr(

                    message.document,

                    "file_name",

                    None
                )

                or

                "file"
            )

            file_size = (

                getattr(

                    message.document,

                    "file_size",

                    0
                )

                or

                0
            )

            file_id = getattr(

                message.document,

                "file_id",

                None
            )

    except Exception as e:

        send_admin_exception(

            "FILE DETAILS ERROR",

            e
        )

    # ---------------------------------------------------------
    # BUTTONS
    # ---------------------------------------------------------

    markup = (
        types.InlineKeyboardMarkup()
    )

    btn1 = (
        types.InlineKeyboardButton(

            "🎬 Video",

            callback_data="convert_video"
        )
    )

    btn2 = (
        types.InlineKeyboardButton(

            "📁 File",

            callback_data="convert_file"
        )
    )

    markup.add(

        btn1,

        btn2
    )

    # ---------------------------------------------------------
    # REPLY
    # ---------------------------------------------------------

    try:

        sent = bot.reply_to(

            message,

            (
                f"✅ *An karɓi fayil:* "
                f"`{file_name}`\n\n"

                "Shin a wanne tsari kake "
                "son dawo da shi?"
            ),

            reply_markup=markup
        )

        PENDING_DATA[
            sent.message_id
        ] = {

            "msg_id":
                message.message_id,

            "chat_id":
                message.chat.id,

            "file_name":
                file_name,

            "file_size":
                file_size,

            "original_type":
                original_type,

            "file_id":
                file_id,

            "created_at":
                time.time()
        }

    except Exception as e:

        send_admin_exception(

            "FILE REPLY ERROR",

            e
        )


# =============================================================
# CALLBACK
# =============================================================

@bot.callback_query_handler(

    func=lambda call:
        call.data.startswith(
            "convert_"
        )
)
def process_conversion_callback(
    call
):

    try:

        # -----------------------------------------------------
        # ANSWER CALLBACK
        # -----------------------------------------------------

        try:

            bot.answer_callback_query(
                call.id
            )

        except Exception:

            pass

        # -----------------------------------------------------
        # ADMIN ONLY
        # -----------------------------------------------------

        if call.from_user.id != ADMIN_ID:

            return

        # -----------------------------------------------------
        # MESSAGE ID
        # -----------------------------------------------------

        msg_id = (
            call.message.message_id
        )

        # -----------------------------------------------------
        # PENDING CHECK
        # -----------------------------------------------------

        if msg_id not in PENDING_DATA:

            bot.edit_message_text(

                (
                    "❌ *Aikin ya fita "
                    "daga tsarin lokaci.*\n\n"

                    "Sake fara `/video`."
                ),

                call.message.chat.id,

                msg_id
            )

            return

        # -----------------------------------------------------
        # GET TASK INFO
        # -----------------------------------------------------

        task_info = PENDING_DATA.pop(
            msg_id
        )

        as_video = (

            call.data ==
            "convert_video"
        )

        selected_mode = (

            "VIDEO"

            if as_video

            else

            "FILE"
        )

        chat_id = (
            task_info["chat_id"]
        )

        target_msg_id = (
            task_info["msg_id"]
        )

        file_name = (

            task_info.get(

                "file_name",

                "Unknown"
            )
        )

        original_type = (

            task_info.get(

                "original_type",

                "unknown"
            )
        )

        # =====================================================
        # PYROGRAM READY CHECK
        # =====================================================

        if not PYRO_READY.is_set():

            bot.edit_message_text(

                (
                    "❌ *Pyrogram bai shirya ba.*\n\n"

                    "Tsarin yana ƙoƙarin sake haɗuwa.\n"

                    "Sake gwadawa bayan ɗan lokaci."
                ),

                chat_id,

                msg_id
            )

            return

        # =====================================================
        # LOOP CHECK
        # =====================================================

        if pyro_loop.is_closed():

            raise Exception(
                "Pyrogram asyncio loop ya rufe."
            )

        # -----------------------------------------------------
        # STATUS
        # -----------------------------------------------------

        bot.edit_message_text(

            (
                "🔄 *Ana fara aikin...*\n\n"

                f"📁 `{file_name}`\n"

                f"🎯 Tsari: `{selected_mode}`"
            ),

            chat_id,

            msg_id
        )

        # =====================================================
        # SEND TO PYRO LOOP
        # =====================================================

        future = (
            asyncio.run_coroutine_threadsafe(

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
        )

        # -----------------------------------------------------
        # FUTURE CALLBACK
        # -----------------------------------------------------

        def future_done_callback(
            done_future
        ):

            try:

                exception = (
                    done_future.exception()
                )

                if exception:

                    send_admin_exception(

                        "FUTURE TASK YA KOMA DA EXCEPTION",

                        exception
                    )

            except asyncio.CancelledError:

                print(
                    "[FUTURE] Task cancelled."
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

            "CALLBACK CRASH",

            e
        )


# =============================================================
# PYROGRAM TASK
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

        else

        "DOCUMENT"
    )

    try:

        # =====================================================
        # PYROGRAM CONNECTION CHECK
        # =====================================================

        if not getattr(
            pyro_bot,
            "is_connected",
            False
        ):

            raise Exception(
                "Pyrogram ba connected ba."
            )

        # -----------------------------------------------------
        # GET ME
        # -----------------------------------------------------

        me = await pyro_bot.get_me()

        if not me:

            raise Exception(
                "Pyrogram bai dawo da bot information ba."
            )

        # =====================================================
        # STATUS MESSAGE
        # =====================================================

        status_msg = (

            await pyro_bot.get_messages(

                chat_id,

                status_msg_id
            )
        )

        if not status_msg:

            raise Exception(
                "Ba a samu status message ba."
            )

        # =====================================================
        # ORIGINAL MESSAGE
        # =====================================================

        msg = (

            await pyro_bot.get_messages(

                chat_id,

                target_msg_id
            )
        )

        if not msg:

            raise Exception(
                "Original message bai samu ba."
            )

        # =====================================================
        # MEDIA INFORMATION
        # =====================================================

        media_type = "UNKNOWN"

        media_size = 0

        media_file_name = (

            selected_file_name

            or

            "unknown"
        )

        # -----------------------------------------------------
        # VIDEO
        # -----------------------------------------------------

        if msg.video:

            media_type = "VIDEO"

            media_size = (

                msg.video.file_size

                or

                0
            )

            media_file_name = (

                msg.video.file_name

                or

                media_file_name

                or

                "video.mp4"
            )

        # -----------------------------------------------------
        # DOCUMENT
        # -----------------------------------------------------

        elif msg.document:

            media_type = "DOCUMENT"

            media_size = (

                msg.document.file_size

                or

                0
            )

            media_file_name = (

                msg.document.file_name

                or

                media_file_name

                or

                "file"
            )

        # -----------------------------------------------------
        # ANIMATION
        # -----------------------------------------------------

        elif msg.animation:

            media_type = "ANIMATION"

            media_size = (

                msg.animation.file_size

                or

                0
            )

            media_file_name = (

                msg.animation.file_name

                or

                media_file_name

                or

                "animation"
            )

        else:

            raise Exception(

                "Original message ba ya dauke da "
                "video/document/animation."
            )

        # =====================================================
        # ADMIN DEBUG - MEDIA
        # =====================================================

        try:

            await admin_debug_send(

                (
                    "📥 *UPLOAD TASK MEDIA INFO*\n\n"

                    f"🎯 Selected: `{selected_mode}`\n"

                    f"📁 File: "
                    f"`{safe_debug_value(media_file_name)}`\n"

                    f"📦 Telegram Size: "
                    f"`{humanbytes(media_size)}`\n"

                    f"📄 Type: `{media_type}`"
                )
            )

        except Exception:

            pass

        # =====================================================
        # DOWNLOAD
        # =====================================================

        (
            file_path,

            local_size,

            download_time

        ) = await download_original_file(

            pyro_message=msg,

            chat_id=chat_id,

            status_msg_id=status_msg_id,

            media_file_name=media_file_name,

            media_size=media_size
        )

        # =====================================================
        # VERIFY DOWNLOADED FILE
        # =====================================================

        if not file_path:

            raise Exception(
                "download_original_file "
                "bai dawo da file_path ba."
            )

        if not os.path.exists(
            file_path
        ):

            raise FileNotFoundError(

                f"Downloaded file bai wanzu ba: "
                f"{file_path}"
            )

        local_size = os.path.getsize(
            file_path
        )

        if local_size <= 0:

            raise Exception(
                "Downloaded file yana da 0 bytes."
            )

        # =====================================================
        # START UPLOAD
        # =====================================================

        await edit_status(

            chat_id,

            status_msg_id,

            (
                "⬆️ *Ana Turawa Telegram...*\n\n"

                f"📁 `{media_file_name}`\n"

                f"📦 `{humanbytes(local_size)}`\n"

                f"🎯 `{selected_mode}`"
            )
        )

        upload_start = time.time()

        # =====================================================
        # UPLOAD
        # =====================================================

        result = await upload_to_telegram(

            chat_id=chat_id,

            file_path=file_path,

            as_video=as_video,

            status_msg_id=status_msg_id
        )

        upload_time = (

            time.time() -
            upload_start
        )

        # =====================================================
        # VERIFY RESULT
        # =====================================================

        sent_message_id = getattr(

            result,

            "id",

            None
        )

        if not sent_message_id:

            raise Exception(

                "Telegram ya dawo result "
                "amma babu Message ID."
            )

        # =====================================================
        # SUCCESS
        # =====================================================

        await edit_status(

            chat_id,

            status_msg_id,

            (
                "🎉 *An gama aikin lafiya!*\n\n"

                "🟢 Download: OK\n"

                "🟢 Local File: OK\n"

                "🟢 Telegram Upload: OK\n"

                "🟢 Delivery: OK"
            )
        )

        total_time = (

            time.time() -
            task_started
        )

        # -----------------------------------------------------
        # ADMIN SUCCESS NOTICE
        # -----------------------------------------------------

        try:

            bot.send_message(

                ADMIN_ID,

                (
                    "✅ *Aiki ya kammala*\n\n"

                    f"📁 `{media_file_name}`\n"

                    f"📦 `{humanbytes(local_size)}`\n"

                    f"🎯 `{selected_mode}`\n"

                    f"⏱️ Download: "
                    f"`{download_time:.1f}s`\n"

                    f"⏱️ Upload: "
                    f"`{upload_time:.1f}s`\n"

                    f"⏱️ Total: "
                    f"`{total_time:.1f}s`\n"

                    f"🆔 Message ID: "
                    f"`{sent_message_id}`"
                ),

                parse_mode="Markdown"
            )

        except Exception as admin_error:

            print(
                "[SUCCESS ADMIN NOTICE ERROR]",
                repr(admin_error)
            )

        # -----------------------------------------------------
        # CONSOLE
        # -----------------------------------------------------

        print(
            "\n" +
            "=" * 80
        )

        print(
            "🎉 SUCCESS"
        )

        print(
            f"File: {media_file_name}"
        )

        print(
            f"Size: {humanbytes(local_size)}"
        )

        print(
            f"Upload: {upload_time:.1f}s"
        )

        print(
            f"Message ID: {sent_message_id}"
        )

        print(
            "=" * 80
        )

    # =========================================================
    # FLOOD WAIT
    # =========================================================

    except FloodWait as e:

        try:

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "⚠️ *Telegram Limit.*\n\n"

                    f"Jira daƙiƙa "
                    f"`{e.value}`."
                )
            )

        except Exception:

            pass

        send_admin_exception(

            "TELEGRAM FLOOD WAIT",

            e
        )

    # =========================================================
    # STALLED
    # =========================================================

    except UploadStalledError as e:

        try:

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "❌ *Upload ya tsaya.*\n\n"

                    "An yi dukkan retries amma "
                    "Telegram bai ci gaba ba."
                )
            )

        except Exception:

            pass

        send_admin_exception(

            "UPLOAD STALLED - ALL RETRIES FAILED",

            e
        )

    # =========================================================
    # TIMEOUT
    # =========================================================

    except UploadTimeoutError as e:

        try:

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "⏰ *Upload ya yi timeout.*\n\n"

                    "An kasa kammala upload."
                )
            )

        except Exception:

            pass

        send_admin_exception(

            "UPLOAD TIMEOUT - ALL RETRIES FAILED",

            e
        )

    # =========================================================
    # NORMAL ERROR
    # =========================================================

    except Exception as e:

        try:

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "❌ *Aiki ya samu kuskure.*\n\n"

                    "An kasa kammala upload."
                )
            )

        except Exception as status_error:

            print(
                "[STATUS ERROR]",
                repr(status_error)
            )

        send_admin_exception(

            (
                "UPLOAD/SEND TASK YA KASA\n"

                f"MODE={selected_mode}\n"

                f"FILE={selected_file_name}\n"

                f"ORIGINAL_TYPE={original_type}"
            ),

            e
        )

    finally:

        # =====================================================
        # CLEANUP LOCAL FILE
        # =====================================================

        if (

            file_path

            and

            os.path.exists(
                file_path
            )

        ):

            try:

                os.remove(
                    file_path
                )

                print(
                    "[CLEANUP] Removed:",
                    file_path
                )

            except Exception as e:

                print(
                    "[CLEANUP ERROR]",
                    file_path,
                    repr(e)
                )

        # =====================================================
        # CLEAN PROGRESS STATES
        # =====================================================

        try:

            keys_to_remove = [

                key

                for key in list(
                    PROGRESS_STATE.keys()
                )

                if (

                    len(key) >= 2

                    and

                    key[0] == chat_id

                    and

                    key[1] == status_msg_id
                )
            ]

            for key in keys_to_remove:

                PROGRESS_STATE.pop(
                    key,
                    None
                )

        except Exception:

            pass

        # =====================================================
        # CLEAN UPLOAD STATE
        # =====================================================

        try:

            UPLOAD_STATE.pop(

                (
                    chat_id,
                    status_msg_id
                ),

                None
            )

        except Exception:

            pass


# =============================================================
# PYROGRAM THREAD
# =============================================================

def start_pyro_loop():

    try:

        asyncio.set_event_loop(
            pyro_loop
        )

        pyro_loop.run_until_complete(
            pyro_main()
        )

    except Exception as e:

        PYRO_FAILED.set()

        PYRO_READY.clear()

        try:

            send_admin_exception(

                "PYROGRAM LOOP YA CRASH",

                e
            )

        except Exception:

            pass

    finally:

        try:

            if not pyro_loop.is_closed():

                pyro_loop.close()

        except Exception as e:

            print(
                "[PYRO LOOP CLOSE ERROR]",
                repr(e)
            )


# =============================================================
# START PYROGRAM THREAD
# =============================================================

pyro_thread = threading.Thread(

    target=start_pyro_loop,

    name="PyrogramThread",

    daemon=True
)

pyro_thread.start()


# =============================================================
# RENDER HTTP SERVER
# =============================================================

class HealthHandler(
    BaseHTTPRequestHandler
):

    def do_GET(
        self
    ):

        # -----------------------------------------------------
        # ROOT
        # -----------------------------------------------------

        if self.path == "/":

            self.send_response(
                200
            )

            self.send_header(

                "Content-Type",

                "text/plain; charset=utf-8"
            )

            self.end_headers()

            self.wfile.write(

                b"Converter Bot is running."
            )

            return

        # -----------------------------------------------------
        # HEALTH
        # -----------------------------------------------------

        if self.path == "/health":

            pyro_status = (

                "ready"

                if PYRO_READY.is_set()

                else

                "not_ready"
            )

            body = (

                '{'

                f'"status":"ok",'

                f'"pyrogram":"{pyro_status}"'

                '}'
            ).encode(
                "utf-8"
            )

            self.send_response(
                200
            )

            self.send_header(

                "Content-Type",

                "application/json"
            )

            self.send_header(

                "Content-Length",

                str(len(body))
            )

            self.end_headers()

            self.wfile.write(
                body
            )

            return

        # -----------------------------------------------------
        # 404
        # -----------------------------------------------------

        self.send_response(
            404
        )

        self.end_headers()

    def log_message(
        self,
        format,
        *args
    ):

        return


# =============================================================
# HTTP SERVER START
# =============================================================

def start_http_server():

    try:

        server = HTTPServer(

            (
                "0.0.0.0",

                PORT
            ),

            HealthHandler
        )

        print(

            "HTTP server yana sauraro "

            f"a 0.0.0.0:{PORT}"
        )

        server.serve_forever()

    except Exception as e:

        try:

            send_admin_exception(

                "HTTP SERVER YA KASA",

                e
            )

        except Exception:

            pass


http_thread = threading.Thread(

    target=start_http_server,

    name="HTTPServerThread",

    daemon=True
)

http_thread.start()


# =============================================================
# STARTUP MONITOR
# =============================================================

def startup_monitor():

    time.sleep(5)

    print(
        "\n" +
        "=" * 80
    )

    print(
        "FULL STARTUP STATUS"
    )

    print(
        "=" * 80
    )

    print(
        f"PID: {os.getpid()}"
    )

    print(
        f"PORT: {PORT}"
    )

    print(
        f"HTTP Thread: "
        f"{http_thread.name}"
    )

    print(
        f"Pyro Thread: "
        f"{pyro_thread.name}"
    )

    print(
        f"Pyro Ready: "
        f"{PYRO_READY.is_set()}"
    )

    print(
        f"Pyro Failed: "
        f"{PYRO_FAILED.is_set()}"
    )

    print(
        system_info()
    )

    print(
        "=" * 80
    )


threading.Thread(

    target=startup_monitor,

    name="StartupMonitor",

    daemon=True

).start()



# =============================================================
# 21. TELEBOT STARTUP + AUTO RETRY
# =============================================================

if __name__ == "__main__":

    print("\n" + "=" * 70)
    print("TELEBOT MAIN PROCESS")
    print("=" * 70)

    try:
        bot.delete_webhook(
            drop_pending_updates=True
        )
        print("An cire tsohon webhook.")

    except Exception as e:
        print(
            f"Webhook removal error: {e}"
        )

    BOT_READY.set()

    retry_count = 0

    while True:
        try:
            print(
                "\nStarting Telebot polling..."
            )

            bot.infinity_polling(
                skip_pending=True,
                timeout=60,
                long_polling_timeout=60,
                allowed_updates=None
            )

            retry_count += 1

            print(
                f"Polling ya tsaya. "
                f"Retry #{retry_count}. "
                f"Ana jira 5 seconds..."
            )

            time.sleep(5)

        except KeyboardInterrupt:
            print("Bot an dakatar da shi.")
            break

        except Exception as e:
            retry_count += 1

            send_admin_exception(
                f"POLLING CRASH - RETRY #{retry_count}",
                e
            )

            print(
                f"\nPolling error: {e}\n"
                f"Retry #{retry_count}\n"
                "Ana jira 10 seconds..."
            )

            time.sleep(10)

            try:
                bot.delete_webhook(
                    drop_pending_updates=True
                )
            except Exception as webhook_error:
                print(
                    f"Webhook cleanup error: "
                    f"{webhook_error}"
                )
