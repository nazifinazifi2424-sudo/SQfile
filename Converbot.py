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
# =============================================================
#              END OF SYSTEM DOWNLOAD
# =============================================================


# =============================================================
# =============================================================
#              ANAN FARKON SYSTEM UPLOAD
# =============================================================
#
# DAGA NAN ZUWA KARSHE:
#
# SEND_VIDEO
# SEND_DOCUMENT
# RETRY
# TIMEOUT
# STALL DETECTION
# UPLOAD DEBUG
# UPLOAD PROGRESS
#
# DUK UPLOAD NE.
#
# =============================================================


# =============================================================
# UPLOAD DEBUG HELPERS
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
            e
        )

        return None


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
            e
        )


def safe_debug_value(value):

    if value is None:
        return "None"

    text = str(value)

    text = text.replace(
        "`",
        "'"
    )

    return text[:500]


# =============================================================
# UPLOAD PROGRESS
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

        diff = (
            now -
            start_time
        )

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

        # -----------------------------------------------------
        # CRITICAL:
        # Update upload state EVERY TIME Pyrogram calls us.
        #
        # Ba wai lokacin da muka edit message kawai ba.
        # -----------------------------------------------------

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
            ) + 1
        )

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

        key = (
            chat_id,
            status_msg_id,
            "UPLOAD",
            attempt
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
                status_msg_id,
                status_text
            )

    except Exception as e:

        # NEVER allow progress error to kill upload.
        print(
            "[UPLOAD PROGRESS ERROR]",
            e
        )


# =============================================================
# UPLOAD DEBUG SNAPSHOT
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

    callback_count = upload_state.get(
        "callback_count",
        0
    )

    no_progress = (
        now -
        last_progress
    )

    elapsed = (
        now -
        started
    )

    return (
        f"🔎 *UPLOAD DEBUG*\n\n"

        f"📌 *Event:*\n"
        f"`{safe_debug_value(event)}`\n\n"

        f"🎯 *Mode:*\n"
        f"`{mode}`\n\n"

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

        f"🕐 *No Progress:*\n"
        f"`{no_progress:.1f}s`\n\n"

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

    while not upload_task.done():

        await asyncio.sleep(5)

        if upload_task.done():
            break

        now = time.time()

        last_progress = upload_state.get(
            "last_progress",
            started
        )

        no_progress = (
            now -
            last_progress
        )

        # -----------------------------------------------------
        # ADMIN DEBUG UPDATE
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

        if no_progress >= UPLOAD_STALL_SECONDS:

            upload_state["stalled"] = True

            upload_state[
                "stall_seconds"
            ] = no_progress

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
                "=" * 70
            )

            print(
                "🚨 UPLOAD STALLED"
            )

            print(
                f"Attempt: {attempt}"
            )

            print(
                f"Progress: "
                f"{upload_state.get('percentage', 0):.2f}%"
            )

            print(
                f"No progress: "
                f"{no_progress:.1f}s"
            )

            print(
                "=" * 70
            )

            # -------------------------------------------------
            # CANCEL CURRENT UPLOAD
            # -------------------------------------------------

            upload_task.cancel()

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
        else "DOCUMENT"
    )

    started = time.time()

    upload_state = {
        "current": 0,
        "total": 0,
        "percentage": 0,
        "speed": 0,
        "last_progress": started,
        "last_progress_bytes": 0,
        "callback_count": 0,
        "stalled": False,
        "stall_seconds": 0
    }

    # ---------------------------------------------------------
    # SAVE CURRENT STATE
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

    # ---------------------------------------------------------
    # SEND FUNCTION
    # ---------------------------------------------------------

    async def send_file():

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

        else:

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

    # ---------------------------------------------------------
    # CREATE UPLOAD TASK
    # ---------------------------------------------------------

    upload_task = asyncio.create_task(
        send_file()
    )

    # ---------------------------------------------------------
    # CREATE WATCHDOG
    # ---------------------------------------------------------

    watchdog_task = asyncio.create_task(

        upload_watchdog(

            upload_task,

            upload_state,

            file_path,

            mode,

            attempt,

            started,

            debug_message_id
        )
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
        # UPLOAD FINISHED
        # -----------------------------------------------------

        if not result:

            raise Exception(
                "Telegram send method ya dawo "
                "da empty result."
            )

        # -----------------------------------------------------
        # CHECK MESSAGE ID
        # -----------------------------------------------------

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
        # FINAL DEBUG
        # -----------------------------------------------------

        upload_state["current"] = (
            upload_state.get(
                "total",
                upload_state.get(
                    "current",
                    0
                )
            )
        )

        upload_state["percentage"] = 100

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
        # WHY WAS IT CANCELLED?
        # -----------------------------------------------------

        if upload_state.get(
            "stalled",
            False
        ):

            raise UploadStalledError(
                "Upload ya tsaya babu progress "
                f"na tsawon "
                f"{upload_state.get('stall_seconds', 0):.1f}s"
            )

        raise

    except asyncio.TimeoutError:

        upload_state["timeout"] = True

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

        if not watchdog_task.done():

            watchdog_task.cancel()

            try:

                await watchdog_task

            except asyncio.CancelledError:

                pass

        # -----------------------------------------------------
        # CLEAN STATE
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
        else "DOCUMENT"
    )

    total_attempts = (
        UPLOAD_RETRIES + 1
    )

    last_error = None

    # ---------------------------------------------------------
    # CREATE ADMIN DEBUG MESSAGE
    # ---------------------------------------------------------

    debug_message = await admin_debug_send(

        (
            "🟡 *UPLOAD SYSTEM START*\n\n"
            f"🎯 Mode: `{mode}`\n"
            f"📁 File: `{file_path}`\n"
            f"📦 Size: "
            f"`{humanbytes(os.path.getsize(file_path))}`\n"
            f"🔁 Attempts: `{total_attempts}`\n"
            f"⏰ Timeout: `{UPLOAD_TIMEOUT}s`\n"
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

    # ---------------------------------------------------------
    # ATTEMPTS
    # ---------------------------------------------------------

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
                "=" * 80
            )

            await admin_debug_edit(

                debug_message_id,

                (
                    f"🚀 *UPLOAD ATTEMPT "
                    f"{attempt}/{total_attempts}*\n\n"
                    f"🎯 Mode: `{mode}`\n"
                    f"📁 `{file_path}`\n\n"
                    "Telegram send method yana farawa..."
                )
            )

            # -------------------------------------------------
            # STATUS
            # -------------------------------------------------

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "⬆️ *Ana Turawa Telegram...*\n\n"
                    f"📁 `{os.path.basename(file_path)}`\n"
                    f"📦 `{humanbytes(os.path.getsize(file_path))}`\n"
                    f"🎯 `{mode}`\n"
                    f"🔁 Attempt `{attempt}/{total_attempts}`"
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
                    f"🟢 *UPLOAD COMPLETE*\n\n"
                    f"🎯 Mode: `{mode}`\n"
                    f"🔁 Attempt: `{attempt}/{total_attempts}`\n"
                    f"📁 `{file_path}`\n\n"
                    "Telegram ya karɓi file lafiya."
                )
            )

            return result

        except FloodWait as e:

            # -------------------------------------------------
            # FLOOD WAIT
            # -------------------------------------------------

            last_error = e

            wait_seconds = getattr(
                e,
                "value",
                0
            )

            await admin_debug_edit(

                debug_message_id,

                (
                    "⚠️ *TELEGRAM FLOOD WAIT*\n\n"
                    f"🎯 Mode: `{mode}`\n"
                    f"🔁 Attempt: `{attempt}/{total_attempts}`\n"
                    f"⏳ Wait: `{wait_seconds}s`\n\n"
                    "Telegram ya hana request na ɗan lokaci."
                )
            )

            raise

        except UploadStalledError as e:

            # -------------------------------------------------
            # STALLED
            # -------------------------------------------------

            last_error = e

            print(
                f"🚨 STALLED: {e}"
            )

            if attempt < total_attempts:

                await admin_debug_edit(

                    debug_message_id,

                    (
                        "🚨 *UPLOAD STALLED*\n\n"
                        f"🎯 Mode: `{mode}`\n"
                        f"🔁 Attempt: "
                        f"`{attempt}/{total_attempts}`\n"
                        f"❌ `{safe_debug_value(e)}`\n\n"
                        f"🔄 *RETRY "
                        f"{attempt + 1}/{total_attempts} "
                        f"ZAI FARA...*"
                    )
                )

                await edit_status(

                    chat_id,

                    status_msg_id,

                    (
                        "⚠️ *Upload ya tsaya.*\n\n"
                        f"🔄 Ana sake gwadawa...\n"
                        f"Attempt "
                        f"`{attempt + 1}/{total_attempts}`"
                    )
                )

                await asyncio.sleep(5)

                continue

            raise

        except UploadTimeoutError as e:

            # -------------------------------------------------
            # TIMEOUT
            # -------------------------------------------------

            last_error = e

            print(
                f"⏰ TIMEOUT: {e}"
            )

            if attempt < total_attempts:

                await admin_debug_edit(

                    debug_message_id,

                    (
                        "⏰ *UPLOAD TIMEOUT*\n\n"
                        f"🎯 Mode: `{mode}`\n"
                        f"🔁 Attempt: "
                        f"`{attempt}/{total_attempts}`\n\n"
                        f"❌ `{safe_debug_value(e)}`\n\n"
                        f"🔄 *RETRY "
                        f"{attempt + 1}/{total_attempts} "
                        f"ZAI FARA...*"
                    )
                )

                await asyncio.sleep(5)

                continue

            raise

        except Exception as e:

            # -------------------------------------------------
            # NORMAL ERROR
            # -------------------------------------------------

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

            if attempt < total_attempts:

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
                        f"{attempt + 1}/{total_attempts} "
                        f"ZAI FARA...*"
                    )
                )

                await asyncio.sleep(5)

                continue

            raise

    if last_error:

        raise last_error

    raise Exception(
        "Upload ya kasa ba tare da error ba."
    )


# =============================================================
# =============================================================
#              NUNA KARSHEN SYSTEM UPLOAD
# =============================================================


# =============================================================
# PYROGRAM MAIN
# =============================================================

async def pyro_main():

    try:

        result = pyro_bot.start()

        if inspect.isawaitable(result):

            await result

        PYRO_READY.set()

        PYRO_FAILED.clear()

        print(
            "Pyrogram ya shirya."
        )

        await asyncio.Event().wait()

    except Exception as e:

        PYRO_FAILED.set()

        PYRO_READY.clear()

        send_admin_exception(
            "PYROGRAM YA CRASH",
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
                "[PYRO STOP ERROR]",
                e
            )


# =============================================================
# /START
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


# =============================================================
# /VIDEO
# =============================================================

@bot.message_handler(
    commands=["video"]
)
def start_video_process(message):

    user_id = message.from_user.id

    if user_id != ADMIN_ID:
        return

    USER_STATES[user_id] = True

    bot.reply_to(

        message,

        "✅ *An kunna tsarin karɓar aiki!*\n\n"
        "Yanzu aiko min da *Video* ko *File* "
        "din da kake son sarrafawa."
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

    original_type = (
        message.content_type
    )

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

        bot.answer_callback_query(
            call.id
        )

        if call.from_user.id != ADMIN_ID:
            return

        msg_id = (
            call.message.message_id
        )

        if msg_id not in PENDING_DATA:

            bot.edit_message_text(

                "❌ *Aikin ya fita "
                "daga tsarin lokaci.*\n\n"
                "Sake fara `/video`.",

                call.message.chat.id,

                msg_id
            )

            return

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
            else "FILE"
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

        if not PYRO_READY.is_set():

            bot.edit_message_text(

                "❌ *Pyrogram bai shirya ba.*\n\n"
                "Sake gwadawa bayan ɗan lokaci.",

                chat_id,

                msg_id
            )

            return

        if not pyro_loop.is_running():

            raise Exception(
                "Pyrogram asyncio loop baya running."
            )

        bot.edit_message_text(

            (
                "🔄 *Ana fara aikin...*\n\n"
                f"📁 `{file_name}`\n"
                f"🎯 Tsari: `{selected_mode}`"
            ),

            chat_id,

            msg_id
        )

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

        def future_done_callback(
            done_future
        ):

            try:

                exception = (
                    done_future.exception()
                )

                if exception:

                    send_admin_exception(

                        "FUTURE TASK "
                        "YA KOMA DA EXCEPTION",

                        exception
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
        else "DOCUMENT"
    )

    try:

        # -----------------------------------------------------
        # CONNECTION
        # -----------------------------------------------------

        me = await pyro_bot.get_me()

        if not me:

            raise Exception(
                "Pyrogram bai dawo da bot information ba."
            )

        # -----------------------------------------------------
        # STATUS
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # ORIGINAL MESSAGE
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # MEDIA INFORMATION
        # -----------------------------------------------------

        media_type = "UNKNOWN"

        media_size = 0

        media_file_name = (
            selected_file_name
            or "unknown"
        )

        if msg.video:

            media_type = "VIDEO"

            media_size = (
                msg.video.file_size
                or 0
            )

            media_file_name = (
                msg.video.file_name
                or media_file_name
                or "video.mp4"
            )

        elif msg.document:

            media_type = "DOCUMENT"

            media_size = (
                msg.document.file_size
                or 0
            )

            media_file_name = (
                msg.document.file_name
                or media_file_name
                or "file"
            )

        elif msg.animation:

            media_type = "ANIMATION"

            media_size = (
                msg.animation.file_size
                or 0
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

        # =====================================================
        # DOWNLOAD SYSTEM
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
        # END DOWNLOAD
        # =====================================================


        # =====================================================
        # START UPLOAD SYSTEM
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
        # END UPLOAD SYSTEM
        # =====================================================


        # -----------------------------------------------------
        # VERIFY RESULT
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # SUCCESS
        # -----------------------------------------------------

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
                admin_error
            )

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

    except UploadStalledError as e:

        try:

            await edit_status(

                chat_id,

                status_msg_id,

                (
                    "❌ *Upload ya tsaya.*\n\n"
                    "An gwada retry amma "
                    "Telegram bai ci gaba ba."
                )
            )

        except Exception:
            pass

        send_admin_exception(
            "UPLOAD STALLED - ALL RETRIES FAILED",
            e
        )

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
                status_error
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

        # -----------------------------------------------------
        # CLEANUP LOCAL FILE
        # -----------------------------------------------------

        if (
            file_path
            and os.path.exists(file_path)
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
                    e
                )

        # -----------------------------------------------------
        # CLEAN PROGRESS
        # -----------------------------------------------------

        try:

            keys_to_remove = [

                key

                for key in PROGRESS_STATE

                if (
                    len(key) >= 2
                    and key[0] == chat_id
                    and key[1] == status_msg_id
                )
            ]

            for key in keys_to_remove:

                PROGRESS_STATE.pop(
                    key,
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

        send_admin_exception(
            "PYROGRAM LOOP YA CRASH",
            e
        )

    finally:

        try:

            if not pyro_loop.is_closed():

                pyro_loop.close()

        except Exception:
            pass


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

    def do_GET(self):

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

        if self.path == "/health":

            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "application/json"
            )

            self.end_headers()

            self.wfile.write(
                b'{"status":"ok"}'
            )

            return

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

        send_admin_exception(
            "HTTP SERVER YA KASA",
            e
        )


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
        "=" * 70
    )

    print(
        "FULL STARTUP STATUS"
    )

    print(
        "=" * 70
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
