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
# /COMP — ADMIN VIDEO COMPRESSOR
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
# Bot shows target-size buttons
#    ↓
# Admin chooses target
#    ↓
# Download
#    ↓
# FFmpeg 2-pass compression
#    ↓
# Upload compressed video
#
# IMPORTANT:
# - Admin only
# - Does NOT touch your database
# - Does NOT touch delivery system
# - Progress Telegram edits are limited to once / 20 sec
# - Original video is never modified
# ============================================================

import os
import time
import math
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
COMP_MAX_INPUT_BYTES = COMP_MAX_INPUT_MB * 1024 * 1024

COMP_MAX_OUTPUT_MB = 50
COMP_MAX_OUTPUT_BYTES = COMP_MAX_OUTPUT_MB * 1024 * 1024

# Telegram progress edit interval
COMP_PROGRESS_INTERVAL = 20

# Audio bitrate
COMP_AUDIO_BITRATE = 64_000

# FFmpeg
COMP_PRESET = "fast"

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
            return f"{size_bytes / 1024:.2f} KB"

        if size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"

        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    except Exception:

        return "Unknown"


# ============================================================
# TIME FORMAT
# ============================================================

def _comp_format_time(seconds):

    try:

        seconds = int(max(0, seconds))

        hours = seconds // 3600

        minutes = (seconds % 3600) // 60

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

def _comp_get_duration(ffprobe, input_file):

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
# TELEGRAM SAFE EDIT
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

            if (
                "message is not modified"
                in str(e).lower()
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

            state["last_edit"] = time.monotonic()

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
# PROGRESS HEARTBEAT
#
# Wannan yana aiki lokacin DOWNLOAD da UPLOAD.
# Ba ya edit fiye da sau ɗaya cikin 20 sec.
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

        if elapsed < COMP_PROGRESS_INTERVAL:

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
# TARGET SIZE BUTTONS
#
# Misali 14 MB:
# 15% ≈ 2 MB
# 45% ≈ 6 MB
# 65% ≈ 9 MB
#
# Wannan yana bada irin tsarin da ka nema.
# ============================================================

def _comp_target_options(original_mb):

    values = []

    ratios = [
        0.15,
        0.45,
        0.65
    ]

    for ratio in ratios:

        target = max(
            1,
            int(original_mb * ratio)
        )

        if target >= original_mb:

            target = max(
                1,
                int(original_mb) - 1
            )

        if target not in values:

            values.append(target)

    return values


# ============================================================
# CALCULATE VIDEO BITRATE
# ============================================================

def _comp_calculate_bitrate(
    target_mb,
    duration
):

    if duration <= 0:

        raise RuntimeError(
            "An kasa gano duration na video."
        )

    target_bytes = (
        target_mb *
        1024 *
        1024
    )

    target_bits = (
        target_bytes *
        8
    )

    # Leave some space for MP4/container overhead
    total_bitrate = (
        target_bits /
        duration *
        0.92
    )

    audio_bitrate = COMP_AUDIO_BITRATE

    video_bitrate = (
        total_bitrate -
        audio_bitrate
    )

    # Extremely low bitrates are unsafe
    if video_bitrate < 24_000:

        raise RuntimeError(
            "Target size ya yi ƙanƙanta "
            "ga tsawon wannan video."
        )

    return int(video_bitrate)


# ============================================================
# RUN FFMPEG 2-PASS
#
# 2-pass ne ake amfani da shi domin
# kusantar target size sosai.
# ============================================================

def _comp_run_ffmpeg(
    ffmpeg,
    input_file,
    output_file,
    duration,
    target_mb,
    progress_message,
    state
):

    video_bitrate = _comp_calculate_bitrate(
        target_mb,
        duration
    )

    bitrate_k = max(
        24,
        int(video_bitrate / 1000)
    )

    passlog = os.path.join(
        os.path.dirname(output_file),
        "comp_pass"
    )

    # ========================================================
    # PASS 1
    # ========================================================

    command_pass1 = [

        ffmpeg,

        "-y",

        "-i",
        input_file,

        "-map",
        "0:v:0",

        "-c:v",
        "libx264",

        "-b:v",
        f"{bitrate_k}k",

        "-preset",
        COMP_PRESET,

        "-pass",
        "1",

        "-passlogfile",
        passlog,

        "-an",

        "-f",
        "null",

        os.devnull
    ]

    print(
        "COMP PASS 1:",
        " ".join(command_pass1)
    )

    process1 = subprocess.Popen(
        command_pass1,

        stdout=subprocess.DEVNULL,

        stderr=subprocess.PIPE,

        text=True
    )

    last_edit = time.monotonic()

    while True:

        if process1.poll() is not None:

            break

        now = time.monotonic()

        if (
            now - last_edit
            >= COMP_PROGRESS_INTERVAL
        ):

            _comp_force_edit(
                progress_message.chat.id,
                progress_message.message_id,

                (
                    "⚙️ <b>Compressing video...</b>\n\n"
                    "🔧 Pass: <b>1 / 2</b>\n"
                    f"🎯 Target: <b>{target_mb} MB</b>\n"
                    f"📊 Video bitrate: "
                    f"<b>{bitrate_k} kbps</b>\n\n"
                    "⏳ Please wait..."
                ),

                state
            )

            last_edit = now

        time.sleep(1)

    stderr1 = ""

    try:

        stderr1 = process1.stderr.read()

    except Exception:

        pass

    if process1.returncode != 0:

        print(
            "PASS 1 ERROR:",
            stderr1[-5000:]
        )

        raise RuntimeError(
            "FFmpeg Pass 1 ya kasa."
        )

    # ========================================================
    # PASS 2
    # ========================================================

    command_pass2 = [

        ffmpeg,

        "-y",

        "-i",
        input_file,

        "-map",
        "0:v:0",

        "-map",
        "0:a?",

        "-c:v",
        "libx264",

        "-b:v",
        f"{bitrate_k}k",

        "-preset",
        COMP_PRESET,

        "-pass",
        "2",

        "-passlogfile",
        passlog,

        "-c:a",
        "aac",

        "-b:a",
        "64k",

        "-movflags",
        "+faststart",

        "-pix_fmt",
        "yuv420p",

        "-progress",
        "pipe:1",

        "-nostats",

        output_file
    ]

    print(
        "COMP PASS 2:",
        " ".join(command_pass2)
    )

    process2 = subprocess.Popen(
        command_pass2,

        stdout=subprocess.PIPE,

        stderr=subprocess.PIPE,

        text=True,

        bufsize=1
    )

    current_time = 0.0

    last_edit = time.monotonic()

    while True:

        line = process2.stdout.readline()

        if not line:

            if process2.poll() is not None:

                break

            time.sleep(0.2)

            continue

        line = line.strip()

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
                    ) / 1_000_000
                )

            except Exception:

                pass

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
                            current_time /
                            duration
                        ) * 100
                    )
                )

            else:

                percent = 0

            _comp_force_edit(

                progress_message.chat.id,

                progress_message.message_id,

                (
                    "⚙️ <b>Compressing video...</b>\n\n"

                    "🔧 Pass: <b>2 / 2</b>\n"

                    f"📊 Progress: "
                    f"<b>{percent}%</b>\n"

                    f"⏱️ "
                    f"<b>{_comp_format_time(current_time)}</b>"
                    " / "
                    f"<b>{_comp_format_time(duration)}</b>\n"

                    f"🎯 Target: "
                    f"<b>{target_mb} MB</b>\n\n"

                    "⏳ Please wait..."
                ),

                state
            )

            last_edit = now

    return_code = process2.wait()

    stderr2 = ""

    try:

        stderr2 = process2.stderr.read()

    except Exception:

        pass

    if return_code != 0:

        print(
            "PASS 2 ERROR:",
            stderr2[-5000:]
        )

        raise RuntimeError(
            "FFmpeg Pass 2 ya kasa."
        )

    # ========================================================
    # CLEAN PASS FILES
    # ========================================================

    for suffix in [
        "-0.log",
        "-0.log.mbtree"
    ]:

        try:

            path = (
                passlog +
                suffix
            )

            if os.path.exists(path):

                os.remove(path)

        except Exception:

            pass

    return True


# ============================================================
# /COMP COMMAND
# ============================================================

@bot.message_handler(
    commands=["comp"]
)
def comp_command(message):

    global _comp_running

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
    # CHECK FFMPEG
    # ========================================================

    try:

        _comp_check_ffmpeg()

    except Exception as e:

        bot.reply_to(
            message,

            (
                "❌ <b>FFmpeg ba a shirya ba.</b>\n\n"
                f"<code>{str(e)}</code>"
            ),

            parse_mode="HTML"
        )

        return

    # ========================================================
    # PREVENT TWO JOBS
    # ========================================================

    with _comp_lock:

        if _comp_running:

            bot.reply_to(
                message,

                (
                    "⚠️ <b>Compression yana gudana yanzu.</b>\n\n"
                    "Da fatan ka jira ya gama."
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

            "Turo min video ɗin da kake son "
            "mu compress.\n\n"

            f"📦 Maximum: "
            f"<b>{COMP_MAX_INPUT_MB} MB</b>\n\n"

            "⏳ Bayan ka turo shi zan karanta "
            "ainihin size ɗinsa sannan zan baka "
            "zabukan size."
        ),

        parse_mode="HTML"
    )


# ============================================================
# RECEIVE VIDEO
#
# MUHIMMI:
# Wannan handler ya kamata ya kasance KAFIN
# wani generic video handler naka idan kana da shi.
# ============================================================

@bot.message_handler(
    content_types=["video"],
    func=lambda message:
        message.from_user.id in
        _comp_waiting_for_video
)
def comp_receive_video(message):

    user_id = message.from_user.id

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
    # GET TELEGRAM FILE INFO
    # ========================================================

    try:

        file_info = bot.get_file(
            message.video.file_id
        )

    except Exception as e:

        bot.reply_to(
            message,

            (
                "❌ An kasa karɓar bayanan "
                "video.\n\n"
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
            "❌ An kasa gano ainihin girman video."
        )

        return

    # ========================================================
    # INPUT LIMIT
    # ========================================================

    if file_size > COMP_MAX_INPUT_BYTES:

        bot.reply_to(

            message,

            (
                "❌ <b>Video ya yi girma.</b>\n\n"

                f"📦 Video: "
                f"<b>{_comp_format_size(file_size)}</b>\n"

                f"📦 Maximum: "
                f"<b>{COMP_MAX_INPUT_MB} MB</b>"
            ),

            parse_mode="HTML"
        )

        return

    original_mb = (
        file_size /
        (1024 * 1024)
    )

    # ========================================================
    # GET DURATION FROM TELEGRAM FIRST
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
    # TARGET BUTTONS
    # ========================================================

    targets = _comp_target_options(
        original_mb
    )

    if not targets:

        bot.reply_to(
            message,
            "❌ Babu target size mai kyau."
        )

        return

    # ========================================================
    # SAVE PENDING VIDEO
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
    # BUTTONS
    # ========================================================

    kb = types.InlineKeyboardMarkup(
        row_width=3
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

    kb.add(*buttons)

    # ========================================================
    # ASK ADMIN
    # ========================================================

    bot.reply_to(

        message,

        (
            "🎬 <b>VIDEO RECEIVED</b>\n\n"

            f"📦 Original size: "
            f"<b>{_comp_format_size(file_size)}</b>\n"

            f"⏱️ Duration: "
            f"<b>{_comp_format_time(duration)}</b>\n\n"

            "❓ <b>MB nawa kake son "
            "video ɗin ya koma?</b>\n\n"

            "Zaɓi ɗaya daga cikin buttons:"
        ),

        reply_markup=kb,

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

    user_id = call.from_user.id

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
    # GET TARGET
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
            "Invalid target."
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
            "❌ Wannan compression session ta ƙare."
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

    bot.answer_callback_query(
        call.id,
        "Starting compression..."
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
# MAIN COMPRESSION WORKER
# ============================================================

def _comp_process_job(
    call,
    job,
    target_mb
):

    global _comp_running

    user_id = call.from_user.id

    temp_dir = None

    input_file = None

    output_file = None

    status_message = None

    heartbeat_stop = None

    heartbeat_thread = None

    state = {

        "last_edit": 0.0,

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
        # INITIAL STATUS
        # ====================================================

        status_message = bot.send_message(

            user_id,

            (
                "⏳ <b>Preparing...</b>\n\n"

                f"📦 Original: "
                f"<b>{job['original_mb']:.2f} MB</b>\n"

                f"🎯 Target: "
                f"<b>{target_mb} MB</b>\n\n"

                "🔧 Checking compression system..."
            ),

            parse_mode="HTML"
        )

        # ====================================================
        # FFMPEG
        # ====================================================

        ffmpeg, ffprobe = (
            _comp_check_ffmpeg()
        )

        # ====================================================
        # DOWNLOAD
        # ====================================================

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "⬇️ <b>Downloading video...</b>\n\n"

                f"📦 Size: "
                f"<b>{job['original_mb']:.2f} MB</b>\n\n"

                "⏳ Please wait..."
            ),

            state
        )

        heartbeat_stop = threading.Event()

        heartbeat_thread = threading.Thread(

            target=_comp_heartbeat,

            args=(

                user_id,

                status_message.message_id,

                state,

                heartbeat_stop,

                "⬇️ <b>Downloading video...</b>"
            ),

            daemon=True
        )

        heartbeat_thread.start()

        # ====================================================
        # ACTUAL DOWNLOAD
        # ====================================================

        file_data = bot.download_file(
            bot.get_file(
                job["file_id"]
            ).file_path
        )

        with open(
            input_file,
            "wb"
        ) as f:

            f.write(file_data)

        heartbeat_stop.set()

        # ====================================================
        # VERIFY DOWNLOAD
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
                "An kasa gano duration na video."
            )

        # ====================================================
        # DOWNLOAD COMPLETE
        # ====================================================

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "✅ <b>Download complete.</b>\n\n"

                f"📦 Downloaded: "
                f"<b>{_comp_format_size(downloaded_size)}</b>\n"

                f"⏱️ Duration: "
                f"<b>{_comp_format_time(duration)}</b>\n\n"

                f"🎯 Target: "
                f"<b>{target_mb} MB</b>\n\n"

                "⚙️ Preparing compression..."
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
                "FFmpeg bai samar da output file ba."
            )

        output_size = os.path.getsize(
            output_file
        )

        if output_size <= 0:

            raise RuntimeError(
                "Compressed file empty ne."
            )

        # ====================================================
        # OUTPUT TOO LARGE
        # ====================================================

        if output_size > COMP_MAX_OUTPUT_BYTES:

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
                "✅ <b>Compression complete!</b>\n\n"

                f"📦 Original: "
                f"<b>{_comp_format_size(downloaded_size)}</b>\n"

                f"📦 Compressed: "
                f"<b>{_comp_format_size(output_size)}</b>\n"

                f"🎯 Target: "
                f"<b>{target_mb} MB</b>\n\n"

                "📤 <b>Uploading compressed video...</b>\n\n"

                "⏳ Please wait..."
            ),

            state
        )

        # ====================================================
        # UPLOAD HEARTBEAT
        # ====================================================

        heartbeat_stop = threading.Event()

        heartbeat_thread = threading.Thread(

            target=_comp_heartbeat,

            args=(

                user_id,

                status_message.message_id,

                state,

                heartbeat_stop,

                "📤 <b>Uploading compressed video...</b>"
            ),

            daemon=True
        )

        heartbeat_thread.start()

        # ====================================================
        # UPLOAD TO ADMIN
        # ====================================================

        with open(
            output_file,
            "rb"
        ) as video_file:

            sent_message = bot.send_video(

                user_id,

                video_file,

                caption=(

                    "✅ <b>Compression Completed</b>\n\n"

                    f"📦 Original: "
                    f"<b>{_comp_format_size(downloaded_size)}</b>\n"

                    f"📦 Compressed: "
                    f"<b>{_comp_format_size(output_size)}</b>\n"

                    f"🎯 Target: "
                    f"<b>{target_mb} MB</b>\n\n"

                    "🎬 Video Compressor"
                ),

                parse_mode="HTML",

                supports_streaming=True,

                timeout=600
            )

        heartbeat_stop.set()

        # ====================================================
        # FINAL STATUS
        # ====================================================

        _comp_force_edit(

            user_id,

            status_message.message_id,

            (
                "✅ <b>COMPRESSION COMPLETED</b>\n\n"

                f"📦 Original: "
                f"<b>{_comp_format_size(downloaded_size)}</b>\n"

                f"📦 Final: "
                f"<b>{_comp_format_size(output_size)}</b>\n"

                f"🎯 Target: "
                f"<b>{target_mb} MB</b>\n\n"

                "📤 Uploaded successfully ✅"
            ),

            state
        )

        print(
            "✅ COMP completed:",
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
    # ERROR HANDLING
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
                        "❌ <b>COMPRESSION ERROR</b>\n\n"

                        f"<code>{str(e)}</code>\n\n"

                        "Da fatan ka sake gwadawa."
                    ),

                    state
                )

            else:

                bot.send_message(

                    user_id,

                    (
                        "❌ <b>Compression Error</b>\n\n"
                        f"<code>{str(e)}</code>"
                    ),

                    parse_mode="HTML"
                )

        except Exception as notify_error:

            print(
                "COMP error notification failed:",
                repr(notify_error)
            )

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
        # DELETE TEMP FILES
        # ====================================================

        try:

            if temp_dir and os.path.exists(
                temp_dir
            ):

                shutil.rmtree(
                    temp_dir,
                    ignore_errors=True
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

