
import os
import time
import shutil
import logging
import tempfile
import threading
from pathlib import Path
import asyncio
import inspect
import math
import telebot
from telebot import types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
import psycopg2
import time
import os

import telebot
from telebot import types

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError




# ============================================================
# ALL CHAT DATABASE CONNECTION
# ============================================================

ALL_CHAT_DATABASE_URL = os.environ.get(
    "ALL_CHAT_DATABASE_URL"
)

if not ALL_CHAT_DATABASE_URL:
    raise RuntimeError(
        "❌ ALL_CHAT_DATABASE_URL is not set"
    )


def get_all_chat_db_conn():

    try:

        chat_conn = psycopg2.connect(
            ALL_CHAT_DATABASE_URL,
            connect_timeout=10,
            sslmode="require"
        )

        chat_conn.autocommit = False

        return chat_conn

    except Exception as e:

        print(
            "❌ ALL CHAT DATABASE CONNECTION ERROR:",
            repr(e)
        )

        raise


# ============================================================
# ALL CHAT DATABASE CONNECTION READY
# ============================================================



# ============================================================
# ALL CHAT DATABASE TABLES
# SHARED BY ALGAITABOT AND ALL WORKERS
# ============================================================

chat_conn = get_all_chat_db_conn()
chat_cur = chat_conn.cursor()


# ================= WORKERS TABLE =================

chat_cur.execute("""
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    bot_username TEXT,
    bot_name TEXT,
    status VARCHAR(30) DEFAULT 'offline',
    capabilities JSONB DEFAULT '{}'::jsonb,
    current_task_id UUID,
    last_heartbeat TIMESTAMPTZ,
    last_started_at TIMESTAMPTZ,
    last_stopped_at TIMESTAMPTZ,
    total_tasks INTEGER DEFAULT 0,
    successful_tasks INTEGER DEFAULT 0,
    failed_tasks INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
)
""")


# ================= CUSTOMERS TABLE =================

chat_cur.execute("""
CREATE TABLE IF NOT EXISTS customers (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    real_name TEXT,
    preferred_name TEXT,
    preferred_name_consent BOOLEAN DEFAULT FALSE,
    preferred_language VARCHAR(20) DEFAULT 'ha',
    detected_language VARCHAR(20) DEFAULT 'ha',
    language_mode VARCHAR(20) DEFAULT 'AUTO',
    language_updated_at TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_interaction_at TIMESTAMPTZ,
    last_session_started_at TIMESTAMPTZ,
    last_session_ended_at TIMESTAMPTZ,
    total_messages BIGINT DEFAULT 0,
    is_blocked BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
)
""")


# ================= TASKS TABLE =================

chat_cur.execute("""
CREATE TABLE IF NOT EXISTS tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id BIGINT NOT NULL
        REFERENCES customers(user_id)
        ON DELETE CASCADE,
    task_type VARCHAR(100) NOT NULL,
    status VARCHAR(30) DEFAULT 'pending',
    priority INTEGER DEFAULT 5,
    assigned_worker TEXT
        REFERENCES workers(worker_id)
        ON DELETE SET NULL,
    payload JSONB DEFAULT '{}'::jsonb,
    result_json JSONB DEFAULT '{}'::jsonb,
    error_message TEXT,
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 5,
    lease_until TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
)
""")


# ================= CHAT MESSAGES TABLE =================

chat_cur.execute("""
CREATE TABLE IF NOT EXISTS chat_messages (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL
        REFERENCES customers(user_id)
        ON DELETE CASCADE,
    task_id UUID
        REFERENCES tasks(task_id)
        ON DELETE SET NULL,
    telegram_chat_id BIGINT,
    telegram_message_id BIGINT,
    reply_to_message_id BIGINT,
    direction VARCHAR(20) NOT NULL,
    sender_type VARCHAR(20) NOT NULL,
    message_type VARCHAR(50) DEFAULT 'text',
    content TEXT,
    file_id TEXT,
    file_unique_id TEXT,
    file_name TEXT,
    mime_type TEXT,
    file_size BIGINT,
    storage_channel_id BIGINT,
    storage_message_id BIGINT,
    language_code VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ
        DEFAULT (NOW() + INTERVAL '12 hours'),
    telegram_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ
)
""")


# ================= CHAT MESSAGES INDEXES =================

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_chat_messages_user
ON chat_messages(user_id, created_at DESC)
""")

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_chat_messages_expiry
ON chat_messages(expires_at)
""")

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_chat_messages_task
ON chat_messages(task_id)
""")

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_chat_messages_telegram
ON chat_messages(telegram_chat_id, telegram_message_id)
""")


# ================= TASKS INDEXES =================

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
ON tasks(status, priority DESC, created_at)
""")

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tasks_worker
ON tasks(assigned_worker, status)
""")

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_tasks_user
ON tasks(user_id, created_at DESC)
""")


# ================= WORKERS INDEX =================

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_workers_status
ON workers(status)
""")


# ================= CUSTOMERS INDEX =================

chat_cur.execute("""
CREATE INDEX IF NOT EXISTS idx_customers_last_interaction
ON customers(last_interaction_at)
""")


# ================= SAVE ALL CHAT TABLES =================

chat_conn.commit()

chat_cur.close()
chat_conn.close()

print("✅ ALL CHAT DATABASE TABLES CREATED SUCCESSFULLY")



# ============================================================
# BOT 2 — MOVIE IDENTIFICATION ONLY
# ============================================================

CLIP_CHANNEL_VIDEO = -1004449824546  # Original video group; configured directly in code
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing from Render Environment Variables.")
if not API_ID_RAW:
    raise RuntimeError("API_ID is missing from Render Environment Variables.")
if not API_HASH:
    raise RuntimeError("API_HASH is missing from Render Environment Variables.")
try:
    API_ID = int(API_ID_RAW)
except ValueError as exc:
    raise RuntimeError("API_ID must be a number.") from exc
try:
    ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW else None
except ValueError:
    ADMIN_ID = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("BOT2")


def send_debug(message):
    """General debug: always log; Telegram admin notification is best effort."""
    try:
        logger.info("DEBUG | %s", str(message).replace("\n", " | "))
    except Exception:
        pass
    try:
        if ADMIN_ID and globals().get("bot"):
            bot.send_message(int(ADMIN_ID), str(message)[:3900], parse_mode=None)
    except Exception as exc:
        logger.warning("Admin debug delivery failed: %s", exc)


bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)

# BOT 2 — INSTANT GROUP MESSAGE LISTENER + DEBUG
# CLIP_CHANNEL_VIDEO VARIABLE REMAINS UNCHANGED
# ==================================================

def bot2_group_message_listener(message):
    try:
        if not message.chat:
            return

        # Check target GROUP ID
        if str(message.chat.id) != str(CLIP_CHANNEL_VIDEO):
            return

        sender = message.from_user
        sender_name = (
            getattr(sender, "first_name", "Unknown")
            if sender else "Unknown"
        )

        sender_id = (
            getattr(sender, "id", "Unknown")
            if sender else "Unknown"
        )

        is_bot = getattr(sender, "is_bot", False) if sender else False

        text = (
            getattr(message, "text", None)
            or getattr(message, "caption", None)
            or "[Babu rubutu]"
        )

        # DEBUG LOG
        BOT2_LOGGER.info(
            "GROUP MESSAGE RECEIVED | "
            "chat_id=%s | message_id=%s | "
            "sender_id=%s | is_bot=%s | text=%s",
            message.chat.id,
            message.message_id,
            sender_id,
            is_bot,
            text[:500]
        )

        # Notify ADMIN
        bot.send_message(
            int(ADMIN_ID),
            "🟢 BOT 2 — AN KARƁI SAƘO A GROUP\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"📩 Saƙo: {text[:2500]}\n"
            f"👤 Sender: {sender_name}\n"
            f"🆔 Sender ID: {sender_id}\n"
            f"🤖 Is Bot: {is_bot}\n"
            f"🆔 Message ID: {message.message_id}\n"
            f"👥 Group ID: {message.chat.id}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ BOT 2 RECEIVED MESSAGE"
        )

    except Exception:
        BOT2_LOGGER.exception(
            "BOT2 GROUP LISTENER ERROR"
        )


@bot.message_handler(
    func=lambda message: (
        message.chat
        and str(message.chat.id) == str(CLIP_CHANNEL_VIDEO)
    ),
    content_types=[
        "text",
        "photo",
        "video",
        "document",
        "audio",
        "voice",
        "video_note",
        "animation"
    ]
)
def bot2_group_message_handler(message):
    bot2_group_message_listener(message)



# BOT 2 — FULL DEBUG MOVIE SCANNER
# BOT 1 COMPATIBLE
# ONE TASK AT A TIME
# ONE GEMINI REQUEST AT A TIME
# ALL 14 ORIGINAL GEMINI MODELS PRESERVED
# FULL VIDEO DOWNLOAD — MAX 50 MB
# EXACTLY 10 FRAMES
# SEND ALL 10 FRAMES TO ADMIN FOR VERIFICATION
# LIVE ADMIN DEBUG AT EVERY STAGE
# BOT 2 OWN PYROGRAM CLIENT — FETCH ORIGINAL GROUP VIDEO
# DATABASE RESULT — USER/TASK ISOLATION
# ============================================================

import os
import asyncio
import re
import json
import time
import base64
import shutil
import tempfile
import threading
import subprocess
import logging
import inspect
import requests
import html

from datetime import datetime, timezone
from telebot.types import InputMediaPhoto


# ============================================================
# CONFIG — MUST MATCH BOT 1
# ============================================================

BOT2_WORKER_ID = "CLIP_BOT_2"
CLIP_TASK_TYPE = "movie_identification"

BOT2_POLL_SECONDS = 3
BOT2_MAX_VIDEO_SIZE = 50 * 1024 * 1024
BOT2_MAX_DOWNLOAD_SECONDS = 3600
BOT2_FRAME_COUNT = 10

BOT2_LEASE_SECONDS = 5400
BOT2_HEARTBEAT_SECONDS = 30

BOT2_ADVISORY_LOCK_ID = 7348291056712345  # Unique lock for this Bot 2 scanner

# Fixed source-video group used by Bot 1 and Bot 2.
CLIP_CHANNEL_VIDEO = -1004449824546

GEMINI_API_KEY = os.getenv(
    "GEMINI_API_KEY", ""
).strip()

# Send all 10 extracted images to admin.
BOT2_SEND_FRAMES_TO_ADMIN = True

# Send download progress updates to admin.
BOT2_DOWNLOAD_PROGRESS_INTERVAL = 10

# Keep admin debug messages in one monitor message.
BOT2_EDIT_MONITOR = True


# ============================================================

# LOGGER AND LOCKS
# ============================================================

BOT2_LOGGER = logging.getLogger("CLIP_BOT_2")

_BOT2_STOP = threading.Event()
_BOT2_STARTED = False
_BOT2_START_LOCK = threading.Lock()

_BOT2_TASK_LOCK = threading.Lock()
_BOT2_GEMINI_LOCK = threading.Lock()

_BOT2_CURRENT_TASK = None
_BOT2_CURRENT_USER_ID = None

_BOT2_ADVISORY_CONN = None

_BOT2_PENDING_MONITORS = {}
_BOT2_PENDING_LOCK = threading.RLock()


# ============================================================

# ============================================================
# START DATABASE SYSTEM
# ============================================================

# GENERAL HELPERS
# ============================================================

def clip2_db():
    return get_all_chat_db_conn()


def clip2_json(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return value or {}


def clip2_size(value):
    try:
        return f"{int(value or 0) / (1024 * 1024):.2f} MB"
    except Exception:
        return "Unknown"


def clip2_safe(value, limit=1500):
    return html.escape(str(value or "Unknown")[:limit])


def clip2_get_user_name(payload):
    return (
        payload.get("real_name")
        or payload.get("preferred_name")
        or payload.get("first_name")
        or payload.get("source_user_name")
        or payload.get("user_name")
        or "Unknown user"
    )


def clip2_get_user_tag(payload):
    username = (
        payload.get("username")
        or payload.get("source_user_tag")
        or payload.get("user_tag")
        or ""
    )

    if username:
        username = str(username).strip()
        if username.startswith("@"):
            return username
        return "@" + username

    return "No username"


def clip2_get_source_ids(payload):

    chat_id = (
        payload.get("telegram_chat_id")
        or payload.get("source_chat_id")
        or payload.get("chat_id")
    )

    message_id = (
        payload.get("telegram_message_id")
        or payload.get("source_message_id")
        or payload.get("message_id")
    )

    if chat_id is None or message_id is None:
        raise RuntimeError(
            "BOT1_SOURCE_MESSAGE_IDS_MISSING"
        )

    return int(chat_id), int(message_id)


# ============================================================
# ADMIN DEBUG SYSTEM
# ALL IMPORTANT STAGES GO TO ADMIN
# ============================================================

def clip2_admin_send(message):

    try:

        if not ADMIN_ID:
            return None

        return bot.send_message(
            int(ADMIN_ID),
            str(message)[:3900]
        )

    except Exception:

        BOT2_LOGGER.exception(
            "ADMIN SEND FAILED"
        )

        return None


def clip2_admin_edit(message_id, message):

    if not message_id:
        return False

    try:

        bot.edit_message_text(
            str(message)[:3900],
            chat_id=int(ADMIN_ID),
            message_id=int(message_id)
        )

        return True

    except Exception:

        BOT2_LOGGER.exception(
            "ADMIN MONITOR EDIT FAILED"
        )

        return False


def clip2_admin_debug(
    task_id,
    user_name,
    user_id,
    stage,
    details,
    monitor_id=None,
    status="INFO"
):

    text = (
        f"{'🔴' if status == 'ERROR' else '🟢'} "
        f"BOT 2 DEBUG — {status}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User: {user_name}\n"
        f"🆔 User ID: <code>{user_id}</code>\n"
        f"🧾 Task ID: <code>{task_id}</code>\n"
        f"📍 Stage: {stage}\n\n"
        f"{details}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    if monitor_id and BOT2_EDIT_MONITOR:

        if clip2_admin_edit(
            monitor_id,
            text
        ):
            return monitor_id

    sent = clip2_admin_send(text)

    if sent:
        return sent.message_id

    return monitor_id


# ============================================================
# ERROR CLASSIFICATION
# ============================================================

def clip2_classify_error(error):

    message = str(error)
    upper = message.upper()

    if "VIDEO_EXCEEDS_50MB" in upper:
        return "VIDEO SIZE"

    if any(x in upper for x in (
        "MESSAGE_NOT_FOUND",
        "CHANNEL_PRIVATE",
        "CHAT_NOT_FOUND",
        "PEER_ID_INVALID",
        "MESSAGE_ID_INVALID",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_MESSAGE_ID"
    )):
        return "TELEGRAM / GROUP MESSAGE"

    if any(x in upper for x in (
        "PYROGRAM",
        "VIDEOCON",
        "DOWNLOAD",
        "FILE_REFERENCE",
        "FILE_ID"
    )):
        return "PYROGRAM / FILE DOWNLOAD"

    if any(x in upper for x in (
        "FFMPEG",
        "FFPROBE",
        "FRAME_",
        "VIDEO_DURATION"
    )):
        return "VIDEO / FFMPEG"

    if any(x in upper for x in (
        "GEMINI",
        "MODEL",
        "API KEY",
        "GENERATECONTENT",
        "HTTP 429",
        "HTTP 404"
    )):
        return "GEMINI API / MODEL"

    if any(x in upper for x in (
        "DATABASE",
        "DB_",
        "POSTGRES",
        "PSYCOPG",
        "CONNECTION"
    )):
        return "DATABASE"

    if any(x in upper for x in (
        "MEMORY",
        "DISK",
        "NO SPACE",
        "RENDER"
    )):
        return "RENDER / SERVER"

    return "BOT 2 PROGRAM / UNKNOWN"


# ============================================================
# POSTGRESQL SINGLE-INSTANCE LOCK
# ============================================================

def clip2_acquire_database_lock():

    global _BOT2_ADVISORY_CONN

    if _BOT2_ADVISORY_CONN is not None:
        return True

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute(
            "SELECT pg_try_advisory_lock(%s)",
            (BOT2_ADVISORY_LOCK_ID,)
        )

        result = cur.fetchone()

        if not result or not result[0]:

            conn.close()

            BOT2_LOGGER.warning(
                "ANOTHER BOT2 INSTANCE OWNS THE LOCK"
            )

            return False

        _BOT2_ADVISORY_CONN = conn

        BOT2_LOGGER.info(
            "BOT2 DATABASE LOCK ACQUIRED"
        )

        return True

    except Exception:

        conn.close()
        raise


def clip2_release_database_lock():

    global _BOT2_ADVISORY_CONN

    conn = _BOT2_ADVISORY_CONN
    _BOT2_ADVISORY_CONN = None

    if conn is None:
        return

    try:

        cur = conn.cursor()

        cur.execute(
            "SELECT pg_advisory_unlock(%s)",
            (BOT2_ADVISORY_LOCK_ID,)
        )

        conn.commit()

    except Exception:

        BOT2_LOGGER.exception(
            "DATABASE LOCK RELEASE FAILED"
        )

    finally:

        try:
            conn.close()
        except Exception:
            pass


# ============================================================
# REGISTER WORKER
# ============================================================

def clip2_register_worker():

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            INSERT INTO workers (
                worker_id,
                bot_name,
                status,
                capabilities,
                last_heartbeat,
                last_started_at,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, 'online', %s::jsonb,
                NOW(), NOW(), NOW(), NOW()
            )
            ON CONFLICT (worker_id)
            DO UPDATE SET
                bot_name = EXCLUDED.bot_name,
                status = 'online',
                capabilities = EXCLUDED.capabilities,
                last_heartbeat = NOW(),
                last_started_at = NOW(),
                updated_at = NOW()
        """, (
            BOT2_WORKER_ID,
            "Clip Movie Scanner Bot 2",
            json.dumps({
                "movie_identification": True,
                "one_task_at_a_time": True,
                "one_gemini_request_at_a_time": True,
                "full_video_download": True,
                "max_video_size": BOT2_MAX_VIDEO_SIZE,
                "frames": BOT2_FRAME_COUNT,
                "gemini_fallback": True,
                "gemini_models": GEMINI_MODELS,
                "admin_monitor": True,
                "admin_frame_preview": True,
                "user_task_isolation": True
            })
        ))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# HEARTBEAT
# ============================================================

def clip2_heartbeat(task_id=None, user_id=None):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE workers
            SET status = 'online',
                current_task_id = %s,
                last_heartbeat = NOW(),
                updated_at = NOW()
            WHERE worker_id = %s
        """, (
            task_id,
            BOT2_WORKER_ID
        ))

        if task_id is not None and user_id is not None:

            cur.execute("""
                UPDATE tasks
                SET heartbeat_at = NOW(),
                    lease_until = NOW() + (%s * INTERVAL '1 second'),
                    updated_at = NOW()
                WHERE task_id = %s
                  AND user_id = %s
                  AND assigned_worker = %s
                  AND status NOT IN (
                      'completed',
                      'delivered',
                      'failed',
                      'timed_out'
                  )
            """, (
                BOT2_LEASE_SECONDS,
                task_id,
                user_id,
                BOT2_WORKER_ID
            ))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


def clip2_heartbeat_loop(task_id, user_id):

    while not _BOT2_STOP.wait(
        BOT2_HEARTBEAT_SECONDS
    ):

        try:

            clip2_heartbeat(
                task_id,
                user_id
            )

        except Exception:

            BOT2_LOGGER.exception(
                "TASK HEARTBEAT FAILED"
            )


# ============================================================
# GET PENDING TASK
# ============================================================

def clip2_get_next_task():

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            SELECT task_id, user_id, payload
            FROM tasks
            WHERE task_type = %s
              AND status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
        """, (CLIP_TASK_TYPE,))

        return cur.fetchone()

    finally:
        conn.close()


# ============================================================
# ADMIN: DETECT PENDING TASK
# SEND USER NAME IMMEDIATELY
# ============================================================

def clip2_pending_admin_notice(
    task_id,
    user_id,
    payload
):

    task_key = str(task_id)

    with _BOT2_PENDING_LOCK:

        if task_key in _BOT2_PENDING_MONITORS:
            return _BOT2_PENDING_MONITORS[task_key]

        user_name = clip2_get_user_name(payload)
        user_tag = clip2_get_user_tag(payload)

        message = (
            "🟡 BOT 2 — AN SAMU PENDING TASK\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 User: {user_name}\n"
            f"🏷 Username: {user_tag}\n"
            f"🆔 User ID: <code>{user_id}</code>\n"
            f"🧾 Task ID: <code>{task_id}</code>\n"
            "📌 Status: PENDING\n"
            "⏳ Bot 2 yana shirin fara aikin wannan user.\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )

        sent = clip2_admin_send(message)

        monitor_id = (
            sent.message_id if sent else None
        )

        _BOT2_PENDING_MONITORS[task_key] = monitor_id

        return monitor_id


# ============================================================
# ATOMIC TASK CLAIM
# ============================================================

def clip2_claim_task(task_id, user_id):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET status = 'processing',
                assigned_worker = %s,
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                lease_until = NOW() + (%s * INTERVAL '1 second'),
                attempts = attempts + 1,
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND task_type = %s
              AND status = 'pending'
            RETURNING task_id
        """, (
            BOT2_WORKER_ID,
            BOT2_LEASE_SECONDS,
            task_id,
            user_id,
            CLIP_TASK_TYPE
        ))

        claimed = cur.fetchone()

        if not claimed:

            conn.commit()
            return False

        cur.execute("""
            UPDATE workers
            SET current_task_id = %s,
                last_heartbeat = NOW(),
                updated_at = NOW()
            WHERE worker_id = %s
        """, (
            task_id,
            BOT2_WORKER_ID
        ))

        conn.commit()

        return True

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# TASK STATUS
# ============================================================

def clip2_set_status(task_id, user_id, status):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET status = %s,
                heartbeat_at = NOW(),
                lease_until = NOW() + (%s * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND assigned_worker = %s
              AND status NOT IN (
                  'completed',
                  'delivered',
                  'failed',
                  'timed_out'
              )
        """, (
            status,
            BOT2_LEASE_SECONDS,
            task_id,
            user_id,
            BOT2_WORKER_ID
        ))

        if cur.rowcount != 1:

            raise RuntimeError(
                f"TASK_STATUS_UPDATE_REJECTED: {status}"
            )

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# SAVE PAYLOAD DEBUG INFORMATION
# ============================================================

def clip2_save_payload(task_id, user_id, extra):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET payload = COALESCE(payload, '{}'::jsonb)
                          || %s::jsonb,
                heartbeat_at = NOW(),
                lease_until = NOW() + (%s * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND assigned_worker = %s
              AND status NOT IN (
                  'completed',
                  'delivered',
                  'failed',
                  'timed_out'
              )
        """, (
            json.dumps(extra),
            BOT2_LEASE_SECONDS,
            task_id,
            user_id,
            BOT2_WORKER_ID
        ))

        if cur.rowcount != 1:

            raise RuntimeError(
                "TASK_PAYLOAD_UPDATE_REJECTED"
            )

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# SAVE GEMINI RESULT TO DATABASE
# ============================================================

def clip2_save_result(
    task_id,
    user_id,
    title,
    year,
    actor,
    model,
    frame_count
):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        result = {
            "title": title,
            "movie_title": title,
            "movie_name": title,
            "year": year,
            "movie_year": year,
            "lead_actor": actor,
            "main_actor": actor,
            "actor": actor,
            "gemini_model": model,
            "frames_extracted": frame_count,
            "identified_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        cur.execute("""
            UPDATE tasks
            SET result_json = %s::jsonb,
                status = 'completed',
                completed_at = NOW(),
                heartbeat_at = NOW(),
                lease_until = NULL,
                error_message = NULL,
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND assigned_worker = %s
              AND status = 'identifying'
            RETURNING task_id
        """, (
            json.dumps(result),
            task_id,
            user_id,
            BOT2_WORKER_ID
        ))

        if not cur.fetchone():

            raise RuntimeError(
                "RESULT_SAVE_STATE_OR_USER_MISMATCH"
            )

        cur.execute("""
            UPDATE workers
            SET successful_tasks = successful_tasks + 1,
                total_tasks = total_tasks + 1,
                current_task_id = NULL,
                last_heartbeat = NOW(),
                updated_at = NOW()
            WHERE worker_id = %s
        """, (BOT2_WORKER_ID,))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# MARK TASK FAILED
# ============================================================

def clip2_mark_failed(task_id, user_id, error):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET status = 'failed',
                error_message = %s,
                lease_until = NULL,
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND assigned_worker = %s
              AND status NOT IN (
                  'completed',
                  'delivered',
                  'failed',
                  'timed_out'
              )
            RETURNING task_id
        """, (
            str(error)[:2000],
            task_id,
            user_id,
            BOT2_WORKER_ID
        ))

        if cur.fetchone():

            cur.execute("""
                UPDATE workers
                SET failed_tasks = failed_tasks + 1,
                    total_tasks = total_tasks + 1,
                    current_task_id = NULL,
                    last_heartbeat = NOW(),
                    updated_at = NOW()
                WHERE worker_id = %s
            """, (BOT2_WORKER_ID,))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================

# ============================================================
# END DATABASE SYSTEM
# ============================================================
# ============================================================
# START GEMINI SPACE PART
# ============================================================


# ============================================================
# ============================================================
# END DATABASE SYSTEM
# ============================================================
# ============================================================
# START GEMINI SPACE PART
# ============================================================

# ALL 14 ORIGINAL MODELS — PRESERVED
# ============================================================

GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-3.1-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemma-4-31b-it",
    "gemma-4-26b-a4b-it",
    "gemma-4-26b-a4b-it",
    "gemini-omni-1.1-flash"
]

_custom_models = os.getenv(
    "MOVIE_AI_MODELS", ""
).strip()

if _custom_models:
    # Preserve all original models; append optional extra models.
    GEMINI_MODELS.extend(
        x.strip()
        for x in _custom_models.split(",")
        if x.strip()
    )

GEMINI_MODELS = list(dict.fromkeys(GEMINI_MODELS))


# ============================================================
# ANSWER PARSER
# ============================================================

def clip2_parse_answer(answer):
    """Parse a strict three-field movie identification response."""
    text = str(answer or "").strip()
    fields = {}

    aliases = {
        "sunan fim": "title",
        "sunan fim din": "title",
        "movie": "title",
        "movie name": "title",
        "title": "title",
        "shekara": "year",
        "year": "year",
        "movie year": "year",
        "jarumi": "actor",
        "jarumai": "actor",
        "actor": "actor",
        "main actor": "actor",
        "lead actor": "actor",
    }

    for line in text.splitlines():
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip().lower().strip("*# ")
        value = value.strip().strip("*# `")

        field = aliases.get(key)
        if field and value:
            fields[field] = value

    title = fields.get("title", "")
    year_raw = fields.get("year", "")
    actor = fields.get("actor", "")

    if not title or title.lower() in {
        "unknown",
        "unknown movie",
        "ba a sani ba",
        "ba a tabbatar ba",
        "not sure",
        "uncertain",
        "n/a"
    }:
        raise RuntimeError(
            "GEMINI_INVALID_OR_UNKNOWN_MOVIE_TITLE"
        )

    year_match = re.search(r"\b(19|20)\d{2}\b", year_raw)

    if not year_match:
        raise RuntimeError(
            "GEMINI_INVALID_OR_MISSING_RELEASE_YEAR"
        )

    year = year_match.group(0)

    if not actor or actor.lower() in {
        "unknown",
        "ba a sani ba",
        "ba a tabbatar ba",
        "n/a",
        "none"
    }:
        raise RuntimeError(
            "GEMINI_INVALID_OR_MISSING_LEAD_ACTOR"
        )

    if len(title) > 160 or len(actor) > 120:
        raise RuntimeError(
            "GEMINI_MALFORMED_IDENTIFICATION_FIELDS"
        )

    return title, year, actor


# ============================================================
# GEMINI ERROR CLASSIFIER
# ============================================================

def clip2_gemini_error_category(
    status_code=None,
    detail="",
    error=None
):
    """Return a human-readable API failure category and next action."""

    msg = str(detail or error or "").lower()

    if (
        status_code == 404
        or "model not found" in msg
        or "not found for api version" in msg
    ):
        return "MODEL_UNAVAILABLE_OR_REMOVED"

    if status_code == 400:
        return "BAD_REQUEST_OR_UNSUPPORTED_MODEL"

    if status_code in (401, 403):
        return "API_KEY_OR_PERMISSION_ERROR"

    if status_code == 429 or "quota" in msg or "rate limit" in msg:
        return "QUOTA_OR_RATE_LIMIT"

    if status_code in (500, 502, 503, 504):
        return "GOOGLE_SERVER_OVERLOAD_OR_CONGESTION"

    if isinstance(error, requests.exceptions.Timeout):
        return "NETWORK_TIMEOUT"

    if isinstance(error, requests.exceptions.ConnectionError):
        return "NETWORK_CONNECTION_ERROR"

    if error:
        return "CLIENT_OR_RESPONSE_ERROR"

    return "UNKNOWN_API_ERROR"


# ============================================================
# CANDIDATE NORMALIZATION
# ============================================================

def clip2_candidate_key(candidate):
    title, year, actor = candidate

    normal = lambda value: re.sub(
        r"[^a-z0-9]+",
        " ",
        value.lower()
    ).strip()

    return (
        normal(title),
        str(year),
        normal(actor)
    )


# ============================================================
# MAIN GEMINI MOVIE IDENTIFICATION SYSTEM
# ============================================================

def clip2_gemini_identify(
    frame_paths,
    task_id,
    user_name,
    user_id,
    monitor_id
):

    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY_MISSING")

    if len(frame_paths) != BOT2_FRAME_COUNT:
        raise RuntimeError("GEMINI_REQUIRES_10_FRAMES")

    # ========================================================
    # SIMPLE HAUSA GEMINI INSTRUCTION
    # ========================================================

    prompt = """Kai ƙwararre ne wajen gano fina-finai daga hotuna.

Ka duba duk hotuna 10 tare domin gano ainihin fim ɗin.

Ka yi bincike sosai kafin ka bayar da amsa.

Ka kula da waɗannan abubuwa:

1. Fuskokin jarumai da yadda suke kama.
2. Tufafi, kayan yaƙi da sauran kayan sawa.
3. Wuraren da aka ɗauki fim ɗin, duwatsu, gine-gine,
   fada, dazuzzuka da yanayin ƙasa.
4. Lokaci da zamanin da yanayin fim ɗin yake nunawa.
5. Abubuwan da ke faruwa a hotunan da alaƙarsu da juna.
6. Idan ka gane jarumai biyu ko fiye, ka yi amfani da
   iliminka wajen tantance fina-finan da suka taɓa
   fitowa tare, da shekarun da suka yi aiki tare.
7. Ka kwatanta fuskokin jarumai, tufafinsu da wuraren
   da ka gani da alamomin fina-finan da ka sani.

Kada ka dogara da fuskar jarumi ɗaya kawai.
Ka haɗa alamomin hotuna 10 kafin ka yanke hukunci.

Misali, idan ka gane jarumi, ka duba ko sauran jaruman,
gine-gine, tufafi da yanayin wurin sun dace da fim ɗin
da kake tunani.

Kada ka ƙirƙiri sunan fim, shekara ko jarumi.
Kada ka bayar da amsar zato kamar tabbatacciyar amsa.
Idan ba ka da isasshen tabbaci, ka rubuta
"BA A TABBATAR BA" a dukkan filayen.

Ka amsa a taƙaice da wannan tsari kawai:

SUNAN FIM: [Sunan fim]
SHEKARA: [Shekarar fitowar fim]
JARUMI: [Babban jarumi]"""

    # ========================================================
    # ATTACH ALL 10 FRAMES
    # ========================================================

    parts = [{"text": prompt}]

    for frame_path in frame_paths:

        with open(frame_path, "rb") as image_file:
            raw = image_file.read()

        if not raw:
            raise RuntimeError(
                "GEMINI_INPUT_FRAME_EMPTY"
            )

        parts.append({
            "inline_data": {
                "mime_type": "image/jpeg",
                "data": base64.b64encode(raw).decode("ascii")
            }
        })

    # ========================================================
    # GEMINI REQUEST BODY
    # ========================================================

    body = {
        "contents": [
            {
                "role": "user",
                "parts": parts
            }
        ],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 180
        }
    }

    failures = []
    valid_candidates = []
    attempted = 0

    # ========================================================
    # MODEL EXECUTION
    # ========================================================

    with _BOT2_GEMINI_LOCK:

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "GEMINI START",
            (
                "🧠 Ana binciken frames 10 duka tare.\n"
                f"🖼 Frames: {len(frame_paths)}\n"
                f"🔢 Original/extra models: {len(GEMINI_MODELS)}\n"
                "🔒 Request concurrency: 1\n"
                "🧪 Ana amfani da sabon umarnin binciken jarumai, "
                "tufafi, wurare da gine-gine."
            ),
            monitor_id
        )

        for index, model_name in enumerate(
            GEMINI_MODELS,
            start=1
        ):

            attempted += 1

            clip2_admin_debug(
                task_id,
                user_name,
                user_id,
                f"GEMINI MODEL {index}/{len(GEMINI_MODELS)}",
                (
                    f"🧠 Model: {model_name}\n"
                    "📡 Ana aika frames 10."
                ),
                monitor_id
            )

            url = (
                "https://generativelanguage.googleapis.com/"
                "v1beta/models/"
                + model_name
                + ":generateContent"
            )

            started = time.time()

            try:

                response = requests.post(
                    url,
                    headers={
                        "Content-Type": "application/json",
                        "x-goog-api-key": GEMINI_API_KEY
                    },
                    json=body,
                    timeout=(20, 240)
                )

                elapsed = time.time() - started

                # =================================================
                # HTTP FAILURE
                # =================================================

                if response.status_code != 200:

                    try:
                        payload = response.json()

                        detail = payload.get(
                            "error", {}
                        ).get(
                            "message",
                            response.text[:900]
                        )

                    except Exception:
                        detail = response.text[:900]

                    category = clip2_gemini_error_category(
                        response.status_code,
                        detail
                    )

                    failures.append(
                        f"{model_name}: HTTP {response.status_code} "
                        f"[{category}]: {detail}"
                    )

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        f"MODEL FAILURE: {model_name}",
                        (
                            f"❌ HTTP: {response.status_code}\n"
                            f"📂 Category: {category}\n"
                            f"📄 Exact reason: {detail}\n"
                            f"⏱ {elapsed:.2f}s\n"
                            "🔄 Ana ci gaba da model na gaba."
                        ),
                        monitor_id,
                        status="ERROR"
                    )

                    continue

                # =================================================
                # JSON RESPONSE
                # =================================================

                try:
                    data = response.json()

                except Exception as error:

                    category = clip2_gemini_error_category(
                        error=error
                    )

                    failures.append(
                        f"{model_name}: INVALID_JSON: {error}"
                    )

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        f"MODEL INVALID JSON: {model_name}",
                        (
                            f"❌ {category}: "
                            f"{str(error)[:900]}"
                        ),
                        monitor_id,
                        status="ERROR"
                    )

                    continue

                # =================================================
                # CANDIDATE CHECK
                # =================================================

                candidates = data.get("candidates") or []

                if not candidates:

                    block_reason = data.get(
                        "promptFeedback", {}
                    ).get(
                        "blockReason",
                        "NO_CANDIDATES"
                    )

                    failures.append(
                        f"{model_name}: {block_reason}"
                    )

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        f"MODEL EMPTY/BLOCKED: {model_name}",
                        (
                            "❌ No candidate. "
                            f"Reason: {block_reason}"
                        ),
                        monitor_id,
                        status="ERROR"
                    )

                    continue

                candidate_obj = candidates[0]

                finish_reason = str(
                    candidate_obj.get(
                        "finishReason",
                        "UNKNOWN"
                    )
                ).upper()

                if finish_reason in (
                    "SAFETY",
                    "RECITATION",
                    "BLOCKLIST"
                ):

                    failures.append(
                        f"{model_name}: {finish_reason}"
                    )

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        f"MODEL BLOCKED: {model_name}",
                        f"❌ finishReason: {finish_reason}",
                        monitor_id,
                        status="ERROR"
                    )

                    continue

                # =================================================
                # EXTRACT GEMINI TEXT
                # =================================================

                response_parts = (
                    candidate_obj.get(
                        "content", {}
                    ).get(
                        "parts", []
                    )
                )

                answer = "\n".join(
                    part.get("text", "")
                    for part in response_parts
                    if part.get("text")
                ).strip()

                if not answer:

                    failures.append(
                        f"{model_name}: EMPTY_RESPONSE"
                    )

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        f"MODEL EMPTY RESPONSE: {model_name}",
                        "❌ HTTP 200 amma babu rubutun amsa.",
                        monitor_id,
                        status="ERROR"
                    )

                    continue

                # =================================================
                # PARSE ANSWER
                # =================================================

                try:

                    parsed = clip2_parse_answer(answer)

                except Exception as error:

                    failures.append(
                        f"{model_name}: INVALID_ANSWER: {error}"
                    )

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        f"MODEL ANSWER REJECTED: {model_name}",
                        (
                            f"❌ Amsa ba ta cika sharudda ba: {error}\n"
                            f"Raw response: {answer[:1000]}\n"
                            "🔄 Ana gwada model na gaba."
                        ),
                        monitor_id,
                        status="ERROR"
                    )

                    continue

                # =================================================
                # SAVE VALID CANDIDATE FOR VERIFICATION
                # =================================================

                valid_candidates.append(
                    (parsed, model_name)
                )

                counts = {}

                for item, _ in valid_candidates:

                    key = clip2_candidate_key(item)

                    counts[key] = counts.get(key, 0) + 1

                clip2_admin_debug(
                    task_id,
                    user_name,
                    user_id,
                    f"MODEL CANDIDATE: {model_name}",
                    (
                        f"🎬 Title: {parsed[0]}\n"
                        f"📅 Year: {parsed[1]}\n"
                        f"⭐ Lead actor: {parsed[2]}\n"
                        f"⏱ {elapsed:.2f}s\n"
                        "🧪 Ana neman independent model confirmation."
                    ),
                    monitor_id
                )

                # =================================================
                # INDEPENDENT VERIFICATION
                # =================================================

                key = clip2_candidate_key(parsed)

                if counts.get(key, 0) >= 2:

                    clip2_admin_debug(
                        task_id,
                        user_name,
                        user_id,
                        "GEMINI INDEPENDENT VERIFICATION PASSED",
                        (
                            "✅ Models biyu sun amince da sakamako.\n"
                            f"🎬 {parsed[0]}\n"
                            f"📅 {parsed[1]}\n"
                            f"⭐ {parsed[2]}\n"
                            f"🧠 Verifying model: {model_name}"
                        ),
                        monitor_id
                    )

                    return (
                        parsed[0],
                        parsed[1],
                        parsed[2],
                        model_name
                    )

                # Next model receives the same frames and prompt.
                # Previous model's answer is not shown to it.
                continue

            # =====================================================
            # NETWORK ERRORS
            # =====================================================

            except requests.exceptions.RequestException as error:

                category = clip2_gemini_error_category(
                    error=error
                )

                failures.append(
                    f"{model_name}: {category}: {error}"
                )

                clip2_admin_debug(
                    task_id,
                    user_name,
                    user_id,
                    f"GEMINI NETWORK FAILURE: {model_name}",
                    (
                        f"❌ {category}\n"
                        f"Exact error: {str(error)[:1200]}\n"
                        "🔄 Next model."
                    ),
                    monitor_id,
                    status="ERROR"
                )

                continue

            # =====================================================
            # OTHER MODEL ERRORS
            # =====================================================

            except Exception as error:

                category = clip2_gemini_error_category(
                    error=error
                )

                failures.append(
                    f"{model_name}: {category}: {error}"
                )

                clip2_admin_debug(
                    task_id,
                    user_name,
                    user_id,
                    f"GEMINI MODEL EXCEPTION: {model_name}",
                    (
                        f"❌ {category}\n"
                        f"{str(error)[:1200]}\n"
                        "🔄 Next model."
                    ),
                    monitor_id,
                    status="ERROR"
                )

                continue

    # ============================================================
    # NO CONSENSUS
    # ============================================================

    if valid_candidates:

        summary = [
            (
                f"{model}: {candidate[0]} | "
                f"{candidate[1]} | {candidate[2]}"
            )
            for candidate, model in valid_candidates
        ]

        raise RuntimeError(
            "GEMINI_NO_INDEPENDENT_CONSENSUS; "
            "no result saved as verified. Candidates: "
            + " || ".join(summary[:8])
        )

    # ============================================================
    # ALL MODELS FAILED
    # ============================================================

    raise RuntimeError(
        "ALL_GEMINI_MODELS_FAILED:\n"
        + "\n".join(failures[-14:])
    )


# ============================================================
# END GEMINI SPACE PART
# ============================================================


# ============================================================
# START PROGRAM SYSTEM
# ============================================================

# BOT 2 PYROGRAM ENGINE
# FETCH THE ORIGINAL GROUP MESSAGE BY chat_id + message_id
# BOT 2 OWNS THIS CLIENT — IT DOES NOT DEPEND ON BOT 1 GLOBALS
# ============================================================

_BOT2_PYRO_CLIENT = None
_BOT2_PYRO_LOOP = None
_BOT2_PYRO_THREAD = None
_BOT2_PYRO_READY = threading.Event()
_BOT2_PYRO_START_ERROR = None
_BOT2_PYRO_START_LOCK = threading.Lock()


def bot2_pyro_run_async(coroutine, timeout=180):
    loop = _BOT2_PYRO_LOOP

    if loop is None or not loop.is_running():
        # Avoid leaving an un-awaited coroutine behind on startup failure.
        try:
            coroutine.close()
        except Exception:
            pass
        raise RuntimeError("BOT2_PYROGRAM_EVENT_LOOP_NOT_RUNNING")

    future = asyncio.run_coroutine_threadsafe(coroutine, loop)

    try:
        return future.result(timeout=timeout)
    except Exception:
        future.cancel()
        raise


async def bot2_pyro_get_message(chat_id, message_id):
    if _BOT2_PYRO_CLIENT is None:
        raise RuntimeError("BOT2_PYROGRAM_CLIENT_NOT_READY")

    message = await _BOT2_PYRO_CLIENT.get_messages(
        chat_id=int(chat_id),
        message_ids=int(message_id)
    )

    # Pyrogram may return a single Message or a list depending on input.
    if isinstance(message, (list, tuple)):
        message = message[0] if message else None

    return message


async def bot2_pyro_download(message, output_path, progress=None, progress_state=None):
    if _BOT2_PYRO_CLIENT is None:
        raise RuntimeError("BOT2_PYROGRAM_CLIENT_NOT_READY")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    downloaded = await _BOT2_PYRO_CLIENT.download_media(
        message,
        file_name=output_path
    )

    if not downloaded:
        raise RuntimeError("BOT2_PYROGRAM_DOWNLOAD_MEDIA_RETURNED_EMPTY")

    return downloaded


def _bot2_pyro_thread_main(api_id, api_hash, bot_token):
    global _BOT2_PYRO_CLIENT
    global _BOT2_PYRO_LOOP
    global _BOT2_PYRO_START_ERROR

    try:
        from pyrogram import Client

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _BOT2_PYRO_LOOP = loop

        client = Client(
            name="bot2_group_video_downloader",
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token,
            in_memory=True,
            no_updates=True
        )

        _BOT2_PYRO_CLIENT = client

        # Pyrogram forks/versions differ: some expose start() as an async
        # method, while others wrap it synchronously and return None.
        # Only pass an actual awaitable to run_until_complete().
        start_result = client.start()
        if inspect.isawaitable(start_result):
            loop.run_until_complete(start_result)

        # Do not announce readiness unless the MTProto client is connected.
        if not getattr(client, "is_connected", False):
            raise RuntimeError(
                "BOT2_PYROGRAM_START_RETURNED_WITHOUT_CONNECTION"
            )

        # Set ready only after Telegram has accepted the MTProto session.
        _BOT2_PYRO_READY.set()
        BOT2_LOGGER.info(
            "BOT2 PYROGRAM CLIENT STARTED — GROUP MESSAGE DOWNLOAD READY"
        )
        clip2_admin_send(
            "✅ BOT 2 PYROGRAM CLIENT STARTED\\n"
            "✅ MTProto connection established\\n"
            "✅ Can fetch original group messages by chat_id/message_id"
        )

        loop.run_forever()

    except Exception as error:
        _BOT2_PYRO_START_ERROR = error
        BOT2_LOGGER.exception("BOT2 PYROGRAM ENGINE START FAILED")
        _BOT2_PYRO_READY.set()
        try:
            clip2_admin_send(
                "🔴 BOT 2 PYROGRAM START FAILED\\n"
                f"Error: {str(error)[:1800]}"
            )
        except Exception:
            BOT2_LOGGER.exception("FAILED TO REPORT PYROGRAM START ERROR")


def bot2_start_pyrogram_engine(timeout=180):
    global _BOT2_PYRO_THREAD

    with _BOT2_PYRO_START_LOCK:
        if _BOT2_PYRO_READY.is_set():
            if _BOT2_PYRO_START_ERROR:
                raise RuntimeError(
                    "BOT2_PYROGRAM_START_FAILED: "
                    + str(_BOT2_PYRO_START_ERROR)
                )
            if _BOT2_PYRO_CLIENT is not None:
                return True

        if _BOT2_PYRO_THREAD is None or not _BOT2_PYRO_THREAD.is_alive():
            api_id_raw = os.getenv("API_ID") or globals().get("API_ID")
            api_hash = (os.getenv("API_HASH") or globals().get("API_HASH") or "").strip()
            bot_token = (
                os.getenv("BOT_TOKEN")
                or globals().get("BOT_TOKEN")
                or getattr(globals().get("bot"), "token", None)
                or ""
            ).strip()

            if not api_id_raw or not api_hash or not bot_token:
                raise RuntimeError(
                    "BOT2_PYROGRAM_CONFIG_MISSING: set API_ID, API_HASH, BOT_TOKEN"
                )

            try:
                api_id = int(api_id_raw)
            except (TypeError, ValueError):
                raise RuntimeError("BOT2_PYROGRAM_API_ID_INVALID")

            _BOT2_PYRO_THREAD = threading.Thread(
                target=_bot2_pyro_thread_main,
                args=(api_id, api_hash, bot_token),
                daemon=True,
                name="bot2-pyrogram-engine"
            )
            _BOT2_PYRO_THREAD.start()

    if not _BOT2_PYRO_READY.wait(timeout=timeout):
        raise RuntimeError("BOT2_PYROGRAM_START_TIMEOUT")

    if _BOT2_PYRO_START_ERROR:
        raise RuntimeError(
            "BOT2_PYROGRAM_START_FAILED: "
            + str(_BOT2_PYRO_START_ERROR)
        )

    if _BOT2_PYRO_CLIENT is None or _BOT2_PYRO_LOOP is None:
        raise RuntimeError("BOT2_PYROGRAM_CLIENT_OR_LOOP_MISSING")

    return True


def clip2_get_group_message(chat_id, message_id):
    if not _BOT2_PYRO_READY.is_set() or _BOT2_PYRO_START_ERROR:
        raise RuntimeError("BOT2_PYROGRAM_NOT_READY")

    message = bot2_pyro_run_async(
        bot2_pyro_get_message(chat_id, message_id),
        timeout=180
    )

    if not message:
        raise RuntimeError(
            f"TELEGRAM_MESSAGE_NOT_FOUND: chat_id={chat_id}, message_id={message_id}"
        )

    return message


# ============================================================
# DOWNLOAD FULL ORIGINAL VIDEO USING BOT 2 PYROGRAM CLIENT
# ============================================================

def clip2_download_video(
    pyro_message,
    output_path,
    task_id,
    user_name,
    user_id,
    monitor_id
):
    progress_state = {
        "time": 0,
        "start_time": time.time(),
        "start_bytes": 0
    }

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "DOWNLOAD START",
        (
            "⬇️ Bot 2 Pyrogram download yana aiki.\\n"
            "📍 An samo ainihin group message ta chat_id/message_id.\\n"
            "📦 Ana sauke cikakken video yanzu."
        ),
        monitor_id
    )

    downloaded_path = bot2_pyro_run_async(
        bot2_pyro_download(
            pyro_message,
            output_path,
            None,
            progress_state
        ),
        timeout=BOT2_MAX_DOWNLOAD_SECONDS
    )

    if not downloaded_path or not os.path.isfile(downloaded_path):
        raise RuntimeError("BOT2_PYROGRAM_FULL_VIDEO_DOWNLOAD_FAILED")

    actual_size = os.path.getsize(downloaded_path)

    if actual_size <= 0:
        raise RuntimeError("DOWNLOADED_VIDEO_IS_EMPTY")

    if actual_size > BOT2_MAX_VIDEO_SIZE:
        raise RuntimeError(
            "DOWNLOADED_VIDEO_EXCEEDS_50MB: " + clip2_size(actual_size)
        )

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "DOWNLOAD COMPLETED",
        (
            "✅ AN SAUKE VIDEO LAFIYA.\\n"
            f"📦 Downloaded size: {clip2_size(actual_size)}\\n"
            f"📁 File path: {downloaded_path}\\n"
            "📌 File yana cikin server yanzu."
        ),
        monitor_id
    )

    return downloaded_path, actual_size


# ============================================================
# VERIFY ORIGINAL VIDEO AND FILE ID
# ============================================================

def clip2_verify_video(
    pyro_message,
    chat_id,
    message_id,
    payload
):

    if not pyro_message:

        raise RuntimeError(
            "TELEGRAM_MESSAGE_NOT_FOUND"
        )

    if int(pyro_message.chat.id) != int(chat_id):

        raise RuntimeError(
            "TELEGRAM_CHAT_ID_MISMATCH"
        )

    if int(pyro_message.id) != int(message_id):

        raise RuntimeError(
            "TELEGRAM_MESSAGE_ID_MISMATCH"
        )

    media = (
        getattr(pyro_message, "video", None)
        or getattr(pyro_message, "document", None)
    )

    if not media:

        raise RuntimeError(
            "TELEGRAM_MESSAGE_HAS_NO_VIDEO"
        )

    if getattr(pyro_message, "document", None):

        mime = (
            getattr(media, "mime_type", "") or ""
        ).lower()

        if not mime.startswith("video/"):

            raise RuntimeError(
                "TELEGRAM_DOCUMENT_IS_NOT_VIDEO"
            )

    actual_size = int(
        getattr(media, "file_size", 0) or 0
    )

    expected_unique = str(
        payload.get("file_unique_id") or ""
    )

    actual_unique = str(
        getattr(media, "file_unique_id", "") or ""
    )

    if (
        expected_unique
        and actual_unique
        and expected_unique != actual_unique
    ):

        raise RuntimeError(
            "TELEGRAM_FILE_UNIQUE_ID_MISMATCH"
        )

    if actual_size <= 0:

        raise RuntimeError(
            "TELEGRAM_VIDEO_SIZE_UNKNOWN"
        )

    if actual_size > BOT2_MAX_VIDEO_SIZE:

        raise RuntimeError(
            "VIDEO_EXCEEDS_50MB: "
            + clip2_size(actual_size)
        )

    if not getattr(media, "file_id", None):

        raise RuntimeError(
            "PYROGRAM_FILE_ID_MISSING"
        )

    return media


# ============================================================
# EXTRACT EXACTLY 10 FRAMES
# ============================================================

def clip2_extract_frames(
    video_path,
    output_dir,
    task_id,
    user_name,
    user_id,
    monitor_id
):

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")

    if not ffmpeg or not ffprobe:

        raise RuntimeError(
            "FFMPEG_OR_FFPROBE_NOT_INSTALLED_ON_RENDER"
        )

    os.makedirs(
        output_dir,
        exist_ok=True
    )

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "VIDEO DURATION CHECK",
        "🎞 Ana karanta tsawon lokacin video ta FFprobe.",
        monitor_id
    )

    probe = subprocess.run(
        [
            ffprobe,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            video_path
        ],
        capture_output=True,
        text=True,
        timeout=90
    )

    if probe.returncode != 0:

        raise RuntimeError(
            "VIDEO_DURATION_READ_FAILED: "
            + probe.stderr[:500]
        )

    try:

        duration = float(
            probe.stdout.strip()
        )

    except Exception:

        raise RuntimeError(
            "VIDEO_DURATION_INVALID"
        )

    if duration <= 0:

        raise RuntimeError(
            "VIDEO_DURATION_ZERO"
        )

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "FRAME EXTRACTION STARTED",
        (
            f"🎞 Video duration: {duration:.2f} seconds\n"
            f"🖼 Ana cire hotuna {BOT2_FRAME_COUNT} daga video."
        ),
        monitor_id
    )

    frames = []

    for index in range(BOT2_FRAME_COUNT):

        timestamp = (
            duration * (index + 0.5)
            / BOT2_FRAME_COUNT
        )

        output_path = os.path.join(
            output_dir,
            f"frame_{index + 1:02d}.jpg"
        )

        result = subprocess.run(
            [
                ffmpeg,
                "-hide_banner",
                "-loglevel", "error",
                "-nostdin",
                "-ss", f"{timestamp:.3f}",
                "-i", video_path,
                "-frames:v", "1",
                "-vf", "scale='min(640,iw)':-2",
                "-q:v", "8",
                "-threads", "1",
                "-y",
                output_path
            ],
            capture_output=True,
            text=True,
            timeout=120
        )

        if (
            result.returncode != 0
            or not os.path.isfile(output_path)
            or os.path.getsize(output_path) <= 0
        ):

            raise RuntimeError(
                f"FRAME_EXTRACTION_FAILED_{index + 1}: "
                + result.stderr[:300]
            )

        frames.append(output_path)

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            f"FRAME {index + 1}/{BOT2_FRAME_COUNT}",
            (
                f"✅ An cire frame {index + 1} cikin 10.\n"
                f"📦 Size: {clip2_size(os.path.getsize(output_path))}"
            ),
            monitor_id
        )

    if len(frames) != BOT2_FRAME_COUNT:

        raise RuntimeError(
            "DID_NOT_EXTRACT_EXACTLY_10_FRAMES"
        )

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "ALL FRAMES READY",
        (
            "✅ An cire hotuna 10 cikin 10.\n"
            "📤 Yanzu za a aika hotunan zuwa Admin domin tabbatarwa."
        ),
        monitor_id
    )

    return frames


# ============================================================
# SEND 10 FRAMES TO ADMIN FOR VISUAL VERIFICATION
# ============================================================

def clip2_send_frames_to_admin(
    frame_paths,
    task_id,
    user_name,
    user_id,
    monitor_id
):

    if not BOT2_SEND_FRAMES_TO_ADMIN:
        return

    if len(frame_paths) != BOT2_FRAME_COUNT:

        raise RuntimeError(
            "ADMIN_FRAME_PREVIEW_REQUIRES_10_FRAMES"
        )

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "ADMIN FRAME PREVIEW",
        "📤 Ana aika hotuna 10 zuwa Admin domin duba abin da Gemini zai gani.",
        monitor_id
    )

    # Telegram media groups allow up to 10 media items.
    media_group = []

    opened_files = []

    try:

        for index, path in enumerate(frame_paths):

            file_obj = open(path, "rb")
            opened_files.append(file_obj)

            media_group.append(
                InputMediaPhoto(
                    media=file_obj,
                    caption=(
                        f"Frame {index + 1}/10 | "
                        f"Task {task_id}"
                    ) if index == 0 else None
                )
            )

        bot.send_media_group(
            chat_id=int(ADMIN_ID),
            media=media_group
        )

    finally:

        for file_obj in opened_files:

            try:
                file_obj.close()
            except Exception:
                pass

    clip2_admin_debug(
        task_id,
        user_name,
        user_id,
        "ADMIN RECEIVED 10 FRAMES",
        (
            "✅ An aika hotuna 10 zuwa Admin.\n"
            "📌 Waɗannan su ne hotunan da aka cire daga video.\n"
            "🧠 Mataki na gaba: Gemini identification."
        ),
        monitor_id
    )


# ============================================================

# TASK FAILURE REPORT
# ============================================================

def clip2_report_failure(
    task_id,
    user_name,
    user_id,
    user_tag,
    stage,
    error,
    monitor_id=None
):

    category = clip2_classify_error(error)

    text = (
        "🔴 BOT 2 — TASK FAILED\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User: {user_name}\n"
        f"🏷 Username: {user_tag}\n"
        f"🆔 User ID: <code>{user_id}</code>\n"
        f"🧾 Task ID: <code>{task_id}</code>\n\n"
        f"📍 Last stage: {stage}\n"
        f"🔍 Error category: {category}\n\n"
        f"❌ Exact error:\n{str(error)[:1800]}\n\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    if monitor_id:
        clip2_admin_edit(
            monitor_id,
            text
        )
    else:
        clip2_admin_send(text)


# ============================================================
# PROCESS ONE TASK
# ============================================================

def clip2_process_one_task(
    task_id,
    user_id,
    raw_payload,
    pending_monitor_id=None
):

    global _BOT2_CURRENT_TASK
    global _BOT2_CURRENT_USER_ID

    task_id = str(task_id)
    user_id = int(user_id)

    payload = clip2_json(raw_payload)

    if not _BOT2_TASK_LOCK.acquire(
        blocking=False
    ):
        return

    temp_dir = None
    stage = "TASK CLAIM"

    monitor_id = pending_monitor_id

    user_name = clip2_get_user_name(payload)
    user_tag = clip2_get_user_tag(payload)

    try:

        _BOT2_CURRENT_TASK = task_id
        _BOT2_CURRENT_USER_ID = user_id

        # ----------------------------------------------------
        # 1. CLAIM TASK
        # ----------------------------------------------------

        if not clip2_claim_task(
            task_id,
            user_id
        ):
            return

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "TASK CLAIMED",
            (
                "✅ Bot 2 ya karɓi task daga database.\n"
                "📌 Task status: processing\n"
                "👤 An tabbatar da user ɗin aikin."
            ),
            monitor_id
        )

        clip2_heartbeat(
            task_id,
            user_id
        )

        heartbeat_thread = threading.Thread(
            target=clip2_heartbeat_loop,
            args=(task_id, user_id),
            daemon=True,
            name=f"bot2-heartbeat-{task_id}"
        )

        heartbeat_thread.start()

        # ----------------------------------------------------
        # 2. VERIFY TASK OWNER
        # ----------------------------------------------------

        stage = "VERIFY TASK OWNER"

        payload_user_id = (
            payload.get("user_id")
            or payload.get("telegram_user_id")
        )

        if (
            payload_user_id is not None
            and int(payload_user_id) != user_id
        ):

            raise RuntimeError(
                "TASK_PAYLOAD_USER_ID_MISMATCH"
            )

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "USER VERIFIED",
            "✅ User ID na task da user ID na payload sun dace.",
            monitor_id
        )

        # ----------------------------------------------------
        # 3. READ BOT 1 SOURCE IDS
        # ----------------------------------------------------

        stage = "READ BOT 1 TASK PAYLOAD"

        chat_id, message_id = (
            clip2_get_source_ids(payload)
        )

        if int(chat_id) != int(CLIP_CHANNEL_VIDEO):

            raise RuntimeError(
                f"WRONG_CLIP_GROUP_ID: {chat_id}"
            )

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "SOURCE MESSAGE IDS RECEIVED",
            (
                f"📍 Group ID: <code>{chat_id}</code>\n"
                f"🆔 Message ID: <code>{message_id}</code>\n"
                "📌 Bot 2 ya karɓi inda ainihin video yake."
            ),
            monitor_id
        )

        # ----------------------------------------------------
        # 4. GET ORIGINAL GROUP MESSAGE
        # ----------------------------------------------------

        stage = "TELEGRAM MESSAGE LOOKUP"

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "PYROGRAM MESSAGE LOOKUP",
            (
                "🔍 Ana neman ainihin sakon video a Group.\n"
                "📡 Pyrogram yana neman chat_id da message_id."
            ),
            monitor_id
        )

        pyro_message = clip2_get_group_message(
            chat_id,
            message_id
        )

        if not pyro_message:

            raise RuntimeError(
                "TELEGRAM_MESSAGE_NOT_FOUND"
            )

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "GROUP MESSAGE FOUND",
            (
                "✅ An samo ainihin sakon Group.\n"
                f"📍 Chat ID: <code>{pyro_message.chat.id}</code>\n"
                f"🆔 Message ID: <code>{pyro_message.id}</code>"
            ),
            monitor_id
        )

        # ----------------------------------------------------
        # 5. VERIFY VIDEO AND FILE ID
        # ----------------------------------------------------

        stage = "VIDEO / FILE ID VERIFICATION"

        media = clip2_verify_video(
            pyro_message,
            chat_id,
            message_id,
            payload
        )

        video_size = int(
            media.file_size or 0
        )

        file_id = media.file_id
        file_unique_id = media.file_unique_id

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "FILE ID RECEIVED",
            (
                "✅ Pyrogram ya samo file_id.\n"
                f"📦 File size: {clip2_size(video_size)}\n"
                f"🆔 File unique ID: <code>{clip2_safe(file_unique_id, 150)}</code>\n"
                f"📄 Media type: {type(media).__name__}\n"
                f"🧾 File ID available: {'YES' if file_id else 'NO'}\n"
                "📌 Video verification ya yi nasara."
            ),
            monitor_id
        )

        clip2_save_payload(
            task_id,
            user_id,
            {
                "bot2_file_id": file_id,
                "bot2_file_unique_id": file_unique_id,
                "bot2_file_size": video_size,
                "bot2_source_verified": True,
                "bot2_verified_user_id": user_id,
                "bot2_verified_chat_id": chat_id,
                "bot2_verified_message_id": message_id
            }
        )

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "FILE INFORMATION SAVED",
            "✅ File information an rubuta a payload na task a DB.",
            monitor_id
        )

        # ----------------------------------------------------
        # 6. START DOWNLOAD
        # ----------------------------------------------------

        stage = "VIDEO DOWNLOAD"

        clip2_set_status(
            task_id,
            user_id,
            "downloading"
        )

        temp_dir = tempfile.mkdtemp(
            prefix="clip_bot2_"
        )

        video_path = os.path.join(
            temp_dir,
            "movie_input.mp4"
        )

        downloaded_path, downloaded_size = (
            clip2_download_video(
                pyro_message,
                video_path,
                task_id,
                user_name,
                user_id,
                monitor_id
            )
        )

        clip2_save_payload(
            task_id,
            user_id,
            {
                "download_completed": True,
                "downloaded_bytes": downloaded_size,
                "downloaded_at": datetime.now(
                    timezone.utc
                ).isoformat()
            }
        )

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "DOWNLOAD DATABASE CHECKPOINT",
            (
                "✅ Download ya kammala.\n"
                f"📦 Downloaded size: {clip2_size(downloaded_size)}\n"
                "💾 An rubuta download_completed=True a DB."
            ),
            monitor_id
        )

        # ----------------------------------------------------
        # 7. EXTRACT 10 FRAMES
        # ----------------------------------------------------

        stage = "FRAME EXTRACTION"

        clip2_set_status(
            task_id,
            user_id,
            "extracting_frames"
        )

        frames_dir = os.path.join(
            temp_dir,
            "frames"
        )

        frames = clip2_extract_frames(
            downloaded_path,
            frames_dir,
            task_id,
            user_name,
            user_id,
            monitor_id
        )

        clip2_save_payload(
            task_id,
            user_id,
            {
                "frames_extracted": len(frames),
                "frames_extraction_completed": True
            }
        )

        # ----------------------------------------------------
        # 8. SEND FRAMES TO ADMIN
        # ----------------------------------------------------

        stage = "ADMIN FRAME PREVIEW"

        clip2_send_frames_to_admin(
            frames,
            task_id,
            user_name,
            user_id,
            monitor_id
        )

        # ----------------------------------------------------
        # 9. GEMINI IDENTIFICATION
        # ----------------------------------------------------

        stage = "GEMINI IDENTIFICATION"

        clip2_set_status(
            task_id,
            user_id,
            "identifying"
        )

        title, year, actor, model = (
            clip2_gemini_identify(
                frames,
                task_id,
                user_name,
                user_id,
                monitor_id
            )
        )

        report = (
            f"Sunan fim: {title}\n"
            f"Shekara: {year or 'Ba a tabbatar ba'}\n"
            f"Jarumi: {actor}"
        )

        # ----------------------------------------------------
        # 10. GEMINI ANSWER RECEIVED
        # ----------------------------------------------------

        stage = "GEMINI ANSWER RECEIVED"

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "GEMINI ANSWER READY",
            (
                "✅ An samu amsa daga Gemini.\n\n"
                f"🎬 Sunan fim: {title}\n"
                f"📅 Shekara: {year or 'Ba a tabbatar ba'}\n"
                f"⭐ Jarumi: {actor}\n"
                f"🧠 Model: {model}\n\n"
                "💾 Mataki na gaba: Ajiye sakamako a database."
            ),
            monitor_id
        )

        # ----------------------------------------------------
        # 11. SAVE RESULT TO DATABASE
        # ----------------------------------------------------

        stage = "DATABASE RESULT SAVE"

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "DATABASE SAVE STARTED",
            (
                "💾 Ana ajiye amsar Gemini a result_json.\n"
                "📌 Ana tabbatar da task_id da user_id."
            ),
            monitor_id
        )

        clip2_save_result(
            task_id,
            user_id,
            title,
            year,
            actor,
            model,
            len(frames)
        )

        # ----------------------------------------------------
        # 12. DATABASE SAVE SUCCESS
        # ----------------------------------------------------

        stage = "DATABASE SAVE VERIFIED"

        clip2_admin_debug(
            task_id,
            user_name,
            user_id,
            "DATABASE SAVE SUCCESS",
            (
                "✅ An ajiye sakamakon Gemini a database.\n"
                "📌 Task status: completed\n"
                "📌 result_json: SAVED\n"
                "📌 Bot 1 zai iya karɓar sakamakon yanzu."
            ),
            monitor_id
        )

        # ----------------------------------------------------
        # 13. ADMIN COMPLETION
        # ----------------------------------------------------

        clip2_admin_send(
            (
                "🎉 BOT 2 — TASK COMPLETED\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 User: {user_name}\n"
                f"🆔 User ID: <code>{user_id}</code>\n"
                f"🧾 Task ID: <code>{task_id}</code>\n\n"
                f"🎬 Sunan fim: {title}\n"
                f"📅 Shekara: {year or 'Ba a tabbatar ba'}\n"
                f"⭐ Jarumi: {actor}\n"
                f"🧠 Model: {model}\n\n"
                "✅ Download: COMPLETED\n"
                "✅ Frames: 10/10\n"
                "✅ Gemini: SUCCESS\n"
                "✅ Database: SAVED\n"
                "━━━━━━━━━━━━━━━━━━━━"
            )
        )

        BOT2_LOGGER.info(
            "BOT2 TASK COMPLETED task=%s user=%s",
            task_id,
            user_id
        )

    except Exception as error:

        BOT2_LOGGER.exception(
            "BOT2 TASK FAILED task=%s user=%s stage=%s",
            task_id,
            user_id,
            stage
        )

        try:

            clip2_mark_failed(
                task_id,
                user_id,
                str(error)
            )

        except Exception:

            BOT2_LOGGER.exception(
                "FAILED TO SAVE TASK FAILURE"
            )

        clip2_report_failure(
            task_id,
            user_name,
            user_id,
            user_tag,
            stage,
            error,
            monitor_id
        )

    finally:

        if temp_dir:

            shutil.rmtree(
                temp_dir,
                ignore_errors=True
            )

        try:

            clip2_heartbeat(None)

        except Exception:

            BOT2_LOGGER.exception(
                "HEARTBEAT CLEANUP FAILED"
            )

        _BOT2_CURRENT_TASK = None
        _BOT2_CURRENT_USER_ID = None

        _BOT2_TASK_LOCK.release()


# ============================================================
# CONTINUOUS DATABASE POLLER
# CHECK PENDING EVERY 3 SECONDS
# ADMIN GETS PENDING USER NAME
# ============================================================

def bot2_clip_database_poller():

    BOT2_LOGGER.info(
        "BOT2 SEQUENTIAL DATABASE POLLER STARTED"
    )

    clip2_admin_send(
        "🟢 BOT 2 DATABASE POLLER STARTED\n"
        "📡 Yana duba pending tasks duk bayan dakika 3."
    )

    while not _BOT2_STOP.is_set():

        try:

            if _BOT2_TASK_LOCK.locked():

                _BOT2_STOP.wait(1)
                continue

            row = clip2_get_next_task()

            if not row:

                _BOT2_STOP.wait(
                    BOT2_POLL_SECONDS
                )

                continue

            task_id, user_id, payload = row

            payload = clip2_json(payload)

            user_name = clip2_get_user_name(
                payload
            )

            # Notify admin immediately after detecting pending.
            pending_monitor_id = (
                clip2_pending_admin_notice(
                    task_id,
                    user_id,
                    payload
                )
            )

            BOT2_LOGGER.info(
                "PENDING TASK FOUND task=%s user=%s name=%s",
                task_id,
                user_id,
                user_name
            )

            # Synchronous: process one task before reading next.
            clip2_process_one_task(
                task_id,
                user_id,
                payload,
                pending_monitor_id
            )

        except Exception as error:

            BOT2_LOGGER.exception(
                "BOT2 DATABASE POLLER ERROR"
            )

            clip2_admin_send(
                "🔴 BOT 2 DATABASE POLLER ERROR\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Category: {clip2_classify_error(error)}\n"
                f"Error: {str(error)[:1800]}\n"
                "━━━━━━━━━━━━━━━━━━━━"
            )

            _BOT2_STOP.wait(5)


# ============================================================
# STARTUP CHECKS
# ============================================================

def clip2_startup():

    global _BOT2_STARTED

    with _BOT2_START_LOCK:

        if _BOT2_STARTED:
            return

        _BOT2_STARTED = True

    try:

        # ----------------------------------------------------
        # 1. CONFIGURATION
        # ----------------------------------------------------

        if not GEMINI_API_KEY:

            raise RuntimeError(
                "GEMINI_API_KEY_MISSING"
            )

        if not GEMINI_MODELS:

            raise RuntimeError(
                "GEMINI_MODELS_EMPTY"
            )

        if "CLIP_CHANNEL_VIDEO" not in globals():

            raise RuntimeError(
                "CLIP_CHANNEL_VIDEO_NOT_DEFINED"
            )

        if not globals().get("bot"):

            raise RuntimeError(
                "TELEGRAM_BOT_OBJECT_NOT_AVAILABLE"
            )

        if not ADMIN_ID:

            raise RuntimeError(
                "ADMIN_ID_NOT_CONFIGURED"
            )

        clip2_admin_send(
            "🟡 BOT 2 STARTUP CHECK\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Checking configuration...\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )

        # ----------------------------------------------------
        # 2. DATABASE CONNECTION TEST
        # ----------------------------------------------------

        conn = clip2_db()

        try:

            cur = conn.cursor()
            cur.execute("SELECT NOW()")
            db_time = cur.fetchone()[0]

        finally:

            conn.close()

        clip2_admin_send(
            "✅ DATABASE CONNECTION OK\n"
            f"Database time: {db_time}"
        )

        # ----------------------------------------------------
        # 3. DATABASE SINGLE INSTANCE LOCK
        # ----------------------------------------------------

        if not clip2_acquire_database_lock():

            raise RuntimeError(
                "ANOTHER_BOT2_INSTANCE_ALREADY_RUNNING"
            )

        clip2_admin_send(
            "✅ DATABASE ADVISORY LOCK ACQUIRED"
        )

        # ----------------------------------------------------
        # 4. REGISTER WORKER
        # ----------------------------------------------------

        clip2_register_worker()

        clip2_admin_send(
            "✅ WORKER REGISTERED IN DATABASE"
        )

        # ----------------------------------------------------
        # 5. START BOT 2 OWN PYROGRAM DOWNLOAD ENGINE
        # ----------------------------------------------------

        clip2_admin_send(
            "⏳ STARTING BOT 2 PYROGRAM DOWNLOAD ENGINE..."
        )

        bot2_start_pyrogram_engine(
            timeout=int(os.getenv("BOT2_PYROGRAM_START_TIMEOUT", "180"))
        )

        clip2_admin_send(
            "✅ BOT 2 PYROGRAM ENGINE READY\\n"
            "✅ Group message lookup helper ready\\n"
            "✅ Full video download helper ready\\n"
            "✅ Independent MTProto client ready"
        )

        # ----------------------------------------------------
        # 6. FFMPEG CHECK
        # ----------------------------------------------------

        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")

        if not ffmpeg or not ffprobe:

            raise RuntimeError(
                "FFMPEG_OR_FFPROBE_NOT_INSTALLED_ON_RENDER"
            )

        clip2_admin_send(
            "✅ FFMPEG AND FFPROBE FOUND\n"
            f"FFmpeg: {ffmpeg}\n"
            f"FFprobe: {ffprobe}"
        )

        # ----------------------------------------------------
        # 7. START DATABASE POLLER
        # ----------------------------------------------------

        threading.Thread(
            target=bot2_clip_database_poller,
            daemon=True,
            name="bot2-sequential-poller"
        ).start()

        # ----------------------------------------------------
        # 8. ADMIN ONLINE MESSAGE
        # ----------------------------------------------------

        clip2_admin_send(
            "🟢 BOT 2 FULL DEBUG SYSTEM ONLINE\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Worker: {BOT2_WORKER_ID}\n"
            f"Task type: {CLIP_TASK_TYPE}\n"
            f"Group: <code>{CLIP_CHANNEL_VIDEO}</code>\n"
            f"Maximum video: {clip2_size(BOT2_MAX_VIDEO_SIZE)}\n"
            f"Frames: {BOT2_FRAME_COUNT}\n"
            f"Gemini models: {len(GEMINI_MODELS)}\n"
            "Task concurrency: 1\n"
            "Gemini concurrency: 1\n"
            "Full video download: ENABLED\n"
            "Admin frame preview: ENABLED\n"
            "Admin stage debugging: ENABLED\n"
            "Database result: ENABLED\n"
            "User/task isolation: ENABLED\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Bot 2 yana jiran pending task."
        )

        BOT2_LOGGER.info(
            "BOT2 FULL DEBUG SYSTEM READY"
        )

    except Exception as error:

        BOT2_LOGGER.exception(
            "BOT2 STARTUP FAILED"
        )

        clip2_release_database_lock()

        clip2_admin_send(
            "🔴 BOT 2 STARTUP FAILED\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Category: {clip2_classify_error(error)}\n"
            f"Error: {str(error)[:1800]}\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )

        raise


# ============================================================
# ============================================================
# END PROGRAM SYSTEM
# ============================================================


# ============================================================
# RENDER HEALTH SERVER + SINGLE TELEBOT POLLING LOOP
# ============================================================

from flask import Flask

server = Flask(__name__)

@server.route("/")
def health_home():
    return "Bot 2 Movie Scanner is running", 200

@server.route("/health")
def health_check():
    return "OK", 200

@server.route("/status")
def health_status():
    return {"status": "online", "service": "BOT_2_MOVIE_SCANNER"}, 200


def run_web_server():
    port = int(os.getenv("PORT", "10000"))
    logger.info("Starting health server on 0.0.0.0:%s", port)
    server.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)


def polling_loop():
    """One TeleBot polling loop for Bot 2 only; retries transient failures."""
    while True:
        try:
            logger.info("Starting Bot 2 TeleBot polling")
            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
                skip_pending=True,
                allowed_updates=["message", "callback_query"]
            )
        except Exception as exc:
            logger.exception("BOT2 TELEBOT POLLING ERROR")
            send_debug(
                "🔴 BOT 2 TELEBOT POLLING ERROR\n"
                f"{type(exc).__name__}: {str(exc)[:1800]}\n"
                "Za sake gwadawa bayan seconds 5."
            )
            time.sleep(5)


def main():
    logger.info("BOT 2 MOVIE SCANNER STARTING")
    web_thread = threading.Thread(
        target=run_web_server,
        name="BOT2-Health-HTTP",
        daemon=True
    )
    web_thread.start()

    # This is the only scanner startup path. It starts Bot 2's own
    # Pyrogram engine, DB poller, worker registration and debug monitor.
    clip2_startup()

    send_debug(
        "🚀 BOT 2 MOVIE SCANNER STARTED\n"
        "✅ Database poller\n"
        "✅ Bot 2-owned Pyrogram downloader\n"
        "✅ Gemini movie identification\n"
        "✅ Full admin debugging\n"
        "❌ Video Converter removed\n"
        "❌ Compressor removed"
    )
    polling_loop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Bot 2 stopped manually")
        _BOT2_STOP.set()
        clip2_release_database_lock()
    except Exception as exc:
        logger.exception("FATAL BOT 2 STARTUP ERROR")
        try:
            send_debug(
                "💥 FATAL BOT 2 ERROR\n"
                f"{type(exc).__name__}: {str(exc)[:2500]}"
            )
        except Exception:
            pass
        raise
