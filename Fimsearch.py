
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

# Serialize shared-schema initialization across Bot 2 instances.
# Use a separate key from BOT2_ADVISORY_LOCK_ID: that key is held as a
# session-level singleton lock for the bot's lifetime. Reusing it here makes
# a redeploy wait on the previous live instance until statement_timeout.
# This transaction-level initialization lock is released by chat_conn.commit().
chat_cur.execute(
    "SELECT pg_advisory_xact_lock(%s)",
    (7348291056712346,)
)


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
ADMIN_GEMI = 6603268127  # Dedicated Gemini admin Telegram ID
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






# ============================================================
# INDEPENDENT GEMINI PHOTO MOVIE TEST
# GOOGLE SEARCH GROUNDING + ALL 14 MODELS
#
# PRIVATE USERS
# NO ADMIN ID DEBUG
# TEST EVERY MODEL EVEN AFTER SUCCESS
# PRINT EVERY ERROR TO THE USER
# PRINT EVERY MODEL ANSWER TO THE USER
#
# DOES NOT TOUCH:
# BOT2 WORKERS
# TASK QUEUE
# EXISTING DATABASE
# MOVIE SCANNER
# EXISTING CONSENSUS SYSTEM
# ============================================================

import uuid
import threading
import requests
import base64
import html
import time
import logging
import json

from telebot import types


# ============================================================
# 1. CONFIGURATION
# ============================================================

MOVIE_TEST_MAX_PHOTOS = 10
MOVIE_TEST_ALBUM_WAIT = 3.0

MOVIE_TEST_CONNECT_TIMEOUT = 25
MOVIE_TEST_READ_TIMEOUT = 120

MOVIE_TEST_TIMEOUT = (
    MOVIE_TEST_CONNECT_TIMEOUT,
    MOVIE_TEST_READ_TIMEOUT
)

MOVIE_TEST_SESSIONS = {}
MOVIE_TEST_LOCK = threading.RLock()

MOVIE_TEST_ENABLE_GOOGLE_SEARCH = True

MOVIE_TEST_MAX_ANSWER_LENGTH = 3000
MOVIE_TEST_MAX_ERROR_LENGTH = 1800
MOVIE_TEST_MAX_SOURCES = 8


# ============================================================
# 2. SAFE LOGGING
# ============================================================

def movie_test_log(message, *args):

    try:
        logger.info(message, *args)
    except Exception:
        logging.info(message, *args)


def movie_test_exception(message):

    try:
        logger.exception(message)
    except Exception:
        logging.exception(message)


def movie_test_safe(value, limit=1500):

    return html.escape(
        str(value or "")[:limit]
    )


# ============================================================
# 3. TELEGRAM DEBUG TO USER
# ============================================================

def movie_test_send_user(chat_id, text):

    try:

        text = str(text)

        # Telegram message limit protection.
        max_length = 3900

        if len(text) <= max_length:

            return bot.send_message(
                chat_id,
                text,
                parse_mode="HTML",
                disable_web_page_preview=True
            )

        sent_messages = []

        for start in range(0, len(text), max_length):

            chunk = text[start:start + max_length]

            sent = bot.send_message(
                chat_id,
                chunk,
                parse_mode="HTML",
                disable_web_page_preview=True
            )

            sent_messages.append(sent)

        return sent_messages[-1] if sent_messages else None

    except Exception as error:

        movie_test_exception(
            "MOVIE TEST TELEGRAM SEND ERROR"
        )

        movie_test_log(
            "MOVIE TEST TELEGRAM ERROR DETAILS: %s",
            str(error)[:500]
        )

        return None


def movie_test_debug(chat_id, text, message_id=None):

    try:

        text = str(text)[:3900]

        if message_id:

            try:

                bot.edit_message_text(
                    text,
                    chat_id,
                    message_id,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )

                return message_id

            except Exception:
                pass

        sent = movie_test_send_user(
            chat_id,
            text
        )

        return sent.message_id if sent else message_id

    except Exception as error:

        movie_test_exception(
            "MOVIE TEST DEBUG DELIVERY ERROR"
        )

        return message_id


# ============================================================
# 4. SESSION KEY
# ============================================================

def movie_test_session_key(message):

    return (
        message.chat.id,
        message.from_user.id
    )


# ============================================================
# 5. RECEIVE PHOTOS
# ============================================================

@bot.message_handler(
    content_types=["photo"],
    func=lambda m: (
        m.chat is not None
        and m.chat.type == "private"
        and m.from_user is not None
    )
)
def movie_test_receive_photo(message):

    try:

        key = movie_test_session_key(message)

        with MOVIE_TEST_LOCK:

            session = MOVIE_TEST_SESSIONS.setdefault(
                key,
                {
                    "photos": [],
                    "timer": None,
                    "token": uuid.uuid4().hex[:12],
                    "prompt_sent": False,
                    "processing": False
                }
            )

            if session["processing"]:

                bot.reply_to(
                    message,
                    "⏳ Ana nazarin hotunanka yanzu. "
                    "Jira a kammala."
                )

                return

            if session["prompt_sent"]:

                session["photos"] = []

                session["prompt_sent"] = False

                session["token"] = uuid.uuid4().hex[:12]

            if len(session["photos"]) >= MOVIE_TEST_MAX_PHOTOS:

                bot.reply_to(
                    message,
                    f"⚠️ Iyakar hotuna ita ce "
                    f"{MOVIE_TEST_MAX_PHOTOS}."
                )

                return

            photo_id = message.photo[-1].file_id

            session["photos"].append(photo_id)

            old_timer = session.get("timer")

            if old_timer:
                old_timer.cancel()

            token = session["token"]

            timer = threading.Timer(
                MOVIE_TEST_ALBUM_WAIT,
                movie_test_ask_confirmation,
                args=(key, token)
            )

            timer.daemon = True

            session["timer"] = timer

            timer.start()

        movie_test_log(
            "MOVIE TEST PHOTO RECEIVED | user=%s | count=%s",
            message.from_user.id,
            len(session["photos"])
        )

    except Exception as error:

        movie_test_exception(
            "MOVIE TEST PHOTO RECEIVE ERROR"
        )

        movie_test_send_user(
            message.chat.id,
            "❌ <b>PHOTO RECEIVE ERROR</b>\n"
            + movie_test_safe(error, 1000)
        )


# ============================================================
# 6. YES / NO CONFIRMATION
# ============================================================

def movie_test_ask_confirmation(key, token):

    try:

        with MOVIE_TEST_LOCK:

            session = MOVIE_TEST_SESSIONS.get(key)

            if not session:
                return

            if session["token"] != token:
                return

            if not session["photos"]:
                return

            if session["prompt_sent"]:
                return

            session["prompt_sent"] = True

            photo_count = len(session["photos"])

        markup = types.InlineKeyboardMarkup(
            row_width=2
        )

        markup.add(

            types.InlineKeyboardButton(
                "✅ YES",
                callback_data=f"mvtest:yes:{token}"
            ),

            types.InlineKeyboardButton(
                "❌ NO",
                callback_data=f"mvtest:no:{token}"
            )
        )

        bot.send_message(
            key[0],
            (
                "🎬 <b>GANO SUNAN FIM</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"📸 Na karɓi hotuna: {photo_count}\n\n"
                "Kana son in bincika waɗannan hotunan "
                "domin gano sunan fim ɗin?"
            ),
            reply_markup=markup,
            parse_mode="HTML"
        )

    except Exception as error:

        movie_test_exception(
            "MOVIE TEST CONFIRMATION ERROR"
        )

        movie_test_send_user(
            key[0],
            "❌ <b>CONFIRMATION ERROR</b>\n"
            + movie_test_safe(error, 1000)
        )


# ============================================================
# 7. DOWNLOAD PHOTOS
# ============================================================

def movie_test_download_photos(
    photo_ids,
    user_chat_id,
    progress_id
):

    downloaded_parts = []

    total = len(photo_ids)

    movie_test_debug(
        user_chat_id,
        (
            "📥 <b>MATAKI 1 — SAUKAR DA HOTUNA</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Hotuna: {total}\n"
            "Ana sauke hotuna daga Telegram..."
        ),
        progress_id
    )

    for index, photo_id in enumerate(
        photo_ids,
        start=1
    ):

        started = time.time()

        try:

            file_info = bot.get_file(photo_id)

            image_bytes = bot.download_file(
                file_info.file_path
            )

            if not image_bytes:

                raise RuntimeError(
                    "Telegram ya dawo da empty image."
                )

            downloaded_parts.append({

                "inline_data": {

                    "mime_type": "image/jpeg",

                    "data": base64.b64encode(
                        image_bytes
                    ).decode("ascii")

                }

            })

            movie_test_log(
                "PHOTO DOWNLOADED | %s/%s | bytes=%s | seconds=%.2f",
                index,
                total,
                len(image_bytes),
                time.time() - started
            )

            movie_test_debug(
                user_chat_id,
                (
                    "📥 <b>ANA SAUKAR DA HOTUNA</b>\n"
                    f"An sauke: {index}/{total}\n"
                    f"Girman hoto na ƙarshe: "
                    f"{len(image_bytes):,} bytes\n"
                    "✅ Ana ci gaba..."
                ),
                progress_id
            )

        except Exception as error:

            movie_test_exception(
                "MOVIE TEST PHOTO DOWNLOAD FAILED"
            )

            raise RuntimeError(
                f"PHOTO {index}/{total} DOWNLOAD FAILED: "
                f"{str(error)[:700]}"
            )

    return downloaded_parts


# ============================================================
# 8. BUILD PROMPT
# ============================================================

def movie_test_build_prompt():

    return """
You are an expert movie identification assistant.

Look carefully at ALL the attached images together.

Your task is to identify the EXACT movie shown in the images.

IMPORTANT INSTRUCTIONS:

1. Examine the faces of the actors.
2. Compare the actors with known movie casts.
3. Examine costumes, scenes, locations and visual details.
4. Do not identify a movie only because its historical
   setting or costumes resemble another movie.
5. Do not guess a movie just because its genre is similar.
6. Search Google when necessary to verify the movie.
7. Search using visible actors, scene details and other
   visual clues.
8. Compare search results with the actual images.
9. Do not invent actor names or movie titles.
10. If you cannot identify the movie confidently,
    clearly say that you are uncertain.

Return the best identification you can.

Include:
- Movie title
- Release year, if verified
- Main actor, if verified
- Short explanation of the visual evidence

Remember:
The most important task is identifying the exact movie,
not simply describing the images.
"""


# ============================================================
# 9. EXTRACT GOOGLE SEARCH SOURCES
# ============================================================

def movie_test_extract_sources(data):

    sources = []

    try:

        candidates = data.get("candidates") or []

        if not candidates:
            return sources

        metadata = candidates[0].get(
            "groundingMetadata",
            {}
        )

        chunks = metadata.get(
            "groundingChunks",
            []
        )

        for chunk in chunks:

            web_data = chunk.get("web", {})

            title = web_data.get(
                "title",
                "Google Search source"
            )

            uri = web_data.get("uri", "")

            if uri:

                sources.append({
                    "title": title,
                    "uri": uri
                })

    except Exception as error:

        movie_test_log(
            "SOURCE EXTRACTION ERROR: %s",
            str(error)[:300]
        )

    return sources[:MOVIE_TEST_MAX_SOURCES]


# ============================================================
# 10. EXTRACT MODEL TEXT
# ============================================================

def movie_test_extract_answer(data):

    candidates = data.get("candidates") or []

    if not candidates:

        feedback = data.get(
            "promptFeedback",
            {}
        )

        reason = feedback.get(
            "blockReason",
            "NO_CANDIDATES"
        )

        raise RuntimeError(
            f"NO CANDIDATES | BLOCK REASON: {reason}"
        )

    candidate = candidates[0]

    content = candidate.get(
        "content",
        {}
    )

    response_parts = content.get(
        "parts",
        []
    )

    answer = "\n".join(
        part.get("text", "")
        for part in response_parts
        if part.get("text")
    ).strip()

    finish_reason = candidate.get(
        "finishReason",
        "UNKNOWN"
    )

    return answer, finish_reason


# ============================================================
# 11. RUN ONE GEMINI MODEL WITH GOOGLE SEARCH
# ============================================================

def movie_test_run_one_model(
    model_name,
    request_body
):

    url = (
        "https://generativelanguage.googleapis.com/"
        "v1beta/models/"
        + model_name
        + ":generateContent"
    )

    started = time.time()

    response = requests.post(

        url,

        headers={

            "Content-Type": "application/json",

            "x-goog-api-key": GEMINI_API_KEY

        },

        json=request_body,

        timeout=MOVIE_TEST_TIMEOUT

    )

    elapsed = time.time() - started

    if response.status_code != 200:

        try:

            error_data = response.json()

            error_obj = error_data.get(
                "error",
                {}
            )

            error_code = error_obj.get(
                "code",
                response.status_code
            )

            error_status = error_obj.get(
                "status",
                "UNKNOWN"
            )

            error_message = error_obj.get(
                "message",
                response.text[:1500]
            )

        except Exception:

            error_code = response.status_code

            error_status = "UNKNOWN"

            error_message = response.text[:1500]

        raise RuntimeError(
            f"HTTP {response.status_code}\n"
            f"API CODE: {error_code}\n"
            f"API STATUS: {error_status}\n"
            f"API MESSAGE: {error_message}"
        )

    try:

        data = response.json()

    except Exception as error:

        raise RuntimeError(
            "INVALID JSON RESPONSE: "
            + str(error)[:700]
        )

    answer, finish_reason = movie_test_extract_answer(
        data
    )

    sources = movie_test_extract_sources(
        data
    )

    return {
        "answer": answer,
        "finish_reason": finish_reason,
        "sources": sources,
        "elapsed": elapsed,
        "raw": data
    }


# ============================================================
# 12. FORMAT MODEL RESULT
# ============================================================

def movie_test_format_model_result(
    index,
    total,
    model_name,
    result
):

    answer = result["answer"]

    finish_reason = result["finish_reason"]

    elapsed = result["elapsed"]

    sources = result["sources"]

    if not answer:

        answer = (
            "[MODEL RETURNED NO TEXT]\n"
            "Duk da HTTP success, babu rubutun amsa."
        )

    answer = movie_test_safe(
        answer,
        MOVIE_TEST_MAX_ANSWER_LENGTH
    )

    source_text = ""

    if sources:

        source_lines = []

        for number, source in enumerate(
            sources,
            start=1
        ):

            title = movie_test_safe(
                source["title"],
                150
            )

            uri = html.escape(
                source["uri"],
                quote=True
            )

            source_lines.append(
                f'{number}. <a href="{uri}">{title}</a>'
            )

        source_text = (
            "\n\n🌐 <b>GOOGLE SEARCH SOURCES</b>\n"
            + "\n".join(source_lines)
        )

    else:

        source_text = (
            "\n\n🌐 Google Search sources: "
            "Babu grounding source da aka dawo da shi."
        )

    return (
        "🎬 <b>SAKAMAKON MODEL</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Model: <code>{movie_test_safe(model_name)}</code>\n"
        f"Ci gaba: {index}/{total}\n"
        f"Lokaci: {elapsed:.2f}s\n"
        f"Finish reason: <code>{movie_test_safe(finish_reason)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{answer}"
        f"{source_text}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✅ An kammala gwajin wannan model."
    )


# ============================================================
# 13. FORMAT MODEL ERROR
# ============================================================

def movie_test_format_model_error(
    index,
    total,
    model_name,
    error,
    elapsed
):

    error_text = movie_test_safe(
        error,
        MOVIE_TEST_MAX_ERROR_LENGTH
    )

    return (
        "❌ <b>GEMINI MODEL ERROR</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Model: <code>{movie_test_safe(model_name)}</code>\n"
        f"Ci gaba: {index}/{total}\n"
        f"Lokaci: {elapsed:.2f}s\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"<code>{error_text}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "➡️ Ba a dakatar da gwajin ba.\n"
        "➡️ Ana gwada model na gaba."
    )


# ============================================================
# 14. MAIN GEMINI TEST
# ALL MODELS MUST RUN
# ============================================================

def movie_test_call_gemini(
    photo_ids,
    user_chat_id,
    progress_id
):

    if not GEMINI_API_KEY:

        raise RuntimeError(
            "GEMINI_API_KEY bai kasance a Render Environment ba."
        )

    if not GEMINI_MODELS:

        raise RuntimeError(
            "GEMINI_MODELS list babu models."
        )

    # Remove duplicate names while preserving order.
    models = list(
        dict.fromkeys(GEMINI_MODELS)
    )

    total_models = len(models)

    if total_models == 0:

        raise RuntimeError(
            "Babu Gemini model da za a gwada."
        )

    # Download photos only once.
    image_parts = movie_test_download_photos(
        photo_ids,
        user_chat_id,
        progress_id
    )

    prompt = movie_test_build_prompt()

    parts = [
        {"text": prompt}
    ] + image_parts

    request_body = {

        "contents": [{

            "role": "user",

            "parts": parts

        }],

        "generationConfig": {

            "temperature": 0.2,

            "maxOutputTokens": 1200

        },

        # GOOGLE SEARCH GROUNDING
        "tools": [

            {

                "googleSearch": {}

            }

        ]

    }

    movie_test_debug(
        user_chat_id,
        (
            "🌐 <b>GOOGLE SEARCH GROUNDING AN KUNNA</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Hotuna: {len(photo_ids)}\n"
            f"Models: {total_models}\n"
            "Google Search: ENABLED\n\n"
            "Muhimmi: Zan gwada KOWANE model.\n"
            "Ko model ya samu amsa, zan ci gaba da sauran "
            "domin kwatantawa."
        ),
        progress_id
    )

    results = []

    failures = []

    overall_started = time.time()

    # --------------------------------------------------------
    # EVERY MODEL IS TESTED
    # SUCCESS DOES NOT STOP THE LOOP
    # --------------------------------------------------------

    for index, model_name in enumerate(
        models,
        start=1
    ):

        model_started = time.time()

        movie_test_debug(
            user_chat_id,
            (
                "🔄 <b>ANA GWADA MODEL</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Model: <code>{movie_test_safe(model_name)}</code>\n"
                f"Ci gaba: {index}/{total_models}\n"
                f"An samu amsa: {len(results)}\n"
                f"Errors: {len(failures)}\n\n"
                "🌐 Google Search: ON\n"
                "⏳ Ana jiran amsar model..."
            ),
            progress_id
        )

        try:

            result = movie_test_run_one_model(
                model_name,
                request_body
            )

            answer = result["answer"]

            if not answer:

                raise RuntimeError(
                    "MODEL RETURNED EMPTY ANSWER"
                )

            results.append({

                "model": model_name,

                "answer": answer,

                "elapsed": result["elapsed"],

                "finish_reason": result["finish_reason"],

                "sources": result["sources"]

            })

            movie_test_log(
                "MODEL SUCCESS | %s | %.2fs | sources=%s",
                model_name,
                result["elapsed"],
                len(result["sources"])
            )

            # SEND THIS MODEL'S RESULT TO THE USER.
            # DO NOT STOP THE NEXT MODEL.

            result_message = movie_test_format_model_result(
                index,
                total_models,
                model_name,
                result
            )

            movie_test_send_user(
                user_chat_id,
                result_message
            )

            movie_test_debug(
                user_chat_id,
                (
                    "✅ <b>MODEL YA KAMMALA</b>\n"
                    f"Model: <code>{movie_test_safe(model_name)}</code>\n"
                    f"Ci gaba: {index}/{total_models}\n"
                    f"Lokaci: {result['elapsed']:.2f}s\n"
                    f"Sources: {len(result['sources'])}\n\n"
                    "➡️ Ana ci gaba da model na gaba."
                ),
                progress_id
            )

        except Exception as error:

            elapsed = time.time() - model_started

            error_text = str(error)

            failures.append({

                "model": model_name,

                "error": error_text,

                "elapsed": elapsed

            })

            movie_test_exception(
                "MOVIE TEST MODEL FAILED | %s",
                model_name
            )

            movie_test_log(
                "MODEL ERROR | %s | %.2fs | %s",
                model_name,
                elapsed,
                error_text[:1000]
            )

            # SEND EVERY ERROR TO THE USER.
            # NEVER STOP THE MODEL LOOP.

            error_message = movie_test_format_model_error(
                index,
                total_models,
                model_name,
                error_text,
                elapsed
            )

            movie_test_send_user(
                user_chat_id,
                error_message
            )

            movie_test_debug(
                user_chat_id,
                (
                    "⚠️ <b>AN TSALLAKE MODEL</b>\n"
                    f"Model: <code>{movie_test_safe(model_name)}</code>\n"
                    f"Ci gaba: {index}/{total_models}\n"
                    f"Lokaci: {elapsed:.2f}s\n"
                    "An rubuta error zuwa Telegram.\n\n"
                    "➡️ Ana gwada model na gaba."
                ),
                progress_id
            )

            continue

    # --------------------------------------------------------
    # FINAL REPORT
    # --------------------------------------------------------

    total_time = time.time() - overall_started

    success_count = len(results)

    failure_count = len(failures)

    report = [

        "🏁 <b>AN KAMMALA GWADIN MODELS 14</b>",

        "━━━━━━━━━━━━━━━━━━━━",

        f"🤖 Jimillar models: {total_models}",

        f"✅ Models da suka dawo da amsa: {success_count}",

        f"❌ Models da suka samu error: {failure_count}",

        f"🌐 Google Search: ENABLED",

        f"⏱ Jimillar lokaci: {total_time:.1f}s",

        "━━━━━━━━━━━━━━━━━━━━"

    ]

    if results:

        report.append(
            "\n<b>MODELS DA SUKA SAMU AMSA</b>"
        )

        for number, item in enumerate(
            results,
            start=1
        ):

            report.append(
                f"{number}. <code>{movie_test_safe(item['model'])}</code>"
                f" — {item['elapsed']:.1f}s"
            )

    if failures:

        report.append(
            "\n<b>MODELS DA SUKA SAMU ERROR</b>"
        )

        for number, item in enumerate(
            failures,
            start=1
        ):

            error_summary = str(
                item["error"]
            ).replace("\n", " ")

            report.append(
                f"{number}. <code>{movie_test_safe(item['model'])}</code>"
                f"\n   {movie_test_safe(error_summary, 350)}"
            )

    if not results:

        report.append(
            "\n❌ Babu model da ya dawo da amsa."
        )

    report.append(
        "\n━━━━━━━━━━━━━━━━━━━━\n"
        "Gwajin ya kammala. "
        "An gwada duk models da ke cikin jerin."
    )

    movie_test_send_user(
        user_chat_id,
        "\n".join(report)
    )

    return {
        "results": results,
        "failures": failures,
        "total_models": total_models,
        "total_time": total_time
    }


# ============================================================
# 15. BACKGROUND TEST
# ============================================================

def movie_test_run(
    key,
    token,
    photo_ids,
    progress_id
):

    started = time.time()

    try:

        movie_test_debug(
            key[0],
            (
                "🚀 <b>AN FARA GWADIN FIM</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Hotuna: {len(photo_ids)}\n"
                f"Models: {len(GEMINI_MODELS)}\n"
                "Google Search: ON\n\n"
                "Zan aika maka sakamakon kowane model "
                "da duk wani error."
            ),
            progress_id
        )

        report = movie_test_call_gemini(
            photo_ids,
            key[0],
            progress_id
        )

        movie_test_log(
            "MOVIE TEST FINISHED | user=%s | success=%s | errors=%s | seconds=%.2f",
            key[1],
            len(report["results"]),
            len(report["failures"]),
            time.time() - started
        )

        movie_test_debug(
            key[0],
            (
                "✅ <b>GWADIN YA KAMMALA</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Models: {report['total_models']}\n"
                f"Successful: {len(report['results'])}\n"
                f"Errors: {len(report['failures'])}\n"
                f"Lokaci: {report['total_time']:.1f}s\n\n"
                "Duk sakamakon models an aika maka "
                "a Telegram."
            ),
            progress_id
        )

    except Exception as error:

        movie_test_exception(
            "MOVIE TEST FATAL ERROR | user=%s",
            key[1]
        )

        movie_test_send_user(
            key[0],
            (
                "🔴 <b>MOVIE TEST FATAL ERROR</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"{movie_test_safe(error, 3000)}\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Lokaci: {time.time() - started:.1f}s"
            )
        )

    finally:

        with MOVIE_TEST_LOCK:

            current = MOVIE_TEST_SESSIONS.get(key)

            if current and current["token"] == token:

                MOVIE_TEST_SESSIONS.pop(
                    key,
                    None
                )

        movie_test_log(
            "MOVIE TEST SESSION CLEANED | user=%s",
            key[1]
        )


# ============================================================
# 16. YES / NO CALLBACK
# ============================================================

@bot.callback_query_handler(
    func=lambda call: (
        call.data
        and call.data.startswith("mvtest:")
    )
)
def movie_test_callback(call):

    try:

        parts = call.data.split(":", 2)

        if len(parts) != 3:

            bot.answer_callback_query(
                call.id,
                "Button ɗin bai cika ba."
            )

            return

        _, action, token = parts

        key = (
            call.message.chat.id,
            call.from_user.id
        )

        with MOVIE_TEST_LOCK:

            session = MOVIE_TEST_SESSIONS.get(key)

            if not session or session["token"] != token:

                bot.answer_callback_query(
                    call.id,
                    "Wannan gwajin ya ƙare."
                )

                return

            if session["processing"]:

                bot.answer_callback_query(
                    call.id,
                    "Ana gudanar da gwajin yanzu."
                )

                return

            if action == "no":

                timer = session.get("timer")

                if timer:
                    timer.cancel()

                MOVIE_TEST_SESSIONS.pop(
                    key,
                    None
                )

                bot.answer_callback_query(
                    call.id,
                    "An soke gwajin."
                )

                bot.edit_message_text(
                    "❌ An soke gwajin gano fim.\n"
                    "Za ka iya sake aika hotuna.",
                    key[0],
                    call.message.message_id
                )

                return

            if action != "yes":

                bot.answer_callback_query(
                    call.id,
                    "Ba a gane zaɓin ba."
                )

                return

            photo_ids = list(
                session["photos"]
            )

            if not photo_ids:

                bot.answer_callback_query(
                    call.id,
                    "Babu hotuna."
                )

                return

            session["processing"] = True

            timer = session.get("timer")

            if timer:
                timer.cancel()

        bot.answer_callback_query(
            call.id,
            "An fara gwajin Gemini."
        )

        progress = movie_test_debug(
            key[0],
            (
                "✅ <b>AN TABBATAR</b>\n"
                "⏳ Ana fara gwajin Gemini..."
            ),
            call.message.message_id
        )

        threading.Thread(
            target=movie_test_run,
            args=(
                key,
                token,
                photo_ids,
                progress
            ),
            daemon=True,
            name="independent-movie-test"
        ).start()

    except Exception as error:

        movie_test_exception(
            "MOVIE TEST CALLBACK ERROR"
        )

        try:

            bot.answer_callback_query(
                call.id,
                "An samu kuskure."
            )

        except Exception:
            pass

        try:

            movie_test_send_user(
                call.message.chat.id,
                (
                    "❌ <b>CALLBACK ERROR</b>\n"
                    + movie_test_safe(error, 1000)
                )
            )

        except Exception:
            pass


# ============================================================
# END INDEPENDENT GEMINI PHOTO MOVIE TEST
# ============================================================



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

# Pyrogram engine state — initialize every shared object before startup.
# The Pyrogram engine uses its own lock so it cannot collide with Bot 2 startup.
_BOT2_PYRO_START_LOCK = threading.Lock()
_BOT2_PYRO_READY = threading.Event()
_BOT2_PYRO_THREAD = None
_BOT2_PYRO_CLIENT = None
_BOT2_PYRO_LOOP = None
_BOT2_PYRO_START_ERROR = None

BOT2_WORKER_COUNT = 3
BOT2_WORKER_IDS = [f"CLIP_BOT_2_{i}" for i in range(1, BOT2_WORKER_COUNT + 1)]
_BOT2_WORKER_LOCAL = threading.local()
_BOT2_TASK_LOCKS = {worker_id: threading.Lock() for worker_id in BOT2_WORKER_IDS}
_BOT2_MODEL_LOCKS = {model: threading.Lock() for model in GEMINI_MODELS} if "GEMINI_MODELS" in globals() else {}

_BOT2_CURRENT_TASK = None
_BOT2_CURRENT_USER_ID = None

def clip2_current_worker_id():
    return getattr(_BOT2_WORKER_LOCAL, "worker_id", BOT2_WORKER_IDS[0])

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


def clip2_gemini_admin_send(message):
    """Send Gemini diagnostics as a NEW message to the dedicated ADMIN_GEMI."""
    try:
        if not ADMIN_GEMI:
            BOT2_LOGGER.error("ADMIN_GEMI_NOT_CONFIGURED | %s", str(message)[:1500])
            return None
        return bot.send_message(
            int(ADMIN_GEMI),
            str(message)[:3900],
            parse_mode=None
        )
    except Exception:
        BOT2_LOGGER.exception("ADMIN_GEMI_SEND_FAILED")
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

def clip2_register_worker(worker_id=None):

    worker_id = worker_id or clip2_current_worker_id()

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
            worker_id,
            "Clip Movie Scanner Bot 2",
            json.dumps({
                "movie_identification": True,
                "one_task_at_a_time_per_worker": True,
                "one_request_per_model_at_a_time": True,
                "full_video_download": True,
                "max_video_size": BOT2_MAX_VIDEO_SIZE,
                "frames": BOT2_FRAME_COUNT,
                "gemini_fallback": True,
                "gemini_models": GEMINI_MODELS,
                "gemini_max_valid_answers": 3,
                "gemini_api_attempts_not_limited_to_three": True,
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
            clip2_current_worker_id()
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
                      'timed_out',
                      'review_1',
                      'review_2'
                  )
            """, (
                BOT2_LEASE_SECONDS,
                task_id,
                user_id,
                clip2_current_worker_id()
            ))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


def clip2_heartbeat_loop(task_id, user_id, worker_id=None):

    _BOT2_WORKER_LOCAL.worker_id = worker_id or clip2_current_worker_id()

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
              AND (
                    (
                        status IN ('pending', 'review_1')
                        AND (
                            payload->>'bot2_next_retry_at' IS NULL
                            OR (payload->>'bot2_next_retry_at')::timestamptz <= NOW()
                        )
                    )
                    OR (
                        status IN ('processing', 'downloading',
                                   'extracting_frames', 'identifying')
                        AND (lease_until IS NULL OR lease_until < NOW())
                    )
              )
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
            f"?? Task ID: <code>{task_id}</code>\n"
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
                attempts = attempts + CASE
                    WHEN status IN ('pending', 'review_1') THEN 1
                    ELSE 0
                END,
                payload = COALESCE(payload, '{}'::jsonb)
                          || jsonb_build_object('bot2_claim_origin', status),
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND task_type = %s
              AND (
                    status IN ('pending', 'review_1')
                    OR (
                        status IN ('processing', 'downloading',
                                   'extracting_frames', 'identifying')
                        AND (lease_until IS NULL OR lease_until < NOW())
                    )
              )
            RETURNING task_id
        """, (
            clip2_current_worker_id(),
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
            clip2_current_worker_id()
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
                  'timed_out',
                  'review_2'
              )
        """, (
            status,
            BOT2_LEASE_SECONDS,
            task_id,
            user_id,
            clip2_current_worker_id()
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
                  'timed_out',
                  'review_2'
              )
        """, (
            json.dumps(extra),
            BOT2_LEASE_SECONDS,
            task_id,
            user_id,
            clip2_current_worker_id()
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
            clip2_current_worker_id()
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
        """, (clip2_current_worker_id(),))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:
        conn.close()


def clip2_retry_delay_from_error(error, default_seconds=10):
    """Honor an API-provided Retry-After when present; otherwise retry after 10 seconds."""
    import re
    message = str(error or "")
    match = re.search(r"retry[_ -]?after\\s*[:=]\\s*([0-9]+(?:\\.[0-9]+)?)", message, re.I)
    if match:
        return max(10, int(float(match.group(1)) + 0.999))
    match = re.search(r"retryDelay[^0-9]*([0-9]+)s", message, re.I)
    if match:
        return max(10, int(match.group(1)))
    return max(10, int(default_seconds))


# ============================================================
# TECHNICAL RETRY — KEEP TASK OUT OF REVIEW
# ============================================================

def clip2_requeue_technical_failure(task_id, user_id, error, delay_seconds=10):
    """Requeue infrastructure/API failures without letting them decide Gemini review status."""
    delay_seconds = max(10, int(delay_seconds or 10))
    conn = clip2_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE tasks
            SET status = 'pending',
                assigned_worker = NULL,
                error_message = %s,
                payload = COALESCE(payload, '{}'::jsonb)
                          || jsonb_build_object(
                              'bot2_next_retry_at',
                              (NOW() + (%s * INTERVAL '1 second'))::text,
                              'bot2_last_technical_error', %s,
                              'bot2_technical_retry_count',
                              COALESCE(
                                  NULLIF(payload->>'bot2_technical_retry_count', '')::integer,
                                  0
                              ) + 1
                          ),
                lease_until = NULL,
                heartbeat_at = NULL,
                updated_at = NOW()
            WHERE task_id = %s
              AND user_id = %s
              AND assigned_worker = %s
              AND status NOT IN ('completed', 'delivered', 'failed', 'timed_out',
                                 'review_1', 'review_2')
            RETURNING payload->>'bot2_next_retry_at',
                      payload->>'bot2_technical_retry_count'
        """, (
            str(error)[:2000],
            delay_seconds,
            str(error)[:2000],
            task_id,
            user_id,
            clip2_current_worker_id()
        ))
        result = cur.fetchone()
        if result:
            cur.execute("""
                UPDATE workers
                SET current_task_id = NULL,
                    last_heartbeat = NOW(),
                    updated_at = NOW()
                WHERE worker_id = %s
            """, (clip2_current_worker_id(),))
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# MOVE TASK TO REVIEW 1 / REVIEW 2
# FIRST FAILURE -> REVIEW 1 (RETRY)
# SECOND FAILURE -> REVIEW 2 (PARK FOR FUTURE REVIEW SYSTEM)
# ============================================================

def clip2_mark_for_review(task_id, user_id, error):

    conn = clip2_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET status = CASE
                    WHEN attempts >= 2 THEN 'review_2'
                    ELSE 'review_1'
                END,
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
                  'timed_out',
                  'review_2'
              )
            RETURNING status, attempts
        """, (
            str(error)[:2000],
            task_id,
            user_id,
            clip2_current_worker_id()
        ))

        review_result = cur.fetchone()

        if review_result:
            review_status, attempt_count = review_result

            cur.execute("""
                UPDATE workers
                SET current_task_id = NULL,
                    last_heartbeat = NOW(),
                    updated_at = NOW()
                WHERE worker_id = %s
            """, (clip2_current_worker_id(),))

            BOT2_LOGGER.warning(
                "TASK MOVED TO %s task=%s attempts=%s",
                review_status,
                task_id,
                attempt_count
            )

        conn.commit()
        return review_result

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


def clip2_gemini_parse_candidate(answer):
    """Read partial fields and a self-reported confidence without inventing missing values."""
    text = str(answer or "").strip()
    aliases = {
        "sunan fim": "title", "sunan fim din": "title",
        "movie": "title", "movie name": "title", "title": "title",
        "shekara": "year", "year": "year", "movie year": "year",
        "jarumi": "actor", "jarumai": "actor", "actor": "actor",
        "main actor": "actor", "lead actor": "actor",
        "tabbaci": "confidence", "tabbaci (%)": "confidence",
        "confidence": "confidence", "confidence level": "confidence",
        "fim din yana wujeni": "movie_exists", "movie exists": "movie_exists",
        "dalilin sunan fim": "title_evidence", "title evidence": "title_evidence",
        "dalilin shekara": "year_evidence", "year evidence": "year_evidence",
        "dalilin jarumi": "actor_evidence", "actor evidence": "actor_evidence"
    }
    fields = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower().strip("*# ")
        value = value.strip().strip("*# `")
        field = aliases.get(key)
        if field and value:
            fields[field] = value

    title = fields.get("title", "").strip()
    year_match = re.search(r"\b(19|20)\d{2}\b", fields.get("year", ""))
    year = year_match.group(0) if year_match else ""
    actor = fields.get("actor", "").strip()
    confidence_match = re.search(r"(\d{1,3})\s*%?", fields.get("confidence", ""))
    confidence = min(100, int(confidence_match.group(1))) if confidence_match else 0

    invalid_titles = {"unknown", "unknown movie", "ba a sani ba",
                      "ba a tabbatar ba", "not sure", "uncertain", "n/a"}
    if (title.lower() in invalid_titles or not re.search(r"[a-zA-ZÀ-ÿ]", title)
            or re.fullmatch(r"[\W\d_]+", title) or len(title) > 160):
        title = ""
    if actor.lower() in {"unknown", "ba a sani ba", "ba a tabbatar ba",
                         "n/a", "none", "uncertain"} or len(actor) > 120:
        actor = ""

    exists_value = fields.get("movie_exists", "").strip().lower()
    movie_exists = exists_value in {"eh", "yes", "e", "true", "confirmed", "tabbatacce"}
    evidence_unknown = {"", "ba a tabbatar ba", "unknown", "none", "n/a", "babu", "uncertain"}
    title_evidence = fields.get("title_evidence", "").strip()
    year_evidence = fields.get("year_evidence", "").strip()
    actor_evidence = fields.get("actor_evidence", "").strip()
    evidence_complete = all(
        value.lower() not in evidence_unknown and len(value) >= 12
        for value in (title_evidence, year_evidence, actor_evidence)
    )
    return {
        "title": title, "year": year, "actor": actor,
        "confidence": confidence, "movie_exists": movie_exists,
        "title_evidence": title_evidence,
        "year_evidence": year_evidence,
        "actor_evidence": actor_evidence,
        "evidence_complete": evidence_complete,
        "raw": text
    }



def clip2_gemini_candidate_verified(candidate):
    """Basic completeness check; confidence/evidence are scored separately."""
    return bool(
        candidate.get("title")
        and candidate.get("year")
        and candidate.get("actor")
    )


def clip2_gemini_title_key(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def clip2_gemini_candidate_score(candidate):
    """Rank only the three candidates actually inspected for this task."""
    score = 0
    if candidate.get("title"):
        score += 20
    if candidate.get("year"):
        score += 15
    if candidate.get("actor"):
        score += 15
    if candidate.get("movie_exists"):
        score += 10
    if candidate.get("evidence_complete"):
        score += 10
    score += max(0, min(100, int(candidate.get("confidence", 0)))) * 0.30
    return score


def clip2_gemini_identify(frame_paths, task_id, user_name, user_id, monitor_id):
    """Try models until 3 VALID answers are received; API errors do not count.

    Model 1 may be accepted on its own only when it is complete and strongly
    confident. Otherwise obtain a second valid answer. Matching movie titles
    are merged field-by-field and accepted; different titles require a third
    valid answer. The third valid answer is the tie-breaker, not the third API
    attempt. All failed/busy/incomplete model attempts are skipped and logged.
    """
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY_MISSING")
    if len(frame_paths) != BOT2_FRAME_COUNT:
        raise RuntimeError("GEMINI_REQUIRES_10_FRAMES")

    # ORIGINAL PROFIM PROMPT KEPT INTACT.
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

    # Existing verification instructions retained unchanged.
    verification_rules = """
KARIN UMARNIN TANTANCEWA:
- Kada ka ɗauki lamba kaɗai, gajeren rubutu, ko rubutun da ke jikin bidiyo a matsayin sunan fim.
- Ka tantance ko sunan fim, shekararsa da jarumin sun dace da hotuna.
- Ka bayar da dalilin gano sunan fim, shekarar da jarumin a taƙaice.
- Kada ka ƙirƙiri hujja ko ka nuna tabbaci fiye da yadda kake da shi.
- Muhimmi: Idan ka ga rubutu kamar "AI generated", "AI-generated" ko makamancinsa a kan bidiyon, KA YI IGNORE ɗINSA GABA ƊAYA. Kada ka ɗauka cewa bidiyon AI ne saboda wannan rubutu. Wata manhajar gyaran bidiyo/cutting app na iya manna wannan alama lokacin yanke ko haɗa bidiyo. Wannan rubutun ba hujja ba ne cewa bidiyon ba na gaske ba ne. Ka tantance hotunan da abin da suke nunawa kawai.
- Kada ka ƙi bidiyon saboda wannan alamar.
- Amsa da waɗannan layukan:
SUNAN FIM: ...
SHEKARA: ...
JARUMI: ...
FIM DIN YANA WUJENI: EH ko BA A TABBATAR BA
DALILIN SUNAN FIM: ...
DALILIN SHEKARA: ...
DALILIN JARUMI: ...
TABBACI: ...%
"""
    parts = [{"text": prompt + "\n\n" + verification_rules}]
    for frame_path in frame_paths:
        with open(frame_path, "rb") as image_file:
            raw = image_file.read()
        if not raw:
            raise RuntimeError("GEMINI_INPUT_FRAME_EMPTY")
        parts.append({"inline_data": {
            "mime_type": "image/jpeg",
            "data": base64.b64encode(raw).decode("ascii")
        }})

    body = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 650}
    }
    failures = []
    candidates = []                 # Only valid, parseable model answers count.
    attempted_models = 0            # API attempts; deliberately NOT capped at 3.
    model_locks = _BOT2_MODEL_LOCKS
    if not model_locks:
        model_locks.update({name: threading.Lock() for name in GEMINI_MODELS})

    def admin_gem(stage, details):
        # Dedicated Gemini admin receives both normal progress and all errors.
        clip2_gemini_admin_send(
            f"GEMINI DEBUG | {stage}\nTask: {task_id}\nUser: {user_id}\n{details}"
        )

    def title_key(value):
        return clip2_gemini_title_key(value)

    def titles_match(a, b):
        import difflib
        ka, kb = title_key(a), title_key(b)
        if not ka or not kb:
            return False
        return ka == kb or difflib.SequenceMatcher(None, ka, kb).ratio() >= 0.84

    def merge_matching_answers(items):
        # Same film title: fill missing year/actor using the best available
        # non-empty field from the agreeing models; never mix different titles.
        ranked = sorted(items, key=clip2_gemini_candidate_score, reverse=True)
        merged = dict(ranked[0])
        for field in ("year", "actor"):
            if not merged.get(field):
                for item in ranked[1:]:
                    if item.get(field):
                        merged[field] = item[field]
                        break
        merged["model"] = "+".join(item["model"] for item in items)
        return merged

    admin_gem(
        "START",
        f"Frames=10 | Models configured={len(GEMINI_MODELS)} | "
        "Maximum VALID answers=3 (not maximum API attempts). "
        "Any error/empty/incomplete response is skipped and the next model is tried."
    )

    for model_name in GEMINI_MODELS:
        if len(candidates) >= 3:
            break
        model_lock = model_locks.setdefault(model_name, threading.Lock())
        if not model_lock.acquire(blocking=False):
            failure = f"{model_name}: MODEL_BUSY_SKIPPED"
            failures.append(failure)
            admin_gem("MODEL SKIPPED - BUSY", failure)
            continue

        attempted_models += 1
        admin_gem("MODEL ATTEMPT", f"Attempt #{attempted_models}: {model_name}; valid answers so far={len(candidates)}/3")
        try:
            request_body = json.loads(json.dumps(body))
            url = (
                "https://generativelanguage.googleapis.com/v1beta/models/"
                + model_name + ":generateContent"
            )
            started = time.time()
            try:
                response = requests.post(
                    url,
                    headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
                    json=request_body,
                    timeout=(20, 240)
                )
                elapsed = time.time() - started
                if response.status_code != 200:
                    try:
                        err_data = response.json()
                        detail = err_data.get("error", {}).get("message", response.text[:900])
                    except Exception:
                        detail = response.text[:900]
                    category = clip2_gemini_error_category(response.status_code, detail)
                    retry_after_header = response.headers.get("Retry-After") or response.headers.get("retry-after")
                    retry_after_note = (
                        f" Retry-After: {retry_after_header}"
                        if retry_after_header else ""
                    )
                    failure = f"{model_name}: HTTP {response.status_code} [{category}]: {detail}{retry_after_note}"
                    failures.append(failure)
                    admin_gem("MODEL ERROR - SKIPPING", f"Model: {model_name}\nHTTP: {response.status_code}\nCategory: {category}\n{detail[:1300]}\nValid answers do not increase.")
                    continue

                data = response.json()
                response_candidates = data.get("candidates") or []
                if not response_candidates:
                    reason = data.get("promptFeedback", {}).get("blockReason", "NO_CANDIDATES")
                    failure = f"{model_name}: {reason}"
                    failures.append(failure)
                    admin_gem("MODEL NO CANDIDATE - SKIPPING", failure)
                    continue

                candidate_obj = response_candidates[0]
                finish_reason = str(candidate_obj.get("finishReason", "UNKNOWN")).upper()
                if finish_reason in ("SAFETY", "RECITATION", "BLOCKLIST"):
                    failure = f"{model_name}: {finish_reason}"
                    failures.append(failure)
                    admin_gem("MODEL BLOCKED - SKIPPING", failure)
                    continue

                response_parts = candidate_obj.get("content", {}).get("parts", [])
                answer = "\n".join(part.get("text", "") for part in response_parts if part.get("text")).strip()
                if not answer:
                    failure = f"{model_name}: EMPTY_RESPONSE"
                    failures.append(failure)
                    admin_gem("MODEL EMPTY ANSWER - SKIPPING", failure)
                    continue

                parsed = clip2_gemini_parse_candidate(answer)
                # A valid candidate must identify a movie title. Year/actor may
                # be missing; the next agreeing model can fill those fields.
                if not parsed.get("title"):
                    failure = f"{model_name}: INVALID_OR_MISSING_MOVIE_TITLE"
                    failures.append(failure)
                    admin_gem("MODEL INVALID ANSWER - SKIPPING", f"{failure}\nRaw: {answer[:1100]}")
                    continue

                parsed["model"] = model_name
                parsed["elapsed"] = elapsed
                candidates.append(parsed)
                answer_no = len(candidates)
                admin_gem(
                    f"VALID ANSWER {answer_no}/3",
                    f"Model: {model_name}\nTitle: {parsed['title']}\nYear: {parsed.get('year') or 'MISSING'}\n"
                    f"Actor: {parsed.get('actor') or 'MISSING'}\nConfidence: {parsed.get('confidence', 0)}%\n"
                    f"Time: {elapsed:.1f}s\nRaw: {answer[:900]}"
                )

                # Model 1 can finish immediately only when its answer is complete
                # and meets the existing strong-confidence threshold.
                if answer_no == 1:
                    complete = bool(parsed.get("year") and parsed.get("actor"))
                    if complete and parsed.get("confidence", 0) >= 85:
                        admin_gem("MODEL 1 ACCEPTED", "Model 1 has a complete answer and confidence >=85%. No second model is needed.")
                        return parsed["title"], parsed["year"], parsed["actor"], model_name
                    admin_gem("MODEL 1 NEEDS CONFIRMATION", "Answer is not yet strong/complete. Continue through remaining models until another VALID answer is received.")
                    continue

                # Model 2 valid answer: if the movie title matches Model 1,
                # combine missing fields and stop; otherwise seek a third VALID answer.
                if answer_no == 2:
                    first, second = candidates[0], candidates[1]
                    if titles_match(first["title"], second["title"]):
                        winner = merge_matching_answers(candidates[:2])
                        admin_gem(
                            "MODEL 1 + MODEL 2 SAME MOVIE - ACCEPTED",
                            f"Titles match: {first['title']} / {second['title']}\n"
                            f"Merged result: {winner['title']} | {winner.get('year') or 'Ba a tabbatar ba'} | {winner.get('actor') or 'Ba a tabbatar ba'}\n"
                            "No third model required."
                        )
                        return winner["title"], winner.get("year", ""), winner.get("actor", ""), winner["model"]
                    admin_gem(
                        "MODEL 1 + MODEL 2 DIFFERENT MOVIES",
                        f"Model 1: {first['title']} | {first.get('year') or '?'} | {first.get('actor') or '?'}\n"
                        f"Model 2: {second['title']} | {second.get('year') or '?'} | {second.get('actor') or '?'}\n"
                        "Titles differ. Continue trying models until a THIRD VALID answer is obtained; errors do not count."
                    )
                    continue

                # Third VALID answer is the tie-breaker. A matching title gives
                # a 2-of-3 agreement; if all differ, rank only valid answers.
                if answer_no == 3:
                    first, second, third = candidates
                    if titles_match(first["title"], third["title"]):
                        agreeing = [first, third]
                        decision = "Model 1 + Model 3 title agreement"
                    elif titles_match(second["title"], third["title"]):
                        agreeing = [second, third]
                        decision = "Model 2 + Model 3 title agreement"
                    else:
                        # No two titles agree: Model 3 is the final independent
                        # adjudicator, rather than counting failed models or
                        # letting API errors influence the decision.
                        agreeing = [third]
                        decision = "All three valid titles differ; Model 3 is the final adjudicator"
                    winner = merge_matching_answers(agreeing) if len(agreeing) >= 2 and titles_match(agreeing[0]["title"], agreeing[1]["title"]) else agreeing[0]
                    report = "\n".join(
                        f"{i+1}. {item['model']}: {item['title']} | {item.get('year') or '?'} | "
                        f"{item.get('actor') or '?'} | confidence={item.get('confidence', 0)}%"
                        for i, item in enumerate(candidates)
                    )
                    admin_gem("MODEL 3 FINAL DECISION", f"{decision}\nChosen: {winner['title']} | {winner.get('year') or '?'} | {winner.get('actor') or '?'}\n\n{report}")
                    return winner["title"], winner.get("year", ""), winner.get("actor", ""), winner["model"]

            except requests.exceptions.RequestException as error:
                category = clip2_gemini_error_category(error=error)
                failure = f"{model_name}: {category}: {error}"
                failures.append(failure)
                admin_gem("NETWORK/API ERROR - SKIPPING", f"Model: {model_name}\n{category}: {str(error)[:1200]}\nValid answers do not increase.")
            except Exception as error:
                category = clip2_gemini_error_category(error=error)
                failure = f"{model_name}: {category}: {error}"
                failures.append(failure)
                admin_gem("MODEL ERROR - SKIPPING", f"Model: {model_name}\n{category}: {str(error)[:1200]}\nValid answers do not increase.")
        finally:
            model_lock.release()

    # If the list was exhausted before a third valid answer could be obtained,
    # do not pretend an error response is a candidate or invent consensus.
    if len(candidates) == 2 and titles_match(candidates[0]["title"], candidates[1]["title"]):
        winner = merge_matching_answers(candidates)
        admin_gem("MODEL LIST EXHAUSTED - TWO ANSWERS AGREE", f"All configured models were tried. Both valid answers identify {winner['title']}; merged the available fields.")
        return winner["title"], winner.get("year", ""), winner.get("actor", ""), winner["model"]

    final_failures = "\n".join(failures[-14:]) or "No valid Gemini response was received."
    admin_gem(
        "MODEL LIST EXHAUSTED - NO SAFE FINAL ANSWER",
        f"Models configured: {len(GEMINI_MODELS)}\nAPI attempts: {attempted_models}\n"
        f"Valid answers: {len(candidates)}/3\nFailures/skips:\n{final_failures[:2300]}"
    )
    raise RuntimeError("ALL_GEMINI_MODELS_FAILED_OR_NO_VALID_CONSENSUS:\n" + final_failures)


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
    monitor_id=None,
    review_status="review_1"
):

    category = clip2_classify_error(error)

    text = (
        f"🟠 BOT 2 — TASK MOVED TO {str(review_status).upper()}\n"
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

    # Mirror every task failure to the dedicated Gemini admin as requested.
    clip2_gemini_admin_send(
        "BOT 2 TASK FAILURE / REVIEW\n"
        f"Task: {task_id}\nUser: {user_id}\nStage: {stage}\n"
        f"Category: {category}\nStatus: {review_status}\n"
        f"Error: {str(error)[:1800]}"
    )


# ============================================================
# TASK ARTIFACT STORAGE — ONE ISOLATED DIRECTORY PER TASK
# Render Free filesystem is ephemeral; this cache is best-effort only.
# After redeploy, missing files are recovered from the original Telegram message.
BOT2_REVIEW_CACHE_DIR = os.getenv(
    "BOT2_REVIEW_CACHE_DIR",
    os.path.join(tempfile.gettempdir(), "bot2_task_artifacts")
).strip()


def clip2_review_frames_dir(task_id):
    safe_task_id = re.sub(r"[^a-zA-Z0-9_-]", "_", str(task_id))
    return os.path.join(BOT2_REVIEW_CACHE_DIR, safe_task_id)


def clip2_task_video_path(task_id):
    return os.path.join(clip2_review_frames_dir(task_id), "source_video.mp4")


def clip2_get_cached_review_frames(task_id):
    frames_dir = clip2_review_frames_dir(task_id)
    frames = [
        os.path.join(frames_dir, f"frame_{i:02d}.jpg")
        for i in range(1, BOT2_FRAME_COUNT + 1)
    ]
    if os.path.isdir(frames_dir) and all(
        os.path.isfile(path) and os.path.getsize(path) > 0 for path in frames
    ):
        return frames

    # Remove incomplete frame files only; preserve a downloaded source video
    # so a restarted task can regenerate all 10 frames without downloading again.
    for path in frames:
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass
    return []


def clip2_get_cached_video(task_id):
    path = clip2_task_video_path(task_id)
    try:
        if (
            os.path.isfile(path)
            and 0 < os.path.getsize(path) <= BOT2_MAX_VIDEO_SIZE
        ):
            return path
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
    return None


def clip2_delete_review_frames(task_id):
    # Historical function name retained; this now removes every artifact
    # (video + frames) belonging only to this task.
    shutil.rmtree(clip2_review_frames_dir(task_id), ignore_errors=True)


# PROCESS ONE TASK
# ============================================================

def clip2_process_one_task(
    task_id,
    user_id,
    raw_payload,
    pending_monitor_id=None,
    worker_id=None
):

    global _BOT2_CURRENT_TASK
    global _BOT2_CURRENT_USER_ID

    task_id = str(task_id)
    user_id = int(user_id)

    payload = clip2_json(raw_payload)
    worker_id = worker_id or clip2_current_worker_id()
    _BOT2_WORKER_LOCAL.worker_id = worker_id
    task_lock = _BOT2_TASK_LOCKS[worker_id]

    if not task_lock.acquire(blocking=False):
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
            args=(task_id, user_id, worker_id),
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
        # RESUME CHECKPOINTS:
        # 1) Reuse all 10 valid saved frames.
        # 2) Otherwise reuse a complete saved video and regenerate frames.
        # 3) If both are missing, fetch the exact Telegram source message and download.
        frames = clip2_get_cached_review_frames(task_id)
        frames_dir = clip2_review_frames_dir(task_id)
        os.makedirs(frames_dir, exist_ok=True)

        if frames:
            stage = "RESUME — SAVED FRAMES"
            clip2_admin_debug(
                task_id, user_name, user_id,
                "RESUME FOUND 10 SAVED FRAMES",
                "♻️ An samo hotuna 10 na wannan task. Ba a sake sauke video ba.",
                monitor_id
            )
        else:
            video_path = clip2_get_cached_video(task_id)

            if video_path:
                stage = "RESUME — SAVED VIDEO"
                clip2_admin_debug(
                    task_id, user_name, user_id,
                    "RESUME FOUND DOWNLOADED VIDEO",
                    (
                        "♻️ An samo cikakken video a local task cache.\\n"
                        "⏭ Za a ci gaba daga frame extraction; ba za a sake download ba."
                    ),
                    monitor_id
                )
            else:
                # Locate and verify the original Telegram group message.
                stage = "TELEGRAM MESSAGE LOOKUP"
                pyro_message = clip2_get_group_message(chat_id, message_id)
                if not pyro_message:
                    raise RuntimeError("TELEGRAM_MESSAGE_NOT_FOUND")

                stage = "VIDEO / FILE ID VERIFICATION"
                media = clip2_verify_video(
                    pyro_message, chat_id, message_id, payload
                )
                video_size = int(media.file_size or 0)
                file_id = media.file_id
                file_unique_id = media.file_unique_id

                clip2_save_payload(
                    task_id, user_id,
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

                stage = "VIDEO DOWNLOAD"
                clip2_set_status(task_id, user_id, "downloading")

                # Download to a temporary suffix, then atomically rename.
                # A crash during download leaves no falsely-complete video.
                partial_video_path = clip2_task_video_path(task_id) + ".part"
                try:
                    if os.path.exists(partial_video_path):
                        os.remove(partial_video_path)
                except OSError:
                    pass

                downloaded_path, downloaded_size = clip2_download_video(
                    pyro_message,
                    partial_video_path,
                    task_id,
                    user_name,
                    user_id,
                    monitor_id
                )
                video_path = clip2_task_video_path(task_id)
                os.replace(downloaded_path, video_path)

                clip2_save_payload(
                    task_id, user_id,
                    {
                        "download_completed": True,
                        "downloaded_bytes": downloaded_size,
                        "downloaded_at": datetime.now(timezone.utc).isoformat(),
                        "downloaded_video_path": video_path,
                        "bot2_artifacts_task_id": task_id,
                        "bot2_artifacts_user_id": user_id
                    }
                )

            # Recheck exact task/user metadata before using persisted artifacts.
            if int(payload.get("bot2_artifacts_user_id", user_id)) != user_id:
                raise RuntimeError("PERSISTED_VIDEO_USER_ID_MISMATCH")

            stage = "FRAME EXTRACTION"
            clip2_set_status(task_id, user_id, "extracting_frames")
            frames = clip2_extract_frames(
                video_path,
                frames_dir,
                task_id,
                user_name,
                user_id,
                monitor_id
            )

            clip2_save_payload(
                task_id, user_id,
                {
                    "frames_extracted": len(frames),
                    "frames_extraction_completed": True,
                    "review_frames_cached": True,
                    "review_frames_dir": frames_dir,
                    "bot2_artifacts_task_id": task_id,
                    "bot2_artifacts_user_id": user_id
                }
            )

        # 8. SEND FRAMES TO ADMIN
        # ----------------------------------------------------

        stage = "ADMIN FRAME PREVIEW"

        try:
            clip2_send_frames_to_admin(
                frames,
                task_id,
                user_name,
                user_id,
                monitor_id
            )
        except Exception as preview_error:
            # Admin preview is optional: report the Telegram error, then
            # continue to Gemini without retrying or stopping this task.
            BOT2_LOGGER.exception(
                "ADMIN FRAME PREVIEW FAILED; SKIPPING PREVIEW task=%s user=%s",
                task_id,
                user_id
            )
            preview_notice = (
                "⚠️ BOT 2 — ADMIN FRAME PREVIEW FAILED; SKIPPED\\n"
                "━━━━━━━━━━━━━━━━━━━━\\n"
                f"👤 User: {user_name}\\n"
                f"🆔 User ID: {user_id}\\n"
                f"🧾 Task ID: {task_id}\\n"
                f"📍 Stage: ADMIN FRAME PREVIEW\\n"
                f"🔍 Error category: {clip2_classify_error(preview_error)}\\n"
                f"❌ Error: {str(preview_error)[:1500]}\\n"
                "➡️ Ana ci gaba da Gemini identification.\\n"
                "━━━━━━━━━━━━━━━━━━━━"
            )
            if monitor_id:
                clip2_admin_edit(monitor_id, preview_notice)
            else:
                clip2_admin_send(preview_notice)
            clip2_gemini_admin_send(
                "BOT 2 ADMIN FRAME PREVIEW FAILED — SKIPPED\\n"
                f"Task: {task_id}\\nUser: {user_id}\\n"
                f"Category: {clip2_classify_error(preview_error)}\\n"
                f"Error: {str(preview_error)[:1500]}\\n"
                "Gemini identification will continue."
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

        # Completed task: immediately free the cached JPEG frames from Render.
        clip2_delete_review_frames(task_id)

    except Exception as error:

        BOT2_LOGGER.exception(
            "BOT2 TASK FAILED task=%s user=%s stage=%s",
            task_id,
            user_id,
            stage
        )

        retry_result = None
        try:
            # Download, Telegram, FFmpeg, preview, database and Gemini API
            # transport failures are technical failures, not movie decisions.
            # They return to pending and never enter review_1/review_2 here.
            retry_result = clip2_requeue_technical_failure(
                task_id,
                user_id,
                str(error),
                delay_seconds=clip2_retry_delay_from_error(error, 10)
            )
        except Exception:
            BOT2_LOGGER.exception("FAILED TO REQUEUE TECHNICAL TASK FAILURE")

        retry_at = retry_result[0] if retry_result else "not requeued (task state changed)"
        technical_retry_count = retry_result[1] if retry_result else "unknown"
        clip2_admin_report = (
            "🟡 BOT 2 — TECHNICAL FAILURE; RETRY SCHEDULED\\n"
            "━━━━━━━━━━━━━━━━━━━━\\n"
            f"👤 User: {user_name}\\n"
            f"🏷 Username: {user_tag}\\n"
            f"🆔 User ID: <code>{user_id}</code>\\n"
            f"🧾 Task ID: <code>{task_id}</code>\\n\\n"
            f"📍 Last stage: {stage}\\n"
            f"🔍 Error category: {clip2_classify_error(error)}\\n"
            f"⏳ Retry at: {retry_at}\\n"
            f"🔁 Technical retries: {technical_retry_count}\\n\\n"
            f"❌ Error: {str(error)[:1500]}\\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        if monitor_id:
            clip2_admin_edit(monitor_id, clip2_admin_report)
        else:
            clip2_admin_send(clip2_admin_report)
        clip2_gemini_admin_send(
            "BOT 2 TECHNICAL FAILURE — TASK REQUEUED\\n"
            f"Task: {task_id}\\nUser: {user_id}\\nStage: {stage}\\n"
            f"Category: {clip2_classify_error(error)}\\n"
            f"Retry at: {retry_at}\\nError: {str(error)[:1500]}"
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

        task_lock.release()


# ============================================================
# CONTINUOUS DATABASE POLLER
# CHECK PENDING EVERY 3 SECONDS
# ADMIN GETS PENDING USER NAME
# ============================================================

def clip2_recover_interrupted_tasks():
    """After acquiring the singleton DB lock, make Bot 2's abandoned work claimable."""
    conn = clip2_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE tasks
            SET status = CASE
                    WHEN payload->>'bot2_claim_origin' = 'review_1'
                         OR status = 'review_1' THEN 'review_1'
                    ELSE 'pending'
                END,
                assigned_worker = NULL,
                lease_until = NULL,
                heartbeat_at = NULL,
                updated_at = NOW()
            WHERE task_type = %s
              AND status IN (
                  'processing', 'downloading',
                  'extracting_frames', 'identifying'
              )
        """, (CLIP_TASK_TYPE,))
        recovered = cur.rowcount
        conn.commit()
        if recovered:
            clip2_admin_send(
                f"♻️ BOT 2 RECOVERY: an dawo da interrupted tasks {recovered}. "
                "Za a duba local saved frames/video; idan sun bace, za a sake download daga Telegram."
            )
        return recovered
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bot2_clip_database_poller(worker_id):
    _BOT2_WORKER_LOCAL.worker_id = worker_id
    BOT2_LOGGER.info("BOT2 WORKER POLLER STARTED worker=%s", worker_id)
    clip2_admin_send(f"🟢 BOT 2 WORKER ONLINE\\nWorker: {worker_id}")

    while not _BOT2_STOP.is_set():
        try:
            if _BOT2_TASK_LOCKS[worker_id].locked():
                _BOT2_STOP.wait(0.5)
                continue
            row = clip2_get_next_task()
            if not row:
                _BOT2_STOP.wait(BOT2_POLL_SECONDS)
                continue
            task_id, user_id, payload = row
            payload = clip2_json(payload)
            pending_monitor_id = clip2_pending_admin_notice(task_id, user_id, payload)
            BOT2_LOGGER.info("PENDING TASK FOUND worker=%s task=%s user=%s", worker_id, task_id, user_id)
            clip2_process_one_task(
                task_id, user_id, payload, pending_monitor_id,
                worker_id=worker_id
            )
        except Exception as error:
            BOT2_LOGGER.exception("BOT2 DATABASE POLLER ERROR worker=%s", worker_id)
            poller_error_text = (
                f"🔴 BOT 2 DATABASE POLLER ERROR\\nWorker: {worker_id}\\n"
                f"Category: {clip2_classify_error(error)}\\nError: {str(error)[:1500]}"
            )
            clip2_admin_send(poller_error_text)
            clip2_gemini_admin_send(poller_error_text)
            _BOT2_STOP.wait(3)


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
            raise RuntimeError("ADMIN_ID_NOT_CONFIGURED")
        if not ADMIN_GEMI:
            raise RuntimeError("ADMIN_GEMI_NOT_CONFIGURED")

        # Render Free has ephemeral local storage. Do not require a paid
        # Persistent Disk or block startup when no disk is mounted.
        # The local cache accelerates retries while this instance is alive;
        # after redeploy, recovery checks the cache and re-downloads the
        # original Telegram video when the cached video/frames are missing.
        os.makedirs(BOT2_REVIEW_CACHE_DIR, exist_ok=True)
        storage_probe = os.path.join(
            BOT2_REVIEW_CACHE_DIR, ".bot2_storage_write_test"
        )
        with open(storage_probe, "w", encoding="utf-8") as probe_file:
            probe_file.write("ok")
        os.remove(storage_probe)

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

        # The singleton lock guarantees the previous Bot 2 instance is gone.
        # Requeue interrupted work while retaining its per-task disk artifacts.
        recovered_tasks = clip2_recover_interrupted_tasks()

        # ----------------------------------------------------
        # 4. REGISTER WORKER
        # ----------------------------------------------------

        for worker_id in BOT2_WORKER_IDS:
            _BOT2_WORKER_LOCAL.worker_id = worker_id
            clip2_register_worker(worker_id)

        _BOT2_WORKER_LOCAL.worker_id = BOT2_WORKER_IDS[0]
        clip2_admin_send("✅ ALL 3 WORKERS REGISTERED IN DATABASE")

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

        for worker_id in BOT2_WORKER_IDS:
            threading.Thread(
                target=bot2_clip_database_poller,
                args=(worker_id,),
                daemon=True,
                name=f"bot2-poller-{worker_id}"
            ).start()

        # ----------------------------------------------------
        # 8. ADMIN ONLINE MESSAGE
        # ----------------------------------------------------

        clip2_admin_send(
            "🟢 BOT 2 FULL DEBUG SYSTEM ONLINE\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Workers: {', '.join(BOT2_WORKER_IDS)}\n"
            f"Task type: {CLIP_TASK_TYPE}\n"
            f"Group: <code>{CLIP_CHANNEL_VIDEO}</code>\n"
            f"Maximum video: {clip2_size(BOT2_MAX_VIDEO_SIZE)}\n"
            f"Frames: {BOT2_FRAME_COUNT}\n"
            f"Gemini models: {len(GEMINI_MODELS)}\n"
            "Task concurrency: 3\n"
            "Gemini concurrency: 1 per model\n"
            "Gemini valid-answer limit: 3 (API attempts can use all configured models)\n"
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

        startup_error_text = (
            "🔴 BOT 2 STARTUP FAILED\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Category: {clip2_classify_error(error)}\n"
            f"Error: {str(error)[:1800]}\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        clip2_admin_send(startup_error_text)
        clip2_gemini_admin_send(startup_error_text)

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
