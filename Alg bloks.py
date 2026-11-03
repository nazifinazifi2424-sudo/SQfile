
# ============================================================
# CHAT MESSAGE SYSTEM - PART 1
# CONFIGURATION
# ============================================================

import re
import time
import json
import logging
import threading
import unicodedata

from psycopg2.extras import RealDictCursor
from telebot.apihelper import ApiTelegramException


CHAT_LOGGER = logging.getLogger("CHAT_MESSAGE_SYSTEM")
CHAT_LOGGER.setLevel(logging.INFO)

CHAT_RESPONSE_POLL_SECONDS = 1

CHAT_TYPING_FRAMES = [
    "Typing.",
    "Typing..",
    "Typing..."
]

CHAT_STOP_EVENT = threading.Event()

CHAT_MAX_WAIT_SECONDS = 1800

CHAT_MESSAGE_SYSTEM_LOCK = threading.Lock()
CHAT_MESSAGE_SYSTEM_RUNNING = False


def chat_log(message, *args):
    CHAT_LOGGER.info(message, *args)


def chat_error(message, *args):
    CHAT_LOGGER.exception(message, *args)


# ============================================================
# END PART 1
# ============================================================



# ============================================================
# CHAT MESSAGE SYSTEM - PART 2
# TEXT FILTER + DATABASE INSERT
# ============================================================

# Iyakar rubutu: haruffa 500
CHAT_MAX_TEXT_LENGTH = 500

CHAT_BLOCKED_TERMS = {
    "porn",
    "pornography",
    "xxx",
    "nude",
    "naked",
    "sexvideo",
    "adultvideo",
}


def chat_normalize_text(text):

    text = unicodedata.normalize("NFKC", text)
    text = text.lower()

    cleaned = []

    for char in text:
        category = unicodedata.category(char)

        if category == "Cf":
            continue

        if category.startswith("M"):
            continue

        cleaned.append(char)

    normalized = "".join(cleaned)

    compact = re.sub(
        r"[\W_]+",
        "",
        normalized,
        flags=re.UNICODE
    )

    return normalized, compact


# ------------------------------------------------------------
# LINK DETECTION
# ------------------------------------------------------------

CHAT_LINK_PATTERN = re.compile(
    r"("
    r"https?://|"
    r"www\.|"
    r"t\.me/|"
    r"telegram\.me/|"
    r"facebook\.com|"
    r"fb\.watch|"
    r"fb\.com|"
    r"tiktok\.com|"
    r"vm\.tiktok\.com|"
    r"youtube\.com|"
    r"youtu\.be|"
    r"\b[a-z0-9-]+\.(com|net|org|ng|co|app|io)\b"
    r")",
    re.IGNORECASE
)


def chat_contains_link(text):

    normalized, compact = chat_normalize_text(text)

    if CHAT_LINK_PATTERN.search(normalized):
        return True

    no_spaces = re.sub(r"\s+", "", normalized)

    if CHAT_LINK_PATTERN.search(no_spaces):
        return True

    return False


# ------------------------------------------------------------
# BASIC ADULT CONTENT FILTER
# ------------------------------------------------------------

def chat_contains_blocked_content(text):

    normalized, compact = chat_normalize_text(text)

    for term in CHAT_BLOCKED_TERMS:

        term_clean = re.sub(
            r"[\W_]+",
            "",
            term.lower()
        )

        if term_clean and term_clean in compact:
            return True

    return False


# ------------------------------------------------------------
# GET USER LANGUAGE
# Hausa = default
# English = if preferred/detected language is English
# ------------------------------------------------------------

def chat_get_user_language(user_id, telegram_language_code=None):

    language = None
    conn = None
    cur = None

    try:
        conn = get_all_chat_db_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT preferred_language, detected_language
            FROM customers
            WHERE user_id = %s
        """, (user_id,))

        row = cur.fetchone()

        if row:
            preferred_language, detected_language = row

            if preferred_language in ("ha", "en"):
                language = preferred_language

            elif detected_language in ("ha", "en"):
                language = detected_language

    except Exception as e:
        chat_error("Could not get user language: %s", repr(e))

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    if language:
        return language

    if telegram_language_code:
        if telegram_language_code.lower().startswith("en"):
            return "en"

    return "ha"


# ------------------------------------------------------------
# CREATE CUSTOMER + TASK + INCOMING MESSAGE
#
# Returns:
# ("created", task_id, message_row_id)
# ("busy", None, None)
# ------------------------------------------------------------

def chat_create_user_task(message):

    conn = get_all_chat_db_conn()
    cur = None

    try:
        cur = conn.cursor()

        user_id = message.from_user.id

        # Create customer if not already present.
        # Do not increase total_messages yet.
        cur.execute("""
            INSERT INTO customers (
                user_id,
                username,
                first_name,
                last_name,
                last_interaction_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, NOW(), NOW())

            ON CONFLICT (user_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                last_interaction_at = NOW(),
                updated_at = NOW()
        """, (
            user_id,
            message.from_user.username,
            message.from_user.first_name,
            message.from_user.last_name
        ))

        # Lock this customer's row.
        # This prevents two simultaneous messages from creating
        # two active tasks for the same user.
        cur.execute("""
            SELECT user_id
            FROM customers
            WHERE user_id = %s
            FOR UPDATE
        """, (user_id,))

        cur.fetchone()

        # Check whether this user already has unfinished work.
        cur.execute("""
            SELECT task_id
            FROM tasks
            WHERE user_id = %s
              AND status IN (
                  'pending',
                  'processing',
                  'answer_ready'
              )
            LIMIT 1
        """, (user_id,))

        active_task = cur.fetchone()

        if active_task:
            conn.commit()
            return "busy", None, None

        # Create new task
        cur.execute("""
            INSERT INTO tasks (
                user_id,
                task_type,
                status,
                priority,
                payload,
                created_at,
                updated_at
            )
            VALUES (
                %s,
                'text_message',
                'pending',
                5,
                %s::jsonb,
                NOW(),
                NOW()
            )
            RETURNING task_id
        """, (
            user_id,
            json.dumps({
                "text": message.text,
                "telegram_chat_id": message.chat.id,
                "telegram_message_id": message.message_id
            })
        ))

        task_id = cur.fetchone()[0]

        # Save incoming Telegram message
        cur.execute("""
            INSERT INTO chat_messages (
                user_id,
                task_id,
                telegram_chat_id,
                telegram_message_id,
                direction,
                sender_type,
                message_type,
                content,
                language_code,
                expires_at
            )
            VALUES (
                %s, %s, %s, %s,
                'incoming',
                'user',
                'text',
                %s,
                %s,
                NOW() + INTERVAL '12 hours'
            )
            RETURNING id
        """, (
            user_id,
            task_id,
            message.chat.id,
            message.message_id,
            message.text,
            message.from_user.language_code
        ))

        message_row_id = cur.fetchone()[0]

        # Increase message count only for accepted messages
        cur.execute("""
            UPDATE customers
            SET total_messages = total_messages + 1,
                last_interaction_at = NOW(),
                updated_at = NOW()
            WHERE user_id = %s
        """, (user_id,))

        conn.commit()

        return "created", str(task_id), message_row_id

    except Exception:
        conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# ============================================================
# END PART 2
# ============================================================



# ============================================================
# CHAT MESSAGE SYSTEM - PART 3
# TYPING + WORKER ANSWER CHECKER
# ============================================================


def chat_save_typing_message(
    user_id,
    task_id,
    chat_id,
    telegram_message_id
):

    conn = get_all_chat_db_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            INSERT INTO chat_messages (
                user_id,
                task_id,
                telegram_chat_id,
                telegram_message_id,
                direction,
                sender_type,
                message_type,
                content,
                expires_at
            )
            VALUES (
                %s, %s, %s, %s,
                'outgoing',
                'bot',
                'text',
                'Typing.',
                NOW() + INTERVAL '12 hours'
            )
            RETURNING id
        """, (
            user_id,
            task_id,
            chat_id,
            telegram_message_id
        ))

        row_id = cur.fetchone()[0]

        conn.commit()

        return row_id

    except Exception:

        conn.rollback()
        raise

    finally:

        cur.close()
        conn.close()


# ------------------------------------------------------------
# GET TASK RESULT
# ------------------------------------------------------------

def chat_get_task_result(task_id):

    conn = get_all_chat_db_conn()

    try:

        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                task_id,
                user_id,
                status,
                result_json,
                error_message
            FROM tasks
            WHERE task_id = %s
        """, (task_id,))

        return cur.fetchone()

    finally:

        cur.close()
        conn.close()


# ------------------------------------------------------------
# EDIT TYPING MESSAGE
# ------------------------------------------------------------

def chat_edit_typing(chat_id, message_id, text):

    try:

        bot.edit_message_text(
            text,
            chat_id,
            message_id
        )

        return True

    except ApiTelegramException as e:

        error_text = str(e).lower()

        if "message is not modified" in error_text:
            return True

        chat_log(
            "Typing edit failed: %s",
            str(e)
        )

        return False

    except Exception:

        chat_error("Typing edit error")
        return False


# ------------------------------------------------------------
# UPDATE BOT MESSAGE CONTENT IN DATABASE
# ------------------------------------------------------------

def chat_update_bot_message(
    chat_id,
    telegram_message_id,
    answer
):

    conn = get_all_chat_db_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE chat_messages
            SET content = %s,
                updated_at = NOW()
            WHERE telegram_chat_id = %s
              AND telegram_message_id = %s
              AND sender_type = 'bot'
              AND direction = 'outgoing'
        """, (
            answer,
            chat_id,
            telegram_message_id
        ))

        # Note:
        # Idan chat_messages ɗinka bai da updated_at,
        # kada a yi amfani da shi a wannan UPDATE.
        # Za mu gyara wannan a ƙasa ta hanyar cire shi.

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:

        cur.close()
        conn.close()


# ------------------------------------------------------------
# COMPLETE TASK AFTER USER RECEIVES ANSWER
# ------------------------------------------------------------

def chat_mark_task_delivered(task_id):

    conn = get_all_chat_db_conn()

    try:

        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET status = 'delivered',
                completed_at = NOW(),
                updated_at = NOW()
            WHERE task_id = %s
              AND status = 'answer_ready'
        """, (task_id,))

        conn.commit()

    except Exception:

        conn.rollback()
        raise

    finally:

        cur.close()
        conn.close()


# ------------------------------------------------------------
# WAIT FOR WORKER
# ------------------------------------------------------------

def chat_wait_for_worker(
    task_id,
    user_id,
    chat_id,
    typing_message_id
):

    frame_index = 0
    last_edit = 0
    started_at = time.monotonic()
    slow_message_shown = False

    while not CHAT_STOP_EVENT.is_set():

        try:

            task = chat_get_task_result(task_id)

        except Exception:

            chat_error("Could not check task result")
            time.sleep(CHAT_RESPONSE_POLL_SECONDS)
            continue

        if task is None:

            time.sleep(CHAT_RESPONSE_POLL_SECONDS)
            continue

        status = task["status"]

        # --------------------------------------------
        # WORKER HAS RETURNED ANSWER
        # --------------------------------------------

        if status == "answer_ready":

            result = task["result_json"] or {}

            if isinstance(result, str):

                try:
                    result = json.loads(result)
                except Exception:
                    result = {"answer": result}

            answer = result.get("answer")

            if not answer:

                answer = (
                    "Yi haƙuri, ba a samu amsa mai inganci ba."
                )

            try:

                # Edit Typing to final answer
                bot.edit_message_text(
                    str(answer),
                    chat_id,
                    typing_message_id
                )

                # Update saved outgoing message
                chat_update_bot_message(
                    chat_id,
                    typing_message_id,
                    str(answer)
                )

                # Mark task delivered
                chat_mark_task_delivered(task_id)

                chat_log(
                    "Task delivered successfully: %s",
                    task_id
                )

                return

            except Exception:

                chat_error(
                    "Answer delivery failed for task %s",
                    task_id
                )

                # Kada a sa delivered idan Telegram bai karɓi amsar ba.
                # A sake dubawa daga baya.
                time.sleep(5)
                continue

        # --------------------------------------------
        # WORKER FAILED
        # --------------------------------------------

        if status in ("failed", "rejected"):

            error_reply = (
                "Yi haƙuri, an samu matsala wajen "
                "sarrafa saƙonka. Ka sake gwadawa daga baya."
            )

            try:

                bot.edit_message_text(
                    error_reply,
                    chat_id,
                    typing_message_id
                )

                chat_update_bot_message(
                    chat_id,
                    typing_message_id,
                    error_reply
                )

                return

            except Exception:

                chat_error("Could not show task failure")
                return

        # --------------------------------------------
        # TYPING ANIMATION
        # --------------------------------------------

        now = time.monotonic()

        if now - last_edit >= 1:

            if (
                now - started_at >= CHAT_MAX_WAIT_SECONDS
                and not slow_message_shown
            ):

                frame = (
                    "Har yanzu ana sarrafa saƙonka. "
                    "Don Allah ka ɗan jira..."
                )

                slow_message_shown = True

            elif not slow_message_shown:

                frame = CHAT_TYPING_FRAMES[frame_index]

                frame_index = (
                    frame_index + 1
                ) % len(CHAT_TYPING_FRAMES)

            else:

                frame = "Ana ci gaba da sarrafa saƙonka..."

            chat_edit_typing(
                chat_id,
                typing_message_id,
                frame
            )

            last_edit = now

        time.sleep(CHAT_RESPONSE_POLL_SECONDS)


# ============================================================
# END PART 3
# ============================================================



# ============================================================
# CHAT MESSAGE SYSTEM - PART 4
# TELEGRAM HANDLERS + SYSTEM START
# ============================================================


# ------------------------------------------------------------
# LANGUAGE MESSAGES
# ------------------------------------------------------------

def chat_get_text(language, message_type):

    messages = {
        "ha": {
            "too_long": (
                "Don Allah ka yi magana a taƙaice. "
                "Saƙonka ya yi tsawo sosai."
            ),
            "busy": (
                "Don Allah ka jira. Har yanzu ina kan "
                "aikin saƙonka na baya."
            ),
            "link": (
                "Yi haƙuri, ba ma karɓar links a nan. "
                "Don Allah ka turo saƙon rubutu kawai."
            ),
            "blocked": (
                "Yi haƙuri, ba a yarda da irin wannan saƙon ba."
            ),
            "database_error": (
                "Yi haƙuri, an samu matsala wajen karɓar saƙonka. "
                "Ka sake gwadawa."
            ),
            "media": (
                "Yi haƙuri, a halin yanzu muna karɓar "
                "saƙon rubutu kawai."
            )
        },

        "en": {
            "too_long": (
                "Please keep your message short. "
                "Your message is too long."
            ),
            "busy": (
                "Please wait. I'm still working on your previous message."
            ),
            "link": (
                "Sorry, links are not accepted here. "
                "Please send a text message only."
            ),
            "blocked": (
                "Sorry, this type of message is not allowed."
            ),
            "database_error": (
                "Sorry, there was a problem receiving your message. "
                "Please try again."
            ),
            "media": (
                "Sorry, only text messages are accepted at the moment."
            )
        }
    }

    return messages.get(language, messages["ha"]).get(
        message_type,
        messages["ha"]["database_error"]
    )


# ------------------------------------------------------------
# SAVE TYPING MESSAGE IN DATABASE
# ------------------------------------------------------------

def chat_save_typing_message(
    user_id,
    task_id,
    chat_id,
    telegram_message_id
):

    conn = get_all_chat_db_conn()
    cur = None

    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO chat_messages (
                user_id,
                task_id,
                telegram_chat_id,
                telegram_message_id,
                direction,
                sender_type,
                message_type,
                content,
                expires_at
            )
            VALUES (
                %s, %s, %s, %s,
                'outgoing',
                'bot',
                'text',
                'Typing.',
                NOW() + INTERVAL '12 hours'
            )
        """, (
            user_id,
            task_id,
            chat_id,
            telegram_message_id
        ))

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# ------------------------------------------------------------
# UPDATE BOT MESSAGE CONTENT
# ------------------------------------------------------------

def chat_update_bot_message(
    chat_id,
    telegram_message_id,
    answer
):

    conn = get_all_chat_db_conn()
    cur = None

    try:
        cur = conn.cursor()

        cur.execute("""
            UPDATE chat_messages
            SET content = %s
            WHERE telegram_chat_id = %s
              AND telegram_message_id = %s
              AND sender_type = 'bot'
              AND direction = 'outgoing'
        """, (
            answer,
            chat_id,
            telegram_message_id
        ))

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        conn.close()


# ------------------------------------------------------------
# MARK TASK FAILED
# Used if Telegram cannot send/save the Typing message
# ------------------------------------------------------------

def chat_mark_task_failed(task_id, error_message):

    conn = get_all_chat_db_conn()
    cur = None

    try:
        cur = conn.cursor()

        cur.execute("""
            UPDATE tasks
            SET status = 'failed',
                error_message = %s,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE task_id = %s
              AND status = 'pending'
        """, (
            str(error_message)[:1000],
            task_id
        ))

        conn.commit()

    except Exception:
        conn.rollback()
        chat_error("Could not mark task as failed")

    finally:
        if cur:
            cur.close()
        conn.close()


# ------------------------------------------------------------
# TEXT MESSAGE HANDLER
# ------------------------------------------------------------

@bot.message_handler(
    content_types=["text"],
    func=lambda message: True
)
def chat_receive_text(message):

    text = message.text or ""

    if not text.strip():
        return

    user_id = message.from_user.id

    language = chat_get_user_language(
        user_id,
        message.from_user.language_code
    )

    # 1. Reject messages that exceed the character limit
    if len(text) > CHAT_MAX_TEXT_LENGTH:

        try:
            bot.reply_to(
                message,
                chat_get_text(language, "too_long")
            )
        except Exception:
            chat_error("Could not send long-message rejection")

        return

    # 2. Reject links
    if chat_contains_link(text):

        try:
            bot.reply_to(
                message,
                chat_get_text(language, "link")
            )
        except Exception:
            chat_error("Could not send link rejection")

        return

    # 3. Basic adult content filter
    if chat_contains_blocked_content(text):

        try:
            bot.reply_to(
                message,
                chat_get_text(language, "blocked")
            )
        except Exception:
            chat_error("Could not send moderation rejection")

        return

    # 4. Save message and create task
    try:
        result, task_id, message_row_id = chat_create_user_task(
            message
        )

    except Exception:
        chat_error("Could not save incoming message")

        try:
            bot.reply_to(
                message,
                chat_get_text(language, "database_error")
            )
        except Exception:
            chat_error("Could not send database error message")

        return

    # 5. If user already has unfinished work, do not queue this message
    if result == "busy":

        try:
            bot.reply_to(
                message,
                chat_get_text(language, "busy")
            )
        except Exception:
            chat_error("Could not send busy response")

        return

    # 6. Send Typing placeholder
    try:
        typing_message = bot.send_message(
            message.chat.id,
            "Typing."
        )

    except Exception as e:
        chat_error("Could not send typing message")

        try:
            chat_mark_task_failed(task_id, repr(e))
        except Exception:
            chat_error("Could not update failed task")

        return

    # 7. Save the Telegram ID of the Typing message
    try:
        chat_save_typing_message(
            user_id=user_id,
            task_id=task_id,
            chat_id=message.chat.id,
            telegram_message_id=typing_message.message_id
        )

    except Exception:
        chat_error("Could not save typing message in DB")

        # Try to remove the unsaved Typing message
        try:
            bot.delete_message(
                message.chat.id,
                typing_message.message_id
            )
        except Exception:
            pass

        try:
            chat_mark_task_failed(
                task_id,
                "Could not save Typing message in database"
            )
        except Exception:
            chat_error("Could not mark task failed")

        return

    # 8. Start a separate thread to wait for the worker
    waiting_thread = threading.Thread(
        target=chat_wait_for_worker,
        args=(
            task_id,
            user_id,
            message.chat.id,
            typing_message.message_id
        ),
        daemon=True
    )

    waiting_thread.start()

    chat_log(
        "Incoming text accepted. Task ID: %s",
        task_id
    )


# ------------------------------------------------------------
# REJECT NON-TEXT MESSAGES
# ------------------------------------------------------------

@bot.message_handler(
    content_types=[
        "photo",
        "video",
        "document",
        "audio",
        "voice",
        "video_note",
        "animation",
        "sticker",
        "contact",
        "location",
        "venue",
        "dice",
        "poll"
    ]
)
def chat_reject_non_text(message):

    language = chat_get_user_language(
        message.from_user.id,
        message.from_user.language_code
    )

    try:
        bot.reply_to(
            message,
            chat_get_text(language, "media")
        )

    except Exception:
        chat_error("Could not reject non-text message")


# ============================================================
# START CHAT MESSAGE SYSTEM
# ============================================================

def start_chat_message_system():

    global CHAT_MESSAGE_SYSTEM_RUNNING

    with CHAT_MESSAGE_SYSTEM_LOCK:

        if CHAT_MESSAGE_SYSTEM_RUNNING:
            chat_log("Chat message system already running")
            return

        CHAT_MESSAGE_SYSTEM_RUNNING = True

    CHAT_STOP_EVENT.clear()

    chat_log("Chat message handlers are ready")


# ============================================================
# RUN START FUNCTION
# ============================================================

start_chat_message_system()

# ============================================================
# END PART 4
# ============================================================
