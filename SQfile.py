## bot.py  (PostgreSQL SAFE – FULL FIX, nothing removed)

import telebot
from telebot import types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
import psycopg2
import time
import os

# ======================
# DATABASE CONNECTION
# ======================
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
    
def get_conn():
    try:
        c = psycopg2.connect(
            DATABASE_URL,
            connect_timeout=5,
            sslmode="require"
        )
        c.autocommit = True
        return c
    except Exception as e:
        print("❌ DB CONNECT ERROR:", e)
        return None
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

# ======================
# GLOBAL STATES
# ======================
admin_states = {}
last_menu_msg = {}
last_category_msg = {}
last_allfilms_msg = {}
allfilms_sessions = {}
cart_sessions = {}
series_sessions = {}
user_states = {}

# =========================
# DATABASE TABLES (SAFE)
# =========================

# -------- MOVIES --------
cur.execute("""
CREATE TABLE IF NOT EXISTS movies (
    id SERIAL PRIMARY KEY,
    title TEXT,
    price INTEGER,
    file_id TEXT,
    file_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    channel_msg_id INTEGER,
    channel_username TEXT
)
""")

# -------- ITEMS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    id SERIAL PRIMARY KEY,
    title TEXT,
    price INTEGER,
    file_id TEXT,
    file_name TEXT,
    group_key TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    channel_msg_id INTEGER,
    channel_username TEXT
)
""")

# -------- ORDERS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id BIGINT,
    movie_id INTEGER,
    item_id INTEGER,
    amount INTEGER,
    paid INTEGER DEFAULT 0,
    pay_ref TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- ORDER ITEMS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id TEXT,
    movie_id INTEGER,
    item_id INTEGER,
    price INTEGER,
    file_id TEXT
)
""")

# -------- WEEKLY --------
cur.execute("""
CREATE TABLE IF NOT EXISTS weekly (
    id SERIAL PRIMARY KEY,
    poster_file_id TEXT,
    items TEXT,
    file_name TEXT,
    file_id TEXT,
    channel_msg_id INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- CART --------
cur.execute("""
CREATE TABLE IF NOT EXISTS cart (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    movie_id INTEGER,
    item_id INTEGER,
    price INTEGER,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- REFERRALS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS referrals (
    id SERIAL PRIMARY KEY,
    referrer_id BIGINT,
    referred_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reward_granted INTEGER DEFAULT 0
)
""")

# -------- REORDERS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS reorders (
    old_order_id INTEGER,
    new_order_id INTEGER,
    user_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (old_order_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS referral_credits (
    id SERIAL PRIMARY KEY,
    referrer_id BIGINT,
    amount INTEGER,
    used INTEGER DEFAULT 0,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- USER PREFS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS user_prefs (
    user_id BIGINT PRIMARY KEY,
    lang TEXT DEFAULT 'ha'
)
""")

# -------- USER LIBRARY --------
cur.execute("""
CREATE TABLE IF NOT EXISTS user_library (
    user_id BIGINT NOT NULL,
    movie_id INTEGER,
    item_id INTEGER,
    acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, movie_id, item_id)
)
""")

# -------- BUY ALL TOKENS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS buyall_tokens (
    token TEXT PRIMARY KEY,
    ids TEXT
)
""")

# -------- USER MOVIES --------
cur.execute("""
CREATE TABLE IF NOT EXISTS user_movies (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    movie_id INTEGER,
    item_id INTEGER,
    order_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resend_count INTEGER DEFAULT 0
)
""")

# =====================
# SERIES
# =====================
cur.execute("""
CREATE TABLE IF NOT EXISTS series (
    id SERIAL PRIMARY KEY,
    title TEXT,
    file_name TEXT,
    file_id TEXT,
    price INTEGER,
    poster_file_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    channel_msg_id INTEGER,
    channel_username TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS series_items (
    id SERIAL PRIMARY KEY,
    series_id INTEGER,
    movie_id INTEGER,
    item_id INTEGER,
    file_id TEXT,
    title TEXT,
    order_id TEXT,
    price INTEGER DEFAULT 0,
    channel_msg_id INTEGER,
    channel_username TEXT,
    file_name TEXT
)
""")

# =====================
# FEEDBACK
# =====================
cur.execute("""
CREATE TABLE IF NOT EXISTS feedbacks (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    user_id BIGINT NOT NULL,
    mood TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS resend_logs (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    used_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# =====================
# HAUSA SERIES
# =====================
cur.execute("""
CREATE TABLE IF NOT EXISTS hausa_series (
    id SERIAL PRIMARY KEY,
    title TEXT,
    file_name TEXT,
    file_id TEXT,
    price INTEGER,
    series_id TEXT,
    poster_file_id TEXT,
    channel_msg_id INTEGER,
    channel_username TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS hausa_series_items (
    id SERIAL PRIMARY KEY,
    hausa_series_id INTEGER,
    movie_id INTEGER,
    item_id INTEGER,
    price INTEGER,
    file_id TEXT,
    title TEXT,
    order_id TEXT,
    series_id INTEGER,
    channel_msg_id INTEGER,
    channel_username TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name TEXT
)
""")

# ================= VISITED USERS =================
cur.execute("""
CREATE TABLE IF NOT EXISTS visited_users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# -------- ADMIN CONTROLS --------
cur.execute("""
CREATE TABLE IF NOT EXISTS admin_controls (
    id SERIAL PRIMARY KEY,
    admin_id BIGINT UNIQUE,
    sendmovie_enabled INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# ================= HOW TO BUY =================
cur.execute("""
CREATE TABLE IF NOT EXISTS how_to_buy (
    id SERIAL PRIMARY KEY,
    hausa_text TEXT,
    english_text TEXT,
    media_file_id TEXT,
    media_type TEXT,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

print("✅ DATABASE READY — BIGINT FIX APPLIED")

import uuid
import re
import json
import requests
import traceback
import random
import difflib
from datetime import datetime, timedelta
import urllib.parse
admin_states = {}
# --- Admins configuration ---
ADMINS = [8537505191, 5009954635] 

  # add more admin IDs here
# ========= CONFIG =========
import os
from datetime import datetime

BOT_TOKEN = os.getenv("BOT_TOKEN")

BOT_MODE = os.getenv("BOT_MODE", "polling")

ADMIN_ID = 8537505191
OTP_ADMIN_ID = 6603268127


BOT_USERNAME = "Danchirinbot"
CHANNEL = "@Danchirinps"

# Flutterwave
FLW_PUBLIC_KEY = os.getenv("FLW_PUBLIC_KEY")
FLW_SECRET_KEY = os.getenv("FLW_SECRET_KEY")
FLW_WEBHOOK_SECRET = os.getenv("FLW_WEBHOOK_SECRET")
FLW_REDIRECT_URL = os.getenv("FLW_REDIRECT_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# === PAYMENTS / STORAGE ===
PAYMENT_NOTIFY_GROUP = -1003553575069
STORAGE_CHANNEL = -1003794258511
SEND_ADMIN_PAYMENT_NOTIF = False

FLW_BASE = "https://api.flutterwave.com/v3"
PAYSTACK_SECRET = None
ADMIN_USERNAME = "Aslamtv1"

# ========= IMPORTS =========
import requests
import telebot
from flask import Flask, request
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ========= BOT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ========= FLASK =========
app = Flask(__name__)

# ========= FLUTTERWAVE PAYMENT =========
def create_flutterwave_payment(user_id, order_id, amount, title):
    if not FLW_SECRET_KEY or not FLW_REDIRECT_URL:
        print("❌ Flutterwave env missing")
        return None

    headers = {
        "Authorization": f"Bearer {FLW_SECRET_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "tx_ref": str(order_id),
        "amount": int(amount),
        "currency": "NGN",
        "redirect_url": FLW_REDIRECT_URL,
        "customer": {
            "email": f"user{user_id}@telegram.com",
            "name": f"TG User {user_id}"
        },
        "customizations": {
            "title": title[:50],
            "description": f"Order {order_id}"
        }
    }

    try:
        r = requests.post(
            f"{FLW_BASE}/payments",
            json=payload,
            headers=headers,
            timeout=30
        )

        data = r.json()

        if r.status_code != 200 or data.get("status") != "success":
            print("❌ Flutterwave error:", data)
            return None

        return data["data"]["link"]

    except Exception as e:
        print("❌ create_flutterwave_payment error:", e)
        return None

# ========= HOME / KEEP ALIVE =========
@app.route("/")
def home():
    return "OK", 200

# ========= CALLBACK PAGE =========
@app.route("/flutterwave-callback", methods=["GET"])
def flutterwave_callback():
    return """
    <html>
    <head>
        <title>Payment Successful</title>
        <meta http-equiv="refresh" content="5;url=https://t.me/Aslamtv2bot">
    </head>
    <body style="font-family: Arial; text-align: center; padding-top: 150px; font-size: 22px;">
    
        <h2>✅ Payment Successful</h2>
        <p>An tabbatar da biyan ka.</p>
        <p>Kashe browser ka koma telegram, SWITCH OFF YOUR BROWSER AND GO BACK TO TELEGRAM</p>
        <a href="https://t.me/Aslamtv2bot">Komawa Telegram yanzu</a>
    </body>
    </html>
    """
# ========= FEEDBACK =========
def send_feedback_prompt(user_id, order_id):
    try:
        conn = get_conn()
        if not conn:
            return

        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM feedbacks WHERE order_id = %s",
            (order_id,)
        )
        exists = cur.fetchone()

        cur.close()
        conn.close()

        if exists:
            return

    except Exception as e:
        print("FEEDBACK CHECK ERROR:", e)
        try:
            cur.close()
            conn.close()
        except:
            pass
        return

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("😁 Very good", callback_data=f"feedback:very:{order_id}"),
        InlineKeyboardButton("🙂 Good", callback_data=f"feedback:good:{order_id}")
    )
    kb.add(
        InlineKeyboardButton("😓 Not sure", callback_data=f"feedback:neutral:{order_id}"),
        InlineKeyboardButton("😠 Angry", callback_data=f"feedback:angry:{order_id}")
    )

    try:
        bot.send_message(
            user_id,
            "Ina fatan ka ji daɗin siyayya 🥰\nDan Allah ka zaɓi yadda kake ji yanzu👇",
            reply_markup=kb
        )
        print("✅ Feedback prompt sent:", user_id, order_id)
    except Exception as e:
        print("FEEDBACK SEND ERROR:", e)

@app.route("/webhook", methods=["POST"])
def flutterwave_webhook():

    # ================= SIGNATURE =================
    signature = request.headers.get("verif-hash")
    if not signature or signature != FLW_WEBHOOK_SECRET:
        return "Invalid signature", 401

    # ================= PAYLOAD =================
    payload = request.json or {}
    data = payload.get("data") or {}

    status = (data.get("status") or "").lower()
    if status not in ("successful", "success"):
        return "Ignored", 200

    order_id = str(data.get("tx_ref") or "")
    currency = data.get("currency")
    paid_amount = int(float(data.get("amount", 0)))

    if not order_id:
        return "Missing order id", 200

    # ================= DB =================
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT user_id, amount, paid FROM orders WHERE id=%s",
        (order_id,)
    )
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return "Order not found", 200

    user_id, expected_amount, paid = row

    if paid == 1:
        cur.close()
        conn.close()
        return "Already processed", 200

    if paid_amount != expected_amount or currency != "NGN":
        cur.close()
        conn.close()
        return "Wrong payment", 200

    # ================= ITEMS =================
    cur.execute(
        """
        SELECT i.title
        FROM order_items oi
        JOIN items i ON i.id = oi.item_id
        WHERE oi.order_id=%s
        """,
        (order_id,)
    )
    titles = [r[0] for r in cur.fetchall()]

    if not titles:
        cur.close()
        conn.close()
        return "Empty order", 200

    titles_text = "\n".join(f"• {t}" for t in titles)
    items_count = len(titles)

    # ================= USER INFO =================
    cur.execute(
        "SELECT first_name, last_name FROM visited_users WHERE user_id=%s",
        (user_id,)
    )
    u = cur.fetchone()

    if u and (u[0] or u[1]):
        full_name = f"{u[0] or ''} {u[1] or ''}".strip()
    else:
        try:
            chat = bot.get_chat(user_id)
            full_name = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
        except:
            full_name = "User"

    # ================= MARK AS PAID =================
    cur.execute(
        "UPDATE orders SET paid=1 WHERE id=%s",
        (order_id,)
    )

    conn.commit()
    cur.close()
    conn.close()

    # ================= USER MESSAGE (OLD FORMAT) =================
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(
            "⬇️ DOWNLOAD ITEMS",
            callback_data=f"deliver:{order_id}"
        )
    )

    bot.send_message(
        user_id,
        f"""Hi {full_name} 👋

🎉 <b>An tabbatar da biyanka cikin nasara.</b>

🎬 <b>Yanzu ka riga ka mallaki:</b>
{titles_text}

━━━━━━━━━━━━━━
📦 <b>Order:</b> Arrived ✅
🔐 <b>Status:</b> Confirmed
🆔 <b>Ref:</b> <code>{order_id}</code>
━━━━━━━━━━━━━━

Mun gode da amincewa da mu 🤍  
Danna <b>DOWNLOAD ITEMS</b> domin karɓa yanzu.
""",
        parse_mode="HTML",
        reply_markup=kb
    )

    # ================= ADMIN GROUP =================
    if PAYMENT_NOTIFY_GROUP:
        bot.send_message(
            PAYMENT_NOTIFY_GROUP,
            f"""🟢 <b>TRANSACTION COMPLETED</b>

📦 Status: Confirmed
🎬 Items: {items_count} files
Item names:
{titles_text}

👤 User full name: {full_name}
🆔 User ID: <code>{user_id}</code>

💳 Total amount: ₦{paid_amount}
🧾 Ref: <code>{order_id}</code>
""",
            parse_mode="HTML"
        )

    return "OK", 200


@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    update = telebot.types.Update.de_json(
        request.stream.read().decode("utf-8")
    )
    bot.process_new_updates([update])
    return "OK", 200

# ================= ALL FILMS (GROUP AWARE) ============
# ======================================================
PER_PAGE = 5
SEARCH_PAGE_SIZE = 5

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import re


# ================== NORMALIZER ==================
def _norm(txt):
    if not txt:
        return ""
    txt = str(txt).lower()
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


# ---------- PAGINATION ----------
def paginate(items, per_page):
    pages = []
    for i in range(0, len(items), per_page):
        pages.append(items[i:i + per_page])
    return pages


# ---------- FETCH ALL ITEMS ----------
def _get_all_movies():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, title, price, file_name, created_at, group_key
        FROM items
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


# ---------- BUILD GROUP-AWARE ROWS ----------

def build_allfilms_rows():
    groups = {}

    for mid, title, price, fname, created, gk in _get_all_movies():
        key = gk or f"single_{mid}"

        if key not in groups:
            groups[key] = {
                "ids": [],
                "title": title,
                "price": price
            }

        groups[key]["ids"].append(mid)

    rows = []
    for g in groups.values():
        rows.append((g["ids"], g["title"], g["price"]))

    return rows


# ---------- SEND / EDIT ALL FILMS PAGE ----------
def send_allfilms_page(uid, page_index):
    sess = allfilms_sessions.get(uid)

    # 🛡️ SAFETY CHECK
    if not sess or "pages" not in sess:
        return

    pages = sess["pages"]
    if page_index < 0 or page_index >= len(pages):
        return

    sess["index"] = page_index
    rows = pages[page_index]

    # ===== TEXT =====
    text = "<b>🎬 All Films</b>\n\n"
    for ids, title, price in rows:
        safe_title = str(title).replace("<", "").replace(">", "")
        text += f"🎬 <b>{safe_title}</b>\n💵 ₦{price}\n\n"

    # ===== BUTTONS =====
    kb = InlineKeyboardMarkup(row_width=2)

    for ids, title, price in rows:
        ids_str = "_".join(str(i) for i in ids)
        kb.add(
            InlineKeyboardButton(
                f"🛒 Add to Cart — {title}",
                callback_data=f"addcartdm:{ids_str}"
            ),
            InlineKeyboardButton(
                f"💳 Buy Now — {title}",
                callback_data=f"buygroup:{ids_str}"
            )
        )

    # ===== NAVIGATION =====
    nav = []
    if page_index > 0:
        nav.append(InlineKeyboardButton("◀️ Back", callback_data="allfilms_prev"))
    if page_index < len(pages) - 1:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data="allfilms_next"))
    if nav:
        kb.row(*nav)

    # ===== EXTRA =====
    kb.add(InlineKeyboardButton("🔍 SEARCH MOVIE", callback_data="search_movie"))
    kb.add(
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
        InlineKeyboardButton("📺 Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    # ===== EDIT OR SEND =====
    try:
        if sess.get("last_msg"):
            bot.edit_message_text(
                text,
                chat_id=uid,
                message_id=sess["last_msg"],
                reply_markup=kb,
                parse_mode="HTML"
            )
        else:
            msg = bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML")
            sess["last_msg"] = msg.message_id
    except:
        pass

    allfilms_sessions[uid] = sess


# ---------- START ALL FILMS ----------
def start_allfilms(uid):
    rows = build_allfilms_rows()
    if not rows:
        bot.send_message(uid, "❌ Babu fim a DB")
        return

    pages = paginate(rows, PER_PAGE)

    allfilms_sessions[uid] = {
        "pages": pages,
        "index": 0,
        "last_msg": None
    }

    send_allfilms_page(uid, 0)

import time
from telebot.apihelper import ApiTelegramException

@bot.callback_query_handler(func=lambda c: c.data.startswith("deliver:"))
def deliver_items(call):

    user_id = call.from_user.id

    try:
        _, order_id = call.data.split(":", 1)
    except:
        bot.answer_callback_query(call.id, "❌ Invalid order info.")
        return

    conn = get_conn()
    cur = conn.cursor()

    # ================= CHECK ORDER =================
    cur.execute(
        "SELECT paid FROM orders WHERE id=%s AND user_id=%s",
        (order_id, user_id)
    )
    row = cur.fetchone()

    if not row or row[0] != 1:
        cur.close()
        conn.close()
        bot.answer_callback_query(
            call.id,
            "❌ Your payment has not been confirmed yet."
        )
        return

    # ================= PREVENT RESEND =================
    cur.execute(
        "SELECT 1 FROM user_movies WHERE order_id=%s LIMIT 1",
        (order_id,)
    )
    if cur.fetchone():
        cur.close()
        conn.close()

        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton(
                "📽 PAID MOVIES",
                callback_data="my_movies"
            )
        )

        bot.send_message(
            user_id,
            "ℹ️ You have already received this movie.\n\n"
            "📽 You can download it again from Paid Movies.",
            reply_markup=kb
        )
        return

    bot.answer_callback_query(call.id, "📤 Sending your items…")

    # ================= FETCH ITEMS =================
    cur.execute(
        """
        SELECT oi.item_id, oi.file_id, i.title
        FROM order_items oi
        JOIN items i ON i.id = oi.item_id
        WHERE oi.order_id=%s
        """,
        (order_id,)
    )
    items = cur.fetchall()

    if not items:
        cur.close()
        conn.close()
        bot.send_message(user_id, "❌ Order items not found.")
        return

    total = len(items)

    if total >= 20:
        bot.send_message(
            user_id,
            "⏳ Your movies are many.\n"
            "Please wait while delivery continues..."
        )

    # ================= SAFE SEND FUNCTION =================
    def safe_send(chat_id, file_id, title):

        while True:
            try:
                try:
                    return bot.send_video(
                        chat_id,
                        file_id,
                        caption=f"🎬 {title}"
                    )
                except:
                    return bot.send_document(
                        chat_id,
                        file_id,
                        caption=f"📁 {title}"
                    )

            except ApiTelegramException as e:

                if e.error_code == 429:
                    retry = int(e.result_json["parameters"]["retry_after"])
                    time.sleep(retry)
                    continue
                else:
                    return None

            except Exception:
                return None

    # ================= SEND LOOP =================
    sent = 0

    for index, (item_id, file_id, title) in enumerate(items, start=1):

        if not file_id:
            continue

        # avoid duplicate per item
        cur.execute(
            "SELECT 1 FROM user_movies WHERE user_id=%s AND item_id=%s",
            (user_id, item_id)
        )
        if cur.fetchone():
            continue

        msg = safe_send(user_id, file_id, title)

        if not msg:
            continue

        cur.execute(
            """
            INSERT INTO user_movies (user_id, item_id, order_id)
            VALUES (%s,%s,%s)
            """,
            (user_id, item_id, order_id)
        )

        sent += 1

        # Soft delay (extra safety)
        time.sleep(1.0)

    conn.commit()
    cur.close()
    conn.close()

    if sent == 0:
        bot.send_message(user_id, "❌ Items could not be sent.")
        return

    bot.send_message(
        user_id,
        f"✅ Your movie(s) have been delivered ({sent}).\n"
        "Thank you for your purchase 🤗"
    )

    send_feedback_prompt(user_id, order_id)
# =========================================================
# ========= HARD START HOWTO (DEEPLINK LOCK) ===============
# =========================================================
@bot.message_handler(
    func=lambda m: (
        m.text
        and m.text.startswith("/start ")
        and len(m.text.split(" ", 1)) > 1
        and m.text.split(" ", 1)[1].startswith("howto_")
    )
)
def __hard_start_howto(msg):
    """
    Wannan handler:
    - Yana rike howto_ deeplink
    - Yana hana komawa main /start
    - Yana kira howto_start_handler kai tsaye
    """
    return howto_start_handler(msg)
# ================= HOW TO BUY STATE =================
HOWTO_STATE = {}


# ======================================================
# /update  (ADMIN ONLY)
# ======================================================
@bot.message_handler(commands=["update"])
def update_howto_cmd(m):
    if m.from_user.id != ADMIN_ID:
        return

    HOWTO_STATE[m.from_user.id] = {"stage": "hausa"}

    bot.send_message(
        m.chat.id,
        "✍️ <b>Rubuta HAUSA version cikakke:</b>",
        parse_mode="HTML"
    )


# ======================================================
# UPDATE FLOW (HAUSA → ENGLISH → MEDIA)
# ======================================================
@bot.message_handler(
    func=lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in HOWTO_STATE,
    content_types=["text", "video", "document", "photo"]
)
def howto_update_flow(m):
    state = HOWTO_STATE.get(m.from_user.id)
    if not state:
        return

    stage = state["stage"]

    # ---------- HAUSA ----------
    if stage == "hausa":
        if m.content_type != "text":
            bot.send_message(m.chat.id, "❌ Hausa text kawai ake bukata.")
            return
        state["hausa_text"] = m.text
        state["stage"] = "english"
        bot.send_message(
            m.chat.id,
            "✍️ <b>Rubuta ENGLISH version:</b>",
            parse_mode="HTML"
        )
        return

    # ---------- ENGLISH ----------
    if stage == "english":
        if m.content_type != "text":
            bot.send_message(m.chat.id, "❌ English text kawai ake bukata.")
            return
        state["english_text"] = m.text
        state["stage"] = "media"
        bot.send_message(
            m.chat.id,
            "🎬 Turo <b>VIDEO / DOCUMENT / PHOTO</b>:",
            parse_mode="HTML"
        )
        return

    # ---------- MEDIA ----------
    if stage == "media":
        file_id = None
        media_type = None

        if m.content_type == "video":
            file_id = m.video.file_id
            media_type = "video"
        elif m.content_type == "document":
            file_id = m.document.file_id
            media_type = "document"
        elif m.content_type == "photo":
            file_id = m.photo[-1].file_id
            media_type = "photo"
        else:
            bot.send_message(m.chat.id, "❌ Media bai dace ba.")
            return

        # ================= DB =================
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "SELECT COALESCE(MAX(version), 0) FROM how_to_buy"
        )
        last_version = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO how_to_buy
            (hausa_text, english_text, media_file_id, media_type, version)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                state["hausa_text"],
                state["english_text"],
                file_id,
                media_type,
                last_version + 1
            )
        )

        conn.commit()
        cur.close()
        conn.close()

        HOWTO_STATE.pop(m.from_user.id, None)

        bot.send_message(
            m.chat.id,
            "✅ <b>HOW TO BUY an sabunta successfully</b>",
            parse_mode="HTML"
        )


# ======================================================
# /post  (ADMIN ONLY)
# ======================================================
@bot.message_handler(commands=["post"])
def post_to_channel(m):
    if m.from_user.id != ADMIN_ID:
        return

    # ================= DB =================
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT version
        FROM how_to_buy
        ORDER BY version DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        bot.send_message(m.chat.id, "❌ Babu HOW TO BUY da aka saita tukuna.")
        return

    version = row[0]
    deeplink = f"https://t.me/{BOT_USERNAME}?start=howto_{version}"

    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(
            "👉 Click here",
            url=deeplink
        )
    )

    bot.send_message(
        CHANNEL,
        " <b>📸📢📢📢📢📢\n\n 👥Koyi yadda zaka siya 🎬fim a 🤖BOT ɗinmu, cikin sauri da sauki sosai\n\n Cikin aminci ba jirah🥰\n\n\n 🤖@Aslamtv2bot\n\nDANNA (Click here)\n\n🔰🔰🔰🔰🔰</b>",
        parse_mode="HTML",
        reply_markup=kb
    )

    bot.send_message(m.chat.id, "✅ An tura post zuwa channel.")


# ======================================================
# DEEPLINK HANDLER
# ======================================================
# HOW TO START (HOWTO ONLY)
# ======================================================
@bot.message_handler(func=lambda m: m.text and m.text.startswith("/start howto_"))
def howto_start_handler(m):
    args = m.text.split()

    # kariya (defensive, ko da filter ya riga ya rufe)
    if len(args) < 2 or not args[1].startswith("howto_"):
        return

    try:
        version = int(args[1].split("_")[1])
    except Exception:
        return

    # ================= DB =================
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT hausa_text, english_text, media_file_id, media_type
        FROM how_to_buy
        WHERE version=%s
        """,
        (version,)
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        bot.send_message(m.chat.id, "❌ Wannan version bai wanzu ba.")
        return

    hausa_text, english_text, file_id, media_type = row

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("🇬🇧 English", callback_data=f"howto_en:{version}"),
        types.InlineKeyboardButton("🇳🇬 Hausa", callback_data=f"howto_ha:{version}")
    )

    caption = hausa_text

    if media_type == "video":
        bot.send_video(
            m.chat.id,
            file_id,
            caption=caption,
            reply_markup=kb
        )
    elif media_type == "document":
        bot.send_document(
            m.chat.id,
            file_id,
            caption=caption,
            reply_markup=kb
        )
    else:
        bot.send_photo(
            m.chat.id,
            file_id,
            caption=caption,
            reply_markup=kb
        )

# ======================================================
# LANGUAGE SWITCH (EDIT ONLY)
# ======================================================
@bot.callback_query_handler(func=lambda c: c.data.startswith("howto_"))
def howto_language_switch(c):
    try:
        lang, version = c.data.split(":")
        version = int(version)
    except:
        return

    # ================= DB =================
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT hausa_text, english_text
        FROM how_to_buy
        WHERE version=%s
        """,
        (version,)
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        bot.answer_callback_query(c.id, "❌ Version bai wanzu ba.")
        return

    hausa_text, english_text = row

    text = english_text if lang == "howto_en" else hausa_text

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("🇬🇧 English", callback_data=f"howto_en:{version}"),
        types.InlineKeyboardButton("🇳🇬 Hausa", callback_data=f"howto_ha:{version}")
    )

    try:
        bot.edit_message_caption(
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            caption=text,
            reply_markup=kb
        )
    except:
        pass

    bot.answer_callback_query(c.id)

# ======================================================

# ========= HARD START BUYD =========
@bot.message_handler(
    func=lambda m: m.text
    and m.text.startswith("/start ")
    and m.text.split(" ", 1)[1].startswith("buyd_")
)
def __hard_start_buyd(msg):
    return buyd_deeplink_handler(msg)


# ========= HARD START GROUPITEM =========
@bot.message_handler(
    func=lambda m: m.text
    and m.text.startswith("/start ")
    and m.text.split(" ", 1)[1].startswith("groupitem_")
)
def __hard_start_groupitem(msg):
    return groupitem_deeplink_handler(msg)

# --- Added deep-link start handler for viewall/weakupdate (runs before other start handlers) ---  
@bot.message_handler(func=lambda m: (m.text or "").strip().split(" ")[0]=="/start" and len((m.text or "").strip().split(" "))>1 and (m.text or "").strip().split(" ")[1] in ("viewall","weakupdate"))  
def _start_deeplink_handler(msg):  
    """  
    Catch /start viewall or /start weakupdate deep-links from channel posts.  
    This handler tries to send the weekly list directly and then returns without invoking the normal start flow.  
    Placed early to take precedence over other start handlers.  
    """  
    try:  
        send_weekly_list(msg)  
    except Exception as e:  
        try:  
            bot.send_message(msg.chat.id, "An samu matsala wajen nuna weekly list.")  
        except:  
            pass  
    return


# ===== SEARCH BY NAME: USER TEXT INPUT =====
@bot.message_handler(
    func=lambda m: admin_states.get(m.from_user.id, {}).get("state") == "search_wait_name"
)
def search_name_text(m):
    uid = m.from_user.id
    text = (m.text or "").strip()

    # kariya
    if not text:
        bot.send_message(uid, "❌ Rubuta sunan fim.")
        return

    # harafi 2 ko 3 kawai
    if len(text) < 2 or len(text) > 3:
        bot.send_message(
            uid,
            "❌ Rubuta *HARAFI 2 KO 3* kawai.\nMisali: *MAS*",
            parse_mode="Markdown"
        )
        return

    # ajiye abin da user ya nema (engine zai karanta daga nan)
    admin_states[uid]["query"] = text.lower()

    # sanar da user
    bot.send_message(
        uid,
        f"🔍 Kana nema: *{text.upper()}*\n⏳ Ina dubawa...",
        parse_mode="Markdown"
    )

    # 👉 KIRA SEARCH ENGINE (RUKUNI C) – PAGE NA FARKO
    send_search_results(uid, 0)


# ===== FALLBACK: IDAN USER YA RUBUTA ABU BA A SEARCH MODE BA =====
@bot.message_handler(
    func=lambda m: m.from_user.id in admin_states
    and admin_states.get(m.from_user.id, {}).get("state") in (
        "search_menu",
        "browse_menu",
        "series_menu",
        "search_trending",
    )
)
def ignore_unexpected_text(m):
    uid = m.from_user.id
    bot.send_message(
        uid,
        "ℹ️ Don Allah ka yi amfani da *buttons* da ke ƙasa.",
        parse_mode="Markdown"
    )
# ======================================================
# ACTIVE BUYERS (ADMIN ONLY | PAGINATION | EDIT MODE)
# ======================================================

# ================== END RUKUNI B ==================

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("cancel:"))
def cancel_order_handler(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    try:
        order_id = c.data.split("cancel:", 1)[1]
    except:
        return

    # ================= DB =================
    conn = get_conn()
    cur = conn.cursor()

    # 🔎 Tabbatar order na wannan user ne kuma unpaid
    cur.execute(
        """
        SELECT id
        FROM orders
        WHERE id=%s AND user_id=%s AND paid=0
        """,
        (order_id, uid)
    )
    order = cur.fetchone()

    if not order:
        cur.close()
        conn.close()
        bot.send_message(
            uid,
            "❌ <b>Ba a sami order ba ko kuma an riga an biya shi.</b>",
            parse_mode="HTML"
        )
        return

    # 🧹 Goge order_items
    cur.execute(
        "DELETE FROM order_items WHERE order_id=%s",
        (order_id,)
    )

    # 🧹 Goge order
    cur.execute(
        "DELETE FROM orders WHERE id=%s",
        (order_id,)
    )

    conn.commit()
    cur.close()
    conn.close()

    bot.send_message(
        uid,
        "❌ <b>An soke wannan order ɗin.</b>",
        parse_mode="HTML"
    )

# --- Added callback handler for in-bot "View All Movies" buttons ---
@bot.callback_query_handler(func=lambda c: c.data in ("view_all_movies","viewall"))
def _callback_view_all(call):
    uid = call.from_user.id
    # Build a small message-like object expected by send_weekly_list
    class _Msg:
        def __init__(self, uid):
            self.chat = type('X', (), {'id': uid})
            self.text = ""
    try:
        send_weekly_list(_Msg(uid))
        bot.answer_callback_query(call.id)
    except Exception as e:
        bot.answer_callback_query(call.id, "An samu matsala wajen nuna jerin.")





# ========== HELPERS ==========

def check_join(uid):
    try:
        member = bot.get_chat_member(CHANNEL, uid)
        return member.status in ("member", "administrator", "creator", "restricted")
    except Exception:
        return False


# name anonymization
def mask_name(fullname):
    """Mask parts of the name as requested: Muhmad, Khid, Sa*i style."""
    if not fullname:
        return "User"
    s = re.sub(r"\s+", " ", fullname.strip())
    # split on non-alphanumeric to preserve parts
    parts = re.split(r'(\W+)', s)
    out = []
    for p in parts:
        if not p or re.match(r'\W+', p):
            out.append(p)
            continue
        # p is a word
        n = len(p)
        if n <= 2:
            out.append(p[0] + "*" * (n - 1))
            continue
        # keep first 2 and last 1, hide middle with **
        if n <= 4:
            keep = p[0] + "*" * (n - 2) + p[-1]
            out.append(keep)
        else:
            # first two, two stars, last one
            out.append(p[:2] + "**" + p[-1])
    return "".join(out)


# language helpers (persisted in DB)
def set_user_lang(user_id, lang_code):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO user_prefs (user_id, lang)
            VALUES (%s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET lang = EXCLUDED.lang
            """,
            (user_id, lang_code)
        )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("set_user_lang error:", e)


def get_user_lang(user_id):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "SELECT lang FROM user_prefs WHERE user_id=%s",
            (user_id,)
        )
        row = cur.fetchone()

        cur.close()
        conn.close()

        if row:
            return row[0]

    except Exception as e:
        print("get_user_lang error:", e)

    return "ha"


# translation map for interface (not movie titles). Hausa (ha) = keep original messages in code.
TRANSLATIONS = {
    "en": {
        "welcome_shop": "Welcome to the film store:",
        "ask_name": "Hello! What do you need?:",
        "joined_ok": "✔ Joined the channel!",
        "not_joined": "❌ You have not joined.",
        "invite_text": "Invite friends and earn rewards! Share your link:",
        "no_movies": "No movies to show right now.",
        "cart_empty": "Your cart is empty.",
        "checkout_msg": "Proceed to checkout",
        "choose_language_prompt": "Choose your language:",
        "language_set_success": "Language changed successfully.",
        "change_language_button": "🌐 Change your language",

        # ===== BUTTONS =====
        "btn_choose_films": "Choose films",
        "btn_weekly_films": "This week's films",
        "btn_cart": "🧾 Cart",
        "btn_help": "Help",
        "btn_films": "🎬 Films",
        "btn_my_orders": "📦 My Orders",
        "btn_search_movie": "🔎 Search Movie",
        "btn_invite": "📨 Invite friends",
        "btn_support": "🆘 Support Help",
        "btn_go_home": "⤴️ Go back Home",
        "btn_channel": "📺 Our Channel",
        "btn_add_cart": "➕ Add to Cart",
        "btn_buy_now": "💳 Buy Now"
    },

    "fr": {
        "welcome_shop": "Bienvenue dans la boutique de films:",
        "ask_name": "Bonjour! Que voulez-vous?:",
        "joined_ok": "✔ Vous avez rejoint!",
        "not_joined": "❌ Vous n'avez pas rejoint.",
        "invite_text": "Invitez des amis et gagnez des récompenses!",
        "no_movies": "Aucun film disponible pour l’instant.",
        "cart_empty": "Votre panier est vide.",
        "checkout_msg": "Passer au paiement",
        "choose_language_prompt": "Choisissez votre langue:",
        "language_set_success": "Langue changée avec succès.",
        "change_language_button": "🌐 Changer la langue",

        # BUTTONS
        "btn_choose_films": "Choisir des films",
        "btn_weekly_films": "Films de cette semaine",
        "btn_cart": "🧾 Panier",
        "btn_help": "Aide",
        "btn_films": "🎬 Films",
        "btn_my_orders": "📦 Mes commandes",
        "btn_search_movie": "🔎 Rechercher un film",
        "btn_invite": "📨 Inviter des amis",
        "btn_support": "🆘 Aide",
        "btn_go_home": "⤴️ Retour",
        "btn_channel": "📺 Notre chaîne",
        "btn_add_cart": "➕ Ajouter au panier",
        "btn_buy_now": "💳 Acheter"
    },

    "ig": {
        "welcome_shop": "Nnọọ n’ụlọ ahịa fim:",
        "ask_name": "Ndewo! Gịnị ka ịchọrọ?:",
        "joined_ok": "✔ Ejikọtara gị!",
        "not_joined": "❌ Ị jụbeghị.",
        "invite_text": "Kpọọ enyi ka ha nweta uru!",
        "no_movies": "Enweghị fim ugbu a.",
        "cart_empty": "Ụgbọ gị dị efu.",
        "checkout_msg": "Gaa ịkwụ ụgwọ",
        "choose_language_prompt": "Họrọ asụsụ:",
        "language_set_success": "Asụsụ agbanweela nke ọma.",
        "change_language_button": "🌐 Gbanwee asụsụ",

        # BUTTONS
        "btn_choose_films": "Họrọ fim",
        "btn_weekly_films": "Fim izu a",
        "btn_cart": "🧾 Cart",
        "btn_help": "Nkwado",
        "btn_films": "🎬 Fim",
        "btn_my_orders": "📦 Oru m",
        "btn_search_movie": "🔎 Chọọ fim",
        "btn_invite": "📨 Kpọọ enyi",
        "btn_support": "🆘 Nkwado",
        "btn_go_home": "⤴️ Laghachi",
        "btn_channel": "📺 Channel anyị",
        "btn_add_cart": "➕ Tinye na Cart",
        "btn_buy_now": "💳 Zụta Ugbu a"
    },

    "yo": {
        "welcome_shop": "Kaabo si ile itaja fiimu:",
        "ask_name": "Bawo! Kini o fẹ?:",
        "joined_ok": "✔ Darapọ mọ ikanni!",
        "not_joined": "❌ O kò tíì darapọ.",
        "invite_text": "Pe awọn ọrẹ ki o jèrè ere!",
        "no_movies": "Ko si fiimu lọwọlọwọ.",
        "cart_empty": "Apo rẹ ṣofo.",
        "checkout_msg": "Tẹsiwaju si isanwo",
        "choose_language_prompt": "Yan èdè:",
        "language_set_success": "Èdè ti yipada.",
        "change_language_button": "🌐 Yi èdè pada",

        # BUTTONS
        "btn_choose_films": "Yan fiimu",
        "btn_weekly_films": "Fiimu ọ̀sẹ̀ yìí",
        "btn_cart": "🧾 Cart",
        "btn_help": "Iranwọ",
        "btn_films": "🎬 Fiimu",
        "btn_my_orders": "📦 Awọn aṣẹ mi",
        "btn_search_movie": "🔎 Wa fiimu",
        "btn_invite": "📨 Pe ọ̀rẹ́",
        "btn_support": "🆘 Iranwọ",
        "btn_go_home": "⤴️ Pada",
        "btn_channel": "📺 Ikanni wa",
        "btn_add_cart": "➕ Fi kun Cart",
        "btn_buy_now": "💳 Ra báyìí"
    },

    "ff": {
        "welcome_shop": "A jaɓɓama e suuɗi fim:",
        "ask_name": "Ina! Hol ko yiɗɗa?:",
        "joined_ok": "✔ A seɗɗii e kanal!",
        "not_joined": "❌ A wonaa seɗaako.",
        "invite_text": "Naatu yamiroɓe ngam jeye jukkere!",
        "no_movies": "Fimmuuji alaa oo sahaa.",
        "cart_empty": "Cart maa ko dulli.",
        "checkout_msg": "Yah to nafawngal",
        "choose_language_prompt": "Labo laawol:",
        "language_set_success": "Laawol waylii no haanirta.",
        "change_language_button": "🌐 Waylu laawol",

        # BUTTONS
        "btn_choose_films": "Suɓo fim",
        "btn_weekly_films": "Fimmuuji ndee yontere",
        "btn_cart": "🧾 Cart",
        "btn_help": "Ballal",
        "btn_films": "🎬 Fimmuuji",
        "btn_my_orders": "📦 Noddu maa",
        "btn_search_movie": "🔎 Yiilu fim",
        "btn_invite": "📨 Naatu yamiroɓe",
        "btn_support": "🆘 Ballal",
        "btn_go_home": "⤴️ Rutto galle",
        "btn_channel": "📺 Kanal amen",
        "btn_add_cart": "➕ Ɓeydu Cart",
        "btn_buy_now": "💳 Soodu Jooni"
    }
}

def tr_user(user_id, key, default=None):
    """Translate key for user language, or return default (Hausa original)"""
    lang = get_user_lang(user_id)
    if lang == "ha":
        return default
    return TRANSLATIONS.get(lang, {}).get(key, default)


# ================= REFERRAL HELPERS =================

def add_referral(referrer_id, referred_id):
    try:
        if referrer_id == referred_id:
            return False

        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id
            FROM referrals
            WHERE referrer_id=%s AND referred_id=%s
            """,
            (referrer_id, referred_id)
        )
        exists = cur.fetchone()

        if exists:
            cur.close()
            conn.close()
            return False

        cur.execute(
            """
            INSERT INTO referrals (referrer_id, referred_id)
            VALUES (%s, %s)
            """,
            (referrer_id, referred_id)
        )

        conn.commit()
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print("add_referral error:", e)
        return False


def get_referrer_for(referred_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT referrer_id, reward_granted, id
        FROM referrals
        WHERE referred_id=%s
        ORDER BY id DESC
        LIMIT 1
        """,
        (referred_id,)
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "referrer_id": row[0],
        "reward_granted": row[1],
        "referral_row_id": row[2]
    }


def grant_referral_reward(referral_row_id, referrer_id, amount=200):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT reward_granted
            FROM referrals
            WHERE id=%s
            """,
            (referral_row_id,)
        )
        row = cur.fetchone()

        if not row or row[0]:
            cur.close()
            conn.close()
            return False

        cur.execute(
            """
            INSERT INTO referral_credits (referrer_id, amount, used)
            VALUES (%s, %s, 0)
            """,
            (referrer_id, amount)
        )

        cur.execute(
            """
            UPDATE referrals
            SET reward_granted=1
            WHERE id=%s
            """,
            (referral_row_id,)
        )

        conn.commit()
        cur.close()
        conn.close()

        try:
            bot.send_message(
                referrer_id,
                f"🎉 An ba ka lada N{amount} saboda wanda ka gayyata ya yi sayayya sau 3."
            )
        except:
            pass

        return True

    except Exception as e:
        print("grant_referral_reward error:", e)
        return False


def get_referrals_by_referrer(referrer_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT referred_id, created_at, reward_granted, id
        FROM referrals
        WHERE referrer_id=%s
        ORDER BY id DESC
        """,
        (referrer_id,)
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


def get_credits_for_user(user_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, amount, used, granted_at
        FROM referral_credits
        WHERE referrer_id=%s
        """,
        (user_id,)
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    total_available = sum(r[1] for r in rows if r[2] == 0)
    return total_available, rows


# ================= CORE CHECK (ITEMS BASED) =================

def check_referral_rewards_for_referred(referred_id):
    try:
        ref = get_referrer_for(referred_id)
        if not ref:
            return False

        referrer_id = ref["referrer_id"]
        reward_granted = ref["reward_granted"]
        referral_row_id = ref["referral_row_id"]

        if reward_granted:
            return False

        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(DISTINCT o.id)
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.user_id=%s AND o.paid=1
            """,
            (referred_id,)
        )
        row = cur.fetchone()

        cur.close()
        conn.close()

        count = row[0] if row else 0

        if count >= 3 and check_join(referred_id):
            return grant_referral_reward(referral_row_id, referrer_id, amount=200)

        return False

    except Exception as e:
        print("check_referral_rewards_for_referred error:", e)
        return False


def apply_credits_to_amount(user_id, amount):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, amount
            FROM referral_credits
            WHERE referrer_id=%s AND used=0
            ORDER BY granted_at
            """,
            (user_id,)
        )
        rows = cur.fetchall()

        if not rows:
            cur.close()
            conn.close()
            return amount, 0, []

        remaining = int(amount)
        applied = 0
        applied_ids = []

        for cid, camount in rows:
            if remaining <= 0:
                break

            cur.execute(
                "UPDATE referral_credits SET used=1 WHERE id=%s",
                (cid,)
            )

            applied += camount
            applied_ids.append(cid)
            remaining -= camount

        conn.commit()
        cur.close()
        conn.close()

        if remaining < 0:
            remaining = 0

        return remaining, applied, applied_ids

    except Exception as e:
        print("apply_credits_to_amount error:", e)
        return amount, 0, []



def reply_menu(uid=None):
    kb = InlineKeyboardMarkup()

    # ===== Labels =====
    all_films_label = "🎬 All Films"
    my_orders_label = "🛒 MY=ORDERS"

    invite_label  = tr_user(uid, "btn_invite", default="📨 Invite Friends")
    cart_label    = tr_user(uid, "btn_cart", default="🧾 Cart")
    support_label = tr_user(uid, "btn_support", default="🆘 Support Help")
    channel_label = tr_user(uid, "btn_channel", default="📺 Our Channel")
    home_label    = tr_user(uid, "btn_go_home", default="⤴️ KOMA FARKO")
    change_label  = tr_user(
        uid,
        "change_language_button",
        default="🌐 Change your language"
    )

    # ===== ROW 1: All Films + MY=ORDERS =====
    kb.row(
        InlineKeyboardButton(all_films_label, callback_data="all_films"),
        InlineKeyboardButton(my_orders_label, callback_data="myorders_new")
    )

    # ===== ROW 2 =====
    kb.add(
        InlineKeyboardButton(invite_label, callback_data="invite")
    )

    if uid in ADMINS:

        kb.add(InlineKeyboardButton("☢SERIES&ADD🎬", callback_data="groupitems"))
        kb.add(InlineKeyboardButton("🧹 ERASER", callback_data="eraser_menu"))
        kb.add(InlineKeyboardButton("📂WEAK UPDATE", callback_data="weak_update"))

    kb.add(InlineKeyboardButton(cart_label, callback_data="viewcart"))
    kb.add(InlineKeyboardButton(support_label, callback_data="support_help"))

    # Add a full-width Our Channel row (as in original layout screenshot)
    kb.add(InlineKeyboardButton(channel_label, url=f"https://t.me/{CHANNEL.lstrip('@')}"))

    # Then add a row with Home (KOMA FARKO) and Our Channel side-by-side
    kb.row(
        InlineKeyboardButton(home_label, callback_data="go_home"),
        InlineKeyboardButton(channel_label, url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))

    return kb



def user_main_menu(uid=None):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)

    weekly_films = tr_user(uid, "btn_weekly_films", default="Films din wannan satin")
    cart_label   = tr_user(uid, "btn_cart", default="🧾 Cart")
    help_label   = tr_user(uid, "btn_help", default="Taimako")

    kb.add(KeyboardButton(weekly_films))
    kb.add(KeyboardButton(cart_label), KeyboardButton(help_label))

    return kb


#Start
def movie_buttons_inline(mid, user_id=None):
    kb = InlineKeyboardMarkup()

    add_cart = tr_user(user_id, "btn_add_cart", default="➕ Add to Cart")
    buy_now  = tr_user(user_id, "btn_buy_now", default="💳 Buy Now")
    home_btn = tr_user(user_id, "btn_go_home", default="⤴️ KOMA FARKO")
    channel  = tr_user(user_id, "btn_channel", default="🫂 Our Channel")
    change_l = tr_user(user_id, "change_language_button", default="🌐 Change your language")

    kb.add(
        InlineKeyboardButton(add_cart, callback_data=f"addcartdm:{mid}"),
        InlineKeyboardButton(
            buy_now,
            url=f"https://t.me/{BOT_USERNAME}?start=buyd_{mid}"
        )
    )

    # 🛑 Idan user_id == None → channel ne → kada a ƙara sauran buttons
    if user_id is None:
        return kb

    # 🔰 Idan private chat ne → saka sauran buttons
    kb.row(
        InlineKeyboardButton(home_btn, callback_data="go_home"),
        InlineKeyboardButton(channel, url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    kb.row(InlineKeyboardButton(change_l, callback_data="change_language"))

    return kb
#END

# ========== START ==========
@bot.message_handler(commands=["start"])
def start(message):
    uid = message.from_user.id
    fname = message.from_user.first_name or ""
    uname = f"@{message.from_user.username}" if message.from_user.username else "Babu username"
    text = (message.text or "").strip()

    # ========= REF =========
    param = None
    if text.startswith("/start "):
        param = text.split(" ", 1)[1].strip()
    elif text.startswith("/start"):
        parts = text.split(" ", 1)
        if len(parts) > 1:
            param = parts[1].strip()

    if param and param.startswith("ref"):
        try:
            ref_id = int(param[3:])
            add_referral(ref_id, uid)
            try:
                bot.send_message(
                    ref_id,
                    f"Someone used your invite link! ID: <code>{uid}</code>",
                    parse_mode="HTML"
                )
            except:
                pass
        except:
            pass

    # ========= ADMIN NOTIFY =========
    try:
        bot.send_message(
            ADMIN_ID,
            f"🟢 SABON VISITOR!\n\n"
            f"👤 Sunan: <b>{fname}</b>\n"
            f"🔗 Username: {uname}\n"
            f"🆔 ID: <code>{uid}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        print("Failed to notify admin about visitor:", e)

    # ========= JOIN CHECK =========
    joined = check_join(uid)



    # ❌ IDAN BAI SHIGA BA
    if not joined:
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton(
                "Join Channel",
                url=f"https://t.me/{CHANNEL.lstrip('@')}"
            )
        )
        kb.add(
            InlineKeyboardButton(
                "I've Joined✅",
                callback_data="checkjoin"
            )
        )
        bot.send_message(
            uid,
            "⚠️ Don cigaba, sai ka shiga channel ɗin mu.",
            reply_markup=kb
        )
        return

    # ========= MENUS =========
    bot.send_message(
        uid,
        "Abokin kasuwanci barka da zuwa shagon fina finai:",
        reply_markup=user_main_menu(uid)
    )
    bot.send_message(
        uid,
        "Sannu da zuwa!\n Me kake bukata?:",
        reply_markup=reply_menu(uid)
    )

# ========== get group id & misc handlers ==========
@bot.message_handler(commands=["getgroupid"])
def getgroupid(message):
    chat = message.chat
    if chat.type in ("group", "supergroup", "channel"):
        bot.reply_to(message, f"Chat title: {chat.title}\nChat id: <code>{chat.id}</code>", parse_mode="HTML")
    else:
        bot.reply_to(message,
                     "Don samun group id: ƙara bot ɗin zuwa group ɗin, sannan a rubita /getgroupid a cikin group. Ko kuma ka forward wani message daga group zuwa nan (DM) kuma zan nuna original chat id idan forwarded.")


@bot.message_handler(
    func=lambda msg: isinstance(getattr(msg, "text", None), str)
    and msg.text in ["Films din wannan satin", "Taimako", "🧾 Cart"]
)
def user_buttons(message):
    txt = message.text
    uid = message.from_user.id

    if txt == "Films din wannan satin":
        try:
            send_weekly_list(message)
        except Exception as e:
            print("Films din wannan satin ERROR:", e)
            bot.send_message(
                message.chat.id,
                "⚠️ An samu matsala wajen nuna fina-finan wannan satin."
            )
        return
# ======= TAIMAKO =======                
    if txt == "Taimako":                
        kb = InlineKeyboardMarkup()                

        # ALWAYS open admin DM directly – no callback, no message sending
        if ADMIN_USERNAME:                
            kb.add(InlineKeyboardButton("Contact Admin", url=f"https://t.me/{ADMIN_USERNAME}"))                
        else:                
            kb.add(InlineKeyboardButton("🆘 Support Help", url="https://t.me/{}".format(ADMIN_USERNAME)))                

        bot.send_message(                
            message.chat.id,                
            "Idan kana bukatar taimako, Yi magana da admin.",                
            reply_markup=kb                
        )                
        return            

    # ======= CART =======            
    if txt == "🧾 Cart":            
        show_cart(message.chat.id, message.from_user.id)            
        return

# ================== FINAL ISOLATED ERASER SYSTEM ==================

import os, json, random, time, re
from datetime import datetime, timedelta
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

ERASER_BACKUP_FOLDER = "eraser_backups"
ERASER_PASSWORD_DEFAULT = "E66337"
ERASER_OTP_TTL = 120
ERASER_MAX_RESEND = 3
ERASER_RESEND_COOLDOWN = 30
ERASER_BACKUP_TTL_DAYS = 30

os.makedirs(ERASER_BACKUP_FOLDER, exist_ok=True)

# ================= DATABASE (POSTGRES) =================
try:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS eraser_settings(
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS eraser_backups(
            id SERIAL PRIMARY KEY,
            filename TEXT,
            created_at TIMESTAMP
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
except Exception as e:
    print("ERASER DB INIT ERROR:", e)

# ================= HELPERS =================
def eraser_reset_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔑 Reset Password", callback_data="eraser_forgot"))
    kb.add(InlineKeyboardButton("✖ Cancel", callback_data="eraser_cancel"))
    return kb

# ================= PASSWORD =================
def _eraser_get_password():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT value FROM eraser_settings WHERE key=%s",
        ("eraser_password",)
    )
    r = cur.fetchone()

    if r and r[0]:
        cur.close()
        conn.close()
        return r[0]

    cur.execute(
        """
        INSERT INTO eraser_settings(key,value)
        VALUES(%s,%s)
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """,
        ("eraser_password", ERASER_PASSWORD_DEFAULT)
    )
    conn.commit()
    cur.close()
    conn.close()
    return ERASER_PASSWORD_DEFAULT


def _eraser_set_password(p):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO eraser_settings(key,value)
        VALUES(%s,%s)
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """,
        ("eraser_password", p)
    )

    conn.commit()
    cur.close()
    conn.close()


def _eraser_password_valid(p):
    return bool(re.fullmatch(r"\d{5}[A-Z]", p))

# ================= OTP =================
_eraser_otp = {}
_eraser_meta = {}

def _eraser_gen_otp():
    return str(random.randint(100000, 999999))


def _eraser_send_otp(uid, resend=False):
    now = time.time()
    meta = _eraser_meta.get(uid, {})

    if resend:
        if meta.get("resends", 0) >= ERASER_MAX_RESEND:
            return False, "OTP resend limit reached."
        if now - meta.get("last", 0) < ERASER_RESEND_COOLDOWN:
            return False, "Wait before resending OTP."

    otp = _eraser_gen_otp()
    _eraser_otp[uid] = {"otp": otp, "expires": now + ERASER_OTP_TTL}
    _eraser_meta[uid] = {
        "resends": meta.get("resends", 0) + (1 if resend else 0),
        "last": now
    }

    bot.send_message(OTP_ADMIN_ID, f"🔐 ERASER OTP for admin {uid}: {otp}")
    return True, None


def _eraser_otp_expired(uid):
    return uid not in _eraser_otp or time.time() > _eraser_otp[uid]["expires"]

# ================= BACKUP =================
def _eraser_create_backup():
    now = datetime.utcnow()
    ts = now.strftime("%Y%m%d%H%M%S")
    fname = f"eraser_backup_{ts}.json"
    path = os.path.join(ERASER_BACKUP_FOLDER, fname)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname='public'
        """
    )
    tables = [r[0] for r in cur.fetchall()]

    data = {}

    for t in tables:
        if t in ("eraser_settings", "eraser_backups"):
            continue

        cur.execute(f'SELECT * FROM "{t}"')
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description] if rows else []
        data[t] = [dict(zip(cols, r)) for r in rows]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    cur.execute(
        """
        INSERT INTO eraser_backups(filename,created_at)
        VALUES(%s,%s)
        """,
        (fname, now)
    )

    conn.commit()
    cur.close()
    conn.close()
    return path

# ================= CALLBACK =================
@bot.callback_query_handler(func=lambda c: c.data.startswith("eraser_"))
def eraser_cb(c):
    uid = c.from_user.id
    data = c.data
    bot.answer_callback_query(c.id)

    if uid != ADMIN_ID:
        return

    if data == "eraser_menu":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✔ Yes – Erase", callback_data="eraser_yes"))
        kb.add(InlineKeyboardButton("📦 BACKUP", callback_data="eraser_backup"))
        kb.add(InlineKeyboardButton("♻ RESTORE DATA", callback_data="eraser_restore"))
        kb.add(InlineKeyboardButton("🔑 FORGET PASSWORD", callback_data="eraser_forgot"))
        kb.add(InlineKeyboardButton("✖ Cancel", callback_data="eraser_cancel"))
        bot.send_message(uid, "🧹 ERASER SYSTEM", reply_markup=kb)

    elif data == "eraser_cancel":
        admin_states.pop(uid, None)
        bot.send_message(uid, "Cancelled.", reply_markup=reply_menu(uid))

    elif data == "eraser_backup":
        admin_states[uid] = {"state": "eraser_backup_pass"}
        bot.send_message(uid, "Enter ERASER password:")

    elif data == "eraser_yes":
        admin_states[uid] = {"state": "eraser_erase_pass"}
        bot.send_message(uid, "Enter ERASER password:")

    elif data == "eraser_restore":
        admin_states[uid] = {"state": "eraser_restore_pass"}
        bot.send_message(uid, "Enter ERASER password:")

    elif data == "eraser_forgot":
        _eraser_send_otp(uid)
        admin_states[uid] = {"state": "eraser_wait_otp"}
        bot.send_message(uid, "OTP sent. Enter OTP:")

# ================= TEXT =================
@bot.message_handler(
    func=lambda m: m.from_user.id == ADMIN_ID
    and admin_states.get(m.from_user.id, {}).get("state", "").startswith("eraser_")
)
def eraser_text(m):
    uid = m.from_user.id
    text = m.text.strip()
    st = admin_states[uid]["state"]

    # ---- BACKUP PASS ----
    if st == "eraser_backup_pass":
        if text != _eraser_get_password():
            bot.send_message(uid, "❌ Wrong password.", reply_markup=eraser_reset_kb())
            return

        path = _eraser_create_backup()
        admin_states.pop(uid)
        bot.send_message(uid, f"✔ Backup created:\n{path}")

    # ---- ERASE PASS ----
    elif st == "eraser_erase_pass":
        if text != _eraser_get_password():
            bot.send_message(uid, "❌ Wrong password.", reply_markup=eraser_reset_kb())
            return

        _eraser_create_backup()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname='public'
        """)
        tables = [r[0] for r in cur.fetchall()]

        for t in tables:
            if t not in ("eraser_settings", "eraser_backups"):
                try:
                    cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE')
                except Exception:
                    pass

        conn.commit()
        cur.close()
        conn.close()

        admin_states.pop(uid)
        bot.send_message(uid, "🧹 ERASE COMPLETE.")

    # ---- RESTORE PASS ----
    elif st == "eraser_restore_pass":
        if text != _eraser_get_password():
            bot.send_message(uid, "❌ Wrong password.", reply_markup=eraser_reset_kb())
            return

        ok, info = _eraser_auto_restore_latest()
        admin_states.pop(uid, None)

        if ok:
            bot.send_message(
                uid,
                f"♻ <b>RESTORE COMPLETE</b>\n\n📦 Backup: <code>{info}</code>",
                parse_mode="HTML"
            )
        else:
            bot.send_message(
                uid,
                f"❌ <b>Restore failed</b>\n\n{info}",
                parse_mode="HTML"
            )

    # ---- OTP ----
    elif st == "eraser_wait_otp":
        if _eraser_otp_expired(uid):
            bot.send_message(uid, "OTP expired.")
            return

        if text != _eraser_otp[uid]["otp"]:
            bot.send_message(uid, "❌ OTP ba daidai ba. Tambayi admin mai karɓa.")
            return

        admin_states[uid] = {"state": "eraser_new_pass"}
        bot.send_message(uid, "Enter new password:")

    elif st == "eraser_new_pass":
        if not _eraser_password_valid(text):
            bot.send_message(uid, "Invalid format. Example: 66788K")
            return

        admin_states[uid] = {"state": "eraser_confirm_pass", "tmp": text}
        bot.send_message(uid, "Confirm password:")

    elif st == "eraser_confirm_pass":
        if text != admin_states[uid]["tmp"]:
            bot.send_message(uid, "Passwords do not match.")
            return

        _eraser_set_password(text)
        admin_states.pop(uid)
        bot.send_message(uid, "✅ Password changed successfully.")


# ================= AUTO MERGE RESTORE (ADD-ON ONLY) =================
def _eraser_auto_restore_latest():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT filename FROM eraser_backups ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return False, "No backup found."

    fname = row[0]
    path = os.path.join(ERASER_BACKUP_FOLDER, fname)

    if not os.path.exists(path):
        cur.close()
        conn.close()
        return False, "Backup file missing."

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for table, rows in data.items():
        if not rows:
            continue

        cols = list(rows[0].keys())
        colnames = ",".join(f'"{c}"' for c in cols)
        placeholders = ",".join(["%s"] * len(cols))

        for r in rows:
            values = [r[c] for c in cols]
            try:
                cur.execute(
                    f"""
                    INSERT INTO "{table}" ({colnames})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                    """,
                    values
                )
            except Exception:
                pass

    conn.commit()
    cur.close()
    conn.close()
    return True, fname


# ================= END ERASER SYSTEM =================
def clear_cart(uid):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM cart WHERE user_id=%s",
        (uid,)
    )

    conn.commit()
    cur.close()
    conn.close()


def get_cart(uid):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            c.item_id,
            i.title,
            i.price,
            i.file_id
        FROM cart c
        JOIN items i ON i.id = c.item_id
        WHERE c.user_id=%s
        ORDER BY c.id DESC
    """, (uid,))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
# ======================================
# PARSE CAPTION (TITLE + PRICE)
# ======================================
def parse_caption_for_title_price(text):
    if not text:
        return None, None

    text = text.replace("₦", "").strip()

    m = re.match(r"^(.*?)[\s\-]+(\d+)$", text)
    if m:
        return m.group(1).strip(), int(m.group(2))

    parts = text.splitlines()
    if len(parts) >= 2 and parts[1].strip().isdigit():
        return parts[0].strip(), int(parts[1].strip())

    return None, None






@bot.message_handler(
    func=lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in admin_states
)
def admin_inputs(message):
    try:
        state_entry = admin_states.get(message.from_user.id)
        if not state_entry:
            return

        state = state_entry.get("state")

        # ⚠️ NOTE:
        # An cire ADD MOVIE logic, amma sauran admin states
        # (weak_update, update_week, da sauransu)
        # suna nan a sauran code ɗinka

        return

    except Exception as e:
        print("ADMIN INPUT ERROR:", e)
        return




    # ========== CANCEL ==========
@bot.message_handler(commands=["cancel"])
def cancel_cmd(message):
    if message.from_user.id == ADMIN_ID and admin_states.get(ADMIN_ID) and admin_states[ADMIN_ID].get("state") in ("weak_update", "update_week"):
        inst = admin_states[ADMIN_ID]
        inst_msg_id = inst.get("inst_msg_id")
        if inst_msg_id:
            try:
                bot.delete_message(chat_id=ADMIN_ID, message_id=inst_msg_id)
            except Exception as e:
                print("Failed to delete instruction message on cancel:", e)
        admin_states.pop(ADMIN_ID, None)
        bot.reply_to(message, "An soke Update/Weak update kuma an goge sakon instruction.")
        return

    if message.from_user.id == ADMIN_ID and admin_states.get(ADMIN_ID):
        admin_states.pop(ADMIN_ID, None)
        bot.reply_to(message, "An soke aikin admin na yanzu.")
        return

# ==================================================
# ========== GET CART (GROUP-AWARE SAFE) ============
# ==================================================
def get_cart(uid):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            c.item_id,          -- movie_id
            i.title,            -- title
            i.price,            -- price (GROUP price)
            i.file_id,          -- file_id
            i.group_key         -- GROUP KEY
        FROM cart c
        JOIN items i ON i.id = c.item_id
        WHERE c.user_id = %s
        """,
        (uid,)
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
# ========== BUILD CART VIEW (GROUP-AWARE - FIXED) ==========
def build_cart_view(uid):
    rows = get_cart(uid)

    kb = InlineKeyboardMarkup()

    # ===== IDAN CART BABU KOMAI =====
    if not rows:
        text = "🛒 <b>Cart ɗinka babu komai.</b>"

        kb.row(
            InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
            InlineKeyboardButton(
                "🫂 Our Channel",
                url=f"https://t.me/{CHANNEL.lstrip('@')}"
            )
        )
        return text, kb

    total = 0
    lines = []

    # ===============================
    # HADA ITEMS TA GROUP_KEY
    # ===============================
    grouped = {}

    for movie_id, title, price, file_id, group_key in rows:
        key = group_key or f"single_{movie_id}"

        if key not in grouped:
            grouped[key] = {
                "ids": [],
                "title": title or "📦 Group / Series Item",
                "price": int(price or 0)
            }

        grouped[key]["ids"].append(movie_id)

    # ===============================
    # DISPLAY ITEMS
    # ===============================
    for g in grouped.values():
        ids = g["ids"]
        title = g["title"]
        price = g["price"]

        total += price

        lines.append(f"🎬 {title} — ₦{price}")

        ids_str = "_".join(str(i) for i in ids)

        kb.add(
            InlineKeyboardButton(
                f"❌ Cire: {title}",
                callback_data=f"removecart:{ids_str}"
            )
        )

    # ===== TOTAL =====
    lines.append("")
    lines.append(f"<b>Jimilla:</b> ₦{total}")

    text = (
        "🛒 <b>YOUR CART / fina-finai da ka zaba domin siya</b>\n\n"
        + "\n".join(lines)
    )

    # ===== ACTION BUTTONS =====
    kb.add(
        InlineKeyboardButton("🧹 Clear Cart", callback_data="clearcart")
    )
    kb.add(
        InlineKeyboardButton("💵 CHECKOUT", callback_data="checkout")
    )

    # ===== NAV BUTTONS =====
    kb.row(
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
        InlineKeyboardButton(
            "🫂 Our Channel",
            url=f"https://t.me/{CHANNEL.lstrip('@')}"
        )
    )

    return text, kb

# ================= ADMIN ON / OFF =================
@bot.message_handler(commands=["on"])
def admin_on(m):
    if m.chat.type != "private" or m.from_user.id != ADMIN_ID:
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO admin_controls (admin_id, sendmovie_enabled)
        VALUES (%s, 1)
        ON CONFLICT (admin_id)
        DO UPDATE SET sendmovie_enabled = EXCLUDED.sendmovie_enabled
        """,
        (ADMIN_ID,)
    )

    conn.commit()
    cur.close()
    conn.close()

    bot.reply_to(m, "✅ An kunna SENDMOVIE / GETID")


@bot.message_handler(commands=["off"])
def admin_off(m):
    if m.chat.type != "private" or m.from_user.id != ADMIN_ID:
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO admin_controls (admin_id, sendmovie_enabled)
        VALUES (%s, 0)
        ON CONFLICT (admin_id)
        DO UPDATE SET sendmovie_enabled = EXCLUDED.sendmovie_enabled
        """,
        (ADMIN_ID,)
    )

    conn.commit()
    cur.close()
    conn.close()

    bot.reply_to(m, "⛔ An kashe SENDMOVIE / GETID")


def admin_feature_enabled():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT sendmovie_enabled FROM admin_controls WHERE admin_id=%s",
        (ADMIN_ID,)
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    return row and row[0] == 1


# ================= GETID (FILE_NAME SEARCH) =================
@bot.message_handler(commands=["getid"])
def getid_command(message):
    # 🔒 TSARO: admin + sai an kunna
    if message.from_user.id != ADMIN_ID:
        return
    if not admin_feature_enabled():
        return

    text = message.text or ""
    parts = text.split(" ", 1)

    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(
            message,
            "Amfani: /getid Sunan item\nMisali: /getid Wutar jeji"
        )
        return

    query = parts[1].strip()

    conn = get_conn()
    cur = conn.cursor()

    # ====== EXACT MATCH (PRIORITY) ======
    cur.execute(
        """
        SELECT id, title
        FROM items
        WHERE LOWER(title) = LOWER(%s)
        LIMIT 1
        """,
        (query,)
    )
    row = cur.fetchone()

    if row:
        bot.reply_to(
            message,
            f"Kamar yadda ka bukata ga ID ɗin fim din <b>{row[1]}</b>: <code>{row[0]}</code>",
            parse_mode="HTML"
        )
        cur.close()
        conn.close()
        return

    # ====== CONTAINS MATCH ======
    cur.execute(
        """
        SELECT id, title
        FROM items
        WHERE LOWER(title) LIKE LOWER(%s)
        ORDER BY title ASC
        LIMIT 10
        """,
        (f"%{query}%",)
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    # ====== BABU KOMAI ======
    if not rows:
        bot.reply_to(
            message,
            "❌ Ban samu fim da kake nema ba."
        )
        return

    # ====== MATCH 1 ======
    if len(rows) == 1:
        r = rows[0]
        bot.reply_to(
            message,
            f"Kamar yadda ka bukata ga ID ɗin fim din da kake nema <b>{r[1]}</b>: <code>{r[0]}</code>",
            parse_mode="HTML"
        )
        return




    # ====== MATCH DAYA FIYE ======
    text_out = "An samu fina-finai masu kama:\n"
    for r in rows:
        text_out += f"• {r['title']} — ID: {r['id']}\n"

    bot.reply_to(message, text_out)


# ================= SENDMOVIE (ID / GROUP_KEY / NAME) =================
@bot.message_handler(commands=["sendmovie"])
def sendmovie_cmd(m):
    if m.from_user.id != ADMIN_ID:
        return
    if not admin_feature_enabled():
        return

    parts = m.text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(
            m,
            "Amfani:\n"
            "/sendmovie 20\n"
            "/sendmovie 1,2,3,7\n"
            "/sendmovie karn tsaye S1\n"
            "/sendmovie avatar"
        )
        return

    raw = parts[1].strip()

    # ===============================
    # MODE 1: ID MODE
    # ===============================
    ids = []
    for x in raw.replace(" ", "").split(","):
        if x.isdigit():
            ids.append(int(x))

    rows = []

    conn = get_conn()
    cur = conn.cursor()

    if ids:
        # ===== FETCH BY IDS =====
        for item_id in ids:
            cur.execute(
                """
                SELECT file_id, title
                FROM items
                WHERE id = %s
                """,
                (item_id,)
            )
            row = cur.fetchone()
            if row:
                rows.append(row)

        # ===== NOT FOUND IDS =====
        cur.execute(
            f"""
            SELECT id
            FROM items
            WHERE id IN ({",".join(["%s"] * len(ids))})
            """,
            ids
        )
        found_ids = [r[0] for r in cur.fetchall()]
        not_found_ids = [str(i) for i in ids if i not in found_ids]

    else:
        # ===============================
        # MODE 2: GROUP_KEY / NAME MODE
        # ===============================
        q = raw.lower()

        # 🔹 1) GROUP_KEY
        cur.execute(
            """
            SELECT file_id, title
            FROM items
            WHERE LOWER(group_key) = %s
            ORDER BY id ASC
            """,
            (q,)
        )
        rows = cur.fetchall()

        # 🔹 2) TITLE / FILE_NAME (fallback)
        if not rows:
            cur.execute(
                """
                SELECT file_id, title
                FROM items
                WHERE LOWER(title) LIKE %s
                   OR LOWER(file_name) LIKE %s
                ORDER BY title ASC
                """,
                (f"%{q}%", f"%{q}%")
            )
            rows = cur.fetchall()

        not_found_ids = []

    cur.close()
    conn.close()

    # ===============================
    # NOTHING FOUND
    # ===============================
    if not rows:
        bot.reply_to(
            m,
            "❌ Ban samu fim ko group ɗin da ka nema ba."
        )
        return

    # ===============================
    # SEND FILES
    # ===============================
    sent = 0

    for file_id, title in rows:
        try:
            try:
                bot.send_video(
                    m.chat.id,
                    file_id,
                    caption=f"🎬 {title}"
                )
            except:
                bot.send_document(
                    m.chat.id,
                    file_id,
                    caption=f"🎬 {title}"
                )
            sent += 1
        except Exception as e:
            print("sendmovie error:", e)

    # ===============================
    # REPORT
    # ===============================
    report = f"✅ An tura fina-finai: {sent}"

    if not_found_ids:
        report += (
            "\n\n❌ Ba a samu waɗannan IDs ba:\n"
            + ", ".join(not_found_ids)
        )

    bot.reply_to(m, report)
    # ================= USER RESEND SEARCH (USING user_movies) =================

@bot.message_handler(
    func=lambda m: m.from_user.id in admin_states
    and admin_states.get(m.from_user.id, {}).get("state") in (
        "search_menu",
        "browse_menu",
        "series_menu",
        "search_trending",
    )
)
def ignore_unexpected_text(m):
    uid = m.from_user.id
    bot.send_message(
        uid,
        "ℹ️ Don Allah ka yi amfani da *buttons* da ke ƙasa.",
        parse_mode="Markdown"
    )
# ======================================================
# ACTIVE BUYERS (ADMIN ONLY | PAGINATION | EDIT MODE)
# ======================================================

# ================== END RUKUNI B ==================

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("cancel:"))
def cancel_order_handler(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    try:
        order_id = c.data.split("cancel:", 1)[1]
    except:
        return

    conn = get_conn()
    cur = conn.cursor()

    # 🔎 Tabbatar order na wannan user ne kuma unpaid
    cur.execute(
        """
        SELECT id
        FROM orders
        WHERE id = %s AND user_id = %s AND paid = 0
        """,
        (order_id, uid)
    )
    order = cur.fetchone()

    if not order:
        cur.close()
        conn.close()
        bot.send_message(
            uid,
            "❌ <b>Ba a sami order ba ko kuma an riga an biya shi.</b>",
            parse_mode="HTML"
        )
        return

    # 🧹 Goge order_items
    cur.execute(
        "DELETE FROM order_items WHERE order_id = %s",
        (order_id,)
    )

    # 🧹 Goge order
    cur.execute(
        "DELETE FROM orders WHERE id = %s",
        (order_id,)
    )

    conn.commit()
    cur.close()
    conn.close()

    bot.send_message(
        uid,
        "❌ <b>An soke wannan order ɗin.</b>",
        parse_mode="HTML"
    )

# --- Added callback handler for in-bot "View All Movies" buttons ---
@bot.callback_query_handler(func=lambda c: c.data in ("view_all_movies","viewall"))
def _callback_view_all(call):
    uid = call.from_user.id
    # Build a small message-like object expected by send_weekly_list
    class _Msg:
        def __init__(self, uid):
            self.chat = type('X', (), {'id': uid})
            self.text = ""
    try:
        send_weekly_list(_Msg(uid))
        bot.answer_callback_query(call.id)
    except Exception as e:
        bot.answer_callback_query(call.id, "An samu matsala wajen nuna jerin.")


@bot.message_handler(
    func=lambda m: user_states.get(m.from_user.id, {}).get("action") == "_resend_search_"
)
def handle_resend_search_text(m):
    uid = m.from_user.id
    query = m.text.strip()

    # 1️⃣ Tabbatar da rubutu
    if len(query) < 2:
        bot.send_message(
            uid,
            "❌ Rubuta akalla haruffa 2 ko fiye.\nMisali: damisa, mash, mai"
        )
        return

    conn = get_conn()
    cur = conn.cursor()

    # 2️⃣ DUBA KO USER YA TABA SAMUN DELIVERY
    cur.execute(
        "SELECT COUNT(*) FROM user_movies WHERE user_id = %s",
        (uid,)
    )
    total_owned = cur.fetchone()[0]

    if total_owned == 0:
        user_states.pop(uid, None)
        cur.close()
        conn.close()
        bot.send_message(
            uid,
            "❌ <b>Baka taɓa siyan wani fim ba.</b>\n"
            "Je ka siya daga bangaren siyayya.",
            parse_mode="HTML"
        )
        return

    # 3️⃣ DUBA IYAKAR SAKE TURAWA
    cur.execute(
        "SELECT COUNT(*) FROM resend_logs WHERE user_id = %s",
        (uid,)
    )
    used = cur.fetchone()[0]

    if used >= 10:
        user_states.pop(uid, None)
        cur.close()
        conn.close()
        bot.send_message(
            uid,
            "⚠️ Ka kai iyakar sake karɓa (sau 10).\n"
            "Sai ka sake siya."
        )
        return

    # 4️⃣ NEMO ITEMS DA USER YA MALLAKA (SINGLE + GROUP KEY)
    cur.execute(
        """
        SELECT
            i.id        AS item_id,
            i.title     AS title,
            i.group_key AS group_key
        FROM user_movies um
        JOIN items i ON i.id = um.item_id
        WHERE um.user_id = %s
          AND i.title ILIKE %s
        ORDER BY i.title ASC
        """,
        (uid, f"%{query}%")
    )
    rows = cur.fetchall()

    # 5️⃣ IDAN BABU MATCH → CI GABA DA JIRA
    if not rows:
        cur.close()
        conn.close()
        bot.send_message(
            uid,
            "❌ Babu fim da wannan suna cikin fina-finai da ka taba siya.\n\n"
            "Sake gwada wani suna.\nIna jiranka… 😊"
        )
        return  # ⚠️ KAR A CIRE STATE

    # 6️⃣ GROUP KEY LOGIC (NUNA SUNA 1 KACAL)
    user_states.pop(uid, None)

    kb = InlineKeyboardMarkup()
    shown_groups = set()

    for item_id, title, group_key in rows:
        if group_key:
            if group_key in shown_groups:
                continue
            shown_groups.add(group_key)

            kb.add(
                InlineKeyboardButton(
                    title,
                    callback_data=f"resend_group:{group_key}"
                )
            )
        else:
            kb.add(
                InlineKeyboardButton(
                    title,
                    callback_data=f"resend_one:{item_id}"
                )
            )

    cur.close()
    conn.close()

    bot.send_message(
        uid,
        "🎬 <b>An samu fina-finai:</b>\n"
        "Danna suna domin a sake turo maka:",
        parse_mode="HTML",
        reply_markup=kb
    )


# ========== detect forwarded channel post ==========
@bot.message_handler(func=lambda m: getattr(m, "forward_from_chat", None) is not None or getattr(m, "forward_from_message_id", None) is not None)
def handle_forwarded_post(m):
    fc = getattr(m, "forward_from_chat", None)
    fid = getattr(m, "forward_from_message_id", None)
    if not fc and not fid:
        return
    try:
        chat_info = ""
        if fc:
            if getattr(fc, "username", None):
                chat_info = f"@{fc.username}"
            else:
                chat_info = f"chat_id:{fc.id}"
        else:
            chat_info = "Unknown channel"
        if fid:
            bot.reply_to(m, f"Original channel: {chat_info}\nOriginal message id: {fid}")
        else:
            bot.reply_to(m, f"Original channel: {chat_info}\nMessage id not found.")
    except Exception as e:
        print("forward handler error:", e)


# ========== show_cart ==========
def show_cart(chat_id, user_id):
    rows = get_cart(user_id)

    if not rows:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
            InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")
        )
        change_label = tr_user(user_id, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        s = tr_user(user_id, "cart_empty", default="🧾 Cart ɗinka babu komai.")
        bot.send_message(chat_id, s, reply_markup=kb)
        return

    text_lines = ["🧾 Kayayyakin da ka zaba:"]
    kb = InlineKeyboardMarkup()

    total = 0  # ✅ total ɗaya kacal

    # ===============================
    # HADA ITEMS TA GROUP_KEY
    # ===============================
    grouped = {}

    for movie_id, title, price, file_id, group_key in rows:
        key = group_key or f"single_{movie_id}"

        if key not in grouped:
            grouped[key] = {
                "ids": [],
                "title": title or "📦 Group / Series Item",
                "price": int(price or 0)
            }

        grouped[key]["ids"].append(movie_id)

    # ===============================
    # DISPLAY (SINGLE + GROUP)
    # ===============================
    for g in grouped.values():
        ids = g["ids"]
        title = g["title"]
        price = g["price"]

        total += price  # ✅ ba ya ninkawa

        if price == 0:
            text_lines.append(f"• {title} — 📦 Series")
        else:
            text_lines.append(f"• {title} — ₦{price}")

        ids_str = "_".join(str(i) for i in ids)

        kb.add(
            InlineKeyboardButton(
                f"❌ Remove: {title[:18]}",
                callback_data=f"removecart:{ids_str}"
            )
        )

    text_lines.append(f"\nJimillar: ₦{total}")

    # ===============================
    # CREDIT INFO (KAMAR YADDA YAKE)
    # ===============================
    total_available, credit_rows = get_credits_for_user(user_id)
    credit_info = ""
    if total_available > 0:
        credit_info = (
            f"\n\nNote: Available referral credit: N{total_available}. "
            f"It will be automatically applied at checkout."
        )

    # ===============================
    # ACTION BUTTONS
    # ===============================
    kb.add(
        InlineKeyboardButton("🧹 Clear Cart", callback_data="clearcart"),
        InlineKeyboardButton("💵 Checkout", callback_data="checkout")
    )

    kb.row(
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"),
        InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")
    )

    change_label = tr_user(user_id, "change_language_button", default="🌐 Change your language")
    kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))

    bot.send_message(
        chat_id,
        "\n".join(text_lines) + credit_info,
        reply_markup=kb
    )

# ====================== WEAK UPDATE (BULK WEEKLY) ======================
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import re
from datetime import datetime
import json

weak_update_temp = {}

# ---------- FLEXIBLE TITLE + PRICE PARSER ----------
def parse_title_price_block(text_block):
    out = []
    lines = (text_block or "").splitlines()
    pending_title = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if re.fullmatch(r"[-:]?\s*(?:₦|N)?\s*\d+", line) and pending_title:
            price = int(re.sub(r"\D", "", line))
            out.append({"title": pending_title, "price": price})
            pending_title = None
            continue

        m = re.match(
            r"^(?P<title>.+?)(?:\s*[–\-:]\s*|\s+)(?:₦|N)?\s*(?P<price>\d+)$",
            line
        )
        if m:
            out.append({
                "title": m.group("title").strip(),
                "price": int(m.group("price"))
            })
            pending_title = None
            continue

        pending_title = line

    return out


# ---------- SMART MATCH ----------
def find_best_match(title, candidates):
    t = (title or "").lower().strip()
    if not t:
        return None

    first = t.split()[0]
    matches = []

    for i, c in enumerate(candidates):
        fn = (c.get("file_name") or "").lower()
        if t in fn or (first and first in fn):
            matches.append(i)

    if len(matches) == 1:
        return matches[0]
    return None


# ---------- START ----------
@bot.callback_query_handler(func=lambda c: c.data == "weak_update")
def start_weak_update(call):
    uid = call.from_user.id
    weak_update_temp[uid] = {
        "stage": "collect_files",
        "movies": [],
        "poster": None,
        "caption": None
    }
    bot.answer_callback_query(call.id)
    bot.send_message(uid, "Turo fina-finai yanzu. Idan ka gama danna YES.")


# ---------- COLLECT FILES ----------
@bot.message_handler(
    func=lambda m: m.from_user.id in weak_update_temp
    and weak_update_temp[m.from_user.id]["stage"] == "collect_files",
    content_types=['video','document','audio','animation','photo']
)
def collect_files(msg):
    uid = msg.from_user.id
    temp = weak_update_temp[uid]

    if msg.document:
        fname = msg.document.file_name
    elif msg.video:
        fname = msg.video.file_name
    elif msg.audio:
        fname = msg.audio.file_name
    elif msg.animation:
        fname = msg.animation.file_name
    else:
        fname = f"photo_{msg.message_id}"

    temp["movies"].append({
        "chat_id": msg.chat.id,
        "message_id": msg.message_id,
        "file_name": fname
    })

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("YES, Na gama", callback_data="weak_files_done"))
    kb.add(InlineKeyboardButton("NO, Zan ci gaba", callback_data="weak_more_files"))

    bot.send_message(uid, "Ka gama?", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "weak_more_files")
def weak_more(call):
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "weak_files_done")
def weak_files_done(call):
    uid = call.from_user.id
    weak_update_temp[uid]["stage"] = "poster"
    bot.answer_callback_query(call.id)
    bot.send_message(uid, "Yanzu turo POSTER (photo + caption).")


# ---------- COLLECT POSTER ----------
@bot.message_handler(
    func=lambda m: m.from_user.id in weak_update_temp
    and weak_update_temp[m.from_user.id]["stage"] == "poster",
    content_types=['photo']
)
def collect_poster(msg):
    uid = msg.from_user.id
    temp = weak_update_temp[uid]

    temp["poster"] = msg.photo[-1].file_id
    temp["caption"] = msg.caption or ""

    process_weak_finalize(uid)


# ---------- FINALIZE ----------
def process_weak_finalize(uid):
    temp = weak_update_temp.get(uid)
    if not temp:
        return

    parsed = parse_title_price_block(temp["caption"])
    if not parsed:
        bot.send_message(uid, "❌ FORMAT ERROR")
        return

    stored_files = []

    for mv in temp["movies"]:
        bot.forward_message(STORAGE_CHANNEL, mv["chat_id"], mv["message_id"])
        debug_msg = bot.forward_message(uid, mv["chat_id"], mv["message_id"])

        if debug_msg.document:
            fid = debug_msg.document.file_id
        elif debug_msg.video:
            fid = debug_msg.video.file_id
        elif debug_msg.audio:
            fid = debug_msg.audio.file_id
        elif debug_msg.animation:
            fid = debug_msg.animation.file_id
        elif debug_msg.photo:
            fid = debug_msg.photo[-1].file_id
        else:
            fid = None

        stored_files.append({
            "file_id": fid,
            "file_name": mv["file_name"]
        })

    bot.send_message(uid, f"DEBUG: stored_files = {len(stored_files)}")

    conn = get_conn()
    cur = conn.cursor()
    weekly_items = []

    for item in parsed:
        idx = find_best_match(item["title"], stored_files)
        bot.send_message(uid, f"DEBUG: matching '{item['title']}' → {idx}")

        if idx is None:
            continue

        sf = stored_files[idx]

        cur.execute(
            """
            INSERT INTO items (title, price, file_id, file_name, created_at)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                item["title"],
                item["price"],
                sf["file_id"],
                sf["file_name"],
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

        item_id = cur.fetchone()[0]
        conn.commit()

        bot.send_message(uid, f"DEBUG: INSERT OK item_id={item_id}")

        weekly_items.append({
            "id": item_id,
            "title": item["title"],
            "price": item["price"],
            "file_id": sf["file_id"]
        })

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(
            "📽 VIEW ALL MOVIES",
            url=f"https://t.me/{BOT_USERNAME}?start=viewall"
        )
    )

    sent = bot.send_photo(
        CHANNEL,
        temp["poster"],
        caption=temp["caption"],
        reply_markup=kb
    )

    bot.send_message(uid, f"DEBUG: channel_msg_id = {sent.message_id}")

    cur.execute(
        "INSERT INTO weekly (poster_file_id, items, channel_msg_id) VALUES (%s,%s,%s)",
        (temp["poster"], json.dumps(weekly_items), sent.message_id)
    )
    conn.commit()

    cur.close()
    conn.close()

    bot.send_message(uid, "✅ WEAK UPDATE COMPLETED")
    weak_update_temp.pop(uid, None)


def send_weekly_list(msg):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT items FROM weekly ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return bot.send_message(msg.chat.id, "Babu weekly films.")

    try:
        items = json.loads(row[0] or "[]")
    except:
        items = []

    if not items:
        cur.close()
        conn.close()
        return bot.send_message(msg.chat.id, "Babu weekly films.")

    today = datetime.now().strftime("%d/%m/%Y")
    text = f"📅 Weekly Update ({today})\n\n"

    kb = InlineKeyboardMarkup()
    all_ids = []

    for m in items:
        mid = m.get("id")
        title = m.get("title")
        price = m.get("price")

        text += f"{title} – ₦{price}\n\n"

        kb.row(
            InlineKeyboardButton(
                f"➕ Add Cart — {title}",
                callback_data=f"addcartdm:{mid}"
            ),
            InlineKeyboardButton(
                f"💳 BUY — {title}",
                callback_data=f"buy:{mid}"
            )
        )

        all_ids.append(str(mid))

    if all_ids:
        kb.add(
            InlineKeyboardButton(
                "🎁 BUY ALL",
                callback_data="buy:" + ",".join(all_ids)
            )
        )

    cur.close()
    conn.close()

    bot.send_message(msg.chat.id, text, reply_markup=kb)

# ---------- weekly button ----------
@bot.callback_query_handler(func=lambda c: c.data == "weekly_films")
def send_weekly_films(call):
    return send_weekly_list(call.message)


# ---------- My Orders (UNPAID with per-item REMOVE) ----------
ORDERS_PER_PAGE = 5

def build_unpaid_orders_view(uid, page):
    offset = page * ORDERS_PER_PAGE

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*) FROM orders WHERE user_id=%s AND paid=0",
        (uid,)
    )
    total = cur.fetchone()[0]

    if total == 0:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"))
        cur.close()
        conn.close()
        return "🧾 <b>Babu unpaid order.</b>", kb

    # ✅ GYARA KAƊAI: TOTAL DIN YANA GANE GROUP_KEY
    cur.execute(
        """
        SELECT COALESCE(SUM(
            CASE
                WHEN gk_count = 1 THEN base_price
                ELSE amount
            END
        ),0)
        FROM (
            SELECT
                o.id,
                COUNT(DISTINCT i.group_key) AS gk_count,
                SUM(oi.price) AS amount,
                MIN(oi.price) AS base_price
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            LEFT JOIN items i ON i.id = oi.item_id
            WHERE o.user_id=%s AND o.paid=0
            GROUP BY o.id
        ) sub
        """,
        (uid,)
    )
    total_amount = cur.fetchone()[0]

    cur.execute(
        """
        SELECT
            o.id,
            COUNT(oi.item_id) AS items_count,
            SUM(oi.price) AS amount,
            MAX(i.title) AS title,
            COUNT(DISTINCT i.group_key) AS gk_count,
            MIN(oi.price) AS base_price,
            MIN(i.group_key) AS group_key
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        LEFT JOIN items i ON i.id = oi.item_id
        WHERE o.user_id=%s AND o.paid=0
        GROUP BY o.id
        ORDER BY o.id DESC
        LIMIT %s OFFSET %s
        """,
        (uid, ORDERS_PER_PAGE, offset)
    )
    rows = cur.fetchall()

    text = f"🧾 <b>Your unpaid orders ({total})</b>\n\n"
    kb = InlineKeyboardMarkup()

    for oid, count, amount, title, gk_count, base_price, group_key in rows:
        if count > 1 and gk_count == 1:
            name = f"{title} (EP {count})"
            show_amount = base_price
        else:
            if count == 1:
                name = title or "Single item"
            else:
                name = f"Group order ({count} items)"
            show_amount = amount

        short = name[:27] + "…" if len(name) > 27 else name
        text += f"• {short} — ₦{int(show_amount)}\n"

        kb.row(
            InlineKeyboardButton(
                f"❌ Cire {short}",
                callback_data=f"remove_unpaid:{oid}"
            )
        )

    text += f"\n<b>Total balance:</b> ₦{int(total_amount)}"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Back", callback_data=f"unpaid_prev:{page-1}"))
    if offset + ORDERS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"unpaid_next:{page+1}"))
    if nav:
        kb.row(*nav)

    kb.row(
        InlineKeyboardButton("💳 Pay all", callback_data="payall:"),
        InlineKeyboardButton("📦 Paid orders", callback_data="paid_orders")
    )
    kb.row(
        InlineKeyboardButton("🗑 Delete unpaid", callback_data="delete_unpaid"),
        InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home")
    )

    cur.close()
    conn.close()

    return text, kb


def build_paid_orders_view(uid, page):
    offset = page * ORDERS_PER_PAGE

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*) FROM orders WHERE user_id=%s AND paid=1",
        (uid,)
    )
    total = cur.fetchone()[0]

    if total == 0:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🎬 MY MOVIES", callback_data="my_movies"))
        kb.add(InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"))
        cur.close()
        conn.close()
        return "📦 <b>Babu paid order tukuna.</b>", kb

    cur.execute(
        """
        SELECT
            o.id,
            COUNT(oi.item_id) AS items_count,
            MAX(i.title) AS title,
            COUNT(DISTINCT i.group_key) AS gk_count
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        LEFT JOIN items i ON i.id = oi.item_id
        WHERE o.user_id=%s AND o.paid=1
        GROUP BY o.id
        ORDER BY o.id DESC
        LIMIT %s OFFSET %s
        """,
        (uid, ORDERS_PER_PAGE, offset)
    )
    rows = cur.fetchall()

    text = f"📦 <b>Your paid orders ({total})</b>\n\n"
    kb = InlineKeyboardMarkup()

    for oid, count, title, gk_count in rows:

        # adadin da aka riga aka deliver (inda deliver ke sakawa)
        cur.execute(
            "SELECT COUNT(*) FROM user_movies WHERE order_id=%s AND user_id=%s",
            (oid, uid)
        )
        delivered = cur.fetchone()[0]

        remain = count - delivered

        if count > 1 and gk_count == 1:
            name = f"{title} (EP {count})"
        else:
            name = title or f"Group order ({count} items)"

        short = name[:27] + "…" if len(name) > 27 else name

        if remain > 0:
            text += f"• {short} — ✅ Paid (Remaining: {remain})\n"
        else:
            text += f"• {short} — ✅ Delivered\n"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Back", callback_data=f"paid_prev:{page-1}"))
    if offset + ORDERS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"paid_next:{page+1}"))
    if nav:
        kb.row(*nav)

    kb.add(InlineKeyboardButton("🎬 MY MOVIES", callback_data="my_movies"))
    kb.add(InlineKeyboardButton("⤴️ KOMA FARKO", callback_data="go_home"))

    cur.close()
    conn.close()

    return text, kb

# ---------- START handler (VIEW) ----------
@bot.message_handler(commands=['start'])
def start_handler(msg):

    track_visited_user(msg)

    # 🛑 BAR BUYD DA GROUPITEM SU WUCE
    if msg.text.startswith("/start buyd_"):
        return
    if msg.text.startswith("/start groupitem_"):
        return
    # ===== ASALIN VIEW DINKA (BA A TABA SHI BA) =====
    args = msg.text.split()
    if len(args) > 1 and args[1] == "weakupdate":
        return send_weekly_list(msg)
    if len(args) > 1 and args[1] == "viewall":
        return send_weekly_list(msg)

    bot.send_message(msg.chat.id, "Welcome!")

# ========= BUYD (ITEM ONLY | DEEP LINK → DM) =========
# ========= BUYD (IDS + GROUP_KEY SUPPORT | FULL SAFE VERSION) =========
from psycopg2.extras import RealDictCursor
import uuid

@bot.message_handler(func=lambda m: m.text and m.text.startswith("/start buyd_"))
def buyd_deeplink_handler(msg):

    try:
        uid = msg.from_user.id
        raw = msg.text.split("buyd_", 1)[1].strip()
    except:
        return

    conn = get_conn()
    if not conn:
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    items = []

    # =====================================================
    # MODE 1: IDS
    # =====================================================
    if all(x.strip().isdigit() for x in raw.replace("_", ",").split(",")):

        sep = "_" if "_" in raw else ","
        item_ids = [int(x) for x in raw.split(sep) if x.strip().isdigit()]

        if not item_ids:
            cur.close()
            conn.close()
            return

        placeholders = ",".join(["%s"] * len(item_ids))

        cur.execute(
            f"""
            SELECT id, title, price, file_id, group_key
            FROM items
            WHERE id IN ({placeholders})
            """,
            tuple(item_ids)
        )

        items = cur.fetchall()

    # =====================================================
    # MODE 2: GROUP_KEY
    # =====================================================
    else:
        cur.execute(
            """
            SELECT id, title, price, file_id, group_key
            FROM items
            WHERE group_key=%s
            ORDER BY id ASC
            """,
            (raw,)
        )

        items = cur.fetchall()

    if not items:
        cur.close()
        conn.close()
        return

    # FILE CHECK
    items = [i for i in items if i.get("file_id")]
    if not items:
        cur.close()
        conn.close()
        return

    item_ids_clean = [i["id"] for i in items]
    placeholders = ",".join(["%s"] * len(item_ids_clean))

    # OWNERSHIP CHECK
    cur.execute(
        f"""
        SELECT 1 FROM user_movies
        WHERE user_id=%s
          AND item_id IN ({placeholders})
        LIMIT 1
        """,
        (uid, *item_ids_clean)
    )
    owned = cur.fetchone()

    if owned:
        cur.close()
        conn.close()
        return

    # GROUP PRICING
    groups = {}
    for i in items:
        key = i["group_key"] or f"single_{i['id']}"
        if key not in groups:
            groups[key] = int(i["price"] or 0)

    total = sum(groups.values())
    if total <= 0:
        cur.close()
        conn.close()
        return

    # REUSE / CREATE ORDER
    cur.execute(
        f"""
        SELECT o.id
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        WHERE o.user_id=%s
          AND o.paid=0
          AND oi.item_id IN ({placeholders})
        GROUP BY o.id
        HAVING COUNT(DISTINCT oi.item_id)=%s
        LIMIT 1
        """,
        (uid, *item_ids_clean, len(item_ids_clean))
    )
    row = cur.fetchone()

    if row:
        order_id = row["id"]
    else:
        order_id = str(uuid.uuid4())

        cur.execute(
            "INSERT INTO orders (id, user_id, amount, paid) VALUES (%s,%s,%s,0)",
            (order_id, uid, total)
        )

        for i in items:
            cur.execute(
                """
                INSERT INTO order_items (order_id, item_id, file_id, price)
                VALUES (%s,%s,%s,%s)
                """,
                (order_id, i["id"], i["file_id"], int(i["price"] or 0))
            )

        conn.commit()

    # PAYMENT
    pay_url = create_flutterwave_payment(uid, order_id, total, items[0]["title"])
    if not pay_url:
        cur.close()
        conn.close()
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    first_name = msg.from_user.first_name or ""
    last_name = msg.from_user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    bot.send_message(
        uid,
        f"""🎉 Thank you {full_name}

🎬 You will buy this movie:
📽 {items[0]["title"]}

🎞 Films ({len(items)})
🆔 Order ID: {order_id}

💰 Please complete your payment below 👇""",
        reply_markup=kb
    )

    cur.close()
    conn.close()
# ======= GROUPITEM (IDS + GROUP_KEY SUPPORT | DEBUG SAFE) =========
from psycopg2.extras import RealDictCursor
import uuid

@bot.message_handler(func=lambda m: m.text and m.text.startswith("/start groupitem_"))
def groupitem_deeplink_handler(msg):

    try:
        uid = msg.from_user.id
        raw = msg.text.split("groupitem_", 1)[1].strip()
    except Exception as e:
        return

    conn = get_conn()
    if not conn:
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    items = []

    # =====================================================
    # MODE 1: IDS (OLD SYSTEM)
    # =====================================================
    if all(x.strip().isdigit() for x in raw.replace("_", ",").split(",")):

        sep = "_" if "_" in raw else ","
        item_ids = [int(x) for x in raw.split(sep) if x.strip().isdigit()]

        if not item_ids:
            cur.close()
            conn.close()
            return

        placeholders = ",".join(["%s"] * len(item_ids))

        cur.execute(
            f"""
            SELECT id, title, price, file_id, group_key
            FROM items
            WHERE id IN ({placeholders})
            """,
            tuple(item_ids)
        )

        items = cur.fetchall()

    # =====================================================
    # MODE 2: GROUP_KEY (NEW SYSTEM)
    # =====================================================
    else:

        cur.execute(
            """
            SELECT id, title, price, file_id, group_key
            FROM items
            WHERE group_key=%s
            ORDER BY id ASC
            """,
            (raw,)
        )

        items = cur.fetchall()

    if not items:
        cur.close()
        conn.close()
        return

    # =====================================================
    # FILE CHECK
    # =====================================================
    items = [i for i in items if i.get("file_id")]

    if not items:
        cur.close()
        conn.close()
        return

    item_ids_clean = [i["id"] for i in items]
    placeholders = ",".join(["%s"] * len(item_ids_clean))

    # =====================================================
    # OWNERSHIP CHECK
    # =====================================================
    try:
        cur.execute(
            f"""
            SELECT 1 FROM user_movies
            WHERE user_id=%s
              AND item_id IN ({placeholders})
            LIMIT 1
            """,
            (uid, *item_ids_clean)
        )
        owned = cur.fetchone()

    except Exception as e:
        cur.close()
        conn.close()
        return

    if owned:
        cur.close()
        conn.close()
        return

    # =====================================================
    # GROUP PRICING (SAFE)
    # =====================================================
    groups = {}

    for i in items:
        key = i["group_key"] or f"single_{i['id']}"
        if key not in groups:
            groups[key] = int(i["price"] or 0)

    total = sum(groups.values())

    if total <= 0:
        cur.close()
        conn.close()
        return

    # =====================================================
    # REUSE / CREATE ORDER
    # =====================================================
    try:
        cur.execute(
            f"""
            SELECT o.id
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.user_id=%s
              AND o.paid=0
              AND oi.item_id IN ({placeholders})
            GROUP BY o.id
            HAVING COUNT(DISTINCT oi.item_id)=%s
            LIMIT 1
            """,
            (uid, *item_ids_clean, len(item_ids_clean))
        )
        row = cur.fetchone()

    except Exception as e:
        cur.close()
        conn.close()
        return

    if row:
        order_id = row["id"]
    else:
        order_id = str(uuid.uuid4())

        try:
            cur.execute(
                "INSERT INTO orders (id, user_id, amount, paid) VALUES (%s,%s,%s,0)",
                (order_id, uid, total)
            )

            for i in items:
                cur.execute(
                    """
                    INSERT INTO order_items (order_id, item_id, file_id, price)
                    VALUES (%s,%s,%s,%s)
                    """,
                    (order_id, i["id"], i["file_id"], int(i["price"] or 0))
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            return

    # =====================================================
    # PAYMENT LINK
    # =====================================================
    try:
        pay_url = create_flutterwave_payment(uid, order_id, total, items[0]["title"])
    except Exception as e:
        cur.close()
        conn.close()
        return

    if not pay_url:
        cur.close()
        conn.close()
        return

    # =====================================================
    # SEND BUTTON
    # =====================================================
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))

    first_name = msg.from_user.first_name or ""
    last_name = msg.from_user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    bot.send_message(
        uid,
        f"""🎉 Thank you {full_name}

🎬 You will buy this movie:
📽 {items[0]["title"]}

🎞 Films ({len(items)})
🆔 Order ID: {order_id}

💰 Please complete your payment below 👇""",
        reply_markup=kb
    )

    cur.close()
    conn.close()

# ==
from psycopg2.extras import RealDictCursor
import uuid
import time

# ======================================================
# BUY AGAIN (FULL SAFE VERSION)
# ======================================================
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buy_again:"))
def buy_again_handler(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    try:
        old_order_id = c.data.split("buy_again:", 1)[1]
    except:
        return

    conn = get_conn()
    if not conn:
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    # FETCH PAID ORDER ITEMS ONLY
    cur.execute(
        """
        SELECT oi.item_id, oi.file_id, oi.price, i.title, i.group_key
        FROM order_items oi
        JOIN orders o ON o.id = oi.order_id
        LEFT JOIN items i ON i.id = oi.item_id
        WHERE o.id=%s AND o.user_id=%s AND o.paid=1
        """,
        (old_order_id, uid)
    )
    rows = cur.fetchall()

    if not rows:
        cur.close()
        conn.close()
        return

    items = [r for r in rows if r.get("file_id")]
    if not items:
        cur.close()
        conn.close()
        return

    # GROUP PRICING SAFE
    groups = {}
    for i in items:
        key = i["group_key"] or f"single_{i['item_id']}"
        if key not in groups:
            groups[key] = int(i["price"] or 0)

    total = sum(groups.values())
    if total <= 0:
        cur.close()
        conn.close()
        return

    item_ids = [i["item_id"] for i in items]
    placeholders = ",".join(["%s"] * len(item_ids))

    # FULL REUSE CHECK
    cur.execute(
        f"""
        SELECT o.id
        FROM orders o
        JOIN order_items oi ON oi.order_id=o.id
        WHERE o.user_id=%s
          AND o.paid=0
          AND oi.item_id IN ({placeholders})
        GROUP BY o.id
        HAVING COUNT(DISTINCT oi.item_id)=%s
        LIMIT 1
        """,
        (uid, *item_ids, len(item_ids))
    )
    row = cur.fetchone()

    if row:
        order_id = row["id"]
    else:
        order_id = str(uuid.uuid4())

        cur.execute(
            "INSERT INTO orders (id,user_id,amount,paid) VALUES (%s,%s,%s,0)",
            (order_id, uid, total)
        )

        for i in items:
            cur.execute(
                """
                INSERT INTO order_items (order_id,item_id,file_id,price)
                VALUES (%s,%s,%s,%s)
                """,
                (order_id, i["item_id"], i["file_id"], int(i["price"] or 0))
            )

        conn.commit()

    pay_url = create_flutterwave_payment(uid, order_id, total, items[0]["title"])
    if not pay_url:
        cur.close()
        conn.close()
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    first_name = c.from_user.first_name or ""
    last_name = c.from_user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    bot.send_message(
        uid,
        f"""🎉 Thank you {full_name}

🎬 You will buy this movie:
📽 {items[0]["title"]}

🎞 Films ({len(items)})
🆔 Order ID: {order_id}

💰 Please complete your payment below 👇""",
        reply_markup=kb
    )

    cur.close()
    conn.close()


# ======================================================
# BUYGROUP (FULL SAFE ENGINE)
# ======================================================
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buygroup:"))
def buygroup_handler(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    try:
        raw = c.data.split("buygroup:", 1)[1]
        sep = "_" if "_" in raw else ","
        item_ids = [int(x) for x in raw.split(sep) if x.strip().isdigit()]
    except:
        return

    if not item_ids:
        return

    conn = get_conn()
    if not conn:
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    placeholders = ",".join(["%s"] * len(item_ids))

    cur.execute(
        f"""
        SELECT id,title,price,file_id,group_key
        FROM items
        WHERE id IN ({placeholders})
        """,
        tuple(item_ids)
    )
    items = cur.fetchall()

    if not items:
        cur.close()
        conn.close()
        return

    items = [i for i in items if i["file_id"]]
    if not items:
        cur.close()
        conn.close()
        return

    # OWNERSHIP
    cur.execute(
        f"""
        SELECT 1 FROM user_movies
        WHERE user_id=%s AND item_id IN ({placeholders})
        LIMIT 1
        """,
        (uid, *[i["id"] for i in items])
    )
    if cur.fetchone():
        cur.close()
        conn.close()
        return

    # GROUP SAFE PRICING
    groups = {}
    for i in items:
        key = i["group_key"] or f"single_{i['id']}"
        if key not in groups:
            groups[key] = int(i["price"] or 0)

    total = sum(groups.values())
    if total <= 0:
        cur.close()
        conn.close()
        return

    item_ids_clean = [i["id"] for i in items]

    # FULL REUSE CHECK
    cur.execute(
        f"""
        SELECT o.id
        FROM orders o
        JOIN order_items oi ON oi.order_id=o.id
        WHERE o.user_id=%s
          AND o.paid=0
          AND oi.item_id IN ({placeholders})
        GROUP BY o.id
        HAVING COUNT(DISTINCT oi.item_id)=%s
        LIMIT 1
        """,
        (uid, *item_ids_clean, len(item_ids_clean))
    )
    row = cur.fetchone()

    if row:
        order_id = row["id"]
    else:
        order_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO orders (id,user_id,amount,paid) VALUES (%s,%s,%s,0)",
            (order_id, uid, total)
        )

        for i in items:
            cur.execute(
                """
                INSERT INTO order_items (order_id,item_id,file_id,price)
                VALUES (%s,%s,%s,%s)
                """,
                (order_id, i["id"], i["file_id"], int(i["price"] or 0))
            )

        conn.commit()

    # DEBUG (UNCHANGED AS YOU REQUESTED)
    dbg = "🤩 <b>GROUP ORDER CREATED</b>\n\n"
    for key in groups:
        dbg += f"• {items[0]['title']}\n"
    bot.send_message(uid, dbg, parse_mode="HTML")

    pay_url = create_flutterwave_payment(uid, order_id, total, items[0]["title"])
    if not pay_url:
        cur.close()
        conn.close()
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    first_name = c.from_user.first_name or ""
    last_name = c.from_user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    bot.send_message(
        uid,
        f"""🎉 Thank you {full_name}

🎬 You will buy this movie:
📽 {items[0]["title"]}

🎞 Films ({len(items)})
🆔 Order ID: {order_id}

💰 Please complete your payment below 👇""",
        reply_markup=kb
    )

    cur.close()
    conn.close()

# ================= ADMIN MANUAL SUPPORT SYSTEM =================

ADMIN_SUPPORT = {}

# ---------- /problem ----------
@bot.message_handler(commands=["problem"])
def admin_problem_cmd(m):
    if m.from_user.id != ADMIN_ID:
        return

    ADMIN_SUPPORT[m.from_user.id] = {"stage": "menu"}

    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("🔁 RESEND ORDER", callback_data="admin_resend"),
        InlineKeyboardButton("🎁 GIFT", callback_data="admin_gift")
    )

    bot.send_message(
        m.chat.id,
        "🧩 <b>ADMIN SUPPORT PANEL</b>\n\nZabi abin da kake so:",
        parse_mode="HTML",
        reply_markup=kb
    )


# ---------- RESEND ----------
@bot.callback_query_handler(func=lambda c: c.data == "admin_resend")
def admin_resend_start(c):
    if c.from_user.id != ADMIN_ID:
        return

    ADMIN_SUPPORT[c.from_user.id] = {"stage": "wait_order_id"}
    bot.answer_callback_query(c.id)
    bot.send_message(
        c.from_user.id,
        "🧾 Turo <b>ORDER ID</b>:",
        parse_mode="HTML"
    )


# ---------- GIFT ----------
@bot.callback_query_handler(func=lambda c: c.data == "admin_gift")
def admin_gift_start(c):
    if c.from_user.id != ADMIN_ID:
        return

    ADMIN_SUPPORT[c.from_user.id] = {"stage": "gift_user"}
    bot.answer_callback_query(c.id)
    bot.send_message(
        c.from_user.id,
        "👤 Turo <b>USER ID</b> wanda za a bawa kyauta:",
        parse_mode="HTML"
    )


# ---------- ADMIN FLOW ----------
@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and m.from_user.id in ADMIN_SUPPORT)
def admin_support_flow(m):
    conn = get_conn()
    cur = conn.cursor()

    data = ADMIN_SUPPORT.get(m.from_user.id)
    if not data:
        cur.close()
        conn.close()
        return

    stage = data.get("stage")
    text = m.text.strip()

    # ===== RESEND ORDER =====
    if stage == "wait_order_id":

        cur.execute(
            "SELECT user_id, amount, paid FROM orders WHERE id=%s",
            (text,)
        )
        row = cur.fetchone()

        # ❌ ORDER ID BAYA WUJUWA
        if not row:
            ADMIN_SUPPORT.pop(m.from_user.id, None)
            cur.close()
            conn.close()
            bot.send_message(
                m.chat.id,
                "❌ <b>Order ID bai dace ba.</b>\nBabu wannan order a system.",
                parse_mode="HTML"
            )
            return

        # ⚠️ ORDER BAI BIYA BA
        if row["paid"] != 1:
            ADMIN_SUPPORT.pop(m.from_user.id, None)
            cur.close()
            conn.close()
            bot.send_message(
                m.chat.id,
                "⚠️ <b>ORDER BAI BIYA BA</b>\nFaɗa wa user ya kammala biya.",
                parse_mode="HTML"
            )
            return

        user_id = row["user_id"]
        amount = row["amount"]

        cur.execute(
            """
            SELECT item_id
            FROM order_items
            WHERE order_id=%s
            """,
            (text,)
        )
        items = cur.fetchall()

        # ❌ BA ITEMS
        if not items:
            ADMIN_SUPPORT.pop(m.from_user.id, None)
            cur.close()
            conn.close()
            bot.send_message(
                m.chat.id,
                "⚠️ Wannan order ɗin babu items a cikinsa.\nDuba order_items table."
            )
            return

        item_ids = [r["item_id"] for r in items]

        ADMIN_SUPPORT[m.from_user.id] = {
            "stage": "resend_confirm",
            "user_id": user_id,
            "items": item_ids
        }

        cur.close()
        conn.close()

        bot.send_message(
            m.chat.id,
            f"""✅ <b>ORDER VERIFIED</b>

🆔 Order ID: <code>{text}</code>
👤 User ID: <code>{user_id}</code>
💰 Amount: ₦{amount}
🎬 Items: {len(item_ids)}

Tura <b>/sendall</b> domin a sake tura items.""",
            parse_mode="HTML"
        )
        return

    # ===== GIFT FLOW =====
    if stage == "gift_user":
        if not text.isdigit():
            bot.send_message(m.chat.id, "❌ Rubuta USER ID mai inganci.")
            cur.close()
            conn.close()
            return

        data["gift_user"] = int(text)
        data["stage"] = "gift_message"
        cur.close()
        conn.close()
        bot.send_message(
            m.chat.id,
            "✍️ Rubuta <b>MESSAGE</b> da user zai gani:",
            parse_mode="HTML"
        )
        return

    if stage == "gift_message":
        data["gift_message"] = text
        data["stage"] = "gift_item"
        cur.close()
        conn.close()
        bot.send_message(
            m.chat.id,
            "🎬 Rubuta <b>SUNAN ITEM</b> (title ko file name):",
            parse_mode="HTML"
        )
        return

    if stage == "gift_item":
        q = text.lower()

        cur.execute(
            """
            SELECT file_id, title
            FROM items
            WHERE title LIKE %s OR file_name LIKE %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (f"%{q}%", f"%{q}%")
        )
        row = cur.fetchone()

        if not row:
            ADMIN_SUPPORT.pop(m.from_user.id, None)
            cur.close()
            conn.close()
            bot.send_message(
                m.chat.id,
                "❌ Ba a samu item a ITEMS table ba.",
                parse_mode="HTML"
            )
            return

        file_id, title = row["file_id"], row["title"]

        try:
            bot.send_video(data["gift_user"], file_id, caption=data["gift_message"])
        except:
            bot.send_document(data["gift_user"], file_id, caption=data["gift_message"])

        bot.send_message(
            m.chat.id,
            f"""🎁 <b>An kammala</b>

👤 User ID: <code>{data['gift_user']}</code>
🎬 Item: <b>{title}</b>""",
            parse_mode="HTML"
        )

        ADMIN_SUPPORT.pop(m.from_user.id, None)

        cur.close()
        conn.close()
        return

    cur.close()
    conn.close()


# ---------- /sendall ----------
@bot.message_handler(commands=["sendall"])
def admin_sendall_cmd(m):
    if m.from_user.id != ADMIN_ID:
        return

    conn = get_conn()
    cur = conn.cursor()

    data = ADMIN_SUPPORT.get(m.from_user.id)
    if not data or data.get("stage") != "resend_confirm":
        cur.close()
        conn.close()
        return

    uid = data["user_id"]
    item_ids = data["items"]
    order_id = data.get("order_id") or "ADMIN_RESEND"

    sent = 0
    failed = []

    for item_id in item_ids:
        cur.execute(
            """
            SELECT file_id, title
            FROM items
            WHERE id=%s
            """,
            (item_id,)
        )
        row = cur.fetchone()

        if not row or not row["file_id"]:
            failed.append(item_id)
            continue

        file_id = row["file_id"]

        try:
            bot.send_video(uid, file_id)
            sent += 1
        except:
            try:
                bot.send_document(uid, file_id)
                sent += 1
            except:
                failed.append(item_id)
                continue

        # ✅ SAKA SHEDA A MALLAKA (ANTI DUP)
        cur.execute(
            """
            INSERT IGNORE INTO user_movies (user_id, item_id, order_id)
            VALUES (%s, %s, %s)
            """,
            (uid, item_id, order_id)
        )

    conn.commit()

    # ===== ADMIN FEEDBACK =====
    msg = f"""✅ <b>An kammala resend</b>

👤 User ID: <code>{uid}</code>
🎬 An tura: <b>{sent}</b>
"""
    if failed:
        msg += f"⚠️ Sun kasa tura: {len(failed)}\n"

    bot.send_message(m.chat.id, msg, parse_mode="HTML")

    # ===== USER FEEDBACK =====
    bot.send_message(
        uid,
        "🙏 Muna ba da haƙuri.\nAn sake tura fim ɗinka kuma an tabbatar da mallakarka ❤️"
    )

    ADMIN_SUPPORT.pop(m.from_user.id, None)

    cur.close()
    conn.close()

from psycopg2.extras import RealDictCursor
import time

# ======================================================
# PAY ALL UNPAID (FULL SAFE – CHECKOUT STYLE)
# ======================================================
@bot.callback_query_handler(func=lambda c: c.data == "payall:")
def pay_all_unpaid(call):
    user_id = call.from_user.id
    bot.answer_callback_query(call.id)

    conn = get_conn()
    if not conn:
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1️⃣ FETCH ALL UNPAID ITEMS
    cur.execute(
        """
        SELECT
            o.id AS order_id,
            oi.item_id,
            oi.file_id,
            oi.price,
            i.title,
            i.group_key
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        JOIN items i ON i.id = oi.item_id
        WHERE o.user_id=%s AND o.paid=0
        """,
        (user_id,)
    )
    rows = cur.fetchall()

    if not rows:
        cur.close()
        conn.close()
        return

    # 🔒 REMOVE INVALID ITEMS
    rows = [
        r for r in rows
        if r["file_id"] and int(r["price"] or 0) > 0
    ]

    if not rows:
        cur.close()
        conn.close()
        return

    # 🛑 REMOVE ITEMS ALREADY PAID BEFORE
    valid_items = []
    for r in rows:
        cur.execute(
            """
            SELECT 1
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.user_id=%s AND o.paid=1 AND oi.item_id=%s
            LIMIT 1
            """,
            (user_id, r["item_id"])
        )
        if not cur.fetchone():
            valid_items.append(r)

    rows = valid_items

    if not rows:
        cur.close()
        conn.close()
        return

    # ================= GROUP SAFE TOTAL =================
    groups = {}

    for r in rows:
        price = int(r["price"] or 0)
        key = r["group_key"] or f"single_{r['item_id']}"

        if key not in groups:
            groups[key] = {
                "price": price,
                "items": []
            }

        groups[key]["items"].append(r)

    total_amount = sum(g["price"] for g in groups.values())

    if total_amount <= 0:
        cur.close()
        conn.close()
        return

    # 🛑 USE EXISTING UNPAID ORDER ONLY
    cur.execute(
        """
        SELECT id
        FROM orders
        WHERE user_id=%s AND paid=0
        ORDER BY id DESC
        LIMIT 1
        """,
        (user_id,)
    )
    old = cur.fetchone()

    if not old:
        cur.close()
        conn.close()
        return

    order_id = old["id"]

    # UPDATE AMOUNT ONLY
    cur.execute(
        "UPDATE orders SET amount=%s WHERE id=%s",
        (total_amount, order_id)
    )
    conn.commit()

    # ================= PAYMENT =================
    tx_ref = f"{order_id}_{int(time.time())}"

    pay_url = create_flutterwave_payment(
        user_id,
        tx_ref,
        total_amount,
        "Pay All Orders"
    )

    if not pay_url:
        cur.close()
        conn.close()
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    first_name = call.from_user.first_name or ""
    last_name = call.from_user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    bot.send_message(
        user_id,
        f"""🎉 Thank you {full_name}

🧾 PAY ALL UNPAID ORDERS

📦 Groups: {len(groups)}
💰 Total: ₦{int(total_amount)}

🆔 Order ID:
{order_id}

💳 Please complete your payment below 👇""",
        reply_markup=kb
    )

    cur.close()
    conn.close()

from psycopg2.extras import RealDictCursor
import uuid

# ===================== BUY ALL (CUSTOM IDS SAFE) =====================
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buyall:"))
def buy_all_handler(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    try:
        ids_raw = c.data.split("buyall:", 1)[1]
        item_ids = [int(x) for x in ids_raw.split(",") if x.strip().isdigit()]
    except:
        return

    if not item_ids:
        return

    conn = get_conn()
    if not conn:
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    placeholders = ",".join(["%s"] * len(item_ids))

    cur.execute(
        f"""
        SELECT id, title, price, file_id, group_key
        FROM items
        WHERE id IN ({placeholders})
        """,
        tuple(item_ids)
    )
    rows = cur.fetchall()

    # 🔒 REMOVE INVALID ITEMS
    items = [
        r for r in rows
        if r["file_id"] and int(r["price"] or 0) > 0
    ]

    if not items:
        cur.close()
        conn.close()
        return

    # 🛑 OWNERSHIP CHECK
    cur.execute(
        f"""
        SELECT 1 FROM user_movies
        WHERE user_id=%s AND item_id IN ({placeholders})
        LIMIT 1
        """,
        (uid, *[i["id"] for i in items])
    )
    if cur.fetchone():
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🎬 MY MOVIES", callback_data="my_movies"))
        bot.send_message(
            uid,
            "✅ <b>Ka riga ka mallaki wannan fim.\n\nDUBA MY MOVIES domin sake karɓa kyauta.</b>",
            parse_mode="HTML",
            reply_markup=kb
        )
        cur.close()
        conn.close()
        return

    # ================= GROUP SAFE TOTAL =================
    groups = {}
    for i in items:
        key = i["group_key"] or f"single_{i['id']}"
        if key not in groups:
            groups[key] = int(i["price"] or 0)

    total = sum(groups.values())
    movie_count = len(items)

    discount = int(total * 0.10) if movie_count >= 10 else 0
    final_total = total - discount

    if final_total <= 0:
        cur.close()
        conn.close()
        return

    item_ids_clean = [i["id"] for i in items]
    placeholders2 = ",".join(["%s"] * len(item_ids_clean))

    # 🛑 EXACT MATCH UNPAID REUSE (FULL SAFE)
    cur.execute(
        f"""
        SELECT o.id
        FROM orders o
        JOIN order_items oi ON oi.order_id=o.id
        WHERE o.user_id=%s
          AND o.paid=0
          AND oi.item_id IN ({placeholders2})
        GROUP BY o.id
        HAVING COUNT(DISTINCT oi.item_id)=%s
        LIMIT 1
        """,
        (uid, *item_ids_clean, len(item_ids_clean))
    )
    old = cur.fetchone()

    if old:
        order_id = old["id"]
    else:
        order_id = str(uuid.uuid4())

        cur.execute(
            "INSERT INTO orders (id,user_id,amount,paid) VALUES (%s,%s,%s,0)",
            (order_id, uid, final_total)
        )

        for i in items:
            cur.execute(
                """
                INSERT INTO order_items (order_id,item_id,file_id,price)
                VALUES (%s,%s,%s,%s)
                """,
                (order_id, i["id"], i["file_id"], int(i["price"] or 0))
            )

        conn.commit()

    # ================= PAYMENT =================
    pay_url = create_flutterwave_payment(
        uid,
        order_id,
        final_total,
        "Buy All Movies"
    )

    if not pay_url:
        cur.close()
        conn.close()
        return

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

    first_name = c.from_user.first_name or ""
    last_name = c.from_user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    bot.send_message(
        uid,
        f"""🎉 Thank you {full_name}

🧾 BUY ALL ORDER

🎞 Movies: {movie_count}
💵 Total: ₦{total}
🏷 Discount: ₦{discount}
✅ Final: ₦{final_total}

🆔 Order ID:
{order_id}

💳 Please complete your payment below 👇""",
        reply_markup=kb
    )

    cur.close()
    conn.close()

import uuid 
from datetime import datetime
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ===============================
# SERIES UPLOAD – FULL FLOW (FIXED)
# ===============================

series_sessions = {}

# ===============================
# COLLECT SERIES FILES (DM → MEMORY ONLY)
# ===============================
@bot.message_handler(
    content_types=["video", "document"],
    func=lambda m: m.from_user.id in series_sessions
)
def series_collect_files(m):
    uid = m.from_user.id
    sess = series_sessions.get(uid)

    if not sess or sess.get("stage") != "collect":
        return

    if m.video:
        dm_file_id = m.video.file_id
        file_name = m.video.file_name or "video.mp4"
    else:
        dm_file_id = m.document.file_id
        file_name = m.document.file_name or "file"

    sess["files"].append({
        "dm_file_id": dm_file_id,
        "file_name": file_name
    })

    bot.send_message(
        uid,
        f"✅ An karɓi: <b>{file_name}</b>",
        parse_mode="HTML"
    )


# ===============================
# DONE
# ===============================
@bot.message_handler(
    func=lambda m: (
        m.text
        and m.text.lower().strip() == "done"
        and m.from_user.id in series_sessions
    )
)
def series_done(m):
    uid = m.from_user.id
    sess = series_sessions.get(uid)

    if not sess or sess.get("stage") != "collect":
        return

    if not sess.get("files"):
        bot.send_message(uid, "❌ Babu fim da aka turo.")
        return

    text = "✅ <b>An karɓi fina-finai:</b>\n\n"
    for f in sess["files"]:
        text += f"• {f['file_name']}\n"

    text += "\n❓ <b>Akwai Hausa series a ciki?</b>"
    sess["stage"] = "ask_hausa"

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ EH", callback_data="hausa_yes"),
        InlineKeyboardButton("❌ A'A", callback_data="hausa_no")
    )

    bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)


# ===============================
# HAUSA CHOICE
# ===============================
@bot.callback_query_handler(
    func=lambda c: c.data in ["hausa_yes", "hausa_no"] and c.from_user.id in series_sessions
)
def handle_hausa_choice(c):
    uid = c.from_user.id
    sess = series_sessions.get(uid)
    bot.answer_callback_query(c.id)

    if c.data == "hausa_no":
        sess["hausa_matches"] = []
        sess["stage"] = "meta"
        bot.send_message(uid, "📸 Turo poster + caption (suna da farashi)")
        return

    sess["stage"] = "hausa_names"
    bot.send_message(uid, "✍️ Rubuta sunayen Hausa series (layi-layi)")


# ===============================
# FINALIZE (UPLOAD + DB)
# ===============================
from telebot.apihelper import ApiTelegramException
import time
import uuid
from datetime import datetime

@bot.message_handler(
    content_types=["photo"],
    func=lambda m: m.from_user.id in series_sessions
)
def series_finalize(m):

    try:
        uid = m.from_user.id
        data = m.caption or ""
    except:
        return

    sess = series_sessions.get(uid)

    if sess.get("stage") != "meta":
        return

    # ================= PARSE CAPTION =================
    try:
        title, raw_price = data.strip().rsplit("\n", 1)
        has_comma = "," in raw_price
        price = int(raw_price.replace(",", "").strip())
    except:
        bot.send_message(uid, "❌ Caption bai dace ba.")
        return

    poster_file_id = m.photo[-1].file_id

    # ================= DB CONNECT =================
    try:
        conn = get_conn()
        cur = conn.cursor()
    except:
        return

    # ================= CREATE SERIES =================
    try:
        cur.execute(
            "INSERT INTO series (title, price, poster_file_id) VALUES (%s,%s,%s) RETURNING id",
            (title, price, poster_file_id)
        )
        series_id = cur.fetchone()[0]
    except:
        return

    item_ids = []
    created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    group_key = str(uuid.uuid4())

    total_files = len(sess["files"])
    saved_count = 0

    # 🔹 Sako ɗaya kawai
    loading_msg = bot.send_message(ADMIN_ID, "⏳ Loading...")

    # ================= SAFE SEND FUNCTION =================
    def safe_send_document(chat_id, file_id, caption):

        while True:
            try:
                return bot.send_document(chat_id, file_id, caption=caption)

            except ApiTelegramException as e:

                if e.error_code == 429:
                    retry = int(e.result_json["parameters"]["retry_after"])

                    bot.edit_message_text(
                        f"⚠️ Rate limit hit.\nSleeping {retry}s...\n\n{saved_count}/{total_files} saved",
                        ADMIN_ID,
                        loading_msg.message_id
                    )

                    time.sleep(retry)

                    bot.edit_message_text(
                        f"⏳ Loading...\n\n{saved_count}/{total_files} saved",
                        ADMIN_ID,
                        loading_msg.message_id
                    )
                    continue
                else:
                    return None

            except:
                return None

    # ================= UPLOAD LOOP =================
    for index, f in enumerate(sess["files"], start=1):

        msg = safe_send_document(
            STORAGE_CHANNEL,
            f["dm_file_id"],
            f["file_name"]
        )

        if not msg:
            continue

        doc = msg.document or msg.video
        if not doc:
            continue

        try:
            cur.execute(
                """
                INSERT INTO items
                (title, price, file_id, file_name, group_key,
                 created_at, channel_msg_id, channel_username)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (
                    title,
                    price,
                    doc.file_id,
                    f["file_name"],
                    group_key,
                    created_at,
                    msg.message_id,
                    STORAGE_CHANNEL
                )
            )
            new_id = cur.fetchone()[0]
            item_ids.append(new_id)
            saved_count += 1

        except:
            continue

        # Update progress every 5 files only (domin rage spam)
        if saved_count % 5 == 0 or saved_count == total_files:
            try:
                bot.edit_message_text(
                    f"⏳ Loading...\n\n{saved_count}/{total_files} saved",
                    ADMIN_ID,
                    loading_msg.message_id
                )
            except:
                pass

        time.sleep(1.2)

    # ================= COMMIT =================
    try:
        conn.commit()
    except:
        pass

    cur.close()
    conn.close()

    # Final update
    try:
        bot.edit_message_text(
            f"✅ Completed!\n\n{saved_count}/{total_files} saved",
            ADMIN_ID,
            loading_msg.message_id
        )
    except:
        pass

    # ================= PUBLIC POST =================
    try:
        display_price = f"{price:,}" if has_comma else str(price)

        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton(
                "🛒 Add to cart",
                callback_data=f"addcartdm:{group_key}"
            ),
            InlineKeyboardButton(
                "💳 Buy now",
                url=f"https://t.me/{BOT_USERNAME}?start=groupitem_{group_key}"
            )
        )

        bot.send_photo(
            CHANNEL,
            poster_file_id,
            caption=f"🎬 <b>{title}</b>\n💵Price: ₦{display_price}",
            parse_mode="HTML",
            reply_markup=kb
        )

    except:
        pass

    bot.send_message(uid, "🎉 Series an adana dukka lafiya.")
    del series_sessions[uid]



# ======================================================

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

def _norm(x):
    return (x or "").lower().strip()

def safe_edit(chat_id, msg_id, text, kb=None):
    try:
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
    except:
        pass


def _unique_add(res, seen, key, title, price, ids):
    if key not in seen:
        res.append((ids, title, price))
        seen.add(key)


def _get_all_items():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, title, price, file_name, created_at, group_key
        FROM items
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


# ---------- SEARCH BY NAME (GROUP + SUBSTRING) ----------
def search_by_name(query):
    q = _norm(query)
    if not q:
        return []

    res, seen, groups = [], set(), {}

    for mid, title, price, fname, _, gk in _get_all_items():
        hay = _norm(title) + " " + _norm(fname)
        if q in hay:
            key = gk or f"single_{mid}"
            groups.setdefault(key, {
                "ids": [],
                "title": title,
                "price": price
            })
            groups[key]["ids"].append(mid)

    for k, g in groups.items():
        _unique_add(res, seen, k, g["title"], g["price"], g["ids"])

    return res


# ---------- ALGAITA ----------
def get_algaita_movies():
    res, seen = [], set()
    for mid, title, price, fname, _, _ in _get_all_items():
        if "algaita" in (_norm(title) + " " + _norm(fname)):
            if mid not in seen:
                res.append(([mid], title, price))
                seen.add(mid)
    return res


# ---------- HAUSA SERIES ----------
def get_hausa_series_movies():
    res, seen, groups = [], set(), {}

    for mid, title, price, fname, _, gk in _get_all_items():
        if title and "(" in title and "-" in title and ")" in title:
            key = gk or f"single_{mid}"
            groups.setdefault(key, {"ids": [], "title": title, "price": price})
            groups[key]["ids"].append(mid)

    for k, g in groups.items():
        _unique_add(res, seen, k, g["title"], g["price"], g["ids"])

    return res


# ---------- OTHERS ----------
def get_public_movies():
    res, seen, groups = [], set(), {}

    for mid, title, price, fname, _, gk in _get_all_items():
        if title and "(" in title and ")" in title and ("-" in title or "+" in title):
            key = gk or f"single_{mid}"
            groups.setdefault(key, {"ids": [], "title": title, "price": price})
            groups[key]["ids"].append(mid)

    for k, g in groups.items():
        _unique_add(res, seen, k, g["title"], g["price"], g["ids"])

    return res


# ======================================================
# ================= DISPLAY HELPERS ====================
# ======================================================

ITEMS_PER_PAGE = 6

def _send_items(uid, items, page, title, edit=None):
    start = page * ITEMS_PER_PAGE
    chunk = items[start:start + ITEMS_PER_PAGE]

    kb = InlineKeyboardMarkup()
    text = f"📂 <b>{title}</b>\n\n"

    for ids, name, price in chunk:
        short = name[:30] + "…" if len(name) > 30 else name
        kb.add(InlineKeyboardButton(
            f"🎬 {short} – ₦{price}",
            # ✅ GYARA KAWAI A NAN
            callback_data=f"buygroup:{'_'.join(map(str, ids))}"
        ))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Back", callback_data=f"C_{title.lower()}_{page-1}"))
    if start + ITEMS_PER_PAGE < len(items):
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"C_{title.lower()}_{page+1}"))
    if nav:
        kb.row(*nav)

    kb.row(
        InlineKeyboardButton("🔎 BROWSING", callback_data="search_movie"),
        InlineKeyboardButton("❌ CANCEL", callback_data="search_cancel")
    )

    if edit:
        safe_edit(edit.chat.id, edit.message_id, text, kb)
    else:
        bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML")


def send_algaita_movies(uid, page=0, edit=None):
    _send_items(uid, get_algaita_movies(), page, "ALGAITA", edit)


def send_hausa_series(uid, page=0, edit=None):
    _send_items(uid, get_hausa_series_movies(), page, "HAUSA", edit)


def send_others_movies(uid, page=0, edit=None):
    _send_items(uid, get_public_movies(), page, "OTHERS", edit)


def send_search_results(uid, page=0, edit=None):
    q = user_states.get(uid, {}).get("query")
    if not q:
        return
    _send_items(uid, search_by_name(q), page, "SEARCH", edit)


# ======================================================
# ================= SEARCH FLOW ========================
# ======================================================

@bot.callback_query_handler(func=lambda c: c.data.lower() == "search_movie")
def search_movie_entry(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔎 NEMA DA SUNA", callback_data="search_by_name"))
    kb.add(InlineKeyboardButton("🎺 ALGAITA", callback_data="C_algaita_0"))
    kb.add(InlineKeyboardButton("📺 HAUSA SERIES", callback_data="C_hausa_0"))
    kb.add(InlineKeyboardButton("🎞 OTHERS", callback_data="C_others_0"))
    kb.add(InlineKeyboardButton("❌ CANCEL", callback_data="search_cancel"))

    bot.send_message(
        uid,
        "🔍 <b>SASHEN NEMAN FIM</b>\nZaɓi yadda kake so:",
        reply_markup=kb,
        parse_mode="HTML"
    )


@bot.callback_query_handler(func=lambda c: c.data == "search_by_name")
def cb_search_by_name(c):
    uid = c.from_user.id
    bot.answer_callback_query(c.id)

    user_states[uid] = {"state": "wait_search_name"}

    bot.send_message(
        uid,
        "✍️ Rubuta <b>kowane harafi ko suna</b> na fim:",
        parse_mode="HTML"
    )


@bot.message_handler(func=lambda m: user_states.get(m.from_user.id, {}).get("state") == "wait_search_name")
def handle_search_text(m):
    uid = m.from_user.id
    q = m.text.strip()

    if not q:
        return

    user_states[uid] = {
        "state": "search_results",
        "query": q
    }

    send_search_results(uid, 0)


# ======================================================
# ================= CALLBACK HANDLERS ==================
# ======================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("C_"))
def handle_rukuni_d_callbacks(c):
    uid = c.from_user.id
    try:
        bot.answer_callback_query(c.id)
    except:
        pass

    try:
        _, ctype, page = c.data.split("_", 2)
        page = int(page)
    except:
        return

    if ctype == "search":
        send_search_results(uid, page, c.message)
    elif ctype == "algaita":
        send_algaita_movies(uid, page, c.message)
    elif ctype == "hausa":
        send_hausa_series(uid, page, c.message)
    elif ctype == "others":
        send_others_movies(uid, page, c.message)


@bot.callback_query_handler(func=lambda c: c.data == "search_cancel")
def handle_search_cancel(c):
    uid = c.from_user.id
    try:
        bot.answer_callback_query(c.id)
    except:
        pass

    user_states.pop(uid, None)

    safe_edit(
        c.message.chat.id,
        c.message.message_id,
        "❌ <b>An fasa.</b>\n\nKa zabi wani abu daga menu."
    )


# ================== END RUKUNI A ==================


# DUKKAN HANDLERS SUN GAMA ↑↑↑


@bot.callback_query_handler(func=lambda c: True)
def handle_callback(c):
    try:
        uid = c.from_user.id
        data = c.data or ""
    except:
        return


# ======================= MAIN CALLBACK HANDLER =======================


    # =====================
    # VIEW CART (SEND + SAVE MESSAGE)
    # =====================
    if data == "viewcart":
        text, kb = build_cart_view(uid)

        msg = bot.send_message(
            uid,
            text,
            reply_markup=kb,
            parse_mode="HTML"
        )

        cart_sessions[uid] = msg.message_id
        bot.answer_callback_query(c.id)
        return


    # =====================
    # REMOVE FROM CART (SINGLE + GROUP)
    # =====================
    if data.startswith("removecart:"):
        raw = data.split("removecart:", 1)[1]

        try:
            ids = [int(i) for i in raw.split("_") if i.isdigit()]
        except:
            bot.answer_callback_query(c.id, "❌ Invalid remove id")
            return

        if not ids:
            bot.answer_callback_query(c.id, "❌ Babu abin cirewa")
            return

        try:
            conn = get_conn()
            cur = conn.cursor()

            for item_id in ids:
                cur.execute(
                    "DELETE FROM cart WHERE user_id=%s AND item_id=%s",
                    (uid, item_id)
                )

            conn.commit()
            cur.close()
        except:
            conn.rollback()
            bot.answer_callback_query(c.id, "❌ Remove failed")
            return

        text, kb = build_cart_view(uid)

        if uid in cart_sessions:
            try:
                bot.edit_message_text(
                    text,
                    uid,
                    cart_sessions[uid],
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except:
                pass

        bot.answer_callback_query(c.id, "🗑 An cire")
        return


    # =====================
    # CLEAR CART (DUKKA)
    # =====================
    if data == "clearcart":
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM cart WHERE user_id=%s",
                (uid,)
            )
            conn.commit()
            cur.close()
        except:
            conn.rollback()
            bot.answer_callback_query(c.id, "❌ Clear failed")
            return

        bot.answer_callback_query(c.id, "🧹 An goge cart")

        msg_id = cart_sessions.get(uid)
        if msg_id:
            text, kb = build_cart_view(uid)
            try:
                bot.edit_message_text(
                    chat_id=uid,
                    message_id=msg_id,
                    text=text,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except:
                pass
        return



    # ================= ADD ITEM(S) TO CART (DM / CHANNEL) =================
    if data.startswith("addcartdm:"):
        import re

        raw = data.split(":", 1)[1]

        # Support: _, comma, space (mixed allowed)
        tokens = [x.strip() for x in re.split(r"[_,\s]+", raw) if x.strip()]
        if not tokens:
            bot.answer_callback_query(c.id, "❌ Invalid")
            return

        added = 0
        skipped = 0

        try:
            conn = get_conn()
            cur = conn.cursor()

            for token in tokens:

                item_ids = []

                # ================= IF ID =================
                if token.isdigit():
                    item_ids = [int(token)]

                # ================= IF GROUP KEY =================
                else:
                    cur.execute(
                        "SELECT id FROM items WHERE group_key=%s",
                        (token,)
                    )
                    rows = cur.fetchall()
                    item_ids = [r[0] for r in rows]

                if not item_ids:
                    continue

                # ================= INSERT ITEMS =================
                for item_id in item_ids:

                    cur.execute(
                        "SELECT 1 FROM cart WHERE user_id=%s AND item_id=%s LIMIT 1",
                        (uid, item_id)
                    )
                    if cur.fetchone():
                        skipped += 1
                        continue

                    cur.execute(
                        "INSERT INTO cart (user_id, item_id) VALUES (%s, %s)",
                        (uid, item_id)
                    )
                    added += 1

            conn.commit()
            cur.close()
            conn.close()

        except:
            try:
                conn.rollback()
            except:
                pass
            bot.answer_callback_query(c.id, "❌ Add to cart failed")
            return

        # ================= RESPONSE =================
        if added and skipped:
            bot.answer_callback_query(
                c.id,
                f"✅ An saka {added} | ⚠️ {skipped} suna cart"
            )
        elif added:
            bot.answer_callback_query(
                c.id,
                f"✅ An saka {added} item(s) a cart"
            )
        else:
            bot.answer_callback_query(
                c.id,
                "⚠️ Duk suna cikin cart"
            )

        return
    # ==================================================


    from psycopg2.extras import RealDictCursor
    import uuid

    # ==================================================
    # CHECKOUT (CART)
    # ==================================================
    if data == "checkout":
        rows = get_cart(uid)
        if not rows:
            bot.answer_callback_query(c.id, "❌ Cart ɗinka babu komai.")
            return

        groups = {}
        total = 0

        for item_id, title, price, file_id, group_key in rows:
            if not file_id:
                continue

            p = int(price or 0)
            if p <= 0:
                continue

            key = group_key or f"single_{item_id}"

            if key not in groups:
                groups[key] = {
                    "price": p,
                    "items": []
                }

            groups[key]["items"].append((item_id, title, file_id))

        if not groups:
            bot.answer_callback_query(c.id, "❌ Babu item mai delivery a cart.")
            return

        for g in groups.values():
            total += g["price"]

        if total <= 0:
            bot.answer_callback_query(c.id, "❌ Farashi bai dace ba.")
            return

        order_id = str(uuid.uuid4())

        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute(
                "INSERT INTO orders (id,user_id,amount,paid) VALUES (%s,%s,%s,0)",
                (order_id, uid, total)
            )

            for g in groups.values():
                for item_id, title, file_id in g["items"]:
                    cur.execute(
                        """
                        INSERT INTO order_items
                        (order_id,item_id,file_id,price)
                        VALUES (%s,%s,%s,%s)
                        """,
                        (order_id, item_id, file_id, g["price"])
                    )

            conn.commit()
            cur.close()
            conn.close()

        except:
            try:
                conn.rollback()
            except:
                pass
            bot.answer_callback_query(c.id, "❌ Checkout failed.")
            return

        clear_cart(uid)

        pay_url = create_flutterwave_payment(uid, order_id, total, "Cart Order")
        if not pay_url:
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

        bot.send_message(
            uid,
            f"""🎉 Thank you

🧾 CART ORDER
📦 Groups: {len(groups)}
💰 Total: ₦{total}

🆔 Order ID:
{order_id}

💳 Please complete your payment below 👇""",
            reply_markup=kb
        )

        bot.answer_callback_query(c.id)
        return

    # ==================================================
    # BUY / BUYDM
    # ==================================================
    if data.startswith("buy:") or data.startswith("buydm:"):

        try:
            raw = data.split(":", 1)[1]
            item_ids = [int(x) for x in raw.split(",") if x.strip().isdigit()]
        except:
            bot.answer_callback_query(c.id, "❌ Invalid buy data.")
            return

        if not item_ids:
            bot.answer_callback_query(c.id, "❌ No item selected.")
            return

        try:
            conn = get_conn()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            placeholders = ",".join(["%s"] * len(item_ids))

            cur.execute(
                f"""
                SELECT id,title,price,file_id,group_key
                FROM items
                WHERE id IN ({placeholders})
                """,
                tuple(item_ids)
            )
            rows = cur.fetchall()

            items = [
                r for r in rows
                if r["file_id"] and int(r["price"] or 0) > 0
            ]

            if not items:
                cur.close()
                conn.close()
                bot.answer_callback_query(c.id, "❌ Babu item mai delivery.")
                return

            # OWNERSHIP CHECK
            ids_clean = [i["id"] for i in items]
            placeholders2 = ",".join(["%s"] * len(ids_clean))

            cur.execute(
                f"""
                SELECT 1 FROM user_movies
                WHERE user_id=%s AND item_id IN ({placeholders2})
                LIMIT 1
                """,
                (uid, *ids_clean)
            )
            if cur.fetchone():
                cur.close()
                conn.close()

                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("🎬 MY MOVIES", callback_data="my_movies"))

                bot.send_message(
                    uid,
                    "✅ <b>Ka riga ka mallaki wannan fim.</b>",
                    parse_mode="HTML",
                    reply_markup=kb
                )
                return

            # GROUP TOTAL
            groups = {}
            for i in items:
                key = i["group_key"] or f"single_{i['id']}"
                if key not in groups:
                    groups[key] = int(i["price"] or 0)

            total = sum(groups.values())

            # EXACT UNPAID REUSE
            cur.execute(
                f"""
                SELECT o.id
                FROM orders o
                JOIN order_items oi ON oi.order_id=o.id
                WHERE o.user_id=%s
                  AND o.paid=0
                  AND oi.item_id IN ({placeholders2})
                GROUP BY o.id
                HAVING COUNT(DISTINCT oi.item_id)=%s
                LIMIT 1
                """,
                (uid, *ids_clean, len(ids_clean))
            )
            old = cur.fetchone()

            if old:
                order_id = old["id"]
            else:
                order_id = str(uuid.uuid4())

                cur.execute(
                    "INSERT INTO orders (id,user_id,amount,paid) VALUES (%s,%s,%s,0)",
                    (order_id, uid, total)
                )

                for i in items:
                    cur.execute(
                        """
                        INSERT INTO order_items
                        (order_id,item_id,file_id,price)
                        VALUES (%s,%s,%s,%s)
                        """,
                        (order_id, i["id"], i["file_id"], int(i["price"] or 0))
                    )

                conn.commit()

            cur.close()
            conn.close()

        except:
            try:
                conn.rollback()
            except:
                pass
            bot.answer_callback_query(c.id, "❌ Buy failed.")
            return

        title = items[0]["title"] if len(items) == 1 else f"{len(items)} Items"

        pay_url = create_flutterwave_payment(uid, order_id, total, title)
        if not pay_url:
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("💳 PAY NOW", url=pay_url))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{order_id}"))

        bot.send_message(
            uid,
            f"""🎉 Thank you

🧾 {title}
📦 Items: {len(items)}
💰 Total: ₦{total}

🆔 Order ID:
{order_id}

💳 Please complete your payment below 👇""",
            reply_markup=kb
        )

        bot.answer_callback_query(c.id)
        return



    
    # ================= MY MOVIES =================
    if data == "my_movies":
        kb = InlineKeyboardMarkup()

        kb.add(InlineKeyboardButton("🔍BINCIKO TA SUNA", callback_data="_resend_search_"))
        kb.add(InlineKeyboardButton("🗓 Last 7 days", callback_data="resend:7"))
        kb.add(InlineKeyboardButton("📆 Last 30 days", callback_data="resend:30"))
        kb.add(InlineKeyboardButton("🕰 Last 90 days", callback_data="resend:90"))

        bot.send_message(
            uid,
            "🎬 <b>My Movies</b>\n"
            "Za a sake turo maka fina-finan da ka taba siya.\n\n"
            "🔍 Idan bincike ne, rubuta sunan fim:",
            parse_mode="HTML",
            reply_markup=kb
        )

        bot.answer_callback_query(c.id)
        return


    # ================= 🔍 RESEND SEARCH (STATE SETTER) =================
    if data == "_resend_search_":
        # ✅ NAN NE MATSALAR DA GYARA
        user_states[uid] = {"action": "_resend_search_"}

        bot.send_message(
            uid,
            "🔍 <b>Binciko ta suna</b>\n"
            "Rubuta sunan fim ɗin da kake nema:",
            parse_mode="HTML"
        )

        bot.answer_callback_query(c.id)
        return



# ================= RESEND BY DAYS =================
    if data.startswith("resend:"):
        try:
            days = int(data.split(":")[1])
        except:
            bot.answer_callback_query(c.id, "❌ Invalid time.")
            return

        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT COUNT(*) FROM resend_logs WHERE user_id=%s",
                (uid,)
            )
            used = cur.fetchone()[0]

            if used >= 10:
                cur.close()
                bot.send_message(
                    uid,
                    "⚠️ Ka kai iyakar sake karɓa (sau 10).\nSai ka sake siya domin a turo maka."
                )
                bot.answer_callback_query(c.id)
                return

            cur.execute("""
                SELECT DISTINCT ui.item_id, i.file_id, i.title
                FROM user_movies ui
                JOIN items i ON i.id = ui.item_id
                WHERE ui.user_id = %s
                  AND ui.created_at >= NOW() - INTERVAL %s
                ORDER BY ui.created_at ASC
            """, (uid, f"{days} days"))

            rows = cur.fetchall()

            if not rows:
                cur.close()
                bot.send_message(uid, "❌ Babu fim a wannan lokacin.")
                bot.answer_callback_query(c.id)
                return

            for item_id, file_id, title in rows:
                try:
                    try:
                        bot.send_video(uid, file_id, caption=f"🎬 {title}")
                    except:
                        bot.send_document(uid, file_id, caption=f"🎬 {title}")
                except Exception as e:
                    print("Resend error:", e)

            cur.execute(
                "INSERT INTO resend_logs (user_id, used_at) VALUES (%s, NOW())",
                (uid,)
            )

            conn.commit()
            cur.close()

        except:
            conn.rollback()
            bot.answer_callback_query(c.id, "❌ Resend failed.")
            return

        bot.send_message(
            uid,
            f"✅ An sake tura fina-finai ({len(rows)}).\n⚠️ Ka tuna: sau 10 kawai zaka iya karɓa."
        )
        bot.answer_callback_query(c.id)
        return


    # ================= RESEND ONE ITEM =================
    if data.startswith("resend_one:"):
        try:
            item_id = int(data.split(":", 1)[1])
        except:
            bot.answer_callback_query(c.id, "❌ Invalid movie.")
            return

        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT COUNT(*) FROM resend_logs WHERE user_id=%s",
                (uid,)
            )
            used = cur.fetchone()[0]

            if used >= 10:
                cur.close()
                bot.send_message(
                    uid,
                    "⚠️ Ka kai iyakar sake karɓa (sau 10).\n"
                    "Sai ka sake siya domin a turo maka."
                )
                bot.answer_callback_query(c.id)
                return

            cur.execute("""
                SELECT i.file_id, i.title
                FROM user_movies ui
                JOIN items i ON i.id = ui.item_id
                WHERE ui.user_id=%s AND ui.item_id=%s
                LIMIT 1
            """, (uid, item_id))

            row = cur.fetchone()

            if not row:
                cur.close()
                bot.answer_callback_query(c.id, "❌ Ba a samu fim ba.")
                return

            file_id, title = row

            try:
                try:
                    bot.send_video(uid, file_id, caption=f"🎬 {title}")
                except:
                    bot.send_document(uid, file_id, caption=f"🎬 {title}")
            except:
                cur.close()
                bot.answer_callback_query(c.id, "❌ Kuskure wajen tura fim.")
                return

            cur.execute(
                "INSERT INTO resend_logs (user_id, used_at) VALUES (%s, NOW())",
                (uid,)
            )

            conn.commit()
            cur.close()

        except:
            conn.rollback()
            bot.answer_callback_query(c.id, "❌ Resend failed.")
            return

        bot.answer_callback_query(
            c.id,
            "✅ An sake tura muku fim.\n⚠️ Ka sani: sau 10 kawai zaka iya karɓa."
        )
        return

     # ================= START SERIES MODE =================
    if data == "start_series":
        series_sessions[uid] = {
            "stage": "collect",
            "files": [],
            "hausa_titles": [],
            "hausa_matches": []
        }

        bot.answer_callback_query(c.id)
        bot.send_message(
            uid,
            "📦 <b>Series mode ya fara</b>\n\n"
            "➡️ Turo dukkan fina-finai (video ko document)\n"
            "➡️ Idan ka gama, rubuta <b>done</b>",
            parse_mode="HTML"
        )
        return




    # =====================
    # OPEN UNPAID ORDERS (PAGE 0)
    # =====================
    if data == "myorders_new":
        text, kb = build_unpaid_orders_view(uid, page=0)
        bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(c.id)
        return

    # =====================
    # UNPAID PAGINATION
    # =====================
    if data.startswith("unpaid_next:") or data.startswith("unpaid_prev:"):
        page = int(data.split(":")[1])
        text, kb = build_unpaid_orders_view(uid, page)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return


 # =====================
    # REMOVE SINGLE UNPAID
    # =====================
    if data.startswith("remove_unpaid:"):
        oid = data.split(":")[1]

        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                "DELETE FROM orders WHERE id=%s AND user_id=%s AND paid=0",
                (oid, uid)
            )

            conn.commit()
            cur.close()
        except:
            conn.rollback()
            bot.answer_callback_query(c.id, "❌ Remove failed")
            return

        text, kb = build_unpaid_orders_view(uid, page=0)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id, "❌ An cire order")
        return

    # ===============================
    # SERIES MODE (ADMIN ONLY)
    # ===============================
    if data == "groupitems":
        if uid != ADMIN_ID:
            return bot.answer_callback_query(c.id, "groupitems.")

        series_sessions[uid] = {
            "files": [],
            "stage": "collect"
        }

        bot.send_message(
            uid,
            "📺 <b>Series Mode ya fara</b>\n\n"
            "Ka fara turo videos/documents.\n"
            "Idan ka gama rubuta <b>Done</b>.",
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return

    # =====================
    # DELETE ALL UNPAID
    # =====================
    if data == "delete_unpaid":

        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                "DELETE FROM orders WHERE user_id=%s AND paid=0",
                (uid,)
            )

            conn.commit()
            cur.close()
        except:
            conn.rollback()
            bot.answer_callback_query(c.id, "❌ Delete failed")
            return

        text, kb = build_unpaid_orders_view(uid, page=0)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id, "🗑 Duk an goge")
        return  

    # =====================
    # OPEN PAID ORDERS (PAGE 0)
    # =====================
    if data == "paid_orders":
        text, kb = build_paid_orders_view(uid, page=0)
        bot.edit_message_text(
            chat_id=uid,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
        bot.answer_callback_query(c.id)
        return

    #
    if data == "allfilms_prev":
        sess = allfilms_sessions.get(uid)
        if not sess:
            bot.answer_callback_query(c.id)
            return
        idx = sess["index"] - 1
        if idx >= 0:
            send_allfilms_page(uid, idx)
        bot.answer_callback_query(c.id)
        return


   




    # ================= FEEDBACK =================
    if data.startswith("feedback:"):
        parts = data.split(":")
        if len(parts) != 3:
            bot.answer_callback_query(c.id)
            return

        mood, order_id = parts[1], parts[2]

        conn = None
        cur = None

        try:
            conn = get_conn()
            cur = conn.cursor()

            # 1️⃣ Tabbatar order paid ne kuma na user
            cur.execute(
                "SELECT 1 FROM orders WHERE id=%s AND user_id=%s AND paid=1",
                (order_id, uid)
            )
            row = cur.fetchone()
            if not row:
                bot.answer_callback_query(
                    c.id,
                    "⚠️ Wannan order ba naka bane.",
                    show_alert=True
                )
                return

            # 2️⃣ Hana feedback sau biyu
            cur.execute(
                "SELECT 1 FROM feedbacks WHERE order_id=%s",
                (order_id,)
            )
            exists = cur.fetchone()
            if exists:
                bot.answer_callback_query(
                    c.id,
                    "Ka riga ka bada ra'ayi.",
                    show_alert=True
                )
                return

            # 3️⃣ Ajiye feedback
            cur.execute(
                "INSERT INTO feedbacks (order_id, user_id, mood) VALUES (%s,%s,%s)",
                (order_id, uid, mood)
            )

            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            bot.answer_callback_query(c.id, "❌ Feedback error.")
            return

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        # 4️⃣ Samo sunan user
        try:
            chat = bot.get_chat(uid)
            fname = chat.first_name or "User"
        except:
            fname = "User"

        admin_messages = {
            "very": (
                "😘 Gaskiya na ji daɗin siyayya da bot ɗinku\n"
                "Alhamdulillah wannan bot yana sauƙaƙa siyan fim sosai 😇\n"
                "Muna godiya ƙwarai 🥰🙏"
            ),
            "good": (
                "🙂 Na ji daɗin siyayya\n"
                "Tsarin bot ɗin yana da kyau kuma mai sauƙi"
            ),
            "neutral": (
                "😓 Ban gama fahimtar bot ɗin sosai ba\n"
                "Amma ina ganin yana da amfani"
            ),
            "angry": (
                "🤬 Wannan bot yana bani ciwon kai\n"
                "Akwai buƙatar ku gyara tsarin kasuwancin ku"
            )
        }

        user_replies = {
            "very": "🥰 Mun gode sosai! Za mu ci gaba da faranta maka rai Insha Allah.",
            "good": "😊 Mun gode da ra'ayinka! Za mu ƙara inganta tsarin.",
            "neutral": "🤍 Mun gode. Idan kana da shawara, muna maraba da ita.",
            "angry": "🙏 Muna baku haƙuri akan bacin ran da kuka samu. Za mu gyara Insha Allah."
        }

        # 5️⃣ Tura wa ADMIN
        admin_text = (
            f"📣 FEEDBACK RECEIVED\n\n"
            f"👤 User: {fname}\n"
            f"🆔 ID: {uid}\n"
            f"📦 Order: {order_id}\n\n"
            f"{admin_messages.get(mood, mood)}"
        )

        try:
            bot.send_message(ADMIN_ID, admin_text)
        except:
            pass

        # 6️⃣ Goge inline buttons
        try:
            bot.edit_message_reply_markup(
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                reply_markup=None
            )
        except:
            pass

        bot.answer_callback_query(c.id)
        bot.send_message(
            uid,
            user_replies.get(mood, "Mun gode da ra'ayinka 🙏")
        )
        return

    # =====================
    # ADD MOVIE (ADMIN)
    # =====================
    if data == "addmovie":
        if uid != ADMIN_ID:
            bot.answer_callback_query(c.id, "Only admin.")
            return
        admin_states[uid] = {"state": "add_movie_wait_file"}
        bot.send_message(uid, "Turo film.")
        bot.answer_callback_query(c.id)
        return
    # =====================


# WEEKLY BUY
    # =====================
    if data.startswith("weekly_buy:"):
        try:
            idx = int(data.split(":",1)[1])
        except:
            bot.answer_callback_query(c.id, "Invalid.")
            return

        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT items FROM weekly ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()

            if not row:
                cur.close()
                bot.answer_callback_query(c.id, "No weekly data.")
                return

            items = json.loads(row[0] or "[]")

            if idx < 0 or idx >= len(items):
                cur.close()
                bot.answer_callback_query(c.id, "Invalid item.")
                return

            item = items[idx]

            title = item["title"]
            price = int(item["price"])

            cur.close()

        except Exception as e:
            conn.rollback()
            bot.answer_callback_query(c.id, "Weekly error.")
            return

        remaining_price, applied_sum, applied_ids = apply_credits_to_amount(uid, price)
        order_id = create_single_order_for_weekly(uid, title, remaining_price)

        bot.send_message(uid, f"Oda {order_id} – ₦{remaining_price}")
        bot.answer_callback_query(c.id)
        return    
    # ======================================================
    # ================= ALL FILMS OPEN =====================
    # ======================================================
    if data == "all_films":
        rows = build_allfilms_rows()
        if not rows:
            bot.answer_callback_query(c.id, "❌ Babu fim a DB")
            return

        pages = paginate(rows, PER_PAGE)

        allfilms_sessions[uid] = {
            "pages": pages,
            "index": 0,
            "last_msg": c.message.message_id
        }

        send_allfilms_page(uid, 0)
        bot.answer_callback_query(c.id)
        return

    # ======================================================
    # ================= ALL FILMS NEXT =====================
    # ======================================================
    if data == "allfilms_next":
        sess = allfilms_sessions.get(uid)
        if not sess:
            bot.answer_callback_query(c.id)
            return

        send_allfilms_page(uid, sess["index"] + 1)
        bot.answer_callback_query(c.id)
        return

    # ======================================================
    # ================= ALL FILMS PREV =====================
    # ======================================================
    if data == "allfilms_prev":
        sess = allfilms_sessions.get(uid)
        if not sess:
            bot.answer_callback_query(c.id)
            return

        send_allfilms_page(uid, sess["index"] - 1)
        bot.answer_callback_query(c.id)
        return


 # Map new erase_all_data callback to existing erase_data handler (compat shim)
    if data == "erase_all_data":
        data = "erase_data"


    # NEW WEAK UPDATE SYSTEM
    if data == "weak_update":
        start_weak_update(msg=c.message)
        return
    # checkjoin: after user clicks I've Joined, prompt language selection
    if data == "checkjoin":
        try:
            if check_join(uid):
                bot.answer_callback_query(callback_query_id=c.id, text=tr_user(uid, "joined_ok", default="✔ An shiga channel!"))
                # prompt language selection now
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("English", callback_data="setlang_en"),
                       InlineKeyboardButton("Françe", callback_data="setlang_fr"))
                kb.add(InlineKeyboardButton("Hausa", callback_data="setlang_ha"),
                       InlineKeyboardButton("Igbo", callback_data="setlang_ig"))
                kb.add(InlineKeyboardButton("Yaruba", callback_data="setlang_yo"),
                       InlineKeyboardButton("Fulani/Fulfulde", callback_data="setlang_ff"))
                bot.send_message(uid, tr_user(uid, "choose_language_prompt", default="Choose language / Zaɓi harshe:"), reply_markup=kb)
            else:
                bot.answer_callback_query(callback_query_id=c.id, text=tr_user(uid, "not_joined", default="❌ Baka shiga ba."))
        except Exception as e:
            print("checkjoin callback error:", e)
        return

    # show change language menu (global button)
    if data == "change_language":
        try:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("English", callback_data="setlang_en"),
                   InlineKeyboardButton("Françe", callback_data="setlang_fr"))
            kb.add(InlineKeyboardButton("Hausa", callback_data="setlang_ha"),
                   InlineKeyboardButton("Igbo", callback_data="setlang_ig"))
            kb.add(InlineKeyboardButton("Yaruba", callback_data="setlang_yo"),
                   InlineKeyboardButton("Fulani/Fulfulde", callback_data="setlang_ff"))
            bot.answer_callback_query(callback_query_id=c.id)
            bot.send_message(uid, tr_user(uid, "choose_language_prompt", default="Choose language / Zaɓi harshe:"), reply_markup=kb)
        except Exception as e:
            print("change_language callback error:", e)
        return

    # set language callbacks
    if data.startswith("setlang_"):
        lang = data.split("_",1)[1]
        set_user_lang(uid, lang)
        # If Hausa selected, keep original Hausa text
        if lang == "ha":
            bot.answer_callback_query(callback_query_id=c.id, text="An saita Hausa. (Ba a canza rubutu Hausa ba.)")
            bot.send_message(uid, "Abokin kasuwanci barka da zuwa shagon fina finai:", reply_markup=user_main_menu(uid))
            bot.send_message(uid, "Sannu da zuwa!\n Me kake bukata?:", reply_markup=reply_menu(uid))
            return
        # for other languages, use translations where available
        welcome = tr_user(uid, "welcome_shop", default="Abokin kasuwanci barka da zuwa shagon fina finai:")
        ask = tr_user(uid, "ask_name", default="Sannu da zuwa!\n Me kake bukata?:")
        bot.answer_callback_query(callback_query_id=c.id, text=tr_user(uid, "language_set_success", default="Language set."))
        bot.send_message(uid, welcome, reply_markup=user_main_menu(uid))
        bot.send_message(uid, ask, reply_markup=reply_menu(uid))
        return

    # go home
    if data == "go_home":
        try:
            bot.answer_callback_query(callback_query_id=c.id)
            bot.send_message(uid, "Sannu! Ga zabuka, domin fara wa:", reply_markup=reply_menu(uid))
        except:
            pass
        return

    if data == "invite":
        try:
            bot_info = bot.get_me()
            bot_username = bot_info.username if bot_info and getattr(bot_info, "username", None) else None
        except:
            bot_username = None
        if bot_username:
            ref_link = f"https://t.me/{bot_username}?start=ref{uid}"
            share_url = "https://t.me/share/url?"+urllib.parse.urlencode({
                "url": ref_link,
                "text": f"Gayyato ni zuwa wannan bot: {ref_link}\nJoin channel: https://t.me/{CHANNEL.lstrip('@')}\nKa samu lada idan wanda ka gayyata yayi join sannan ya siya fim 3×."
            })
        else:
            ref_link = f"/start ref{uid}"
            share_url = f"https://t.me/{CHANNEL.lstrip('@')}"
        text = (
            "Gayyato abokanka👨‍👨‍👦‍👦 suyi join domin samun GARABASA!🎁\n\n"
            "Ka tura musu wannan link ɗin.\n\n"
            "Idan wanda ka gayyata ya shiga channel ɗinmu kuma ya sayi fim uku, za'a baka N200🎊🎉\n"
            "10 friends N2000😲🥳🤑\n(yi amfani Kyautar wajen sayen fim).\n\n"
            "Danne alamar COPY karka daga zaka samu damar kofe link din ka, ko!\n"
            "ka taba 📤SHARE kai tsaye"
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔗 Copy / Open Link", url=ref_link))
        kb.add(InlineKeyboardButton("📤 Share", url=share_url))
        kb.row(InlineKeyboardButton("👥 My referrals", callback_data="my_referrals"),
               InlineKeyboardButton("💰 My credits", callback_data="my_credits"))
        kb.row(InlineKeyboardButton(" ⤴️ KOMA FARKO", callback_data="go_home"),
               InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        change_label = tr_user(uid, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        bot.answer_callback_query(callback_query_id=c.id)
        bot.send_message(uid, text, reply_markup=kb)
        return

    if data == "my_referrals":
        rows = get_referrals_by_referrer(uid)
        if not rows:
            bot.answer_callback_query(callback_query_id=c.id, text="Babu wanda ka gayyata tukuna.")
            bot.send_message(uid, "Babu wanda ka gayyata tukuna.", reply_markup=reply_menu(uid))
            return
        text = "Mutanen da ka gayyata:\n\n"
        for referred_id, created_at, reward_granted, rowid in rows:
            name = None
            try:
                chat = bot.get_chat(referred_id)
                fname = getattr(chat, "first_name", "") or ""
                uname = getattr(chat, "username", None)
                if uname:
                    name = "@" + uname
                elif fname:
                    name = fname
            except:
                s = str(referred_id)
                name = s[:3] + "*"*(len(s)-6) + s[-3:] if len(s) > 6 else "User"+s[-4:]
            status = "+reward success" if reward_granted else "pending👀"
            text += f"• {name} — {status}\n"
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton(" ⤴️ KOMA FARKO", callback_data="go_home"),
               InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        change_label = tr_user(uid, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        bot.answer_callback_query(callback_query_id=c.id)
        bot.send_message(uid, text, reply_markup=kb)
        return

    if data == "my_credits":
        total, rows = get_credits_for_user(uid)
        text = f"Total available credit: N{total}\n\n"
        for cid, amount, used, granted_at in rows:
            text += f"• ID:{cid} — N{amount} — {'USED' if used else 'AVAILABLE'} — {granted_at}\n"
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton(" ⤴️ KOMA FARKO", callback_data="go_home"),
               InlineKeyboardButton("🫂Our Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}"))
        change_label = tr_user(uid, "change_language_button", default="🌐 Change your language")
        kb.row(InlineKeyboardButton(change_label, callback_data="change_language"))
        bot.answer_callback_query(callback_query_id=c.id)
        bot.send_message(uid, text, reply_markup=kb)
        return



    # Support Help -> Open admin DM directly (NO messages to admin, NO notifications)
    if data == "support_help":
        try:
            bot.answer_callback_query(callback_query_id=c.id)
        except:
            pass

        if ADMIN_USERNAME:
            # Open admin DM directly
            bot.send_message(uid, f"👉 Tuntuɓi admin kai tsaye: https://t.me/{ADMIN_USERNAME}")
        else:
            bot.send_message(uid, "Admin username bai sa ba. Tuntubi support.")
        return


    # fallback
    try:
        bot.answer_callback_query(callback_query_id=c.id)
    except:
        pass






# ========== /myorders command (SAFE – ITEMS BASED | POSTGRESQL) ==========
@bot.message_handler(commands=["myorders"])
def myorders(message):
    uid = message.from_user.id

    conn = None
    cur = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, amount, paid
            FROM orders
            WHERE user_id=%s
            ORDER BY id DESC
            """,
            (uid,)
        )

        rows = cur.fetchall()

        if not rows:
            bot.reply_to(
                message,
                "❌ Babu odarka tukuna.",
                reply_markup=reply_menu(uid)
            )
            return

        txt = "🧾 <b>Your Orders</b>\n\n"

        for row in rows:
            oid = row[0]
            amount = int(row[1] or 0)
            paid = row[2]

            # 🔒 SAFE COUNT (order_items ONLY)
            cur.execute(
                """
                SELECT COUNT(*) 
                FROM order_items
                WHERE order_id=%s
                """,
                (oid,)
            )

            info = cur.fetchone()
            items_count = info[0] if info else 0

            if items_count <= 0:
                continue

            label = "1 item" if items_count == 1 else f"Group items ({items_count})"

            txt += (
                f"🆔 <code>{oid}</code>\n"
                f"📦 {label}\n"
                f"💰 Amount: ₦{amount}\n"
                f"💳 Status: {'✅ Paid' if paid else '❌ Unpaid'}\n\n"
            )

        bot.send_message(
            uid,
            txt,
            parse_mode="HTML",
            reply_markup=reply_menu(uid)
        )

    except Exception as e:
        if conn:
            conn.rollback()
        bot.send_message(uid, "❌ System error wajen karanta orders.")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
# ========== ADMIN FILE UPLOAD (ITEMS ONLY, FIXED) ==========
@bot.message_handler(content_types=["photo", "video", "document"])
def file_upload(message):

    # 1️⃣ IDAN ADMIN NA CIKIN WANI FLOW
    if message.from_user.id in ADMINS and admin_states.get(message.from_user.id):
        try:
            admin_inputs(message)
        except Exception as e:
            bot.send_message(ADMIN_ID, f"❌ admin_inputs error: {e}")
        return

    # 2️⃣ IDAN POST YA FITO DAGA CHANNEL
    chat_username = getattr(message.chat, "username", None)
    if chat_username and ("@" + chat_username).lower() == CHANNEL.lower():

        caption = message.caption or ""
        title, price = parse_caption_for_title_price(caption)

        if not title:
            title = f"Item {uuid.uuid4().hex[:6]}"
            price = 0

        # FILE ID
        if message.content_type == "photo":
            file_id = message.photo[-1].file_id
            file_type = "photo"
        elif message.content_type == "video":
            file_id = message.video.file_id
            file_type = "video"
        else:
            file_id = message.document.file_id
            file_type = "document"

        try:
            exists = conn.execute(
                "SELECT id FROM items WHERE title=? COLLATE NOCASE",
                (title,)
            ).fetchone()

            if exists:
                bot.send_message(
                    ADMIN_ID,
                    f"⚠️ ITEM YA RIGA YA WUJU\n<b>{title}</b>",
                    parse_mode="HTML"
                )
                return

            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            channel_msg_id = message.message_id

            cur = conn.execute(
                """
                INSERT INTO items
                (title, price, file_id, file_type, created_at, channel_msg_id)
                VALUES (?,?,?,?,?,?)
                """,
                (title, price or 0, file_id, file_type, now, channel_msg_id)
            )
            conn.commit()

            item_id = cur.lastrowid

            bot.send_message(
                ADMIN_ID,
                f"✅ <b>ITEM AN ADANA</b>\n\n"
                f"🆔 ID: <code>{item_id}</code>\n"
                f"🎬 Title: {title}\n"
                f"💰 Price: ₦{price}",
                parse_mode="HTML"
            )

        except Exception as e:
            bot.send_message(
                ADMIN_ID,
                f"❌ ERROR YAYIN SAVE ITEM:\n{e}"
            )

        return

    # 3️⃣ IDAN ADMIN YA TURA FILE A PRIVATE
    if message.from_user.id != ADMIN_ID:
        return

    caption = message.caption or ""
    title, price = parse_caption_for_title_price(caption)

    if not title:
        title = f"Item {uuid.uuid4().hex[:6]}"
        price = 0

    if message.content_type == "photo":
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.content_type == "video":
        file_id = message.video.file_id
        file_type = "video"
    else:
        file_id = message.document.file_id
        file_type = "document"

    try:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        cur = conn.execute(
            """
            INSERT INTO items
            (title, price, file_id, file_type, created_at)
            VALUES (?,?,?,?,?)
            """,
            (title, price or 0, file_id, file_type, now)
        )
        conn.commit()

        item_id = cur.lastrowid

        post_caption = (
            f"🎬 <b>{title}</b>\n"
            f"💵 ₦{price}\n"
            f"Danna maɓalli domin saya ko saka a cart."
        )

        markup = item_buttons_inline(item_id)

        if file_type == "photo":
            sent = bot.send_photo(
                CHANNEL,
                file_id,
                caption=post_caption,
                parse_mode="HTML",
                reply_markup=markup
            )
        elif file_type == "video":
            sent = bot.send_video(
                CHANNEL,
                file_id,
                caption=post_caption,
                parse_mode="HTML",
                reply_markup=markup
            )
        else:
            sent = bot.send_document(
                CHANNEL,
                file_id,
                caption=post_caption,
                parse_mode="HTML",
                reply_markup=markup
            )

        conn.execute(
            "UPDATE items SET channel_msg_id=? WHERE id=?",
            (sent.message_id, item_id)
        )
        conn.commit()

        bot.send_message(
            ADMIN_ID,
            f"✅ <b>AN TURA ZUWA CHANNEL</b>\n"
            f"🆔 Item ID: <code>{item_id}</code>",
            parse_mode="HTML"
        )

    except Exception as e:
        bot.send_message(
            ADMIN_ID,
            f"❌ POST FAILED:\n{e}"
        )




# ================== SALES REPORT SYSTEM (ITEMS BASED) ==================

import threading
import time
from datetime import datetime, timedelta


def _ng_now():
    return datetime.utcnow() + timedelta(hours=1)


def _last_day_of_month(dt):
    next_month = dt.replace(day=28) + timedelta(days=4)
    return (next_month - timedelta(days=next_month.day)).day


# ================= WEEKLY REPORT =================
def send_weekly_sales_report():
    try:
        if not PAYMENT_NOTIFY_GROUP:
            return

        now = _ng_now()
        week_ago = now - timedelta(days=7)

        rows = conn.execute(
            """
            SELECT
                oi.item_id,
                COUNT(*) AS qty,
                SUM(COALESCE(oi.price,0)) AS total
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.paid = 1
              AND o.created_at >= ?
            GROUP BY oi.item_id
            """,
            (week_ago.strftime("%Y-%m-%d %H:%M:%S"),)
        ).fetchall()

        if not rows:
            bot.send_message(
                PAYMENT_NOTIFY_GROUP,
                "📊 WEEKLY SALES REPORT\n\nBabu siyarwa."
            )
            return

        msg = "📊 WEEKLY SALES REPORT\n\n"
        grand = 0

        for item_id, qty, total in rows:
            row = conn.execute(
                "SELECT title FROM items WHERE id=?",
                (item_id,)
            ).fetchone()

            title = row["title"] if row else f"ITEM {item_id}"
            total = int(total or 0)
            grand += total

            msg += f"• {title} ({qty}) — ₦{total}\n"

        msg += f"\n💰 Total: ₦{grand}"
        bot.send_message(PAYMENT_NOTIFY_GROUP, msg)

    except Exception as e:
        print("weekly report error:", e)


# ================= MONTHLY REPORT =================
def send_monthly_sales_report():
    try:
        if not PAYMENT_NOTIFY_GROUP:
            return

        now = _ng_now()
        first_day = now.replace(day=1, hour=0, minute=0, second=0)

        rows = conn.execute(
            """
            SELECT
                oi.item_id,
                COUNT(*) AS qty,
                SUM(COALESCE(oi.price,0)) AS total
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.paid = 1
              AND o.created_at >= ?
            GROUP BY oi.item_id
            """,
            (first_day.strftime("%Y-%m-%d %H:%M:%S"),)
        ).fetchall()

        if not rows:
            bot.send_message(
                PAYMENT_NOTIFY_GROUP,
                "📊 MONTHLY SALES REPORT\n\nBabu siyarwa."
            )
            return

        msg = "📊 MONTHLY SALES REPORT\n\n"
        grand = 0

        for item_id, qty, total in rows:
            row = conn.execute(
                "SELECT title FROM items WHERE id=?",
                (item_id,)
            ).fetchone()

            title = row["title"] if row else f"ITEM {item_id}"
            total = int(total or 0)
            grand += total

            msg += f"• {title} ({qty}) — ₦{total}\n"

        msg += f"\n💰 Total: ₦{grand}"
        bot.send_message(PAYMENT_NOTIFY_GROUP, msg)

    except Exception as e:
        print("monthly report error:", e)


# ================= SCHEDULER =================
def sales_report_scheduler():
    weekly_sent = False
    monthly_sent = False

    while True:
        now = _ng_now()

        # Friday 23:50
        if now.weekday() == 4 and now.hour == 23 and now.minute == 50:
            if not weekly_sent:
                send_weekly_sales_report()
                weekly_sent = True
        else:
            weekly_sent = False

        # Last day of month 23:50
        if now.day == _last_day_of_month(now) and now.hour == 23 and now.minute == 50:
            if not monthly_sent:
                send_monthly_sales_report()
                monthly_sent = True
        else:
            monthly_sent = False

        time.sleep(20)



# ▶️ START BACKGROUND REPORT THREAD
# ================== START SERVER ==================
if __name__ == "__main__":

    if BOT_MODE == "webhook":
        print("🌐 Running in WEBHOOK mode")

        try:
            bot.remove_webhook()
            bot.set_webhook(f"{WEBHOOK_URL}/telegram")
            print("✅ Telegram webhook set successfully")
        except Exception as e:
            print("❌ Failed to set webhook:", e)

        port = int(os.environ.get("PORT", 10000))
        print(f"🚀 Flask server running on port {port}")
        app.run(host="0.0.0.0", port=port)

    else:
        # fallback (local testing only)
        print("🤖 Running in POLLING mode")
        bot.infinity_polling(skip_pending=True)