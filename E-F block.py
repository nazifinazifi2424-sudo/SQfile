
# =============================================================
# PART E/6
# BANDWIDTH MONITORING + RENDER API + MONTHLY RESET
# =============================================================

import calendar
from datetime import datetime, timezone, timedelta

# -------------------------------------------------------------
# 1. BANDWIDTH SETTINGS
# -------------------------------------------------------------

WORKER_ADVISORY_LOCK_ID = 887766

RENDER_BANDWIDTH_API_URL = (
    "https://api.render.com/v1/metrics/bandwidth"
)

RENDER_API_TIMEOUT = 20

BANDWIDTH_MONITOR_INTERVAL = 300

# Render metrics can be delayed.
RENDER_METRICS_DELAY_MINUTES = 70

# Local estimated usage is tracked in bytes.
BYTES_PER_GB = 1024 ** 3


# -------------------------------------------------------------
# 2. ADD BANDWIDTH BLOCK COLUMN TO EXISTING WORKERS TABLE
# No new table is created.
# -------------------------------------------------------------

def prepare_bandwidth_database():

    chat_conn = None
    chat_cur = None

    try:
        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            ALTER TABLE workers
            ADD COLUMN IF NOT EXISTS bandwidth_blocked
            BOOLEAN DEFAULT FALSE
        """)

        chat_cur.execute("""
            ALTER TABLE workers
            ADD COLUMN IF NOT EXISTS local_outbound_bytes
            BIGINT DEFAULT 0
        """)

        chat_cur.execute("""
            ALTER TABLE workers
            ADD COLUMN IF NOT EXISTS render_metrics_updated_at
            TIMESTAMPTZ
        """)

        chat_cur.execute("""
            ALTER TABLE workers
            ADD COLUMN IF NOT EXISTS bandwidth_last_reset_month
            TEXT
        """)

        chat_conn.commit()

        worker_log(
            "BANDWIDTH DATABASE PREPARED"
        )

    except Exception as e:

        if chat_conn:
            with suppress(Exception):
                chat_conn.rollback()

        worker_log(
            "BANDWIDTH DATABASE ERROR: " + repr(e),
            logging.ERROR
        )

        raise

    finally:

        if chat_cur:
            with suppress(Exception):
                chat_cur.close()

        if chat_conn:
            with suppress(Exception):
                chat_conn.close()


prepare_bandwidth_database()


# -------------------------------------------------------------
# 3. RECORD LOCAL OUTBOUND BANDWIDTH
#
# Call this after a successful file/video/document send.
# Pass the actual file size in bytes.
# -------------------------------------------------------------

def worker_record_outbound_bytes(size_bytes):

    try:

        size_bytes = int(size_bytes)

        if size_bytes <= 0:
            return False

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            UPDATE workers
            SET local_outbound_bytes =
                COALESCE(local_outbound_bytes, 0) + %s,

                bandwidth_used_gb =
                COALESCE(bandwidth_used_gb, 0) +
                (%s::NUMERIC / %s),

                updated_at = NOW()

            WHERE worker_id = %s
              AND bandwidth_month = %s
        """, (
            size_bytes,
            size_bytes,
            BYTES_PER_GB,
            WORKER_ID,
            worker_month_key()
        ))

        changed = chat_cur.rowcount

        chat_conn.commit()

        chat_cur.close()
        chat_conn.close()

        return changed > 0

    except Exception as e:

        worker_log(
            "LOCAL BANDWIDTH RECORD ERROR: " + repr(e),
            logging.ERROR
        )

        return False


# -------------------------------------------------------------
# 4. READ CURRENT WORKER BANDWIDTH
# -------------------------------------------------------------

def worker_get_bandwidth_usage():

    chat_conn = None
    chat_cur = None

    try:

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            SELECT
                bandwidth_used_gb,
                render_bandwidth_gb,
                bandwidth_limit_gb,
                bandwidth_blocked,
                local_outbound_bytes
            FROM workers
            WHERE worker_id = %s
        """, (WORKER_ID,))

        row = chat_cur.fetchone()

        if not row:
            return None

        return {
            "local_used_gb": float(row[0] or 0),
            "render_used_gb": float(row[1] or 0),
            "limit_gb": float(
                row[2] or RENDER_MONTHLY_BANDWIDTH_GB
            ),
            "blocked": bool(row[3]),
            "local_outbound_bytes": int(row[4] or 0)
        }

    except Exception as e:

        worker_log(
            "BANDWIDTH READ ERROR: " + repr(e),
            logging.ERROR
        )

        return None

    finally:

        if chat_cur:
            with suppress(Exception):
                chat_cur.close()

        if chat_conn:
            with suppress(Exception):
                chat_conn.close()


# -------------------------------------------------------------
# 5. GET RENDER BANDWIDTH METRICS
#
# The API result format can vary.
# This function extracts numeric values from common structures.
# It does not pretend that delayed metrics are real-time.
# -------------------------------------------------------------

def worker_fetch_render_bandwidth():

    if not RENDER_API_KEY or not RENDER_SERVICE_ID:
        worker_log(
            "RENDER BANDWIDTH CHECK SKIPPED: API key or service ID missing.",
            logging.WARNING
        )
        return None

    now = datetime.now(timezone.utc)

    # Exclude the most recent 70 minutes because Render metrics
    # may not yet have been updated.
    end_time = now - timedelta(
        minutes=RENDER_METRICS_DELAY_MINUTES
    )

    month_start = datetime(
        now.year,
        now.month,
        1,
        tzinfo=timezone.utc
    )

    start_time = month_start

    if end_time <= start_time:
        return None

    params = {
        "resource": RENDER_SERVICE_ID,
        "startTime": start_time.isoformat(),
        "endTime": end_time.isoformat()
    }

    headers = {
        "Authorization": f"Bearer {RENDER_API_KEY}",
        "Accept": "application/json"
    }

    try:

        response = requests.get(
            RENDER_BANDWIDTH_API_URL,
            headers=headers,
            params=params,
            timeout=RENDER_API_TIMEOUT
        )

        response.raise_for_status()

        data = response.json()

        worker_log(
            "RENDER BANDWIDTH API RESPONSE RECEIVED"
        )

        # Extract only fields that clearly represent bytes.
        byte_values = []

        def scan_metrics(obj):

            if isinstance(obj, dict):

                for key, value in obj.items():

                    normalized = str(key).lower()

                    if isinstance(value, (int, float)):

                        if (
                            "byte" in normalized
                            and "second" not in normalized
                        ):
                            byte_values.append(float(value))

                    elif isinstance(value, (dict, list)):
                        scan_metrics(value)

            elif isinstance(obj, list):

                for item in obj:
                    scan_metrics(item)

        scan_metrics(data)

        if not byte_values:
            worker_log(
                "RENDER METRICS FORMAT NOT RECOGNIZED. "
                "Keeping local bandwidth estimate.",
                logging.WARNING
            )
            return None

        # Use the largest clearly identified byte total.
        # Do not sum repeated aggregate fields.
        total_bytes = max(byte_values)

        if total_bytes < 0:
            return None

        return total_bytes / BYTES_PER_GB

    except Exception as e:

        worker_log(
            "RENDER BANDWIDTH API ERROR: " + repr(e),
            logging.ERROR
        )

        return None


# -------------------------------------------------------------
# 6. SAVE RENDER BANDWIDTH VALUE
# -------------------------------------------------------------

def worker_save_render_bandwidth(render_gb):

    if render_gb is None:
        return False

    try:

        render_gb = max(0.0, float(render_gb))

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            UPDATE workers
            SET render_bandwidth_gb = %s,
                render_metrics_updated_at = NOW(),
                bandwidth_checked_at = NOW(),
                updated_at = NOW()
            WHERE worker_id = %s
        """, (
            render_gb,
            WORKER_ID
        ))

        chat_conn.commit()

        chat_cur.close()
        chat_conn.close()

        return True

    except Exception as e:

        worker_log(
            "RENDER BANDWIDTH SAVE ERROR: " + repr(e),
            logging.ERROR
        )

        return False


# -------------------------------------------------------------
# 7. APPLY BANDWIDTH LIMIT
# If the worker reaches the threshold:
# - block it for the rest of the month
# - stop new tasks
# - request handover
# -------------------------------------------------------------

def worker_apply_bandwidth_limit():

    usage = worker_get_bandwidth_usage()

    if not usage:
        return False

    local_used = usage["local_used_gb"]
    render_used = usage["render_used_gb"]
    limit = min(
        usage["limit_gb"],
        RENDER_BANDWIDTH_STOP_GB
    )

    # Render metrics can be delayed. Use the larger reading
    # as a conservative safety estimate.
    effective_used = max(local_used, render_used)

    worker_log(
        f"BANDWIDTH STATUS: local={local_used:.3f}GB, "
        f"render={render_used:.3f}GB, "
        f"limit={limit:.3f}GB"
    )

    if effective_used >= limit:

        chat_conn = None
        chat_cur = None

        try:

            chat_conn = get_all_chat_db_conn()
            chat_cur = chat_conn.cursor()

            chat_cur.execute("""
                UPDATE workers
                SET bandwidth_blocked = TRUE,
                    worker_enabled = FALSE,
                    handover_requested = TRUE,
                    status = 'bandwidth_limit',
                    updated_at = NOW()
                WHERE worker_id = %s
            """, (WORKER_ID,))

            chat_conn.commit()

            worker_log(
                "BANDWIDTH LIMIT REACHED. "
                "WORKER BLOCKED UNTIL MONTHLY RESET.",
                logging.WARNING
            )

            worker_request_handover(
                "Monthly bandwidth limit reached"
            )

            return True

        except Exception as e:

            if chat_conn:
                with suppress(Exception):
                    chat_conn.rollback()

            worker_log(
                "BANDWIDTH LIMIT UPDATE ERROR: " + repr(e),
                logging.ERROR
            )

        finally:

            if chat_cur:
                with suppress(Exception):
                    chat_cur.close()

            if chat_conn:
                with suppress(Exception):
                    chat_conn.close()

    return False


# -------------------------------------------------------------
# 8. MONTHLY BANDWIDTH RESET
#
# Uses existing workers table.
# Does not delete tasks, customers, orders or messages.
# -------------------------------------------------------------

def worker_monthly_reset():

    current_month = worker_month_key()

    chat_conn = None
    chat_cur = None

    try:

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            SELECT pg_advisory_xact_lock(%s)
        """, (WORKER_ADVISORY_LOCK_ID + 1,))

        chat_cur.execute("""
            UPDATE workers
            SET bandwidth_used_gb = 0,
                render_bandwidth_gb = 0,
                local_outbound_bytes = 0,

                bandwidth_month = %s,
                bandwidth_last_reset_month = %s,

                bandwidth_blocked = FALSE,
                handover_requested = FALSE,

                worker_enabled = CASE
                    WHEN bandwidth_blocked = TRUE
                        THEN TRUE
                    ELSE worker_enabled
                END,

                is_leader = FALSE,
                leader_lease_until = NULL,
                leader_generation = COALESCE(
                    leader_generation, 0
                ),

                status = CASE
                    WHEN worker_enabled = FALSE
                         AND bandwidth_blocked = FALSE
                        THEN status
                    ELSE 'standby'
                END,

                monthly_reset_at = NOW(),
                updated_at = NOW()

            WHERE bandwidth_month IS DISTINCT FROM %s
               OR bandwidth_blocked = TRUE
        """, (
            current_month,
            current_month,
            current_month
        ))

        changed = chat_cur.rowcount

        chat_conn.commit()

        worker_log(
            f"MONTHLY BANDWIDTH RESET CHECK COMPLETE. "
            f"UPDATED ROWS: {changed}"
        )

        return True

    except Exception as e:

        if chat_conn:
            with suppress(Exception):
                chat_conn.rollback()

        worker_log(
            "MONTHLY RESET ERROR: " + repr(e),
            logging.ERROR
        )

        return False

    finally:

        if chat_cur:
            with suppress(Exception):
                chat_cur.close()

        if chat_conn:
            with suppress(Exception):
                chat_conn.close()


# -------------------------------------------------------------
# 9. BANDWIDTH MONITOR LOOP
# -------------------------------------------------------------

BANDWIDTH_MONITOR_STOP = threading.Event()
BANDWIDTH_MONITOR_THREAD = None


def worker_bandwidth_monitor_loop():

    worker_log(
        "BANDWIDTH MONITOR STARTED"
    )

    while not BANDWIDTH_MONITOR_STOP.is_set():

        try:

            # Check for a new calendar month.
            worker_monthly_reset()

            # Get delayed Render metrics.
            render_gb = worker_fetch_render_bandwidth()

            if render_gb is not None:
                worker_save_render_bandwidth(render_gb)

            # Enforce worker bandwidth limit.
            worker_apply_bandwidth_limit()

        except Exception as e:

            worker_log(
                "BANDWIDTH MONITOR ERROR: " + repr(e),
                logging.ERROR
            )

        BANDWIDTH_MONITOR_STOP.wait(
            BANDWIDTH_MONITOR_INTERVAL
        )

    worker_log(
        "BANDWIDTH MONITOR STOPPED"
    )


def start_worker_bandwidth_monitor():

    global BANDWIDTH_MONITOR_THREAD

    if (
        BANDWIDTH_MONITOR_THREAD
        and BANDWIDTH_MONITOR_THREAD.is_alive()
    ):
        return

    BANDWIDTH_MONITOR_STOP.clear()

    BANDWIDTH_MONITOR_THREAD = threading.Thread(
        target=worker_bandwidth_monitor_loop,
        name=f"{WORKER_ID}-BANDWIDTH",
        daemon=True
    )

    BANDWIDTH_MONITOR_THREAD.start()


def stop_worker_bandwidth_monitor():

    BANDWIDTH_MONITOR_STOP.set()

    if (
        BANDWIDTH_MONITOR_THREAD
        and BANDWIDTH_MONITOR_THREAD.is_alive()
    ):
        BANDWIDTH_MONITOR_THREAD.join(timeout=5)


# -------------------------------------------------------------
# 10. START BANDWIDTH MONITOR
# -------------------------------------------------------------

start_worker_bandwidth_monitor()

worker_log("PART E/6 LOADED SUCCESSFULLY")



# =============================================================
# PART F/6
# FINAL WORKER CONTROL + STARTUP + SHUTDOWN
# =============================================================

import signal
import atexit
from contextlib import suppress


# -------------------------------------------------------------
# 1. FINAL SYSTEM SETTINGS
# -------------------------------------------------------------

WORKER_SYSTEM_STARTED = False
WORKER_SYSTEM_STOPPING = False

WORKER_STARTUP_LOCK = threading.RLock()
WORKER_SHUTDOWN_LOCK = threading.RLock()


# -------------------------------------------------------------
# 2. FIXED MONTHLY RESET
#
# IMPORTANT:
# A worker blocked by bandwidth must NOT be re-enabled
# during the same month.
# It can only be re-enabled when the month changes.
# -------------------------------------------------------------

def worker_monthly_reset():

    current_month = worker_month_key()

    chat_conn = None
    chat_cur = None

    try:

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            SELECT pg_advisory_xact_lock(%s)
        """, (WORKER_ADVISORY_LOCK_ID + 1,))

        chat_cur.execute("""
            UPDATE workers
            SET bandwidth_used_gb = 0,
                render_bandwidth_gb = 0,
                local_outbound_bytes = 0,

                bandwidth_month = %s,
                bandwidth_last_reset_month = %s,

                bandwidth_blocked = FALSE,

                worker_enabled = CASE
                    WHEN bandwidth_blocked = TRUE
                        THEN TRUE
                    ELSE worker_enabled
                END,

                handover_requested = CASE
                    WHEN bandwidth_blocked = TRUE
                        THEN FALSE
                    ELSE handover_requested
                END,

                is_leader = FALSE,
                leader_lease_until = NULL,

                status = CASE
                    WHEN bandwidth_blocked = TRUE
                        THEN 'standby'
                    ELSE status
                END,

                monthly_reset_at = NOW(),
                updated_at = NOW()

            WHERE bandwidth_month IS DISTINCT FROM %s
        """, (
            current_month,
            current_month,
            current_month
        ))

        changed = chat_cur.rowcount

        chat_conn.commit()

        if changed:
            worker_log(
                f"NEW MONTH RESET COMPLETE: {current_month}. "
                f"UPDATED WORKERS: {changed}",
                logging.WARNING
            )

        return True

    except Exception as e:

        if chat_conn:
            with suppress(Exception):
                chat_conn.rollback()

        worker_log(
            "FINAL MONTHLY RESET ERROR: " + repr(e),
            logging.ERROR
        )

        return False

    finally:

        if chat_cur:
            with suppress(Exception):
                chat_cur.close()

        if chat_conn:
            with suppress(Exception):
                chat_conn.close()


# -------------------------------------------------------------
# 3. CHECK WORKER DATABASE REGISTRATION
# -------------------------------------------------------------

def worker_startup_database_check():

    chat_conn = None
    chat_cur = None

    try:

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            SELECT
                worker_id,
                worker_rank,
                worker_enabled,
                bandwidth_blocked,
                bandwidth_month
            FROM workers
            WHERE worker_id = %s
        """, (WORKER_ID,))

        row = chat_cur.fetchone()

        if not row:
            worker_log(
                "STARTUP FAILED: worker is not registered.",
                logging.ERROR
            )
            return False

        (
            worker_id,
            rank,
            enabled,
            blocked,
            month
        ) = row

        if not enabled or blocked:
            worker_log(
                f"STARTUP BLOCKED: {worker_id} is disabled "
                f"or bandwidth-blocked.",
                logging.WARNING
            )
            return False

        if month != worker_month_key():
            worker_log(
                "STARTUP BLOCKED: bandwidth month is outdated. "
                "Monthly reset is required.",
                logging.WARNING
            )
            return False

        worker_log(
            f"STARTUP DATABASE CHECK OK: "
            f"{worker_id}, rank={rank}"
        )

        return True

    except Exception as e:

        worker_log(
            "STARTUP DATABASE CHECK ERROR: " + repr(e),
            logging.ERROR
        )

        return False

    finally:

        if chat_cur:
            with suppress(Exception):
                chat_cur.close()

        if chat_conn:
            with suppress(Exception):
                chat_conn.close()


# -------------------------------------------------------------
# 4. FINAL CUSTOMER MESSAGE GUARD
# -------------------------------------------------------------

def worker_final_message_guard():

    if WORKER_SYSTEM_STOPPING:
        return False

    if WORKER_SHUTTING_DOWN:
        return False

    if not WORKER_ACCEPTING_TASKS:
        return False

    if WORKER_HANDOVER_REQUESTED:
        return False

    if not worker_startup_database_check():
        return False

    if not worker_has_valid_lease():
        clear_local_leader_state()
        return False

    if not verify_worker_leadership():
        clear_local_leader_state()
        return False

    return True


# -------------------------------------------------------------
# 5. CALLBACK QUERY GUARD
# Use for customer-facing inline button callbacks.
# -------------------------------------------------------------

def worker_callback_guard():

    return worker_final_message_guard()


# -------------------------------------------------------------
# 6. MANUAL RESUME AFTER HANDOVER
#
# Use only when you intentionally want to allow this worker
# to participate in election again.
# Never use this for a bandwidth-blocked worker.
# -------------------------------------------------------------

def worker_resume_after_handover():

    chat_conn = None
    chat_cur = None

    try:

        chat_conn = get_all_chat_db_conn()
        chat_cur = chat_conn.cursor()

        chat_cur.execute("""
            SELECT bandwidth_blocked, worker_enabled
            FROM workers
            WHERE worker_id = %s
        """, (WORKER_ID,))

        row = chat_cur.fetchone()

        if not row:
            return False

        blocked, enabled = row

        if blocked or not enabled:
            worker_log(
                "RESUME DENIED: worker is disabled "
                "or bandwidth-blocked.",
                logging.WARNING
            )
            return False

        chat_cur.execute("""
            UPDATE workers
            SET handover_requested = FALSE,
                status = 'standby',
                is_leader = FALSE,
                leader_lease_until = NULL,
                updated_at = NOW()
            WHERE worker_id = %s
              AND worker_enabled = TRUE
              AND bandwidth_blocked = FALSE
        """, (WORKER_ID,))

        chat_conn.commit()

        global WORKER_HANDOVER_REQUESTED
        global WORKER_ACCEPTING_TASKS

        WORKER_HANDOVER_REQUESTED = False
        WORKER_ACCEPTING_TASKS = True

        worker_log(
            "WORKER RESUMED: eligible for future election."
        )

        return True

    except Exception as e:

        if chat_conn:
            with suppress(Exception):
                chat_conn.rollback()

        worker_log(
            "WORKER RESUME ERROR: " + repr(e),
            logging.ERROR
        )

        return False

    finally:

        if chat_cur:
            with suppress(Exception):
                chat_cur.close()

        if chat_conn:
            with suppress(Exception):
                chat_conn.close()


# -------------------------------------------------------------
# 7. FINAL WORKER START FUNCTION
# -------------------------------------------------------------

def start_complete_worker_system():

    global WORKER_SYSTEM_STARTED

    with WORKER_STARTUP_LOCK:

        if WORKER_SYSTEM_STARTED:
            worker_log(
                "WORKER SYSTEM ALREADY STARTED."
            )
            return True

        worker_log(
            "STARTING COMPLETE WORKER SYSTEM..."
        )

        # Run monthly reset before checking registration.
        if not worker_monthly_reset():
            worker_log(
                "STARTUP FAILED: monthly reset failed.",
                logging.ERROR
            )
            return False

        if not worker_startup_database_check():
            worker_log(
                "STARTUP FAILED: database check failed.",
                logging.ERROR
            )
            return False

        try:

            # Start heartbeat.
            start_worker_heartbeat()

            # Start leader election monitor.
            start_worker_leader_monitor()

            # Start bandwidth monitor.
            start_worker_bandwidth_monitor()

            WORKER_SYSTEM_STARTED = True

            worker_log(
                "COMPLETE WORKER SYSTEM STARTED SUCCESSFULLY."
            )

            return True

        except Exception as e:

            worker_log(
                "WORKER SYSTEM START ERROR: " + repr(e),
                logging.ERROR
            )

            return False


# -------------------------------------------------------------
# 8. FINAL SAFE SHUTDOWN
# -------------------------------------------------------------

def stop_complete_worker_system():

    global WORKER_SYSTEM_STOPPING
    global WORKER_ACCEPTING_TASKS

    with WORKER_SHUTDOWN_LOCK:

        if WORKER_SYSTEM_STOPPING:
            return

        WORKER_SYSTEM_STOPPING = True
        WORKER_ACCEPTING_TASKS = False

        worker_log(
            "COMPLETE WORKER SHUTDOWN REQUESTED.",
            logging.WARNING
        )

        # Stop accepting new customer work.
        with suppress(Exception):
            worker_request_handover(
                "Complete worker shutdown"
            )

        # Stop monitoring threads.
        with suppress(Exception):
            stop_worker_leader_monitor()

        with suppress(Exception):
            stop_worker_bandwidth_monitor()

        with suppress(Exception):
            stop_worker_heartbeat()

        # If no task is active, release leadership.
        if not WORKER_CURRENT_TASK_ID:

            with suppress(Exception):
                worker_complete_handover()

        worker_log(
            "COMPLETE WORKER SHUTDOWN FINISHED.",
            logging.WARNING
        )


# -------------------------------------------------------------
# 9. OPERATING SYSTEM SHUTDOWN SIGNALS
# -------------------------------------------------------------

def worker_signal_handler(signum, frame):

    worker_log(
        f"SHUTDOWN SIGNAL RECEIVED: {signum}",
        logging.WARNING
    )

    stop_complete_worker_system()


with suppress(Exception):
    signal.signal(
        signal.SIGTERM,
        worker_signal_handler
    )

with suppress(Exception):
    signal.signal(
        signal.SIGINT,
        worker_signal_handler
    )


# -------------------------------------------------------------
# 10. REGISTER FINAL SHUTDOWN
# -------------------------------------------------------------

atexit.register(stop_complete_worker_system)


# -------------------------------------------------------------
# 11. START THE COMPLETE SYSTEM
# -------------------------------------------------------------

if start_complete_worker_system():

    worker_log(
        f"{WORKER_ID} READY. "
        "WAITING FOR LEADER ELECTION."
    )

else:

    worker_log(
        f"{WORKER_ID} STARTUP FAILED. "
        "CHECK DATABASE AND ENVIRONMENT VARIABLES.",
        logging.ERROR
    )


worker_log("PART F/6 LOADED SUCCESSFULLY")
