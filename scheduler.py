"""
scheduler.py — APScheduler wrapper.

Runs all main.py jobs on their configured cron schedules anchored to
New York time (config.TZ), regardless of host machine timezone.

Fixed cron jobs (registered at startup):
  sunday_universe_update — every Sunday at SP500_UPDATE_TIME  (no calendar gate)
  sunday_reauth          — every Sunday at IBC_2FA_TIME       (no calendar gate)
  nightly_sync           — Mon–Fri at TIME_NIGHTLY_SYNC       (NYSE calendar gate)
  market_open_check      — Mon–Fri at 11:00 ET                (NYSE calendar gate)

market_open_check fires at 11:00 ET and dynamically registers four intraday
DateTrigger jobs at the correct times for that day:

  signal_snap         close_time − 20 min   (normal: 15:40 ET)
  order_submission    close_time − 16 min   (normal: 15:44 ET)
  fill_reconciliation close_time + 10 min   (normal: 16:10 ET)
  daily_report        close_time + 15 min   (normal: 16:15 ET)

On NYSE half days (early close at 13:00 ET) the intraday jobs shift
automatically to 12:40 / 12:44 / 13:10 / 13:15 ET without any manual
intervention.  config.HALF_DAY_DATES provides a fallback override list for
dates where the library data has not yet been updated.

Usage
─────
  python scheduler.py          # blocking — runs until Ctrl-C
"""

import logging
import subprocess
import threading
import time
from datetime import date, datetime, timedelta

import pandas_market_calendars as mcal
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

import config
import db
import main
import monitor
import risk_engine
import td_data

logger = logging.getLogger("murphy")

# ── NYSE calendar (loaded once at module import) ───────────────────────────────
_nyse = mcal.get_calendar("NYSE")

# ── Running scheduler reference — set by build_scheduler(), used by market_open_check()
_scheduler: BlockingScheduler | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# Calendar helpers
# ═══════════════════════════════════════════════════════════════════════════════

def get_market_schedule(d: date | None = None) -> dict:
    """
    Return the NYSE trading schedule for *d* (default: today).

    Returns
    -------
    dict with three keys:
        is_open    : bool              True if NYSE is open on this date
        close_time : datetime | None   tz-aware ET datetime of market close;
                                       None when is_open is False
        is_half_day: bool              True when close_time is before 14:00 ET

    Half-day detection uses pandas_market_calendars.  Dates listed in
    config.HALF_DAY_DATES override the calendar data with a 13:00 ET close —
    useful when the library has not yet been updated for announced schedule
    changes.
    """
    if d is None:
        d = date.today()

    schedule = _nyse.schedule(start_date=d.isoformat(), end_date=d.isoformat())

    if schedule.empty:
        return {"is_open": False, "close_time": None, "is_half_day": False}

    close_utc = schedule.iloc[0]["market_close"]
    close_et  = close_utc.tz_convert(config.TZ).to_pydatetime()

    # config.HALF_DAY_DATES takes precedence over the calendar
    if d.isoformat() in config.HALF_DAY_DATES:
        close_et    = datetime(d.year, d.month, d.day, 13, 0, tzinfo=config.TZ)
        is_half_day = True
    else:
        is_half_day = close_et.hour < 14

    return {"is_open": True, "close_time": close_et, "is_half_day": is_half_day}


def _market_guard(job_name: str) -> dict | None:
    """
    Return the market schedule dict for today when it is a trading day, or
    None (with an INFO log) when it is not.

    Callers can treat the return value as a bool:
        if not _market_guard("job_name"): return
    """
    sched = get_market_schedule()
    if not sched["is_open"]:
        logger.info("[scheduler] %s: not a trading day — skipping", job_name)
        return None
    return sched


# ═══════════════════════════════════════════════════════════════════════════════
# Wrapped job functions
# ═══════════════════════════════════════════════════════════════════════════════

def job_connectivity_check() -> None:
    # No calendar gate — runs every weekday regardless of trading day
    if not main.bridge.is_connected():
        logger.warning("[scheduler] job_connectivity_check: bridge not connected — calling connect()")
        try:
            main.bridge.connect()
        except Exception as exc:
            logger.critical("[scheduler] job_connectivity_check: connect() failed: %s", exc)
    main.connectivity_check()


def job_nightly_sync() -> None:
    if not _market_guard("nightly_sync"):
        return
    main.nightly_sync()


# The four intraday job wrappers below are registered as DateTrigger jobs by
# market_open_check() — they are never added as fixed cron triggers.

def job_signal_snap() -> None:
    main.signal_snap()


def job_order_submission() -> None:
    main.order_submission()


def job_fill_reconciliation() -> None:
    main.fill_reconciliation()


def job_daily_report() -> None:
    main.daily_report()


def job_sunday_universe_update() -> None:
    # No calendar gate — fires unconditionally every Sunday
    main.sunday_universe_update()


def job_sunday_reauth() -> None:
    # No calendar gate — fires unconditionally every Sunday
    main.sunday_reauth()


# ═══════════════════════════════════════════════════════════════════════════════
# market_open_check — dynamic intraday job scheduler
# ═══════════════════════════════════════════════════════════════════════════════

def market_open_check() -> None:
    """
    11:00 ET Mon–Fri — determine today's market schedule and register the four
    intraday jobs as one-off DateTrigger jobs at the correct times.

    Normal day (close 16:00 ET):
        signal_snap         → 15:40 ET
        order_submission    → 15:44 ET
        fill_reconciliation → 16:10 ET
        daily_report        → 16:15 ET

    Half day (close 13:00 ET):
        signal_snap         → 12:40 ET
        order_submission    → 12:44 ET
        fill_reconciliation → 13:10 ET
        daily_report        → 13:15 ET
    """
    sched = get_market_schedule()
    if not sched["is_open"]:
        logger.info("[scheduler] market_open_check: not a trading day — skipping")
        return

    close_time = sched["close_time"]
    logger.info(
        "[scheduler] market_open_check: close=%s half_day=%s",
        close_time.strftime("%H:%M"), sched["is_half_day"],
    )

    t_signal = close_time + timedelta(minutes=config.SCHED_SIGNAL_OFFSET_MIN)
    t_order  = close_time + timedelta(minutes=config.SCHED_ORDER_OFFSET_MIN)
    t_fill   = close_time + timedelta(minutes=config.SCHED_FILL_OFFSET_MIN)
    t_report = close_time + timedelta(minutes=config.SCHED_REPORT_OFFSET_MIN)

    now      = datetime.now(config.TZ)
    min_lead = timedelta(minutes=config.SCHED_MIN_LEAD_MINS)

    for fn, job_id, name, run_time, grace in [
        (job_signal_snap,         "signal_snap",         "IB snapshot + signal evaluation",    t_signal, 120),
        (job_order_submission,    "order_submission",    "LOC/MOC order submission",            t_order,   60),
        (job_fill_reconciliation, "fill_reconciliation", "Fill confirmation + position update", t_fill,   300),
        (job_daily_report,        "daily_report",        "Daily / weekly report",               t_report, 300),
    ]:
        if run_time - now < min_lead:
            logger.info(
                "[scheduler] %s: skipped — run_time %s is less than %d min away",
                job_id, run_time.strftime("%H:%M"), config.SCHED_MIN_LEAD_MINS,
            )
            continue
        _scheduler.add_job(
            fn,
            DateTrigger(run_date=run_time, timezone=config.TZ),
            id=job_id,
            name=name,
            misfire_grace_time=grace,
            replace_existing=True,
        )
        logger.info("[scheduler] %s scheduled at %s ET", job_id, run_time.strftime("%H:%M"))


# ═══════════════════════════════════════════════════════════════════════════════
# Startup helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _startup_halt_warning() -> None:
    """
    Called once at startup after the IB connection is established.

    If a halt or shutdown flag is active in the DB, logs at CRITICAL and sends
    a warning alert.  Does NOT abort startup — data maintenance jobs continue
    running regardless.
    """
    halted   = risk_engine.is_halted()
    shutdown = risk_engine.is_shutdown()
    if halted or shutdown:
        state = "SHUTDOWN" if shutdown else "HALT"
        logger.critical(
            "[scheduler] STARTED WITH %s ACTIVE — trading suspended; "
            "call risk_engine.clear_halt() to resume",
            state,
        )
        monitor.send_alert(
            subject="⚠️ Murphy's Law started with HALT/SHUTDOWN active",
            body=(
                f"{state} flag is active in the DB. Trading is suspended. "
                f"Data maintenance jobs (nightly sync, universe update) will continue "
                f"running. Call risk_engine.clear_halt() to resume trading."
            ),
            level="critical",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Startup catch-up
# ═══════════════════════════════════════════════════════════════════════════════

_UNIVERSE_STALE_DAYS = 7  # trigger a catch-up update if last run was older than this


def startup_catchup() -> None:
    """
    Run once at scheduler startup (before sched.start()).  Handles two scenarios:

    1. Same-day intraday catch-up:
       If today is a trading day and the current time is past 11:00 ET (when
       market_open_check normally fires) but before fill_reconciliation time,
       call market_open_check() immediately so any remaining intraday jobs are
       registered for today.

    2. Stale universe catch-up:
       Read last_universe_update from the system_state table.  If the value is
       absent or older than _UNIVERSE_STALE_DAYS days, call
       main.sunday_universe_update() immediately.
    """
    now = datetime.now(config.TZ)
    logger.info("[startup_catchup] running at %s ET", now.strftime("%Y-%m-%d %H:%M"))

    # ── Market catch-up ───────────────────────────────────────────────────────
    sched = get_market_schedule()
    if sched["is_open"]:
        close_time = sched["close_time"]
        t_check = datetime(now.year, now.month, now.day, 11, 0, tzinfo=config.TZ)
        t_fill  = close_time + timedelta(minutes=config.SCHED_FILL_OFFSET_MIN)

        if t_check <= now < t_fill:
            logger.info(
                "[startup_catchup] started after 11:00 ET on trading day — calling market_open_check()"
            )
            market_open_check()
        else:
            logger.info(
                "[startup_catchup] market catch-up not needed (now=%s, window 11:00–%s)",
                now.strftime("%H:%M"), t_fill.strftime("%H:%M"),
            )
    else:
        logger.info("[startup_catchup] not a trading day — skipping market catch-up")

    # ── Universe catch-up ─────────────────────────────────────────────────────
    last_update: datetime | None = None
    try:
        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS system_state "
                "(key TEXT PRIMARY KEY, value TEXT)"
            )
            row = conn.execute(
                "SELECT value FROM system_state WHERE key = 'last_universe_update'"
            ).fetchone()
            if row:
                last_update = datetime.fromisoformat(row[0])
    except Exception as exc:
        logger.warning("[startup_catchup] could not read system_state: %s", exc)

    if last_update is None or (now - last_update).days >= _UNIVERSE_STALE_DAYS:
        reason = "absent" if last_update is None else f"last={last_update.date()}"
        logger.info(
            "[startup_catchup] universe update stale or absent (%s) — running sunday_universe_update()",
            reason,
        )
        main.sunday_universe_update()
    else:
        logger.info("[startup_catchup] universe up to date (last=%s)", last_update.date())

    # ── Nightly data catch-up ─────────────────────────────────────────────────
    max_bar_date = None
    try:
        with db.connect() as conn:
            row = conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
            if row and row[0]:
                max_bar_date = row[0]
    except Exception as exc:
        logger.warning("[startup_catchup] could not read MAX(date) from daily_bars: %s", exc)

    last_close_date = None
    try:
        look_back = _nyse.schedule(
            start_date=(now.date() - timedelta(days=10)).isoformat(),
            end_date=now.date().isoformat(),
        )
        if not look_back.empty:
            closes        = look_back["market_close"].dt.tz_convert(config.TZ)
            past_sessions = look_back[closes < now]
            if not past_sessions.empty:
                last_close_date = past_sessions.index[-1].date()
    except Exception as exc:
        logger.warning("[startup_catchup] could not determine last NYSE close: %s", exc)

    if max_bar_date is not None and last_close_date is not None:
        from datetime import date as _date
        max_date = (
            _date.fromisoformat(max_bar_date)
            if isinstance(max_bar_date, str)
            else max_bar_date
        )
        if max_date < last_close_date:
            gap_days = (now.date() - max_date).days
            n_days   = gap_days + 2
            symbols  = main._load_universe()
            logger.info(
                "[startup_catchup] startup catch-up sync: DB last date=%s, "
                "last market close=%s, fetching %d days",
                max_date, last_close_date, n_days,
            )
            try:
                td_data.fetch_incremental(symbols, n_days=n_days)
                main.precompute_watchlist()
            except Exception as exc:
                logger.warning("[startup_catchup] catch-up sync failed: %s", exc)
        else:
            logger.debug("[startup_catchup] DB is current (last date=%s)", max_bar_date)
    else:
        logger.debug(
            "[startup_catchup] skipping data catch-up (max_bar_date=%s, last_close=%s)",
            max_bar_date, last_close_date,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Post-reconnect catch-up
# ═══════════════════════════════════════════════════════════════════════════════

def _post_reconnect_catchup() -> None:
    """
    Called by connection_watchdog() immediately after a successful reconnect.

    Inspects current time against today's scheduled intraday job times and either:
      • schedules the job at its natural run_time if still in the future, or
      • schedules it immediately (now + 5 s) if its window has passed but it is
        still worth running.

    Sequencing rules
    ─────────────────
      signal_snap         skipped if order_submission time has already passed
      order_submission    skipped if fill_reconciliation time has already passed
      fill_reconciliation always run if still the same trading day (before midnight ET)
      daily_report        always run if still the same trading day (before midnight ET)

    On a non-trading day the function returns immediately with no jobs scheduled.
    """
    sched = get_market_schedule()
    if not sched["is_open"]:
        logger.info("[watchdog] _post_reconnect_catchup: not a trading day — nothing to catch up")
        return

    now        = datetime.now(config.TZ)
    close_time = sched["close_time"]

    t_signal = close_time + timedelta(minutes=config.SCHED_SIGNAL_OFFSET_MIN)
    t_order  = close_time + timedelta(minutes=config.SCHED_ORDER_OFFSET_MIN)
    t_fill   = close_time + timedelta(minutes=config.SCHED_FILL_OFFSET_MIN)
    t_report = close_time + timedelta(minutes=config.SCHED_REPORT_OFFSET_MIN)

    min_lead = timedelta(minutes=config.SCHED_MIN_LEAD_MINS)
    same_day = now.date() == close_time.date()

    # (fn, job_id, name, run_time, grace, worth_running)
    jobs = [
        (job_signal_snap,         "signal_snap",         "IB snapshot + signal evaluation",    t_signal, 120, now < t_order),
        (job_order_submission,    "order_submission",    "LOC/MOC order submission",            t_order,   60, now < t_fill),
        (job_fill_reconciliation, "fill_reconciliation", "Fill confirmation + position update", t_fill,   300, same_day),
        (job_daily_report,        "daily_report",        "Daily / weekly report",               t_report, 300, same_day),
    ]

    for fn, job_id, name, run_time, grace, worth_running in jobs:
        if not worth_running:
            logger.info(
                "[watchdog] _post_reconnect_catchup: %s skipped — too late in sequence or not same day",
                job_id,
            )
            continue

        if run_time - now >= min_lead:
            # Job time is still in the future — schedule at natural time
            _scheduler.add_job(
                fn,
                DateTrigger(run_date=run_time, timezone=config.TZ),
                id=job_id,
                name=name,
                misfire_grace_time=grace,
                replace_existing=True,
            )
            logger.info(
                "[watchdog] _post_reconnect_catchup: %s scheduled at %s ET",
                job_id, run_time.strftime("%H:%M"),
            )
        else:
            # Job time already passed — run immediately
            immediate = now + timedelta(seconds=5)
            _scheduler.add_job(
                fn,
                DateTrigger(run_date=immediate, timezone=config.TZ),
                id=job_id,
                name=name,
                misfire_grace_time=grace,
                replace_existing=True,
            )
            logger.info(
                "[watchdog] _post_reconnect_catchup: %s missed — running immediately",
                job_id,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Connection watchdog
# ═══════════════════════════════════════════════════════════════════════════════

def connection_watchdog() -> None:
    """
    Daemon thread — blocks on bridge.wait_for_disconnect() and reconnects when
    IB fires connectionClosed().

    Retry policy:
      • Clears the disconnect event immediately so any subsequent disconnect
        during the retry loop is captured.
      • Sleeps IB_RECONNECT_INTERVAL_SEC between each attempt.
      • Retries indefinitely until connect() succeeds.
      • After IB_RECONNECT_ALERT_AFTER failures sends a warning alert once.
      • On success: if a warning alert was sent, sends a recovery info alert.
      • Calls _post_reconnect_catchup() to re-register any missed intraday jobs.
      • Loops back to wait_for_disconnect().
    """
    logger.info("[watchdog] connection_watchdog started")

    while True:
        main.bridge.wait_for_disconnect()
        main.bridge.clear_disconnect()   # reset now so next disconnect is captured
        logger.warning("[watchdog] disconnect detected — starting reconnect loop")

        attempt    = 0
        alert_sent = False

        while True:
            time.sleep(config.IB_RECONNECT_INTERVAL_SEC)
            attempt += 1
            try:
                main.bridge.connect()
                logger.info("[watchdog] reconnect succeeded after %d attempt(s)", attempt)
                break
            except Exception as exc:
                logger.warning("[watchdog] reconnect attempt %d failed: %s", attempt, exc)
                if attempt == config.IB_RECONNECT_ALERT_AFTER and not alert_sent:
                    monitor.send_alert(
                        "IB connection lost",
                        f"IB connection lost — {attempt} reconnect attempts failed, "
                        f"retrying every {config.IB_RECONNECT_INTERVAL_SEC}s",
                        level="warning",
                    )
                    alert_sent = True

        if alert_sent:
            monitor.send_alert(
                "IB connection restored",
                f"IB connection restored after {attempt} attempt(s)",
                level="info",
            )

        _post_reconnect_catchup()


# ═══════════════════════════════════════════════════════════════════════════════
# Scheduler factory
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_hhmm(t: str) -> tuple[int, int]:
    """Parse 'HH:MM' string into (hour, minute) ints."""
    h, m = t.split(":")
    return int(h), int(m)


def build_scheduler() -> BlockingScheduler:
    """
    Construct and return a configured BlockingScheduler.
    Does NOT start it — call scheduler.start() to begin execution.
    """
    global _scheduler
    scheduler = BlockingScheduler(timezone=config.TZ)
    _scheduler = scheduler

    # ── Sun SP500_UPDATE_TIME — sunday_universe_update (before reauth) ──────────
    h_uu, m_uu = _parse_hhmm(config.SP500_UPDATE_TIME)
    _day_abbrev = config.SP500_UPDATE_DAY[:3].lower()
    scheduler.add_job(
        job_sunday_universe_update,
        CronTrigger(day_of_week=_day_abbrev, hour=h_uu, minute=m_uu, timezone=config.TZ),
        id="sunday_universe_update",
        name="S&P 500 universe refresh + full history fetch",
        misfire_grace_time=600,
    )

    # ── Sun IBC_2FA_TIME — sunday_reauth (no calendar gate) ──────────────────
    h2fa, m2fa = _parse_hhmm(config.IBC_2FA_TIME)
    scheduler.add_job(
        job_sunday_reauth,
        CronTrigger(day_of_week="sun", hour=h2fa, minute=m2fa, timezone=config.TZ),
        id="sunday_reauth",
        name="IBC 2FA restart",
        misfire_grace_time=600,
    )

    # ── Mon–Fri 09:00 ET — connectivity_check (no calendar gate) ────────────
    scheduler.add_job(
        job_connectivity_check,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=0, timezone=config.TZ),
        id="connectivity_check",
        name="IB TWS/Gateway heartbeat",
        misfire_grace_time=300,
    )

    # ── Mon–Fri TIME_NIGHTLY_SYNC — nightly_sync ─────────────────────────────
    h_ns, m_ns = _parse_hhmm(config.TIME_NIGHTLY_SYNC)
    scheduler.add_job(
        job_nightly_sync,
        CronTrigger(day_of_week="mon-fri", hour=h_ns, minute=m_ns, timezone=config.TZ),
        id="nightly_sync",
        name="TwelveData sync + watchlist precompute",
        misfire_grace_time=600,
    )

    # ── Mon–Fri 11:00 ET — market_open_check (registers four intraday jobs) ───
    scheduler.add_job(
        market_open_check,
        CronTrigger(day_of_week="mon-fri", hour=11, minute=0, timezone=config.TZ),
        id="market_open_check",
        name="Determine market schedule and register intraday jobs",
        misfire_grace_time=600,
    )

    return scheduler


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    monitor.setup_logging()

    subprocess.Popen(["uv", "--directory", config.API_CONTROLLER_PATH, "run", "main.py"])        
    time.sleep(2)
    
    logger.info("[scheduler] Connecting to IB TWS/Gateway")
    try:
        main.bridge.connect()
        logger.info("[scheduler] IB connection established")
    except Exception as exc:
        logger.critical("[scheduler] Failed to connect to IB: %s — aborting", exc)
        raise SystemExit(1)

    _startup_halt_warning()

    watchdog = threading.Thread(
        target=connection_watchdog,
        daemon=True,
        name="ib-watchdog",
    )
    watchdog.start()
    logger.info("[scheduler] connection_watchdog thread started")

    sched = build_scheduler()

    startup_catchup()

    logger.info("[scheduler] Starting Murphy's Law scheduler")
    logger.info("[scheduler] Timezone: %s", config.TZ)
    logger.info("[scheduler] Fixed jobs:")
    logger.info("  connectivity_check: mon-fri 09:00 ET (no calendar gate)")
    logger.info("  universe_update   : %s %s ET", config.SP500_UPDATE_DAY, config.SP500_UPDATE_TIME)
    logger.info("  sunday_reauth     : sun %s ET", config.IBC_2FA_TIME)
    logger.info("  nightly_sync      : mon-fri %s ET", config.TIME_NIGHTLY_SYNC)
    logger.info("  market_open_check : mon-fri 11:00 ET")
    logger.info("[scheduler] Intraday jobs (registered dynamically at 11:00 based on close_time):")
    logger.info("  signal_snap      : close %+d min", config.SCHED_SIGNAL_OFFSET_MIN)
    logger.info("  order_submission : close %+d min", config.SCHED_ORDER_OFFSET_MIN)
    logger.info("  fill_reconcil.   : close %+d min", config.SCHED_FILL_OFFSET_MIN)
    logger.info("  daily_report     : close %+d min", config.SCHED_REPORT_OFFSET_MIN)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] Shutting down")
        sched.shutdown(wait=False)
