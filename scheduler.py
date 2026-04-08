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
import threading
import time
from datetime import date, datetime, timedelta

import pandas_market_calendars as mcal
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

import config
import main
import monitor

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

    for fn, job_id, name, run_time, grace in [
        (job_signal_snap,         "signal_snap",         "IB snapshot + signal evaluation",    t_signal, 120),
        (job_order_submission,    "order_submission",    "LOC/MOC order submission",            t_order,   60),
        (job_fill_reconciliation, "fill_reconciliation", "Fill confirmation + position update", t_fill,   300),
        (job_daily_report,        "daily_report",        "Daily / weekly report",               t_report, 300),
    ]:
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
# Connection watchdog
# ═══════════════════════════════════════════════════════════════════════════════

def connection_watchdog() -> None:
    """
    Daemon thread — blocks on bridge._disconnect_event and reconnects immediately
    when IB fires connectionClosed().

    Retry policy: up to 3 attempts at 10-second intervals.
    On all retries exhausted: critical alert via monitor.send_alert().
    On success: clears the disconnect event and loops back to waiting.
    """
    _MAX_RETRIES   = 3
    _RETRY_DELAY_S = 10

    logger.info("[watchdog] connection_watchdog started")

    while True:
        main.bridge.wait_for_disconnect()
        logger.warning("[watchdog] disconnect detected — attempting reconnect")

        connected = False
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                main.bridge.connect()
                logger.info("[watchdog] reconnect succeeded (attempt %d)", attempt)
                connected = True
                break
            except Exception as exc:
                logger.warning(
                    "[watchdog] reconnect attempt %d/%d failed: %s",
                    attempt, _MAX_RETRIES, exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY_S)

        if connected:
            main.bridge.clear_disconnect()
        else:
            logger.critical("[watchdog] all %d reconnect attempts failed", _MAX_RETRIES)
            monitor.send_alert(
                "IB reconnect FAILED",
                f"connectionClosed() detected and all {_MAX_RETRIES} reconnect attempts "
                f"failed. Manual intervention required.",
                level="critical",
            )
            # Reset the event so the watchdog can detect the next disconnect
            main.bridge.clear_disconnect()


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

    logger.info("[scheduler] Connecting to IB TWS/Gateway")
    try:
        main.bridge.connect()
        logger.info("[scheduler] IB connection established")
    except Exception as exc:
        logger.critical("[scheduler] Failed to connect to IB: %s — aborting", exc)
        raise SystemExit(1)

    watchdog = threading.Thread(
        target=connection_watchdog,
        daemon=True,
        name="ib-watchdog",
    )
    watchdog.start()
    logger.info("[scheduler] connection_watchdog thread started")

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

    sched = build_scheduler()
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] Shutting down")
        sched.shutdown(wait=False)
