"""
tests/test_scheduler.py — Unit tests for scheduler.py

Real-date tests use pandas_market_calendars directly — no mocking.
market_open_check tests mock get_market_schedule and _scheduler so no live
APScheduler instance is required.
"""

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

import config
import db
import scheduler as sched_module
from apscheduler.triggers.date import DateTrigger
from scheduler import (
    get_market_schedule, market_open_check, startup_catchup,
    connection_watchdog, _post_reconnect_catchup, _startup_halt_warning,
    _reset_daily_job_tracker,
)


# ═══════════════════════════════════════════════════════════════════════════════
# TestGetMarketSchedule — real calendar data
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetMarketSchedule:

    def test_full_trading_day_is_open(self):
        """2024-01-02 is a normal NYSE trading day."""
        result = get_market_schedule(date(2024, 1, 2))
        assert result["is_open"] is True

    def test_full_trading_day_close_time(self):
        """Normal close is 16:00 ET."""
        result = get_market_schedule(date(2024, 1, 2))
        close_et = result["close_time"].astimezone(config.TZ)
        assert close_et.hour == 16
        assert close_et.minute == 0

    def test_full_trading_day_not_half_day(self):
        result = get_market_schedule(date(2024, 1, 2))
        assert result["is_half_day"] is False

    def test_holiday_is_not_open(self):
        """2024-01-01 (New Year's Day) is not a trading day."""
        result = get_market_schedule(date(2024, 1, 1))
        assert result["is_open"] is False
        assert result["close_time"] is None
        assert result["is_half_day"] is False

    def test_half_day_is_open(self):
        """2024-11-29 (Black Friday) is an NYSE early-close day."""
        result = get_market_schedule(date(2024, 11, 29))
        assert result["is_open"] is True

    def test_half_day_close_at_13(self):
        """Black Friday 2024 closes at 13:00 ET."""
        result = get_market_schedule(date(2024, 11, 29))
        close_et = result["close_time"].astimezone(config.TZ)
        assert close_et.hour == 13

    def test_half_day_flag_set(self):
        result = get_market_schedule(date(2024, 11, 29))
        assert result["is_half_day"] is True

    def test_half_day_dates_config_overrides_calendar(self, monkeypatch):
        """A date in HALF_DAY_DATES overrides the calendar with a 13:00 ET close."""
        monkeypatch.setattr(config, "HALF_DAY_DATES", ["2024-01-02"])
        result = get_market_schedule(date(2024, 1, 2))
        assert result["is_open"] is True
        assert result["is_half_day"] is True
        assert result["close_time"].hour == 13
        assert result["close_time"].minute == 0

    def test_close_time_is_python_datetime(self):
        """close_time must be a Python datetime (not a pandas Timestamp)."""
        result = get_market_schedule(date(2024, 1, 2))
        assert type(result["close_time"]) is datetime

    def test_close_time_is_tz_aware(self):
        result = get_market_schedule(date(2024, 1, 2))
        assert result["close_time"].tzinfo is not None


# ═══════════════════════════════════════════════════════════════════════════════
# TestMarketGuard
# ═══════════════════════════════════════════════════════════════════════════════

class TestMarketGuard:

    def test_returns_none_on_non_trading_day(self, monkeypatch):
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: {
            "is_open": False, "close_time": None, "is_half_day": False,
        })
        assert sched_module._market_guard("test_job") is None

    def test_returns_schedule_dict_on_trading_day(self, monkeypatch):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        expected = {"is_open": True, "close_time": close, "is_half_day": False}
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: expected)
        result = sched_module._market_guard("test_job")
        assert result == expected

    def test_return_value_is_falsy_on_non_trading_day(self, monkeypatch):
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: {
            "is_open": False, "close_time": None, "is_half_day": False,
        })
        # Callers use: if not _market_guard(...): return
        assert not sched_module._market_guard("test_job")

    def test_return_value_is_truthy_on_trading_day(self, monkeypatch):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: {
            "is_open": True, "close_time": close, "is_half_day": False,
        })
        assert sched_module._market_guard("test_job")


# ═══════════════════════════════════════════════════════════════════════════════
# TestDerivedJobTimes — pure arithmetic, no APScheduler
# ═══════════════════════════════════════════════════════════════════════════════

class TestDerivedJobTimes:

    def test_normal_day_signal_snap(self):
        """close − 20 min reproduces config.TIME_SIGNAL_SNAP on a normal day."""
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        assert (close - timedelta(minutes=20)).strftime("%H:%M") == "15:40"

    def test_normal_day_order_submission(self):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        assert (close - timedelta(minutes=16)).strftime("%H:%M") == "15:44"

    def test_normal_day_fill_reconciliation(self):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        assert (close + timedelta(minutes=10)).strftime("%H:%M") == "16:10"

    def test_normal_day_daily_report(self):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        assert (close + timedelta(minutes=15)).strftime("%H:%M") == "16:15"

    def test_half_day_signal_snap_shifts(self):
        """On a half day (close 13:00), signal_snap moves to 12:40."""
        close = datetime(2024, 11, 29, 13, 0, tzinfo=config.TZ)
        assert (close - timedelta(minutes=20)).strftime("%H:%M") == "12:40"

    def test_half_day_order_submission_shifts(self):
        close = datetime(2024, 11, 29, 13, 0, tzinfo=config.TZ)
        assert (close - timedelta(minutes=16)).strftime("%H:%M") == "12:44"

    def test_half_day_fill_reconciliation_shifts(self):
        close = datetime(2024, 11, 29, 13, 0, tzinfo=config.TZ)
        assert (close + timedelta(minutes=10)).strftime("%H:%M") == "13:10"

    def test_half_day_daily_report_shifts(self):
        close = datetime(2024, 11, 29, 13, 0, tzinfo=config.TZ)
        assert (close + timedelta(minutes=15)).strftime("%H:%M") == "13:15"


# ═══════════════════════════════════════════════════════════════════════════════
# TestMarketOpenCheck — mocked scheduler and schedule
# ═══════════════════════════════════════════════════════════════════════════════

class TestMarketOpenCheck:

    def _mock_schedule(self, is_open, close_et=None, is_half_day=False):
        return {"is_open": is_open, "close_time": close_et, "is_half_day": is_half_day}

    def _run_and_collect(self, monkeypatch, schedule_dict, now=None):
        """Set up mocks, call market_open_check, return list of add_job calls.

        ``now`` defaults to 11:00 ET on the day of the close (so all jobs have
        sufficient lead time and none are skipped by the SCHED_MIN_LEAD_MINS
        filter).
        """
        close = schedule_dict.get("close_time")
        if now is None and close is not None:
            now = close.replace(hour=11, minute=0, second=0, microsecond=0)
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: schedule_dict)
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            sched_module.market_open_check()
        return mock_sched.add_job.call_args_list

    def test_skips_non_trading_day(self, monkeypatch):
        calls = self._run_and_collect(monkeypatch, self._mock_schedule(False))
        assert len(calls) == 0

    def test_schedules_four_jobs_on_normal_day(self, monkeypatch):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(monkeypatch, self._mock_schedule(True, close))
        assert len(calls) == 4

    def test_job_ids_on_normal_day(self, monkeypatch):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(monkeypatch, self._mock_schedule(True, close))
        job_ids = {c[1]["id"] for c in calls}
        assert job_ids == {"signal_snap", "order_submission", "fill_reconciliation", "daily_report"}

    def test_all_triggers_are_datetrigger_on_normal_day(self, monkeypatch):
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(monkeypatch, self._mock_schedule(True, close))
        for c in calls:
            trigger = c[0][1]
            assert isinstance(trigger, DateTrigger)

    def test_replace_existing_true(self, monkeypatch):
        """replace_existing=True prevents duplicate jobs on re-run."""
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(monkeypatch, self._mock_schedule(True, close))
        for c in calls:
            assert c[1].get("replace_existing") is True

    def test_normal_day_trigger_times(self, monkeypatch):
        """DateTrigger run_dates match close_time offsets on a normal day."""
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(monkeypatch, self._mock_schedule(True, close))

        by_id = {c[1]["id"]: c[0][1] for c in calls}  # id → DateTrigger

        expected = {
            "signal_snap":         close - timedelta(minutes=20),
            "order_submission":    close - timedelta(minutes=16),
            "fill_reconciliation": close + timedelta(minutes=10),
            "daily_report":        close + timedelta(minutes=15),
        }
        for job_id, exp_time in expected.items():
            actual_ts = by_id[job_id].run_date.timestamp()
            assert abs(actual_ts - exp_time.timestamp()) < 1, (
                f"{job_id}: expected {exp_time.strftime('%H:%M')}, "
                f"got {by_id[job_id].run_date.strftime('%H:%M')}"
            )

    def test_half_day_trigger_times_shift(self, monkeypatch):
        """On a half day (close 13:00) all four jobs fire at shifted times."""
        close = datetime(2024, 11, 29, 13, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(
            monkeypatch, self._mock_schedule(True, close, is_half_day=True)
        )

        by_id = {c[1]["id"]: c[0][1] for c in calls}

        expected = {
            "signal_snap":         close - timedelta(minutes=20),
            "order_submission":    close - timedelta(minutes=16),
            "fill_reconciliation": close + timedelta(minutes=10),
            "daily_report":        close + timedelta(minutes=15),
        }
        for job_id, exp_time in expected.items():
            actual_ts = by_id[job_id].run_date.timestamp()
            assert abs(actual_ts - exp_time.timestamp()) < 1, (
                f"{job_id}: expected {exp_time.strftime('%H:%M')}, "
                f"got {by_id[job_id].run_date.strftime('%H:%M')}"
            )

    def test_half_day_signal_snap_not_at_normal_time(self, monkeypatch):
        """Sanity check: half-day signal_snap is NOT at 15:40 ET."""
        close = datetime(2024, 11, 29, 13, 0, tzinfo=config.TZ)
        calls = self._run_and_collect(
            monkeypatch, self._mock_schedule(True, close, is_half_day=True)
        )
        by_id = {c[1]["id"]: c[0][1] for c in calls}
        normal_signal = datetime(2024, 11, 29, 15, 40, tzinfo=config.TZ)
        assert abs(by_id["signal_snap"].run_date.timestamp() - normal_signal.timestamp()) > 60


# ═══════════════════════════════════════════════════════════════════════════════
# TestMarketOpenCheckLeadTimeFilter
# ═══════════════════════════════════════════════════════════════════════════════

class TestMarketOpenCheckLeadTimeFilter:
    """market_open_check skips jobs that are less than SCHED_MIN_LEAD_MINS away."""

    def _run_and_collect(self, monkeypatch, schedule_dict, now_et):
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: schedule_dict)
        monkeypatch.setattr(sched_module, "datetime",
                            type("_DT", (), {"now": staticmethod(lambda tz=None: now_et)})())
        # Need timedelta still — patch only datetime.now
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now_et
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            sched_module.market_open_check()
        return mock_sched.add_job.call_args_list

    def test_all_jobs_registered_when_called_at_11(self, monkeypatch):
        """Called at 11:00 — all four jobs are far enough in the future."""
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        now   = datetime(2024, 1, 2, 11, 0, tzinfo=config.TZ)
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": True, "close_time": close, "is_half_day": False})
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            sched_module.market_open_check()
        assert mock_sched.add_job.call_count == 4

    def test_past_jobs_skipped_when_called_late(self, monkeypatch, tmp_path):
        """Called at 15:50 — signal_snap (15:40) and order_submission (15:44) are in the past."""
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        now   = datetime(2024, 1, 2, 15, 50, tzinfo=config.TZ)
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": True, "close_time": close, "is_half_day": False})
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS", 5)
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            sched_module.market_open_check()
        # fill_reconciliation (16:10) and daily_report (16:15) still have enough lead
        registered_ids = {c[1]["id"] for c in mock_sched.add_job.call_args_list}
        assert "signal_snap"      not in registered_ids
        assert "order_submission" not in registered_ids
        assert "fill_reconciliation" in registered_ids
        assert "daily_report"        in registered_ids

    def test_no_jobs_registered_when_all_past(self, monkeypatch):
        """Called after 16:20 — all four jobs are in the past."""
        close = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
        now   = datetime(2024, 1, 2, 16, 25, tzinfo=config.TZ)
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": True, "close_time": close, "is_half_day": False})
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS", 5)
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            sched_module.market_open_check()
        assert mock_sched.add_job.call_count == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TestStartupCatchup
# ═══════════════════════════════════════════════════════════════════════════════

class TestStartupCatchup:

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    def _trading_schedule(self, close_hour=16):
        close = datetime(2024, 1, 2, close_hour, 0, tzinfo=config.TZ)
        return {"is_open": True, "close_time": close, "is_half_day": close_hour < 14}

    def _non_trading_schedule(self):
        return {"is_open": False, "close_time": None, "is_half_day": False}

    def test_calls_market_open_check_when_started_after_1100(self, monkeypatch):
        """Started at 13:00 on a trading day → market_open_check called."""
        now = datetime(2024, 1, 2, 13, 0, tzinfo=config.TZ)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check") as mock_moc:
                with patch("scheduler.main") as mock_main:
                    # Stub universe update to avoid HTTP
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_moc.assert_called_once()

    def test_skips_market_open_check_before_1100(self, monkeypatch):
        """Started at 09:00 on a trading day → market_open_check NOT called."""
        now = datetime(2024, 1, 2, 9, 0, tzinfo=config.TZ)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check") as mock_moc:
                with patch("scheduler.main") as mock_main:
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_moc.assert_not_called()

    def test_skips_market_jobs_on_non_trading_day(self, monkeypatch):
        """Non-trading day → market_open_check NOT called regardless of time."""
        now = datetime(2024, 1, 1, 13, 0, tzinfo=config.TZ)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._non_trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check") as mock_moc:
                with patch("scheduler.main") as mock_main:
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_moc.assert_not_called()

    def test_skips_market_open_check_after_fill_reconciliation(self, monkeypatch):
        """Started after fill_reconciliation time (16:15) → market_open_check NOT called."""
        now = datetime(2024, 1, 2, 16, 30, tzinfo=config.TZ)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check") as mock_moc:
                with patch("scheduler.main") as mock_main:
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_moc.assert_not_called()

    def test_triggers_universe_update_when_stale(self, monkeypatch, tmp_path):
        """last_universe_update absent → sunday_universe_update called."""
        now = datetime(2024, 1, 2, 9, 0, tzinfo=config.TZ)
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._non_trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                with patch("scheduler.main") as mock_main:
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_main.sunday_universe_update.assert_called_once()

    def test_triggers_universe_update_when_8_days_old(self, monkeypatch):
        """last_universe_update is 8 days old → sunday_universe_update called."""
        now = datetime(2024, 1, 10, 9, 0, tzinfo=config.TZ)
        stale = (now - timedelta(days=8)).isoformat()
        # Write stale value directly
        with db.connect() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS system_state (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT OR REPLACE INTO system_state VALUES ('last_universe_update', ?)", (stale,))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._non_trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromisoformat = datetime.fromisoformat
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                with patch("scheduler.main") as mock_main:
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_main.sunday_universe_update.assert_called_once()

    def test_skips_universe_update_when_recent(self, monkeypatch):
        """last_universe_update is 2 days old → sunday_universe_update NOT called."""
        now = datetime(2024, 1, 10, 9, 0, tzinfo=config.TZ)
        recent = (now - timedelta(days=2)).isoformat()
        with db.connect() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS system_state (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT OR REPLACE INTO system_state VALUES ('last_universe_update', ?)", (recent,))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: self._non_trading_schedule())
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromisoformat = datetime.fromisoformat
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                with patch("scheduler.main") as mock_main:
                    mock_main.sunday_universe_update = MagicMock()
                    startup_catchup()
        mock_main.sunday_universe_update.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers shared by watchdog tests
# ═══════════════════════════════════════════════════════════════════════════════

class _StopWatchdog(BaseException):
    """Raised to break connection_watchdog()'s outer while-True loop in tests."""


def _make_bridge_mock(connect_effects):
    """
    Build a mock IBBridge whose connect() side effects are given by connect_effects:
    each item is an Exception to raise or None to succeed.  After the list is
    exhausted connect() always succeeds.

    wait_for_disconnect() returns on the first call and raises _StopWatchdog on
    all subsequent calls so the outer while-True exits cleanly after one cycle.
    """
    wait_calls = [0]

    def fake_wait():
        wait_calls[0] += 1
        if wait_calls[0] > 1:
            raise _StopWatchdog()

    effect_iter = iter(connect_effects)

    def fake_connect():
        try:
            effect = next(effect_iter)
            if effect is not None:
                raise effect
        except StopIteration:
            pass  # exhausted — succeed silently

    mock_bridge = MagicMock()
    mock_bridge.wait_for_disconnect.side_effect = fake_wait
    mock_bridge.connect.side_effect = fake_connect
    return mock_bridge


# ═══════════════════════════════════════════════════════════════════════════════
# TestConnectionWatchdog
# ═══════════════════════════════════════════════════════════════════════════════

class TestConnectionWatchdog:
    """connection_watchdog: indefinite retry, threshold alert, recovery message."""

    @pytest.fixture(autouse=True)
    def no_sleep(self, monkeypatch):
        monkeypatch.setattr(sched_module.time, "sleep", lambda _: None)

    def _run_cycle(self, monkeypatch, connect_effects, alert_after=None):
        """
        Run one disconnect→reconnect cycle of connection_watchdog().
        Returns (mock_monitor, attempt_count_via_connect_call_count).
        """
        if alert_after is not None:
            monkeypatch.setattr(config, "IB_RECONNECT_ALERT_AFTER", alert_after)

        mock_bridge = _make_bridge_mock(connect_effects)
        mock_main   = MagicMock()
        mock_main.bridge = mock_bridge

        mock_monitor = MagicMock()
        monkeypatch.setattr(sched_module, "main",    mock_main)
        monkeypatch.setattr(sched_module, "monitor", mock_monitor)
        monkeypatch.setattr(sched_module, "_post_reconnect_catchup", MagicMock())

        try:
            connection_watchdog()
        except _StopWatchdog:
            pass

        return mock_monitor, mock_bridge

    def test_sends_warning_alert_after_threshold_failures(self, monkeypatch):
        """After IB_RECONNECT_ALERT_AFTER failures a warning alert is sent exactly once."""
        n = 3  # threshold for this test
        # fail n times, then succeed
        effects = [RuntimeError("refused")] * n + [None]
        mock_monitor, _ = self._run_cycle(monkeypatch, effects, alert_after=n)

        warning_calls = [
            c for c in mock_monitor.send_alert.call_args_list
            if c.kwargs.get("level") == "warning" or (c.args and "lost" in c.args[0])
        ]
        assert len(warning_calls) == 1

    def test_no_alert_when_succeeds_before_threshold(self, monkeypatch):
        """Fewer than IB_RECONNECT_ALERT_AFTER failures → no alert sent at all."""
        # fail 2 times, threshold = 5, then succeed
        effects = [RuntimeError("refused")] * 2 + [None]
        mock_monitor, _ = self._run_cycle(monkeypatch, effects, alert_after=5)
        mock_monitor.send_alert.assert_not_called()

    def test_sends_recovery_alert_when_alert_was_sent(self, monkeypatch):
        """If a warning alert was sent, a recovery 'info' alert must follow on success."""
        n = 3
        effects = [RuntimeError("refused")] * n + [None]
        mock_monitor, _ = self._run_cycle(monkeypatch, effects, alert_after=n)

        info_calls = [
            c for c in mock_monitor.send_alert.call_args_list
            if c.kwargs.get("level") == "info" or (c.args and "restored" in c.args[0].lower())
        ]
        assert len(info_calls) == 1

    def test_no_recovery_alert_when_no_warning_sent(self, monkeypatch):
        """If no warning was sent, no recovery alert is sent either."""
        effects = [RuntimeError("refused")] * 2 + [None]  # threshold = 5
        mock_monitor, _ = self._run_cycle(monkeypatch, effects, alert_after=5)
        mock_monitor.send_alert.assert_not_called()

    def test_retries_indefinitely_past_old_limit(self, monkeypatch):
        """Watchdog retries far beyond 3 (the old hard limit) without giving up."""
        # fail 20 times, then succeed — no exception expected
        effects = [RuntimeError("refused")] * 20 + [None]
        mock_monitor, mock_bridge = self._run_cycle(monkeypatch, effects, alert_after=5)
        assert mock_bridge.connect.call_count == 21  # 20 failures + 1 success

    def test_clears_disconnect_event_before_retrying(self, monkeypatch):
        """clear_disconnect() is called once as soon as a disconnect is detected."""
        effects = [None]  # succeed on first attempt
        _, mock_bridge = self._run_cycle(monkeypatch, effects)
        mock_bridge.clear_disconnect.assert_called_once()

    def test_calls_post_reconnect_catchup_after_success(self, monkeypatch):
        """_post_reconnect_catchup() is called once after a successful reconnect."""
        effects = [None]
        mock_catchup = MagicMock()
        monkeypatch.setattr(config, "IB_RECONNECT_ALERT_AFTER", 5)

        mock_bridge = _make_bridge_mock(effects)
        mock_main   = MagicMock()
        mock_main.bridge = mock_bridge

        monkeypatch.setattr(sched_module, "main",    mock_main)
        monkeypatch.setattr(sched_module, "monitor", MagicMock())
        monkeypatch.setattr(sched_module, "_post_reconnect_catchup", mock_catchup)
        monkeypatch.setattr(sched_module.time, "sleep", lambda _: None)

        try:
            connection_watchdog()
        except _StopWatchdog:
            pass

        mock_catchup.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# TestPostReconnectCatchup
# ═══════════════════════════════════════════════════════════════════════════════

class TestPostReconnectCatchup:
    """_post_reconnect_catchup: correct job sequencing logic after reconnect."""

    # close 16:00 ET on 2024-01-02 (normal trading day)
    _CLOSE = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)

    def _run(self, monkeypatch, now):
        """
        Call _post_reconnect_catchup() with a fixed schedule and fixed now.
        Returns mock_scheduler.add_job.call_args_list.
        """
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(
            sched_module, "get_market_schedule",
            lambda d=None: {"is_open": True, "close_time": self._CLOSE, "is_half_day": False},
        )
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS", 5)

        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            _post_reconnect_catchup()

        return mock_sched.add_job.call_args_list

    def _job_ids(self, calls):
        return {c[1]["id"] for c in calls}

    def _immediate_ids(self, calls):
        """IDs whose DateTrigger run_date is close to now+5s (i.e. not the natural time)."""
        natural_times = {
            self._CLOSE + timedelta(minutes=config.SCHED_SIGNAL_OFFSET_MIN),
            self._CLOSE + timedelta(minutes=config.SCHED_ORDER_OFFSET_MIN),
            self._CLOSE + timedelta(minutes=config.SCHED_FILL_OFFSET_MIN),
            self._CLOSE + timedelta(minutes=config.SCHED_REPORT_OFFSET_MIN),
        }
        result = set()
        for c in calls:
            trigger = c[0][1]
            if all(abs(trigger.run_date.timestamp() - t.timestamp()) > 30 for t in natural_times):
                result.add(c[1]["id"])
        return result

    def test_all_jobs_scheduled_at_natural_time_early_reconnect(self, monkeypatch):
        """Reconnect at 11:00 — all 4 jobs scheduled at their natural times."""
        now   = datetime(2024, 1, 2, 11, 0, tzinfo=config.TZ)
        calls = self._run(monkeypatch, now)
        assert self._job_ids(calls) == {"signal_snap", "order_submission", "fill_reconciliation", "daily_report"}
        assert len(self._immediate_ids(calls)) == 0  # none are immediate

    def test_signal_snap_runs_immediately_when_missed(self, monkeypatch):
        """
        Reconnect at 15:45 (after signal_snap 15:40, before order_submission 15:44 is
        already passed too — use 15:41 instead to have order_submission still future).
        Reconnect at 15:41: signal_snap missed → immediate; order_submission (15:44) future.
        """
        now   = datetime(2024, 1, 2, 15, 41, tzinfo=config.TZ)
        calls = self._run(monkeypatch, now)
        ids = self._job_ids(calls)
        assert "signal_snap" in ids
        # signal_snap must be scheduled immediately (15:40 already past)
        assert "signal_snap" in self._immediate_ids(calls)

    def test_signal_snap_skipped_when_order_submission_passed(self, monkeypatch):
        """
        Reconnect at 15:50 — order_submission (15:44) has passed so signal_snap is skipped.
        order_submission itself is still worth running (fill_reconciliation at 16:10 not yet
        passed), so it runs immediately.
        """
        now   = datetime(2024, 1, 2, 15, 50, tzinfo=config.TZ)
        calls = self._run(monkeypatch, now)
        assert "signal_snap" not in self._job_ids(calls)
        # order_submission missed but still valid — runs immediately
        assert "order_submission"    in self._job_ids(calls)
        assert "order_submission"    in self._immediate_ids(calls)
        assert "fill_reconciliation" in self._job_ids(calls)
        assert "daily_report"        in self._job_ids(calls)

    def _run_tracking(self, monkeypatch, now):
        """
        Like _run(), but also patches job_fill_reconciliation and job_daily_report
        to track direct (past-window sequential) calls.

        Returns (add_job_calls, directly_called_ids) where directly_called_ids
        is a list of job IDs whose functions were called directly (in call order).
        """
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(
            sched_module, "get_market_schedule",
            lambda d=None: {"is_open": True, "close_time": self._CLOSE, "is_half_day": False},
        )
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS", 5)

        call_order: list[str] = []
        mock_fill   = MagicMock(side_effect=lambda: call_order.append("fill_reconciliation"))
        mock_report = MagicMock(side_effect=lambda: call_order.append("daily_report"))
        monkeypatch.setattr(sched_module, "job_fill_reconciliation", mock_fill)
        monkeypatch.setattr(sched_module, "job_daily_report",        mock_report)

        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            _post_reconnect_catchup()

        return mock_sched.add_job.call_args_list, call_order

    def test_order_submission_skipped_when_fill_passed(self, monkeypatch):
        """Reconnect at 16:12 (fill_reconciliation 16:10 passed) → signal & order skipped;
        fill and report called directly (sequential)."""
        now = datetime(2024, 1, 2, 16, 12, tzinfo=config.TZ)
        calls, direct = self._run_tracking(monkeypatch, now)
        assert "signal_snap"         not in self._job_ids(calls)
        assert "order_submission"    not in self._job_ids(calls)
        assert "fill_reconciliation" in direct
        assert "daily_report"        in direct

    def test_fill_and_report_run_immediately_when_missed_same_day(self, monkeypatch):
        """Reconnect at 16:20 (both fill and report times passed) → both called directly."""
        now = datetime(2024, 1, 2, 16, 20, tzinfo=config.TZ)
        calls, direct = self._run_tracking(monkeypatch, now)
        assert "fill_reconciliation" in direct
        assert "daily_report"        in direct
        assert "fill_reconciliation" not in self._job_ids(calls)
        assert "daily_report"        not in self._job_ids(calls)

    def test_fill_called_before_report_when_both_missed(self, monkeypatch):
        """When both fill and report are past their window, fill runs before report."""
        now = datetime(2024, 1, 2, 16, 20, tzinfo=config.TZ)
        _calls, direct = self._run_tracking(monkeypatch, now)
        assert direct.index("fill_reconciliation") < direct.index("daily_report")

    def test_nothing_scheduled_on_non_trading_day(self, monkeypatch):
        """Non-trading day → _post_reconnect_catchup schedules nothing."""
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(
            sched_module, "get_market_schedule",
            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False},
        )
        now = datetime(2024, 1, 1, 13, 0, tzinfo=config.TZ)
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            _post_reconnect_catchup()
        mock_sched.add_job.assert_not_called()

    def test_fill_and_report_skipped_on_next_day(self, monkeypatch):
        """Reconnect next day (different date than close_time) → fill and report skipped."""
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        # close is 2024-01-02, reconnect is 2024-01-03
        monkeypatch.setattr(
            sched_module, "get_market_schedule",
            lambda d=None: {"is_open": True, "close_time": self._CLOSE, "is_half_day": False},
        )
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS", 5)
        now = datetime(2024, 1, 3, 9, 0, tzinfo=config.TZ)
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            _post_reconnect_catchup()
        scheduled_ids = {c[1]["id"] for c in mock_sched.add_job.call_args_list}
        assert "fill_reconciliation" not in scheduled_ids
        assert "daily_report"        not in scheduled_ids


# ═══════════════════════════════════════════════════════════════════════════════
# TestStartupHaltWarning
# ═══════════════════════════════════════════════════════════════════════════════

class TestStartupHaltWarning:
    """_startup_halt_warning: logs CRITICAL and sends alert when halt/shutdown active."""

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH",   str(tmp_path / "test.db"))
        import db as db_mod
        db_mod.init_db()

    def test_logs_critical_when_halted(self, monkeypatch, caplog):
        """is_halted() True → logger.critical fires."""
        monkeypatch.setattr(sched_module, "risk_engine", MagicMock(
            is_halted=lambda: True,
            is_shutdown=lambda: False,
        ))
        with caplog.at_level("CRITICAL", logger="murphy"):
            _startup_halt_warning()
        assert any("HALT" in r.message for r in caplog.records if r.levelname == "CRITICAL")

    def test_logs_critical_when_shutdown(self, monkeypatch, caplog):
        """is_shutdown() True → logger.critical fires with SHUTDOWN in message."""
        monkeypatch.setattr(sched_module, "risk_engine", MagicMock(
            is_halted=lambda: True,
            is_shutdown=lambda: True,
        ))
        with caplog.at_level("CRITICAL", logger="murphy"):
            _startup_halt_warning()
        assert any("SHUTDOWN" in r.message for r in caplog.records if r.levelname == "CRITICAL")

    def test_sends_critical_alert_when_halted(self, monkeypatch):
        """is_halted() True → monitor.send_alert called with level='critical'."""
        monkeypatch.setattr(sched_module, "risk_engine", MagicMock(
            is_halted=lambda: True,
            is_shutdown=lambda: False,
        ))
        mock_monitor = MagicMock()
        monkeypatch.setattr(sched_module, "monitor", mock_monitor)
        _startup_halt_warning()
        mock_monitor.send_alert.assert_called_once()
        assert mock_monitor.send_alert.call_args.kwargs.get("level") == "critical" or \
               mock_monitor.send_alert.call_args[1].get("level") == "critical" or \
               (mock_monitor.send_alert.call_args[0] and len(mock_monitor.send_alert.call_args[0]) >= 3 and
                mock_monitor.send_alert.call_args[0][2] == "critical")

    def test_no_alert_when_not_halted(self, monkeypatch):
        """Neither halt nor shutdown active → no alert, no CRITICAL log."""
        monkeypatch.setattr(sched_module, "risk_engine", MagicMock(
            is_halted=lambda: False,
            is_shutdown=lambda: False,
        ))
        mock_monitor = MagicMock()
        monkeypatch.setattr(sched_module, "monitor", mock_monitor)
        _startup_halt_warning()
        mock_monitor.send_alert.assert_not_called()

    def test_scheduler_continues_when_halted(self, monkeypatch):
        """_startup_halt_warning returns normally even when halt active (does not abort)."""
        monkeypatch.setattr(sched_module, "risk_engine", MagicMock(
            is_halted=lambda: True,
            is_shutdown=lambda: False,
        ))
        monkeypatch.setattr(sched_module, "monitor", MagicMock())
        # Must not raise
        _startup_halt_warning()


# ═══════════════════════════════════════════════════════════════════════════════
# TestStartupCatchupDataSync
# ═══════════════════════════════════════════════════════════════════════════════

class TestStartupCatchupDataSync:
    """startup_catchup: nightly data catch-up logic."""

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH",   str(tmp_path / "test.db"))
        db.init_db()

    def _make_nyse_mock(self, last_close_et: datetime):
        """Build a mock _nyse whose schedule() returns a DataFrame with one row."""
        import pandas as pd
        from zoneinfo import ZoneInfo
        utc = ZoneInfo("UTC")
        close_utc = last_close_et.astimezone(utc)
        ts = pd.Timestamp(close_utc)
        idx = pd.DatetimeIndex([last_close_et.date()], name="date")
        df  = pd.DataFrame({"market_close": [ts]}, index=idx)
        mock_nyse = MagicMock()
        mock_nyse.schedule.return_value = df
        return mock_nyse

    def _base_patches(self, monkeypatch, now, max_bar_date_str, last_close_et):
        """Apply common monkeypatches for data catch-up tests."""
        monkeypatch.setattr(sched_module, "_nyse", self._make_nyse_mock(last_close_et))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False})
        # Write max_bar_date into daily_bars via direct SQL
        if max_bar_date_str:
            with db.connect() as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS daily_bars "
                    "(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
                )
                conn.execute(
                    "INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)",
                    ("AAPL", max_bar_date_str, 100, 110, 90, 105, 1000000),
                )
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            yield mock_dt

    def test_fetch_called_when_db_stale(self, monkeypatch, tmp_path):
        """DB last date is 3 days before last NYSE close → fetch_incremental called."""
        last_close_et = datetime(2024, 1, 5, 16, 0, tzinfo=config.TZ)  # Friday
        max_bar_date  = "2024-01-02"  # 3 days before close
        now           = datetime(2024, 1, 8, 9, 0, tzinfo=config.TZ)   # Monday morning

        mock_fetch = MagicMock(return_value=10)
        mock_main  = MagicMock()
        mock_main.sunday_universe_update = MagicMock()
        mock_main._load_universe.return_value = ["AAPL", "MSFT"]

        monkeypatch.setattr(sched_module, "_nyse", self._make_nyse_mock(last_close_et))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False})
        monkeypatch.setattr(sched_module, "td_data", MagicMock(fetch_incremental=mock_fetch))
        monkeypatch.setattr(sched_module, "main", mock_main)

        # Insert stale bar date
        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS daily_bars "
                "(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            conn.execute("INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)",
                         ("AAPL", max_bar_date, 100, 110, 90, 105, 1000000))

        # Patch fromisoformat for the datetime mock
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                startup_catchup()

        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args
        # n_days should be gap + 2 = (2024-01-08 - 2024-01-02).days + 2 = 6 + 2 = 8
        n_days_arg = call_kwargs[1].get("n_days") or call_kwargs[0][1]
        assert n_days_arg >= 5  # at minimum gap+2

    def test_precompute_called_after_fetch(self, monkeypatch):
        """After fetch_incremental, precompute_watchlist is called."""
        last_close_et = datetime(2024, 1, 5, 16, 0, tzinfo=config.TZ)
        max_bar_date  = "2024-01-02"
        now           = datetime(2024, 1, 8, 9, 0, tzinfo=config.TZ)

        mock_fetch = MagicMock(return_value=10)
        mock_main  = MagicMock()
        mock_main.sunday_universe_update = MagicMock()
        mock_main._load_universe.return_value = ["AAPL"]

        monkeypatch.setattr(sched_module, "_nyse", self._make_nyse_mock(last_close_et))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False})
        monkeypatch.setattr(sched_module, "td_data", MagicMock(fetch_incremental=mock_fetch))
        monkeypatch.setattr(sched_module, "main", mock_main)

        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS daily_bars "
                "(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            conn.execute("INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)",
                         ("AAPL", max_bar_date, 100, 110, 90, 105, 1000000))

        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                startup_catchup()

        mock_main.precompute_watchlist.assert_called_once()

    def test_no_fetch_when_db_current(self, monkeypatch):
        """DB last date == last NYSE close → fetch_incremental NOT called."""
        last_close_et = datetime(2024, 1, 5, 16, 0, tzinfo=config.TZ)
        max_bar_date  = "2024-01-05"  # same as close date
        now           = datetime(2024, 1, 8, 9, 0, tzinfo=config.TZ)

        mock_fetch = MagicMock(return_value=0)
        mock_main  = MagicMock()
        mock_main.sunday_universe_update = MagicMock()
        mock_main._load_universe.return_value = ["AAPL"]

        monkeypatch.setattr(sched_module, "_nyse", self._make_nyse_mock(last_close_et))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False})
        monkeypatch.setattr(sched_module, "td_data", MagicMock(fetch_incremental=mock_fetch))
        monkeypatch.setattr(sched_module, "main", mock_main)

        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS daily_bars "
                "(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            conn.execute("INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)",
                         ("AAPL", max_bar_date, 100, 110, 90, 105, 1000000))

        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                startup_catchup()

        mock_fetch.assert_not_called()

    def test_sync_skipped_when_would_overlap_market_open(self, monkeypatch):
        """
        Startup at 10:50 with 500 symbols — estimated sync time far exceeds the
        5-min window to the 10:55 deadline (11:00 − 5 min lead) → fetch skipped.
        """
        last_close_et = datetime(2024, 1, 5, 16, 0, tzinfo=config.TZ)
        max_bar_date  = "2024-01-02"
        now           = datetime(2024, 1, 8, 10, 50, tzinfo=config.TZ)  # 5 min to deadline

        mock_fetch = MagicMock()
        mock_main  = MagicMock()
        mock_main.sunday_universe_update = MagicMock()
        mock_main._load_universe.return_value = ["SYM"] * 500  # ceil(500/8)=63 batches → 3780s

        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",        8)
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 8)
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS",           5)
        monkeypatch.setattr(sched_module, "_nyse", self._make_nyse_mock(last_close_et))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False})
        monkeypatch.setattr(sched_module, "td_data", MagicMock(fetch_incremental=mock_fetch))
        monkeypatch.setattr(sched_module, "main", mock_main)

        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS daily_bars "
                "(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            conn.execute("INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)",
                         ("AAPL", max_bar_date, 100, 110, 90, 105, 1000000))

        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                startup_catchup()

        mock_fetch.assert_not_called()

    def test_sync_proceeds_when_ample_time_before_market_open(self, monkeypatch):
        """
        Startup at 05:00 with 2 symbols — 1 batch → 60s estimate, well within
        the ~6-hour window to the deadline → fetch proceeds normally.
        """
        last_close_et = datetime(2024, 1, 5, 16, 0, tzinfo=config.TZ)
        max_bar_date  = "2024-01-02"
        now           = datetime(2024, 1, 8, 5, 0, tzinfo=config.TZ)  # 6 h before deadline

        mock_fetch = MagicMock(return_value=10)
        mock_main  = MagicMock()
        mock_main.sunday_universe_update = MagicMock()
        mock_main._load_universe.return_value = ["AAPL", "MSFT"]  # 1 batch → 60s

        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",        8)
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 8)
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS",           5)
        monkeypatch.setattr(sched_module, "_nyse", self._make_nyse_mock(last_close_et))
        monkeypatch.setattr(sched_module, "get_market_schedule",
                            lambda d=None: {"is_open": False, "close_time": None, "is_half_day": False})
        monkeypatch.setattr(sched_module, "td_data", MagicMock(fetch_incremental=mock_fetch))
        monkeypatch.setattr(sched_module, "main", mock_main)

        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS daily_bars "
                "(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            conn.execute("INSERT INTO daily_bars VALUES (?,?,?,?,?,?,?)",
                         ("AAPL", max_bar_date, 100, 110, 90, 105, 1000000))

        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("scheduler.market_open_check"):
                startup_catchup()

        mock_fetch.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# TestJobTracker — _jobs_run_today idempotency
# ═══════════════════════════════════════════════════════════════════════════════

class TestJobTracker:
    """
    Tests for _jobs_run_today completion tracking:
      - _reset_daily_job_tracker() clears the dict
      - _post_reconnect_catchup() skips a job whose job_id is already in the tracker
      - _post_reconnect_catchup() schedules a job whose job_id is absent from the tracker
    """

    # Normal trading day used in all sub-tests — close 16:00 ET on 2024-01-02
    _CLOSE = datetime(2024, 1, 2, 16, 0, tzinfo=config.TZ)
    _TODAY = date(2024, 1, 2)

    @pytest.fixture(autouse=True)
    def reset_tracker(self):
        """Ensure _jobs_run_today is empty before and after every test."""
        sched_module._jobs_run_today.clear()
        yield
        sched_module._jobs_run_today.clear()

    def _run_catchup(self, monkeypatch, now):
        """
        Call _post_reconnect_catchup() with a fixed schedule.

        Patches job_fill_reconciliation and job_daily_report to prevent real
        execution when they are called directly (past-window sequential path).

        Returns (add_job_calls, directly_called_ids) where directly_called_ids
        is a list of job IDs called directly in call order.
        """
        mock_sched  = MagicMock()
        direct_ids: list[str] = []
        mock_fill   = MagicMock(side_effect=lambda: direct_ids.append("fill_reconciliation"))
        mock_report = MagicMock(side_effect=lambda: direct_ids.append("daily_report"))
        monkeypatch.setattr(sched_module, "_scheduler",              mock_sched)
        monkeypatch.setattr(sched_module, "job_fill_reconciliation", mock_fill)
        monkeypatch.setattr(sched_module, "job_daily_report",        mock_report)
        monkeypatch.setattr(
            sched_module, "get_market_schedule",
            lambda d=None: {"is_open": True, "close_time": self._CLOSE, "is_half_day": False},
        )
        monkeypatch.setattr(config, "SCHED_MIN_LEAD_MINS", 5)
        with patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            _post_reconnect_catchup()
        return mock_sched.add_job.call_args_list, direct_ids

    def test_reset_clears_all_entries(self):
        """_reset_daily_job_tracker() empties _jobs_run_today regardless of prior content."""
        sched_module._jobs_run_today["fill_reconciliation"] = self._TODAY
        sched_module._jobs_run_today["daily_report"]        = self._TODAY
        _reset_daily_job_tracker()
        assert sched_module._jobs_run_today == {}

    def test_post_reconnect_skips_fill_when_already_run_today(self, monkeypatch):
        """
        fill_reconciliation recorded in tracker → _post_reconnect_catchup must not
        call it even though it is still the same trading day.

        Reconnect at 16:12 (fill_reconciliation at 16:10 has passed, same day).
        Without the tracker, fill_reconciliation would be called directly.
        With the tracker entry, it must be skipped entirely.
        """
        now = datetime(2024, 1, 2, 16, 12, tzinfo=config.TZ)
        sched_module._jobs_run_today["fill_reconciliation"] = self._TODAY

        calls, direct_ids = self._run_catchup(monkeypatch, now)
        scheduled_ids = {c[1]["id"] for c in calls}
        assert "fill_reconciliation" not in scheduled_ids
        assert "fill_reconciliation" not in direct_ids

    def test_post_reconnect_runs_fill_when_not_yet_run_today(self, monkeypatch):
        """
        fill_reconciliation absent from tracker → _post_reconnect_catchup must call it directly.

        Same scenario (reconnect at 16:12, same trading day) but tracker is empty.
        fill_reconciliation is past its window so it is called directly, not scheduled.
        """
        now = datetime(2024, 1, 2, 16, 12, tzinfo=config.TZ)
        # tracker is empty (autouse fixture ensures this)

        calls, direct_ids = self._run_catchup(monkeypatch, now)
        assert "fill_reconciliation" in direct_ids

    def test_post_reconnect_skips_only_already_run_jobs(self, monkeypatch):
        """
        Partial tracker: fill_reconciliation ran, daily_report did not.
        fill_reconciliation must be skipped; daily_report must be called directly.
        """
        now = datetime(2024, 1, 2, 16, 20, tzinfo=config.TZ)
        sched_module._jobs_run_today["fill_reconciliation"] = self._TODAY
        # daily_report not in tracker

        calls, direct_ids = self._run_catchup(monkeypatch, now)
        scheduled_ids = {c[1]["id"] for c in calls}
        assert "fill_reconciliation" not in scheduled_ids
        assert "fill_reconciliation" not in direct_ids
        assert "daily_report"        in direct_ids
