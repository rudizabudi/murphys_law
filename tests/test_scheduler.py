"""
tests/test_scheduler.py — Unit tests for scheduler.py

Real-date tests use pandas_market_calendars directly — no mocking.
market_open_check tests mock get_market_schedule and _scheduler so no live
APScheduler instance is required.
"""

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

import config
import scheduler as sched_module
from apscheduler.triggers.date import DateTrigger
from scheduler import get_market_schedule, market_open_check


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

    def _run_and_collect(self, monkeypatch, schedule_dict):
        """Set up mocks, call market_open_check, return list of add_job calls."""
        mock_sched = MagicMock()
        monkeypatch.setattr(sched_module, "_scheduler", mock_sched)
        monkeypatch.setattr(sched_module, "get_market_schedule", lambda d=None: schedule_dict)
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
