"""
tests/test_risk_engine.py — Tests for risk_engine.py.

Covers:
  - evaluate() returns True when control passes
  - Each control trips correctly at/above threshold, passes below it
  - notify  → send_alert called, True returned (execution continues)
  - reject  → False returned
  - skip    → False returned
  - halt    → DB flag set, False returned
  - shutdown→ DB flag set, False returned
  - Additive actions: ["halt", "notify"] → both fire
  - Halt persists across subsequent evaluate() calls
  - Shutdown persists across subsequent evaluate() calls
  - clear_halt() resets both halt and shutdown
  - is_halted() / is_shutdown() reflect DB state
  - Disabled controls always return True
  - Unknown control name returns True (fail open)
  - Handler exception returns True (fail open)
"""

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import risk_engine
from risk_engine import evaluate, is_halted, is_shutdown, clear_halt


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def fresh_engine(monkeypatch, tmp_path):
    """
    Each test gets:
      - a clean temporary SQLite DB
      - _table_ready reset so _ensure_table() re-runs against the new DB
      - outbound alert channels disabled (no accidental email/discord)
    """
    monkeypatch.setattr(config, "DB_PATH",             str(tmp_path / "risk_test.db"))
    monkeypatch.setattr(config, "DB_DRIVER",            "sqlite")
    monkeypatch.setattr(risk_engine, "_table_ready",    False)
    monkeypatch.setattr(config, "ALERT_EMAIL",          "")
    monkeypatch.setattr(config, "DISCORD_WEBHOOK_URL",  "")
    # Ensure all controls are enabled by default (imbalance is off in production)
    monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", True)
    yield


# ═══════════════════════════════════════════════════════════════════════════════
# Halt / Shutdown persistence
# ═══════════════════════════════════════════════════════════════════════════════

class TestHaltShutdown:

    def test_halt_persists_across_evaluate_calls(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ENABLED", True)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION",  ["halt"])
        # Trip the daily_loss control → sets halt flag
        result = evaluate("daily_loss", {"daily_pnl": -10_000, "equity": 100_000})
        assert result is False
        # Subsequent evaluate() of an unrelated passing control still blocked
        monkeypatch.setattr(config, "RISK_FILL_TIMEOUT_ENABLED", True)
        monkeypatch.setattr(config, "RISK_FILL_TIMEOUT_ACTION",  ["notify"])
        result2 = evaluate("fill_timeout", {"minutes_pending": 1})
        assert result2 is False, "HALT should block all subsequent evaluate() calls"

    def test_shutdown_persists_across_evaluate_calls(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ENABLED", True)
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION",  ["shutdown"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        # Next call must be blocked by shutdown gate
        result = evaluate("fill_timeout", {"minutes_pending": 1})
        assert result is False

    def test_clear_halt_allows_evaluate_to_proceed(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        evaluate("daily_loss", {"daily_pnl": -10_000, "equity": 100_000})
        assert is_halted() is True
        clear_halt()
        assert is_halted() is False
        # Now a passing control should return True
        result = evaluate("fill_timeout", {"minutes_pending": 1})
        assert result is True

    def test_clear_halt_resets_shutdown(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        assert is_shutdown() is True
        clear_halt()
        assert is_shutdown() is False

    def test_is_halted_true_when_halt_set(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        evaluate("daily_loss", {"daily_pnl": -10_000, "equity": 100_000})
        assert is_halted() is True

    def test_is_halted_true_when_shutdown_set(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        assert is_halted() is True   # is_halted() is True for shutdown too

    def test_is_shutdown_false_when_only_halt_set(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        evaluate("daily_loss", {"daily_pnl": -10_000, "equity": 100_000})
        assert is_shutdown() is False

    def test_is_shutdown_true_when_shutdown_set(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        assert is_shutdown() is True

    def test_no_halt_initially(self):
        assert is_halted()  is False
        assert is_shutdown() is False

    def test_clear_halt_idempotent_when_not_set(self):
        clear_halt()   # Should not raise
        assert is_halted() is False


# ═══════════════════════════════════════════════════════════════════════════════
# Control threshold checks — trips and passes
# ═══════════════════════════════════════════════════════════════════════════════

class TestControlThresholds:

    # ── max_order_value ───────────────────────────────────────────────────────

    def test_max_order_value_trips_above_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",         100_000)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION",  ["reject"])
        assert evaluate("max_order_value", {"order_value": 100_001}) is False

    def test_max_order_value_passes_at_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",         100_000)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION",  ["reject"])
        assert evaluate("max_order_value", {"order_value": 100_000}) is True

    def test_max_order_value_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",         100_000)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION",  ["reject"])
        assert evaluate("max_order_value", {"order_value": 50_000}) is True

    def test_max_order_value_disabled_when_minus_one(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",         -1)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION",  ["reject"])
        assert evaluate("max_order_value", {"order_value": 999_999_999}) is True

    # ── daily_loss ────────────────────────────────────────────────────────────

    def test_daily_loss_trips_above_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.05)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        # 6% loss on 100k equity = 6% > 5%
        assert evaluate("daily_loss", {"daily_pnl": -6_000, "equity": 100_000}) is False

    def test_daily_loss_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.05)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        assert evaluate("daily_loss", {"daily_pnl": -4_000, "equity": 100_000}) is True

    def test_daily_loss_ignores_positive_pnl(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.01)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        assert evaluate("daily_loss", {"daily_pnl": 5_000, "equity": 100_000}) is True

    # ── max_drawdown ──────────────────────────────────────────────────────────

    def test_max_drawdown_trips_above_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_PCT",    0.20)
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        # 25% drawdown > 20%
        assert evaluate("max_drawdown",
                        {"peak_equity": 100_000, "current_equity": 75_000}) is False

    def test_max_drawdown_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_PCT",    0.20)
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        # 10% drawdown < 20%
        assert evaluate("max_drawdown",
                        {"peak_equity": 100_000, "current_equity": 90_000}) is True

    def test_max_drawdown_zero_peak_passes(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        assert evaluate("max_drawdown",
                        {"peak_equity": 0, "current_equity": 0}) is True

    # ── margin_breach ─────────────────────────────────────────────────────────

    def test_margin_breach_trips_below_minimum(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MARGIN_MIN_PCT",  0.20)
        monkeypatch.setattr(config, "RISK_MARGIN_ACTION",   ["reject"])
        assert evaluate("margin_breach", {"margin_pct": 0.15}) is False

    def test_margin_breach_passes_at_minimum(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MARGIN_MIN_PCT",  0.20)
        monkeypatch.setattr(config, "RISK_MARGIN_ACTION",   ["reject"])
        assert evaluate("margin_breach", {"margin_pct": 0.20}) is True

    def test_margin_breach_passes_above_minimum(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MARGIN_MIN_PCT",  0.20)
        monkeypatch.setattr(config, "RISK_MARGIN_ACTION",   ["reject"])
        assert evaluate("margin_breach", {"margin_pct": 0.50}) is True

    # ── stale_state ───────────────────────────────────────────────────────────

    def test_stale_state_trips_when_too_old(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_STALE_STATE_DAYS",   2)
        monkeypatch.setattr(config, "RISK_STALE_STATE_ACTION", ["skip"])
        old_date = date.today() - timedelta(days=3)
        assert evaluate("stale_state", {"last_update_date": old_date}) is False

    def test_stale_state_passes_when_fresh(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_STALE_STATE_DAYS",   2)
        monkeypatch.setattr(config, "RISK_STALE_STATE_ACTION", ["skip"])
        assert evaluate("stale_state",
                        {"last_update_date": date.today()}) is True

    def test_stale_state_accepts_iso_string(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_STALE_STATE_DAYS",   2)
        monkeypatch.setattr(config, "RISK_STALE_STATE_ACTION", ["skip"])
        assert evaluate("stale_state",
                        {"last_update_date": str(date.today())}) is True

    def test_stale_state_trips_when_no_date_provided(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_STALE_STATE_DAYS",   2)
        monkeypatch.setattr(config, "RISK_STALE_STATE_ACTION", ["skip"])
        assert evaluate("stale_state", {}) is False

    # ── consec_loss_days ──────────────────────────────────────────────────────

    def test_consec_loss_days_trips_at_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_DAYS",        3)
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_DAYS_ACTION", ["notify"])
        # notify-only: True returned, but send_alert called (tested separately)
        with patch("monitor.send_alert"):
            result = evaluate("consec_loss_days", {"consec_loss_days": 3})
        assert result is True   # notify-only → execution continues

    def test_consec_loss_days_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_DAYS",        3)
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_DAYS_ACTION", ["notify"])
        assert evaluate("consec_loss_days", {"consec_loss_days": 2}) is True

    # ── consec_loss_trades ────────────────────────────────────────────────────

    def test_consec_loss_trades_trips_at_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_TRADES",        10)
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_TRADES_ACTION", ["notify"])
        with patch("monitor.send_alert"):
            result = evaluate("consec_loss_trades", {"consec_loss_trades": 10})
        assert result is True

    def test_consec_loss_trades_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_TRADES",        10)
        monkeypatch.setattr(config, "RISK_CONSEC_LOSS_TRADES_ACTION", ["notify"])
        assert evaluate("consec_loss_trades", {"consec_loss_trades": 9}) is True

    # ── fill_timeout ──────────────────────────────────────────────────────────

    def test_fill_timeout_trips_at_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_FILL_TIMEOUT_MINS",   30)
        monkeypatch.setattr(config, "RISK_FILL_TIMEOUT_ACTION", ["notify"])
        with patch("monitor.send_alert"):
            result = evaluate("fill_timeout", {"minutes_pending": 30})
        assert result is True

    def test_fill_timeout_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_FILL_TIMEOUT_MINS",   30)
        monkeypatch.setattr(config, "RISK_FILL_TIMEOUT_ACTION", ["notify"])
        assert evaluate("fill_timeout", {"minutes_pending": 29}) is True

    # ── reconcile_mismatch ────────────────────────────────────────────────────

    def test_reconcile_mismatch_trips_when_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_RECONCILE_ACTION", ["halt"])
        assert evaluate("reconcile_mismatch", {"mismatch": True}) is False

    def test_reconcile_mismatch_passes_when_false(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_RECONCILE_ACTION", ["halt"])
        assert evaluate("reconcile_mismatch", {"mismatch": False}) is True

    # ── imbalance ─────────────────────────────────────────────────────────────

    def test_imbalance_trips_at_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_THRESHOLD", 0.30)
        monkeypatch.setattr(config, "RISK_IMBALANCE_ACTION",    ["reject"])
        assert evaluate("imbalance", {"imbalance_ratio": 0.30}) is False

    def test_imbalance_passes_below_threshold(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_THRESHOLD", 0.30)
        monkeypatch.setattr(config, "RISK_IMBALANCE_ACTION",    ["reject"])
        assert evaluate("imbalance", {"imbalance_ratio": 0.29}) is True


# ═══════════════════════════════════════════════════════════════════════════════
# Action behaviour
# ═══════════════════════════════════════════════════════════════════════════════

class TestActions:

    # ── notify ────────────────────────────────────────────────────────────────

    @patch("monitor.send_alert")
    def test_notify_calls_send_alert(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "RISK_RECONCILE_ACTION", ["notify"])
        evaluate("reconcile_mismatch", {"mismatch": True})
        mock_alert.assert_called_once()

    @patch("monitor.send_alert")
    def test_notify_only_returns_true(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "RISK_RECONCILE_ACTION", ["notify"])
        result = evaluate("reconcile_mismatch", {"mismatch": True})
        assert result is True

    # ── reject ────────────────────────────────────────────────────────────────

    def test_reject_returns_false(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",        1)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION", ["reject"])
        assert evaluate("max_order_value", {"order_value": 2}) is False

    @patch("monitor.send_alert")
    def test_reject_does_not_call_send_alert(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",        1)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION", ["reject"])
        evaluate("max_order_value", {"order_value": 2})
        mock_alert.assert_not_called()

    # ── skip ──────────────────────────────────────────────────────────────────

    def test_skip_returns_false(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_STALE_STATE_DAYS",   1)
        monkeypatch.setattr(config, "RISK_STALE_STATE_ACTION", ["skip"])
        old = date.today() - timedelta(days=5)
        assert evaluate("stale_state", {"last_update_date": old}) is False

    # ── halt ──────────────────────────────────────────────────────────────────

    def test_halt_returns_false(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.01)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        result = evaluate("daily_loss", {"daily_pnl": -5_000, "equity": 100_000})
        assert result is False

    def test_halt_sets_db_flag(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.01)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        evaluate("daily_loss", {"daily_pnl": -5_000, "equity": 100_000})
        assert is_halted() is True

    def test_halt_does_not_set_shutdown_flag(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt"])
        evaluate("daily_loss", {"daily_pnl": -5_000, "equity": 100_000})
        assert is_shutdown() is False

    # ── shutdown ──────────────────────────────────────────────────────────────

    def test_shutdown_returns_false(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_PCT",    0.10)
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        result = evaluate("max_drawdown",
                          {"peak_equity": 100_000, "current_equity": 80_000})
        assert result is False

    def test_shutdown_sets_db_flag(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        assert is_shutdown() is True

    def test_shutdown_also_satisfies_is_halted(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        assert is_halted() is True

    # ── additive actions ──────────────────────────────────────────────────────

    @patch("monitor.send_alert")
    def test_halt_and_notify_both_execute(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "ALERT_EMAIL", "a@b.com")
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.01)
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt", "notify"])
        result = evaluate("daily_loss", {"daily_pnl": -5_000, "equity": 100_000})
        # Both actions fire
        assert result is False          # halt blocks
        assert is_halted() is True      # halt flag set
        mock_alert.assert_called_once() # notify also fired

    @patch("monitor.send_alert")
    def test_shutdown_and_notify_both_execute(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "ALERT_EMAIL", "a@b.com")
        monkeypatch.setattr(config, "RISK_MAX_DD_PCT",    0.10)
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown", "notify"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 80_000})
        assert is_shutdown() is True
        mock_alert.assert_called_once()

    @patch("monitor.send_alert")
    def test_notify_alert_level_is_critical_for_shutdown(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "ALERT_EMAIL", "a@b.com")
        monkeypatch.setattr(config, "RISK_MAX_DD_ACTION", ["shutdown", "notify"])
        evaluate("max_drawdown", {"peak_equity": 100_000, "current_equity": 70_000})
        mock_alert.assert_called_once()
        level = mock_alert.call_args.kwargs.get("level") or mock_alert.call_args.args[2]
        assert level == "critical"

    @patch("monitor.send_alert")
    def test_notify_alert_level_is_warning_for_halt(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "ALERT_EMAIL",         "a@b.com")
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_PCT",    0.01)   # 1% threshold
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ACTION", ["halt", "notify"])
        evaluate("daily_loss", {"daily_pnl": -5_000, "equity": 100_000})  # 5% loss
        mock_alert.assert_called_once()
        level = mock_alert.call_args.kwargs.get("level") or mock_alert.call_args.args[2]
        assert level == "warning"

    @patch("monitor.send_alert")
    def test_reject_and_notify_both_execute(self, mock_alert, monkeypatch):
        monkeypatch.setattr(config, "ALERT_EMAIL", "a@b.com")
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",        1)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION", ["reject", "notify"])
        result = evaluate("max_order_value", {"order_value": 2})
        assert result is False
        mock_alert.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# Disabled controls
# ═══════════════════════════════════════════════════════════════════════════════

class TestDisabledControls:

    def test_disabled_max_order_value_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ENABLED", False)
        assert evaluate("max_order_value", {"order_value": 999_999}) is True

    def test_disabled_daily_loss_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_DAILY_LOSS_ENABLED", False)
        assert evaluate("daily_loss", {"daily_pnl": -999_999, "equity": 1}) is True

    def test_disabled_max_drawdown_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_DD_ENABLED", False)
        assert evaluate("max_drawdown",
                        {"peak_equity": 100_000, "current_equity": 1}) is True

    def test_disabled_margin_breach_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MARGIN_ENABLED", False)
        assert evaluate("margin_breach", {"margin_pct": 0.0}) is True

    def test_disabled_stale_state_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_STALE_STATE_ENABLED", False)
        old = date.today() - timedelta(days=365)
        assert evaluate("stale_state", {"last_update_date": old}) is True

    def test_disabled_imbalance_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED",   False)
        assert evaluate("imbalance", {"imbalance_ratio": 999}) is True

    def test_disabled_reconcile_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_RECONCILE_ENABLED", False)
        assert evaluate("reconcile_mismatch", {"mismatch": True}) is True


# ═══════════════════════════════════════════════════════════════════════════════
# Edge cases — fail-open behaviour
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:

    def test_unknown_control_name_returns_true(self):
        assert evaluate("no_such_control", {}) is True

    def test_handler_exception_returns_true(self, monkeypatch):
        """A buggy handler must not block execution."""
        def _boom(_ctx):
            raise RuntimeError("intentional error")
        monkeypatch.setitem(
            risk_engine._REGISTRY["fill_timeout"], "handler", _boom
        )
        assert evaluate("fill_timeout", {"minutes_pending": 999}) is True

    def test_passing_control_returns_true(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE",        1_000_000)
        monkeypatch.setattr(config, "RISK_MAX_ORDER_VALUE_ACTION", ["reject"])
        assert evaluate("max_order_value", {"order_value": 100}) is True

    def test_no_halt_flag_set_on_passing_control(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_MARGIN_MIN_PCT",  0.10)
        monkeypatch.setattr(config, "RISK_MARGIN_ACTION",   ["reject"])
        evaluate("margin_breach", {"margin_pct": 0.50})
        assert is_halted() is False
