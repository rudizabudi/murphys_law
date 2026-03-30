"""
tests/test_main.py — Unit tests for main.py

All IB calls, DB reads, and external module calls are mocked.
Tests focus on the job-level logic: connectivity_check flow and the
order_submission loop's handling of OrderRejectedError.
"""

from unittest.mock import MagicMock, call, patch

import pytest

import main
import monitor
from ib_exec import Order, OrderRejectedError


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_order(symbol="AAPL", action="BUY", order_type="LOC"):
    return Order(
        symbol=symbol, action=action, order_type=order_type,
        quantity=100, limit_price=155.0, reason="entry", pos_id="",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TestConnectivityCheck
# ═══════════════════════════════════════════════════════════════════════════════

class TestConnectivityCheck:

    @pytest.fixture(autouse=True)
    def patch_bridge(self, monkeypatch):
        """Replace main.bridge with a fresh MagicMock for every test."""
        self.mock_bridge = MagicMock()
        monkeypatch.setattr(main, "bridge", self.mock_bridge)

    @pytest.fixture(autouse=True)
    def patch_send_alert(self, monkeypatch):
        self.mock_alert = MagicMock()
        monkeypatch.setattr(monitor, "send_alert", self.mock_alert)

    @pytest.fixture(autouse=True)
    def patch_risk_engine(self, monkeypatch):
        import risk_engine
        self.mock_risk = MagicMock(return_value=True)
        monkeypatch.setattr(risk_engine, "evaluate", self.mock_risk)

    def test_healthy_heartbeat_returns_immediately(self):
        self.mock_bridge.heartbeat.return_value = True
        main.connectivity_check()
        self.mock_bridge.reconnect.assert_not_called()
        self.mock_alert.assert_not_called()

    def test_failed_heartbeat_triggers_reconnect(self):
        self.mock_bridge.heartbeat.side_effect = [False, True]
        main.connectivity_check()
        self.mock_bridge.reconnect.assert_called_once()

    def test_reconnect_success_sends_info_alert(self):
        self.mock_bridge.heartbeat.side_effect = [False, True]
        main.connectivity_check()
        self.mock_alert.assert_called_once()
        _, kwargs = self.mock_alert.call_args
        assert kwargs.get("level") == "info" or self.mock_alert.call_args[0][2] == "info"

    def test_reconnect_success_no_risk_engine_call(self):
        self.mock_bridge.heartbeat.side_effect = [False, True]
        main.connectivity_check()
        self.mock_risk.assert_not_called()

    def test_reconnect_failure_sends_critical_alert(self):
        self.mock_bridge.heartbeat.return_value = False
        main.connectivity_check()
        assert self.mock_alert.called
        # Find the critical-level call
        critical_calls = [
            c for c in self.mock_alert.call_args_list
            if "critical" in str(c)
        ]
        assert len(critical_calls) == 1

    def test_reconnect_failure_calls_risk_engine(self):
        self.mock_bridge.heartbeat.return_value = False
        main.connectivity_check()
        self.mock_risk.assert_called_once()
        args = self.mock_risk.call_args[0]
        assert args[0] == "reconcile_mismatch"

    def test_reconnect_exception_does_not_propagate(self):
        """If reconnect() raises, connectivity_check continues to second heartbeat."""
        self.mock_bridge.heartbeat.return_value = False
        self.mock_bridge.reconnect.side_effect = OSError("socket error")
        # Should not raise — exception is caught and second heartbeat runs
        main.connectivity_check()
        assert self.mock_bridge.heartbeat.call_count == 2

    def test_heartbeat_called_twice_on_failure(self):
        self.mock_bridge.heartbeat.return_value = False
        main.connectivity_check()
        assert self.mock_bridge.heartbeat.call_count == 2


# ═══════════════════════════════════════════════════════════════════════════════
# TestOrderSubmissionRejection
# ═══════════════════════════════════════════════════════════════════════════════

class TestOrderSubmissionRejection:
    """
    Verify that OrderRejectedError for one order does not abort the loop.
    Tests are scoped to the submission loop only — we short-circuit
    everything else in order_submission() via monkeypatching.
    """

    def _build_snap_state(self, entry_signals=None, exit_signals=None, snap_prices=None):
        return {
            "entry_signals":  entry_signals or [],
            "exit_signals":   exit_signals  or [],
            "snap_prices":    snap_prices   or {},
            "open_positions": [],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }

    @pytest.fixture(autouse=True)
    def patch_all(self, monkeypatch):
        self.mock_bridge = MagicMock()
        monkeypatch.setattr(main, "bridge", self.mock_bridge)

        self.mock_alert = MagicMock()
        monkeypatch.setattr(monitor, "send_alert", self.mock_alert)

        import risk_engine
        monkeypatch.setattr(risk_engine, "is_shutdown", lambda: False)
        monkeypatch.setattr(risk_engine, "is_halted",   lambda: False)
        monkeypatch.setattr(risk_engine, "evaluate",    lambda *a, **kw: True)

        import order_manager
        monkeypatch.setattr(order_manager, "build_exit_orders",  lambda *a, **kw: [])
        self.mock_build_entry = MagicMock(return_value=[])
        monkeypatch.setattr(order_manager, "build_entry_orders", self.mock_build_entry)

        # Stub DB call inside order_submission
        import db
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__  = MagicMock(return_value=False)
        mock_conn.execute   = MagicMock(return_value=MagicMock(fetchone=MagicMock(return_value=None)))
        monkeypatch.setattr(db, "connect", lambda: mock_conn)

    def test_rejection_does_not_abort_loop(self, monkeypatch):
        """A rejection on the first order must not prevent the second from being submitted."""
        orders = [_make_order("AAPL"), _make_order("MSFT")]

        import order_manager
        monkeypatch.setattr(order_manager, "build_entry_orders", MagicMock(return_value=orders))

        call_count = 0

        def _submit(bridge, order):
            nonlocal call_count
            call_count += 1
            if order.symbol == "AAPL":
                raise OrderRejectedError(100, "[201] Insufficient funds")
            return 101

        monkeypatch.setattr(main, "submit_order", _submit)
        main._snap_state = self._build_snap_state()
        main.order_submission()

        assert call_count == 2   # both attempted

    def test_rejection_sends_alert(self, monkeypatch):
        orders = [_make_order("AAPL")]
        import order_manager
        monkeypatch.setattr(order_manager, "build_entry_orders", MagicMock(return_value=orders))

        def _reject(bridge, order):
            raise OrderRejectedError(100, "[201] Rejected")

        monkeypatch.setattr(main, "submit_order", _reject)
        main._snap_state = self._build_snap_state()
        main.order_submission()

        self.mock_alert.assert_called_once()
        alert_args = self.mock_alert.call_args[0]
        assert "AAPL" in alert_args[0]

    def test_rejection_not_added_to_submitted(self, monkeypatch):
        """Rejected orders must not appear in _submitted."""
        orders = [_make_order("AAPL")]
        import order_manager
        monkeypatch.setattr(order_manager, "build_entry_orders", MagicMock(return_value=orders))

        def _reject(bridge, order):
            raise OrderRejectedError(100, "[201] Rejected")

        monkeypatch.setattr(main, "submit_order", _reject)
        main._snap_state = self._build_snap_state()
        main.order_submission()

        assert len(main._submitted) == 0

    def test_accepted_order_after_rejection_added_to_submitted(self, monkeypatch):
        orders = [_make_order("AAPL"), _make_order("MSFT")]
        import order_manager
        monkeypatch.setattr(order_manager, "build_entry_orders", MagicMock(return_value=orders))

        def _submit(bridge, order):
            if order.symbol == "AAPL":
                raise OrderRejectedError(100, "[201] Rejected")
            return 101

        monkeypatch.setattr(main, "submit_order", _submit)
        main._snap_state = self._build_snap_state()
        main.order_submission()

        assert 101 in main._submitted
        assert main._submitted[101]["symbol"] == "MSFT"

    def test_exit_rejection_continues_to_next_exit(self, monkeypatch):
        exit_orders = [
            _make_order("AAPL", action="SELL", order_type="MOC"),
            _make_order("TSLA", action="SELL", order_type="MOC"),
        ]
        import order_manager
        monkeypatch.setattr(order_manager, "build_exit_orders", MagicMock(return_value=exit_orders))

        submitted_syms = []

        def _submit(bridge, order):
            if order.symbol == "AAPL":
                raise OrderRejectedError(100, "[201] Rejected")
            submitted_syms.append(order.symbol)
            return 101

        monkeypatch.setattr(main, "submit_order", _submit)
        main._snap_state = self._build_snap_state()
        main.order_submission()

        assert "TSLA" in submitted_syms
