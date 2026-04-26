"""
tests/test_main.py — Unit tests for main.py

All IB calls, DB reads, and external module calls are mocked.
Tests focus on the job-level logic: connectivity_check flow and the
order_submission loop's handling of OrderRejectedError.
"""

from unittest.mock import MagicMock, call, patch

import pytest

import config
import db
import main
import monitor
import portfolio_state
import risk_engine
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

    def _build_snap_state(self, entry_signals=None, exit_signals=None,
                          snap_prices=None, open_positions=None):
        return {
            "entry_signals":  entry_signals  or [],
            "exit_signals":   exit_signals   or [],
            "snap_prices":    snap_prices    or {},
            "open_positions": open_positions or [],
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

    def test_build_entry_orders_receives_exit_orders(self, monkeypatch):
        """
        build_entry_orders must be called with exit_orders= so the pending-exit
        notional credit is applied to the total-notional gate.
        """
        exit_order = Order(
            symbol="HELD", action="SELL", order_type="MOC",
            quantity=50, limit_price=None, reason="exit", pos_id="P1",
        )
        import order_manager
        monkeypatch.setattr(order_manager, "build_exit_orders", MagicMock(return_value=[exit_order]))
        monkeypatch.setattr(main, "submit_order", MagicMock(return_value=99))

        main._snap_state = self._build_snap_state()
        main.order_submission()

        self.mock_build_entry.assert_called_once()
        _, kwargs = self.mock_build_entry.call_args
        assert "exit_orders" in kwargs
        assert kwargs["exit_orders"] == [exit_order]

    def test_positions_post_exit_excludes_exiting_symbols(self, monkeypatch):
        """
        With 13 open positions and 4 exit orders, build_entry_orders must receive
        a positions list of length 9, not 13 — so both the slot counter and the
        notional gate use the post-exit portfolio view.

        MAX_POSITIONS=15 and 9 positions remaining → 6 free slots, meaning up to
        6 entry orders can be built (subject to other gates).
        """
        # 13 open positions: P0..P12
        open_positions = [
            {"symbol": f"SYM{i}", "shares": 100, "fill_price": 50.0}
            for i in range(13)
        ]
        # 4 of them are exiting: SYM0..SYM3
        exit_orders_list = [
            Order(
                symbol=f"SYM{i}", action="SELL", order_type="MOC",
                quantity=100, limit_price=None, reason="exit", pos_id=f"P{i}",
            )
            for i in range(4)
        ]

        import order_manager, config
        monkeypatch.setattr(order_manager, "build_exit_orders",
                            MagicMock(return_value=exit_orders_list))
        monkeypatch.setattr(main, "submit_order", MagicMock(return_value=99))
        monkeypatch.setattr(config, "MAX_POSITIONS", 15)

        captured_positions: list[list] = []
        def _capture_positions(signals, positions, equity, prices, **kw):
            captured_positions.append(positions)
            return []
        monkeypatch.setattr(order_manager, "build_entry_orders", _capture_positions)

        main._snap_state = self._build_snap_state(
            open_positions=open_positions,
            snap_prices={f"SYM{i}": 50.0 for i in range(13)},
        )
        main.order_submission()

        assert len(captured_positions) == 1
        positions_received = captured_positions[0]
        # 13 open - 4 exiting = 9 remaining
        assert len(positions_received) == 9
        received_syms = {p["symbol"] for p in positions_received}
        for i in range(4):
            assert f"SYM{i}" not in received_syms, f"SYM{i} should be excluded (has exit order)"
        # 15 - 9 = 6 free slots — verify none of the exiting symbols leaked through
        for i in range(4, 13):
            assert f"SYM{i}" in received_syms


# ═══════════════════════════════════════════════════════════════════════════════
# TestDailyPnlRiskCheck
# ═══════════════════════════════════════════════════════════════════════════════

class TestDailyPnlRiskCheck:
    """
    Verify that order_submission() passes the correct unrealised intraday P&L
    to risk_engine.evaluate('daily_loss', ...).
    """

    @pytest.fixture(autouse=True)
    def patch_all(self, monkeypatch):
        monkeypatch.setattr(main, "bridge", MagicMock())
        monkeypatch.setattr(monitor, "send_alert", MagicMock())

        import risk_engine
        monkeypatch.setattr(risk_engine, "is_shutdown", lambda: False)
        monkeypatch.setattr(risk_engine, "is_halted",   lambda: False)

        # Capture evaluate calls so we can inspect daily_loss context
        self.evaluate_calls: list[tuple] = []
        def _evaluate(name, ctx):
            self.evaluate_calls.append((name, ctx))
            return True
        monkeypatch.setattr(risk_engine, "evaluate", _evaluate)

        import order_manager
        monkeypatch.setattr(order_manager, "build_exit_orders",  lambda *a, **kw: [])
        monkeypatch.setattr(order_manager, "build_entry_orders", lambda *a, **kw: [])

        import db
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__  = MagicMock(return_value=False)
        mock_conn.execute   = MagicMock(return_value=MagicMock(fetchone=MagicMock(return_value=None)))
        monkeypatch.setattr(db, "connect", lambda: mock_conn)

    def _daily_loss_ctx(self):
        """Return the context dict passed to the 'daily_loss' evaluate call."""
        for name, ctx in self.evaluate_calls:
            if name == "daily_loss":
                return ctx
        return None

    def test_non_zero_pnl_when_price_above_fill(self):
        """Snap price > fill_price → positive daily_pnl passed to risk_engine."""
        main._snap_state = {
            "entry_signals":  [],
            "exit_signals":   [],
            "snap_prices":    {"AAPL": 155.0},
            "open_positions": [{"symbol": "AAPL", "fill_price": 150.0, "shares": 100}],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }
        main.order_submission()
        ctx = self._daily_loss_ctx()
        assert ctx is not None
        assert ctx["daily_pnl"] == pytest.approx(500.0)   # (155 - 150) × 100

    def test_non_zero_pnl_when_price_below_fill(self):
        """Snap price < fill_price → negative daily_pnl (loss) passed to risk_engine."""
        main._snap_state = {
            "entry_signals":  [],
            "exit_signals":   [],
            "snap_prices":    {"AAPL": 145.0},
            "open_positions": [{"symbol": "AAPL", "fill_price": 150.0, "shares": 100}],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }
        main.order_submission()
        ctx = self._daily_loss_ctx()
        assert ctx["daily_pnl"] == pytest.approx(-500.0)  # (145 - 150) × 100

    def test_zero_pnl_when_no_open_positions(self):
        """No open positions → daily_pnl == 0."""
        main._snap_state = {
            "entry_signals":  [],
            "exit_signals":   [],
            "snap_prices":    {"AAPL": 155.0},
            "open_positions": [],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }
        main.order_submission()
        ctx = self._daily_loss_ctx()
        assert ctx["daily_pnl"] == pytest.approx(0.0)

    def test_pnl_sums_across_multiple_positions(self):
        """daily_pnl aggregates all open positions correctly."""
        main._snap_state = {
            "entry_signals":  [],
            "exit_signals":   [],
            "snap_prices":    {"AAPL": 155.0, "MSFT": 290.0},
            "open_positions": [
                {"symbol": "AAPL", "fill_price": 150.0, "shares": 100},  # +500
                {"symbol": "MSFT", "fill_price": 300.0, "shares":  50},  # -500
            ],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }
        main.order_submission()
        ctx = self._daily_loss_ctx()
        assert ctx["daily_pnl"] == pytest.approx(0.0)   # +500 - 500 = 0

    def test_fallback_to_fill_price_when_no_snap(self):
        """If a position has no snap price, fill_price is used → P&L contribution is 0."""
        main._snap_state = {
            "entry_signals":  [],
            "exit_signals":   [],
            "snap_prices":    {},           # no snapshot data
            "open_positions": [{"symbol": "AAPL", "fill_price": 150.0, "shares": 100}],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }
        main.order_submission()
        ctx = self._daily_loss_ctx()
        assert ctx["daily_pnl"] == pytest.approx(0.0)

    def test_equity_also_passed_to_daily_loss(self):
        """net_liquidation is forwarded as 'equity' alongside daily_pnl."""
        main._snap_state = {
            "entry_signals":  [],
            "exit_signals":   [],
            "snap_prices":    {},
            "open_positions": [],
            "account":        {"net_liquidation": 123_456.0},
            "snap_date":      "2024-01-02",
        }
        main.order_submission()
        ctx = self._daily_loss_ctx()
        assert ctx["equity"] == pytest.approx(123_456.0)


# ═══════════════════════════════════════════════════════════════════════════════
# TestFillReconciliationEntryColumns
# ═══════════════════════════════════════════════════════════════════════════════

class TestFillReconciliationEntryColumns:
    """
    Verify fill_reconciliation() persists order_type, limit_price,
    qpi_at_entry, ibs_at_entry for new BUY positions.
    Uses a real temp SQLite DB; IB-facing calls are mocked.
    """

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    def _run(self, monkeypatch):
        monkeypatch.setattr(main, "bridge",            MagicMock())
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value={
            101: {"symbol": "AAPL", "fill_price": 150.0, "fill_qty": 100, "status": "Filled"},
        }))
        # Return AAPL in IB positions so reconciliation finds no mismatch
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[{"symbol": "AAPL"}]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", MagicMock())
        monkeypatch.setattr(risk_engine, "evaluate",   lambda *a, **kw: True)
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)

        main._submitted = {
            101: {
                "symbol":      "AAPL",
                "action":      "BUY",
                "pos_id":      "",
                "shares":      100,
                "fill_price":  150.0,
                "reason":      "entry",
                "order_type":  "LOC",
                "limit_price": 155.5,
            }
        }
        main._snap_state = {
            "entry_signals": [
                {"symbol": "AAPL", "n_day_ret": 0.08, "ibs_entry": 0.14},
            ],
            "snap_prices":    {"AAPL": 150.0},
            "open_positions": [],
            "account":        {"net_liquidation": 100_000.0},
            "snap_date":      "2024-01-02",
        }
        main.fill_reconciliation()

    def _row(self):
        with db.connect() as conn:
            return dict(conn.execute(
                "SELECT order_type, limit_price, qpi_at_entry, ibs_at_entry "
                "FROM positions WHERE symbol = 'AAPL'"
            ).fetchone())

    def test_order_type_persisted(self, monkeypatch):
        self._run(monkeypatch)
        assert self._row()["order_type"] == "LOC"

    def test_limit_price_persisted(self, monkeypatch):
        self._run(monkeypatch)
        assert self._row()["limit_price"] == pytest.approx(155.5)

    def test_qpi_at_entry_persisted(self, monkeypatch):
        self._run(monkeypatch)
        assert self._row()["qpi_at_entry"] == pytest.approx(0.08)

    def test_ibs_at_entry_persisted(self, monkeypatch):
        self._run(monkeypatch)
        assert self._row()["ibs_at_entry"] == pytest.approx(0.14)

    def test_missing_signal_leaves_qpi_null(self, monkeypatch):
        """If no entry_signal matches, qpi_at_entry / ibs_at_entry are NULL."""
        monkeypatch.setattr(main, "bridge",            MagicMock())
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value={
            101: {"symbol": "AAPL", "fill_price": 150.0, "fill_qty": 100, "status": "Filled"},
        }))
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[{"symbol": "AAPL"}]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", MagicMock())
        monkeypatch.setattr(risk_engine, "evaluate",   lambda *a, **kw: True)
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)

        main._submitted = {
            101: {
                "symbol": "AAPL", "action": "BUY", "pos_id": "",
                "shares": 100, "fill_price": 150.0, "reason": "entry",
                "order_type": "LOC", "limit_price": None,
            }
        }
        main._snap_state = {
            "entry_signals": [],   # no signal for AAPL
            "snap_prices": {}, "open_positions": [],
            "account": {"net_liquidation": 100_000.0}, "snap_date": "2024-01-02",
        }
        main.fill_reconciliation()
        row = self._row()
        assert row["qpi_at_entry"] is None
        assert row["ibs_at_entry"] is None


# ═══════════════════════════════════════════════════════════════════════════════
# TestDailyReportEntryMapping
# ═══════════════════════════════════════════════════════════════════════════════

class TestDailyReportEntryMapping:
    """
    Verify daily_report() maps DB columns to the keys expected by
    build_daily_report(): order_type, limit_price, qpi, ibs.
    Uses a real temp SQLite DB with hand-crafted position rows.
    """

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    def _insert_position(self, today_str: str, **kwargs):
        defaults = dict(
            pos_id="TEST_1", symbol="AAPL", direction="long",
            entry_date=today_str, fill_price=150.0, shares=100,
            notional=15_000.0, bars_held=0,
            order_type="LOC", limit_price=155.5,
            qpi_at_entry=0.08, ibs_at_entry=0.14,
        )
        defaults.update(kwargs)
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO positions "
                "(pos_id, symbol, direction, entry_date, fill_price, shares, notional, "
                "bars_held, order_type, limit_price, qpi_at_entry, ibs_at_entry) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    defaults["pos_id"], defaults["symbol"], defaults["direction"],
                    defaults["entry_date"], defaults["fill_price"], defaults["shares"],
                    defaults["notional"], defaults["bars_held"],
                    defaults["order_type"], defaults["limit_price"],
                    defaults["qpi_at_entry"], defaults["ibs_at_entry"],
                ),
            )

    @pytest.fixture(autouse=True)
    def patch_externals(self, monkeypatch):
        monkeypatch.setattr(monitor, "send_report",         MagicMock())
        monkeypatch.setattr(monitor, "build_daily_report",  MagicMock(return_value=""))
        monkeypatch.setattr(portfolio_state, "load_positions", MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "get_open_equity", MagicMock(return_value=0.0))
        monkeypatch.setattr(config, "REPORT_DAILY", True)
        monkeypatch.setattr(config, "REPORT_WEEKLY", False)
        main._snap_state = {"account": {}, "snap_prices": {}}

    def _captured_entries(self):
        call_args = monitor.build_daily_report.call_args
        return call_args[0][0]["entries"]

    def test_order_type_mapped(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, order_type="LOC")
        main.daily_report()
        entries = self._captured_entries()
        assert len(entries) == 1
        assert entries[0]["order_type"] == "LOC"

    def test_limit_price_mapped(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, limit_price=155.5)
        main.daily_report()
        assert self._captured_entries()[0]["limit_price"] == pytest.approx(155.5)

    def test_qpi_mapped_from_qpi_at_entry(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, qpi_at_entry=0.08)
        main.daily_report()
        assert self._captured_entries()[0]["qpi"] == pytest.approx(0.08)

    def test_ibs_mapped_from_ibs_at_entry(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, ibs_at_entry=0.14)
        main.daily_report()
        assert self._captured_entries()[0]["ibs"] == pytest.approx(0.14)

    def test_null_order_type_falls_back_to_config(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, order_type=None)
        main.daily_report()
        assert self._captured_entries()[0]["order_type"] == config.ENTRY_ORDER_TYPE

    def test_null_qpi_defaults_to_zero(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, qpi_at_entry=None)
        main.daily_report()
        assert self._captured_entries()[0]["qpi"] == pytest.approx(0.0)

    def test_null_ibs_defaults_to_zero(self, monkeypatch):
        from datetime import date
        today = str(date.today())
        self._insert_position(today, ibs_at_entry=None)
        main.daily_report()
        assert self._captured_entries()[0]["ibs"] == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# TestReconcileWithIb
# ═══════════════════════════════════════════════════════════════════════════════

class TestReconcileWithIb:
    """
    Verify _reconcile_with_ib() compares DB positions against the full IB
    position list (not today's filled orders).
    """

    def test_no_mismatch_when_symbols_match(self):
        positions   = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
        ib_positions = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
        mismatch, detail = main._reconcile_with_ib(positions, ib_positions)
        assert mismatch is False
        assert detail == ""

    def test_detects_extra_in_ib(self):
        positions    = [{"symbol": "AAPL"}]
        ib_positions = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
        mismatch, detail = main._reconcile_with_ib(positions, ib_positions)
        assert mismatch is True
        assert "MSFT" in detail

    def test_detects_extra_in_db(self):
        positions    = [{"symbol": "AAPL"}, {"symbol": "GOOG"}]
        ib_positions = [{"symbol": "AAPL"}]
        mismatch, detail = main._reconcile_with_ib(positions, ib_positions)
        assert mismatch is True
        assert "GOOG" in detail

    def test_both_empty_no_mismatch(self):
        mismatch, _ = main._reconcile_with_ib([], [])
        assert mismatch is False

    def test_db_empty_ib_has_positions(self):
        """IB holds positions the DB doesn't know about → mismatch."""
        mismatch, detail = main._reconcile_with_ib([], [{"symbol": "AAPL"}])
        assert mismatch is True
        assert "AAPL" in detail

    def test_ib_empty_db_has_positions(self):
        """DB has open positions IB no longer holds → mismatch."""
        mismatch, detail = main._reconcile_with_ib([{"symbol": "AAPL"}], [])
        assert mismatch is True
        assert "AAPL" in detail

    def test_exclude_symbols_prevents_false_positive(self):
        """Symbols in exclude_symbols are dropped from both sides before comparison."""
        positions    = []                          # DB: no open positions
        ib_positions = [{"symbol": "AAPL"}]       # IB: AAPL still showing
        # Without exclude: mismatch
        mismatch, _ = main._reconcile_with_ib(positions, ib_positions)
        assert mismatch is True
        # With exclude: no mismatch
        mismatch, _ = main._reconcile_with_ib(
            positions, ib_positions, exclude_symbols={"AAPL"}
        )
        assert mismatch is False

    def test_exclude_does_not_hide_other_mismatches(self):
        """Excluding AAPL must not hide an unrelated MSFT discrepancy."""
        positions    = []
        ib_positions = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
        mismatch, detail = main._reconcile_with_ib(
            positions, ib_positions, exclude_symbols={"AAPL"}
        )
        assert mismatch is True
        assert "MSFT" in detail
        assert "AAPL" not in detail

    def test_exclude_symbols_defaults_to_empty(self):
        """Omitting exclude_symbols behaves the same as passing an empty set."""
        positions    = [{"symbol": "AAPL"}]
        ib_positions = [{"symbol": "AAPL"}]
        mismatch, _ = main._reconcile_with_ib(positions, ib_positions)
        assert mismatch is False

    def test_fill_reconciliation_excludes_exited_today_from_reconciliation(
        self, monkeypatch, tmp_path
    ):
        """
        fill_reconciliation() must not flag a same-day exit as a reconciliation
        mismatch: after close_position() the symbol is gone from DB, but IB may
        still report it until settlement.  Passing exited_today to _reconcile_with_ib
        must suppress the false positive.
        """
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

        monkeypatch.setattr(main, "bridge", MagicMock())
        # AAPL was sold today → appears in filled as SELL
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value={
            201: {"symbol": "AAPL", "fill_price": 155.0, "fill_qty": 100, "status": "Filled"},
        }))
        # IB still shows AAPL (not yet settled)
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[{"symbol": "AAPL"}]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", MagicMock())
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)

        evaluate_calls: list[tuple] = []
        monkeypatch.setattr(risk_engine, "evaluate", lambda n, c: evaluate_calls.append((n, c)) or True)

        # Pre-insert an open AAPL position so close_position() can find it
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO positions (pos_id, symbol, direction, entry_date, "
                "fill_price, shares, notional, bars_held) VALUES (?,?,?,?,?,?,?,?)",
                ("AAPL_2026-01-02", "AAPL", "long", "2026-01-02", 150.0, 100, 15000.0, 5),
            )

        main._submitted = {
            201: {
                "symbol": "AAPL", "action": "SELL",
                "pos_id": "AAPL_2026-01-02", "shares": 100,
                "fill_price": 155.0, "reason": "ibs_exit",
            }
        }
        main._snap_state = {
            "entry_signals": [], "snap_prices": {"AAPL": 155.0},
            "open_positions": [], "account": {"net_liquidation": 100_000.0},
        }
        main.fill_reconciliation()

        reconcile_calls = [c for c in evaluate_calls if c[0] == "reconcile_mismatch"]
        assert len(reconcile_calls) == 0, (
            f"Expected no mismatch for same-day exit, got: {reconcile_calls}"
        )

    def test_fill_reconciliation_passes_ib_pos_to_reconciler(self, monkeypatch, tmp_path):
        """
        fill_reconciliation() must use the live IB position list for reconciliation,
        not today's filled orders.  A pre-existing IB position absent from DB
        triggers a mismatch even with no today's fills.
        """
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

        monkeypatch.setattr(main, "bridge",            MagicMock())
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value={}))
        # IB reports MSFT but DB has no open positions
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[{"symbol": "MSFT"}]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", MagicMock())
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)

        evaluate_calls: list[tuple] = []
        def _evaluate(name, ctx):
            evaluate_calls.append((name, ctx))
            return True
        monkeypatch.setattr(risk_engine, "evaluate", _evaluate)

        main._submitted  = {}
        main._snap_state = {
            "entry_signals": [], "snap_prices": {},
            "open_positions": [], "account": {"net_liquidation": 0},
        }
        main.fill_reconciliation()

        reconcile_calls = [c for c in evaluate_calls if c[0] == "reconcile_mismatch"]
        assert len(reconcile_calls) == 1
        assert reconcile_calls[0][1]["mismatch"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# TestBarsHeldPersistence
# ═══════════════════════════════════════════════════════════════════════════════

class TestBarsHeldPersistence:
    """
    Verify that fill_reconciliation() persists updated bars_held / consec_lows
    for positions that were NOT exited today.
    """

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    def _run(self, monkeypatch, snap_open, submitted=None, filled=None):
        monkeypatch.setattr(main, "bridge",            MagicMock())
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value=filled or {}))
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", MagicMock())
        monkeypatch.setattr(risk_engine, "evaluate",   lambda *a, **kw: True)
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)

        main._submitted  = submitted or {}
        main._snap_state = {
            "entry_signals": [], "snap_prices": {},
            "open_positions": snap_open,
            "account": {"net_liquidation": 100_000.0},
        }
        main.fill_reconciliation()

    def test_save_position_called_for_non_exited(self, monkeypatch, tmp_path):
        """bars_held update in snap_state must be written back to DB."""
        # Insert a position in DB
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO positions (pos_id, symbol, direction, entry_date, "
                "fill_price, shares, notional, bars_held, consec_lows) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                ("AAPL_2026-01-02", "AAPL", "long", "2026-01-02",
                 150.0, 100, 15000.0, 2, 0),
            )

        # snap_state has the in-memory version with bars_held incremented by get_exit_signals
        snap_open = [{
            "pos_id": "AAPL_2026-01-02", "symbol": "AAPL", "direction": "long",
            "entry_date": "2026-01-02", "fill_price": 150.0, "shares": 100,
            "notional": 15000.0, "bars_held": 3, "consec_lows": 0,
        }]
        self._run(monkeypatch, snap_open)

        with db.connect() as conn:
            row = conn.execute(
                "SELECT bars_held FROM positions WHERE pos_id = 'AAPL_2026-01-02'"
            ).fetchone()
        assert row is not None
        assert row[0] == 3   # updated from 2 → 3

    def test_exited_position_not_saved_back(self, monkeypatch, tmp_path):
        """A position that was just closed must NOT be re-saved by the bars_held loop."""
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO positions (pos_id, symbol, direction, entry_date, "
                "fill_price, shares, notional, bars_held, consec_lows) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                ("AAPL_2026-01-02", "AAPL", "long", "2026-01-02",
                 150.0, 100, 15000.0, 5, 0),
            )

        snap_open = [{
            "pos_id": "AAPL_2026-01-02", "symbol": "AAPL", "direction": "long",
            "entry_date": "2026-01-02", "fill_price": 150.0, "shares": 100,
            "notional": 15000.0, "bars_held": 6, "consec_lows": 0,
        }]
        submitted = {
            301: {
                "symbol": "AAPL", "action": "SELL",
                "pos_id": "AAPL_2026-01-02", "shares": 100,
                "fill_price": 155.0, "reason": "ibs_exit",
            }
        }
        filled = {
            301: {"symbol": "AAPL", "fill_price": 155.0, "fill_qty": 100, "status": "Filled"},
        }
        self._run(monkeypatch, snap_open, submitted=submitted, filled=filled)

        # Position was closed → row should be gone from positions table
        with db.connect() as conn:
            row = conn.execute(
                "SELECT bars_held FROM positions WHERE pos_id = 'AAPL_2026-01-02'"
            ).fetchone()
        assert row is None   # confirmed closed, not re-inserted


# ═══════════════════════════════════════════════════════════════════════════════
# TestDeployedPctFix
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeployedPctFix:
    """
    Verify fill_reconciliation() uses actual fill prices (not snap prices) when
    computing deployed_pct for newly entered positions.
    """

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    def test_deployed_pct_uses_fill_price_not_snap_price(self, monkeypatch):
        """
        snap_price = 150, fill_price = 160.  deployed_pct should reflect
        the fill price (160 × 100 shares = $16 000) not the snap price
        ($15 000), so the value passed to append_equity_snapshot is higher.
        """
        snap_calls: list[tuple] = []

        def _capture_snapshot(*args, **kwargs):
            snap_calls.append(args)

        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", _capture_snapshot)
        monkeypatch.setattr(main, "bridge",            MagicMock())
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value={
            401: {"symbol": "AAPL", "fill_price": 160.0, "fill_qty": 100, "status": "Filled"},
        }))
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[{"symbol": "AAPL"}]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))
        monkeypatch.setattr(risk_engine, "evaluate",   lambda *a, **kw: True)
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)

        main._submitted = {
            401: {
                "symbol": "AAPL", "action": "BUY", "pos_id": "",
                "shares": 100, "fill_price": 150.0, "reason": "entry",
                "order_type": "LOC", "limit_price": 155.0,
            }
        }
        main._snap_state = {
            "entry_signals": [{"symbol": "AAPL", "n_day_ret": 0.08, "ibs_entry": 0.14}],
            "snap_prices": {"AAPL": 150.0},   # snap price is 150, fill is 160
            "open_positions": [],
            "account": {"net_liquidation": 100_000.0},
        }
        main.fill_reconciliation()

        assert len(snap_calls) == 1
        _date, _bod, _eod, _n_pos, deployed_pct = snap_calls[0]
        # 160 × 100 = 16 000 / 100 000 = 0.16 (not 0.15 from snap price)
        assert deployed_pct == pytest.approx(0.16)


# ═══════════════════════════════════════════════════════════════════════════════
# TestDailyReportDeployedPct
# ═══════════════════════════════════════════════════════════════════════════════

class TestDailyReportDeployedPct:
    """
    Verify daily_report() computes deployed_pct from live snap_prices and
    portfolio_state.get_open_equity(), not from any stale equity_log value.
    """

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    def test_deployed_pct_uses_snap_prices_not_equity_log(self, monkeypatch):
        """
        Two open positions with fill_price=100 each (100 shares each).
        snap_prices carry current prices of 120 each → open equity = 24 000.
        NLV from snap_state = 100 000.
        Expected deployed_pct in report_data = 24 000 / 100 000 = 0.24.

        The equity_log row carries a stale deployed figure (not present in this
        test) — if daily_report() were reading from equity_log it would either
        find 0.0 or a hardcoded value, neither of which is 0.24.
        """
        captured: list[dict] = []

        def _capture(data: dict) -> str:
            captured.append(data)
            return ""

        monkeypatch.setattr(monitor, "send_report",        MagicMock())
        monkeypatch.setattr(monitor, "build_daily_report", _capture)
        monkeypatch.setattr(config, "REPORT_DAILY",  True)
        monkeypatch.setattr(config, "REPORT_WEEKLY", False)

        # Insert two open positions at fill_price=100, 100 shares each
        with db.connect() as conn:
            for i, sym in enumerate(("AAPL", "MSFT"), start=1):
                conn.execute(
                    "INSERT INTO positions "
                    "(pos_id, symbol, direction, entry_date, fill_price, shares, notional, "
                    "bars_held, order_type, limit_price, qpi_at_entry, ibs_at_entry) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"POS_{i}", sym, "long", "2026-04-01",
                        100.0, 100, 10_000.0, 5,
                        "LOC", 99.0, 0.05, 0.10,
                    ),
                )

        # snap_prices carry current prices of 120 for both symbols
        main._snap_state = {
            "account":      {"net_liquidation": 100_000.0, "accrued_interest": 0.0},
            "snap_prices":  {"AAPL": 120.0, "MSFT": 120.0},
            "open_positions": [],
        }

        main.daily_report()

        assert len(captured) == 1
        deployed = captured[0]["deployed_pct"]
        # open_equity = 120 × 100 + 120 × 100 = 24 000; NLV = 100 000
        assert deployed == pytest.approx(0.24)


# ═══════════════════════════════════════════════════════════════════════════════
# TestWeeklyReport
# ═══════════════════════════════════════════════════════════════════════════════

class TestWeeklyReport:
    """
    Verify daily_report() calls build_weekly_report() (not build_daily_report())
    on the configured weekly report day, and passes a week_data dict.
    """

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    @pytest.fixture(autouse=True)
    def patch_externals(self, monkeypatch):
        self.mock_send_report      = MagicMock()
        self.mock_build_daily      = MagicMock(return_value="daily text")
        self.mock_build_weekly     = MagicMock(return_value="weekly text")
        monkeypatch.setattr(monitor, "send_report",         self.mock_send_report)
        monkeypatch.setattr(monitor, "build_daily_report",  self.mock_build_daily)
        monkeypatch.setattr(monitor, "build_weekly_report", self.mock_build_weekly)
        monkeypatch.setattr(portfolio_state, "load_positions",   MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "get_open_equity",  MagicMock(return_value=0.0))
        monkeypatch.setattr(config, "REPORT_DAILY",  True)
        monkeypatch.setattr(config, "REPORT_WEEKLY", True)
        main._snap_state = {"account": {}, "snap_prices": {}}

    def _run_on_friday(self, monkeypatch):
        from datetime import date
        # Find a Friday
        friday = date(2026, 4, 10)   # known Friday
        with patch("main.date") as mock_date:
            mock_date.today.return_value = friday
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            monkeypatch.setattr(config, "REPORT_WEEKLY_DAY", "friday")
            main.daily_report()

    def test_weekly_day_calls_build_weekly_report(self, monkeypatch):
        self._run_on_friday(monkeypatch)
        self.mock_build_weekly.assert_called_once()

    def test_weekly_day_calls_send_report_with_is_weekly_true(self, monkeypatch):
        self._run_on_friday(monkeypatch)
        weekly_calls = [
            c for c in self.mock_send_report.call_args_list
            if c.kwargs.get("is_weekly") is True or (len(c.args) > 1 and c.args[1] is True)
        ]
        assert len(weekly_calls) == 1

    def test_weekly_report_receives_week_start_and_end(self, monkeypatch):
        self._run_on_friday(monkeypatch)
        week_data = self.mock_build_weekly.call_args[0][0]
        assert "week_start" in week_data
        assert "week_end"   in week_data

    def test_non_weekly_day_does_not_call_build_weekly_report(self, monkeypatch):
        from datetime import date
        thursday = date(2026, 4, 9)  # Thursday
        with patch("main.date") as mock_date:
            mock_date.today.return_value = thursday
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            monkeypatch.setattr(config, "REPORT_WEEKLY_DAY", "friday")
            main.daily_report()
        self.mock_build_weekly.assert_not_called()

    def test_weekly_report_disabled_config_skips_weekly(self, monkeypatch):
        monkeypatch.setattr(config, "REPORT_WEEKLY", False)
        self._run_on_friday(monkeypatch)
        self.mock_build_weekly.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# TestFillReconciliationSubmittedClear
# ═══════════════════════════════════════════════════════════════════════════════

class TestFillReconciliationSubmittedClear:
    """fill_reconciliation() clears _submitted on completion."""

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        db.init_db()

    @pytest.fixture(autouse=True)
    def patch_ib(self, monkeypatch):
        monkeypatch.setattr(main, "bridge",            MagicMock())
        monkeypatch.setattr(main, "get_filled_orders", MagicMock(return_value={}))
        monkeypatch.setattr(main, "get_ib_positions",  MagicMock(return_value=[]))
        monkeypatch.setattr(main, "detect_splits",     MagicMock(return_value=[]))

    @pytest.fixture(autouse=True)
    def patch_portfolio(self, monkeypatch):
        self.mock_save  = MagicMock()
        self.mock_close = MagicMock()
        monkeypatch.setattr(portfolio_state, "save_position",          self.mock_save)
        monkeypatch.setattr(portfolio_state, "close_position",         self.mock_close)
        monkeypatch.setattr(portfolio_state, "load_positions",         MagicMock(return_value=[]))
        monkeypatch.setattr(portfolio_state, "get_open_equity",        MagicMock(return_value=0.0))
        monkeypatch.setattr(portfolio_state, "append_equity_snapshot", MagicMock())

    @pytest.fixture(autouse=True)
    def patch_misc(self, monkeypatch):
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)
        monkeypatch.setattr(risk_engine, "evaluate", lambda *a, **kw: True)

    @pytest.fixture(autouse=True)
    def reset_state(self):
        main._snap_state = {
            "entry_signals": [], "snap_prices": {},
            "open_positions": [], "account": {"net_liquidation": 100_000.0},
        }
        yield
        main._submitted  = {}
        main._snap_state = {}

    def test_submitted_cleared_after_fill_reconciliation(self):
        """fill_reconciliation() clears _submitted dict on completion."""
        main._submitted = {
            101: {"symbol": "AAPL", "action": "BUY", "pos_id": "", "shares": 10,
                  "fill_price": 150.0, "reason": "entry", "order_type": "MOC", "limit_price": None}
        }
        assert main._submitted  # non-empty before
        main.fill_reconciliation()
        assert main._submitted == {}

    def test_second_call_processes_no_fills(self):
        """After _submitted is cleared, a second call processes no fills."""
        main._submitted = {
            101: {"symbol": "AAPL", "action": "BUY", "pos_id": "", "shares": 10,
                  "fill_price": 150.0, "reason": "entry", "order_type": "MOC", "limit_price": None}
        }
        main.fill_reconciliation()
        assert main._submitted == {}

        save_before  = self.mock_save.call_count
        close_before = self.mock_close.call_count
        main.fill_reconciliation()
        assert self.mock_save.call_count  == save_before
        assert self.mock_close.call_count == close_before
