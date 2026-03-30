"""
tests/test_order_manager.py — Unit tests for order_manager.py

All tests use synthetic in-memory data — no DB or IB connection required.
"""

import math
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

import config
import order_manager
from ib_exec import Order
from order_manager import build_entry_orders, build_exit_orders


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_position(
    symbol="AAPL",
    pos_id="P1",
    shares=100,
    fill_price=150.0,
    bars_held=3,
) -> dict:
    return {
        "pos_id":     pos_id,
        "symbol":     symbol,
        "shares":     shares,
        "fill_price": fill_price,
        "bars_held":  bars_held,
        "direction":  "long",
    }


def _make_exit_signal(
    symbol="AAPL",
    pos_id="P1",
    shares=100,
    fill_price=150.0,
    exit_reason="ibs_exit",
) -> dict:
    pos = _make_position(symbol=symbol, pos_id=pos_id, shares=shares, fill_price=fill_price)
    return {**pos, "exit_reason": exit_reason}


def _make_entry_signal(
    symbol="AAPL",
    fill_price=100.0,
    n_day_ret=-0.05,
    ibs_entry=0.10,
    adv63=5_000_000.0,
    q_threshold=-0.03,
) -> dict:
    return {
        "symbol":      symbol,
        "fill_price":  fill_price,
        "n_day_ret":   n_day_ret,
        "ibs_entry":   ibs_entry,
        "adv63":       adv63,
        "q_threshold": q_threshold,
    }


def _snap(symbols_prices: dict) -> dict:
    return symbols_prices


# ═══════════════════════════════════════════════════════════════════════════════
# TestBuildExitOrders
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildExitOrders:

    def test_one_exit_signal_produces_one_order(self):
        sig = _make_exit_signal()
        orders = build_exit_orders([sig], [])
        assert len(orders) == 1

    def test_multiple_signals_produce_multiple_orders(self):
        sigs = [
            _make_exit_signal(symbol="AAPL", pos_id="P1"),
            _make_exit_signal(symbol="MSFT", pos_id="P2"),
        ]
        orders = build_exit_orders(sigs, [])
        assert len(orders) == 2

    def test_action_is_always_sell(self):
        orders = build_exit_orders([_make_exit_signal()], [])
        assert orders[0].action == "SELL"

    def test_order_type_is_always_moc(self, monkeypatch):
        """MOC exit is enforced regardless of config.EXIT_ORDER_TYPE."""
        monkeypatch.setattr(config, "EXIT_ORDER_TYPE", "LOC")
        orders = build_exit_orders([_make_exit_signal()], [])
        assert orders[0].order_type == "MOC"

    def test_quantity_matches_position_shares(self):
        sig = _make_exit_signal(shares=250)
        orders = build_exit_orders([sig], [])
        assert orders[0].quantity == 250

    def test_limit_price_is_none(self):
        orders = build_exit_orders([_make_exit_signal()], [])
        assert orders[0].limit_price is None

    def test_symbol_carried_through(self):
        orders = build_exit_orders([_make_exit_signal(symbol="TSLA")], [])
        assert orders[0].symbol == "TSLA"

    def test_pos_id_carried_through(self):
        orders = build_exit_orders([_make_exit_signal(pos_id="P99")], [])
        assert orders[0].pos_id == "P99"

    def test_reason_is_exit_reason(self):
        orders = build_exit_orders([_make_exit_signal(exit_reason="time_stop")], [])
        assert orders[0].reason == "time_stop"

    def test_zero_shares_signal_skipped(self):
        sig = _make_exit_signal(shares=0)
        orders = build_exit_orders([sig], [])
        assert orders == []

    def test_shares_from_positions_fallback(self):
        """If exit signal lacks shares, resolve from positions list."""
        sig = {"symbol": "AAPL", "pos_id": "P1", "exit_reason": "ibs_exit"}
        positions = [_make_position(symbol="AAPL", pos_id="P1", shares=77)]
        orders = build_exit_orders([sig], positions)
        assert len(orders) == 1
        assert orders[0].quantity == 77

    def test_empty_signals_returns_empty(self):
        assert build_exit_orders([], []) == []


# ═══════════════════════════════════════════════════════════════════════════════
# TestBuildEntryOrders — slot and symbol filters
# ═══════════════════════════════════════════════════════════════════════════════

class TestEntrySlotAndSymbolFilters:

    def test_already_held_symbol_skipped(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        positions = [_make_position(symbol="AAPL")]
        sig = _make_entry_signal(symbol="AAPL")
        orders = build_entry_orders([sig], positions, 100_000.0, {"AAPL": 100.0})
        assert orders == []

    def test_new_symbol_not_skipped(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "MAX_POSITIONS", 15)
        positions = [_make_position(symbol="MSFT")]
        sig = _make_entry_signal(symbol="AAPL", fill_price=50.0, adv63=10_000_000.0)
        orders = build_entry_orders([sig], positions, 100_000.0, {"AAPL": 50.0})
        assert len(orders) == 1
        assert orders[0].symbol == "AAPL"

    def test_respects_max_positions_slot_limit(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "MAX_POSITIONS", 2)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.5)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.5)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)  # disable

        # 2 already open → no slots
        positions = [
            _make_position(symbol="X1", pos_id="P1"),
            _make_position(symbol="X2", pos_id="P2"),
        ]
        sigs = [_make_entry_signal(symbol=s) for s in ["A", "B", "C"]]
        orders = build_entry_orders(sigs, positions, 100_000.0,
                                    {"A": 100.0, "B": 100.0, "C": 100.0})
        assert orders == []

    def test_only_fills_remaining_free_slots(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "MAX_POSITIONS", 3)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 3.0)  # large budget
        monkeypatch.setattr(config, "MAX_NOTIONAL", 1.0)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)

        positions = [_make_position(symbol="X1", pos_id="P1", shares=1, fill_price=1.0)]
        sigs = [_make_entry_signal(symbol=s, adv63=0) for s in ["A", "B", "C"]]
        snap = {"X1": 1.0, "A": 50.0, "B": 50.0, "C": 50.0}
        orders = build_entry_orders(sigs, positions, 100_000.0, snap)
        # 1 open, MAX=3 → 2 free slots
        assert len(orders) == 2

    def test_empty_signals_returns_empty(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        orders = build_entry_orders([], [], 100_000.0, {})
        assert orders == []

    def test_zero_equity_returns_empty(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        sig = _make_entry_signal()
        orders = build_entry_orders([sig], [], 0.0, {"AAPL": 100.0})
        assert orders == []


# ═══════════════════════════════════════════════════════════════════════════════
# TestEntryOrders — sizing formula
# ═══════════════════════════════════════════════════════════════════════════════

class TestEntrySizing:

    def _base_monkeypatch(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)   # disable gate
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.5)
        monkeypatch.setattr(config, "MAX_POSITIONS", 15)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.1)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "MOC")

    def test_target_shares_formula_matches_reference(self, monkeypatch):
        """
        target_shares = int((equity × MAX_TOTAL_NOTIONAL / MAX_POSITIONS) / snap_price)
        With equity=100_000, MAX_TOTAL_NOTIONAL=1.5, MAX_POSITIONS=15, snap_price=100:
          = int((100_000 × 1.5 / 15) / 100) = int(10_000 / 100) = 100
        max_cap_shares = int((100_000 × 0.1) / 100) = 100
        shares = min(100, 100) = 100
        """
        self._base_monkeypatch(monkeypatch)
        equity     = 100_000.0
        snap_price = 100.0
        sig = _make_entry_signal(symbol="AAPL", fill_price=snap_price, adv63=0)
        orders = build_entry_orders([sig], [], equity, {"AAPL": snap_price})
        assert len(orders) == 1
        assert orders[0].quantity == 100

    def test_max_notional_cap_applied(self, monkeypatch):
        """
        With MAX_NOTIONAL=0.05 and a cheap stock:
        target = int((100_000 × 1.5 / 15) / 10) = int(10_000 / 10) = 1_000
        cap    = int(100_000 × 0.05 / 10) = 500
        shares = min(1_000, 500) = 500
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.05)
        equity     = 100_000.0
        snap_price = 10.0
        sig = _make_entry_signal(symbol="AAPL", fill_price=snap_price, adv63=0)
        orders = build_entry_orders([sig], [], equity, {"AAPL": snap_price})
        assert len(orders) == 1
        expected_cap = int(equity * 0.05 / snap_price)
        assert orders[0].quantity == expected_cap

    def test_shares_less_than_one_skipped(self, monkeypatch):
        """Very high price relative to equity → shares rounds to 0 → skipped."""
        self._base_monkeypatch(monkeypatch)
        equity     = 1_000.0
        snap_price = 100_000.0   # absurdly high
        sig = _make_entry_signal(symbol="AAPL", fill_price=snap_price, adv63=0)
        orders = build_entry_orders([sig], [], equity, {"AAPL": snap_price})
        assert orders == []

    def test_target_shares_formula_various_prices(self, monkeypatch):
        """Verify formula for several snap prices using exact reference arithmetic."""
        self._base_monkeypatch(monkeypatch)
        equity = 200_000.0
        for price in [25.0, 50.0, 75.0, 200.0]:
            expected_target = int((equity * 1.5 / 15) / price)
            expected_cap    = int(equity * 0.1 / price)
            expected_shares = min(expected_target, expected_cap)
            if expected_shares < 1:
                continue
            sig = _make_entry_signal(symbol="AAPL", fill_price=price, adv63=0)
            orders = build_entry_orders([sig], [], equity, {"AAPL": price})
            assert len(orders) == 1
            assert orders[0].quantity == expected_shares, (
                f"price={price}: expected {expected_shares}, got {orders[0].quantity}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TestEntryGates
# ═══════════════════════════════════════════════════════════════════════════════

class TestEntryGates:

    def _base_monkeypatch(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.5)
        monkeypatch.setattr(config, "MAX_POSITIONS", 15)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.1)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "MOC")

    def test_liquidity_gate_rejects_oversized_order(self, monkeypatch):
        """
        notional = shares × price.
        Gate: notional > adv63 × LIQUIDITY_ADV_MAX_PCT → reject.
        With equity=100_000, snap=100, shares=100 → notional=10_000.
        adv63=50_000, LIQUIDITY_ADV_MAX_PCT=0.05 → limit=2_500.
        10_000 > 2_500 → rejected.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0.05)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=50_000.0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": 100.0})
        assert orders == []

    def test_liquidity_gate_passes_when_notional_within_limit(self, monkeypatch):
        """
        notional=10_000, adv63=1_000_000, LIQUIDITY_ADV_MAX_PCT=0.05 → limit=50_000.
        10_000 < 50_000 → passes.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0.05)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=1_000_000.0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": 100.0})
        assert len(orders) == 1

    def test_liquidity_gate_disabled_when_pct_zero(self, monkeypatch):
        """LIQUIDITY_ADV_MAX_PCT=0 → gate disabled, order passes."""
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=1.0)  # tiny adv63
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": 100.0})
        assert len(orders) == 1

    def test_total_notional_gate_rejects_when_budget_exceeded(self, monkeypatch):
        """
        equity=100_000, MAX_TOTAL_NOTIONAL=1.0.
        Budget = 100_000.
        Open position MTM = 95_000 (already near budget).
        New notional would push total over budget → rejected.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.0)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.5)

        equity = 100_000.0
        # Open position worth 95,000 at snap price
        positions = [{"symbol": "HELD", "shares": 950, "fill_price": 100.0}]
        snap_prices = {"HELD": 100.0, "AAPL": 50.0}

        sig = _make_entry_signal(symbol="AAPL", fill_price=50.0, adv63=0)
        orders = build_entry_orders([sig], positions, equity, snap_prices)
        # deployed=95_000, new notional=shares×50; budget=100_000 → no room
        assert orders == []

    def test_total_notional_gate_passes_when_within_budget(self, monkeypatch):
        """
        equity=100_000, MAX_TOTAL_NOTIONAL=1.5. Budget=150_000.
        Open position MTM=10_000. New notional=10_000. Total=20_000 < 150_000 → passes.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.5)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)

        equity = 100_000.0
        positions = [{"symbol": "HELD", "shares": 100, "fill_price": 100.0}]
        snap_prices = {"HELD": 100.0, "AAPL": 100.0}

        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        orders = build_entry_orders([sig], positions, equity, snap_prices)
        assert len(orders) == 1

    def test_total_notional_gate_uses_snap_prices_not_entry_price(self, monkeypatch):
        """
        Existing position was entered at 50, now worth 200 (snap).
        Gate must use snap price (MTM), not entry price.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.0)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.5)

        equity = 100_000.0
        # 500 shares entered at 50 (notional 25_000) — but MTM is 500×200=100_000
        positions = [{"symbol": "HELD", "shares": 500, "fill_price": 50.0}]
        snap_prices = {"HELD": 200.0, "AAPL": 100.0}

        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        orders = build_entry_orders([sig], positions, equity, snap_prices)
        # deployed_mtm = 500 × 200 = 100_000; budget = 100_000 → no room
        assert orders == []


# ═══════════════════════════════════════════════════════════════════════════════
# TestEntryOrderType — LOC vs MOC
# ═══════════════════════════════════════════════════════════════════════════════

class TestEntryOrderType:

    def _base_monkeypatch(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.5)
        monkeypatch.setattr(config, "MAX_POSITIONS", 15)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.1)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)

    def test_loc_order_type(self, monkeypatch):
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "LOC")
        monkeypatch.setattr(config, "ENTRY_LOC_BUFFER_PCT", 0.003)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": 100.0})
        assert orders[0].order_type == "LOC"

    def test_loc_limit_price_formula(self, monkeypatch):
        """limit_price = snap_price × (1 + ENTRY_LOC_BUFFER_PCT)"""
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "LOC")
        monkeypatch.setattr(config, "ENTRY_LOC_BUFFER_PCT", 0.003)
        snap_price = 100.0
        sig = _make_entry_signal(symbol="AAPL", fill_price=snap_price, adv63=0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": snap_price})
        expected = snap_price * (1.0 + 0.003)
        assert abs(orders[0].limit_price - expected) < 1e-9

    def test_moc_entry_has_no_limit_price(self, monkeypatch):
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "MOC")
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": 100.0})
        assert orders[0].order_type  == "MOC"
        assert orders[0].limit_price is None

    def test_loc_limit_price_uses_snap_price_not_fill_price(self, monkeypatch):
        """snap_price from snap_prices dict overrides signal fill_price for the limit calc."""
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "LOC")
        monkeypatch.setattr(config, "ENTRY_LOC_BUFFER_PCT", 0.005)
        sig_fill  = 90.0    # stale close price in signal
        snap_now  = 95.0    # 15:40 snap
        sig = _make_entry_signal(symbol="AAPL", fill_price=sig_fill, adv63=0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": snap_now})
        expected = snap_now * (1.0 + 0.005)
        assert abs(orders[0].limit_price - expected) < 1e-9


# ═══════════════════════════════════════════════════════════════════════════════
# TestRankBy — sort order
# ═══════════════════════════════════════════════════════════════════════════════

class TestRankBy:

    def _base_monkeypatch(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 3.0)   # large budget
        monkeypatch.setattr(config, "MAX_NOTIONAL", 1.0)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "MOC")

    def test_rank_by_qpi_sorts_n_day_ret_ascending(self, monkeypatch):
        """
        RANK_BY=qpi → sort by n_day_ret ascending (most negative first).
        With MAX_POSITIONS=2 and 3 candidates, the two with lowest n_day_ret
        are chosen.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "RANK_BY", "qpi")
        monkeypatch.setattr(config, "MAX_POSITIONS", 2)

        sigs = [
            _make_entry_signal(symbol="A", fill_price=50.0, n_day_ret=-0.01, adv63=0),
            _make_entry_signal(symbol="B", fill_price=50.0, n_day_ret=-0.10, adv63=0),
            _make_entry_signal(symbol="C", fill_price=50.0, n_day_ret=-0.05, adv63=0),
        ]
        snap = {"A": 50.0, "B": 50.0, "C": 50.0}
        orders = build_entry_orders(sigs, [], 100_000.0, snap)

        assert len(orders) == 2
        symbols = [o.symbol for o in orders]
        # B (most negative) and C must be chosen; A (least negative) dropped
        assert "B" in symbols
        assert "C" in symbols
        assert "A" not in symbols

    def test_rank_by_ibs_sorts_ibs_entry_ascending(self, monkeypatch):
        """
        RANK_BY=ibs → sort by ibs_entry ascending (lowest IBS first).
        With MAX_POSITIONS=2 and 3 candidates, the two with lowest ibs_entry
        are chosen.
        """
        self._base_monkeypatch(monkeypatch)
        monkeypatch.setattr(config, "RANK_BY", "ibs")
        monkeypatch.setattr(config, "MAX_POSITIONS", 2)

        sigs = [
            _make_entry_signal(symbol="A", fill_price=50.0, ibs_entry=0.18, adv63=0),
            _make_entry_signal(symbol="B", fill_price=50.0, ibs_entry=0.02, adv63=0),
            _make_entry_signal(symbol="C", fill_price=50.0, ibs_entry=0.10, adv63=0),
        ]
        snap = {"A": 50.0, "B": 50.0, "C": 50.0}
        orders = build_entry_orders(sigs, [], 100_000.0, snap)

        assert len(orders) == 2
        symbols = [o.symbol for o in orders]
        # B (lowest IBS) and C must be chosen; A (highest) dropped
        assert "B" in symbols
        assert "C" in symbols
        assert "A" not in symbols


# ═══════════════════════════════════════════════════════════════════════════════
# TestImbalanceFilter
# ═══════════════════════════════════════════════════════════════════════════════

class TestImbalanceFilter:

    def _base_monkeypatch(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
        monkeypatch.setattr(config, "MAX_TOTAL_NOTIONAL", 1.5)
        monkeypatch.setattr(config, "MAX_POSITIONS", 15)
        monkeypatch.setattr(config, "MAX_NOTIONAL", 0.1)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "MOC")
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", True)
        monkeypatch.setattr(config, "RISK_IMBALANCE_THRESHOLD", 0.3)
        monkeypatch.setattr(config, "RISK_IMBALANCE_ACTION", ["reject"])

        import db
        db.init_db()

    def test_imbalance_filter_blocks_high_ratio(self, monkeypatch, tmp_path):
        """Imbalance ratio above threshold → order skipped."""
        self._base_monkeypatch(monkeypatch, tmp_path)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        # ratio 0.5 > threshold 0.3 → reject
        orders = build_entry_orders(
            [sig], [], 100_000.0, {"AAPL": 100.0},
            imbalance_data={"AAPL": 0.5},
        )
        assert orders == []

    def test_imbalance_filter_passes_low_ratio(self, monkeypatch, tmp_path):
        """Imbalance ratio below threshold → order proceeds."""
        self._base_monkeypatch(monkeypatch, tmp_path)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        # ratio 0.1 < threshold 0.3 → passes
        orders = build_entry_orders(
            [sig], [], 100_000.0, {"AAPL": 100.0},
            imbalance_data={"AAPL": 0.1},
        )
        assert len(orders) == 1

    def test_imbalance_filter_skipped_when_data_is_none(self, monkeypatch, tmp_path):
        """If imbalance_data is None, skip check even when RISK_IMBALANCE_ENABLED."""
        self._base_monkeypatch(monkeypatch, tmp_path)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        orders = build_entry_orders(
            [sig], [], 100_000.0, {"AAPL": 100.0},
            imbalance_data=None,
        )
        assert len(orders) == 1

    def test_imbalance_filter_skipped_when_disabled(self, monkeypatch, tmp_path):
        """RISK_IMBALANCE_ENABLED=False → no check regardless of imbalance_data."""
        self._base_monkeypatch(monkeypatch, tmp_path)
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        sig = _make_entry_signal(symbol="AAPL", fill_price=100.0, adv63=0)
        # Would-be-rejected ratio, but filter is off
        orders = build_entry_orders(
            [sig], [], 100_000.0, {"AAPL": 100.0},
            imbalance_data={"AAPL": 0.99},
        )
        assert len(orders) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# TestOrderDataclass — Order is from ib_exec
# ═══════════════════════════════════════════════════════════════════════════════

class TestOrderDataclass:

    def test_order_imported_from_ib_exec(self):
        from ib_exec import Order as IbOrder
        assert Order is IbOrder

    def test_exit_orders_are_order_instances(self):
        sig = _make_exit_signal()
        orders = build_exit_orders([sig], [])
        assert all(isinstance(o, Order) for o in orders)

    def test_entry_orders_are_order_instances(self, monkeypatch):
        monkeypatch.setattr(config, "RISK_IMBALANCE_ENABLED", False)
        monkeypatch.setattr(config, "LIQUIDITY_ADV_MAX_PCT", 0)
        monkeypatch.setattr(config, "ENTRY_ORDER_TYPE", "MOC")
        sig = _make_entry_signal(adv63=0)
        orders = build_entry_orders([sig], [], 100_000.0, {"AAPL": 100.0})
        assert all(isinstance(o, Order) for o in orders)
