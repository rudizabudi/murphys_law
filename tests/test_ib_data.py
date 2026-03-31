"""
tests/test_ib_data.py — Unit tests for ib_data.py

All IB network calls are mocked — no live TWS/Gateway connection required.
"""

import queue
import threading
from unittest.mock import MagicMock

import pytest

import ib_data
from ib_data import (
    _BATCH_SIZE,
    _INTER_BATCH_SLEEP,
    _SNAP_TIMEOUT,
    _TICK_HIGH,
    _TICK_LAST,
    _TICK_LOW,
    _TICK_OPEN,
    _TICK_PREV_CLOSE,
    _TICK_VOLUME,
    _snapshot_callbacks,
    fetch_snapshot,
)
from ib_exec import _SENTINEL


# ── Helpers ───────────────────────────────────────────────────────────────────

class _FakeBridge:
    """
    Minimal bridge stub with a thread-safe order-ID counter, mocked
    reqMktData / cancelMktData, and the queue attributes IBBridge normally has.
    """

    def __init__(self):
        self._next_order_id = 100
        self._lock = threading.Lock()
        self.reqMktData    = MagicMock()
        self.cancelMktData = MagicMock()
        self.__dict__      # ensure instance __dict__ is accessible

    def _get_next_order_id(self) -> int:
        with self._lock:
            oid = self._next_order_id
            self._next_order_id += 1
        return oid


def _make_bridge() -> _FakeBridge:
    return _FakeBridge()


def _seed_snap(bridge: _FakeBridge, open_: float, high: float, low: float,
               close: float, volume: int):
    """
    Configure bridge.reqMktData to fire all five tick callbacks synchronously
    (via side_effect) so fetch_snapshot receives data without any real IB call.
    """
    def _side_effect(req_id, contract, generic_ticks, snapshot, reg_snap, options):
        bridge.tickPrice(req_id, _TICK_OPEN,  open_,  None)
        bridge.tickPrice(req_id, _TICK_HIGH,  high,   None)
        bridge.tickPrice(req_id, _TICK_LOW,   low,    None)
        bridge.tickPrice(req_id, _TICK_LAST,  close,  None)
        bridge.tickSize( req_id, _TICK_VOLUME, volume)
        bridge.tickSnapshotEnd(req_id)

    bridge.reqMktData.side_effect = _side_effect


# ═══════════════════════════════════════════════════════════════════════════════
# TestSnapshotCallbacks  (context manager behaviour)
# ═══════════════════════════════════════════════════════════════════════════════

class TestSnapshotCallbacks:

    def test_callbacks_installed_on_entry(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge):
            assert "tickPrice"       in bridge.__dict__
            assert "tickSize"        in bridge.__dict__
            assert "tickSnapshotEnd" in bridge.__dict__

    def test_callbacks_removed_on_exit(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge):
            pass
        assert "tickPrice"       not in bridge.__dict__
        assert "tickSize"        not in bridge.__dict__
        assert "tickSnapshotEnd" not in bridge.__dict__

    def test_callbacks_removed_on_exception(self):
        bridge = _make_bridge()
        try:
            with _snapshot_callbacks(bridge):
                raise RuntimeError("test error")
        except RuntimeError:
            pass
        assert "tickPrice" not in bridge.__dict__

    def test_tickPrice_puts_to_queue(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge) as (snap_queues, lock):
            q = queue.Queue()
            snap_queues[42] = q
            bridge.tickPrice(42, _TICK_LAST, 150.0, None)
        assert not q.empty()
        item = q.get_nowait()
        assert item == ("P", _TICK_LAST, 150.0)

    def test_tickSize_puts_to_queue(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge) as (snap_queues, lock):
            q = queue.Queue()
            snap_queues[7] = q
            bridge.tickSize(7, _TICK_VOLUME, 500_000)
        item = q.get_nowait()
        assert item == ("S", _TICK_VOLUME, 500_000)

    def test_tickSnapshotEnd_puts_sentinel(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge) as (snap_queues, lock):
            q = queue.Queue()
            snap_queues[3] = q
            bridge.tickSnapshotEnd(3)
        assert q.get_nowait() is _SENTINEL

    def test_unknown_reqId_ignored(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge):
            # Should not raise even when reqId has no queue entry
            bridge.tickPrice(9999, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(9999)

    def test_zero_price_not_queued(self):
        bridge = _make_bridge()
        with _snapshot_callbacks(bridge) as (snap_queues, lock):
            q = queue.Queue()
            snap_queues[1] = q
            bridge.tickPrice(1, _TICK_LAST, 0.0, None)   # zero → skip
        assert q.empty()


# ═══════════════════════════════════════════════════════════════════════════════
# TestFetchSnapshot
# ═══════════════════════════════════════════════════════════════════════════════

class TestFetchSnapshot:

    def test_empty_symbols_returns_empty(self):
        bridge = _make_bridge()
        assert fetch_snapshot([], bridge) == {}

    def test_single_symbol_returns_data(self):
        bridge = _make_bridge()
        _seed_snap(bridge, open_=100.0, high=102.0, low=99.0, close=101.0, volume=500_000)
        result = fetch_snapshot(["AAPL"], bridge)
        assert "AAPL" in result
        assert result["AAPL"]["close"] == 101.0

    def test_all_fields_populated(self):
        bridge = _make_bridge()
        _seed_snap(bridge, open_=100.0, high=102.0, low=99.0, close=101.0, volume=750_000)
        snap = fetch_snapshot(["AAPL"], bridge)["AAPL"]
        assert snap["open"]   == 100.0
        assert snap["high"]   == 102.0
        assert snap["low"]    == 99.0
        assert snap["close"]  == 101.0
        assert snap["volume"] == 750_000

    def test_reqMktData_called_for_each_symbol(self):
        bridge = _make_bridge()
        _seed_snap(bridge, 1.0, 1.0, 1.0, 1.0, 0)
        fetch_snapshot(["AAPL", "MSFT", "GOOG"], bridge)
        assert bridge.reqMktData.call_count == 3

    def test_cancelMktData_called_for_each_symbol(self):
        bridge = _make_bridge()
        _seed_snap(bridge, 1.0, 1.0, 1.0, 1.0, 0)
        fetch_snapshot(["AAPL", "MSFT"], bridge)
        assert bridge.cancelMktData.call_count == 2

    def test_reqMktData_uses_snapshot_true(self):
        bridge = _make_bridge()
        _seed_snap(bridge, 1.0, 1.0, 1.0, 1.0, 0)
        fetch_snapshot(["AAPL"], bridge)
        _, call_kwargs = bridge.reqMktData.call_args
        pos_args = bridge.reqMktData.call_args[0]
        # signature: reqMktData(reqId, contract, genericTickList, snapshot, ...)
        assert pos_args[3] is True   # snapshot=True

    def test_symbol_without_close_excluded(self):
        """If only volume arrives and no LAST price, symbol should be absent."""
        bridge = _make_bridge()

        def _no_close(req_id, contract, generic, snap, reg, opts):
            bridge.tickSize(req_id, _TICK_VOLUME, 100_000)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _no_close
        result = fetch_snapshot(["AAPL"], bridge)
        assert "AAPL" not in result

    def test_prev_close_used_as_fallback(self):
        """PREV_CLOSE tick (type 9) is used when LAST (type 4) is absent."""
        bridge = _make_bridge()

        def _prev_close_only(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_PREV_CLOSE, 99.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _prev_close_only
        result = fetch_snapshot(["AAPL"], bridge)
        assert "AAPL" in result
        assert result["AAPL"]["close"] == 99.0

    def test_last_overrides_prev_close(self):
        """LAST price takes priority; PREV_CLOSE seen first should not win."""
        bridge = _make_bridge()

        def _both(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_PREV_CLOSE, 99.0, None)
            bridge.tickPrice(req_id, _TICK_LAST,       101.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _both
        result = fetch_snapshot(["AAPL"], bridge)
        assert result["AAPL"]["close"] == 101.0

    def test_multiple_symbols_independent(self):
        bridge = _make_bridge()
        prices = {"AAPL": 150.0, "MSFT": 300.0}

        def _side(req_id, contract, generic, snap, reg, opts):
            sym   = contract.symbol
            price = prices[sym]
            bridge.tickPrice(req_id, _TICK_LAST, price, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        result = fetch_snapshot(["AAPL", "MSFT"], bridge)
        assert result["AAPL"]["close"] == 150.0
        assert result["MSFT"]["close"] == 300.0

    def test_timeout_symbol_excluded_others_included(self, monkeypatch):
        """A symbol that times out is absent; others still return data."""
        monkeypatch.setattr(ib_data, "_SNAP_TIMEOUT", 0.05)
        bridge = _make_bridge()

        def _side(req_id, contract, generic, snap, reg, opts):
            if contract.symbol == "MSFT":
                bridge.tickPrice(req_id, _TICK_LAST, 300.0, None)
                bridge.tickSnapshotEnd(req_id)
            # AAPL: do nothing → timeout

        bridge.reqMktData.side_effect = _side
        result = fetch_snapshot(["AAPL", "MSFT"], bridge)
        assert "AAPL" not in result
        assert "MSFT" in result

    def test_stk_contract_used(self):
        bridge = _make_bridge()
        _seed_snap(bridge, 1.0, 1.0, 1.0, 1.0, 0)
        fetch_snapshot(["AAPL"], bridge)
        contract = bridge.reqMktData.call_args[0][1]
        assert contract.secType  == "STK"
        assert contract.currency == "USD"
        assert contract.symbol   == "AAPL"

    def test_callbacks_cleaned_up_after_call(self):
        bridge = _make_bridge()
        _seed_snap(bridge, 1.0, 1.0, 1.0, 1.0, 0)
        fetch_snapshot(["AAPL"], bridge)
        assert "tickPrice"       not in bridge.__dict__
        assert "tickSize"        not in bridge.__dict__
        assert "tickSnapshotEnd" not in bridge.__dict__

    def test_callbacks_cleaned_up_on_timeout(self, monkeypatch):
        monkeypatch.setattr(ib_data, "_SNAP_TIMEOUT", 0.02)
        bridge = _make_bridge()
        # reqMktData does nothing → timeout
        fetch_snapshot(["AAPL"], bridge)
        assert "tickPrice" not in bridge.__dict__

    def test_large_symbol_list(self):
        """Sanity: 10 symbols all return data."""
        bridge = _make_bridge()
        symbols = [f"SYM{i}" for i in range(10)]

        def _side(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        result = fetch_snapshot(symbols, bridge)
        assert len(result) == 10


# ═══════════════════════════════════════════════════════════════════════════════
# TestBatching
# ═══════════════════════════════════════════════════════════════════════════════

class TestBatching:

    def test_single_batch_no_sleep(self, monkeypatch):
        """Fewer than _BATCH_SIZE symbols → time.sleep never called."""
        sleep_calls = []
        monkeypatch.setattr(ib_data.time, "sleep", lambda s: sleep_calls.append(s))

        bridge = _make_bridge()
        symbols = [f"S{i}" for i in range(_BATCH_SIZE - 1)]

        def _side(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        fetch_snapshot(symbols, bridge)
        assert sleep_calls == []

    def test_two_batches_one_sleep(self, monkeypatch):
        """_BATCH_SIZE + 1 symbols → exactly one sleep between the two batches."""
        sleep_calls = []
        monkeypatch.setattr(ib_data.time, "sleep", lambda s: sleep_calls.append(s))

        bridge = _make_bridge()
        symbols = [f"S{i:03d}" for i in range(_BATCH_SIZE + 1)]

        def _side(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        fetch_snapshot(symbols, bridge)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == _INTER_BATCH_SLEEP

    def test_three_batches_two_sleeps(self, monkeypatch):
        """2 * _BATCH_SIZE + 1 symbols → two sleeps."""
        sleep_calls = []
        monkeypatch.setattr(ib_data.time, "sleep", lambda s: sleep_calls.append(s))

        bridge = _make_bridge()
        symbols = [f"S{i:03d}" for i in range(2 * _BATCH_SIZE + 1)]

        def _side(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        fetch_snapshot(symbols, bridge)
        assert len(sleep_calls) == 2

    def test_all_symbols_returned_across_batches(self, monkeypatch):
        """All symbols across multiple batches are present in the result."""
        monkeypatch.setattr(ib_data.time, "sleep", lambda s: None)

        bridge = _make_bridge()
        symbols = [f"S{i:03d}" for i in range(_BATCH_SIZE + 5)]

        def _side(req_id, contract, generic, snap, reg, opts):
            bridge.tickPrice(req_id, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        result = fetch_snapshot(symbols, bridge)
        assert len(result) == len(symbols)

    def test_timeout_in_second_batch_excluded(self, monkeypatch):
        """A timeout in the second batch excludes only that symbol."""
        monkeypatch.setattr(ib_data, "_SNAP_TIMEOUT", 0.05)
        monkeypatch.setattr(ib_data.time, "sleep", lambda s: None)

        bridge = _make_bridge()
        # Fill first batch fully; make last symbol in second batch time out
        symbols = [f"S{i:03d}" for i in range(_BATCH_SIZE + 1)]
        timeout_sym = symbols[-1]

        def _side(req_id, contract, generic, snap, reg, opts):
            if contract.symbol == timeout_sym:
                return   # no response → timeout
            bridge.tickPrice(req_id, _TICK_LAST, 100.0, None)
            bridge.tickSnapshotEnd(req_id)

        bridge.reqMktData.side_effect = _side
        result = fetch_snapshot(symbols, bridge)
        assert timeout_sym not in result
        assert len(result) == _BATCH_SIZE
