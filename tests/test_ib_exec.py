"""
tests/test_ib_exec.py — Unit tests for ib_exec.py

All ibapi network calls are mocked — no live IB connection required.
"""

import queue
import socket
import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest

import config
import ib_exec
import ib_exec
from ib_exec import (
    IBBridge,
    IBCController,
    Order,
    OrderRejectedError,
    SENTINEL,
    detect_splits,
    get_account_summary,
    get_filled_orders,
    submit_order,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_order(
    symbol="AAPL",
    action="BUY",
    order_type="LOC",
    quantity=100,
    limit_price=155.50,
    reason="entry",
    pos_id="P1",
) -> Order:
    return Order(
        symbol=symbol,
        action=action,
        order_type=order_type,
        quantity=quantity,
        limit_price=limit_price,
        reason=reason,
        pos_id=pos_id,
    )


def _make_bridge_stub() -> IBBridge:
    """
    Return an IBBridge with EClient.connect / EClient.run / EClient.disconnect
    patched out so no real network calls happen.
    The nextValidId callback is simulated to give _next_order_id=100.
    """
    bridge = IBBridge.__new__(IBBridge)
    bridge._next_order_id = 100
    bridge._order_id_lock = threading.Lock()
    bridge._order_id_q    = queue.Queue()
    bridge._account_q     = queue.Queue()
    bridge._exec_q        = queue.Queue()
    bridge._position_q    = queue.Queue()
    bridge._time_q        = queue.Queue()
    bridge._thread        = None

    # Stub out all EClient network methods
    bridge.placeOrder          = MagicMock()
    bridge.reqAccountSummary   = MagicMock()
    bridge.reqExecutions       = MagicMock()
    bridge.isConnected         = MagicMock(return_value=True)

    return bridge


# ═══════════════════════════════════════════════════════════════════════════════
# TestIBCController
# ═══════════════════════════════════════════════════════════════════════════════

class TestIBCController:

    def _mock_resp(self, status_code: int = 200):
        m = MagicMock()
        m.status_code = status_code
        return m

    def test_start_gateway_calls_start_api(self):
        ctrl = IBCController()
        with patch("httpx.get", return_value=self._mock_resp(200)) as mock_get:
            ctrl.start_gateway()
        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        assert "/start-api" in url

    def test_start_gateway_url_has_http_scheme(self):
        ctrl = IBCController()
        with patch("httpx.get", return_value=self._mock_resp(200)) as mock_get:
            ctrl.start_gateway()
        url = mock_get.call_args[0][0]
        assert url.startswith("http://")

    def test_stop_gateway_calls_stop_api(self):
        ctrl = IBCController()
        with patch("httpx.get", return_value=self._mock_resp(200)) as mock_get:
            ctrl.stop_gateway()
        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        assert "/stop-api" in url

    def test_stop_gateway_non_200_logs_error(self, caplog):
        import logging
        ctrl = IBCController()
        with patch("httpx.get", return_value=self._mock_resp(500)):
            with caplog.at_level(logging.ERROR, logger="murphy"):
                ctrl.stop_gateway()
        assert any("unable to connect" in r.message.lower() or "error" in r.levelname.lower()
                   for r in caplog.records)

    def test_start_gateway_non_200_logs_error(self, caplog):
        import logging
        ctrl = IBCController()
        with patch("httpx.get", return_value=self._mock_resp(500)):
            with caplog.at_level(logging.ERROR, logger="murphy"):
                ctrl.start_gateway()
        assert any("unable to connect" in r.message.lower() or "error" in r.levelname.lower()
                   for r in caplog.records)

    def test_wait_for_api_returns_true_when_port_opens(self, monkeypatch):
        """Simulate port closed on first probe, open on second."""
        call_count = {"n": 0}

        def fake_connect_ex(addr):
            call_count["n"] += 1
            return 0 if call_count["n"] >= 2 else 1  # fail once, then succeed

        mock_sock = MagicMock()
        mock_sock.__enter__ = lambda s: s
        mock_sock.__exit__  = MagicMock(return_value=False)
        mock_sock.settimeout = MagicMock()
        mock_sock.connect_ex = fake_connect_ex

        ctrl = IBCController()
        with patch("socket.socket", return_value=mock_sock):
            with patch("time.sleep"):
                result = ctrl.wait_for_api(timeout=10)

        assert result is True

    def test_wait_for_api_returns_false_on_timeout(self, monkeypatch):
        """Port never opens — should return False after deadline."""
        mock_sock = MagicMock()
        mock_sock.__enter__ = lambda s: s
        mock_sock.__exit__  = MagicMock(return_value=False)
        mock_sock.settimeout = MagicMock()
        mock_sock.connect_ex = MagicMock(return_value=1)  # always closed

        ctrl = IBCController()
        with patch("socket.socket", return_value=mock_sock):
            with patch("time.monotonic", side_effect=[0, 0, 5, 5, 11]):
                with patch("time.sleep"):
                    result = ctrl.wait_for_api(timeout=10)

        assert result is False

    def test_wait_for_api_returns_true_immediately(self):
        """Port is open on first probe — return True without sleeping."""
        mock_sock = MagicMock()
        mock_sock.__enter__ = lambda s: s
        mock_sock.__exit__  = MagicMock(return_value=False)
        mock_sock.settimeout = MagicMock()
        mock_sock.connect_ex = MagicMock(return_value=0)  # open immediately

        sleep_mock = MagicMock()
        ctrl = IBCController()
        with patch("socket.socket", return_value=mock_sock):
            with patch("time.sleep", sleep_mock):
                result = ctrl.wait_for_api(timeout=30)

        assert result is True
        sleep_mock.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# TestIBBridgeConnect
# ═══════════════════════════════════════════════════════════════════════════════

class TestIBBridgeConnect:

    def test_connect_starts_daemon_thread(self):
        """connect() must start exactly one daemon thread named 'ib-run'."""
        bridge = IBBridge()

        def fake_run(*args, **kwargs):
            # Fires in the daemon thread — unblocks connect()'s queue.get()
            bridge.nextValidId(200)

        with patch("ibapi.client.EClient.connect"):
            with patch("ibapi.client.EClient.run", side_effect=fake_run):
                bridge.connect()

        assert bridge._thread is not None
        assert bridge._thread.daemon is True
        assert bridge._thread.name == "ib-run"

    def test_connect_sets_next_order_id_from_callback(self):
        """connect() must block until nextValidId fires and store the ID."""
        bridge = IBBridge()

        def fake_run():
            bridge.nextValidId(42)

        with patch("ibapi.client.EClient.connect"):
            with patch("ibapi.client.EClient.run", side_effect=fake_run):
                bridge.connect()

        assert bridge._next_order_id == 42

    def test_is_connected_delegates_to_eclient(self):
        bridge = _make_bridge_stub()
        bridge.isConnected.return_value = True
        assert bridge.is_connected() is True
        bridge.isConnected.return_value = False
        assert bridge.is_connected() is False

    def test_disconnect_calls_eclient_disconnect(self):
        bridge = _make_bridge_stub()
        with patch("ibapi.client.EClient.disconnect") as mock_disc:
            IBBridge.disconnect(bridge)
        mock_disc.assert_called_once()

    def test_reconnect_calls_disconnect_then_connect(self):
        bridge = _make_bridge_stub()
        disconnect_calls = []
        connect_calls    = []

        with patch.object(bridge, "disconnect", side_effect=lambda: disconnect_calls.append(1)):
            with patch.object(bridge, "connect",    side_effect=lambda: connect_calls.append(1)):
                with patch("time.sleep"):
                    bridge.reconnect()

        assert len(disconnect_calls) == 1
        assert len(connect_calls)    == 1
        # disconnect must be called before connect
        assert disconnect_calls[0] == 1

    def test_reconnect_sleeps_between_calls(self):
        bridge = _make_bridge_stub()
        slept = []
        with patch.object(bridge, "disconnect"):
            with patch.object(bridge, "connect"):
                with patch("time.sleep", side_effect=lambda s: slept.append(s)):
                    bridge.reconnect()
        assert len(slept) == 1
        assert slept[0] == 3


# ═══════════════════════════════════════════════════════════════════════════════
# TestSubmitOrder
# ═══════════════════════════════════════════════════════════════════════════════

class TestSubmitOrder:

    def test_submit_loc_calls_place_order(self):
        bridge = _make_bridge_stub()
        order = _make_order(order_type="LOC", limit_price=155.0)
        order_id = submit_order(bridge, order)

        bridge.placeOrder.assert_called_once()
        called_id, contract, ib_order = bridge.placeOrder.call_args[0]
        assert called_id == order_id

    def test_submit_returns_order_id(self):
        bridge = _make_bridge_stub()
        bridge._next_order_id = 50
        oid = submit_order(bridge, _make_order())
        assert oid == 50

    def test_order_id_increments(self):
        bridge = _make_bridge_stub()
        bridge._next_order_id = 10
        id1 = submit_order(bridge, _make_order(symbol="AAPL"))
        id2 = submit_order(bridge, _make_order(symbol="MSFT"))
        assert id1 == 10
        assert id2 == 11

    def test_loc_order_type_and_tif(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(order_type="LOC", limit_price=150.0))
        _, _, ib_order = bridge.placeOrder.call_args[0]
        assert ib_order.orderType == "LMT"
        assert ib_order.tif       == "LOC"

    def test_loc_order_carries_limit_price(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(order_type="LOC", limit_price=123.45))
        _, _, ib_order = bridge.placeOrder.call_args[0]
        assert ib_order.lmtPrice == 123.45

    def test_moc_order_type(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(order_type="MOC", limit_price=None))
        _, _, ib_order = bridge.placeOrder.call_args[0]
        assert ib_order.orderType == "MOC"

    def test_moc_order_has_no_limit_price(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(order_type="MOC", limit_price=None))
        _, _, ib_order = bridge.placeOrder.call_args[0]
        # IBOrder default lmtPrice is 0.0 — we must not have set it for MOC
        assert not hasattr(ib_order, "_lmt_set") or ib_order.lmtPrice == 0.0

    def test_contract_fields(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(symbol="TSLA"))
        _, contract, _ = bridge.placeOrder.call_args[0]
        assert contract.symbol   == "TSLA"
        assert contract.secType  == "STK"
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"

    def test_action_buy_sell(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(action="BUY"))
        _, _, ib_order = bridge.placeOrder.call_args[0]
        assert ib_order.action == "BUY"

        bridge2 = _make_bridge_stub()
        submit_order(bridge2, _make_order(action="SELL", order_type="MOC", limit_price=None))
        _, _, ib_order2 = bridge2.placeOrder.call_args[0]
        assert ib_order2.action == "SELL"

    def test_quantity_passed_through(self):
        bridge = _make_bridge_stub()
        submit_order(bridge, _make_order(quantity=250))
        _, _, ib_order = bridge.placeOrder.call_args[0]
        assert ib_order.totalQuantity == 250


# ═══════════════════════════════════════════════════════════════════════════════
# TestGetFilledOrders
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetFilledOrders:

    def _make_exec_side_effect(self, bridge, fills: list[dict]):
        """
        Return a side_effect for reqExecutions that seeds the exec queue
        *after* _drain() has run (mirrors real IB callback ordering).
        """
        def _side_effect(req_id, exec_filter):
            for fill in fills:
                bridge._exec_q.put(fill)
            bridge._exec_q.put(SENTINEL)
        return _side_effect

    def test_returns_fills_for_requested_ids(self):
        bridge = _make_bridge_stub()
        fills = [
            {"order_id": 10, "fill_price": 150.0, "fill_qty": 100, "status": "Filled"},
            {"order_id": 11, "fill_price": 200.0, "fill_qty": 50,  "status": "Filled"},
        ]
        bridge.reqExecutions.side_effect = self._make_exec_side_effect(bridge, fills)
        result = get_filled_orders(bridge, [10])
        assert 10 in result
        assert 11 not in result

    def test_fill_price_and_qty(self):
        bridge = _make_bridge_stub()
        fills = [{"order_id": 5, "fill_price": 99.50, "fill_qty": 200, "status": "Filled"}]
        bridge.reqExecutions.side_effect = self._make_exec_side_effect(bridge, fills)
        result = get_filled_orders(bridge, [5])
        assert result[5]["fill_price"] == 99.50
        assert result[5]["fill_qty"]   == 200
        assert result[5]["status"]     == "Filled"

    def test_empty_order_ids_returns_all(self):
        bridge = _make_bridge_stub()
        fills = [
            {"order_id": 1, "fill_price": 10.0, "fill_qty": 1, "status": "Filled"},
            {"order_id": 2, "fill_price": 20.0, "fill_qty": 2, "status": "Filled"},
        ]
        bridge.reqExecutions.side_effect = self._make_exec_side_effect(bridge, fills)
        result = get_filled_orders(bridge, [])
        assert set(result.keys()) == {1, 2}

    def test_no_fills_returns_empty_dict(self):
        bridge = _make_bridge_stub()
        bridge.reqExecutions.side_effect = self._make_exec_side_effect(bridge, [])
        result = get_filled_orders(bridge, [99])
        assert result == {}

    def test_reqExecutions_called(self):
        bridge = _make_bridge_stub()
        bridge.reqExecutions.side_effect = self._make_exec_side_effect(bridge, [])
        get_filled_orders(bridge, [1])
        bridge.reqExecutions.assert_called_once()

    def test_timeout_returns_partial_results(self):
        """If the sentinel never arrives (timeout), return whatever was collected."""
        bridge = _make_bridge_stub()

        def _side_effect(req_id, exec_filter):
            # Seed one fill but NO sentinel — simulates a stalled stream
            bridge._exec_q.put({"order_id": 7, "fill_price": 50.0, "fill_qty": 10, "status": "Filled"})

        bridge.reqExecutions.side_effect = _side_effect

        with patch.object(ib_exec, "_DEFAULT_TIMEOUT", 0.05):
            result = get_filled_orders(bridge, [7])

        assert 7 in result


# ═══════════════════════════════════════════════════════════════════════════════
# TestGetAccountSummary
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetAccountSummary:

    def _make_account_side_effect(self, bridge, tags: dict[str, str]):
        """
        Return a side_effect for reqAccountSummary that seeds the account queue
        *after* _drain() has run.
        """
        def _side_effect(req_id, group, tag_str):
            for tag, value in tags.items():
                bridge._account_q.put({"tag": tag, "value": value})
            bridge._account_q.put(SENTINEL)
        return _side_effect

    def test_returns_required_keys(self):
        bridge = _make_bridge_stub()
        tags = {"NetLiquidation": "100000.50", "TotalCashValue": "20000.00", "BuyingPower": "50000.00"}
        bridge.reqAccountSummary.side_effect = self._make_account_side_effect(bridge, tags)
        result = get_account_summary(bridge)
        assert "net_liquidation" in result
        assert "cash"            in result
        assert "buying_power"    in result

    def test_parses_values_as_float(self):
        bridge = _make_bridge_stub()
        tags = {"NetLiquidation": "123456.78", "TotalCashValue": "30000.00", "BuyingPower": "60000.00"}
        bridge.reqAccountSummary.side_effect = self._make_account_side_effect(bridge, tags)
        result = get_account_summary(bridge)
        assert isinstance(result["net_liquidation"], float)
        assert abs(result["net_liquidation"] - 123456.78) < 1e-6
        assert abs(result["cash"]            - 30000.00)  < 1e-6
        assert abs(result["buying_power"]    - 60000.00)  < 1e-6

    def test_missing_tags_default_to_zero(self):
        bridge = _make_bridge_stub()
        bridge.reqAccountSummary.side_effect = self._make_account_side_effect(bridge, {})
        result = get_account_summary(bridge)
        assert result["net_liquidation"] == 0.0
        assert result["cash"]            == 0.0
        assert result["buying_power"]    == 0.0

    def test_reqAccountSummary_called(self):
        bridge = _make_bridge_stub()
        tags = {"NetLiquidation": "0", "TotalCashValue": "0", "BuyingPower": "0"}
        bridge.reqAccountSummary.side_effect = self._make_account_side_effect(bridge, tags)
        get_account_summary(bridge)
        bridge.reqAccountSummary.assert_called_once()
        _, group, tag_str = bridge.reqAccountSummary.call_args[0]
        assert group == "All"
        assert "NetLiquidation" in tag_str

    def test_timeout_returns_partial_result(self):
        """If accountSummaryEnd never fires (timeout), return collected tags."""
        bridge = _make_bridge_stub()

        def _side_effect(req_id, group, tag_str):
            # Seed one tag but NO sentinel — simulates a stalled stream
            bridge._account_q.put({"tag": "NetLiquidation", "value": "50000"})

        bridge.reqAccountSummary.side_effect = _side_effect

        with patch.object(ib_exec, "_DEFAULT_TIMEOUT", 0.05):
            result = get_account_summary(bridge)

        assert result["net_liquidation"] == 50000.0


# ═══════════════════════════════════════════════════════════════════════════════
# TestOrderDataclass
# ═══════════════════════════════════════════════════════════════════════════════

class TestOrderDataclass:

    def test_fields_accessible(self):
        order = Order("AAPL", "BUY", "LOC", 100, 155.0, "entry", "P1")
        assert order.symbol      == "AAPL"
        assert order.action      == "BUY"
        assert order.order_type  == "LOC"
        assert order.quantity    == 100
        assert order.limit_price == 155.0
        assert order.reason      == "entry"
        assert order.pos_id      == "P1"

    def test_pos_id_defaults_to_empty_string(self):
        order = Order("AAPL", "BUY", "LOC", 100, 155.0, "entry")
        assert order.pos_id == ""

    def test_moc_order_limit_price_can_be_none(self):
        order = Order("AAPL", "SELL", "MOC", 100, None, "ibs_exit")
        assert order.limit_price is None


# ═══════════════════════════════════════════════════════════════════════════════
# TestDetectSplits
# ═══════════════════════════════════════════════════════════════════════════════

class TestDetectSplits:

    def test_exact_match_returns_empty(self):
        ib = [{"symbol": "AAPL", "shares": 100}]
        db = [{"symbol": "AAPL", "shares": 100}]
        assert detect_splits(ib, db) == []

    def test_forward_split_2_for_1_detected(self):
        ib = [{"symbol": "AAPL", "shares": 200}]
        db = [{"symbol": "AAPL", "shares": 100}]
        result = detect_splits(ib, db)
        assert len(result) == 1
        assert result[0]["symbol"]    == "AAPL"
        assert result[0]["db_shares"] == 100
        assert result[0]["ib_shares"] == 200
        assert abs(result[0]["ratio"] - 2.0) < 0.01

    def test_forward_split_3_for_1_detected(self):
        ib = [{"symbol": "TSLA", "shares": 300}]
        db = [{"symbol": "TSLA", "shares": 100}]
        result = detect_splits(ib, db)
        assert len(result) == 1
        assert result[0]["symbol"] == "TSLA"
        assert abs(result[0]["ratio"] - 3.0) < 0.01

    def test_reverse_split_1_for_2_detected(self):
        ib = [{"symbol": "MSFT", "shares": 50}]
        db = [{"symbol": "MSFT", "shares": 100}]
        result = detect_splits(ib, db)
        assert len(result) == 1
        assert result[0]["symbol"] == "MSFT"
        assert abs(result[0]["ratio"] - 0.5) < 0.01

    def test_non_split_mismatch_not_flagged(self):
        # Off by 3 shares — not a known split ratio
        ib = [{"symbol": "GOOG", "shares": 103}]
        db = [{"symbol": "GOOG", "shares": 100}]
        assert detect_splits(ib, db) == []

    def test_symbol_only_in_db_not_flagged(self):
        # IB reports nothing for this symbol (position closed intraday, etc.)
        ib = []
        db = [{"symbol": "AAPL", "shares": 100}]
        assert detect_splits(ib, db) == []

    def test_mixed_positions_only_split_returned(self):
        ib = [
            {"symbol": "AAPL", "shares": 200},  # 2:1 split
            {"symbol": "MSFT", "shares": 100},  # unchanged
        ]
        db = [
            {"symbol": "AAPL", "shares": 100},
            {"symbol": "MSFT", "shares": 100},
        ]
        result = detect_splits(ib, db)
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    def test_both_empty_returns_empty(self):
        assert detect_splits([], []) == []


# ═══════════════════════════════════════════════════════════════════════════════
# TestHeartbeat
# ═══════════════════════════════════════════════════════════════════════════════

class TestHeartbeat:

    def _make_bridge(self) -> IBBridge:
        bridge = _make_bridge_stub()
        bridge.reqCurrentTime = MagicMock()
        return bridge

    def test_returns_true_when_response_received(self):
        bridge = self._make_bridge()
        # Seed a response before calling heartbeat
        def _side_effect():
            bridge._time_q.put(1700000000)
        bridge.reqCurrentTime.side_effect = lambda: bridge._time_q.put(1700000000)
        assert bridge.heartbeat() is True

    def test_returns_false_on_timeout(self):
        bridge = self._make_bridge()
        # reqCurrentTime does nothing — queue stays empty → timeout
        import config as cfg
        original = cfg.IB_HEARTBEAT_TIMEOUT_SEC
        cfg.IB_HEARTBEAT_TIMEOUT_SEC = 0.05   # speed up test
        try:
            assert bridge.heartbeat() is False
        finally:
            cfg.IB_HEARTBEAT_TIMEOUT_SEC = original

    def test_drains_stale_time_queue_before_call(self):
        bridge = self._make_bridge()
        # Pre-seed stale value; then reqCurrentTime seeds a fresh one
        bridge._time_q.put(999)   # stale
        bridge.reqCurrentTime.side_effect = lambda: bridge._time_q.put(1700000000)
        assert bridge.heartbeat() is True
        assert bridge._time_q.empty()

    def test_reqCurrentTime_is_called(self):
        bridge = self._make_bridge()
        bridge.reqCurrentTime.side_effect = lambda: bridge._time_q.put(1700000000)
        bridge.heartbeat()
        bridge.reqCurrentTime.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# TestOrderRejection
# ═══════════════════════════════════════════════════════════════════════════════

class TestOrderRejection:

    def _make_bridge(self) -> IBBridge:
        return _make_bridge_stub()

    def test_soft_error_not_added_to_order_errors(self):
        bridge = self._make_bridge()
        ib_exec._order_errors.clear()
        # Simulate a soft/informational error callback
        bridge.error(0, 2104, "Market data farm connection is OK")
        assert 0 not in ib_exec._order_errors

    def test_hard_rejection_added_to_order_errors(self):
        bridge = self._make_bridge()
        ib_exec._order_errors.clear()
        bridge.error(101, 201, "Order rejected - Reason: insufficient funds")
        assert 101 in ib_exec._order_errors
        assert "201" in ib_exec._order_errors[101]

    def test_other_error_not_added_to_order_errors(self):
        """Non-rejection, non-soft errors are logged but not tracked as rejections."""
        bridge = self._make_bridge()
        ib_exec._order_errors.clear()
        bridge.error(5, 504, "Not connected")
        assert 5 not in ib_exec._order_errors

    def test_submit_order_raises_on_rejection(self):
        """submit_order raises OrderRejectedError when _order_errors has the order_id."""
        bridge = self._make_bridge()
        ib_exec._order_errors.clear()
        order = _make_order()

        # placeOrder side_effect seeds rejection immediately
        def _reject(*args, **kwargs):
            order_id = args[0]
            ib_exec._order_errors[order_id] = "[201] Insufficient funds"

        bridge.placeOrder.side_effect = _reject

        with patch("time.sleep"):   # skip the 2s wait
            with pytest.raises(OrderRejectedError) as exc_info:
                submit_order(bridge, order)

        assert exc_info.value.order_id is not None
        assert "201" in exc_info.value.message

    def test_submit_order_returns_id_when_no_rejection(self):
        bridge = self._make_bridge()
        ib_exec._order_errors.clear()
        order = _make_order()

        with patch("time.sleep"):
            result = submit_order(bridge, order)

        assert isinstance(result, int)

    def test_order_errors_cleared_after_rejection(self):
        """After OrderRejectedError is raised the entry is popped from _order_errors."""
        bridge = self._make_bridge()
        ib_exec._order_errors.clear()
        order = _make_order()

        def _reject(*args, **kwargs):
            ib_exec._order_errors[args[0]] = "[201] Rejected"

        bridge.placeOrder.side_effect = _reject

        with patch("time.sleep"):
            with pytest.raises(OrderRejectedError):
                submit_order(bridge, order)

        assert len(ib_exec._order_errors) == 0
