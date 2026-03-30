"""
ib_exec.py — Interactive Brokers execution bridge.

Two public objects
──────────────────
  IBCController   Thin wrapper around IBC shell commands (start/stop Gateway).
  IBBridge        EClient + EWrapper subclass. Responses synchronized via
                  queue.Queue — no time.sleep polling.

Public functions
────────────────
  submit_order(bridge, order)            → int (IB order_id)
  get_filled_orders(bridge, order_ids)   → dict[int, dict]
  get_account_summary(bridge)            → dict
  get_ib_positions(bridge)               → list[dict]
  detect_splits(ib_positions, db_positions) → list[dict]

IBBridge methods
────────────────
  heartbeat()    → bool    lightest possible connectivity check via reqCurrentTime()

Exceptions
──────────
  OrderRejectedError   raised by submit_order() when IB returns a hard rejection

Order dataclass
───────────────
  Defined here; imported by order_manager.py when that module is built.
"""

import logging
import queue
import socket
import subprocess
import threading
import time
from dataclasses import dataclass

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.execution import ExecutionFilter
from ibapi.order import Order as IBOrder
from ibapi.wrapper import EWrapper

import config

logger = logging.getLogger("murphy")

# ── Sentinel: marks the end of a streaming response on a queue ────────────────
_SENTINEL = object()

# ── Account tags requested from IB ───────────────────────────────────────────
_ACCOUNT_TAGS = "NetLiquidation,TotalCashValue,BuyingPower"

# ── Default timeout for blocking queue reads (seconds) ───────────────────────
_DEFAULT_TIMEOUT = 15

# ── Known stock-split ratios (ib_shares / db_shares) checked with 1% tolerance
_SPLIT_RATIOS    = (2.0, 3.0, 0.5, 1 / 3)
_SPLIT_TOLERANCE = 0.01

# ── Order rejection tracking (populated by error() callback) ─────────────────
# Maps IB order_id → error_message string for hard rejections only.
# submit_order() pops entries after checking; no unbounded growth.
_order_errors: dict[int, str] = {}


# ═══════════════════════════════════════════════════════════════════════════════
# Exceptions
# ═══════════════════════════════════════════════════════════════════════════════

class OrderRejectedError(Exception):
    """Raised by submit_order() when IB returns a hard rejection for an order."""

    def __init__(self, order_id: int, message: str) -> None:
        self.order_id = order_id
        self.message  = message
        super().__init__(f"Order {order_id} rejected by IB: {message}")


# ═══════════════════════════════════════════════════════════════════════════════
# Order dataclass  (imported by order_manager.py)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Order:
    """
    Internal order representation — converted to an ibapi IBOrder in submit_order().

    Fields
    ──────
    symbol      : ticker (e.g. "AAPL")
    action      : "BUY" or "SELL"
    order_type  : "LOC" or "MOC"
    quantity    : number of shares (positive integer)
    limit_price : LOC limit price (ignored for MOC — set to None)
    reason      : human-readable entry/exit reason for logging
    pos_id      : links back to the positions table row
    """
    symbol:      str
    action:      str
    order_type:  str
    quantity:    int
    limit_price: float | None
    reason:      str
    pos_id:      str = ""


# ═══════════════════════════════════════════════════════════════════════════════
# IBCController
# ═══════════════════════════════════════════════════════════════════════════════

class IBCController:
    """
    Thin wrapper around IBC shell commands.

    IBC is assumed to be installed at config.IBC_PATH (e.g. /opt/ibc/ibc.sh).
    The script is expected to accept "stop" and "start" as its first positional
    argument — adjust if your IBC installation uses a different interface.
    """

    def stop_gateway(self) -> None:
        """Send a graceful stop command to IBC / IB Gateway."""
        logger.info("[ibc] Stopping Gateway via IBC.")
        subprocess.run(
            [config.IBC_PATH, "stop"],
            check=False,
            capture_output=True,
            text=True,
        )

    def start_gateway(self) -> None:
        """Launch IB Gateway through IBC (handles 2FA via TwsLoginMode in config.ini)."""
        logger.info("[ibc] Starting Gateway via IBC: %s", config.IBC_PATH)
        subprocess.run(
            [
                config.IBC_PATH,
                "start",
                "--tws-path",     config.IBC_TWS_PATH,
                "--ibc-ini",      config.IBC_CONFIG_PATH,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def wait_for_api(self, timeout: int = 120) -> bool:
        """
        Poll config.IB_HOST:config.IB_PORT every 2 seconds until the TCP port
        accepts connections or *timeout* seconds have elapsed.

        Returns True when the port is open, False on timeout.
        No time.sleep polling — each iteration is a blocking socket probe,
        the 2-second gap is intentional backoff (not a spin-wait).
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                result = sock.connect_ex((config.IB_HOST, config.IB_PORT))
            if result == 0:
                logger.info("[ibc] API port %s:%d is open.", config.IB_HOST, config.IB_PORT)
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(2, remaining))
        logger.warning(
            "[ibc] wait_for_api timed out after %d s (port %s:%d still closed).",
            timeout, config.IB_HOST, config.IB_PORT,
        )
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# IBBridge  (EClient + EWrapper)
# ═══════════════════════════════════════════════════════════════════════════════

class IBBridge(EWrapper, EClient):
    """
    Unified IB connection object.

    Threading model
    ───────────────
    EClient.run() is called in a daemon thread started by connect().
    All EWrapper callbacks execute on that thread and communicate back
    to the main thread exclusively via queue.Queue — no shared mutable
    state, no polling, no time.sleep waits (except the 3 s backoff in
    reconnect() which is intentional delay, not a poll loop).
    """

    def __init__(self) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        # Next valid order ID — set once on connect, incremented locally.
        self._next_order_id: int = 0
        self._order_id_lock: threading.Lock = threading.Lock()
        self._order_id_q:    queue.Queue = queue.Queue()

        # Per-request response queues.
        self._account_q:   queue.Queue = queue.Queue()
        self._exec_q:      queue.Queue = queue.Queue()
        self._position_q:  queue.Queue = queue.Queue()
        self._time_q:      queue.Queue = queue.Queue()

        self._thread: threading.Thread | None = None

    # ── EWrapper callbacks ────────────────────────────────────────────────────

    def nextValidId(self, orderId: int) -> None:
        """Fires once after connect; signals that the API link is live."""
        self._order_id_q.put(orderId)

    def accountSummary(
        self, reqId: int, account: str, tag: str, value: str, currency: str
    ) -> None:
        self._account_q.put({"tag": tag, "value": value})

    def accountSummaryEnd(self, reqId: int) -> None:
        self._account_q.put(_SENTINEL)

    def execDetails(
        self, reqId: int, contract: Contract, execution
    ) -> None:
        self._exec_q.put({
            "order_id":   execution.orderId,
            "fill_price": float(execution.avgPrice),
            "fill_qty":   int(execution.cumQty),
            "status":     "Filled",
        })

    def execDetailsEnd(self, reqId: int) -> None:
        self._exec_q.put(_SENTINEL)

    def position(
        self, account: str, contract: Contract, pos: float, avgCost: float
    ) -> None:
        if contract.secType == "STK":
            self._position_q.put({
                "symbol":   contract.symbol,
                "shares":   int(pos),
                "avg_cost": float(avgCost),
            })

    def positionEnd(self) -> None:
        self._position_q.put(_SENTINEL)

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        logger.debug(
            "[ib] orderStatus id=%d status=%s filled=%.0f avgPrice=%.4f",
            orderId, status, filled, avgFillPrice,
        )

    def currentTime(self, time_val: int) -> None:
        self._time_q.put(time_val)

    def error(self, reqId: int, errorCode: int, errorString: str) -> None:
        if errorCode in frozenset(config.IB_SOFT_ERROR_CODES):
            logger.debug("[ib] info code=%d: %s", errorCode, errorString)
        elif errorCode in frozenset(config.IB_REJECTION_CODES):
            logger.warning(
                "[ib] order rejection reqId=%d code=%d: %s", reqId, errorCode, errorString
            )
            _order_errors[reqId] = f"[{errorCode}] {errorString}"
        else:
            logger.warning("[ib] error reqId=%d code=%d: %s", reqId, errorCode, errorString)

    # ── Connection management ─────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Open connection to TWS/Gateway and start the EClient message loop in a
        daemon thread. Blocks until nextValidId fires (confirms the API is live).
        """
        EClient.connect(self, config.IB_HOST, config.IB_PORT, config.IB_CLIENT_ID)
        self._thread = threading.Thread(target=self.run, daemon=True, name="ib-run")
        self._thread.start()
        # Block until IB acknowledges the connection
        self._next_order_id = self._order_id_q.get(timeout=_DEFAULT_TIMEOUT)
        logger.info("[ib] Connected to %s:%d. Next order id: %d",
                    config.IB_HOST, config.IB_PORT, self._next_order_id)

    def disconnect(self) -> None:
        EClient.disconnect(self)
        logger.info("[ib] Disconnected.")

    def is_connected(self) -> bool:
        return self.isConnected()

    def reconnect(self) -> None:
        """Disconnect, wait 3 seconds, then reconnect."""
        self.disconnect()
        time.sleep(3)
        self.connect()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_next_order_id(self) -> int:
        """Return the current next-valid order ID and increment the local counter."""
        with self._order_id_lock:
            oid = self._next_order_id
            self._next_order_id += 1
        return oid

    def heartbeat(self) -> bool:
        """
        Confirm the TWS/Gateway connection is alive via the lightest possible
        API call: reqCurrentTime() → currentTime() callback.

        Returns True when the response arrives within config.IB_HEARTBEAT_TIMEOUT_SEC,
        False on timeout.
        """
        self._drain(self._time_q)
        self.reqCurrentTime()
        try:
            self._time_q.get(timeout=config.IB_HEARTBEAT_TIMEOUT_SEC)
            logger.debug("[ib] heartbeat: OK")
            return True
        except queue.Empty:
            logger.warning(
                "[ib] heartbeat: no response within %ds", config.IB_HEARTBEAT_TIMEOUT_SEC
            )
            return False

    @staticmethod
    def _drain(q: queue.Queue) -> None:
        """Discard any stale items left on *q* from a previous request."""
        while not q.empty():
            try:
                q.get_nowait()
            except queue.Empty:
                break


# ═══════════════════════════════════════════════════════════════════════════════
# Public functions
# ═══════════════════════════════════════════════════════════════════════════════

def submit_order(bridge: IBBridge, order: Order) -> int:
    """
    Submit a LOC or MOC order to IB.

    LOC entry  — orderType="LMT", tif="LOC", lmtPrice=order.limit_price
    MOC exit   — orderType="MOC"

    Returns the IB order_id used for the submission.
    """
    contract = Contract()
    contract.symbol   = order.symbol
    contract.secType  = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    ib_order = IBOrder()
    ib_order.action        = order.action
    ib_order.totalQuantity = order.quantity

    if order.order_type == "LOC":
        ib_order.orderType = "LMT"
        ib_order.tif       = "LOC"
        ib_order.lmtPrice  = order.limit_price
    else:  # MOC
        ib_order.orderType = "MOC"

    if config.IB_SUBACCOUNT:
        ib_order.account = config.IB_SUBACCOUNT

    order_id = bridge._get_next_order_id()
    bridge.placeOrder(order_id, contract, ib_order)

    logger.info(
        "[ib] submit_order id=%d %s %s qty=%d type=%s%s reason=%s",
        order_id, order.action, order.symbol, order.quantity, order.order_type,
        f" lmt={order.limit_price}" if order.order_type == "LOC" else "",
        order.reason,
    )

    # Give IB 2 seconds to fire a rejection callback before returning
    time.sleep(2)
    rejection_msg = _order_errors.pop(order_id, None)
    if rejection_msg is not None:
        raise OrderRejectedError(order_id, rejection_msg)

    return order_id


def get_filled_orders(
    bridge: IBBridge,
    order_ids: list[int],
) -> dict[int, dict]:
    """
    Request execution reports from IB and return fills for the given order IDs.

    Calls reqExecutions() which triggers execDetails() callbacks followed by
    execDetailsEnd(). Blocks on queue.Queue.get() — no polling.

    Returns {order_id: {"fill_price": float, "fill_qty": int, "status": str}}
    Only order IDs present in *order_ids* are included; pass an empty list to
    return all executions from the session.
    """
    bridge._drain(bridge._exec_q)

    req_id = bridge._get_next_order_id()
    bridge.reqExecutions(req_id, ExecutionFilter())

    results: dict[int, dict] = {}
    filter_set = set(order_ids)

    while True:
        try:
            item = bridge._exec_q.get(timeout=_DEFAULT_TIMEOUT)
        except queue.Empty:
            logger.warning("[ib] get_filled_orders timed out waiting for execDetailsEnd")
            break
        if item is _SENTINEL:
            break
        oid = item["order_id"]
        if not filter_set or oid in filter_set:
            results[oid] = {
                "fill_price": item["fill_price"],
                "fill_qty":   item["fill_qty"],
                "status":     item["status"],
            }

    logger.info("[ib] get_filled_orders: %d fill(s) for %d requested id(s)",
                len(results), len(order_ids))
    return results


def get_account_summary(bridge: IBBridge) -> dict:
    """
    Request account summary from IB and return key equity metrics.

    Calls reqAccountSummary() which triggers accountSummary() callbacks
    followed by accountSummaryEnd(). Blocks on queue.Queue.get() — no polling.

    Returns {"net_liquidation": float, "cash": float, "buying_power": float}
    """
    bridge._drain(bridge._account_q)

    req_id  = bridge._get_next_order_id()
    account = config.IB_SUBACCOUNT if config.IB_SUBACCOUNT else "All"
    bridge.reqAccountSummary(req_id, account, _ACCOUNT_TAGS)

    raw: dict[str, str] = {}
    while True:
        try:
            item = bridge._account_q.get(timeout=_DEFAULT_TIMEOUT)
        except queue.Empty:
            logger.warning("[ib] get_account_summary timed out waiting for accountSummaryEnd")
            break
        if item is _SENTINEL:
            break
        raw[item["tag"]] = item["value"]

    result = {
        "net_liquidation": float(raw.get("NetLiquidation", 0)),
        "cash":            float(raw.get("TotalCashValue",  0)),
        "buying_power":    float(raw.get("BuyingPower",     0)),
    }
    logger.info("[ib] account summary: nlv=%.2f cash=%.2f bp=%.2f",
                result["net_liquidation"], result["cash"], result["buying_power"])
    return result


def get_ib_positions(bridge: IBBridge) -> list[dict]:
    """
    Request current open positions from IB via reqPositions().

    Triggers position() callbacks followed by positionEnd(). Blocks on
    queue.Queue.get() — no polling.

    Returns list of {"symbol": str, "shares": int, "avg_cost": float}.
    Only STK (equity) positions are included; other asset classes are filtered
    inside the position() callback.
    """
    bridge._drain(bridge._position_q)
    bridge.reqPositions()

    results: list[dict] = []
    while True:
        try:
            item = bridge._position_q.get(timeout=_DEFAULT_TIMEOUT)
        except queue.Empty:
            logger.warning("[ib] get_ib_positions timed out waiting for positionEnd")
            break
        if item is _SENTINEL:
            break
        results.append(item)

    logger.info("[ib] get_ib_positions: %d equity position(s)", len(results))
    return results


def detect_splits(
    ib_positions: list[dict],
    db_positions: list[dict],
) -> list[dict]:
    """
    Compare IB-reported share counts against DB records for each symbol.

    A split is detected when the ratio ``ib_shares / db_shares`` is within
    ``_SPLIT_TOLERANCE`` (1%) of a known split ratio (2:1, 3:1, 1:2, 1:3).

    Parameters
    ----------
    ib_positions : list[dict]
        Each dict must have keys ``symbol`` (str) and ``shares`` (int).
        Typically the output of get_ib_positions().
    db_positions : list[dict]
        Each dict must have keys ``symbol`` (str) and ``shares`` (int).
        Typically the output of portfolio_state.load_positions().

    Returns
    -------
    list[dict]
        One entry per detected split: ``{"symbol", "db_shares", "ib_shares", "ratio"}``.
        Empty list when all share counts match or no known ratio matches.
    """
    db_map = {p["symbol"]: int(p["shares"]) for p in db_positions}
    splits: list[dict] = []

    for pos in ib_positions:
        sym       = pos["symbol"]
        ib_shares = int(pos["shares"])
        db_shares = db_map.get(sym)

        if db_shares is None or ib_shares == db_shares:
            continue

        ratio = ib_shares / db_shares
        for known in _SPLIT_RATIOS:
            if abs(ratio - known) / known <= _SPLIT_TOLERANCE:
                splits.append({
                    "symbol":    sym,
                    "db_shares": db_shares,
                    "ib_shares": ib_shares,
                    "ratio":     ratio,
                })
                break

    return splits
