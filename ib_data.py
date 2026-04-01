"""
ib_data.py — IB intraday snapshot fetcher.

Public API
──────────
  fetch_snapshot(symbols, bridge) → dict[str, dict]

Calls reqMktData(snapshot=True) for each symbol via the provided IBBridge.
Symbols are processed in batches of _BATCH_SIZE with _INTER_BATCH_SLEEP seconds
between successive batches to avoid flooding the IB gateway.  Within each batch,
all requests are in-flight simultaneously via daemon threads.

Returns {symbol: {"open": float, "high": float, "low": float,
                  "close": float, "volume": int}}.

Symbols that time out or return no usable price are absent from the result —
callers fall back to the previous day's DB close for those symbols.

Callback patching
─────────────────
tickPrice / tickSize / tickSnapshotEnd are temporarily set as instance
attributes on the bridge for the duration of the call; they are deleted on
exit so the class-level (no-op) EWrapper defaults are restored.
"""

import contextlib
import logging
import queue
import threading
import time
from typing import Any

from ibapi.contract import Contract

from ib_exec import IBBridge, SENTINEL

logger = logging.getLogger("murphy")

_BATCH_SIZE        = 50    # symbols processed per batch (concurrently within each batch)
_INTER_BATCH_SLEEP = 1.0   # seconds to sleep between successive batches
_SNAP_TIMEOUT      = 10    # seconds to wait for tickSnapshotEnd per symbol

# IB tick-type constants used for intraday snapshot data
_TICK_OPEN       = 14   # tickPrice: today's open
_TICK_HIGH       = 6    # tickPrice: today's high
_TICK_LOW        = 7    # tickPrice: today's low
_TICK_LAST       = 4    # tickPrice: last trade price (close proxy)
_TICK_PREV_CLOSE = 9    # tickPrice: previous session's close (fallback if no LAST)
_TICK_VOLUME     = 8    # tickSize:  cumulative volume


# ═══════════════════════════════════════════════════════════════════════════════
# Callback context manager
# ═══════════════════════════════════════════════════════════════════════════════

@contextlib.contextmanager
def _snapshot_callbacks(bridge: IBBridge):
    """
    Temporarily install tick-collection callbacks as instance attributes on
    *bridge*.  Yields (snap_queues, queues_lock) for use by worker threads.

    snap_queues : dict[int, queue.Queue]   reqId → per-symbol data queue
    queues_lock : threading.Lock           protects snap_queues mutations

    On exit all three instance attributes are removed so the EWrapper
    class-level no-ops are restored.
    """
    snap_queues: dict[int, queue.Queue] = {}
    queues_lock = threading.Lock()

    def _tick_price(reqId: int, tickType: int, price: float, attrib: Any) -> None:
        with queues_lock:
            q = snap_queues.get(reqId)
        if q is not None and price > 0:
            q.put(("P", tickType, float(price)))

    def _tick_size(reqId: int, tickType: int, size: int) -> None:
        with queues_lock:
            q = snap_queues.get(reqId)
        if q is not None:
            q.put(("S", tickType, int(size)))

    def _tick_snapshot_end(reqId: int) -> None:
        with queues_lock:
            q = snap_queues.get(reqId)
        if q is not None:
            q.put(SENTINEL)

    bridge.tickPrice       = _tick_price
    bridge.tickSize        = _tick_size
    bridge.tickSnapshotEnd = _tick_snapshot_end

    try:
        yield snap_queues, queues_lock
    finally:
        for attr in ("tickPrice", "tickSize", "tickSnapshotEnd"):
            bridge.__dict__.pop(attr, None)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_snapshot(
    symbols: list[str],
    bridge: IBBridge,
) -> dict[str, dict]:
    """
    Fetch today's intraday snapshot for each symbol via reqMktData(snapshot=True).

    Symbols are processed in batches of _BATCH_SIZE.  Within each batch all
    requests run concurrently (one daemon thread per symbol).  _INTER_BATCH_SLEEP
    seconds are inserted between successive batches.

    Returns
    -------
    dict[str, dict]
        {symbol: {"open": float, "high": float, "low": float,
                  "close": float, "volume": int}}

        Symbols that timed out or returned no usable price are absent.
    """
    if not symbols:
        return {}

    results: dict[str, dict] = {}
    results_lock = threading.Lock()

    batches = [
        symbols[i: i + _BATCH_SIZE]
        for i in range(0, len(symbols), _BATCH_SIZE)
    ]

    with _snapshot_callbacks(bridge) as (snap_queues, queues_lock):

        def _fetch_one(sym: str) -> None:
            try:
                req_id = bridge.get_next_order_id()
                q: queue.Queue = queue.Queue()
                with queues_lock:
                    snap_queues[req_id] = q

                contract = Contract()
                contract.symbol   = sym
                contract.secType  = "STK"
                contract.exchange = "SMART"
                contract.currency = "USD"

                bridge.reqMktData(req_id, contract, "", True, False, [])

                snap = {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "volume": 0}

                while True:
                    try:
                        item = q.get(timeout=_SNAP_TIMEOUT)
                    except queue.Empty:
                        logger.warning(
                            "[ib_data] snapshot timeout for %s (reqId=%d)", sym, req_id
                        )
                        break
                    if item is SENTINEL:
                        break
                    kind, tick_type, val = item
                    if kind == "P":
                        if tick_type == _TICK_OPEN:
                            snap["open"] = val
                        elif tick_type == _TICK_HIGH:
                            snap["high"] = val
                        elif tick_type == _TICK_LOW:
                            snap["low"] = val
                        elif tick_type == _TICK_LAST:
                            snap["close"] = val
                        elif tick_type == _TICK_PREV_CLOSE and snap["close"] == 0.0:
                            snap["close"] = val
                    elif kind == "S" and tick_type == _TICK_VOLUME:
                        snap["volume"] = val

                bridge.cancelMktData(req_id)
                with queues_lock:
                    snap_queues.pop(req_id, None)

                if snap["close"] > 0:
                    with results_lock:
                        results[sym] = snap
                else:
                    logger.warning("[ib_data] no usable close price for %s — excluded", sym)

            except Exception as exc:
                logger.warning("[ib_data] error fetching snapshot for %s: %s", sym, exc)

        for idx, batch in enumerate(batches):
            if idx > 0:
                time.sleep(_INTER_BATCH_SLEEP)

            threads = [
                threading.Thread(
                    target=_fetch_one,
                    args=(sym,),
                    daemon=True,
                    name=f"ib-snap-{sym}",
                )
                for sym in batch
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

    logger.info(
        "[ib_data] fetch_snapshot: %d/%d symbol(s) returned usable data",
        len(results), len(symbols),
    )
    return results
