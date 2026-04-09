"""
td_data.py — TwelveData daily bar integration.

Public API
──────────
  fetch_bars(symbol, n_days)               → list[dict] | None  single-symbol fetch
  fetch_incremental(symbols, n_days=None)  → int   last N days for each symbol
  fetch_full_history(symbols, n_days=None) → int   full history for new symbols

fetch_bars() is rate-limited to config.TWELVEDATA_RATE_LIMIT_PER_MIN requests
per minute (default 8, matching the free-tier cap of 8 req/min).  A sleep is
inserted only when the time since the last request is less than the required
interval — no unnecessary sleeping when the fetch itself takes longer.

fetch_incremental() and fetch_full_history() batch multiple symbols into a
single HTTP request (_BATCH_SIZE = 55) and use _INTER_BATCH_DELAY between
successive batches; they are intended for use when the account has a paid plan.

TwelveData response format
──────────────────────────
Single symbol  → {"meta": {...}, "values": [{datetime, open, high, low, close, volume}, ...], "status": "ok"}
Multiple syms  → {"AAPL": {"meta": ..., "values": [...], "status": "ok"}, "MSFT": {...}}
Error          → {"code": 400, "message": "...", "status": "error"}
"""

import logging
import time

import httpx

import config
import db

logger = logging.getLogger("murphy")

_TD_BASE_URL = "https://api.twelvedata.com/time_series"


def _inter_batch_delay() -> float:
    """
    Compute the inter-batch sleep duration from current config values.

    Each symbol in a batch consumes one API credit; the delay ensures the
    per-minute credit consumption stays within TWELVEDATA_RATE_LIMIT_PER_MIN.
    Evaluated at call time so monkeypatching config in tests is reflected
    immediately.
    """
    return (config.TWELVEDATA_BATCH_SIZE / config.TWELVEDATA_RATE_LIMIT_PER_MIN) * 60

# Rate-limiter state for fetch_bars() (single-symbol path)
_last_request_time: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════════

_RATE_LIMIT_RETRY_DELAY = 60  # seconds to wait before retrying a rate-limited batch


def _is_rate_limit_error(data: dict) -> bool:
    """Return True if the response indicates API credit exhaustion."""
    return data.get("code") == 429 or "run out of API credits" in str(
        data.get("message", "")
    )


def _parse_batch_response(symbols: list[str], data: dict) -> dict[str, list[dict]]:
    """Extract per-symbol value lists from a successful TwelveData response dict."""
    # Single-symbol response has "values" at top level
    if "values" in data:
        return {symbols[0]: data["values"]}

    # Multi-symbol response: top-level keys are ticker symbols
    result: dict[str, list[dict]] = {}
    for sym in symbols:
        sym_data = data.get(sym, {})
        if not isinstance(sym_data, dict):
            continue
        if sym_data.get("status") == "error":
            logger.warning("[td_data] %s: %s", sym, sym_data.get("message", "unknown error"))
            continue
        if "values" in sym_data:
            result[sym] = sym_data["values"]
    return result


def _fetch_batch(symbols: list[str], outputsize: int) -> dict[str, list[dict]]:
    """
    Fetch daily bars for a batch of symbols from the TwelveData /time_series
    endpoint in a single HTTP request.

    Returns {symbol: [raw_value_dict, ...]} for successfully fetched symbols.
    Symbols with an error status or missing from the response are omitted.

    Rate-limit handling: if the response indicates API credit exhaustion (code
    429 or message containing 'run out of API credits'), sleep
    _RATE_LIMIT_RETRY_DELAY seconds and retry once.  If the retry also fails,
    log the affected symbols and return {}.
    """
    params = {
        "symbol":     ",".join(symbols),
        "interval":   "1day",
        "outputsize": outputsize,
        "apikey":     config.TWELVEDATA_API_KEY,
        "format":     "JSON",
    }

    for attempt in range(2):
        try:
            resp = httpx.get(_TD_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning(
                "[td_data] HTTP request failed for batch starting %s: %s",
                symbols[0], exc,
            )
            return {}

        data = resp.json()

        # Rate-limit: sleep and retry once
        if _is_rate_limit_error(data):
            if attempt == 0:
                logger.warning(
                    "[td_data] rate limit hit, waiting 60s before retry"
                )
                time.sleep(_RATE_LIMIT_RETRY_DELAY)
                continue
            else:
                logger.warning(
                    "[td_data] rate limit retry failed for symbols: %s",
                    symbols,
                )
                return {}

        # Other top-level API error
        if data.get("status") == "error" or "code" in data:
            logger.warning(
                "[td_data] API error for batch starting %s: %s",
                symbols[0], data.get("message", data),
            )
            return {}

        return _parse_batch_response(symbols, data)

    return {}  # unreachable, but satisfies type checker


def _parse_rows(symbol: str, values: list[dict]) -> list[dict]:
    """
    Convert a list of TwelveData value dicts to daily_bars row dicts.
    Rows with missing or unparseable fields are skipped with a warning.
    """
    rows: list[dict] = []
    for v in values:
        try:
            rows.append({
                "symbol": symbol,
                "date":   str(v["datetime"])[:10],   # strip time component if present
                "open":   float(v["open"]),
                "high":   float(v["high"]),
                "low":    float(v["low"]),
                "close":  float(v["close"]),
                "volume": float(v["volume"]),
            })
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("[td_data] parse error for %s row %s: %s", symbol, v, exc)
    return rows


def _fetch_and_upsert(symbols: list[str], outputsize: int) -> int:
    """
    Fetch bars for all symbols in batches and upsert into daily_bars.
    Returns total rows upserted across all batches.
    """
    if not symbols:
        return 0

    total = 0
    batch_size = config.TWELVEDATA_BATCH_SIZE
    batches = [
        symbols[i: i + batch_size]
        for i in range(0, len(symbols), batch_size)
    ]
    n_batches = len(batches)

    for idx, batch in enumerate(batches):
        if idx > 0:
            time.sleep(_inter_batch_delay())

        fetched = _fetch_batch(batch, outputsize)
        if not fetched:
            logger.warning(
                "[td_data] batch %d/%d: no data returned for %s…",
                idx + 1, n_batches, batch[0],
            )
            continue

        rows: list[dict] = []
        for sym, values in fetched.items():
            rows.extend(_parse_rows(sym, values))

        if rows:
            n = db.upsert_daily_bars(rows)
            total += n
            logger.info(
                "[td_data] batch %d/%d: upserted %d row(s) for %d symbol(s)",
                idx + 1, n_batches, n, len(fetched),
            )
        else:
            logger.warning(
                "[td_data] batch %d/%d: fetched %d symbol(s) but parsed 0 rows",
                idx + 1, n_batches, len(fetched),
            )

    return total


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_bars(symbol: str, n_days: int) -> list[dict] | None:
    """
    Fetch the last *n_days* daily bars for a single symbol from TwelveData.

    Applies a rate limit of config.TWELVEDATA_RATE_LIMIT_PER_MIN requests per
    minute.  A sleep is inserted only when the elapsed time since the previous
    request is less than the required interval.

    Returns a list of OHLCV dicts (keys: symbol, date, open, high, low, close,
    volume) ready for db.upsert_daily_bars(), or None on any error.
    """
    global _last_request_time

    min_interval = 60.0 / config.TWELVEDATA_RATE_LIMIT_PER_MIN
    elapsed = time.time() - _last_request_time
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)

    fetched = _fetch_batch([symbol], outputsize=n_days)
    _last_request_time = time.time()

    if symbol not in fetched:
        return None
    rows = _parse_rows(symbol, fetched[symbol])
    return rows if rows else None


def fetch_incremental(
    symbols: list[str],
    n_days: int | None = None,
) -> int:
    """
    Fetch the last *n_days* (default: config.TWELVEDATA_INCREMENTAL_DAYS) of
    daily bars for each symbol and upsert into daily_bars.

    Used by nightly_sync() for routine incremental updates.
    Returns total rows upserted.
    """
    if n_days is None:
        n_days = config.TWELVEDATA_INCREMENTAL_DAYS
    logger.info(
        "[td_data] fetch_incremental: %d symbol(s), outputsize=%d",
        len(symbols), n_days,
    )
    return _fetch_and_upsert(symbols, outputsize=n_days)


def fetch_full_history(
    symbols: list[str],
    n_days: int | None = None,
) -> int:
    """
    Fetch *n_days* (default: config.TWELVEDATA_HISTORY_DAYS) of daily bars for
    each symbol and upsert into daily_bars.

    Used for newly-added symbols that have fewer than MIN_BARS_REQUIRED bars.
    Returns total rows upserted.
    """
    if n_days is None:
        n_days = config.TWELVEDATA_HISTORY_DAYS
    logger.info(
        "[td_data] fetch_full_history: %d symbol(s), outputsize=%d",
        len(symbols), n_days,
    )
    return _fetch_and_upsert(symbols, outputsize=n_days)
