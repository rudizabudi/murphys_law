"""
signals.py — Signal generator.

Direct port of the entry/exit logic from reference/v300.py:
  get_entry_signals() — port of collect_signals()
  get_exit_signals()  — port of the exit block inside simulate_portfolio()

All thresholds read from config — no hardcoded values.
"""

import logging
from datetime import date

import pandas as pd

import config

logger = logging.getLogger("murphy")


def get_entry_signals(
    loaded_data: dict[str, pd.DataFrame],
    as_of_date: pd.Timestamp,
) -> list[dict]:
    """
    Evaluate price-sensitive entry conditions on the as_of_date bar
    for all symbols in loaded_data (typically watchlist symbols only).

    Assumes indicator columns are already present in each DataFrame
    (compute_indicators() was called upstream before this function).

    Portfolio gating (slot count, notional cap, liquidity gate) is NOT
    done here — that is order_manager.py's responsibility.

    Returns a list of signal dicts, one per qualifying symbol:
      symbol, bar_time, fill_price, n_day_ret, q_threshold, ibs_entry, adv63
    """
    signals: list[dict] = []

    for sym, df in loaded_data.items():
        if as_of_date not in df.index:
            continue

        row = df.loc[as_of_date]

        # ── Entry conditions (direct port of collect_signals mask) ────────────
        if not row.get("qpi_signal", False):
            continue
        if pd.isna(row.get("sma200")) or row["close"] <= row["sma200"]:
            continue
        if row["ibs"] >= config.IBS_ENTRY_FILTER:
            continue
        if pd.isna(row.get("adv63")) or row["adv63"] <= 0:
            continue
        if row["close"] <= 0:
            continue
        if pd.isna(row.get("n_day_ret")):
            continue

        signals.append({
            "symbol":      sym,
            "bar_time":    as_of_date,
            "fill_price":  float(row["close"]),
            "n_day_ret":   float(row["n_day_ret"]),
            "q_threshold": (
                float(row["q_threshold"])
                if pd.notna(row.get("q_threshold"))
                else None
            ),
            "ibs_entry":   float(row["ibs"]),
            "adv63":       float(row["adv63"]),
        })

    return signals


def get_exit_signals(
    open_positions: list[dict],
    loaded_data: dict[str, pd.DataFrame],
    as_of_date: pd.Timestamp,
) -> list[dict]:
    """
    Evaluate all exit conditions for each open position on the as_of_date bar.

    Side effect: modifies bars_held and consec_lows in-place on EVERY position
    dict (including those that are not exiting) so the caller can persist the
    updated state without a second pass.

    Returns only positions that should be exited, each augmented with an
    'exit_reason' field.

    Exit priority (first match wins — identical to backtest):
      deterioration_stop → ibs_exit → rsi2_exit → time_stop
    """
    as_of_d = _to_date(as_of_date)
    exits: list[dict] = []

    for pos in open_positions:

        # ── Never exit on the entry bar itself ────────────────────────────────
        # Mirrors backtest: if pos["fill_time"] == bar_time: continue
        entry_d = _to_date(pos.get("entry_date"))
        if entry_d is not None and entry_d == as_of_d:
            continue

        # ── Increment bars_held ───────────────────────────────────────────────
        pos["bars_held"] = pos.get("bars_held", 0) + 1

        # ── Retrieve today's bar; keep position if data is missing ───────────
        sym = pos["symbol"]
        df  = loaded_data.get(sym)
        if df is None or as_of_date not in df.index:
            continue

        row        = df.loc[as_of_date]
        exit_close = float(row["close"])
        ibs_val    = float(row["ibs"])  if pd.notna(row.get("ibs"))  else 0.5
        rsi2_val   = float(row["rsi2"]) if pd.notna(row.get("rsi2")) else 50.0
        fill_price = float(pos["fill_price"])

        # ── Deterioration stop (v3) ───────────────────────────────────────────
        # Counter updates only after the grace period (bars_held >= STOP_MIN_BARS).
        # A close at or above fill_price resets the counter.
        # Note: with default config (STOP_MIN_BARS == MAX_HOLDING_PERIOD == 15)
        # this code path never trips (confirmed n=0 in backtest v3.0.0).
        # Carried as-is to preserve exact backtest parity.
        if pos["bars_held"] >= config.STOP_MIN_BARS:
            if exit_close < fill_price:
                pos["consec_lows"] = pos.get("consec_lows", 0) + 1
            else:
                pos["consec_lows"] = 0

        # ── Exit priority: first match wins ───────────────────────────────────
        exit_reason: str | None = None

        if pos.get("consec_lows", 0) >= config.STOP_CONSEC_LOWS:
            exit_reason = "deterioration_stop"
        elif ibs_val > config.IBS_EXIT_FILTER:
            exit_reason = "ibs_exit"
        elif rsi2_val > config.RSI_EXIT_FILTER:
            exit_reason = "rsi2_exit"
        elif (
            config.MAX_HOLDING_PERIOD is not None
            and pos["bars_held"] >= config.MAX_HOLDING_PERIOD
        ):
            exit_reason = "time_stop"

        if exit_reason:
            exiting = dict(pos)          # snapshot with updated bars_held/consec_lows
            exiting["exit_reason"] = exit_reason
            exits.append(exiting)

    return exits


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_date(value) -> date | None:
    """Coerce a date-like value to datetime.date, or return None."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, type(pd.Timestamp("today"))):
        return value
    if hasattr(value, "date"):          # pd.Timestamp, datetime
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None
