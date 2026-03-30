#!/usr/bin/env python3
"""
murphy_law_backtest.py — Murphy's Law Mean Reversion Backtest v1.0.0
─────────────────────────────────────────────────────────────────────
Strategy : Long-only mean reversion on S&P 500 constituents (daily bars).
           Based on Quantitativo "Murphy's Law" (Quant Trading Rules, Dec 2025).

Entry conditions (filled at bar close — zero look-ahead):
  1. QPI < ENTRY_TRIGGER          N-day return ranks below ENTRY_TRIGGER-th
                                   quantile of own rolling RETURN_RANK_RANGE history
  2. Close > SMA(200)              uptrend filter
  3. IBS < IBS_ENTRY_FILTER        closed in the bottom fraction of intraday range

Exit conditions (checked at subsequent bar closes — first trigger wins):
  1. IBS > IBS_EXIT_FILTER         closed near intraday high → reversion complete
  2. RSI(2) > RSI_EXIT_FILTER      extremely short-term overbought
  3. bars_held >= MAX_HOLDING_PERIOD  time stop (None = disabled)

QPI (Quantile Position Index):
  Implemented as: n_day_ret[t] < quantile(past RETURN_RANK_RANGE bars, ENTRY_TRIGGER)
  The past-only window is enforced via shift(1) before rolling — no look-ahead.

Position sizing (adapted from PC blueprint fixed-fractional logic):
  target_shares     = floor((equity / MAX_POSITIONS) / fill_price)
  max_shares_cap    = floor((equity * MAX_NOTIONAL) / fill_price)
  shares = min(target, cap)  — capped at MAX_NOTIONAL as a safety valve
  Gate: new_notional + open_notional ≤ equity × MAX_TOTAL_NOTIONAL
  Gate: new_notional ≤ 63-day avg dollar vol × LIQUIDITY_ADV_MAX_PCT

When > MAX_POSITIONS candidates fire on the same bar, rank by RANK_BY.

Costs:  ROUND_TRIP_COST_BPS applied proportionally to entry notional at exit.
Equity: mark-to-market (unrealised gains/losses reflected in daily equity curve).

Data format: SYMBOL_1D.json — list of dicts with keys:
  "datetime" (or "timestamp"), "open", "high", "low", "close", "volume"
"""

import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

version = "3.0.0"

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
# +43,28% -20,2%
DATA_DIR    = Path(__file__).resolve().parent.parent / "data_old"
RESULTS_DIR = Path(__file__).resolve().parent / "results"

BALANCE_START = 100_000.0

# ── Backtest window ────────────────────────────────────────────────────────────
START_DATE: str = "2020/03/01"   # yyyy/mm/dd  (indicators warm up on all prior bars)

# ── Universe ───────────────────────────────────────────────────────────────────
WHITELIST: list[str] = []   # empty = all symbols in DATA_DIR
BLACKLIST: list[str] = []
#   'QQQ', 'IWM', 'XLF', 'XLK', 'XLV', 'XLY', 'XLC', 'XLI',
#   'XLP', 'XLE', 'XLRE', 'RSP', 'GLD', 'IBIT', 'USO', 'TLT', 'SPY'
#]

# ── Strategy parameters ────────────────────────────────────────────────────────
ENTRY_N_DAY_RETURN: int   = 3      # N in "N-day return" for QPI signal
RETURN_RANK_RANGE:  int   = 252    # rolling lookback for QPI distribution (~1 yr)
ENTRY_TRIGGER:      float = 0.1   # enter when QPI < this (bottom 30th percentile)
IBS_ENTRY_FILTER:   float = 0.2   # enter only when IBS < this (bottom 10% of range)
IBS_EXIT_FILTER:    float = 0.90   # exit when IBS > this (top 10% of range)
RSI_EXIT_FILTER:    float = 90.0   # exit when RSI(2) > this
MAX_HOLDING_PERIOD: int | None = 15  # hard backstop in bars (safety net); None = disabled

# ── Deterioration stop (v3) ────────────────────────────────────────────────────
# Exit when the position has been open ≥ STOP_MIN_BARS AND has printed
# STOP_CONSEC_LOWS consecutive closes strictly below the entry close.
# Grace period prevents premature exits in the normal early dip.
# Reset on any close at/above entry so a genuine bounce clears the counter.
STOP_MIN_BARS:    int = 15    # bars held before deterioration check activates
STOP_CONSEC_LOWS: int = 9     # consecutive closes < entry_close → exit

# ── Trend filter ───────────────────────────────────────────────────────────────
SMA_PERIOD: int = 200

# ── Position sizing ────────────────────────────────────────────────────────────
MAX_POSITIONS:       int   = 15     #20 max simultaneous open positions
MAX_TOTAL_NOTIONAL:  float = 1.5    #2 total notional budget as × equity  (1.0=unleveraged,
                                    # 1.5=50% leverage).  Per-slot target =
                                    # equity × MAX_TOTAL_NOTIONAL / MAX_POSITIONS
MAX_NOTIONAL:        float = 0.1    #0.1 hard cap on a single position (safety valve);
                                    # only bites when MAX_TOTAL_NOTIONAL / MAX_POSITIONS
                                    # exceeds this (e.g. MAX_TOTAL_NOTIONAL>1.5, 10 slots)
RANK_BY:             str   = "qpi"  # "qpi" (lowest return) | "ibs" (lowest IBS)
                                    # used when > MAX_POSITIONS candidates on same bar

# ── Liquidity filter ───────────────────────────────────────────────────────────
LIQUIDITY_ADV_WINDOW:  int   = 63
LIQUIDITY_ADV_MAX_PCT: float = 0.05   # max notional as fraction of 63-day dollar ADV

# ── Trading costs ──────────────────────────────────────────────────────────────
ROUND_TRIP_COST_BPS: float = 2.0   # round-trip bps, applied to entry notional at exit

# ── Data integrity ─────────────────────────────────────────────────────────────
SPLIT_DROP_THRESHOLD:  float = -0.50
CRASH_QUORUM_DROP:     float = 0.15
CRASH_QUORUM_FRACTION: float = 0.30

# Minimum bars per symbol before it can be included (partial QPI warmup allowed)
MIN_BARS_REQUIRED: int = (
    max(SMA_PERIOD, RETURN_RANK_RANGE // 4, LIQUIDITY_ADV_WINDOW)
    + ENTRY_N_DAY_RETURN + 30
)

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING & INTEGRITY
# ═══════════════════════════════════════════════════════════════════════════════

def load_symbol_file_5min(path: Path) -> pd.DataFrame | None:
    """
    Load a 5-min intraday OHLCV JSON.
    Accepts 'datetime' or 'timestamp' as the time-column key.
    Filters to regular market hours (09:30–16:00) before returning.
    """
    try:
        with open(path) as f:
            records = json.load(f)
        df = pd.DataFrame(records)
        tc = "datetime" if "datetime" in df.columns else "timestamp"
        df[tc] = pd.to_datetime(df[tc])
        df = df.set_index(tc).sort_index()
        df = df[["open", "high", "low", "close", "volume"]].astype(float)
        df = df[~df.index.duplicated(keep="first")]
        df = df.between_time("09:30", "16:00")
        return df
    except Exception as e:
        print(f"  [load] {path.name}: {e}")
        return None


def resample_to_daily(df_5min: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate 5-min bars to business-day OHLCV.
    open  = first bar of session
    high  = intraday high
    low   = intraday low
    close = last bar of session
    volume = sum of all bars
    Empty days (holidays/gaps) are dropped.
    """
    daily = (
        df_5min
        .resample("B", label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min",
              "close": "last", "volume": "sum"})
        .dropna(subset=["close"])
    )
    # Drop days where open is NaN (resampler created an empty bucket)
    daily = daily[daily["open"].notna()]
    return daily


def find_systemic_crash_dates(all_closes: dict[str, pd.Series]) -> set:
    """Dates where ≥ CRASH_QUORUM_FRACTION of universe fell ≥ CRASH_QUORUM_DROP."""
    if not all_closes:
        return set()
    rets = pd.DataFrame(all_closes).pct_change()
    frac = (rets < -CRASH_QUORUM_DROP).mean(axis=1)
    return {ts.date() for ts in rets.index[frac >= CRASH_QUORUM_FRACTION]}


def symbol_likely_has_split(df_daily: pd.DataFrame, crash_dates: set) -> bool:
    """
    True if the daily close series shows a single-bar drop below
    SPLIT_DROP_THRESHOLD on a date that is NOT a systemic crash date.
    """
    rets = df_daily["close"].pct_change()
    for ts in rets[rets < SPLIT_DROP_THRESHOLD].index:
        if ts.date() not in crash_dates:
            return True
    return False


def discover_symbols() -> list[str]:
    """All symbols with a _5min.json file, filtered by WHITELIST / BLACKLIST."""
    files   = sorted(DATA_DIR.glob("*_5min.json"))
    symbols = [f.stem.replace("_5min", "") for f in files]
    if WHITELIST:
        symbols = [s for s in symbols if s in set(WHITELIST)]
    return [s for s in symbols if s not in set(BLACKLIST)]


def load_all_data(symbols: list[str]) -> dict[str, pd.DataFrame]:
    """
    Load 5-min source files, resample to daily, validate, and return accepted
    symbol DataFrames ready for indicator computation.

    Pipeline per symbol
    ───────────────────
    1. Load *_5min.json  (market hours only: 09:30–16:00)
    2. Resample to business-day OHLCV
    3. Split check on daily closes (non-crash dates only)
    4. Require at least MIN_BARS_REQUIRED daily bars
    """
    print(f"\n[data] Loading {len(symbols)} candidates (5-min → daily resampling)...")

    raw_daily:  dict[str, pd.DataFrame] = {}
    daily_close: dict[str, pd.Series]   = {}

    for sym in symbols:
        df_5min = load_symbol_file_5min(DATA_DIR / f"{sym}_5min.json")
        if df_5min is None or len(df_5min) < 390:   # < ~1 full trading day of 5-min bars
            continue
        df_day = resample_to_daily(df_5min)
        if len(df_day) > 10:
            raw_daily[sym]   = df_day
            daily_close[sym] = df_day["close"]

    print(f"[data] Resampled        : {len(raw_daily)} symbols")

    crash_dates = find_systemic_crash_dates(daily_close)
    if crash_dates:
        print(f"[data] {len(crash_dates)} systemic crash date(s) identified "
              f"(exempt from split filter)")

    loaded:         dict[str, pd.DataFrame] = {}
    skipped_split:  list[str] = []
    skipped_short:  list[str] = []

    for sym, df in raw_daily.items():
        if symbol_likely_has_split(df, crash_dates):
            skipped_split.append(sym)
            continue
        if len(df) < MIN_BARS_REQUIRED:
            skipped_short.append(sym)
            continue
        loaded[sym] = df

    print(f"[data] Accepted         : {len(loaded)}")
    print(f"[data] Skipped split    : {len(skipped_split)} {skipped_split[:15]}")
    print(f"[data] Skipped too short: {len(skipped_short)}")
    return loaded

# ═══════════════════════════════════════════════════════════════════════════════
# INDICATOR CALCULATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def calc_rsi2(close: pd.Series, period: int = 2) -> pd.Series:
    """
    Wilder's RSI — period=2 (RMA smoothing: α = 1/period).
    When avg_loss = 0, RSI = 100.
    """
    delta    = close.diff()
    gain     = delta.clip(lower=0.0)
    loss     = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0.0, np.nan)
    return (100.0 - 100.0 / (1.0 + rs)).fillna(100.0)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach all signal and exit indicator columns to a daily OHLCV DataFrame.
    All computations are strictly look-back — no information from the future.

    Columns added
    ─────────────
    n_day_ret    : ENTRY_N_DAY_RETURN-period log-close return
    q_threshold  : rolling ENTRY_TRIGGER quantile of past n_day_ret values
    qpi_signal   : True when n_day_ret < q_threshold  (entry condition 1)
    sma200       : SMA(SMA_PERIOD) of close
    ibs          : Internal Bar Strength = (close−low)/(high−low)
    rsi2         : Wilder's RSI(2) of close
    adv63        : 63-day rolling mean dollar volume  (liquidity gate)
    """
    out = df.copy()

    # ── N-day return ──────────────────────────────────────────────────────────
    n_day_ret       = out["close"].pct_change(ENTRY_N_DAY_RETURN)
    out["n_day_ret"] = n_day_ret

    # ── Rolling quantile threshold — past data only ───────────────────────────
    # shift(1): element at index t = return at t-1 → rolling window covers
    # [t-RETURN_RANK_RANGE .. t-1], which is purely historical at bar t.
    min_p = max(int(RETURN_RANK_RANGE * 0.25), 50)
    q_thr = (
        n_day_ret.shift(1)
        .rolling(window=RETURN_RANK_RANGE, min_periods=min_p)
        .quantile(ENTRY_TRIGGER)
    )
    out["q_threshold"] = q_thr
    out["qpi_signal"]  = n_day_ret < q_thr   # False when q_thr is NaN

    # ── 200-day SMA ───────────────────────────────────────────────────────────
    out["sma200"] = out["close"].rolling(SMA_PERIOD, min_periods=SMA_PERIOD).mean()

    # ── IBS ───────────────────────────────────────────────────────────────────
    bar_range    = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["ibs"]   = ((out["close"] - out["low"]) / bar_range).fillna(0.5).clip(0.0, 1.0)

    # ── RSI(2) ────────────────────────────────────────────────────────────────
    out["rsi2"]  = calc_rsi2(out["close"])

    # ── 63-day dollar ADV ─────────────────────────────────────────────────────
    out["adv63"] = (out["close"] * out["volume"]).rolling(
        LIQUIDITY_ADV_WINDOW, min_periods=LIQUIDITY_ADV_WINDOW // 2
    ).mean()

    return out

# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL COLLECTION
# ═══════════════════════════════════════════════════════════════════════════════

def collect_signals(sym: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame of entry signal bars for one symbol (from START_DATE onward).
    All three entry conditions must hold simultaneously.
    Each row = one entry candidate; fill_price = close of signal bar.
    """
    trade_start = pd.Timestamp(START_DATE.replace("/", "-"))
    d = df[df.index >= trade_start]
    if d.empty:
        return pd.DataFrame()

    mask = (
        d["qpi_signal"].fillna(False)               # QPI < threshold
        & (d["close"] > d["sma200"])                # above 200-day SMA
        & (d["ibs"] < IBS_ENTRY_FILTER)             # closed near intraday low
        & d["adv63"].notna() & d["adv63"].gt(0)     # liquidity data available
        & d["close"].gt(0)
        & d["n_day_ret"].notna()
    )

    sigs = d[mask].copy()
    if sigs.empty:
        return pd.DataFrame()

    sigs["symbol"]     = sym
    sigs["fill_price"] = sigs["close"]
    sigs["ibs_entry"]  = sigs["ibs"]

    return (
        sigs[["symbol", "fill_price", "n_day_ret", "q_threshold", "ibs_entry", "adv63"]]
        .rename_axis("bar_time")
        .reset_index()
    )

# ═══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO SIMULATION
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_portfolio(
    all_signals: dict[str, pd.DataFrame],
    loaded_data: dict[str, pd.DataFrame],
) -> tuple[list[dict], pd.Series, dict]:
    """
    Walk forward through daily bars, processing close-priced entries and exits.

    Entry  : at close of signal bar (bars_held = 0 on entry bar; exit not checked yet)
    Exit   : at close of a subsequent bar when IBS/RSI2/time-stop condition fires
    Equity : mark-to-market each bar — cash + sum(shares × current_close)

    Returns
    ───────
    trade_log      — list of completed trade dicts
    equity_series  — pd.Series(timestamp → mark-to-market equity)
    sizing_stats   — counts of portfolio-level filter rejections
    """

    # ── Build signals lookup: bar_time → [signal_dict, ...] ──────────────────
    signals_at_time: dict[pd.Timestamp, list[dict]] = {}
    for sym, sig_df in all_signals.items():
        if sig_df.empty:
            continue
        for rec in sig_df.to_dict("records"):
            signals_at_time.setdefault(rec["bar_time"], []).append(rec)

    # ── Pre-compute per-symbol column dicts for O(1) bar access ──────────────
    ibs_map   = {sym: df["ibs"].to_dict()   for sym, df in loaded_data.items()}
    rsi2_map  = {sym: df["rsi2"].to_dict()  for sym, df in loaded_data.items()}
    close_map = {sym: df["close"].to_dict() for sym, df in loaded_data.items()}

    # ── Master timeline ───────────────────────────────────────────────────────
    trade_start    = pd.Timestamp(START_DATE.replace("/", "-"))
    all_timestamps = sorted({
        ts for df in loaded_data.values()
        for ts in df.index if ts >= trade_start
    })

    cash           = BALANCE_START
    open_positions: list[dict] = []
    trade_log:      list[dict] = []
    equity_curve:   list[tuple] = []
    sizing_stats = {
        "filtered_max_pos":        0,
        "filtered_notional_cap":   0,
        "filtered_total_notional": 0,
        "filtered_liquidity":      0,
    }

    def current_position_value() -> float:
        """Mark-to-market value of all open positions at bar_time."""
        val = 0.0
        for pos in open_positions:
            price = close_map.get(pos["symbol"], {}).get(bar_time, pos["fill_price"])
            val  += pos["shares"] * price
        return val

    for bar_time in all_timestamps:

        # ── Mark-to-market equity (recorded before any transactions) ─────────
        equity_curve.append((bar_time, cash + current_position_value()))

        # ── 1. Process exits at today's close ─────────────────────────────────
        remaining = []
        for pos in open_positions:

            # Never exit on the entry bar itself
            if pos["fill_time"] == bar_time:
                remaining.append(pos)
                continue

            sym = pos["symbol"]
            pos["bars_held"] = pos.get("bars_held", 0) + 1

            # Retrieve today's indicator values; keep position if bar missing
            exit_close = close_map.get(sym, {}).get(bar_time)
            if exit_close is None:
                remaining.append(pos)
                continue

            ibs_val  = ibs_map.get(sym,  {}).get(bar_time, 0.5)
            rsi2_val = rsi2_map.get(sym, {}).get(bar_time, 50.0)

            # ── Deterioration stop (v3) ─────────────────────────────────────
            # Update consecutive-lows counter (only after grace period).
            # A close at or above entry_close resets the counter.
            if pos["bars_held"] >= STOP_MIN_BARS:
                if exit_close < pos["fill_price"]:
                    pos["consec_lows"] = pos.get("consec_lows", 0) + 1
                else:
                    pos["consec_lows"] = 0

            exit_reason = None
            if pos.get("consec_lows", 0) >= STOP_CONSEC_LOWS:
                exit_reason = "deterioration_stop"
            elif ibs_val > IBS_EXIT_FILTER:
                exit_reason = "ibs_exit"
            elif rsi2_val > RSI_EXIT_FILTER:
                exit_reason = "rsi2_exit"
            elif MAX_HOLDING_PERIOD is not None and pos["bars_held"] >= MAX_HOLDING_PERIOD:
                exit_reason = "time_stop"

            if exit_reason:
                round_trip_cost = pos["notional"] * (ROUND_TRIP_COST_BPS / 10_000)
                gross_pnl       = pos["shares"] * (exit_close - pos["fill_price"])
                net_pnl         = gross_pnl - round_trip_cost
                cash           += pos["shares"] * exit_close - round_trip_cost
                trade_log.append({
                    **pos,
                    "exit_price":  exit_close,
                    "exit_time":   bar_time,
                    "exit_reason": exit_reason,
                    "pnl":         net_pnl,
                    "cost":        round_trip_cost,
                })
            else:
                remaining.append(pos)

        open_positions = remaining

        # ── 2. Process new entries at today's close ────────────────────────────
        candidates = signals_at_time.get(bar_time, [])
        if not candidates:
            continue

        open_syms  = {p["symbol"] for p in open_positions}
        candidates = [s for s in candidates if s["symbol"] not in open_syms]
        if not candidates:
            continue

        slots_free = MAX_POSITIONS - len(open_positions)
        if slots_free <= 0:
            sizing_stats["filtered_max_pos"] += len(candidates)
            continue

        # Rank: deepest dislocation first
        if RANK_BY == "ibs":
            candidates.sort(key=lambda s: s["ibs_entry"])
        else:
            candidates.sort(key=lambda s: s["n_day_ret"])

        # Snapshot equity for sizing (post-exit, pre-entry)
        sizing_equity = cash + current_position_value()

        for sig in candidates:
            if len(open_positions) >= MAX_POSITIONS:
                sizing_stats["filtered_max_pos"] += 1
                continue

            sym        = sig["symbol"]
            fill_price = sig["fill_price"]
            adv63      = sig["adv63"]

            if fill_price <= 0:
                continue

            # ── Sizing ──────────────────────────────────────────────────────
            # Per-slot target: spread the full notional budget evenly.
            # MAX_TOTAL_NOTIONAL=1.0 → ~10% each (unleveraged).
            # MAX_TOTAL_NOTIONAL=1.5 → ~15% each (50% leverage).
            target_shares  = int((sizing_equity * MAX_TOTAL_NOTIONAL / MAX_POSITIONS) / fill_price)
            # Hard single-position cap — safety valve for high leverage settings.
            max_cap_shares = int((sizing_equity * MAX_NOTIONAL) / fill_price)
            shares = min(target_shares, max_cap_shares)

            if shares < 1:
                if target_shares > max_cap_shares:
                    sizing_stats["filtered_notional_cap"] += 1
                continue

            notional = shares * fill_price

            # ── Liquidity gate ───────────────────────────────────────────────
            if LIQUIDITY_ADV_MAX_PCT > 0 and adv63 > 0:
                if notional > adv63 * LIQUIDITY_ADV_MAX_PCT:
                    sizing_stats["filtered_liquidity"] += 1
                    continue

            # ── Total notional gate ──────────────────────────────────────────
            deployed = sum(
                p["shares"] * close_map.get(p["symbol"], {}).get(bar_time, p["fill_price"])
                for p in open_positions
            )
            if sizing_equity > 0 and (deployed + notional) / sizing_equity > MAX_TOTAL_NOTIONAL:
                sizing_stats["filtered_total_notional"] += 1
                continue

            round_trip_cost = notional * (ROUND_TRIP_COST_BPS / 10_000)
            cash -= notional

            open_positions.append({
                "pos_id":           f"{sym}_{bar_time.isoformat()}",
                "symbol":           sym,
                "direction":        1,
                "entry_time":       bar_time,
                "fill_time":        bar_time,
                "fill_price":       fill_price,
                "shares":           shares,
                "notional":         notional,
                "bars_held":        0,
                "equity_at_entry":  sizing_equity,
                "actual_risk_frac": notional / sizing_equity if sizing_equity > 0 else 0,
                "cost":             round_trip_cost,
                "consec_lows":      0,   # deterioration stop counter
            })

    # ── Force-close any positions still open at end of data ───────────────────
    if all_timestamps:
        last_bar = all_timestamps[-1]
        for pos in open_positions:
            sym        = pos["symbol"]
            exit_close = close_map.get(sym, {}).get(last_bar, pos["fill_price"])
            round_trip_cost = pos["notional"] * (ROUND_TRIP_COST_BPS / 10_000)
            gross_pnl       = pos["shares"] * (exit_close - pos["fill_price"])
            net_pnl         = gross_pnl - round_trip_cost
            cash           += pos["shares"] * exit_close - round_trip_cost
            trade_log.append({
                **pos,
                "exit_price":  exit_close,
                "exit_time":   last_bar,
                "exit_reason": "end_of_data",
                "pnl":         net_pnl,
                "cost":        round_trip_cost,
            })

    equity_series = pd.Series(
        {ts: eq for ts, eq in equity_curve},
        name="equity",
    )
    return trade_log, equity_series, sizing_stats

# ═══════════════════════════════════════════════════════════════════════════════
# TRADE LOG NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_trade_log(trade_log_list: list[dict]) -> pd.DataFrame:
    """Convert raw trade dicts to a normalised DataFrame matching PC blueprint schema."""
    if not trade_log_list:
        return pd.DataFrame()
    tl = pd.DataFrame(trade_log_list)
    tl["entry_date"] = pd.to_datetime(tl["fill_time"])
    tl["exit_date"]  = pd.to_datetime(tl["exit_time"])
    tl["net_pnl"]    = tl["pnl"]
    tl["commission"] = tl["cost"]
    tl["direction"]  = tl["direction"].map({1: "long", -1: "short"})
    return tl

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _side_metrics(subset: pd.DataFrame) -> dict:
    if subset.empty:
        return {"n": 0, "win_rate": 0.0, "profit_factor": 0.0, "ev_per_trade": 0.0}
    winners      = subset[subset["net_pnl"] > 0]
    losers       = subset[subset["net_pnl"] <= 0]
    total_wins   = winners["net_pnl"].sum()
    total_losses = abs(losers["net_pnl"].sum())
    pf = (total_wins / total_losses) if total_losses > 0 else float("inf")
    return {
        "n":             len(subset),
        "win_rate":      round(len(winners) / len(subset), 4),
        "profit_factor": round(pf, 4),
        "ev_per_trade":  round(subset["net_pnl"].mean(), 2),
    }


def _trade_series_sharpe(pnl_series: pd.Series, trades_per_year: float) -> float:
    if len(pnl_series) < 2 or pnl_series.std() == 0:
        return 0.0
    return round(float((pnl_series.mean() / pnl_series.std()) * (trades_per_year ** 0.5)), 3)


def _format_dur(total_seconds) -> str:
    """Format a seconds duration as calendar days (appropriate for daily strategy)."""
    if pd.isna(total_seconds) or total_seconds == 0:
        return "n/a"
    return f"{total_seconds / 86400:.1f}d"


def _max_consecutive_losses(pnl_series: pd.Series) -> int:
    longest = current = 0
    for pnl in pnl_series:
        current = current + 1 if pnl <= 0 else 0
        longest = max(longest, current)
    return longest


def _monthly_consistency(
    monthly_pnl: dict,
    daily_equity: pd.Series,
) -> dict:
    """
    Compute a comprehensive set of monthly P&L steadiness metrics.

    monthly_pnl   : {"YYYY-MM": net_pnl_float, ...}  (trade-exit based)
    daily_equity  : business-day equity series  (for month-start equity denominator)
    """
    if not monthly_pnl:
        return {}

    mo = pd.Series(monthly_pnl)
    mo.index = pd.PeriodIndex(mo.index, freq="M")
    mo = mo.sort_index()

    # Month-start equity for each month → pct return
    mo_start_eq = daily_equity.resample("MS").first()
    mo_start_eq.index = mo_start_eq.index.to_period("M")
    common = mo.index.intersection(mo_start_eq.index)
    mo_pct = (mo[common] / mo_start_eq[common] * 100).dropna()

    n_months     = len(mo)
    n_pos        = int((mo > 0).sum())
    n_neg        = int((mo < 0).sum())
    n_flat       = n_months - n_pos - n_neg

    mean_pct     = round(float(mo_pct.mean()),  2) if len(mo_pct) else 0.0
    median_pct   = round(float(mo_pct.median()), 2) if len(mo_pct) else 0.0
    std_pct      = round(float(mo_pct.std()),   2) if len(mo_pct) else 0.0
    best_pct     = round(float(mo_pct.max()),   2) if len(mo_pct) else 0.0
    worst_pct    = round(float(mo_pct.min()),   2) if len(mo_pct) else 0.0
    skew         = round(float(mo_pct.skew()),  3) if len(mo_pct) > 2 else 0.0
    kurt         = round(float(mo_pct.kurt()),  3) if len(mo_pct) > 3 else 0.0

    # Monthly Sharpe (annualised): mean / std × sqrt(12)
    mo_sharpe    = round((mo_pct.mean() / mo_pct.std()) * np.sqrt(12), 3)                    if std_pct > 0 else 0.0

    # Smoothness: ratio of mean to std (higher = steadier, like a monthly IR)
    smoothness   = round(float(mo_pct.mean() / mo_pct.std()), 3)                    if std_pct > 0 else 0.0

    # Max consecutive negative months
    consec_neg = consec = 0
    for v in mo.values:
        consec = consec + 1 if v < 0 else 0
        consec_neg = max(consec_neg, consec)

    # % months within a comfortable band
    in_band_1 = round(float(((mo_pct >= -2) & (mo_pct <= 8)).mean() * 100), 1)
    in_band_2 = round(float(((mo_pct >= -5) & (mo_pct <= 15)).mean() * 100), 1)

    # Per-year positive month counts
    hit_by_year = {}
    for period, val in mo.items():
        yr = str(period.year)
        hit_by_year.setdefault(yr, {"pos": 0, "total": 0})
        hit_by_year[yr]["total"] += 1
        if val > 0:
            hit_by_year[yr]["pos"] += 1

    return {
        "n_months":              n_months,
        "n_positive":            n_pos,
        "n_negative":            n_neg,
        "pct_positive":          round(n_pos / n_months * 100, 1) if n_months else 0.0,
        "mean_monthly_pct":      mean_pct,
        "median_monthly_pct":    median_pct,
        "std_monthly_pct":       std_pct,
        "best_month_pct":        best_pct,
        "worst_month_pct":       worst_pct,
        "monthly_sharpe":        mo_sharpe,
        "smoothness_ratio":      smoothness,
        "skew":                  skew,
        "kurt":                  kurt,
        "max_consec_neg_months": consec_neg,
        "pct_in_band_2_8":       in_band_1,   # -2% to +8%
        "pct_in_band_5_15":      in_band_2,   # -5% to +15%
        "hit_rate_by_year":      hit_by_year,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════════════════════════

def compute_metrics(
    equity:       pd.Series,
    trade_log:    pd.DataFrame,
    symbols:      list,
    filter_stats: dict,
) -> dict:
    """Full suite of backtest performance metrics (matches PC blueprint schema)."""
    if trade_log.empty:
        return {"error": "no trades generated"}

    daily_equity  = equity.resample("B").last().ffill()
    daily_returns = daily_equity.pct_change().dropna()
    period_days   = (daily_equity.index[-1] - daily_equity.index[0]).days
    years_elapsed = max(period_days / 365.25, 0.01)

    cagr      = (daily_equity.iloc[-1] / BALANCE_START) ** (1 / years_elapsed) - 1
    ann_ret   = daily_returns.mean() * 252
    ann_vol   = daily_returns.std() * np.sqrt(252)
    sharpe    = ann_ret / ann_vol if ann_vol > 0 else 0.0
    dn_vol    = daily_returns[daily_returns < 0].std() * np.sqrt(252)
    sortino   = ann_ret / dn_vol if dn_vol > 0 else 0.0

    dd_series        = daily_equity / daily_equity.cummax() - 1
    max_drawdown     = float(dd_series.min())
    calmar           = abs(cagr / max_drawdown) if max_drawdown != 0 else 0.0
    in_dd            = (dd_series < 0).astype(int)
    dd_groups        = in_dd.groupby(in_dd.diff().ne(0).cumsum()).transform("count")
    max_dd_duration  = int(dd_groups[dd_series < 0].max()) if (dd_series < 0).any() else 0

    tl = trade_log.copy()
    tl["entry_date"] = pd.to_datetime(tl["entry_date"])
    tl["exit_date"]  = pd.to_datetime(tl["exit_date"])

    winners      = tl[tl["net_pnl"] > 0]
    losers       = tl[tl["net_pnl"] <= 0]
    total_trades = len(tl)
    win_rate     = len(winners) / total_trades
    tpy          = total_trades / years_elapsed
    trade_sharpe = _trade_series_sharpe(tl["net_pnl"], tpy)

    long_trades  = tl[tl["direction"] == "long"]
    short_trades = tl[tl["direction"] == "short"]

    hold_dur      = (tl["exit_date"] - tl["entry_date"]).dt.total_seconds()
    avg_dur_all   = _format_dur(hold_dur.mean())
    avg_dur_wins  = _format_dur(hold_dur[tl["net_pnl"] > 0].mean())
    avg_dur_loss  = _format_dur(hold_dur[tl["net_pnl"] <= 0].mean())

    notional_pct     = tl["notional"] / tl["equity_at_entry"] * 100
    avg_notional_pct = round(float(notional_pct.mean()), 2)
    p05_notional     = round(float(notional_pct.quantile(0.05)), 2)
    p95_notional     = round(float(notional_pct.quantile(0.95)), 2)
    avg_risk_pct     = round(float(tl["actual_risk_frac"].mean()) * 100, 3)
    p95_risk_pct     = round(float(tl["actual_risk_frac"].quantile(0.95)) * 100, 3)

    monthly_grp = (
        tl.groupby(tl["exit_date"].dt.to_period("M"))["net_pnl"]
        .sum().reset_index()
    )
    monthly_grp.columns = ["month", "pnl"]
    monthly_pnl = {str(r["month"]): round(r["pnl"], 2) for _, r in monthly_grp.iterrows()}

    annual_pnl = {
        str(yr): round(grp["net_pnl"].sum(), 2)
        for yr, grp in tl.groupby(tl["exit_date"].dt.year)
    }

    monthly_consistency = _monthly_consistency(monthly_pnl, daily_equity)

    exit_codes = ("ibs_exit", "rsi2_exit", "deterioration_stop", "time_stop", "end_of_data")
    by_exit_reason = {}
    for code in exit_codes:
        sub = tl[tl["exit_reason"] == code]
        by_exit_reason[code] = {
            "n":       int(len(sub)),
            "wr_pct":  round(len(sub[sub["net_pnl"] > 0]) / len(sub) * 100, 1) if len(sub) else 0.0,
            "avg_pnl": round(sub["net_pnl"].mean(), 2) if len(sub) else 0.0,
        }

    tl["_dow"] = tl["entry_date"].dt.day_name()
    dow_metrics = {}
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        dt = tl[tl["_dow"] == day]
        if not dt.empty:
            dow_metrics[day] = {
                "n":       int(len(dt)),
                "wr_pct":  round(len(dt[dt["net_pnl"] > 0]) / len(dt) * 100, 1),
                "avg_pnl": round(dt["net_pnl"].mean(), 2),
            }

    gross_profit = winners["net_pnl"].sum() if len(winners) > 0 else 0.0
    gross_loss   = abs(losers["net_pnl"].sum()) if len(losers) > 0 else 0.0

    top_symbols = (
        tl.groupby("symbol")["net_pnl"].sum()
        .sort_values(ascending=False)
        .head(15).round(2).to_dict()
    )

    return {
        "version":              version,
        "strategy":             "murphy_law",
        "period_start":         str(daily_equity.index[0].date()),
        "period_end":           str(daily_equity.index[-1].date()),
        "start_date_filter":    START_DATE,
        "n_symbols":            len(symbols),
        "capital_start":        BALANCE_START,
        "capital_end":          round(float(daily_equity.iloc[-1]), 2),
        "total_return_pct":     round((float(daily_equity.iloc[-1]) / BALANCE_START - 1) * 100, 2),
        "cagr_pct":             round(cagr * 100, 2),
        "daily_sharpe":         round(float(sharpe), 3),
        "daily_sortino":        round(float(sortino), 3),
        "trade_sharpe":         round(float(trade_sharpe), 3),
        "calmar":               round(float(calmar), 3),
        "max_drawdown_pct":     round(max_drawdown * 100, 2),
        "max_dd_duration_bars": max_dd_duration,
        "n_trades":             total_trades,
        "trades_per_year":      round(tpy, 1),
        "long_trades":          int(len(long_trades)),
        "short_trades":         int(len(short_trades)),
        "win_rate_pct":         round(win_rate * 100, 2),
        "profit_factor":        round(gross_profit / gross_loss, 3) if gross_loss > 0 else None,
        "avg_win":              round(float(winners["net_pnl"].mean()), 2) if len(winners) else 0,
        "avg_loss":             round(float(losers["net_pnl"].mean()), 2)  if len(losers)  else 0,
        "ev_per_trade":         round(float(tl["net_pnl"].mean()), 2),
        "total_commission":     round(float(tl["commission"].sum()), 2),
        "avg_trade_duration":   avg_dur_all,
        "avg_winning_duration": avg_dur_wins,
        "avg_losing_duration":  avg_dur_loss,
        "avg_notional_pct":     avg_notional_pct,
        "notional_pct_p05_p95": [p05_notional, p95_notional],
        "avg_risk_pct":         avg_risk_pct,
        "risk_pct_p95":         p95_risk_pct,
        "max_consec_losses":    _max_consecutive_losses(tl.sort_values("entry_date")["net_pnl"]),
        "monthly_pnl":          monthly_pnl,
        "annual_pnl":           annual_pnl,
        "monthly_best_pct":     round(float(daily_returns.resample("ME").sum().max()) * 100, 2),
        "monthly_worst_pct":    round(float(daily_returns.resample("ME").sum().min()) * 100, 2),
        "side_metrics":         {"long": _side_metrics(long_trades), "short": _side_metrics(short_trades)},
        "by_exit_reason":       by_exit_reason,
        "dow_metrics":          dow_metrics,
        "top_symbols_by_pnl":   top_symbols,
        "monthly_consistency":  monthly_consistency,
        "filter_stats":         filter_stats,
        "params": {
            "start_date":            START_DATE,
            "entry_n_day_return":    ENTRY_N_DAY_RETURN,
            "return_rank_range":     RETURN_RANK_RANGE,
            "entry_trigger":         ENTRY_TRIGGER,
            "ibs_entry_filter":      IBS_ENTRY_FILTER,
            "ibs_exit_filter":       IBS_EXIT_FILTER,
            "rsi_exit_filter":       RSI_EXIT_FILTER,
            "max_holding_period":    MAX_HOLDING_PERIOD,
            "stop_min_bars":         STOP_MIN_BARS,
            "stop_consec_lows":      STOP_CONSEC_LOWS,
            "sma_period":            SMA_PERIOD,
            "max_positions":         MAX_POSITIONS,
            "max_notional":          MAX_NOTIONAL,
            "max_total_notional":    MAX_TOTAL_NOTIONAL,
            "rank_by":               RANK_BY,
            "liquidity_adv_window":  LIQUIDITY_ADV_WINDOW,
            "liquidity_adv_max_pct": LIQUIDITY_ADV_MAX_PCT,
            "round_trip_cost_bps":   ROUND_TRIP_COST_BPS,
        },
    }

# ═══════════════════════════════════════════════════════════════════════════════
# CHART
# ═══════════════════════════════════════════════════════════════════════════════

def plot_results(
    equity:    pd.Series,
    trade_log: pd.DataFrame,
    symbols:   list,
    out_dir:   Path,
) -> None:
    """
    Dark-theme 2×2 chart:
      Top row (full width) : mark-to-market equity curve
      Bottom left          : cumulative P&L per exit reason
      Bottom right         : monthly P&L bar chart
    """
    if trade_log.empty:
        print("[No trades — chart skipped]")
        return

    BG    = "#1a1a2e"
    GRID  = "#2d2d4e"
    TEXT  = "#e0e0ff"
    GREEN = "#00d4aa"
    RED   = "firebrick"

    REASON_COLOURS = {
        "ibs_exit":          "#00d4aa",
        "rsi2_exit":         "#7b61ff",
        "deterioration_stop":"#ff4d6d",
        "time_stop":         "darkorange",
        "end_of_data":       "#888",
    }

    fig = plt.figure(figsize=(16, 9), facecolor=BG)
    gs  = gridspec.GridSpec(
        2, 2, figure=fig, hspace=0.38, wspace=0.28,
        left=0.06, right=0.97, top=0.94, bottom=0.07,
    )

    # ── Top row: equity curve ──────────────────────────────────────────────────
    daily_eq = equity.resample("B").last().ffill()
    ax_eq    = fig.add_subplot(gs[0, :])
    ax_eq.set_facecolor(BG)
    ax_eq.spines[:].set_color(GRID)
    ax_eq.tick_params(colors=TEXT)
    ax_eq.plot(daily_eq.index, daily_eq.values, color=GREEN, lw=1.5)
    ax_eq.axhline(BALANCE_START, color="#555", lw=0.8, ls="--")
    ax_eq.fill_between(daily_eq.index, BALANCE_START, daily_eq.values,
                       where=daily_eq.values >= BALANCE_START, alpha=0.15, color=GREEN)
    ax_eq.fill_between(daily_eq.index, BALANCE_START, daily_eq.values,
                       where=daily_eq.values < BALANCE_START,  alpha=0.25, color=RED)
    ax_eq.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax_eq.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax_eq.grid(axis="y", color=GRID, lw=0.5)
    total_ret = (daily_eq.iloc[-1] / BALANCE_START - 1) * 100
    ax_eq.set_title(
        f"Equity {total_ret:+.1f}%  |  Murphy's Law v{version}  |  "
        f"N={ENTRY_N_DAY_RETURN}d  QPI<{ENTRY_TRIGGER}  "
        f"IBS_in<{IBS_ENTRY_FILTER}  IBS_out>{IBS_EXIT_FILTER}  "
        f"RSI2_out>{RSI_EXIT_FILTER:.0f}  MaxHold={MAX_HOLDING_PERIOD}  "
        f"MaxPos={MAX_POSITIONS}  {ROUND_TRIP_COST_BPS}bps",
        color=TEXT, fontsize=8, pad=6,
    )

    # ── Bottom left: cumulative P&L by exit reason ─────────────────────────────
    ax_exit = fig.add_subplot(gs[1, 0])
    ax_exit.set_facecolor(BG)
    ax_exit.spines[:].set_color(GRID)
    ax_exit.tick_params(colors=TEXT, labelsize=7)

    tl_sorted = trade_log.sort_values("exit_date")
    for reason, colour in REASON_COLOURS.items():
        sub = tl_sorted[tl_sorted["exit_reason"] == reason]
        if sub.empty:
            continue
        cum = sub["net_pnl"].cumsum()
        ax_exit.plot(
            pd.to_datetime(sub["exit_date"]).values,
            cum.values, color=colour, lw=1.2, label=f"{reason} (n={len(sub)})"
        )
    ax_exit.axhline(0, color="#555", lw=0.8, ls="--")
    ax_exit.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax_exit.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax_exit.grid(axis="y", color=GRID, lw=0.5)
    ax_exit.legend(fontsize=6.5, facecolor=BG, labelcolor=TEXT, framealpha=0.5)
    ax_exit.set_title("Cumulative P&L by exit reason", color=TEXT, fontsize=8, pad=6)

    # ── Bottom right: monthly P&L bar chart with consistency bands ──────────────
    ax_mo = fig.add_subplot(gs[1, 1])
    ax_mo.set_facecolor(BG)
    ax_mo.spines[:].set_color(GRID)
    ax_mo.tick_params(colors=TEXT, labelsize=7)

    tl_mo = trade_log.copy()
    tl_mo["ym"] = pd.to_datetime(tl_mo["exit_date"]).dt.to_period("M")
    monthly     = tl_mo.groupby("ym")["net_pnl"].sum()
    mo_idx      = monthly.index.to_timestamp()
    colours_mo  = [GREEN if v >= 0 else RED for v in monthly.values]
    ax_mo.bar(mo_idx, monthly.values, color=colours_mo, width=20, alpha=0.80)

    # Mean line + ±1 std band (steadiness visual)
    mo_mean = monthly.mean()
    mo_std  = monthly.std()
    ax_mo.axhline(mo_mean, color=GREEN, lw=1.0, ls="-",  alpha=0.7, label=f"mean ${mo_mean:+,.0f}")
    ax_mo.axhline(0,       color="#555", lw=0.8, ls="--")
    ax_mo.axhspan(mo_mean - mo_std, mo_mean + mo_std,
                  color=GREEN, alpha=0.07, label=f"±1σ ${mo_std:,.0f}")

    # Rolling 6-month average line
    mo_roll = monthly.rolling(6, min_periods=3).mean()
    ax_mo.plot(mo_idx, mo_roll.values, color="#ffd700", lw=1.1, ls="-",
               alpha=0.8, label="6mo avg")

    ax_mo.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax_mo.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax_mo.grid(axis="y", color=GRID, lw=0.5)
    ax_mo.legend(fontsize=6, facecolor=BG, labelcolor=TEXT, framealpha=0.4, loc="upper left")
    pos_months   = (monthly > 0).sum()
    pct_pos      = pos_months / len(monthly) * 100
    mo_sharpe    = (monthly.mean() / monthly.std()) * np.sqrt(12) if monthly.std() > 0 else 0
    ax_mo.set_title(
        f"Monthly P&L  |  {pos_months}/{len(monthly)} positive ({pct_pos:.0f}%)  "
        f"|  mo. Sharpe={mo_sharpe:.2f}",
        color=TEXT, fontsize=8, pad=6,
    )

    fig.suptitle(
        f"Murphy's Law v{version}  —  {len(symbols)} symbols  "
        f"QPI<{ENTRY_TRIGGER}  MaxPos={MAX_POSITIONS}  "
        f"Budget={MAX_TOTAL_NOTIONAL:.1f}x (slot={MAX_TOTAL_NOTIONAL/MAX_POSITIONS*100:.0f}%)  "
        f"{ROUND_TRIP_COST_BPS}bps",
        color=TEXT, fontsize=9, y=0.999,
    )

    chart_path = out_dir / "ml_backtest.png"
    plt.savefig(str(chart_path), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()
    print(f"[Chart saved → {chart_path}]")

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def make_output_dir(run_timestamp: str) -> Path:
    out_dir = RESULTS_DIR / f"ml_v{version}_{run_timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir       = make_output_dir(run_timestamp)

    W = 68

    print(f"\nMurphy's Law Backtest v{version}")
    print(f"{'─'*W}")
    print(f"Data source        : *_5min.json  →  resampled to daily (business day)")
    print(f"Start date         : {START_DATE}  (indicators warm up on all prior bars)")
    print(f"Entry signal       : N-day QPI < {ENTRY_TRIGGER}  |  "
          f"N={ENTRY_N_DAY_RETURN}  rank_range={RETURN_RANK_RANGE} bars")
    print(f"Entry filters      : Close > SMA({SMA_PERIOD})  |  IBS < {IBS_ENTRY_FILTER}")
    print(f"Exit triggers      : IBS > {IBS_EXIT_FILTER}  |  "
          f"RSI(2) > {RSI_EXIT_FILTER}  |  "
          f"Deterioration ≥{STOP_CONSEC_LOWS} consec lows (after {STOP_MIN_BARS} bars)  |  "
          f"Hard stop = {MAX_HOLDING_PERIOD if MAX_HOLDING_PERIOD else 'disabled'} bars")
    print(f"Universe           : {len(WHITELIST) or 'all'} symbols  "
          f"(blacklist: {len(BLACKLIST)})")
    print(f"Sizing             : budget={MAX_TOTAL_NOTIONAL:.1f}x equity  "
          f"per slot={MAX_TOTAL_NOTIONAL/MAX_POSITIONS*100:.1f}%  "
          f"single cap≤{MAX_NOTIONAL*100:.0f}%  "
          f"max_pos={MAX_POSITIONS}")
    print(f"Liquidity gate     : notional ≤ {LIQUIDITY_ADV_MAX_PCT*100:.0f}% "
          f"of {LIQUIDITY_ADV_WINDOW}-day ADV")
    print(f"Costs              : {ROUND_TRIP_COST_BPS} bps round-trip (proportional)")
    print(f"Output dir         : {out_dir}")
    print(f"{'─'*W}")

    # ── 1. Discover and load data ─────────────────────────────────────────────
    raw_symbols = discover_symbols()
    loaded_data = load_all_data(raw_symbols)
    symbols     = list(loaded_data.keys())

    if not symbols:
        print("\n[No usable symbols found — check DATA_DIR]")
        return

    print(f"\nUniverse: {len(symbols)} symbols ready")

    # ── 2. Compute indicators per symbol ──────────────────────────────────────
    print(f"\n── Indicator computation ────────────────────────────────────────")
    for sym in symbols:
        loaded_data[sym] = compute_indicators(loaded_data[sym])
    print(f"  Indicators computed for {len(symbols)} symbols")

    # ── 3. Collect signals per symbol ─────────────────────────────────────────
    print(f"\n── Signal collection ────────────────────────────────────────────")
    all_signals: dict[str, pd.DataFrame] = {}
    raw_signal_count = 0

    for sym in symbols:
        sig_df = collect_signals(sym, loaded_data[sym])
        all_signals[sym] = sig_df
        raw_signal_count += len(sig_df)

    print(f"  Raw entry signals (all symbols): {raw_signal_count:,}")

    # ── 4. Portfolio simulation ───────────────────────────────────────────────
    print(f"\n── Portfolio simulation ─────────────────────────────────────────")
    trade_log_list, equity_series, sizing_stats = simulate_portfolio(
        all_signals, loaded_data
    )

    if not trade_log_list:
        print("\n[No trades generated — check parameters and data range]")
        return

    # ── 5. Normalise trade log ────────────────────────────────────────────────
    trade_log = normalize_trade_log(trade_log_list)

    # ── 6. Filter funnel summary ──────────────────────────────────────────────
    filter_stats = {
        "raw_signals":             raw_signal_count,
        "filtered_max_pos":        sizing_stats["filtered_max_pos"],
        "filtered_notional_cap":   sizing_stats["filtered_notional_cap"],
        "filtered_total_notional": sizing_stats["filtered_total_notional"],
        "filtered_liquidity":      sizing_stats["filtered_liquidity"],
    }

    print(f"\n  Filter funnel summary (all symbols combined):")
    print(f"    Raw entry signals          : {filter_stats['raw_signals']:>8,}")
    print(f"    Blocked / max positions    : {filter_stats['filtered_max_pos']:>8,}")
    print(f"    Trimmed / notional cap     : {filter_stats['filtered_notional_cap']:>8,}")
    print(f"    Blocked / total notional   : {filter_stats['filtered_total_notional']:>8,}")
    print(f"    Blocked / ADV liquidity    : {filter_stats['filtered_liquidity']:>8,}")
    print(f"    Executed trades            : {len(trade_log):>8,}")

    # ── 7. Compute metrics ────────────────────────────────────────────────────
    metrics = compute_metrics(equity_series, trade_log, symbols, filter_stats)

    # ── 8. Print results ──────────────────────────────────────────────────────
    print(f"\n{'═'*W}")
    print(f"  Murphy's Law Backtest v{version}  —  Results")
    print(f"{'═'*W}")
    print(f"  Period        : {metrics['period_start']}  →  {metrics['period_end']}"
          f"  ({metrics['n_symbols']} symbols)")
    print(f"  Capital       : ${BALANCE_START:,.0f}  →  ${metrics['capital_end']:,.0f}"
          f"  ({metrics['total_return_pct']:+.2f}%)")
    print(f"  CAGR          : {metrics['cagr_pct']:+.2f}%")
    print(f"  Max Drawdown  : {metrics['max_drawdown_pct']:.2f}%"
          f"  (duration {metrics['max_dd_duration_bars']} days)")
    print(f"  Sharpe (daily): {metrics['daily_sharpe']:.3f}   "
          f"Sortino: {metrics['daily_sortino']:.3f}   "
          f"Calmar: {metrics['calmar']:.3f}")
    print(f"  Trade Sharpe  : {metrics['trade_sharpe']:.3f}")
    print(f"{'─'*W}")
    print(f"  Trades        : {metrics['n_trades']}"
          f"  ({metrics['trades_per_year']:.1f}/yr)")
    print(f"  Win rate      : {metrics['win_rate_pct']:.2f}%   "
          f"PF={metrics['profit_factor']}   "
          f"EV=${metrics['ev_per_trade']:.2f}/trade")
    print(f"  Avg win       : ${metrics['avg_win']:,.2f}   "
          f"Avg loss: ${metrics['avg_loss']:,.2f}")
    print(f"  Max consec L  : {metrics['max_consec_losses']}   "
          f"Commission total: ${metrics['total_commission']:,.2f}")
    print(f"  Hold time     : avg={metrics['avg_trade_duration']}"
          f"  wins={metrics['avg_winning_duration']}"
          f"  losses={metrics['avg_losing_duration']}")
    print(f"  Notional avg  : {metrics['avg_notional_pct']:.1f}%"
          f"  (p05={metrics['notional_pct_p05_p95'][0]:.1f}%"
          f"  p95={metrics['notional_pct_p05_p95'][1]:.1f}%)")
    print(f"  Actual risk   : avg={metrics['avg_risk_pct']:.3f}%"
          f"  p95={metrics['risk_pct_p95']:.3f}%")

    print(f"\n  Exit reason breakdown:")
    for code, stats in metrics["by_exit_reason"].items():
        if stats["n"] > 0:
            print(f"    {code:<20} n={stats['n']:>5}  "
                  f"wr={stats['wr_pct']:>5.1f}%  "
                  f"avg=${stats['avg_pnl']:>+8.2f}")

    print(f"\n  Direction breakdown:")
    for direction in ("long", "short"):
        m = metrics["side_metrics"][direction]
        if m["n"] > 0:
            print(f"    {direction:<8} n={m['n']:>5}  "
                  f"wr={m['win_rate']*100:>5.1f}%  "
                  f"pf={m['profit_factor']:>6.3f}  "
                  f"ev=${m['ev_per_trade']:>+8.2f}")

    print(f"\n  Annual P&L:")
    for yr, pnl in sorted(metrics["annual_pnl"].items()):
        sign = "+" if pnl >= 0 else ""
        print(f"    {yr}  {sign}${pnl:,.2f}")

    print(f"\n  Monthly P&L table (net $):")
    months_by_year: dict = {}
    for month_key, month_pnl in metrics["monthly_pnl"].items():
        yr, mo = month_key.split("-")
        months_by_year.setdefault(yr, {})[int(mo)] = month_pnl

    print(f"  {'YEAR':>4} " + " ".join(f"{'M'+str(m):>7}" for m in range(1, 13))
          + f"  {'TOTAL':>9}")
    for yr in sorted(months_by_year):
        row_values = [months_by_year[yr].get(m, None) for m in range(1, 13)]
        year_total = sum(v for v in row_values if v is not None)
        cells = []
        for val in row_values:
            if val is None:
                cells.append(f"{'':>7}")
            else:
                cells.append(
                    f"{val:>+7,.0f}" if abs(val) < 1_000_000 else f"{val/1e3:>+6.0f}k"
                )
        sign = "+" if year_total >= 0 else ""
        print(f"  {yr:>4} " + " ".join(cells) + f"  {sign}${year_total:>8,.0f}")

    # ── Monthly consistency ───────────────────────────────────────────────────
    mc = metrics.get("monthly_consistency", {})
    if mc:
        print(f"\n  Monthly consistency  ({mc['n_months']} months):")
        print(f"    Positive months     : {mc['n_positive']}/{mc['n_months']}  ({mc['pct_positive']:.1f}%)")
        print(f"    Max consec negative : {mc['max_consec_neg_months']} months")
        print(f"    Monthly return      : "
              f"mean={mc['mean_monthly_pct']:+.2f}%  "
              f"median={mc['median_monthly_pct']:+.2f}%  "
              f"std={mc['std_monthly_pct']:.2f}%")
        print(f"    Range               : "
              f"best={mc['best_month_pct']:+.2f}%  "
              f"worst={mc['worst_month_pct']:+.2f}%")
        print(f"    Monthly Sharpe      : {mc['monthly_sharpe']:.3f}   "
              f"Smoothness (mean/std): {mc['smoothness_ratio']:.3f}")
        print(f"    Skew / Kurt         : {mc['skew']:+.3f} / {mc['kurt']:+.3f}")
        print(f"    In [-2%, +8%] band  : {mc['pct_in_band_2_8']:.1f}% of months")
        print(f"    In [-5%,+15%] band  : {mc['pct_in_band_5_15']:.1f}% of months")
        print(f"\n  Positive months per year:")
        for yr, v in sorted(mc.get("hit_rate_by_year", {}).items()):
            bar = "█" * v["pos"] + "░" * (v["total"] - v["pos"])
            print(f"    {yr}  {bar}  {v['pos']}/{v['total']}")

    print(f"{'═'*W}")

    # ── 9. Save outputs ───────────────────────────────────────────────────────
    trade_log_path = out_dir / f"ml_trade_log_v{version}_{run_timestamp}.csv"
    equity_path    = out_dir / f"ml_equity_v{version}_{run_timestamp}.csv"
    metrics_path   = out_dir / f"ml_metrics_v{version}_{run_timestamp}.json"

    trade_log.to_csv(str(trade_log_path), index=False)
    equity_series.to_csv(str(equity_path), header=["equity"])
    with open(str(metrics_path), "w") as f:
        json.dump(metrics, f, indent=2)

    plot_results(equity_series, trade_log, symbols, out_dir)

    print(f"\n[Exports saved to {out_dir}]")
    print(f"  {trade_log_path.name}  ({len(trade_log):,} rows)")
    print(f"  {equity_path.name}")
    print(f"  {metrics_path.name}")
    print(f"  ml_backtest.png")
    print(f"\n[Paste ml_metrics JSON to Claude for interpretation]")


if __name__ == "__main__":
    main()