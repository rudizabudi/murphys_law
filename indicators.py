"""
indicators.py — Indicator engine.

Line-for-line port of calc_rsi2() and compute_indicators() from reference/v300.py.
Zero logic changes — only module-level constants replaced with config references.
"""

import numpy as np
import pandas as pd

import config


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
    n_day_ret        = out["close"].pct_change(config.ENTRY_N_DAY_RETURN)
    out["n_day_ret"] = n_day_ret

    # ── Rolling quantile threshold — past data only ───────────────────────────
    # shift(1): element at index t = return at t-1 → rolling window covers
    # [t-RETURN_RANK_RANGE .. t-1], which is purely historical at bar t.
    min_p = max(int(config.RETURN_RANK_RANGE * 0.25), 50)
    q_thr = (
        n_day_ret.shift(1)
        .rolling(window=config.RETURN_RANK_RANGE, min_periods=min_p)
        .quantile(config.ENTRY_TRIGGER)
    )
    out["q_threshold"] = q_thr
    out["qpi_signal"]  = n_day_ret < q_thr   # False when q_thr is NaN

    # ── 200-day SMA ───────────────────────────────────────────────────────────
    out["sma200"] = out["close"].rolling(
        config.SMA_PERIOD, min_periods=config.SMA_PERIOD
    ).mean()

    # ── IBS ───────────────────────────────────────────────────────────────────
    bar_range  = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["ibs"] = ((out["close"] - out["low"]) / bar_range).fillna(0.5).clip(0.0, 1.0)

    # ── RSI(2) ────────────────────────────────────────────────────────────────
    out["rsi2"] = calc_rsi2(out["close"])

    # ── 63-day dollar ADV ─────────────────────────────────────────────────────
    out["adv63"] = (out["close"] * out["volume"]).rolling(
        config.LIQUIDITY_ADV_WINDOW, min_periods=config.LIQUIDITY_ADV_WINDOW // 2
    ).mean()

    return out
