"""
migrate.py — One-time bootstrap: /data JSON files → SQLite daily_bars table.

Run once before any other component can function:
    python migrate.py

Pipeline per symbol (identical to backtest load_all_data):
  1. Load *_5min.json (market hours 09:30–16:00)
  2. Resample to business-day OHLCV
  3. Split integrity check (non-crash dates only)
  4. Require MIN_BARS_REQUIRED daily bars
  5. Upsert accepted rows into daily_bars

After migration, /data is never read by the live system again.
"""

import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import config
import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("murphy.migrate")

# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING — exact port from backtest load_all_data pipeline
# ═══════════════════════════════════════════════════════════════════════════════

def _load_5min(path: Path) -> pd.DataFrame | None:
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
        logger.warning("  [load] %s: %s", path.name, e)
        return None


def _resample_to_daily(df_5min: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate 5-min bars to business-day OHLCV.
    open   = first bar of session
    high   = intraday high
    low    = intraday low
    close  = last bar of session
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
    daily = daily[daily["open"].notna()]
    return daily


def _find_crash_dates(all_closes: dict[str, pd.Series]) -> set:
    """Dates where ≥ CRASH_QUORUM_FRACTION of universe fell ≥ CRASH_QUORUM_DROP."""
    if not all_closes:
        return set()
    rets = pd.DataFrame(all_closes).pct_change()
    frac = (rets < -config.CRASH_QUORUM_DROP).mean(axis=1)
    return {ts.date() for ts in rets.index[frac >= config.CRASH_QUORUM_FRACTION]}


def _has_split(df_daily: pd.DataFrame, crash_dates: set) -> bool:
    """
    True if the daily close series shows a single-bar drop below
    SPLIT_DROP_THRESHOLD on a date that is NOT a systemic crash date.
    """
    rets = df_daily["close"].pct_change()
    for ts in rets[rets < config.SPLIT_DROP_THRESHOLD].index:
        if ts.date() not in crash_dates:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN MIGRATION
# ═══════════════════════════════════════════════════════════════════════════════

def run() -> None:
    data_dir = config.DATA_DIR
    if not data_dir.exists():
        logger.error("Data directory not found: %s", data_dir)
        sys.exit(1)

    files = sorted(data_dir.glob("*_5min.json"))
    if not files:
        logger.error("No *_5min.json files found in %s", data_dir)
        sys.exit(1)

    logger.info("Found %d candidate files in %s", len(files), data_dir)

    # ── Pass 1: load and resample ─────────────────────────────────────────────
    raw_daily:   dict[str, pd.DataFrame] = {}
    daily_close: dict[str, pd.Series]   = {}

    logger.info("Loading and resampling (5-min → daily)...")
    for path in files:
        sym = path.stem.replace("_5min", "")
        df_5min = _load_5min(path)
        if df_5min is None or len(df_5min) < 390:   # < ~1 full trading day
            continue
        df_day = _resample_to_daily(df_5min)
        if len(df_day) > 10:
            raw_daily[sym]   = df_day
            daily_close[sym] = df_day["close"]

    logger.info("Resampled         : %d symbols", len(raw_daily))

    # ── Systemic crash dates (exempt from split filter) ───────────────────────
    crash_dates = _find_crash_dates(daily_close)
    if crash_dates:
        logger.info(
            "%d systemic crash date(s) identified (exempt from split filter)",
            len(crash_dates),
        )

    # ── Pass 2: validate and collect rows ─────────────────────────────────────
    accepted:       list[dict] = []
    skipped_split:  list[str]  = []
    skipped_short:  list[str]  = []

    for sym, df in raw_daily.items():
        if _has_split(df, crash_dates):
            skipped_split.append(sym)
            continue
        if len(df) < config.MIN_BARS_REQUIRED:
            skipped_short.append(sym)
            continue

        for ts, row in df.iterrows():
            accepted.append({
                "symbol": sym,
                "date":   ts.date(),
                "open":   row["open"],
                "high":   row["high"],
                "low":    row["low"],
                "close":  row["close"],
                "volume": row["volume"],
            })

    logger.info("Accepted          : %d symbols", len(raw_daily) - len(skipped_split) - len(skipped_short))
    logger.info("Skipped (split)   : %d  %s", len(skipped_split), skipped_split[:15])
    logger.info("Skipped (too short): %d", len(skipped_short))
    logger.info("Total rows to upsert: %d", len(accepted))

    if not accepted:
        logger.warning("Nothing to migrate. Check your /data directory.")
        return

    # ── Upsert into daily_bars ────────────────────────────────────────────────
    logger.info("Initialising database...")
    db.init_db()

    logger.info("Upserting rows into daily_bars...")
    # Insert in symbol-sized batches to avoid very large transactions
    BATCH = 50_000
    total = 0
    for i in range(0, len(accepted), BATCH):
        chunk = accepted[i : i + BATCH]
        db.upsert_daily_bars(chunk)
        total += len(chunk)
        logger.info("  %d / %d rows written", total, len(accepted))

    logger.info("Migration complete. %d rows in daily_bars.", total)


if __name__ == "__main__":
    run()
