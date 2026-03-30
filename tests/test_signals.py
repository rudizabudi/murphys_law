"""
tests/test_signals.py — Tests for signals.py.

Structure
─────────
  TestEntrySignals       — seeded synthetic OHLCV + compute_indicators(),
                           scanned for bars that satisfy/violate each condition.
  TestExitSignals        — minimal hand-crafted DataFrames with specific indicator
                           values; position dicts built directly.
  TestConseqLowsCounter  — consec_lows increment / reset / grace-period behaviour.
  TestSignalsDB          — 2-3 integration tests loading a real symbol from daily_bars.
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from indicators import compute_indicators
from signals import get_entry_signals, get_exit_signals


# ═══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_synthetic_ohlcv(n: int = 400, seed: int = 42) -> pd.DataFrame:
    """Identical generator to test_indicators.py for cross-test consistency."""
    rng   = np.random.default_rng(seed)
    dates = pd.bdate_range("2018-01-02", periods=n)
    log_r = rng.normal(0.0003, 0.015, size=n)
    close = 100.0 * np.exp(np.cumsum(log_r))
    intra = close * rng.uniform(0.005, 0.025, size=n)
    high  = np.maximum(close + intra * rng.uniform(0.2, 0.8, size=n), close)
    low   = np.minimum(close - intra * rng.uniform(0.2, 0.8, size=n), close)
    open_ = low + (high - low) * rng.uniform(0.0, 1.0, size=n)
    vol   = rng.integers(500_000, 5_000_000, size=n).astype(float)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol},
        index=dates,
    )


def _indicator_df(n: int = 400, seed: int = 42) -> pd.DataFrame:
    return compute_indicators(_make_synthetic_ohlcv(n=n, seed=seed))


def _make_bar(
    idx: pd.Timestamp,
    close: float = 100.0,
    ibs: float   = 0.5,
    rsi2: float  = 50.0,
    sma200: float | None = None,
    qpi_signal: bool = True,
    adv63: float = 1_000_000.0,
    n_day_ret: float = -0.02,
    q_threshold: float = -0.01,
) -> pd.DataFrame:
    """Single-bar DataFrame with all indicator columns set explicitly."""
    return pd.DataFrame({
        "open":        [close * 0.99],
        "high":        [close * (1 + (1 - ibs) * 0.02)],
        "low":         [close * (1 - ibs * 0.02)],
        "close":       [close],
        "volume":      [1_000_000.0],
        "ibs":         [ibs],
        "rsi2":        [rsi2],
        "sma200":      [sma200 if sma200 is not None else close * 0.9],
        "qpi_signal":  [qpi_signal],
        "adv63":       [adv63],
        "n_day_ret":   [n_day_ret],
        "q_threshold": [q_threshold],
    }, index=pd.DatetimeIndex([idx]))


def _make_position(
    symbol:      str,
    entry_date,
    fill_price:  float,
    bars_held:   int   = 0,
    consec_lows: int   = 0,
) -> dict:
    """Position dict matching the live system's DB schema."""
    return {
        "pos_id":          f"{symbol}_{entry_date}",
        "symbol":          symbol,
        "direction":       "long",
        "entry_date":      entry_date,
        "fill_price":      fill_price,
        "shares":          100,
        "notional":        fill_price * 100,
        "bars_held":       bars_held,
        "equity_at_entry": 100_000.0,
        "actual_risk_frac": 0.1,
        "consec_lows":     consec_lows,
        "ib_order_id":     None,
    }


# ─── scan helpers (fail-fast if synthetic data lacks the needed variety) ──────

def _first_where(df: pd.DataFrame, mask: pd.Series) -> pd.Timestamp | None:
    bars = df[mask].index
    return bars[0] if len(bars) else None


def _full_entry_mask(df: pd.DataFrame) -> pd.Series:
    return (
        df["qpi_signal"].fillna(False)
        & df["close"].gt(df["sma200"])
        & df["ibs"].lt(config.IBS_ENTRY_FILTER)
        & df["adv63"].notna()
        & df["adv63"].gt(0)
        & df["close"].gt(0)
        & df["n_day_ret"].notna()
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Entry signals
# ═══════════════════════════════════════════════════════════════════════════════

class TestEntrySignals:

    @pytest.fixture(scope="class")
    def idf(self):
        return _indicator_df()

    # ── Signal fires when all conditions met ──────────────────────────────────

    def test_signal_returned_when_all_conditions_met(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip("No qualifying entry bar in synthetic data")
        result = get_entry_signals({"SYM": idf}, as_of)
        assert len(result) == 1
        sig = result[0]
        assert sig["symbol"]    == "SYM"
        assert sig["bar_time"]  == as_of
        assert sig["fill_price"] == pytest.approx(float(idf.loc[as_of, "close"]))

    def test_signal_keys_complete(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip("No qualifying entry bar in synthetic data")
        sig = get_entry_signals({"SYM": idf}, as_of)[0]
        for key in ("symbol", "bar_time", "fill_price", "n_day_ret",
                    "q_threshold", "ibs_entry", "adv63"):
            assert key in sig, f"Missing key: {key}"

    # ── Individual condition failures ─────────────────────────────────────────

    def test_no_signal_when_qpi_false(self, idf):
        """QPI condition fails → no signal."""
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "qpi_signal"] = False
        assert get_entry_signals({"SYM": df2}, as_of) == []

    def test_no_signal_when_close_below_sma200(self, idf):
        """SMA200 filter fails → no signal."""
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "sma200"] = df2.loc[as_of, "close"] * 1.5
        assert get_entry_signals({"SYM": df2}, as_of) == []

    def test_no_signal_when_ibs_at_filter_threshold(self, idf):
        """IBS >= IBS_ENTRY_FILTER (not strictly below) → no signal."""
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "ibs"] = config.IBS_ENTRY_FILTER   # exactly at threshold
        assert get_entry_signals({"SYM": df2}, as_of) == []

    def test_no_signal_when_ibs_above_filter(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "ibs"] = config.IBS_ENTRY_FILTER + 0.01
        assert get_entry_signals({"SYM": df2}, as_of) == []

    def test_no_signal_when_adv63_is_nan(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "adv63"] = float("nan")
        assert get_entry_signals({"SYM": df2}, as_of) == []

    def test_no_signal_when_adv63_is_zero(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "adv63"] = 0.0
        assert get_entry_signals({"SYM": df2}, as_of) == []

    def test_no_signal_when_n_day_ret_is_nan(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df2 = idf.copy()
        df2.loc[as_of, "n_day_ret"] = float("nan")
        assert get_entry_signals({"SYM": df2}, as_of) == []

    # ── as_of_date not in DataFrame ───────────────────────────────────────────

    def test_no_signal_when_date_missing_from_df(self, idf):
        future = pd.Timestamp("2099-01-01")
        assert get_entry_signals({"SYM": idf}, future) == []

    # ── Multiple symbols ──────────────────────────────────────────────────────

    def test_multiple_symbols_both_qualifying(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        result = get_entry_signals({"A": idf, "B": idf}, as_of)
        assert len(result) == 2
        assert {s["symbol"] for s in result} == {"A", "B"}

    def test_multiple_symbols_only_one_qualifying(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        df_bad = idf.copy()
        df_bad.loc[as_of, "qpi_signal"] = False
        result = get_entry_signals({"GOOD": idf, "BAD": df_bad}, as_of)
        assert len(result) == 1
        assert result[0]["symbol"] == "GOOD"

    # ── IBS value is stored correctly ─────────────────────────────────────────

    def test_ibs_entry_value_matches_bar(self, idf):
        as_of = _first_where(idf, _full_entry_mask(idf))
        if as_of is None:
            pytest.skip()
        sig = get_entry_signals({"SYM": idf}, as_of)[0]
        assert sig["ibs_entry"] == pytest.approx(float(idf.loc[as_of, "ibs"]))


# ═══════════════════════════════════════════════════════════════════════════════
# Exit signals — each reason fires; priority order; entry-bar protection
# ═══════════════════════════════════════════════════════════════════════════════

class TestExitSignals:

    _DATE = pd.Timestamp("2024-06-03")
    _PREV = pd.Timestamp("2024-06-01")   # earlier date for entry

    # ── ibs_exit ──────────────────────────────────────────────────────────────

    def test_ibs_exit_fires(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.95, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "ibs_exit"

    def test_ibs_exit_requires_strictly_above(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=config.IBS_EXIT_FILTER)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits == []   # exactly at threshold — no exit

    # ── rsi2_exit ─────────────────────────────────────────────────────────────

    def test_rsi2_exit_fires(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=95.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "rsi2_exit"

    def test_rsi2_exit_requires_strictly_above(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=config.RSI_EXIT_FILTER)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        assert get_exit_signals([pos], {"X": bar}, self._DATE) == []

    # ── time_stop ─────────────────────────────────────────────────────────────

    def test_time_stop_fires_at_max_holding_period(self, monkeypatch):
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", 5)
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=50.0)
        # bars_held=4 → incremented to 5 → time_stop fires
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=4)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "time_stop"

    def test_time_stop_does_not_fire_before_period(self, monkeypatch):
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", 5)
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=3)
        assert get_exit_signals([pos], {"X": bar}, self._DATE) == []

    def test_time_stop_disabled_when_none(self, monkeypatch):
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=999)
        assert get_exit_signals([pos], {"X": bar}, self._DATE) == []

    # ── deterioration_stop ────────────────────────────────────────────────────

    def test_deterioration_stop_fires(self, monkeypatch):
        monkeypatch.setattr(config, "STOP_MIN_BARS",    3)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 3)
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        # bars_held=2 → incremented to 3 = STOP_MIN_BARS; close < fill_price
        # → consec_lows 2 → 3 → fires
        bar = _make_bar(self._DATE, close=90.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=2, consec_lows=2)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "deterioration_stop"

    # ── Exit priority ─────────────────────────────────────────────────────────

    def test_priority_deterioration_beats_ibs(self, monkeypatch):
        """deterioration_stop fires even when ibs_exit condition is also true."""
        monkeypatch.setattr(config, "STOP_MIN_BARS",    3)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 3)
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=90.0, ibs=0.95, rsi2=95.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=2, consec_lows=2)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits[0]["exit_reason"] == "deterioration_stop"

    def test_priority_ibs_beats_rsi2(self):
        """ibs_exit fires before rsi2_exit when both conditions are true."""
        bar = _make_bar(self._DATE, close=100.0, ibs=0.95, rsi2=95.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits[0]["exit_reason"] == "ibs_exit"

    def test_priority_rsi2_beats_time_stop(self, monkeypatch):
        """rsi2_exit fires before time_stop when both conditions are true."""
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", 5)
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=95.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=4)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits[0]["exit_reason"] == "rsi2_exit"

    # ── Entry-bar protection ──────────────────────────────────────────────────

    def test_entry_bar_not_exited(self):
        """Position entered today (entry_date == as_of_date) must not be exited."""
        bar = _make_bar(self._DATE, close=100.0, ibs=0.99, rsi2=99.0)
        pos = _make_position("X", self._DATE.date(), fill_price=100.0, bars_held=0)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits == []

    def test_entry_bar_bars_held_not_incremented(self):
        """bars_held must not change for the entry-bar skip."""
        bar = _make_bar(self._DATE, close=100.0, ibs=0.99, rsi2=99.0)
        pos = _make_position("X", self._DATE.date(), fill_price=100.0, bars_held=0)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["bars_held"] == 0

    # ── Missing data keeps position ───────────────────────────────────────────

    def test_no_exit_when_symbol_not_in_loaded_data(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.99, rsi2=99.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"OTHER": bar}, self._DATE)
        assert exits == []

    def test_no_exit_when_date_missing_from_df(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.99, rsi2=99.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        future = pd.Timestamp("2099-01-01")
        exits = get_exit_signals([pos], {"X": bar}, future)
        assert exits == []

    # ── bars_held incremented in place ───────────────────────────────────────

    def test_bars_held_incremented_for_non_exiting_position(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=2)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["bars_held"] == 3   # in-place update

    def test_bars_held_incremented_on_exiting_position(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.95, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits[0]["bars_held"] == 2

    # ── exit_reason in returned dict ─────────────────────────────────────────

    def test_exit_dict_contains_all_original_keys(self):
        bar  = _make_bar(self._DATE, close=100.0, ibs=0.95, rsi2=50.0)
        orig = _make_position("X", self._PREV.date(), fill_price=100.0, bars_held=1)
        exit_pos = get_exit_signals([orig], {"X": bar}, self._DATE)[0]
        for key in ("pos_id", "symbol", "fill_price", "bars_held", "exit_reason"):
            assert key in exit_pos

    # ── entry_date as string or Timestamp ────────────────────────────────────

    def test_entry_date_as_string(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.95, rsi2=50.0)
        pos = _make_position("X", str(self._PREV.date()), fill_price=100.0, bars_held=1)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert len(exits) == 1

    def test_entry_date_as_timestamp_skips_exit(self):
        bar = _make_bar(self._DATE, close=100.0, ibs=0.99, rsi2=99.0)
        pos = _make_position("X", self._DATE, fill_price=100.0, bars_held=0)
        exits = get_exit_signals([pos], {"X": bar}, self._DATE)
        assert exits == []


# ═══════════════════════════════════════════════════════════════════════════════
# consec_lows counter: increment, reset, grace period
# ═══════════════════════════════════════════════════════════════════════════════

class TestConseqLowsCounter:

    _DATE = pd.Timestamp("2024-07-01")
    _PREV = pd.Timestamp("2024-06-01")

    def test_counter_increments_when_close_below_fill_price(self, monkeypatch):
        monkeypatch.setattr(config, "STOP_MIN_BARS",    3)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 99)   # prevent exit
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=90.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=2, consec_lows=3)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["consec_lows"] == 4

    def test_counter_resets_when_close_at_or_above_fill_price(self, monkeypatch):
        monkeypatch.setattr(config, "STOP_MIN_BARS",    3)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 99)
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=105.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=2, consec_lows=5)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["consec_lows"] == 0

    def test_counter_not_updated_before_grace_period(self, monkeypatch):
        """Grace period: bars_held (after increment) must be >= STOP_MIN_BARS."""
        monkeypatch.setattr(config, "STOP_MIN_BARS",    5)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 99)
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=90.0, ibs=0.3, rsi2=50.0)
        # bars_held=1 → incremented to 2, still < STOP_MIN_BARS (5)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=1, consec_lows=3)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["consec_lows"] == 3    # unchanged

    def test_counter_updated_exactly_at_grace_period(self, monkeypatch):
        """bars_held (after increment) == STOP_MIN_BARS → update begins."""
        monkeypatch.setattr(config, "STOP_MIN_BARS",    3)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 99)
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=90.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=2, consec_lows=1)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["consec_lows"] == 2    # incremented

    def test_reset_at_exactly_fill_price(self, monkeypatch):
        """Close == fill_price should reset the counter (not strictly above)."""
        monkeypatch.setattr(config, "STOP_MIN_BARS",    3)
        monkeypatch.setattr(config, "STOP_CONSEC_LOWS", 99)
        monkeypatch.setattr(config, "MAX_HOLDING_PERIOD", None)
        bar = _make_bar(self._DATE, close=100.0, ibs=0.3, rsi2=50.0)
        pos = _make_position("X", self._PREV.date(), fill_price=100.0,
                             bars_held=2, consec_lows=4)
        get_exit_signals([pos], {"X": bar}, self._DATE)
        assert pos["consec_lows"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# DB-backed integration tests
# ═══════════════════════════════════════════════════════════════════════════════

def _load_symbol_from_db(min_bars: int = 300) -> tuple[str, pd.DataFrame] | None:
    """Return (symbol, daily_bar_df) for the first qualifying symbol, or None."""
    import sqlite3
    db_path = Path(config.DB_PATH)
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path))
        row  = conn.execute(
            "SELECT symbol FROM daily_bars GROUP BY symbol "
            "HAVING COUNT(*) >= ? ORDER BY symbol LIMIT 1",
            (min_bars,),
        ).fetchone()
        if row is None:
            conn.close()
            return None
        sym = row[0]
        df  = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume "
            "FROM daily_bars WHERE symbol = ? ORDER BY date",
            conn, params=(sym,), parse_dates=["date"], index_col="date",
        )
        conn.close()
        return sym, df
    except Exception:
        return None


@pytest.fixture(scope="module")
def db_sym_df():
    result = _load_symbol_from_db()
    if result is None:
        pytest.skip("daily_bars not populated — run migrate.py first")
    sym, raw = result
    return sym, compute_indicators(raw)


class TestSignalsDB:

    def test_entry_signals_return_list(self, db_sym_df):
        sym, df = db_sym_df
        as_of = df.index[-1]
        result = get_entry_signals({sym: df}, as_of)
        assert isinstance(result, list)

    def test_entry_signal_values_are_finite_when_present(self, db_sym_df):
        """Any returned signal must have finite numeric values."""
        sym, df = db_sym_df
        for as_of in df.index[-10:]:
            for sig in get_entry_signals({sym: df}, as_of):
                for key in ("fill_price", "n_day_ret", "ibs_entry", "adv63"):
                    assert isinstance(sig[key], float), f"{key} not float"
                    assert not (sig[key] != sig[key]), f"{key} is NaN"  # NaN check

    def test_exit_logic_with_real_bar(self, db_sym_df):
        """
        Construct a synthetic position against a real bar; verify the exit
        function returns the correct type and doesn't crash.
        """
        sym, df = db_sym_df
        as_of     = df.index[-1]
        prev_date = df.index[-2].date()
        pos = _make_position(sym, prev_date, fill_price=1.0, bars_held=1)
        result = get_exit_signals([pos], {sym: df}, as_of)
        assert isinstance(result, list)
        for exit_pos in result:
            assert "exit_reason" in exit_pos
            assert exit_pos["exit_reason"] in (
                "deterioration_stop", "ibs_exit", "rsi2_exit", "time_stop"
            )
