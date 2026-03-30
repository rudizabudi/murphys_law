"""
tests/test_indicators.py — Parity checks for indicators.py vs reference/v300.py logic.

Two test classes:
  TestIndicatorsSynthetic  — deterministic synthetic data; no DB required.
                             Verifies every column against independently-derived
                             expected values using the exact same formulas as the
                             reference backtest (reproduced verbatim below).
  TestIndicatorsDB         — loads a real symbol from daily_bars and checks that
                             compute_indicators() returns the expected columns with
                             finite, in-range values. Skipped if the DB is absent
                             or empty.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Allow imports from project root regardless of how pytest is invoked
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import indicators


TOL = 1e-6   # floating-point comparison tolerance for all assertions

# ═══════════════════════════════════════════════════════════════════════════════
# Reference implementations (copied verbatim from reference/v300.py)
# Isolated here to avoid importing the reference module (which pulls in
# matplotlib and other deps not needed at test time).
# ═══════════════════════════════════════════════════════════════════════════════

def _ref_calc_rsi2(close: pd.Series, period: int = 2) -> pd.Series:
    delta    = close.diff()
    gain     = delta.clip(lower=0.0)
    loss     = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0.0, np.nan)
    return (100.0 - 100.0 / (1.0 + rs)).fillna(100.0)


def _ref_compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reference implementation using the same constants as config
    (values are identical — verified in config.py comments).
    """
    out = df.copy()

    n_day_ret        = out["close"].pct_change(config.ENTRY_N_DAY_RETURN)
    out["n_day_ret"] = n_day_ret

    min_p = max(int(config.RETURN_RANK_RANGE * 0.25), 50)
    q_thr = (
        n_day_ret.shift(1)
        .rolling(window=config.RETURN_RANK_RANGE, min_periods=min_p)
        .quantile(config.ENTRY_TRIGGER)
    )
    out["q_threshold"] = q_thr
    out["qpi_signal"]  = n_day_ret < q_thr

    out["sma200"] = out["close"].rolling(
        config.SMA_PERIOD, min_periods=config.SMA_PERIOD
    ).mean()

    bar_range  = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["ibs"] = ((out["close"] - out["low"]) / bar_range).fillna(0.5).clip(0.0, 1.0)

    out["rsi2"] = _ref_calc_rsi2(out["close"])

    out["adv63"] = (out["close"] * out["volume"]).rolling(
        config.LIQUIDITY_ADV_WINDOW, min_periods=config.LIQUIDITY_ADV_WINDOW // 2
    ).mean()

    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Synthetic OHLCV fixture
# ═══════════════════════════════════════════════════════════════════════════════

def _make_synthetic_ohlcv(n: int = 350, seed: int = 42) -> pd.DataFrame:
    """
    Generate n business-day bars of realistic synthetic OHLCV data.
    Uses a seeded random walk so the test is fully deterministic.
    """
    rng   = np.random.default_rng(seed)
    dates = pd.bdate_range("2018-01-02", periods=n)

    # Random-walk close prices starting at 100
    log_returns = rng.normal(0.0003, 0.015, size=n)
    close       = 100.0 * np.exp(np.cumsum(log_returns))

    # Intraday range: high/low drawn around close
    intra_range = close * rng.uniform(0.005, 0.025, size=n)
    high        = close + intra_range * rng.uniform(0.2, 0.8, size=n)
    low         = close - intra_range * rng.uniform(0.2, 0.8, size=n)
    # Ensure close is always within [low, high]
    high        = np.maximum(high, close)
    low         = np.minimum(low,  close)
    open_       = low + (high - low) * rng.uniform(0.0, 1.0, size=n)

    volume = rng.integers(500_000, 5_000_000, size=n).astype(float)

    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=dates,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Test class 1 — synthetic parity
# ═══════════════════════════════════════════════════════════════════════════════

class TestIndicatorsSynthetic:

    @pytest.fixture(scope="class")
    def frames(self):
        df  = _make_synthetic_ohlcv()
        got = indicators.compute_indicators(df)
        exp = _ref_compute_indicators(df)
        return got, exp

    def test_output_columns_present(self, frames):
        got, _ = frames
        for col in ("n_day_ret", "q_threshold", "qpi_signal", "sma200", "ibs", "rsi2", "adv63"):
            assert col in got.columns, f"Missing column: {col}"

    def test_no_extra_columns_dropped(self, frames):
        """OHLCV columns must still be present in the output."""
        got, _ = frames
        for col in ("open", "high", "low", "close", "volume"):
            assert col in got.columns, f"OHLCV column missing from output: {col}"

    def test_n_day_ret(self, frames):
        got, exp = frames
        mask = exp["n_day_ret"].notna()
        np.testing.assert_allclose(
            got.loc[mask, "n_day_ret"].values,
            exp.loc[mask, "n_day_ret"].values,
            atol=TOL, rtol=0,
            err_msg="n_day_ret mismatch",
        )

    def test_q_threshold(self, frames):
        got, exp = frames
        mask = exp["q_threshold"].notna()
        np.testing.assert_allclose(
            got.loc[mask, "q_threshold"].values,
            exp.loc[mask, "q_threshold"].values,
            atol=TOL, rtol=0,
            err_msg="q_threshold mismatch",
        )

    def test_qpi_signal(self, frames):
        got, exp = frames
        mask = exp["q_threshold"].notna() & exp["n_day_ret"].notna()
        assert (got.loc[mask, "qpi_signal"] == exp.loc[mask, "qpi_signal"]).all(), \
            "qpi_signal mismatch"

    def test_sma200(self, frames):
        got, exp = frames
        mask = exp["sma200"].notna()
        np.testing.assert_allclose(
            got.loc[mask, "sma200"].values,
            exp.loc[mask, "sma200"].values,
            atol=TOL, rtol=0,
            err_msg="sma200 mismatch",
        )

    def test_ibs(self, frames):
        got, exp = frames
        np.testing.assert_allclose(
            got["ibs"].values,
            exp["ibs"].values,
            atol=TOL, rtol=0,
            err_msg="ibs mismatch",
        )

    def test_ibs_bounded(self, frames):
        got, _ = frames
        assert got["ibs"].between(0.0, 1.0).all(), "IBS out of [0, 1]"

    def test_rsi2(self, frames):
        got, exp = frames
        np.testing.assert_allclose(
            got["rsi2"].values,
            exp["rsi2"].values,
            atol=TOL, rtol=0,
            err_msg="rsi2 mismatch",
        )

    def test_rsi2_bounded(self, frames):
        got, _ = frames
        assert got["rsi2"].between(0.0, 100.0).all(), "RSI2 out of [0, 100]"

    def test_adv63(self, frames):
        got, exp = frames
        mask = exp["adv63"].notna()
        np.testing.assert_allclose(
            got.loc[mask, "adv63"].values,
            exp.loc[mask, "adv63"].values,
            atol=TOL, rtol=0,
            err_msg="adv63 mismatch",
        )

    def test_row_count_unchanged(self, frames):
        got, exp = frames
        assert len(got) == len(exp) == 350

    def test_index_unchanged(self, frames):
        got, exp = frames
        assert got.index.equals(exp.index)

    def test_calc_rsi2_zero_loss_returns_100(self):
        """When all price moves are non-negative, RSI should be 100."""
        close = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
        result = indicators.calc_rsi2(close)
        # First `period` bars are NaN-derived; after warmup, expect 100
        assert result.iloc[-1] == pytest.approx(100.0, abs=TOL)

    def test_calc_rsi2_flat_prices(self):
        """Flat prices → no gains, no losses → RSI should be 100 (fillna branch)."""
        close = pd.Series([50.0] * 10)
        result = indicators.calc_rsi2(close)
        # diff() == 0 everywhere → avg_loss == 0 → fillna(100)
        assert (result.dropna() == 100.0).all()

    def test_ibs_zero_range_bar(self):
        """A bar where high == low should produce IBS = 0.5 (fillna branch)."""
        df = _make_synthetic_ohlcv(n=210)
        df.loc[df.index[100], "high"] = df.loc[df.index[100], "low"]
        df.loc[df.index[100], "close"] = df.loc[df.index[100], "low"]
        result = indicators.compute_indicators(df)
        assert result.loc[df.index[100], "ibs"] == pytest.approx(0.5, abs=TOL)


# ═══════════════════════════════════════════════════════════════════════════════
# Test class 2 — DB-backed (skipped when daily_bars is absent or empty)
# ═══════════════════════════════════════════════════════════════════════════════

def _load_symbol_from_db(min_bars: int = 300) -> pd.DataFrame | None:
    """
    Return a DataFrame for the first symbol in daily_bars with >= min_bars rows,
    or None if the DB doesn't exist / has no qualifying symbol.
    """
    db_path = Path(config.DB_PATH)
    if not db_path.exists():
        return None
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT symbol FROM daily_bars GROUP BY symbol "
            "HAVING COUNT(*) >= ? ORDER BY symbol LIMIT 1",
            (min_bars,),
        ).fetchone()
        if row is None:
            conn.close()
            return None
        symbol = row[0]
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume "
            "FROM daily_bars WHERE symbol = ? ORDER BY date",
            conn,
            params=(symbol,),
            parse_dates=["date"],
            index_col="date",
        )
        conn.close()
        return df
    except Exception:
        return None


@pytest.fixture(scope="module")
def db_frame():
    df = _load_symbol_from_db()
    if df is None:
        pytest.skip("daily_bars not populated — run migrate.py first")
    return indicators.compute_indicators(df)


class TestIndicatorsDB:

    def test_columns_present(self, db_frame):
        for col in ("n_day_ret", "q_threshold", "qpi_signal", "sma200", "ibs", "rsi2", "adv63"):
            assert col in db_frame.columns

    def test_ibs_bounded(self, db_frame):
        assert db_frame["ibs"].between(0.0, 1.0).all()

    def test_rsi2_bounded(self, db_frame):
        assert db_frame["rsi2"].between(0.0, 100.0).all()

    def test_adv63_positive(self, db_frame):
        valid = db_frame["adv63"].dropna()
        assert (valid > 0).all()

    def test_sma200_lags_correctly(self, db_frame):
        """SMA(200) must be NaN for the first 199 bars."""
        assert db_frame["sma200"].iloc[:config.SMA_PERIOD - 1].isna().all()

    def test_no_future_leakage_q_threshold(self, db_frame):
        """
        q_threshold at index t is based on returns up to t-1 (shift(1) guard).
        Verify that q_threshold at row 0 is NaN (no history to rank against).
        """
        assert pd.isna(db_frame["q_threshold"].iloc[0])

    def test_parity_vs_reference_first_300_bars(self, db_frame):
        """
        Re-run the reference implementation on the same data and compare
        all numeric columns within tolerance.
        """
        # Reconstruct the input OHLCV by selecting only base columns
        ohlcv = db_frame[["open", "high", "low", "close", "volume"]].copy()
        ref   = _ref_compute_indicators(ohlcv)

        for col in ("n_day_ret", "q_threshold", "sma200", "ibs", "rsi2", "adv63"):
            mask = ref[col].notna() & db_frame[col].notna()
            np.testing.assert_allclose(
                db_frame.loc[mask, col].values,
                ref.loc[mask, col].values,
                atol=TOL, rtol=0,
                err_msg=f"DB parity failed for column: {col}",
            )
