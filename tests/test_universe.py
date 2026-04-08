"""
tests/test_universe.py — Unit tests for universe.py

All HTTP calls are mocked — no real network requests.
DB tests use a fresh temp SQLite database per test.
"""

import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import config
import db
import universe
from universe import (
    _normalise_ticker,
    _read_universe_csv,
    _write_universe_csv,
    fetch_sp500_symbols,
    get_new_symbols,
    update_universe,
)

# ── Sample CSV payload matching the real dataset format ───────────────────────
_SAMPLE_CSV = """\
Symbol,Security,GICS Sector,GICS Sub-Industry,Headquarters Location,Date added,CIK,Founded
AAPL,Apple Inc.,Information Technology,Technology Hardware Storage & Peripherals,Cupertino CA,1982-11-30,320193,1977
MSFT,Microsoft Corporation,Information Technology,Systems Software,Redmond WA,1994-06-01,789019,1975
BRK.B,Berkshire Hathaway,Financials,Multi-line Insurance,Omaha NE,1976-12-31,1067983,1839
BF.B,Brown-Forman,Consumer Staples,Distillers & Vintners,Louisville KY,1982-07-01,14693,1870
GOOG,Alphabet Inc.,Communication Services,Interactive Media & Services,Mountain View CA,2006-04-03,1652044,1998
"""


def _mock_httpx_response(text: str, status_code: int = 200):
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.text = text
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


# ═══════════════════════════════════════════════════════════════════════════════
# TestNormaliseTicker
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormaliseTicker:

    def test_plain_ticker_unchanged(self):
        assert _normalise_ticker("AAPL") == "AAPL"

    def test_dot_removed(self):
        assert _normalise_ticker("BRK.B") == "BRKB"

    def test_double_dot_removed(self):
        assert _normalise_ticker("A.B.C") == "ABC"

    def test_leading_trailing_whitespace_stripped(self):
        assert _normalise_ticker("  MSFT  ") == "MSFT"

    def test_whitespace_and_dot(self):
        assert _normalise_ticker("  BF.B  ") == "BFB"


# ═══════════════════════════════════════════════════════════════════════════════
# TestFetchSp500Symbols
# ═══════════════════════════════════════════════════════════════════════════════

class TestFetchSp500Symbols:

    def test_returns_list_of_strings(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = fetch_sp500_symbols()
        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)

    def test_symbols_parsed_correctly(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = fetch_sp500_symbols()
        assert "AAPL" in result
        assert "MSFT" in result
        assert "GOOG" in result

    def test_dot_removal_applied(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = fetch_sp500_symbols()
        assert "BRKB" in result
        assert "BRK.B" not in result
        assert "BFB" in result
        assert "BF.B" not in result

    def test_result_is_sorted(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = fetch_sp500_symbols()
        assert result == sorted(result)

    def test_result_is_deduplicated(self):
        # Duplicate row
        doubled = _SAMPLE_CSV + "AAPL,Apple Inc.,Info Tech,Hardware,Cupertino CA,1982-11-30,320193,1977\n"
        with patch("httpx.get", return_value=_mock_httpx_response(doubled)):
            result = fetch_sp500_symbols()
        assert result.count("AAPL") == 1

    def test_http_error_raises_runtime_error(self):
        mock_resp = _mock_httpx_response("", status_code=500)
        mock_resp.raise_for_status.side_effect = Exception("HTTP 500")
        with patch("httpx.get", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="HTTP request failed"):
                fetch_sp500_symbols()

    def test_empty_csv_returns_empty_list(self):
        with patch("httpx.get", return_value=_mock_httpx_response("Symbol\n")):
            result = fetch_sp500_symbols()
        assert result == []

    def test_correct_url_requested(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)) as mock_get:
            fetch_sp500_symbols()
        called_url = mock_get.call_args[0][0]
        assert called_url == config.SP500_CSV_URL


# ═══════════════════════════════════════════════════════════════════════════════
# TestUpdateUniverse
# ═══════════════════════════════════════════════════════════════════════════════

class TestUpdateUniverse:

    @pytest.fixture(autouse=True)
    def temp_universe(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "universe.csv"
        monkeypatch.setattr(config, "UNIVERSE_CSV", str(csv_path))
        monkeypatch.setattr(config, "SYMBOL_WHITELIST", [])
        monkeypatch.setattr(config, "SYMBOL_BLACKLIST", [])
        return csv_path

    def test_writes_universe_csv(self, tmp_path):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            update_universe()
        path = Path(config.UNIVERSE_CSV)
        assert path.exists()

    def test_csv_content_matches_fetched_symbols(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            update_universe()
        written = _read_universe_csv()
        assert "AAPL" in written
        assert "BRKB" in written   # dot removed
        assert "BRK.B" not in written

    def test_detects_added_symbols(self):
        # Pre-seed with only AAPL
        _write_universe_csv(["AAPL"])
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert "MSFT" in result["added"]
        assert "GOOG" in result["added"]
        assert "AAPL" not in result["added"]

    def test_detects_removed_symbols(self):
        # Pre-seed with extra symbol XYZ not in new fetch
        _write_universe_csv(["AAPL", "XYZ"])
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert "XYZ" in result["removed"]

    def test_returns_total_count(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        # 5 symbols in sample CSV (with dot-normalisation)
        assert result["total"] == 5

    def test_whitelist_always_included(self, monkeypatch):
        monkeypatch.setattr(config, "SYMBOL_WHITELIST", ["MYWL"])
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        written = _read_universe_csv()
        assert "MYWL" in written
        assert "MYWL" in result["added"]

    def test_whitelist_not_removed_even_if_not_in_sp500(self, monkeypatch):
        monkeypatch.setattr(config, "SYMBOL_WHITELIST", ["MYWL"])
        # First update — adds MYWL
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            update_universe()
        # Second update — MYWL still not in S&P, but is in whitelist → kept
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        written = _read_universe_csv()
        assert "MYWL" in written
        assert "MYWL" not in result["removed"]

    def test_empty_universe_first_run(self):
        """First run with no existing universe.csv → all symbols added."""
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert len(result["added"]) == result["total"]
        assert result["removed"] == []

    def test_no_change_returns_empty_added_removed(self):
        # Run once to create the file
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            update_universe()
        # Run again with identical fetch
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert result["added"]   == []
        assert result["removed"] == []

    def test_whitelist_dot_normalised(self, monkeypatch):
        monkeypatch.setattr(config, "SYMBOL_WHITELIST", ["BRK.B"])
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            update_universe()
        written = _read_universe_csv()
        # BRK.B should appear as BRKB (already in S&P too, but normalisation applied)
        assert "BRKB" in written
        assert "BRK.B" not in written

    def test_fetch_failure_raises(self):
        mock_resp = _mock_httpx_response("", 500)
        mock_resp.raise_for_status.side_effect = Exception("HTTP 500")
        with patch("httpx.get", return_value=mock_resp):
            with pytest.raises(RuntimeError):
                update_universe()


# ═══════════════════════════════════════════════════════════════════════════════
# TestGetNewSymbols
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetNewSymbols:

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / "state" / "bars.db")
        csv_path = str(tmp_path / "universe.csv")
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", db_path)
        monkeypatch.setattr(config, "UNIVERSE_CSV", csv_path)
        monkeypatch.setattr(config, "SYMBOL_BLACKLIST", [])
        db.init_db()

    def _seed_bars(self, symbol: str, n_rows: int) -> None:
        """Insert n_rows of dummy bar rows for symbol into daily_bars."""
        rows = [
            {
                "symbol": symbol,
                "date":   f"2020-01-{i+1:02d}",
                "open":   100.0, "high": 101.0, "low": 99.0,
                "close":  100.0, "volume": 1_000_000.0,
            }
            for i in range(n_rows)
        ]
        db.upsert_daily_bars(rows)

    def test_empty_universe_returns_empty(self):
        # universe.csv doesn't exist → empty result
        result = get_new_symbols()
        assert result == []

    def test_symbol_with_no_bars_returned(self):
        _write_universe_csv(["AAPL"])
        result = get_new_symbols()
        assert "AAPL" in result

    def test_symbol_with_sufficient_bars_not_returned(self):
        n = config.MIN_BARS_REQUIRED
        _write_universe_csv(["AAPL"])
        self._seed_bars("AAPL", n)
        result = get_new_symbols()
        assert "AAPL" not in result

    def test_symbol_with_insufficient_bars_returned(self):
        n = config.MIN_BARS_REQUIRED - 1
        _write_universe_csv(["AAPL"])
        self._seed_bars("AAPL", n)
        result = get_new_symbols()
        assert "AAPL" in result

    def test_mixed_symbols(self):
        n = config.MIN_BARS_REQUIRED
        _write_universe_csv(["AAPL", "MSFT", "GOOG"])
        self._seed_bars("AAPL", n)       # sufficient
        self._seed_bars("MSFT", n - 10)  # insufficient
        # GOOG: no bars at all
        result = get_new_symbols()
        assert "AAPL" not in result
        assert "MSFT" in result
        assert "GOOG" in result

    def test_exactly_min_bars_not_returned(self):
        n = config.MIN_BARS_REQUIRED
        _write_universe_csv(["AAPL"])
        self._seed_bars("AAPL", n)
        result = get_new_symbols()
        assert "AAPL" not in result

    def test_one_below_min_bars_returned(self):
        n = config.MIN_BARS_REQUIRED - 1
        _write_universe_csv(["AAPL"])
        self._seed_bars("AAPL", n)
        result = get_new_symbols()
        assert "AAPL" in result


# ═══════════════════════════════════════════════════════════════════════════════
# TestBlacklist
# ═══════════════════════════════════════════════════════════════════════════════

class TestBlacklist:
    """Blacklisted symbols are excluded from update_universe() and get_new_symbols()."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path, monkeypatch):
        csv_path = str(tmp_path / "universe.csv")
        db_path  = str(tmp_path / "state" / "bars.db")
        monkeypatch.setattr(config, "UNIVERSE_CSV", csv_path)
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", db_path)
        monkeypatch.setattr(config, "SYMBOL_WHITELIST", [])
        monkeypatch.setattr(config, "SYMBOL_BLACKLIST", ["BRKB", "BFB"])
        db.init_db()

    def test_update_universe_excludes_blacklisted(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        written = _read_universe_csv()
        assert "BRKB" not in written
        assert "BFB"  not in written
        assert "AAPL" in written

    def test_update_universe_total_excludes_blacklisted(self):
        # _SAMPLE_CSV has 5 symbols; BRKB and BFB are blacklisted → total = 3
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert result["total"] == 3

    def test_update_universe_blacklisted_not_in_added(self):
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert "BRKB" not in result["added"]
        assert "BFB"  not in result["added"]

    def test_blacklisted_symbol_in_csv_excluded_by_read(self, monkeypatch):
        """If a blacklisted symbol is already on disk, _read_universe_csv must exclude it."""
        # Write directly (bypassing blacklist filtering in update_universe)
        _write_universe_csv(["AAPL", "BRKB", "MSFT"])
        symbols = _read_universe_csv()
        assert "BRKB" not in symbols
        assert "AAPL" in symbols
        assert "MSFT" in symbols

    def test_get_new_symbols_excludes_blacklisted(self):
        """get_new_symbols relies on _read_universe_csv so blacklist applies there too."""
        _write_universe_csv(["AAPL", "BRKB"])
        result = get_new_symbols()
        assert "BRKB" not in result
        assert "AAPL" in result

    def test_empty_blacklist_no_exclusion(self, monkeypatch):
        monkeypatch.setattr(config, "SYMBOL_BLACKLIST", [])
        with patch("httpx.get", return_value=_mock_httpx_response(_SAMPLE_CSV)):
            result = update_universe()
        assert result["total"] == 5
        written = _read_universe_csv()
        assert "BRKB" in written
        assert "BFB"  in written
