"""
tests/test_td_data.py — Unit tests for td_data.py

All HTTP calls are mocked — no real network requests.
DB tests use a temporary SQLite database per test class.
"""

import json
from unittest.mock import MagicMock, call, patch

import pytest

import config
import db
import td_data
from td_data import (
    _BATCH_SIZE,
    _fetch_batch,
    _parse_rows,
    fetch_full_history,
    fetch_incremental,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_td_values(n: int = 3, symbol: str = "AAPL") -> list[dict]:
    """Return n fake TwelveData value dicts."""
    return [
        {
            "datetime": f"2024-01-{i + 1:02d}",
            "open":   f"{100 + i}.00",
            "high":   f"{101 + i}.00",
            "low":    f"{99 + i}.00",
            "close":  f"{100 + i}.50",
            "volume": f"{1_000_000 + i * 1000}",
        }
        for i in range(n)
    ]


def _mock_response(body: dict, status_code: int = 200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = body
    mock.raise_for_status = MagicMock()
    return mock


def _single_sym_body(symbol: str = "AAPL", n: int = 3) -> dict:
    return {
        "meta":   {"symbol": symbol, "interval": "1day"},
        "values": _make_td_values(n, symbol),
        "status": "ok",
    }


def _multi_sym_body(symbols: list[str], n: int = 3) -> dict:
    return {
        sym: {
            "meta":   {"symbol": sym, "interval": "1day"},
            "values": _make_td_values(n, sym),
            "status": "ok",
        }
        for sym in symbols
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TestParseRows
# ═══════════════════════════════════════════════════════════════════════════════

class TestParseRows:

    def test_returns_correct_number_of_rows(self):
        rows = _parse_rows("AAPL", _make_td_values(5))
        assert len(rows) == 5

    def test_symbol_field_set(self):
        rows = _parse_rows("MSFT", _make_td_values(1))
        assert rows[0]["symbol"] == "MSFT"

    def test_date_stripped_to_10_chars(self):
        values = [{"datetime": "2024-01-05 00:00:00",
                   "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1"}]
        rows = _parse_rows("AAPL", values)
        assert rows[0]["date"] == "2024-01-05"

    def test_numeric_fields_cast_to_float(self):
        rows = _parse_rows("AAPL", _make_td_values(1))
        for field in ("open", "high", "low", "close", "volume"):
            assert isinstance(rows[0][field], float)

    def test_bad_row_skipped_not_raised(self):
        values = [
            {"datetime": "2024-01-01",
             "open": "not_a_number", "high": "1", "low": "1", "close": "1", "volume": "1"},
            {"datetime": "2024-01-02",
             "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "1000"},
        ]
        rows = _parse_rows("AAPL", values)
        assert len(rows) == 1
        assert rows[0]["date"] == "2024-01-02"

    def test_missing_field_skipped(self):
        values = [{"datetime": "2024-01-01", "open": "100"}]  # missing other fields
        rows = _parse_rows("AAPL", values)
        assert rows == []

    def test_empty_values_returns_empty(self):
        assert _parse_rows("AAPL", []) == []


# ═══════════════════════════════════════════════════════════════════════════════
# TestFetchBatch
# ═══════════════════════════════════════════════════════════════════════════════

class TestFetchBatch:

    def test_single_symbol_returns_values(self):
        body = _single_sym_body("AAPL", 3)
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["AAPL"], outputsize=3)
        assert "AAPL" in result
        assert len(result["AAPL"]) == 3

    def test_multi_symbol_returns_all(self):
        body = _multi_sym_body(["AAPL", "MSFT"], 2)
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["AAPL", "MSFT"], outputsize=2)
        assert "AAPL" in result
        assert "MSFT" in result

    def test_http_error_returns_empty(self):
        mock = _mock_response({}, 500)
        mock.raise_for_status.side_effect = Exception("500")
        with patch("httpx.get", return_value=mock):
            result = _fetch_batch(["AAPL"], outputsize=5)
        assert result == {}

    def test_single_symbol_error_status_returns_empty(self):
        body = {"status": "error", "message": "symbol not found"}
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["ZZZZ"], outputsize=5)
        assert result == {}

    def test_multi_symbol_partial_error_returns_good_symbols(self):
        body = {
            "AAPL": {"meta": {}, "values": _make_td_values(2, "AAPL"), "status": "ok"},
            "ZZZZ": {"status": "error", "message": "symbol not found"},
        }
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["AAPL", "ZZZZ"], outputsize=2)
        assert "AAPL" in result
        assert "ZZZZ" not in result

    def test_correct_params_sent(self):
        body = _single_sym_body("AAPL", 5)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            _fetch_batch(["AAPL"], outputsize=5)
        params = mock_get.call_args[1]["params"]
        assert params["symbol"] == "AAPL"
        assert params["outputsize"] == 5
        assert params["interval"] == "1day"
        assert params["apikey"] == config.TWELVEDATA_API_KEY

    def test_multi_symbol_joined_with_comma(self):
        body = _multi_sym_body(["AAPL", "MSFT"])
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            _fetch_batch(["AAPL", "MSFT"], outputsize=3)
        params = mock_get.call_args[1]["params"]
        assert set(params["symbol"].split(",")) == {"AAPL", "MSFT"}

    def test_httpx_exception_returns_empty(self):
        with patch("httpx.get", side_effect=OSError("network error")):
            result = _fetch_batch(["AAPL"], outputsize=5)
        assert result == {}


# ═══════════════════════════════════════════════════════════════════════════════
# TestFetchIncremental
# ═══════════════════════════════════════════════════════════════════════════════

class TestFetchIncremental:

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "bars.db"))
        db.init_db()

    def test_uses_incremental_days_by_default(self):
        body = _single_sym_body("AAPL", config.TWELVEDATA_INCREMENTAL_DAYS)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_incremental(["AAPL"])
        params = mock_get.call_args[1]["params"]
        assert params["outputsize"] == config.TWELVEDATA_INCREMENTAL_DAYS

    def test_explicit_n_days_overrides_config(self):
        body = _single_sym_body("AAPL", 10)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_incremental(["AAPL"], n_days=10)
        params = mock_get.call_args[1]["params"]
        assert params["outputsize"] == 10

    def test_rows_upserted_to_db(self):
        body = _single_sym_body("AAPL", 3)
        with patch("httpx.get", return_value=_mock_response(body)):
            fetch_incremental(["AAPL"])
        with db.connect() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM daily_bars WHERE symbol = 'AAPL'"
            ).fetchone()[0]
        assert count == 3

    def test_returns_total_rows_upserted(self):
        body = _single_sym_body("AAPL", 4)
        with patch("httpx.get", return_value=_mock_response(body)):
            n = fetch_incremental(["AAPL"])
        assert n == 4

    def test_empty_symbols_returns_zero(self):
        assert fetch_incremental([]) == 0

    def test_http_failure_returns_zero(self):
        mock = _mock_response({}, 500)
        mock.raise_for_status.side_effect = Exception("500")
        with patch("httpx.get", return_value=mock):
            n = fetch_incremental(["AAPL"])
        assert n == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TestFetchFullHistory
# ═══════════════════════════════════════════════════════════════════════════════

class TestFetchFullHistory:

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "bars.db"))
        db.init_db()

    def test_uses_history_days_by_default(self):
        body = _single_sym_body("AAPL", config.TWELVEDATA_HISTORY_DAYS)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_full_history(["AAPL"])
        params = mock_get.call_args[1]["params"]
        assert params["outputsize"] == config.TWELVEDATA_HISTORY_DAYS

    def test_explicit_n_days_overrides_config(self):
        body = _single_sym_body("AAPL", 200)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_full_history(["AAPL"], n_days=200)
        params = mock_get.call_args[1]["params"]
        assert params["outputsize"] == 200

    def test_rows_upserted_to_db(self):
        body = _single_sym_body("MSFT", 5)
        with patch("httpx.get", return_value=_mock_response(body)):
            fetch_full_history(["MSFT"])
        with db.connect() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM daily_bars WHERE symbol = 'MSFT'"
            ).fetchone()[0]
        assert count == 5

    def test_empty_symbols_returns_zero(self):
        assert fetch_full_history([]) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TestBatching
# ═══════════════════════════════════════════════════════════════════════════════

class TestBatching:

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "bars.db"))
        db.init_db()

    def test_symbols_split_into_batches(self, monkeypatch):
        """BATCH_SIZE + 1 symbols → two HTTP requests."""
        symbols = [f"SYM{i:03d}" for i in range(_BATCH_SIZE + 1)]

        call_count = 0
        def _fake_get(url, params, timeout):
            nonlocal call_count
            call_count += 1
            sym_list = params["symbol"].split(",")
            body = _multi_sym_body(sym_list, n=2)
            return _mock_response(body)

        monkeypatch.setattr(td_data, "_INTER_BATCH_DELAY", 0)
        with patch("httpx.get", side_effect=_fake_get):
            fetch_incremental(symbols, n_days=2)

        assert call_count == 2

    def test_single_batch_one_request(self):
        """Fewer than BATCH_SIZE symbols → one request."""
        symbols = ["AAPL", "MSFT"]
        body = _multi_sym_body(symbols, n=2)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_incremental(symbols, n_days=2)
        assert mock_get.call_count == 1

    def test_inter_batch_delay_called(self, monkeypatch):
        """time.sleep called between batches (not before first batch)."""
        symbols = [f"S{i}" for i in range(_BATCH_SIZE + 1)]
        sleep_calls = []
        monkeypatch.setattr(td_data.time, "sleep", lambda s: sleep_calls.append(s))

        def _fake_get(url, params, timeout):
            sym_list = params["symbol"].split(",")
            return _mock_response(_multi_sym_body(sym_list, n=1))

        with patch("httpx.get", side_effect=_fake_get):
            fetch_incremental(symbols, n_days=1)

        assert len(sleep_calls) == 1   # exactly one sleep between 2 batches

    def test_total_rows_across_batches(self, monkeypatch):
        """Total returned = sum of all batches."""
        symbols = [f"S{i:02d}" for i in range(_BATCH_SIZE + 3)]
        monkeypatch.setattr(td_data, "_INTER_BATCH_DELAY", 0)

        def _fake_get(url, params, timeout):
            sym_list = params["symbol"].split(",")
            return _mock_response(_multi_sym_body(sym_list, n=2))

        with patch("httpx.get", side_effect=_fake_get):
            total = fetch_incremental(symbols, n_days=2)

        assert total == len(symbols) * 2
