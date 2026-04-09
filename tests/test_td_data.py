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
    _fetch_batch,
    _inter_batch_delay,
    _parse_rows,
    fetch_bars,
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
        symbols = [f"SYM{i:03d}" for i in range(config.TWELVEDATA_BATCH_SIZE + 1)]

        call_count = 0
        def _fake_get(url, params, timeout):
            nonlocal call_count
            call_count += 1
            sym_list = params["symbol"].split(",")
            body = _multi_sym_body(sym_list, n=2)
            return _mock_response(body)

        monkeypatch.setattr(td_data, "_inter_batch_delay", lambda: 0)
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
        symbols = [f"S{i}" for i in range(config.TWELVEDATA_BATCH_SIZE + 1)]
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
        symbols = [f"S{i:02d}" for i in range(config.TWELVEDATA_BATCH_SIZE + 3)]
        monkeypatch.setattr(td_data, "_inter_batch_delay", lambda: 0)

        def _fake_get(url, params, timeout):
            sym_list = params["symbol"].split(",")
            return _mock_response(_multi_sym_body(sym_list, n=2))

        with patch("httpx.get", side_effect=_fake_get):
            total = fetch_incremental(symbols, n_days=2)

        assert total == len(symbols) * 2


# ═══════════════════════════════════════════════════════════════════════════════
# TestFetchBars
# ═══════════════════════════════════════════════════════════════════════════════

class TestFetchBars:

    @pytest.fixture(autouse=True)
    def reset_rate_limiter(self, monkeypatch):
        """Reset module-level rate-limiter state and suppress sleep between tests."""
        monkeypatch.setattr(td_data, "_last_request_time", 0.0)
        monkeypatch.setattr(td_data.time, "sleep", MagicMock())

    def test_returns_list_for_valid_symbol(self):
        body = _single_sym_body("AAPL", 3)
        with patch("httpx.get", return_value=_mock_response(body)):
            result = fetch_bars("AAPL", n_days=3)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_all_ohlcv_fields_present(self):
        body = _single_sym_body("AAPL", 1)
        with patch("httpx.get", return_value=_mock_response(body)):
            result = fetch_bars("AAPL", n_days=1)
        row = result[0]
        for field in ("symbol", "date", "open", "high", "low", "close", "volume"):
            assert field in row

    def test_returns_none_on_http_error(self):
        mock = _mock_response({}, 500)
        mock.raise_for_status.side_effect = Exception("500")
        with patch("httpx.get", return_value=mock):
            result = fetch_bars("AAPL", n_days=5)
        assert result is None

    def test_returns_none_when_symbol_not_in_response(self):
        # Multi-symbol response format that contains only MSFT, not AAPL
        body = _multi_sym_body(["MSFT"], 3)
        with patch("httpx.get", return_value=_mock_response(body)):
            result = fetch_bars("AAPL", n_days=3)
        assert result is None

    def test_uses_correct_n_days(self):
        body = _single_sym_body("AAPL", 10)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_bars("AAPL", n_days=10)
        params = mock_get.call_args[1]["params"]
        assert params["outputsize"] == 10

    def test_symbol_field_in_rows(self):
        body = _single_sym_body("TSLA", 2)
        with patch("httpx.get", return_value=_mock_response(body)):
            result = fetch_bars("TSLA", n_days=2)
        assert all(r["symbol"] == "TSLA" for r in result)


# ═══════════════════════════════════════════════════════════════════════════════
# TestRateLimiter
# ═══════════════════════════════════════════════════════════════════════════════

class TestRateLimiter:
    """
    Verify fetch_bars() rate-limits requests to TWELVEDATA_RATE_LIMIT_PER_MIN.
    time.time and time.sleep are both mocked for deterministic assertions.
    """

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch):
        monkeypatch.setattr(td_data, "_last_request_time", 0.0)
        self.sleep_calls: list[float] = []
        monkeypatch.setattr(td_data.time, "sleep", lambda s: self.sleep_calls.append(s))
        # Default: successful response for AAPL
        body = _single_sym_body("AAPL", 1)
        monkeypatch.setattr(
            "httpx.get", lambda *a, **kw: _mock_response(body)
        )

    def test_no_sleep_when_last_request_is_old(self, monkeypatch):
        """_last_request_time = 0.0 (epoch) → elapsed is huge → no sleep."""
        fetch_bars("AAPL", n_days=1)
        assert self.sleep_calls == []

    def test_sleep_fires_when_under_interval(self, monkeypatch):
        """_last_request_time = just now → elapsed < interval → sleep called."""
        import time as _time
        monkeypatch.setattr(td_data, "_last_request_time", _time.time())
        fetch_bars("AAPL", n_days=1)
        assert len(self.sleep_calls) == 1
        assert self.sleep_calls[0] > 0

    def test_sleep_duration_equals_remaining_interval(self, monkeypatch):
        """Sleep amount = min_interval - elapsed."""
        fake_now    = 1_000.0
        last_req    = 999.0          # 1 second ago
        monkeypatch.setattr(td_data, "_last_request_time", last_req)
        monkeypatch.setattr(td_data.time, "time", lambda: fake_now)

        fetch_bars("AAPL", n_days=1)

        min_interval   = 60.0 / config.TWELVEDATA_RATE_LIMIT_PER_MIN
        expected_sleep = min_interval - (fake_now - last_req)
        assert self.sleep_calls == [pytest.approx(expected_sleep)]

    def test_no_sleep_when_exactly_at_interval(self, monkeypatch):
        """Elapsed == min_interval exactly → no sleep."""
        min_interval = 60.0 / config.TWELVEDATA_RATE_LIMIT_PER_MIN
        fake_now     = 1_000.0
        last_req     = fake_now - min_interval   # exactly the interval ago
        monkeypatch.setattr(td_data, "_last_request_time", last_req)
        monkeypatch.setattr(td_data.time, "time", lambda: fake_now)

        fetch_bars("AAPL", n_days=1)

        assert self.sleep_calls == []

    def test_respects_custom_rate_limit(self, monkeypatch):
        """Doubling RATE_LIMIT_PER_MIN halves the interval → sleep is shorter."""
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 16)
        fake_now = 1_000.0
        last_req = 999.0
        monkeypatch.setattr(td_data, "_last_request_time", last_req)
        monkeypatch.setattr(td_data.time, "time", lambda: fake_now)

        fetch_bars("AAPL", n_days=1)

        min_interval   = 60.0 / 16
        expected_sleep = min_interval - (fake_now - last_req)
        assert self.sleep_calls == [pytest.approx(expected_sleep)]

    def test_last_request_time_updated_after_call(self, monkeypatch):
        """_last_request_time is updated so subsequent calls can enforce the limit."""
        fetch_bars("AAPL", n_days=1)
        assert td_data._last_request_time > 0.0

    def test_sleep_not_called_for_batch_functions(self, monkeypatch):
        """fetch_incremental uses _fetch_batch directly — rate limiter does not fire."""
        import time as _time
        # Set _last_request_time to just now, so fetch_bars would sleep
        monkeypatch.setattr(td_data, "_last_request_time", _time.time())

        body = _single_sym_body("AAPL", 1)
        with patch("httpx.get", return_value=_mock_response(body)):
            # fetch_incremental does NOT go through fetch_bars
            fetch_incremental(["AAPL"], n_days=1)

        # The rate-limiter sleep must not have fired (only _inter_batch_delay, which
        # is not triggered for a single batch)
        assert self.sleep_calls == []


# ═══════════════════════════════════════════════════════════════════════════════
# TestTopLevelErrorResponse
# ═══════════════════════════════════════════════════════════════════════════════

class TestTopLevelErrorResponse:
    """_fetch_batch: top-level API error responses are handled gracefully."""

    def test_top_level_status_error_returns_empty(self):
        """Response with top-level status='error' → empty dict returned."""
        body = {"status": "error", "message": "Invalid API key", "code": 401}
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["AAPL"], outputsize=5)
        assert result == {}

    def test_top_level_status_error_logs_warning(self, caplog):
        """Response with top-level status='error' → WARNING logged with message."""
        body = {"status": "error", "message": "Invalid API key", "code": 401}
        with patch("httpx.get", return_value=_mock_response(body)):
            import logging
            with caplog.at_level(logging.WARNING, logger="murphy"):
                _fetch_batch(["AAPL"], outputsize=5)
        assert any("Invalid API key" in r.message for r in caplog.records)

    def test_top_level_code_key_returns_empty(self):
        """Response containing a top-level 'code' key (even without status='error') → empty dict."""
        body = {"code": 400, "message": "symbol not found", "status": "error"}
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["ZZZZ"], outputsize=5)
        assert result == {}

    def test_top_level_code_without_message_logs_warning(self, caplog):
        """Response with code but no message → WARNING still logged."""
        body = {"code": 403, "status": "error"}  # non-429 so rate-limit retry is not triggered
        with patch("httpx.get", return_value=_mock_response(body)):
            import logging
            with caplog.at_level(logging.WARNING, logger="murphy"):
                _fetch_batch(["AAPL"], outputsize=5)
        assert any(r.levelname == "WARNING" for r in caplog.records)


# ═══════════════════════════════════════════════════════════════════════════════
# TestBatchSizeConfig
# ═══════════════════════════════════════════════════════════════════════════════

class TestBatchSizeConfig:
    """Batch size reads from config.TWELVEDATA_BATCH_SIZE at call time."""

    @pytest.fixture(autouse=True)
    def fresh_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "bars.db"))
        db.init_db()

    def test_batch_size_from_config(self, monkeypatch):
        """With TWELVEDATA_BATCH_SIZE=3, 4 symbols → 2 HTTP requests."""
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE", 3)
        monkeypatch.setattr(td_data, "_inter_batch_delay", lambda: 0)

        symbols = ["A", "B", "C", "D"]
        call_count = 0

        def _fake_get(url, params, timeout):
            nonlocal call_count
            call_count += 1
            sym_list = params["symbol"].split(",")
            return _mock_response(_multi_sym_body(sym_list, n=1))

        with patch("httpx.get", side_effect=_fake_get):
            fetch_incremental(symbols, n_days=1)

        assert call_count == 2  # ceil(4/3) = 2

    def test_larger_batch_size_fewer_requests(self, monkeypatch):
        """With TWELVEDATA_BATCH_SIZE=10, 5 symbols → 1 HTTP request."""
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE", 10)
        monkeypatch.setattr(td_data, "_inter_batch_delay", lambda: 0)

        symbols = [f"S{i}" for i in range(5)]
        body = _multi_sym_body(symbols, n=2)
        with patch("httpx.get", return_value=_mock_response(body)) as mock_get:
            fetch_incremental(symbols, n_days=2)

        assert mock_get.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# TestInterBatchDelay
# ═══════════════════════════════════════════════════════════════════════════════

class TestInterBatchDelay:
    """_inter_batch_delay() reflects current config values at call time."""

    def test_delay_equals_batch_over_rate_times_60(self, monkeypatch):
        """delay = (BATCH_SIZE / RATE_LIMIT) * 60."""
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",     8)
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 8)
        assert _inter_batch_delay() == pytest.approx(60.0)

    def test_delay_reflects_changed_batch_size(self, monkeypatch):
        """Doubling batch size doubles the delay."""
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",     16)
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 8)
        assert _inter_batch_delay() == pytest.approx(120.0)

    def test_delay_reflects_changed_rate_limit(self, monkeypatch):
        """Doubling rate limit halves the delay."""
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",     8)
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 16)
        assert _inter_batch_delay() == pytest.approx(30.0)

    def test_delay_computed_at_call_time(self, monkeypatch):
        """Changing config after import is reflected in _inter_batch_delay()."""
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",     4)
        monkeypatch.setattr(config, "TWELVEDATA_RATE_LIMIT_PER_MIN", 8)
        first = _inter_batch_delay()
        monkeypatch.setattr(config, "TWELVEDATA_BATCH_SIZE",     8)
        second = _inter_batch_delay()
        assert second == pytest.approx(first * 2)


# ═══════════════════════════════════════════════════════════════════════════════
# TestRateLimitRetry
# ═══════════════════════════════════════════════════════════════════════════════

class TestRateLimitRetry:
    """_fetch_batch: rate-limit responses trigger a 60s sleep + one retry."""

    _RATE_LIMIT_BODY_429  = {"code": 429,   "status": "error", "message": "You have run out of API credits"}
    _RATE_LIMIT_BODY_MSG  = {"code": 400,   "status": "error", "message": "You have run out of API credits for the current minute"}
    _GOOD_BODY            = None  # set per-test

    @pytest.fixture(autouse=True)
    def no_real_sleep(self, monkeypatch):
        self.sleep_args: list[float] = []
        monkeypatch.setattr(td_data.time, "sleep", lambda s: self.sleep_args.append(s))

    def _good_body(self, symbol="AAPL"):
        return _single_sym_body(symbol, 3)

    def test_rate_limit_429_triggers_sleep(self):
        """code=429 response → time.sleep(60) called before retry."""
        responses = [
            _mock_response(self._RATE_LIMIT_BODY_429),
            _mock_response(self._good_body()),
        ]
        with patch("httpx.get", side_effect=responses):
            _fetch_batch(["AAPL"], outputsize=3)
        assert len(self.sleep_args) == 1
        assert self.sleep_args[0] == 60

    def test_rate_limit_message_triggers_sleep(self):
        """'run out of API credits' message → sleep triggered."""
        responses = [
            _mock_response(self._RATE_LIMIT_BODY_MSG),
            _mock_response(self._good_body()),
        ]
        with patch("httpx.get", side_effect=responses):
            _fetch_batch(["AAPL"], outputsize=3)
        assert len(self.sleep_args) == 1

    def test_successful_retry_returns_data(self):
        """After rate-limit sleep, successful retry returns the expected data."""
        responses = [
            _mock_response(self._RATE_LIMIT_BODY_429),
            _mock_response(self._good_body()),
        ]
        with patch("httpx.get", side_effect=responses):
            result = _fetch_batch(["AAPL"], outputsize=3)
        assert "AAPL" in result
        assert len(result["AAPL"]) == 3

    def test_double_rate_limit_returns_empty(self):
        """Two consecutive rate-limit responses → empty dict returned."""
        responses = [
            _mock_response(self._RATE_LIMIT_BODY_429),
            _mock_response(self._RATE_LIMIT_BODY_429),
        ]
        with patch("httpx.get", side_effect=responses):
            result = _fetch_batch(["AAPL", "MSFT"], outputsize=3)
        assert result == {}

    def test_double_rate_limit_logs_warning_with_symbols(self, caplog):
        """On double failure, WARNING is logged listing the affected symbols."""
        responses = [
            _mock_response(self._RATE_LIMIT_BODY_429),
            _mock_response(self._RATE_LIMIT_BODY_429),
        ]
        import logging
        with patch("httpx.get", side_effect=responses):
            with caplog.at_level(logging.WARNING, logger="murphy"):
                _fetch_batch(["AAPL", "MSFT"], outputsize=3)
        # The retry-failure log must mention the symbols
        assert any("AAPL" in r.message for r in caplog.records)

    def test_rate_limit_only_retries_once(self):
        """Only one retry attempt is made — httpx.get called exactly twice."""
        responses = [
            _mock_response(self._RATE_LIMIT_BODY_429),
            _mock_response(self._RATE_LIMIT_BODY_429),
            _mock_response(self._good_body()),  # third call must never happen
        ]
        with patch("httpx.get", side_effect=responses) as mock_get:
            _fetch_batch(["AAPL"], outputsize=3)
        assert mock_get.call_count == 2

    def test_non_rate_limit_error_does_not_retry(self):
        """A generic API error (not rate limit) returns empty without sleeping."""
        body = {"code": 400, "status": "error", "message": "symbol not found"}
        with patch("httpx.get", return_value=_mock_response(body)):
            result = _fetch_batch(["ZZZZ"], outputsize=3)
        assert result == {}
        assert self.sleep_args == []
