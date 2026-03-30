"""
tests/test_portfolio_state.py — Unit tests for portfolio_state.py

Uses a fresh in-memory / temp-file SQLite DB for every test class so tests
are hermetic and do not touch the production database.
"""

import json
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
import db
import portfolio_state


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    """
    Point the entire system at a fresh temp SQLite database for each test.
    Ensures full isolation — no leftover rows between tests.
    """
    db_file = str(tmp_path / "test_state.db")
    monkeypatch.setattr(config, "DB_DRIVER", "sqlite")
    monkeypatch.setattr(config, "DB_PATH", db_file)
    monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)
    db.init_db()


def _make_pos(
    pos_id="POS_001",
    symbol="AAPL",
    direction="long",
    entry_date=date(2024, 1, 10),
    fill_price=150.0,
    shares=100,
    notional=15_000.0,
    bars_held=0,
    equity_at_entry=100_000.0,
    actual_risk_frac=0.15,
    consec_lows=0,
    ib_order_id=None,
) -> dict:
    return {
        "pos_id":           pos_id,
        "symbol":           symbol,
        "direction":        direction,
        "entry_date":       str(entry_date),
        "fill_price":       fill_price,
        "shares":           shares,
        "notional":         notional,
        "bars_held":        bars_held,
        "equity_at_entry":  equity_at_entry,
        "actual_risk_frac": actual_risk_frac,
        "consec_lows":      consec_lows,
        "ib_order_id":      ib_order_id,
    }


def _make_exit(
    exit_price=160.0,
    exit_date=date(2024, 1, 20),
    exit_reason="ibs_exit",
    pnl=1_000.0,
) -> dict:
    return {
        "exit_price":  exit_price,
        "exit_date":   str(exit_date),
        "exit_reason": exit_reason,
        "pnl":         pnl,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TestLoadPositions
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoadPositions:

    def test_empty_table_returns_empty_list(self):
        result = portfolio_state.load_positions()
        assert result == []

    def test_returns_list_of_dicts(self):
        portfolio_state.save_position(_make_pos())
        result = portfolio_state.load_positions()
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], dict)

    def test_all_core_fields_present(self):
        portfolio_state.save_position(_make_pos())
        row = portfolio_state.load_positions()[0]
        for field in ("pos_id", "symbol", "direction", "fill_price", "shares", "notional"):
            assert field in row

    def test_multiple_positions(self):
        portfolio_state.save_position(_make_pos(pos_id="P1", symbol="AAPL"))
        portfolio_state.save_position(_make_pos(pos_id="P2", symbol="MSFT"))
        result = portfolio_state.load_positions()
        assert len(result) == 2
        symbols = {r["symbol"] for r in result}
        assert symbols == {"AAPL", "MSFT"}


# ═══════════════════════════════════════════════════════════════════════════════
# TestSavePosition
# ═══════════════════════════════════════════════════════════════════════════════

class TestSavePosition:

    def test_insert_new_position(self):
        portfolio_state.save_position(_make_pos(pos_id="P1", fill_price=100.0))
        rows = portfolio_state.load_positions()
        assert len(rows) == 1
        assert rows[0]["fill_price"] == 100.0

    def test_upsert_updates_existing(self):
        portfolio_state.save_position(_make_pos(pos_id="P1", bars_held=0))
        portfolio_state.save_position(_make_pos(pos_id="P1", bars_held=5))
        rows = portfolio_state.load_positions()
        assert len(rows) == 1
        assert rows[0]["bars_held"] == 5

    def test_bars_held_default(self):
        pos = _make_pos()
        pos.pop("bars_held")
        portfolio_state.save_position(pos)
        rows = portfolio_state.load_positions()
        # bars_held has DEFAULT 0 in schema
        assert rows[0].get("bars_held", 0) == 0

    def test_ib_order_id_nullable(self):
        portfolio_state.save_position(_make_pos(ib_order_id=None))
        rows = portfolio_state.load_positions()
        assert rows[0]["ib_order_id"] is None

    def test_ib_order_id_set(self):
        portfolio_state.save_position(_make_pos(ib_order_id=42))
        rows = portfolio_state.load_positions()
        assert rows[0]["ib_order_id"] == 42

    def test_consec_lows_persisted(self):
        portfolio_state.save_position(_make_pos(consec_lows=3))
        rows = portfolio_state.load_positions()
        assert rows[0]["consec_lows"] == 3


# ═══════════════════════════════════════════════════════════════════════════════
# TestClosePosition
# ═══════════════════════════════════════════════════════════════════════════════

class TestClosePosition:

    def test_removes_from_positions(self):
        portfolio_state.save_position(_make_pos(pos_id="P1"))
        assert len(portfolio_state.load_positions()) == 1

        portfolio_state.close_position("P1", _make_exit())
        assert portfolio_state.load_positions() == []

    def test_inserts_into_trade_log(self):
        portfolio_state.save_position(_make_pos(pos_id="P1", symbol="AAPL"))
        portfolio_state.close_position("P1", _make_exit(exit_reason="rsi2_exit"))

        with db.connect() as conn:
            rows = conn.execute("SELECT * FROM trade_log").fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row["symbol"] == "AAPL"
        assert row["exit_reason"] == "rsi2_exit"

    def test_trade_log_exit_price(self):
        portfolio_state.save_position(_make_pos(pos_id="P1"))
        portfolio_state.close_position("P1", _make_exit(exit_price=175.5))

        with db.connect() as conn:
            row = dict(conn.execute("SELECT exit_price FROM trade_log").fetchone())
        assert row["exit_price"] == 175.5

    def test_trade_log_pnl(self):
        portfolio_state.save_position(_make_pos(pos_id="P1"))
        portfolio_state.close_position("P1", _make_exit(pnl=2_500.0))

        with db.connect() as conn:
            row = dict(conn.execute("SELECT pnl FROM trade_log").fetchone())
        assert row["pnl"] == 2_500.0

    def test_commission_computed_from_notional(self):
        """If commission not provided, it must equal notional × ROUND_TRIP_COST_BPS / 10_000."""
        notional = 20_000.0
        portfolio_state.save_position(_make_pos(pos_id="P1", notional=notional))
        portfolio_state.close_position("P1", _make_exit())

        expected = notional * (config.ROUND_TRIP_COST_BPS / 10_000)
        with db.connect() as conn:
            row = dict(conn.execute("SELECT commission FROM trade_log").fetchone())
        assert abs(row["commission"] - expected) < 1e-9

    def test_commission_explicit_override(self):
        portfolio_state.save_position(_make_pos(pos_id="P1"))
        exit_data = {**_make_exit(), "commission": 99.0}
        portfolio_state.close_position("P1", exit_data)

        with db.connect() as conn:
            row = dict(conn.execute("SELECT commission FROM trade_log").fetchone())
        assert row["commission"] == 99.0

    def test_missing_pos_id_is_noop(self):
        """close_position on a non-existent pos_id must not raise."""
        portfolio_state.close_position("DOES_NOT_EXIST", _make_exit())
        # Still empty
        assert portfolio_state.load_positions() == []

    def test_original_entry_fields_preserved(self):
        portfolio_state.save_position(_make_pos(pos_id="P1", fill_price=123.45, shares=50))
        portfolio_state.close_position("P1", _make_exit())

        with db.connect() as conn:
            row = dict(conn.execute("SELECT * FROM trade_log").fetchone())
        assert row["fill_price"] == 123.45
        assert row["shares"] == 50

    def test_only_target_position_removed(self):
        portfolio_state.save_position(_make_pos(pos_id="P1", symbol="AAPL"))
        portfolio_state.save_position(_make_pos(pos_id="P2", symbol="MSFT"))
        portfolio_state.close_position("P1", _make_exit())

        remaining = portfolio_state.load_positions()
        assert len(remaining) == 1
        assert remaining[0]["symbol"] == "MSFT"


# ═══════════════════════════════════════════════════════════════════════════════
# TestGetOpenEquity
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetOpenEquity:

    def test_empty_positions_returns_zero(self):
        assert portfolio_state.get_open_equity([], {}) == 0.0

    def test_single_position_at_fill_price(self):
        pos = _make_pos(shares=100, fill_price=150.0)
        result = portfolio_state.get_open_equity([pos], {})
        assert result == 15_000.0   # fallback to fill_price when no close_prices

    def test_single_position_at_current_price(self):
        pos = _make_pos(symbol="AAPL", shares=100, fill_price=150.0)
        result = portfolio_state.get_open_equity([pos], {"AAPL": 160.0})
        assert result == 16_000.0

    def test_multiple_positions(self):
        p1 = _make_pos(pos_id="P1", symbol="AAPL", shares=100, fill_price=150.0)
        p2 = _make_pos(pos_id="P2", symbol="MSFT", shares=50,  fill_price=300.0)
        prices = {"AAPL": 160.0, "MSFT": 320.0}
        result = portfolio_state.get_open_equity([p1, p2], prices)
        assert result == 100 * 160.0 + 50 * 320.0

    def test_missing_price_falls_back_to_fill_price(self):
        pos = _make_pos(symbol="AAPL", shares=10, fill_price=200.0)
        result = portfolio_state.get_open_equity([pos], {})
        assert result == 2_000.0

    def test_partial_prices_dict(self):
        """One symbol has a current price, another falls back to fill_price."""
        p1 = _make_pos(pos_id="P1", symbol="AAPL", shares=10, fill_price=100.0)
        p2 = _make_pos(pos_id="P2", symbol="MSFT", shares=10, fill_price=200.0)
        result = portfolio_state.get_open_equity([p1, p2], {"AAPL": 110.0})
        assert result == 10 * 110.0 + 10 * 200.0


# ═══════════════════════════════════════════════════════════════════════════════
# TestGetTotalEquity
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetTotalEquity:

    def test_cash_only(self):
        assert portfolio_state.get_total_equity(50_000.0, [], {}) == 50_000.0

    def test_cash_plus_position(self):
        pos = _make_pos(shares=100, fill_price=150.0)
        total = portfolio_state.get_total_equity(50_000.0, [pos], {"AAPL": 150.0})
        assert total == 50_000.0 + 15_000.0

    def test_leveraged_total(self):
        pos = _make_pos(symbol="AAPL", shares=200, fill_price=150.0)
        # Cash is 0 — all equity is in positions; leverage ratio > 1
        total = portfolio_state.get_total_equity(0.0, [pos], {"AAPL": 160.0})
        assert total == 32_000.0


# ═══════════════════════════════════════════════════════════════════════════════
# TestAppendEquitySnapshot
# ═══════════════════════════════════════════════════════════════════════════════

class TestAppendEquitySnapshot:

    def test_inserts_row(self):
        d = date(2024, 3, 15)
        portfolio_state.append_equity_snapshot(d, 100_000.0, 101_000.0, 3, 0.45)

        with db.connect() as conn:
            rows = conn.execute("SELECT * FROM equity_log").fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row["equity_eod"] == 101_000.0
        assert row["n_open_positions"] == 3

    def test_upsert_replaces_existing_date(self):
        d = date(2024, 3, 15)
        portfolio_state.append_equity_snapshot(d, 100_000.0, 101_000.0, 3, 0.45)
        portfolio_state.append_equity_snapshot(d, 100_000.0, 102_500.0, 5, 0.55)

        with db.connect() as conn:
            rows = conn.execute("SELECT * FROM equity_log").fetchall()
        assert len(rows) == 1
        assert dict(rows[0])["equity_eod"] == 102_500.0

    def test_multiple_dates(self):
        for i in range(5):
            portfolio_state.append_equity_snapshot(
                date(2024, 3, i + 1), 100_000.0, 100_000.0 + i * 100, i, 0.0
            )
        with db.connect() as conn:
            rows = conn.execute("SELECT * FROM equity_log ORDER BY date").fetchall()
        assert len(rows) == 5

    def test_deployed_pct_stored(self):
        portfolio_state.append_equity_snapshot(date(2024, 1, 1), 0.0, 0.0, 0, 0.72)
        with db.connect() as conn:
            row = dict(conn.execute("SELECT deployed_pct FROM equity_log").fetchone())
        assert abs(row["deployed_pct"] - 0.72) < 1e-9


# ═══════════════════════════════════════════════════════════════════════════════
# TestExportPositionsJson
# ═══════════════════════════════════════════════════════════════════════════════

class TestExportPositionsJson:

    def test_no_file_when_disabled(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", False)
        monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "state" / "bars.db"))
        portfolio_state.export_positions_json()
        assert not (tmp_path / "state" / "positions.json").exists()

    def test_file_created_when_enabled(self, monkeypatch, tmp_path):
        db_path = str(tmp_path / "state" / "bars.db")
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", True)
        monkeypatch.setattr(config, "DB_PATH", db_path)
        db.init_db()

        portfolio_state.save_position(_make_pos(pos_id="P1", symbol="AAPL"))
        portfolio_state.export_positions_json()

        dest = tmp_path / "state" / "positions.json"
        assert dest.exists()

    def test_json_content_matches_positions(self, monkeypatch, tmp_path):
        db_path = str(tmp_path / "state" / "bars.db")
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", True)
        monkeypatch.setattr(config, "DB_PATH", db_path)
        db.init_db()

        portfolio_state.save_position(_make_pos(pos_id="P1", symbol="AAPL", shares=50))
        portfolio_state.save_position(_make_pos(pos_id="P2", symbol="MSFT", shares=30))
        portfolio_state.export_positions_json()

        dest = tmp_path / "state" / "positions.json"
        with open(dest) as fh:
            data = json.load(fh)

        assert len(data) == 2
        symbols = {row["symbol"] for row in data}
        assert symbols == {"AAPL", "MSFT"}

    def test_empty_positions_creates_empty_array(self, monkeypatch, tmp_path):
        db_path = str(tmp_path / "state" / "bars.db")
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", True)
        monkeypatch.setattr(config, "DB_PATH", db_path)
        db.init_db()

        portfolio_state.export_positions_json()

        dest = tmp_path / "state" / "positions.json"
        assert dest.exists()
        with open(dest) as fh:
            data = json.load(fh)
        assert data == []

    def test_export_triggered_by_close_position(self, monkeypatch, tmp_path):
        db_path = str(tmp_path / "state" / "bars.db")
        monkeypatch.setattr(config, "EXPORT_STATE_JSON", True)
        monkeypatch.setattr(config, "DB_PATH", db_path)
        db.init_db()

        portfolio_state.save_position(_make_pos(pos_id="P1"))
        portfolio_state.close_position("P1", _make_exit())

        dest = tmp_path / "state" / "positions.json"
        assert dest.exists()
        with open(dest) as fh:
            data = json.load(fh)
        assert data == []  # position was closed, so list is empty
