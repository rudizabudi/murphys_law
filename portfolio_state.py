"""
portfolio_state.py — Portfolio state persistence.

All position state lives natively in SQLite (positions, trade_log, equity_log).
JSON export is a parallel output only — never read back.

Public API
──────────
  load_positions()                              → list[dict]
  save_position(pos)                            Upsert to positions table
  close_position(pos_id, exit_data)             Move to trade_log, remove from positions
  get_open_equity(positions, close_prices)      Mark-to-market value of open positions
  get_total_equity(cash, positions, close_prices)
  append_equity_snapshot(date, bod, eod, n_pos, deployed_pct)
  export_positions_json()                       Atomic write (only if EXPORT_STATE_JSON=True)
"""

import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path

import config
import db

logger = logging.getLogger("murphy")

# Columns that map directly from the position dict to the positions table.
_POS_COLUMNS = (
    "pos_id",
    "symbol",
    "direction",
    "entry_date",
    "fill_price",
    "shares",
    "notional",
    "bars_held",
    "equity_at_entry",
    "actual_risk_frac",
    "consec_lows",
    "ib_order_id",
    "order_type",
    "limit_price",
    "qpi_at_entry",
    "ibs_at_entry",
)

# Fields that may be updated on an existing position via save_position().
# bars_held, consec_lows, and created_at are intentionally excluded — they are
# managed by explicit targeted UPDATEs so they are never accidentally reset.
_METADATA_COLS = ("ib_order_id", "order_type", "limit_price", "qpi_at_entry", "ibs_at_entry")


# ═══════════════════════════════════════════════════════════════════════════════
# Read
# ═══════════════════════════════════════════════════════════════════════════════

def load_positions() -> list[dict]:
    """
    Return all rows from the positions table as plain dicts.
    Returns an empty list if the table is empty or does not exist yet.
    """
    db.init_db()
    p = db.ph()
    sql = "SELECT * FROM positions"
    try:
        with db.connect() as conn:
            rows = conn.execute(sql).fetchall()
        return [dict(row) for row in rows]
    except Exception as exc:
        logger.error("[portfolio] load_positions failed: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Write — positions
# ═══════════════════════════════════════════════════════════════════════════════

def save_position(pos: dict) -> None:
    """
    Upsert a position dict into the positions table.

    New positions: all supplied columns are inserted.
    Existing positions: only _METADATA_COLS are updated. bars_held, consec_lows,
    and created_at are never overwritten — use an explicit
    ``UPDATE positions SET bars_held=?, consec_lows=? WHERE pos_id=?`` for those.

    pos must contain at minimum: pos_id, symbol, direction, entry_date,
    fill_price, shares, notional.
    """
    db.init_db()
    p = db.ph()

    cols   = [c for c in _POS_COLUMNS if c in pos]
    values = [pos[c] for c in cols]

    upd_cols   = [c for c in _METADATA_COLS if c in pos]
    upd_values = [pos[c] for c in upd_cols] + ([pos["pos_id"]] if upd_cols else [])

    if config.DB_DRIVER == "sqlite":
        placeholders = ", ".join(p for _ in cols)
        insert_sql = (
            f"INSERT OR IGNORE INTO positions ({', '.join(cols)}) "
            f"VALUES ({placeholders})"
        )
        update_sql = (
            f"UPDATE positions "
            f"SET {', '.join(f'{c}={p}' for c in upd_cols)} "
            f"WHERE pos_id={p}"
        ) if upd_cols else None
        with db.connect() as conn:
            conn.execute(insert_sql, values)
            if update_sql:
                conn.execute(update_sql, upd_values)
    else:
        placeholders = ", ".join(p for _ in cols)
        if upd_cols:
            updates     = ", ".join(f"{c}=EXCLUDED.{c}" for c in upd_cols)
            on_conflict = f"DO UPDATE SET {updates}"
        else:
            on_conflict = "DO NOTHING"
        sql = (
            f"INSERT INTO positions ({', '.join(cols)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT (pos_id) {on_conflict}"
        )
        with db.connect() as conn:
            conn.execute(sql, values)

    logger.debug("[portfolio] save_position %s", pos.get("pos_id"))


def close_position(pos_id: str, exit_data: dict) -> None:
    """
    Move a position to trade_log and delete it from positions.

    exit_data must contain: exit_price, exit_date, exit_reason, pnl.
    commission is computed from notional × (ROUND_TRIP_COST_BPS / 10_000)
    if not supplied in exit_data.

    The row is first loaded from positions so that all original fields are
    preserved in trade_log.
    """
    db.init_db()
    p = db.ph()

    # Load the live position row
    with db.connect() as conn:
        row = conn.execute(
            f"SELECT * FROM positions WHERE pos_id = {p}", (pos_id,)
        ).fetchone()

    if row is None:
        logger.warning("[portfolio] close_position: pos_id %r not found", pos_id)
        return

    pos = dict(row)

    commission = exit_data.get(
        "commission",
        pos.get("notional", 0.0) * (config.ROUND_TRIP_COST_BPS / 10_000),
    )

    log_cols = (
        "pos_id", "symbol", "direction", "entry_date", "fill_price",
        "shares", "notional", "bars_held", "equity_at_entry", "actual_risk_frac",
        "exit_price", "exit_date", "exit_reason", "pnl", "commission",
    )
    log_values = (
        pos.get("pos_id"),
        pos.get("symbol"),
        pos.get("direction"),
        pos.get("entry_date"),
        pos.get("fill_price"),
        pos.get("shares"),
        pos.get("notional"),
        pos.get("bars_held", 0),
        pos.get("equity_at_entry"),
        pos.get("actual_risk_frac"),
        exit_data.get("exit_price"),
        exit_data.get("exit_date"),
        exit_data.get("exit_reason"),
        exit_data.get("pnl"),
        commission,
    )

    placeholders = ", ".join(p for _ in log_cols)
    insert_sql = (
        f"INSERT INTO trade_log ({', '.join(log_cols)}) "
        f"VALUES ({placeholders})"
    )
    delete_sql = f"DELETE FROM positions WHERE pos_id = {p}"

    with db.connect() as conn:
        conn.execute(insert_sql, log_values)
        conn.execute(delete_sql, (pos_id,))

    logger.debug(
        "[portfolio] close_position %s → trade_log (%s)",
        pos_id,
        exit_data.get("exit_reason"),
    )

    if config.EXPORT_STATE_JSON:
        export_positions_json()


# ═══════════════════════════════════════════════════════════════════════════════
# Mark-to-market helpers
# ═══════════════════════════════════════════════════════════════════════════════

def get_open_equity(
    positions: list[dict],
    close_prices: dict[str, float],
) -> float:
    """
    Mark-to-market value of all open positions.

    close_prices: {symbol: current_close}
    Falls back to fill_price when a symbol has no current price.
    """
    total = 0.0
    for pos in positions:
        price = close_prices.get(pos["symbol"], pos.get("fill_price", 0.0))
        total += int(pos.get("shares", 0)) * float(price)
    return total


def get_total_equity(
    cash: float,
    positions: list[dict],
    close_prices: dict[str, float],
) -> float:
    """
    Total account equity: cash + mark-to-market open positions.
    Mirrors the backtest's `cash + current_position_value()`.
    """
    return cash + get_open_equity(positions, close_prices)


# ═══════════════════════════════════════════════════════════════════════════════
# Equity log
# ═══════════════════════════════════════════════════════════════════════════════

def append_equity_snapshot(
    snap_date: date,
    equity_bod: float,
    equity_eod: float,
    n_open_positions: int,
    deployed_pct: float,
) -> None:
    """
    Upsert a daily equity snapshot into equity_log.
    If a row already exists for snap_date it is replaced.
    """
    db.init_db()
    p = db.ph()

    if config.DB_DRIVER == "sqlite":
        sql = (
            f"INSERT OR REPLACE INTO equity_log "
            f"(date, equity_bod, equity_eod, n_open_positions, deployed_pct) "
            f"VALUES ({p},{p},{p},{p},{p})"
        )
    else:
        sql = (
            f"INSERT INTO equity_log "
            f"(date, equity_bod, equity_eod, n_open_positions, deployed_pct) "
            f"VALUES ({p},{p},{p},{p},{p}) "
            f"ON CONFLICT (date) DO UPDATE SET "
            f"equity_bod=EXCLUDED.equity_bod, equity_eod=EXCLUDED.equity_eod, "
            f"n_open_positions=EXCLUDED.n_open_positions, "
            f"deployed_pct=EXCLUDED.deployed_pct"
        )

    with db.connect() as conn:
        conn.execute(sql, (str(snap_date), equity_bod, equity_eod, n_open_positions, deployed_pct))

    logger.debug("[portfolio] equity snapshot %s eod=%.2f", snap_date, equity_eod)


# ═══════════════════════════════════════════════════════════════════════════════
# JSON export (optional)
# ═══════════════════════════════════════════════════════════════════════════════

def export_positions_json() -> None:
    """
    Atomically write all open positions to state/positions.json.
    Uses write-to-temp-then-rename so the file is always either complete
    or absent — never a partial write.

    Only called when config.EXPORT_STATE_JSON is True.
    """
    if not config.EXPORT_STATE_JSON:
        return

    positions = load_positions()

    # Coerce any non-JSON-serialisable types (dates, etc.) to strings
    def _serialise(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")

    dest = Path(config.DB_PATH).parent / "positions.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=dest.parent, suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(positions, fh, indent=2, default=_serialise)
        os.replace(tmp_path, dest)
        logger.debug("[portfolio] positions.json updated (%d rows)", len(positions))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
