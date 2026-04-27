"""
db.py — Database abstraction layer.

All DB access in the live system goes through this module.
Switching from SQLite to PostgreSQL requires only config changes — no upstream changes.

Public API
──────────
  connect()            context manager → yields an open, committed-on-exit connection
  init_db()            create all tables (idempotent)
  upsert_daily_bars()  batch upsert rows into daily_bars
  ph()                 parameter placeholder string for the active driver ("?" or "%s")
"""

import contextlib
import sqlite3
from pathlib import Path
from typing import Generator

import config

# ═══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _sqlite_conn() -> sqlite3.Connection:
    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _pg_conn():
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        raise RuntimeError(
            "psycopg2 is required for PostgreSQL: pip install psycopg2-binary"
        )
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


def get_connection():
    """Return a raw open connection for the configured driver."""
    if config.DB_DRIVER == "sqlite":
        return _sqlite_conn()
    if config.DB_DRIVER == "postgresql":
        return _pg_conn()
    raise ValueError(
        f"Unknown DB_DRIVER: {config.DB_DRIVER!r}. Must be 'sqlite' or 'postgresql'."
    )


@contextlib.contextmanager
def connect() -> Generator:
    """
    Context manager that yields an open connection.
    Commits on clean exit; rolls back and re-raises on exception.
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ph() -> str:
    """Parameter placeholder for the active driver: '?' (SQLite) or '%s' (PostgreSQL)."""
    return "?" if config.DB_DRIVER == "sqlite" else "%s"


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════

_TABLES = [
    # Historical and live daily bars (primary data store)
    """
    CREATE TABLE IF NOT EXISTS daily_bars (
        symbol   TEXT NOT NULL,
        date     DATE NOT NULL,
        open     REAL,
        high     REAL,
        low      REAL,
        close    REAL,
        volume   REAL,
        PRIMARY KEY (symbol, date)
    )
    """,
    # Precomputed watchlist (refreshed nightly — not append-only)
    """
    CREATE TABLE IF NOT EXISTS watchlist (
        symbol        TEXT PRIMARY KEY,
        updated_date  DATE NOT NULL,
        sma200        REAL,
        q_threshold   REAL,
        adv63         REAL
    )
    """,
    # Open positions (persisted after every update)
    """
    CREATE TABLE IF NOT EXISTS positions (
        pos_id            TEXT PRIMARY KEY,
        symbol            TEXT    NOT NULL,
        direction         TEXT    NOT NULL,
        entry_date        DATE    NOT NULL,
        fill_price        REAL    NOT NULL,
        shares            INTEGER NOT NULL,
        notional          REAL    NOT NULL,
        bars_held         INTEGER NOT NULL DEFAULT 0,
        equity_at_entry   REAL,
        actual_risk_frac  REAL,
        consec_lows       INTEGER NOT NULL DEFAULT 0,
        ib_order_id       INTEGER,
        order_type        TEXT,
        limit_price       REAL,
        qpi_at_entry      REAL,
        ibs_at_entry      REAL,
        created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Completed trades — column names match backtest trade log exactly
    """
    CREATE TABLE IF NOT EXISTS trade_log (
        pos_id            TEXT,
        symbol            TEXT,
        direction         TEXT,
        entry_date        DATE,
        fill_price        REAL,
        shares            INTEGER,
        notional          REAL,
        bars_held         INTEGER,
        equity_at_entry   REAL,
        actual_risk_frac  REAL,
        exit_price        REAL,
        exit_date         DATE,
        exit_reason       TEXT,
        pnl               REAL,
        commission        REAL,
        created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Daily equity snapshots
    """
    CREATE TABLE IF NOT EXISTS equity_log (
        date                DATE PRIMARY KEY,
        equity_bod          REAL,
        equity_eod          REAL,
        n_open_positions    INTEGER,
        deployed_pct        REAL,
        created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Key-value store for cross-session state (last_order_id, last_universe_update, etc.)
    """
    CREATE TABLE IF NOT EXISTS system_state (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
]


# Additive migrations — each is a no-op if the column already exists.
# Catching the exception is necessary for SQLite < 3.37 (no ADD COLUMN IF NOT EXISTS).
# Data-fix UPDATEs are guarded by the condition they target so they are idempotent.
_MIGRATIONS = [
    "ALTER TABLE positions ADD COLUMN order_type   TEXT",
    "ALTER TABLE positions ADD COLUMN limit_price  REAL",
    "ALTER TABLE positions ADD COLUMN qpi_at_entry REAL",
    "ALTER TABLE positions ADD COLUMN ibs_at_entry REAL",
    # April 9 → April 28 = 13 trading days; bars_held was incorrectly frozen at 9
    "UPDATE positions SET bars_held=13 WHERE pos_id='XOM_2026-04-09' AND bars_held=9",
    "UPDATE positions SET bars_held=13 WHERE pos_id='GD_2026-04-09'  AND bars_held=9",
]


def init_db() -> None:
    """
    Create all tables if they do not exist. Safe to call multiple times.
    Also ensures the state/ directory exists.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        for ddl in _TABLES:
            cur.execute(ddl)
        for stmt in _MIGRATIONS:
            try:
                cur.execute(stmt)
            except Exception:
                pass  # column already exists
        print("Migrations done.")
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# CORE WRITE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_daily_bars(rows: list[dict]) -> int:
    """
    Batch-upsert a list of OHLCV dicts into daily_bars.
    Each dict must have: symbol, date, open, high, low, close, volume.
    Returns the number of rows processed.
    """
    if not rows:
        return 0

    p = ph()

    if config.DB_DRIVER == "sqlite":
        sql = (
            f"INSERT OR REPLACE INTO daily_bars "
            f"(symbol, date, open, high, low, close, volume) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p})"
        )
    else:
        sql = (
            f"INSERT INTO daily_bars (symbol, date, open, high, low, close, volume) "
            f"VALUES ({p},{p},{p},{p},{p},{p},{p}) "
            f"ON CONFLICT (symbol, date) DO UPDATE SET "
            f"open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
            f"close=EXCLUDED.close, volume=EXCLUDED.volume"
        )

    params = [
        (r["symbol"], str(r["date"]), r["open"], r["high"], r["low"], r["close"], r["volume"])
        for r in rows
    ]

    with connect() as conn:
        conn.executemany(sql, params)

    return len(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# SYSTEM STATE  (key-value store)
# ═══════════════════════════════════════════════════════════════════════════════

def get_system_state(key: str) -> str | None:
    """Return the stored value for *key*, or None if absent or on any error."""
    p = ph()
    try:
        with connect() as conn:
            row = conn.execute(
                f"SELECT value FROM system_state WHERE key = {p}", (key,)
            ).fetchone()
        return dict(row)["value"] if row else None
    except Exception:
        return None


def set_system_state(key: str, value: str) -> None:
    """Upsert a key-value pair in system_state."""
    p = ph()
    if config.DB_DRIVER == "sqlite":
        sql = f"INSERT OR REPLACE INTO system_state (key, value) VALUES ({p}, {p})"
    else:
        sql = (
            f"INSERT INTO system_state (key, value) VALUES ({p}, {p}) "
            f"ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value"
        )
    with connect() as conn:
        conn.execute(sql, (key, value))
