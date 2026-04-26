# Murphy's Law — Live Implementation Roadmap
> Strategy: Mean Reversion · Long-only · Daily bars · S&P 500 universe  
> Backtest reference: v3.0.0 · CAGR 43.3% · Sharpe 1.54 · MaxDD −20.2%  
> Broker: Interactive Brokers (TWS / IB Gateway)  
> Language: Python 3.11+

---

## Session Guide for Claude Code

**Read this before starting any session.**

All times in this system are **New York time (ET)** — `zoneinfo.ZoneInfo("America/New_York")` — regardless of host machine timezone.

### Backtest Reference Fidelity

`reference/v300.py` is the canonical implementation of this strategy. It must be read in full before writing any code. The live system is a faithful port of that backtest — not a rewrite, not an interpretation.

**Default rule: if the backtest does it a certain way, the live system does it the same way.** This applies to indicator calculations, signal conditions, exit priority order, sizing formulas, gate checks, and edge case handling. Do not improve, simplify, or refactor backtest logic unless it is explicitly listed as a permitted divergence below.

The only points where the live system intentionally diverges from the backtest are:

| # | Topic | Backtest | Live system |
|---|---|---|---|
| 1 | Data source | Local `*_5min.json` files resampled to daily | SQLite `daily_bars` table (seeded from same files via `migrate.py`, updated nightly via TwelveData) |
| 2 | Today's bar | Not applicable (historical simulation) | IB `reqMktData(snapshot=True)` at 15:40 ET for watchlist symbols; merged with DB history before indicator computation |
| 3 | Execution | Fill assumed at bar close, no order type | Entries: LOC at `snap_price × (1 + ENTRY_LOC_BUFFER_PCT)`; Exits: plain MOC |
| 4 | Universe filtering | All symbols with a `*_5min.json` file | Two-stage: nightly watchlist precompute (price-independent conditions) + 15:40 price-sensitive check |
| 5 | Position state | In-memory `open_positions` list | SQLite `positions` table, persisted after every update |
| 6 | Equity | Computed from in-memory cash + MTM | IB `net_liquidation` as primary equity source; cross-checked against computed value |
| 7 | Risk controls | None | `risk_engine.py` — all controls configurable and additive; do not interfere with backtest logic |
| 8 | Costs | `ROUND_TRIP_COST_BPS` applied at exit | Same formula; commission recorded in `trade_log` table |

Everything else — `calc_rsi2()`, `compute_indicators()`, `collect_signals()`, the deterioration stop counter, the sizing formula, the total notional gate, the liquidity gate, the exit priority chain — must be a line-for-line port from `reference/v300.py`.

### Build Order

Modules are listed dependency-first. Never implement a module before its dependencies exist.

```
Tier 0 — No dependencies (build first)
  config.py
  db.py

Tier 1 — Depends on Tier 0
  migrate.py        ← must be RUN once before anything else can function
  indicators.py
  monitor.py
  risk_engine.py
  universe.py       ← depends only on config.py, db.py, and httpx

Tier 2 — Depends on Tier 1
  signals.py
  portfolio_state.py
  ib_exec.py

Tier 3 — Depends on Tier 2
  order_manager.py

Tier 4 — Depends on Tier 3
  main.py
  scheduler.py
```

### Session Scoping

Scope each Claude Code session to **one module at a time**. Example prompt:
> "Read ROADMAP.md. We are implementing `db.py` per the spec in Section 4. Do not touch any other module."

### Data Bootstrap

Historical data is provided as 5-min OHLCV JSON files in `/data` (same format as the backtest: `SYMBOL_5min.json`). These are the **only input** to `migrate.py`. After migration, `/data` is never read by the live system again. All historical data lives in SQL.

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Repository Layout](#2-repository-layout)
3. [Prerequisites & Setup](#3-prerequisites--setup)
4. [Module Specifications](#4-module-specifications)
   - 4.1 [Config](#41-config)
   - 4.2 [Data Pipeline](#42-data-pipeline)
     - 4.2.6 [Universe Management](#426-universe-management)
   - 4.3 [Indicator Engine](#43-indicator-engine)
   - 4.4 [Signal Generator](#44-signal-generator)
   - 4.5 [Portfolio State](#45-portfolio-state)
   - 4.6 [Order Manager](#46-order-manager)
   - 4.7 [Execution Bridge (IB)](#47-execution-bridge-ib)
   - 4.8 [Scheduler](#48-scheduler)
   - 4.9 [Logger & Monitor](#49-logger--monitor)
5. [Execution Timing & MOC/LOC](#5-execution-timing--mocloc)
6. [Position Sizing (Live)](#6-position-sizing-live)
7. [Risk Controls](#7-risk-controls)
8. [State Persistence](#8-state-persistence)
9. [Known Gaps & Limitations](#9-known-gaps--limitations)

---

## 1. Architecture Overview

```
                        ┌──────────────────────────────────────┐
                        │           Daily Scheduler            │
                        │  ~20:00 ET : TwelveData nightly sync │
                        │             + watchlist precompute   │
                        │   15:40 ET : IB intraday snap        │
                        │   15:44 ET : submit LOC/MOC orders   │
                        │   16:10 ET : confirm fills + update  │
                        └──────────────┬───────────────────────┘
                                       │
              ┌────────────────────────▼──────────────────────┐
              │              Orchestrator (main.py)           │
              └──┬──────────────┬──────────────┬──────────────┘
                 │              │              │
         ┌───────▼──────┐  ┌───▼───────┐  ┌───▼──────────┐
         │ Data Pipeline│  │  Signal   │  │  Portfolio   │
         │              │  │  Engine   │  │    State     │
         │ ┌──────────┐ │  │(signals.py│  │ (state.py)   │
         │ │TwelveData│ │  └───────────┘  └──────┬───────┘
         │ │(history) │ │                        │
         │ └──────────┘ │               ┌────────▼───────┐
         │ ┌──────────┐ │               │  Order Manager │
         │ │IB API    │ │               │  (orders.py)   │
         │ │(today's  │ │               └────────┬───────┘
         │ │intraday) │ │                        │
         │ └──────────┘ │               ┌────────▼───────┐
         └──────────────┘               │   IB Exec      │
                                        │  (ib_exec.py)  │
                                        └────────────────┘
```

**Data pipeline split:**
- **TwelveData** — nightly incremental update of the local SQLite daily bar database (~20:00 ET). Provides the full historical depth needed for SMA(200), the 252-bar QPI rolling window, and the 63-day ADV. Also triggers the watchlist precompute step.
- **IB API** — at 15:40 ET, fetches today's intraday snapshot for **watchlist symbols only** (not all 509). This is a `reqMktData(snapshot=True)` call — not a historical data request — and is not subject to IB's 60 req/10min pacing limit. Today's bar is merged with the TwelveData history before price-sensitive indicators are computed.

**Key design principles:**
- The live system mirrors the backtest's two-pass architecture. Pass 1 (Signal Engine) is a direct port of `compute_indicators()` + `collect_signals()`. Pass 2 (Portfolio State + Order Manager) is a direct port of `simulate_portfolio()`, operating on live state.
- All modules communicate through plain data structures (dicts, DataFrames). No shared global state. Each component is independently testable.
- `config.py` is the single source of truth for all parameters. No hardcoded values anywhere else.

**Future extension points** *(not built in v1)*:
- **SQL trade log** — `state/` DB tables are designed to be drop-in replaceable with a PostgreSQL backend without changing any upstream module.
- **REST API / remote control** — the Orchestrator is a callable Python module, not a script. Wrapping it in a FastAPI layer later requires no refactoring.
- **Web dashboard** — the daily report in `monitor.py` is structured data first, formatted string second. Can be redirected to a web frontend without changes to reporting logic.

---

## 2. Repository Layout

```
murphy_law/
├── config.py               # All parameters — single source of truth
├── main.py                 # Orchestrator — daily entry point
├── db.py                   # Database abstraction layer (SQLite / PostgreSQL)
├── migrate.py              # One-time seeder: /data JSON → SQL daily_bars
├── ib_data.py              # IB intraday snapshot fetching
├── indicators.py           # compute_indicators() — exact port from backtest
├── signals.py              # collect_signals() — exact port from backtest
├── portfolio_state.py      # Open position tracking, equity calc, state I/O
├── order_manager.py        # Sizing, gate checks, LOC/MOC order construction
├── ib_exec.py              # IB TWS/Gateway connection, IBC control, order submission
├── risk_engine.py          # All risk controls — single evaluate() interface
├── monitor.py              # Logging, alerts, daily/weekly reports
├── scheduler.py            # APScheduler — all scheduled jobs
├── universe.py             # S&P 500 constituent fetch, diff, and new-symbol detection
├── state/
│   ├── bars.db             # SQLite database (all historical + live state)
│   ├── universe.csv        # S&P 500 symbol list
│   └── positions.json      # Optional JSON export (if EXPORT_STATE_JSON=True)
├── data/
│   └── *_5min.json         # Input for migrate.py only — never read after migration
├── logs/
│   └── murphy_YYYYMMDD.log # Rotating daily log (if LOG_TO_FILE=True)
├── reference/
│   └── v300.py             # Backtest source — reference for parity checks
└── tests/
    ├── test_indicators.py      # Parity checks against backtest outputs
    ├── test_monitor.py
    ├── test_risk_engine.py
    ├── test_signals.py
    ├── test_portfolio_state.py
    ├── test_ib_exec.py
    ├── test_order_manager.py
    ├── test_td_data.py
    ├── test_ib_data.py
    ├── test_main.py
    ├── test_scheduler.py
    └── test_universe.py
```

---

## 3. Prerequisites & Setup

### 3.1 Broker
- IB account with **margin enabled** (strategy uses up to 1.5× leverage)
- TWS or IB Gateway running, API enabled — port is a config parameter (`IB_PORT`)
- IBC installed for automated login and weekly 2FA handling

### 3.2 Data Sources

**TwelveData** — nightly incremental update (~20:00 ET) of the `daily_bars` SQL table. Provides the full historical depth required: SMA(200), 252-bar QPI rolling window, 63-day ADV.

**IB API (snapshot)** — at 15:40 ET, fetches today's partial bar for watchlist symbols only via `reqMktData(snapshot=True)`. Not subject to the 60 req/10min historical data pacing limit. At ~50 concurrent threads, watchlist symbols (typically 40–80) resolve in well under 30 seconds.

**Initial data bootstrap** — 5-min OHLCV JSON files in `/data` (same format as backtest). Consumed once by `migrate.py` to populate `daily_bars`. After that, TwelveData handles all updates and `/data` is never read again.

### 3.3 IB API Approach
Official synchronous `ibapi` library with Python threading. Standard `EClient` + `EWrapper` subclass, `run()` in a daemon thread, responses synchronized via `queue.Queue`. Well-suited for a once-daily batch workflow.

### 3.4 Database
SQLite for v1. All DB access is abstracted behind `db.py` — switching to PostgreSQL requires changing two config values, nothing else.

### 3.5 Python Environment
```
python                    >= 3.11
ibapi                     # Official IB Python API
httpx                     >= 0.27      # TwelveData HTTP requests
pandas                    >= 2.0
numpy                     >= 1.25
apscheduler               >= 3.10
pandas_market_calendars   # Holiday / trading day checks
sqlite3                   # stdlib
zoneinfo                  # stdlib
```

---

## 4. Module Specifications

### 4.1 Config

`config.py` is the single source of truth for the entire system. All infrastructure parameters live here alongside the backtest-parity strategy parameters. No other module contains hardcoded values.

```python
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")   # All times in this system are NY time

# ── Backtest-parity parameters (do not change without re-backtesting) ──────────
ENTRY_N_DAY_RETURN        = 3
RETURN_RANK_RANGE         = 252
ENTRY_TRIGGER             = 0.1
IBS_ENTRY_FILTER          = 0.20
IBS_EXIT_FILTER           = 0.90
RSI_EXIT_FILTER           = 90.0
MAX_HOLDING_PERIOD        = 15
STOP_MIN_BARS             = 15
STOP_CONSEC_LOWS          = 9
SMA_PERIOD                = 200
MAX_POSITIONS             = 15
MAX_TOTAL_NOTIONAL        = 1.5
MAX_NOTIONAL              = 0.1
RANK_BY                   = "qpi"       # "qpi" | "ibs"
LIQUIDITY_ADV_WINDOW      = 63
LIQUIDITY_ADV_MAX_PCT     = 0.05
ROUND_TRIP_COST_BPS       = 2.0

# ── Data integrity (used by migrate.py — must match backtest values) ───────────
SPLIT_DROP_THRESHOLD      = -0.50       # Single-bar close drop flagging a likely split
CRASH_QUORUM_DROP         = 0.15        # Per-symbol drop threshold for crash detection
CRASH_QUORUM_FRACTION     = 0.30        # Fraction of universe dropping to call a crash day

# Minimum daily bars per symbol before it can enter daily_bars (partial warmup ok)
MIN_BARS_REQUIRED: int = (
    max(SMA_PERIOD, RETURN_RANK_RANGE // 4, LIQUIDITY_ADV_WINDOW)
    + ENTRY_N_DAY_RETURN + 30
)

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR                  = "data/"     # Input for migrate.py only; never read after migration

# ── IB Connection ───────────────────────────────────────────────────────────────
IB_HOST                   = "127.0.0.1"
IB_PORT                   = 7496        # 7497 for paper trading
IB_CLIENT_ID              = 1
IB_SUBACCOUNT             = ""          # Subaccount ID (e.g. "DU1234567"); empty = use master account
IB_HEARTBEAT_TIMEOUT_SEC  = 5           # Seconds to wait for reqCurrentTime response in heartbeat()
IB_RECONNECT_INTERVAL_SEC = 30          # Seconds between reconnect attempts in connection_watchdog
IB_RECONNECT_ALERT_AFTER  = 10         # Send warning alert after this many failed reconnect attempts

# ── IB error codes ──────────────────────────────────────────────────────────────
IB_SOFT_ERROR_CODES       = [2104, 2106, 2107, 2108, 2158]   # Informational; logged at DEBUG, never stored
IB_REJECTION_CODES        = [201, 202, 203, 321, 322]         # Hard order rejections; raise OrderRejectedError

# ── IBC (automated TWS/Gateway login) ──────────────────────────────────────────
IBC_MODE              = "gateway"          # "gateway" | "tws"
IBC_CONTROLLER_HOST   = "127.0.0.1"       # tws_controller_api host
IBC_CONTROLLER_PORT   = 8123              # tws_controller_api port
IBC_2FA_DAY           = "sunday"
IBC_2FA_TIME          = "18:00"
IBC_RESTART_TIMEOUT   = 120               # Seconds to wait for TWS/Gateway restart

# ── Scheduling (all times NY / ET) ─────────────────────────────────────────────
TIME_NIGHTLY_SYNC           = "20:00"   # Fixed — not relative to market close

# ── Intraday job offsets from market close (minutes, signed) ───────────────────
# market_open_check() at 11:00 ET computes run_time = close_time + timedelta(minutes=offset).
# On a half day (13:00 close) jobs shift automatically — no manual intervention needed.
SCHED_SIGNAL_OFFSET_MIN     = -20       # signal_snap:         close − 20 min  (normal: 15:40 ET)
SCHED_ORDER_OFFSET_MIN      = -16       # order_submission:    close − 16 min  (normal: 15:44 ET)
SCHED_FILL_OFFSET_MIN       = +10       # fill_reconciliation: close + 10 min  (normal: 16:10 ET)
SCHED_REPORT_OFFSET_MIN     = +15       # daily_report:        close + 15 min  (normal: 16:15 ET)
SCHED_MIN_LEAD_MINS         = 5         # Jobs with less than this many minutes of lead time are skipped

# ── Half-day calendar fallback ──────────────────────────────────────────────────
HALF_DAY_DATES: list[str] = [
    # NYSE early-close dates (13:00 ET) not yet reflected in pandas_market_calendars.
    # Add YYYY-MM-DD strings here when the library lags behind announced schedule changes.
    # Example: "2026-11-27"   # Black Friday 2026
]

# ── Order execution ─────────────────────────────────────────────────────────────
ENTRY_ORDER_TYPE          = "LOC"       # "MOC" | "LOC"
ENTRY_LOC_BUFFER_PCT      = 0.003       # 0.3% above snap price; ignored if MOC
EXIT_ORDER_TYPE           = "MOC"       # Keep exits as MOC — non-execution risk too high

# ── Data sources ────────────────────────────────────────────────────────────────
TWELVEDATA_API_KEY            = "YOUR_KEY_HERE"
TWELVEDATA_INCREMENTAL_DAYS   = 5           # Normal nightly lookback (fetch_bars per-symbol path)
TWELVEDATA_HISTORY_DAYS       = 550         # Full history depth for new symbols (~252 bars + buffer)
TWELVEDATA_BATCH_SIZE         = 8           # Symbols per HTTP request; free tier = 8, paid plans support higher values
TWELVEDATA_RATE_LIMIT_PER_MIN = 8           # Free tier: 8 credits/min. Paid plans support higher limits. Each symbol in a batch = 1 credit.
UNIVERSE_CSV                  = "state/universe.csv"

# ── Database ────────────────────────────────────────────────────────────────────
DB_DRIVER                 = "sqlite"    # "sqlite" | "postgresql"
DB_PATH                   = "state/bars.db"     # SQLite only
DB_HOST                   = "localhost"          # PostgreSQL only
DB_PORT                   = 5432                 # PostgreSQL only
DB_NAME                   = "murphy"             # PostgreSQL only
DB_USER                   = ""                   # PostgreSQL only
DB_PASSWORD               = ""                   # PostgreSQL only

# ── State export ────────────────────────────────────────────────────────────────
EXPORT_STATE_JSON         = True        # Also write state/positions.json on every update

# ── Logging ─────────────────────────────────────────────────────────────────────
LOG_TO_FILE               = True        # Write logs/murphy_YYYYMMDD.log
LOG_LEVEL                 = "INFO"      # DEBUG | INFO | WARNING | ERROR

# ── Reporting ───────────────────────────────────────────────────────────────────
REPORT_DAILY              = True
REPORT_WEEKLY             = True
REPORT_WEEKLY_DAY         = "friday"

# ── Alerting ────────────────────────────────────────────────────────────────────
ALERT_EMAIL               = ""          # Recipient address; empty = disabled
SMTP_HOST                 = "smtp.gmail.com"
SMTP_PORT                 = 587
SMTP_USER                 = ""
SMTP_PASSWORD             = ""          # Use an app password, not account password
DISCORD_WEBHOOK_URL       = ""          # Empty = disabled
DISCORD_ALERT_MENTIONS    = ""          # e.g. "<@USER_ID>" prepended on critical alerts only

# ── Risk controls (see Section 7 for full documentation) ───────────────────────
RISK_MAX_ORDER_VALUE_ENABLED    = True
RISK_MAX_ORDER_VALUE            = 500_000       # $; -1 = disabled
RISK_MAX_ORDER_VALUE_ACTION     = ["reject", "notify"]

RISK_DAILY_LOSS_ENABLED         = True
RISK_DAILY_LOSS_PCT             = 0.05
RISK_DAILY_LOSS_ACTION          = ["halt", "notify"]

RISK_MAX_DD_ENABLED             = True
RISK_MAX_DD_PCT                 = 0.20
RISK_MAX_DD_ACTION              = ["shutdown", "notify"]

RISK_MARGIN_ENABLED             = True
RISK_MARGIN_MIN_PCT             = 0.20
RISK_MARGIN_ACTION              = ["reject", "notify"]

RISK_STALE_STATE_ENABLED        = True
RISK_STALE_STATE_DAYS           = 2
RISK_STALE_STATE_ACTION         = ["skip", "notify"]

RISK_CONSEC_LOSS_DAYS_ENABLED   = True
RISK_CONSEC_LOSS_DAYS           = 3
RISK_CONSEC_LOSS_DAYS_ACTION    = ["notify"]

RISK_CONSEC_LOSS_TRADES_ENABLED = True
RISK_CONSEC_LOSS_TRADES         = 10
RISK_CONSEC_LOSS_TRADES_ACTION  = ["notify"]

RISK_FILL_TIMEOUT_ENABLED       = True
RISK_FILL_TIMEOUT_MINS          = 30
RISK_FILL_TIMEOUT_ACTION        = ["notify"]

RISK_RECONCILE_ENABLED          = True
RISK_RECONCILE_HALT             = False      # Set True to halt on mismatch; False for notify-only (safe default for multi-strategy / paper accounts)
RISK_RECONCILE_ACTION           = ["notify"] # Change to ["halt", "notify"] when RISK_RECONCILE_HALT=True

RISK_IMBALANCE_ENABLED          = False     # Optional; disabled by default
RISK_IMBALANCE_THRESHOLD        = 0.3
RISK_IMBALANCE_ACTION           = ["reject"]

# ── S&P 500 universe management ────────────────────────────────────────────────
SYMBOL_WHITELIST          = []          # Always included regardless of S&P membership
SYMBOL_BLACKLIST          = ["BRKB", "BFB"]   # Excluded from universe (e.g. not available on TwelveData plan)
SP500_CSV_URL             = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
SP500_UPDATE_DAY          = "sunday"    # Day to refresh constituent list
SP500_UPDATE_TIME         = "17:00"     # NY time — before IBC reauth

# ── Future: Web dashboard ───────────────────────────────────────────────────────
WEBSERVER_HOST            = "0.0.0.0"
WEBSERVER_PORT            = 8080

# ── Future: Remote API ──────────────────────────────────────────────────────────
API_HOST                  = "0.0.0.0"
API_PORT                  = 8081
API_SECRET_KEY            = "CHANGE_ME"
```

---

### 4.2 Data Pipeline

#### 4.2.1 Database Schema

```sql
-- Historical and live daily bars (primary data store)
CREATE TABLE IF NOT EXISTS daily_bars (
    symbol   TEXT NOT NULL,
    date     DATE NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    PRIMARY KEY (symbol, date)
);

-- Precomputed watchlist (refreshed nightly — not append-only)
CREATE TABLE IF NOT EXISTS watchlist (
    symbol        TEXT PRIMARY KEY,
    updated_date  DATE NOT NULL,
    sma200        REAL,
    q_threshold   REAL,
    adv63         REAL
);
```

#### 4.2.2 Migration (one-time bootstrap)

**File:** `migrate.py`

Consumes all `*_5min.json` files in `/data`. Applies the same pipeline as the backtest:
1. Load 5-min JSON (market hours 09:30–16:00)
2. Resample to daily OHLCV (same logic as backtest `resample_to_daily()`)
3. RIf toggled on run split integrity check (same logic as backtest `symbol_likely_has_split()`)
4. Require `MIN_BARS_REQUIRED` daily bars
5. Upsert accepted symbols into `daily_bars`

Must be run once before any other component can function. After migration, `/data` is never read by the live system again.

```
python migrate.py
→ Logs: accepted N symbols, skipped M (split), skipped K (too short)
→ Populates daily_bars table
```

#### 4.2.3 Nightly TwelveData Sync (~20:00 ET)

**File:** `td_data.py`

For each symbol in `universe.csv`, request the last few days of daily bars from TwelveData and upsert into `daily_bars`. After the DB update, trigger `precompute_watchlist()`.

Symbols are batched into HTTP requests of `config.TWELVEDATA_BATCH_SIZE` symbols each (default 8, matching the free-tier credit limit per request). The inter-batch delay is computed at call time as `(TWELVEDATA_BATCH_SIZE / TWELVEDATA_RATE_LIMIT_PER_MIN) * 60` seconds so that credit consumption stays within the per-minute cap. Both values are configurable — raise `TWELVEDATA_BATCH_SIZE` and `TWELVEDATA_RATE_LIMIT_PER_MIN` together on a paid plan.

#### 4.2.4 Watchlist Precompute (runs immediately after nightly sync)

After the DB is updated, evaluate all price-independent entry conditions across the full universe:
- `close > SMA(200)`
- `adv63` liquidity gate passes
- QPI rolling window sufficiently warmed up
- Symbol not already in an open position

Symbols passing all conditions are written to the `watchlist` table with their precomputed `sma200`, `q_threshold`, and `adv63` values. Typically reduces 509 symbols to 40–80 candidates.

#### 4.2.5 Intraday Snapshot (15:40 ET)

**File:** `ib_data.py` (IB section)

Fetches today's intraday bar for **watchlist symbols only** via `reqMktData(snapshot=True)`. Merges today's open/high/low/last/volume with the existing history loaded from `daily_bars`. Returns a dict of `{symbol: pd.DataFrame}` ready for indicator computation.

#### 4.2.6 Universe Management

**File:** `universe.py`

Manages the S&P 500 constituent list stored in `state/universe.csv`. Runs once per week (Sunday, before IBC reauth) to keep the tradeable universe current.

**Public API:**

```python
def fetch_sp500_symbols() -> list[str]:
    """
    Fetch the current S&P 500 constituent list from config.SP500_CSV_URL.
    Normalises tickers to IB format (dots removed: BRK.B → BRKB, BF.B → BFB).
    Returns a deduplicated, sorted list. Raises RuntimeError on HTTP or parse failure.
    """

def update_universe() -> dict:
    """
    Fetch current S&P 500 list, merge with config.SYMBOL_WHITELIST, diff against
    the existing universe.csv, and rewrite the file atomically.
    Returns {"added": [...], "removed": [...], "total": int}.
    Whitelist symbols are always kept — never appear in 'removed'.
    """

def get_new_symbols() -> list[str]:
    """
    Return symbols from universe.csv that have fewer than config.MIN_BARS_REQUIRED
    daily bars in daily_bars (i.e. need a full history build via TwelveData).
    Uses a single COUNT query per symbol — no DataFrame loading.
    """
```

**Sunday update job timing** (`sunday_universe_update`, registered in `scheduler.py`):
- Fires at `config.SP500_UPDATE_TIME` (default 17:00 ET) on `config.SP500_UPDATE_DAY` (default Sunday)
- Runs **before** `sunday_reauth` at `IBC_2FA_TIME` (default 18:00 ET) so any newly added symbols are visible to the next nightly sync
- No NYSE calendar gate — fires every configured Sunday regardless of trading day
- After completing, writes `last_universe_update = <now ISO>` to the `system_state` SQLite table so that `startup_catchup()` can detect a stale universe if the scheduler was offline for more than 7 days

**Full vs incremental history fetch:**
- **Incremental** (nightly, `TWELVEDATA_INCREMENTAL_DAYS = 5`): every symbol in `universe.csv` gets the last 5 days upserted into `daily_bars` during `nightly_sync()`
- **Full bootstrap** (`TWELVEDATA_HISTORY_DAYS = 550`): after `update_universe()` runs, `nightly_sync()` calls `get_new_symbols()` and fetches the full 550-day history for any symbol with fewer than `MIN_BARS_REQUIRED` bars — this catches newly added S&P 500 constituents in the same nightly job

**Whitelist behaviour:**
- `config.SYMBOL_WHITELIST` accepts raw tickers (dots normalised automatically)
- Whitelist symbols are unioned into the universe on every `update_universe()` call
- A whitelist symbol is never written to `removed` even if it drops out of the S&P 500
- Whitelist symbols with insufficient history are caught by `get_new_symbols()` on the next nightly sync, same as any new constituent

**Blacklist behaviour:**
- `config.SYMBOL_BLACKLIST` accepts normalised tickers (same format as `SYMBOL_WHITELIST`)
- Blacklisted symbols are subtracted from the universe in `update_universe()` — they never appear in the written CSV or in `added`/`removed` diffs
- `_read_universe_csv()` also filters them out on every read, so any symbol already present in `universe.csv` is silently excluded until the blacklist entry is removed
- Exclusions are logged at INFO level

---

### 4.3 Indicator Engine

**File:** `indicators.py`

**Direct port from `compute_indicators()` in v300.py — zero logic changes.**

```python
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attaches: n_day_ret, q_threshold, qpi_signal, sma200, ibs, rsi2, adv63
    Identical implementation to backtest v3.0.0.
    """
```

A unit test in `tests/test_indicators.py` must assert that running `compute_indicators()` on a symbol's historical data produces outputs matching the backtest's stored values within floating-point tolerance. This is the single most important parity check in the system.

---

### 4.4 Signal Generator

**File:** `signals.py`

#### 4.4.1 Entry Signals

**Direct port from `collect_signals()` in v300.py — zero logic changes.**

```python
def get_entry_signals(
    loaded_data: dict[str, pd.DataFrame],
    as_of_date: pd.Timestamp,
) -> list[dict]:
    """
    Evaluates price-sensitive entry conditions on the as_of_date bar
    for watchlist symbols only.
    Returns list of signal dicts (same schema as backtest signal records).
    Portfolio gating happens in order_manager.py, not here.
    """
```

#### 4.4.2 Exit Signals

```python
def get_exit_signals(
    open_positions: list[dict],
    loaded_data: dict[str, pd.DataFrame],
    as_of_date: pd.Timestamp,
) -> list[dict]:
    """
    Evaluates all exit conditions for each open position.
    Returns positions flagged for exit with 'exit_reason' field.
    Priority (first match wins):
      deterioration_stop → ibs_exit → rsi2_exit → time_stop
    """
```

---

### 4.5 Portfolio State

**File:** `portfolio_state.py`

All state lives natively in SQLite. JSON export is an optional parallel output controlled by `EXPORT_STATE_JSON` — never read back by the system.

#### 4.5.1 Database Schema

```sql
-- Open positions
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
);

-- Completed trades (append-only, matches backtest trade log schema exactly)
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
);

-- Daily equity snapshots
CREATE TABLE IF NOT EXISTS equity_log (
    date                DATE PRIMARY KEY,
    equity_bod          REAL,
    equity_eod          REAL,
    n_open_positions    INTEGER,
    deployed_pct        REAL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 4.5.2 Key Functions

```python
def load_positions() -> list[dict]
def save_position(pos: dict)                            # Upsert
def close_position(pos_id: str, exit_data: dict)        # Move to trade_log
def get_open_equity(positions, close_prices) -> float   # Mark-to-market
def get_total_equity(cash, positions, close_prices) -> float
def append_equity_snapshot(date, bod, eod, n_pos, deployed_pct)
def export_positions_json()                             # Only if EXPORT_STATE_JSON=True
```

`export_positions_json()` uses atomic write-to-temp-then-rename — always either complete or absent, never a partial write.

#### 4.5.3 IB Reconciliation & Alerting

At 16:10 ET, live IB positions are compared against the `positions` table. When a discrepancy is found, `fill_reconciliation()` always fires a prominent **critical** `monitor.send_alert()` with subject `"🚨 TRADING HALT — Position Reconciliation Mismatch"` directly — regardless of the risk engine action list. The body states whether a halt was set (governed by `RISK_RECONCILE_HALT`) and includes the full mismatch detail. `risk_engine.evaluate("reconcile_mismatch", ...)` is then called as a second step to execute the configured action (`RISK_RECONCILE_ACTION`).

**Same-day exit exclusion:** `_reconcile_with_ib()` accepts an optional `exclude_symbols: set[str]` parameter. `fill_reconciliation()` builds an `exited_today` set from all SELL fills processed in the same run and passes it as `exclude_symbols`. This prevents false-positive mismatches: after a position is closed, the DB row is gone but IB may still show the symbol until settlement; excluding it from both sides of the comparison avoids a spurious halt.

**Default behaviour (`RISK_RECONCILE_HALT = False`):** The alert fires at critical level but no trading halt is set. This is the safe default for multi-strategy or paper accounts where an IB position mismatch may originate from a separate manual trade and should not suspend the whole system automatically. Set `RISK_RECONCILE_HALT = True` and `RISK_RECONCILE_ACTION = ["halt", "notify"]` to restore the halt-on-mismatch behaviour.

The alert interface in `monitor.py` is a single dispatcher. All risk controls, reconciliation mismatches, fill failures, and circuit breakers call only `send_alert()`. Adding a new channel means adding one `_send_*` function and one `if config.X` line — nothing else changes.

```python
def send_alert(subject: str, body: str, level: str = "info"):
    """
    level: "info" | "warning" | "critical"
    Routes to all configured channels.
    """
    if config.ALERT_EMAIL:
        _send_email(subject, body, level)
    if config.DISCORD_WEBHOOK_URL:
        _send_discord(subject, body, level)
    # Future:
    # if config.SLACK_WEBHOOK_URL:
    #     _send_slack(subject, body, level)
    # if config.TELEGRAM_TOKEN:
    #     _send_telegram(subject, body, level)

def _send_email(subject: str, body: str, level: str):
    """stdlib smtplib — no extra dependency."""
    ...

def _send_discord(subject: str, body: str, level: str):
    """
    POST to DISCORD_WEBHOOK_URL via httpx.
    Message wrapped in code block for monospace formatting.
    DISCORD_ALERT_MENTIONS prepended on 'critical' level only.
    Long reports split into sequential posts to respect 2000-char limit.
    """
    ...
```

---

### 4.6 Order Manager

**File:** `order_manager.py`

Mirrors the sizing and gate logic inside `simulate_portfolio()` in v300.py exactly.

#### 4.6.1 Exit Order Construction

```python
def build_exit_orders(
    exit_signals: list[dict],
    state: dict,
) -> list[Order]:
    """
    For each position flagged for exit, build a plain MOC sell order.
    Order size = shares from position state (no recalculation).
    """
```

#### 4.6.2 Entry Order Construction

```python
def build_entry_orders(
    entry_signals: list[dict],
    state: dict,
    current_equity: float,
) -> list[Order]:
    """
    1. Remove signals for symbols already held
    2. Sort by RANK_BY (qpi or ibs — deepest dislocation first)
    3. For each candidate (up to free slots):
       a. Compute target_shares  = equity × MAX_TOTAL_NOTIONAL / MAX_POSITIONS / snap_price
       b. Apply MAX_NOTIONAL hard cap
       c. Check liquidity gate   (notional ≤ adv63 × LIQUIDITY_ADV_MAX_PCT)
       d. Check total notional gate (deployed_mtm + new ≤ equity × MAX_TOTAL_NOTIONAL)
       e. If RISK_IMBALANCE_ENABLED: check imbalance feed, apply RISK_IMBALANCE_ACTION
       f. Build LOC or MOC order per ENTRY_ORDER_TYPE
    """
```

Note on deployed capital for the total notional gate: use **mark-to-market values** of open positions at the 15:40 snap price — not entry prices. This matches the backtest's `close_map` calculation and keeps leverage accurate.

**Pending-exit slot and notional accounting** — before calling `build_entry_orders()`, `order_submission()` filters `open_positions` to exclude symbols that have an exit order:

```python
exiting_syms        = {o.symbol for o in exit_orders}
positions_post_exit = [p for p in open_positions if p["symbol"] not in exiting_syms]
```

`positions_post_exit` is passed as the `positions` argument so that both the slot counter (`slots_free = MAX_POSITIONS - len(positions)`) and the held-symbol filter correctly reflect the post-exit portfolio.  `exit_orders=exit_orders` is still forwarded separately so the notional credit is applied at the MTM gate.  The full gate logic becomes:

```
exit_credit        = sum(o.quantity × snap_price for o in exit_orders)
deployed_mtm       = sum(p.shares × snap_price for p in positions_post_exit)
effective_deployed = max(0, deployed_mtm − exit_credit)   # credit is 0 here since exiting
                                                            # symbols are already excluded,
                                                            # but guards against rounding
(effective_deployed + new_notional) / equity ≤ MAX_TOTAL_NOTIONAL
```

This prevents the bot from treating exiting slots as occupied and holding entry orders hostage to capital already earmarked for release.  `exit_orders=None` (default) leaves behaviour unchanged.

#### 4.6.3 Closing Auction Imbalance Filter (optional)

Controlled by `RISK_IMBALANCE_ENABLED` (default `False`). When enabled, fetches NYSE/NASDAQ imbalance data between signal snap and order submission. Candidates with an adverse imbalance ratio above `RISK_IMBALANCE_THRESHOLD` are handled per `RISK_IMBALANCE_ACTION` (default: `["reject"]`). Measure fill rate impact during paper trading before enabling in production.

#### 4.6.4 Order Object

```python
@dataclass
class Order:
    symbol:      str
    action:      str        # "BUY" | "SELL"
    order_type:  str        # "LOC" | "MOC"
    quantity:    int
    limit_price: float      # LOC only — snap_price × (1 + ENTRY_LOC_BUFFER_PCT); None for MOC
    reason:      str        # entry/exit reason for logging
    pos_id:      str        # links back to position record
```

---

### 4.7 Execution Bridge (IB)

**File:** `ib_exec.py`

Uses the official synchronous `ibapi` library with Python threading (`EClient` + `EWrapper`, `run()` in a daemon thread, responses synchronized via `queue.Queue`).

### 4.7.1 Connection Management & IBC Integration

Two layers of connection management:

**IBC layer** — controls the TWS/Gateway process itself (start, stop, restart, 2FA handling).

```python
class IBCController:
    """HTTP client for the tws_controller_api service."""
    def stop_gateway(self): ...     # GET http://IBC_CONTROLLER_HOST:IBC_CONTROLLER_PORT/stop-api
    def start_gateway(self): ...    # GET http://IBC_CONTROLLER_HOST:IBC_CONTROLLER_PORT/start-api
    def wait_for_api(self, timeout: int) -> bool: ...  # Poll IB_PORT until open (unchanged)

class IBBridge:
    def connect(self): ...
    def disconnect(self): ...
    def is_connected(self) -> bool: ...
    def reconnect(self): ...           # disconnect → short sleep → connect
    def wait_for_disconnect(self): ... # blocks until connectionClosed() fires
    def clear_disconnect(self): ...    # resets the event for the next disconnect cycle
```

**Disconnect event mechanism:** `IBBridge._disconnect_event` is a `threading.Event` initialised in `__init__`. The `connectionClosed()` EWrapper callback (fired by ibapi when the TCP connection is lost) calls `_disconnect_event.set()`. `wait_for_disconnect()` blocks on this event; `clear_disconnect()` resets it.

**`connect()` cleanup on reconnect:** Before opening a new socket, `connect()` always:
1. Calls `_disconnect_event.clear()` so the watchdog cannot trip on a stale event from the previous session.
2. Checks `self._thread.is_alive()`. If the old run-thread is still live, calls `EClient.disconnect(self)` to close the socket, then `self._thread.join(timeout=5)` to wait for it to exit.
3. Drains `_order_id_q`, `_account_q`, `_exec_q`, `_position_q`, and `_time_q` via `_drain()` so no stale callback data from the previous session is consumed by new requests.

These steps run unconditionally on every call to `connect()`, including the initial startup call.

**`connection_watchdog()` daemon thread** (in `scheduler.py`):
1. Blocks on `wait_for_disconnect()` — returns when `connectionClosed()` fires.
2. Calls `clear_disconnect()` immediately so any subsequent disconnect during the retry loop can be detected on the next outer iteration.
3. Enters an indefinite retry loop: sleep `IB_RECONNECT_INTERVAL_SEC` seconds, then call `bridge.connect()`.
4. After `IB_RECONNECT_ALERT_AFTER` consecutive failures sends a **warning** alert once: *"IB connection lost — N reconnect attempts failed, retrying every Xs"*.
5. On success: logs INFO. If a warning alert was sent, sends a recovery **info** alert: *"IB connection restored after N attempt(s)"*.
6. Calls `_post_reconnect_catchup()` to re-register any intraday jobs missed during the outage.
7. Loops back to step 1.

**Sunday reauth job** (runs at `IBC_2FA_TIME` every `IBC_2FA_DAY`):
1. `IBCController.stop_gateway()` — `GET /stop-api` on the `tws_controller_api` service; non-200 response logged at ERROR
2. Wait for process to terminate
3. `IBCController.start_gateway()` — `GET /start-api` on the `tws_controller_api` service; non-200 response logged at ERROR
4. `IBCController.wait_for_api(IBC_RESTART_TIMEOUT)` — poll `IB_HOST:IB_PORT` until the TCP port accepts connections (unchanged)
5. `IBBridge.reconnect()`
6. `send_alert()` with success or failure

#### 4.7.2 Order Submission

```python
def submit_order(bridge: IBBridge, order: Order) -> int:
    """
    Submit a LOC or MOC order via IB API.
    Returns IB order_id.
    """
```

When `config.IB_SUBACCOUNT` is non-empty, `ib_order.account` is set to that value before `placeOrder()` so the order is routed to the specified subaccount. When blank, the field is left unset and IB defaults to the master account.

#### 4.7.3 Fill Confirmation

```python
def get_filled_orders(
    bridge: IBBridge,
    order_ids: list[int],
) -> dict[int, dict]:
    """
    After 16:05 ET, poll ib.trades() to get fill price and quantity.
    Returns {order_id: {"fill_price": float, "fill_qty": int, "status": str}}
    """
```

#### 4.7.4 Account Snapshot

```python
def get_account_summary(bridge: IBBridge) -> dict:
    """
    Returns: {
        "net_liquidation":  float,
        "cash":             float,
        "buying_power":     float,
        "accrued_interest": float,   # month-to-date accrued cash interest (AccruedCash tag)
    }
    Cross-checks computed equity vs IB reported equity at 16:10 ET.
    """
```

`_ACCOUNT_TAGS` requests `NetLiquidation,TotalCashValue,BuyingPower,AccruedCash` from IB. The `accrued_interest` value is stored in `_snap_state["account"]` and forwarded to the daily/weekly report footer (shown only when non-zero).

When `config.IB_SUBACCOUNT` is non-empty, `reqAccountSummary()` is called with that account ID instead of `"All"`, so the returned equity and buying power figures reflect the subaccount rather than the consolidated master account.

#### 4.7.5 Connectivity Heartbeat

```python
def IBBridge.heartbeat(self) -> bool:
    """
    Lightest possible connectivity check: reqCurrentTime() round-trip.
    Returns True if currentTime() callback fires within IB_HEARTBEAT_TIMEOUT_SEC (5s),
    False on timeout.
    """
```

Called by `main.connectivity_check()` at 09:00 ET. On failure: one reconnect attempt, then critical alert + risk_engine halt if still unreachable.

#### 4.7.6 Order Rejection Handling

`submit_order()` waits 2 seconds after `placeOrder()` for IB to fire a rejection callback. Hard rejection codes (`IB_REJECTION_CODES`: 201, 202, 203, 321, 322) are caught by the `error()` callback and stored in a module-level `_order_errors` dict. If the submitted order_id appears there, `submit_order()` raises `OrderRejectedError(order_id, message)`.

Informational codes (`IB_SOFT_ERROR_CODES`: 2104, 2106, 2107, 2108, 2158) are logged at DEBUG and never stored. Both lists are configurable in `config.py`.

`main.order_submission()` catches `OrderRejectedError` per-order: logs at ERROR, sends a "warning" alert, and continues with remaining orders. A single rejection never aborts the full submission loop.

---

### 4.8 Scheduler

**File:** `scheduler.py`

Uses APScheduler `BlockingScheduler` with `timezone=config.TZ`. All jobs anchored to NY time regardless of host machine timezone.

#### Daemon threads (started at startup, before the scheduler)

| Thread | Name | Description |
|---|---|---|
| `connection_watchdog` | `ib-watchdog` | Blocks on `bridge.wait_for_disconnect()`; on disconnect retries indefinitely at `IB_RECONNECT_INTERVAL_SEC`-second intervals; warning alert after `IB_RECONNECT_ALERT_AFTER` failures; recovery info alert on success; calls `_post_reconnect_catchup()` |

#### Fixed cron triggers (registered at startup)

| Job | Schedule (ET) | Calendar gate | Description |
|---|---|---|---|
| `sunday_universe_update` | `SP500_UPDATE_DAY` at `SP500_UPDATE_TIME` | None | S&P 500 universe refresh + full history fetch for new symbols |
| `sunday_reauth` | Sunday at `IBC_2FA_TIME` | None | IBC stop → start → 2FA → reconnect → alert |
| `connectivity_check` | Mon–Fri 09:00 | None | `bridge.heartbeat()` → reconnect on failure → critical alert + halt if unrecoverable |
| `nightly_sync` | Mon–Fri `TIME_NIGHTLY_SYNC` | NYSE | TwelveData incremental bar update → `precompute_watchlist()` |
| `market_open_check` | Mon–Fri 11:00 | NYSE | Determine close time; register four intraday DateTrigger jobs |

#### Startup halt warning (`_startup_halt_warning()`)

Called once at startup after the IB connection is established (before `build_scheduler()`). If `risk_engine.is_halted()` or `risk_engine.is_shutdown()` returns True:
- Logs at `CRITICAL` with the active state (HALT or SHUTDOWN).
- Sends a critical `monitor.send_alert()` with subject `"⚠️ Murphy's Law started with HALT/SHUTDOWN active"` and a body explaining that trading is suspended and `risk_engine.clear_halt()` is required to resume.
- Does **not** abort startup — all data maintenance jobs (nightly sync, universe update) continue running regardless.

#### Startup catch-up (`startup_catchup()`)

`startup_catchup()` is called once at scheduler startup (after `build_scheduler()`). It handles three scenarios that arise when the scheduler is (re)started mid-day or after a gap:

1. **Same-day intraday catch-up** — If today is a trading day and the current time is between 11:00 ET (when `market_open_check` normally fires) and `fill_reconciliation` time (`close_time + SCHED_FILL_OFFSET_MIN`), `market_open_check()` is called immediately so any remaining intraday jobs are registered for today. Jobs whose `run_time` is less than `SCHED_MIN_LEAD_MINS` away are automatically skipped by `market_open_check()`, so there is no risk of scheduling already-missed jobs.

2. **Stale universe catch-up** — Reads `last_universe_update` from the `system_state` SQLite table (key-value store, `CREATE TABLE IF NOT EXISTS`). If the value is absent or older than 7 days, `main.sunday_universe_update()` is called immediately.

3. **Nightly data catch-up** — Queries `MAX(date)` from `daily_bars`. Determines the last passed NYSE market close date using `pandas_market_calendars` (filters to sessions whose `market_close` UTC timestamp has already elapsed). If `MAX(date) < last_close_date`, computes `gap_days = (today − MAX(date)).days` and calls `td_data.fetch_incremental(symbols, n_days=gap_days + 2)` (the `+2` provides an overlap buffer). After the fetch, calls `main.precompute_watchlist()` so the watchlist reflects the freshly updated data. Logs: `"startup catch-up sync: DB last date=…, last market close=…, fetching N days"`. If `MAX(date)` already matches the last close, logs at DEBUG and skips.

   **Time-budget guard** — Before starting the sync, estimates how long it will take using `ceil(len(symbols) / TWELVEDATA_BATCH_SIZE) × (TWELVEDATA_BATCH_SIZE / TWELVEDATA_RATE_LIMIT_PER_MIN) × 60` seconds and compares it against the time remaining until the market-open safety deadline (`11:00 ET − SCHED_MIN_LEAD_MINS`). If the estimate exceeds the available window, the sync is skipped with a WARNING: `"startup catch-up sync skipped — would overlap market open; nightly sync will catch up tonight"`. Execution continues without raising; data will be refreshed by tonight's `nightly_sync` job.

#### Post-reconnect catch-up (`_post_reconnect_catchup()`)

Called by `connection_watchdog()` immediately after each successful reconnect. Determines which intraday jobs were missed during the outage and re-registers them:

- If a job's `run_time` is still `>= SCHED_MIN_LEAD_MINS` in the future → schedule at the natural `run_time` (same as `market_open_check` would) via `_scheduler.add_job(DateTrigger(...))`.
- If a job's `run_time` has already passed but it is still worth running:
  - `signal_snap` / `order_submission` (non-sequential): re-schedule immediately via `_scheduler.add_job(DateTrigger(now + 5s))`.
  - `fill_reconciliation` / `daily_report` (sequential): **called directly** (`fn()`) in order — fill first, then report — so they run synchronously within the watchdog cycle rather than as two concurrent near-simultaneous DateTrigger jobs. This guarantees the report always sees the reconciled position state.

**Sequencing rules** (a job is worth running only if its successor has not already fired):

| Job | Worth running if… | Past-window execution |
|---|---|---|
| `signal_snap` | `now < t_order` (order_submission time has not passed) | immediate DateTrigger |
| `order_submission` | `now < t_fill` (fill_reconciliation time has not passed) | immediate DateTrigger |
| `fill_reconciliation` | same calendar day as `close_time` (before midnight ET) | direct call |
| `daily_report` | same calendar day as `close_time` (before midnight ET) | direct call |

On a non-trading day the function returns immediately with no jobs scheduled.

#### Intraday job completion tracker (`_jobs_run_today`)

`_jobs_run_today: dict[str, date]` is a module-level dict that records the date each intraday job last completed successfully.  It provides an idempotency guarantee: each of the four market jobs runs **at most once per trading day** regardless of reconnects or `startup_catchup()` re-entries.

**Recording:** each intraday job wrapper (`job_signal_snap`, `job_order_submission`, `job_fill_reconciliation`, `job_daily_report`) appends `_jobs_run_today[job_id] = date.today()` after its `main.*` call returns without raising.  An exception propagating out of the wrapper leaves the entry absent, so a subsequent reconnect-catchup may retry it.

**Reset:** `_reset_daily_job_tracker()` clears the dict.  It is called at the start of `market_open_check()` (11:00 ET every trading day) so each new trading day starts with an empty tracker.

**Check in `_post_reconnect_catchup()`:** before scheduling each job, the function checks `_jobs_run_today.get(job_id) == now.date()`.  If True, the job is skipped with a DEBUG log (`"job_id already ran today — skipping"`).  This prevents a reconnect that happens after fill_reconciliation or daily_report has already completed from scheduling a duplicate run.

**Check in `startup_catchup()`:** before calling `market_open_check()`, the function checks whether `fill_reconciliation` or `daily_report` have already been recorded for today.  If either is present, the `market_open_check()` call is skipped (DEBUG log).  In practice this guard fires only when the scheduler process is warm-restarted mid-day after some jobs have already completed.

#### Dynamically registered intraday jobs (DateTrigger, one-off)

The four intraday jobs are **not** fixed cron triggers. At 11:00 ET, `market_open_check()` calls `get_market_schedule()` to obtain today's `close_time`, then registers each job as a one-off `DateTrigger` at `close_time + timedelta(minutes=SCHED_*_OFFSET_MIN)`. All four jobs are registered with `replace_existing=True`, making the 11:00 call idempotent if it fires more than once in a session.

Jobs whose `run_time − now < SCHED_MIN_LEAD_MINS` are silently skipped (logged at INFO). This applies both at the normal 11:00 ET fire and during `startup_catchup()` when the scheduler starts late in the day.

| Job | Config param | Default | Normal day (close 16:00) | Half day (close 13:00) |
|---|---|---|---|---|
| `signal_snap` | `SCHED_SIGNAL_OFFSET_MIN` | −20 | 15:40 ET | 12:40 ET |
| `order_submission` | `SCHED_ORDER_OFFSET_MIN` | −16 | 15:44 ET | 12:44 ET |
| `fill_reconciliation` | `SCHED_FILL_OFFSET_MIN` | +10 | 16:10 ET | 13:10 ET |
| `daily_report` | `SCHED_REPORT_OFFSET_MIN` | +15 | 16:15 ET | 13:15 ET |

```
signal_snap
    IB snapshot for watchlist symbols only
    → merge today's bar with DB history
    → compute_indicators() on watchlist symbols
    → get_entry_signals() + get_exit_signals()

order_submission
    → risk_engine.evaluate() pre-checks
    → build_exit_orders() → submit MOC sells
    → build_entry_orders() (with optional imbalance filter) → submit LOC/MOC buys
    → log all submitted order_ids

fill_reconciliation
    → get_filled_orders() from IB
    → detect_splits() — update positions table if any split detected; alert
    → update positions table (new entries)
    → close_position() for exits → append to trade_log
    → append_equity_snapshot()
    → _reconcile_with_ib() against full live IB position list → alert on mismatch (split symbols excluded)
    → export_positions_json() if EXPORT_STATE_JSON=True
    → _submitted = {}  (clear submitted dict to prevent re-processing on a second call)

daily_report
    → build report from equity_log + trade_log
    → send_report() via all configured channels
    → send weekly report if REPORT_WEEKLY_DAY matches
```

#### Half-day handling

`get_market_schedule(d)` returns `{"is_open": bool, "close_time": datetime, "is_half_day": bool}`. A session is flagged as a half day when `close_time` (ET) is before 14:00. `pandas_market_calendars` handles detection automatically for all known NYSE early-close dates (day before July 4th, Black Friday, Christmas Eve when a weekday, etc.).

`config.HALF_DAY_DATES` is a fallback list of `YYYY-MM-DD` strings. Any date in this list overrides the calendar data with a 13:00 ET close — use it when the library has not yet been updated for a newly announced schedule change. The override is applied inside `get_market_schedule()` before the `is_half_day` flag is set, so all downstream logic (intraday job times, half-day logging) is consistent.

---

### 4.9 Logger & Monitor

**File:** `monitor.py`

#### 4.9.1 Logging

```python
def setup_logging():
    logger = logging.getLogger("murphy")
    logger.setLevel(config.LOG_LEVEL)

    # Always log to stdout
    logger.addHandler(logging.StreamHandler())

    # File handler only if LOG_TO_FILE=True
    if config.LOG_TO_FILE:
        Path("logs").mkdir(exist_ok=True)
        handler = logging.handlers.TimedRotatingFileHandler(
            filename=f"logs/murphy_{date.today():%Y%m%d}.log",
            when="midnight",
            backupCount=90
        )
        logger.addHandler(handler)
```

The same `logger` instance is imported across all modules. No `print()` statements anywhere in the codebase. Every signal evaluated, order submitted, fill confirmed, and alert dispatched is logged.

#### 4.9.2 Daily & Weekly Reports

Report is built as structured data first, rendered to plain text second — same content reused for email, Discord, and future web dashboard without duplication.

**Daily report** (`build_daily_report(data)`):

```
Murphy's Law — Daily Report 2026-03-28
────────────────────────────────────────
Equity (BOD):       $845,210
Equity (EOD):       $851,430   +$6,220  (+0.74%)

Exits today:        3
  NVDA  ibs_exit      +$1,842   (3 bars)
  AAPL  time_stop     -$2,105   (15 bars)
  META  ibs_exit      +$844     (5 bars)

Entries today:      2
  PLTR  LOC buy  142 shares @ limit $45.23  [QPI=0.08, IBS=0.14]
  SMCI  LOC buy   87 shares @ limit $28.11  [QPI=0.06, IBS=0.11]

Open positions:     13 / 15
────────────────────────────────────────────────────────────────────────
  Symbol   Entry        Days      Cost       Price  Unreal P&L        %
  XOM      2026-04-09      5   $155.17    $161.20      +$603     +3.9%
  CVX      2026-04-07      7   $200.00    $195.00      -$250     -2.5%
────────────────────────────────────────────────────────────────────────
  Total unrealised                                      +$353     +1.4%

Total deployed:     132.4% of equity

APY (inception):   +43.2%   APY (90d):   +38.1%
APY (7d):             n/a   APY (30d):   +51.4%
Drawdown: -4.2%  from ATH $1,052,340
Accrued interest: +$124.50  (month-to-date)

YTD P&L:  +$92,500  (+12.3%)
────────────────────────────────────────
```

- **Positions table** (`open_positions_enriched`): rendered when the key is present and non-empty. Current price uses `snap_prices` from the 15:40 snap; `unrealised_pnl` and `unrealised_pnl_pct` are computed in `daily_report()`. All cash columns are right-justified to `_COL_WIDTH = 10`.
- **APY rows** always rendered (four windows). `None` → `"n/a"`. Computed from `equity_log` in `daily_report()`:
  - *inception*: requires ≥ 30 trading-day rows; uses first row as start.
  - *7d / 30d / 90d*: require ≥ 7 / 30 / 90 rows respectively; use the N-th row from the end.
- **Drawdown** shown only when `drawdown_pct` is not `None` (i.e. `equity_eod < ATH`).
- **Accrued interest** shown only when non-zero; formatted with 2 decimal places (`_usd2()`).

**Weekly report** (`build_weekly_report(data)`) — sent on `REPORT_WEEKLY_DAY` instead of reusing the daily format. `daily_report()` queries `equity_log`, `trade_log`, and `positions` for the full Monday→today window and passes a `week_data` dict. The weekly report includes the same positions table, APY rows, drawdown, and accrued interest as the daily report.

```
Murphy's Law — Weekly Report  2026-04-06 → 2026-04-10
────────────────────────────────────────
Equity (week start): $840,000
Equity (week end):   $855,000   +$15,000  (+1.8%)

Exits this week:    3  ...

Entries this week:  2  ...

Open positions:     13 / 15
[positions table — same format as daily]

Total deployed:     132.4% of equity

APY (inception):  +43.2%   APY (90d):  +38.1%
APY (7d):            n/a   APY (30d):  +51.4%
Drawdown: -4.2%  from ATH $1,052,340
Accrued interest: +$124.50  (month-to-date)

YTD P&L:  +$92,500  (+12.3%)
────────────────────────────────────────
```

`report_data` / `week_data` keys: `date` (daily) or `week_start`/`week_end` (weekly), `equity_bod`/`equity_eod` (daily) or `equity_start`/`equity_end` (weekly), `exits`, `entries`, `n_open`, `deployed_pct`, `ytd_pnl`, `ytd_pnl_pct`, `open_positions_enriched`, `apy_inception`, `apy_7d`, `apy_30d`, `apy_90d`, `ath`, `drawdown_pct`, `accrued_interest`.

```python
def send_report(report_text: str, is_weekly: bool = False):
    """Delivers formatted daily/weekly report via all configured channels."""
    subject = "Murphy's Law — Weekly Report" if is_weekly else "Murphy's Law — Daily Report"
    send_alert(subject, report_text, level="info")
```

**`_pct()` formatting:** values with `abs(v) < 0.05` override to 2 decimal places so tiny returns (e.g. `+0.03%`) are not truncated to `+0.0%`. Values ≥ 0.05 use the `decimals` argument (default 1). A `-0.0` guard returns `"0.0%"` directly.

**`_usd2()` formatting:** like `_usd()` but with two decimal places — used for accrued interest.

#### 4.9.3 Alert Dispatcher

See Section 4.5.3 for full `send_alert()` implementation. Channels: email (stdlib `smtplib`) and Discord webhook (`httpx`). Further channels added without touching any call sites.

---

## 5. Execution Timing & MOC/LOC

### Exchange Deadlines
- **NYSE**: MOC/LOC orders must be entered by **15:45 ET** (hard deadline)
- **NASDAQ**: MOC orders must be entered by **15:55 ET**
- **Catch-all**: Orders submitted by **15:44 ET** — one minute before NYSE hard stop

### Flow
```
15:40  Signal snap (watchlist symbols only, ~40-80 symbols, <30 seconds)
15:42  Optional imbalance feed check
15:44  Order submission deadline
15:45  NYSE hard wall — no new MOC/LOC accepted
```

### LOC vs MOC
- **Entries**: LOC at `snap_price × (1 + ENTRY_LOC_BUFFER_PCT)`. If the closing auction prints above the limit, the order does not fill — acceptable for entries. For mean reversion entries, a stock ripping hard into the close is arguably one you don't want anyway.
- **Exits**: Plain MOC. LOC on exits risks non-execution if the stock drops in the final minutes, leaving you holding a position past its exit signal with no clean recovery path.

### Execution Quality
MOC/LOC fills execute at the official closing auction price. The closing auction is consistently the highest-volume event of the trading day (8–15% of daily volume in a single print). This means:
- Bid-ask spread is effectively zero at the auction
- Market impact is minimal — orders are absorbed into institutional flow
- The backtest's fill-at-close assumption is an accurate model of reality
- The 2 bps round-trip cost assumption is conservative — actual friction is likely lower

The main source of slippage vs. backtest is the **15:40 snap vs. actual close divergence** for IBS and QPI signals. For liquid large-caps this is typically within 0.1–0.3% and does not materially affect signal accuracy.

---

## 6. Position Sizing (Live)

Live sizing mirrors the backtest formula exactly:

```python
# Use IB net liquidation value as equity proxy
equity = ib_account["net_liquidation"]

# Per-slot target (identical to backtest)
target_shares = int((equity * MAX_TOTAL_NOTIONAL / MAX_POSITIONS) / snap_price)

# Hard single-position cap
max_cap_shares = int((equity * MAX_NOTIONAL) / snap_price)

shares = min(target_shares, max_cap_shares)
```

`snap_price` is the 15:40 IB snapshot price. The actual LOC/MOC fill will differ slightly — accepted as implementation slippage.

**Mark-to-market for total notional gate**: deployed capital uses current market prices of open positions (from the 15:40 snapshot), not entry prices. This matches the backtest's `close_map` calculation. Entry price is a sunk cost — market value reflects actual current exposure and keeps leverage accurate.

For positions in symbols not on the watchlist (already held, not new candidates), a supplementary IB snapshot call fetches their current prices at 15:40.

---

## 7. Risk Controls

All controls are independently toggleable with configurable thresholds and actions. Actions are additive — `["halt", "notify"]` both halts and notifies simultaneously.

Available actions:
- `"notify"` — send alert via all configured channels, continue normally
- `"reject"` — block the specific order that triggered the control
- `"skip"` — skip the current scheduler job entirely
- `"halt"` — halt all new entries until manually reset (exits still process)
- `"shutdown"` — halt entries AND exits, require full manual intervention

All controls are evaluated by `risk_engine.py` through a single interface:

```python
def evaluate(control_name: str, context: dict) -> bool:
    """
    Evaluates a named control against current context.
    Executes all configured actions if the control trips.
    Returns True if execution should proceed, False if blocked.
    """
```

Every scheduler job passes its context through `evaluate()` before proceeding. All risk logic lives in `risk_engine.py` — not in the scheduler, order manager, or portfolio state.

| Control | Config key | Default action |
|---|---|---|
| Max single order value | `RISK_MAX_ORDER_VALUE` | reject, notify |
| Daily loss circuit breaker | `RISK_DAILY_LOSS_PCT` | halt, notify |
| Max drawdown circuit breaker | `RISK_MAX_DD_PCT` | shutdown, notify |
| IB margin breach | `RISK_MARGIN_MIN_PCT` | reject, notify |
| Stale state guard | `RISK_STALE_STATE_DAYS` | skip, notify |
| Consecutive losing days | `RISK_CONSEC_LOSS_DAYS` | notify |
| Consecutive losing trades | `RISK_CONSEC_LOSS_TRADES` | notify |
| Order fill timeout | `RISK_FILL_TIMEOUT_MINS` | notify |
| State reconciliation mismatch | `RISK_RECONCILE_HALT` | notify (default); set `RISK_RECONCILE_HALT=True` and `RISK_RECONCILE_ACTION=["halt","notify"]` to halt |
| Imbalance filter (optional) | `RISK_IMBALANCE_THRESHOLD` | reject |

See `config.py` in Section 4.1 for full parameter definitions.

---

## 8. State Persistence

All state lives in the SQLite database. No CSV files anywhere in the system.

### 8.1 Positions
Upserted to the `positions` table after every entry and exit. Atomic by nature of SQLite transactions.

### 8.2 Trade Log
Appended to the `trade_log` table on every position close. Column names match the backtest trade log exactly (`ml_trade_log_v3.0.0_*.csv`), enabling direct SQL comparison between live and backtest performance.

### 8.3 Equity Log
Appended to the `equity_log` table at 16:10 ET after fill reconciliation, once BOD and EOD equity are both known.

### 8.4 Watchlist
Written to the `watchlist` table nightly after TwelveData sync. Overwritten in full each night — not append-only.

### 8.5 Optional JSON Export
If `EXPORT_STATE_JSON = True`, `state/positions.json` is written atomically (write-to-temp-then-rename) after every DB update. Never read back by the system — DB is always the source of truth.

### 8.6 Database Abstraction
All DB access goes through `db.py`. Connection string and driver are derived from `DB_DRIVER` in config. Switching to PostgreSQL requires changing `DB_DRIVER`, `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` — no upstream module changes.

---

## 9. Known Gaps & Limitations

### 9.1 Universe Drift
The system starts with a static universe from `state/universe.csv`. In production, S&P 500 constituents change. The live system needs a periodic update mechanism (monthly recommended). Until implemented: continue holding but do not enter new positions in de-listed symbols; manually add newly-added constituents.

### 9.2 Deterioration Stop — Dead Code Path
Confirmed `n=0` in backtest v3.0.0. `STOP_MIN_BARS = 15` equals `MAX_HOLDING_PERIOD = 15` — the time stop always fires before the grace period expires. Carried into live as-is to match backtest behavior. Note this in code comments so it is not mistaken for a bug.

### 9.3 Short Side
Backtest is long-only. No short logic required.

### 9.4 Partial Fills
MOC/LOC orders may receive partial fills for lower-liquidity names. Handle by updating `shares` and `notional` in the position record to actual filled quantities and logging the shortfall. The liquidity gate (`LIQUIDITY_ADV_MAX_PCT = 0.05`) makes this unlikely for most symbols.

### 9.5 Dividends & Corporate Actions
The backtest does not model dividends. Live portfolio will receive cash dividends — track separately in equity log but do not attempt to reconcile against backtest P&L.

### 9.6 Stock Splits
Split detection runs inside `fill_reconciliation()` (16:10 ET) by comparing IB-reported share counts against DB records for each open position. If `ib_shares / db_shares` is within 1% of a known ratio (2:1, 3:1, 1:2, 1:3), the split is confirmed and IB is treated as the source of truth: `shares` is updated to `ib_shares`, `fill_price` is recomputed as `notional / ib_shares` (notional preserved), a WARNING is logged, and an alert is dispatched. Split symbols are excluded from the reconciliation mismatch check so they do not trigger a halt.

**Dangerous window**: between the split ex-date and the next nightly TwelveData sync (~20:00 ET) the `daily_bars` table still contains pre-split prices, so indicators (SMA, RSI, IBS, QPI) would reflect incorrect per-share values for that symbol. This is mitigated by two factors: (1) TwelveData returns split-adjusted data by default, so the nightly sync overwrites affected rows with correctly adjusted prices before the next trading day's signal snap; (2) order submission runs before 16:10 ET, so no new orders are generated using stale post-split data on the same day the split is detected.

### 9.7 Implemented Reliability Controls

Both items below were previously listed as gaps and have been implemented.

**Order rejection handling** — `submit_order()` waits 2 s after `placeOrder()` and raises `OrderRejectedError` if IB fires a hard rejection callback (codes 201, 202, 203, 321, 322). `order_submission()` catches this per-order, logs at ERROR, sends a "warning" alert, and continues with remaining orders.

**TWS connectivity heartbeat** — `connectivity_check()` runs at 09:00 ET every weekday (no calendar gate). It calls `bridge.heartbeat()` (a `reqCurrentTime()` round-trip). On failure it attempts one reconnect; on recovery it sends an info alert. If still unreachable it sends a critical alert and triggers `risk_engine.evaluate("reconcile_mismatch", ...)` to apply the configured action.

### 9.8 Earnings Blackout
No earnings blackout filter (unlike ZMS strategy). Optional enhancement for a later version — not required for backtest parity.

### 9.9 Blacklisted Symbols (BRKB, BFB)
`BRKB` (BRK.B) and `BFB` (BF.B) are excluded from the universe by default via `config.SYMBOL_BLACKLIST` because TwelveData's free tier does not provide data for Berkshire Hathaway B-shares and Brown-Forman B-shares. Both are legitimate S&P 500 constituents that the backtest traded. To include them, either upgrade to a TwelveData plan that covers these symbols or add an alternative data source and remove them from `SYMBOL_BLACKLIST`.

---

*Last updated: 2026-03-29*
*Based on Murphy's Law backtest v3.0.0 (reference/v300.py)*
