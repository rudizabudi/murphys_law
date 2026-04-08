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

# ── IB error codes ──────────────────────────────────────────────────────────────
IB_SOFT_ERROR_CODES       = [2104, 2106, 2107, 2108, 2158]   # Informational; logged at DEBUG, never stored
IB_REJECTION_CODES        = [201, 202, 203, 321, 322]         # Hard order rejections; raise OrderRejectedError

# ── IBC (automated TWS/Gateway login) ──────────────────────────────────────────
IBC_MODE              = "gateway"          # "gateway" | "tws"
IBC_DIR               = "/opt/ibc"         # IBC installation directory
IBC_GATEWAY_START     = "/opt/ibc/gatewaystart.sh"
IBC_TWS_START         = "/opt/ibc/twsstart.sh"
IBC_COMMAND_SEND      = "/opt/ibc/commandsend.sh"
IBC_TWS_PATH          = "/opt/Trader Workstation"
IBC_CONFIG_PATH       = "/opt/ibc/config.ini"
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
RISK_RECONCILE_ACTION           = ["halt", "notify"]

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

At 16:10 ET, live IB positions are compared against the `positions` table. Any discrepancy halts the next day's order submission until manually resolved and routes through `send_alert()`.

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
    """Thin wrapper around IBC shell commands."""
    def stop_gateway(self): ...     # commandsend.sh stop — sends stop to running IBC instance
    def start_gateway(self): ...    # gatewaystart.sh (IBC_MODE="gateway") or twsstart.sh (IBC_MODE="tws")
    def wait_for_api(self, timeout: int) -> bool: ...  # Poll IB_PORT until open

class IBBridge:
    def connect(self): ...
    def disconnect(self): ...
    def is_connected(self) -> bool: ...
    def reconnect(self): ...           # disconnect → short sleep → connect
    def wait_for_disconnect(self): ... # blocks until connectionClosed() fires
    def clear_disconnect(self): ...    # resets the event for the next disconnect cycle
```

**Disconnect event mechanism:** `IBBridge._disconnect_event` is a `threading.Event` initialised in `__init__`. The `connectionClosed()` EWrapper callback (fired by ibapi when the TCP connection is lost) calls `_disconnect_event.set()`. `wait_for_disconnect()` blocks on this event; `clear_disconnect()` resets it. The `connection_watchdog()` daemon thread in `scheduler.py` blocks on `wait_for_disconnect()` and reconnects immediately when the event fires — up to 3 retries at 10-second intervals, critical alert if all fail.

**Sunday reauth job** (runs at `IBC_2FA_TIME` every `IBC_2FA_DAY`):
1. `IBCController.stop_gateway()` — runs `commandsend.sh stop` to gracefully shut down the running instance
2. Wait for process to terminate
3. `IBCController.start_gateway()` — runs `gatewaystart.sh` (or `twsstart.sh` if `IBC_MODE="tws"`), passing `IBC_DIR` and `IBC_CONFIG_PATH`; IBC handles 2FA automatically via `TwsLoginMode` in config.ini
4. `IBCController.wait_for_api(IBC_RESTART_TIMEOUT)` — poll until API port responds
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
    Returns: {"net_liquidation": float, "cash": float, "buying_power": float}
    Cross-checks computed equity vs IB reported equity at 16:10 ET.
    """
```

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
| `connection_watchdog` | `ib-watchdog` | Blocks on `bridge.wait_for_disconnect()`; on disconnect attempts `bridge.connect()` up to 3 times at 10-second intervals; critical alert if all retries fail; resets the event and loops |

#### Fixed cron triggers (registered at startup)

| Job | Schedule (ET) | Calendar gate | Description |
|---|---|---|---|
| `sunday_universe_update` | `SP500_UPDATE_DAY` at `SP500_UPDATE_TIME` | None | S&P 500 universe refresh + full history fetch for new symbols |
| `sunday_reauth` | Sunday at `IBC_2FA_TIME` | None | IBC stop → start → 2FA → reconnect → alert |
| `connectivity_check` | Mon–Fri 09:00 | None | `bridge.heartbeat()` → reconnect on failure → critical alert + halt if unrecoverable |
| `nightly_sync` | Mon–Fri `TIME_NIGHTLY_SYNC` | NYSE | TwelveData incremental bar update → `precompute_watchlist()` |
| `market_open_check` | Mon–Fri 11:00 | NYSE | Determine close time; register four intraday DateTrigger jobs |

#### Dynamically registered intraday jobs (DateTrigger, one-off)

The four intraday jobs are **not** fixed cron triggers. At 11:00 ET, `market_open_check()` calls `get_market_schedule()` to obtain today's `close_time`, then registers each job as a one-off `DateTrigger` at `close_time + timedelta(minutes=SCHED_*_OFFSET_MIN)`. All four jobs are registered with `replace_existing=True`, making the 11:00 call idempotent if it fires more than once in a session.

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
Total deployed:     132.4% of equity

YTD P&L:  +$92,500  (+12.3%)
────────────────────────────────────────
```

```python
def send_report(report_text: str, is_weekly: bool = False):
    """Delivers formatted daily/weekly report via all configured channels."""
    subject = "Murphy's Law — Weekly Report" if is_weekly else "Murphy's Law — Daily Report"
    send_alert(subject, report_text, level="info")
```

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
| State reconciliation mismatch | — | halt, notify |
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
