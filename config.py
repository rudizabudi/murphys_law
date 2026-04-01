"""
config.py — Single source of truth for all parameters.

No other module contains hardcoded values.
All times in this system are New York time (ET): ZoneInfo("America/New_York").
"""

from pathlib import Path
from zoneinfo import ZoneInfo

TZ    = ZoneInfo("America/New_York")
_BASE = Path(__file__).resolve().parent

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
SPLIT_DROP_THRESHOLD      = -0.50       # single-bar close drop flagging a likely split
CRASH_QUORUM_DROP         = 0.15        # per-symbol drop threshold for crash detection
CRASH_QUORUM_FRACTION     = 0.30        # fraction of universe dropping to call a crash day

# Minimum daily bars per symbol before it can enter daily_bars (partial warmup ok)
MIN_BARS_REQUIRED: int = (
    max(SMA_PERIOD, RETURN_RANK_RANGE // 4, LIQUIDITY_ADV_WINDOW)
    + ENTRY_N_DAY_RETURN + 30
)

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR                  = _BASE / "data"   # input for migrate.py only

# ── IB Connection ───────────────────────────────────────────────────────────────
IB_HOST                   = "127.0.0.1"
IB_PORT                   = 7496        # 7497 for paper trading
IB_CLIENT_ID              = 1
IB_SUBACCOUNT             = ""          # Subaccount ID (e.g. "DU1234567"); empty = use master account
IB_HEARTBEAT_TIMEOUT_SEC  = 5           # seconds to wait for reqCurrentTime response
IB_SOFT_ERROR_CODES       = [2104, 2106, 2107, 2108, 2158]   # informational; not logged as errors
IB_REJECTION_CODES        = [201, 202, 203, 321, 322]         # hard order rejections

# ── IBC (automated TWS/Gateway login) ──────────────────────────────────────────
IBC_PATH                  = "/opt/ibc/ibc.sh"
IBC_TWS_PATH              = "/opt/Trader Workstation"
IBC_CONFIG_PATH           = "/opt/ibc/config.ini"
IBC_2FA_DAY               = "sunday"
IBC_2FA_TIME              = "18:00"
IBC_RESTART_TIMEOUT       = 120         # seconds to wait for TWS restart

# ── Scheduling (all times NY / ET) ─────────────────────────────────────────────
TIME_NIGHTLY_SYNC           = "20:00"   # Fixed — not relative to market close

# ── Intraday job offsets from market close (minutes) ───────────────────────────
SCHED_SIGNAL_OFFSET_MIN     = -20       # Signal snap: close - 20 min
SCHED_ORDER_OFFSET_MIN      = -16       # Order submission: close - 16 min
SCHED_FILL_OFFSET_MIN       = +10       # Fill reconciliation: close + 10 min
SCHED_REPORT_OFFSET_MIN     = +15       # Daily report: close + 15 min

# ── Half-day calendar fallback ──────────────────────────────────────────────────
HALF_DAY_DATES: list[str] = [
    # NYSE early-close dates (13:00 ET) not yet reflected in pandas_market_calendars.
    # Add YYYY-MM-DD strings here when the library lags behind announced schedule changes.
    # Example: "2026-11-27"   # Black Friday 2026
]

# ── Order execution ─────────────────────────────────────────────────────────────
ENTRY_ORDER_TYPE          = "LOC"       # "MOC" | "LOC"
ENTRY_LOC_BUFFER_PCT      = 0.003       # 0.3% above snap price; ignored if MOC
EXIT_ORDER_TYPE           = "MOC"       # keep exits as MOC — non-execution risk too high

# ── Data sources ────────────────────────────────────────────────────────────────
TWELVEDATA_API_KEY            = "YOUR_KEY_HERE"   # free tier: 8 credits/min (8 symbols/min when batching); paid plans support higher limits
TWELVEDATA_INCREMENTAL_DAYS   = 5           # normal nightly lookback
TWELVEDATA_HISTORY_DAYS       = 550         # full history depth for new symbols (~252 bars + buffer)
TWELVEDATA_RATE_LIMIT_PER_MIN = 8           # max requests per minute (free tier = 8)
UNIVERSE_CSV                  = str(_BASE / "state" / "universe.csv")

# ── Database ────────────────────────────────────────────────────────────────────
DB_DRIVER                 = "sqlite"    # "sqlite" | "postgresql"
DB_PATH                   = str(_BASE / "state" / "bars.db")   # SQLite only
DB_HOST                   = "localhost"                         # PostgreSQL only
DB_PORT                   = 5432                               # PostgreSQL only
DB_NAME                   = "murphy"                           # PostgreSQL only
DB_USER                   = ""                                 # PostgreSQL only
DB_PASSWORD               = ""                                 # PostgreSQL only

# ── State export ────────────────────────────────────────────────────────────────
EXPORT_STATE_JSON         = True        # also write state/positions.json on every update

# ── Logging ─────────────────────────────────────────────────────────────────────
LOG_TO_FILE               = True        # write logs/murphy_YYYYMMDD.log
LOG_LEVEL                 = "INFO"      # DEBUG | INFO | WARNING | ERROR

# ── Reporting ───────────────────────────────────────────────────────────────────
REPORT_DAILY              = True
REPORT_WEEKLY             = True
REPORT_WEEKLY_DAY         = "friday"

# ── Alerting ────────────────────────────────────────────────────────────────────
ALERT_EMAIL               = ""          # recipient address; empty = disabled
SMTP_HOST                 = "smtp.gmail.com"
SMTP_PORT                 = 587
SMTP_USER                 = ""
SMTP_PASSWORD             = ""          # use an app password, not account password
DISCORD_WEBHOOK_URL       = ""          # empty = disabled
DISCORD_ALERT_MENTIONS    = ""          # e.g. "<@USER_ID>" prepended on critical alerts only

# ── Risk controls (see Section 7 of ROADMAP for full documentation) ────────────
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

RISK_IMBALANCE_ENABLED          = False     # optional; disabled by default
RISK_IMBALANCE_THRESHOLD        = 0.3
RISK_IMBALANCE_ACTION           = ["reject"]

# ── S&P 500 universe management ────────────────────────────────────────────────
SYMBOL_WHITELIST          = []          # always included regardless of S&P membership
SP500_CSV_URL             = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
SP500_UPDATE_DAY          = "sunday"    # day to refresh constituent list
SP500_UPDATE_TIME         = "17:00"     # NY time — before IBC reauth

# ── Future: Web dashboard ───────────────────────────────────────────────────────
WEBSERVER_HOST            = "0.0.0.0"
WEBSERVER_PORT            = 8080

# ── Future: Remote API ──────────────────────────────────────────────────────────
API_HOST                  = "0.0.0.0"
API_PORT                  = 8081
API_SECRET_KEY            = "CHANGE_ME"
