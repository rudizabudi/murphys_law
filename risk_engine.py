"""
risk_engine.py — Risk controls engine.

All risk logic lives here. No risk checks anywhere else.

Public API
──────────
  evaluate(control_name, context) -> bool
      Evaluate a named control. Returns True = proceed, False = blocked.
      Checks persistent halt/shutdown state first on every call.

  is_halted()  -> bool   True if halt OR shutdown is active in the DB.
  is_shutdown() -> bool  True if shutdown is active.
  clear_halt()           Clear both halt and shutdown flags (manual reset).

Context dict keys by control
─────────────────────────────
  max_order_value      order_value: float          — order notional ($)
  daily_loss           daily_pnl: float,           — day P&L (negative = loss)
                       equity: float               — current total equity
  max_drawdown         current_equity: float
                       peak_equity: float
  margin_breach        margin_pct: float           — excess margin as fraction (0.25 = 25%)
  stale_state          last_update_date: date|str  — date of last DB state write
  consec_loss_days     consec_loss_days: int
  consec_loss_trades   consec_loss_trades: int
  fill_timeout         minutes_pending: float
  reconcile_mismatch   mismatch: bool,
                       detail: str (optional)
  imbalance            imbalance_ratio: float,
                       symbol: str (optional)
"""

import logging
from datetime import date

import config
import db
import monitor

logger = logging.getLogger("murphy")

# ═══════════════════════════════════════════════════════════════════════════════
# system_state table  (key/value; survives process restarts)
# ═══════════════════════════════════════════════════════════════════════════════

_DDL_SYSTEM_STATE = """
CREATE TABLE IF NOT EXISTS system_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
)
"""

_table_ready: bool = False


def _ensure_table() -> None:
    global _table_ready
    if _table_ready:
        return
    with db.connect() as conn:
        conn.execute(_DDL_SYSTEM_STATE)
    _table_ready = True


def _get_state(key: str) -> str:
    """Read a key from system_state; returns '' on any error or missing key."""
    try:
        _ensure_table()
        with db.connect() as conn:
            row = conn.execute(
                f"SELECT value FROM system_state WHERE key = {db.ph()}",
                (key,),
            ).fetchone()
        return (row[0] if row else "") or ""
    except Exception as exc:
        logger.debug("[risk] _get_state(%r) failed: %s", key, exc)
        return ""


def _set_state(key: str, value: str) -> None:
    """Write a key/value pair to system_state (upsert)."""
    _ensure_table()
    p = db.ph()
    if config.DB_DRIVER == "sqlite":
        sql = (
            f"INSERT OR REPLACE INTO system_state (key, value) "
            f"VALUES ({p}, {p})"
        )
    else:
        sql = (
            f"INSERT INTO system_state (key, value) VALUES ({p}, {p}) "
            f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        )
    with db.connect() as conn:
        conn.execute(sql, (key, value))


# ═══════════════════════════════════════════════════════════════════════════════
# Public state helpers
# ═══════════════════════════════════════════════════════════════════════════════

def is_halted() -> bool:
    """True if a halt OR shutdown flag is active in the DB."""
    return _get_state("halt") == "1" or _get_state("shutdown") == "1"


def is_shutdown() -> bool:
    """True if a shutdown flag is active in the DB."""
    return _get_state("shutdown") == "1"


def clear_halt() -> None:
    """
    Clear both halt and shutdown flags. Manual reset only.
    Safe to call when no halt is active.
    """
    _set_state("halt",     "")
    _set_state("shutdown", "")
    logger.info("[risk] Halt/shutdown flags cleared.")


# ═══════════════════════════════════════════════════════════════════════════════
# Action executor
# ═══════════════════════════════════════════════════════════════════════════════

def _execute_actions(
    actions: list[str],
    control_name: str,
    message: str,
) -> bool:
    """
    Execute every action in *actions* (no short-circuit — notify always fires
    alongside halt/shutdown).  Returns False if any blocking action was executed.
    """
    if "shutdown" in actions:
        alert_level = "critical"
    else:
        alert_level = "warning"

    blocked = False
    for action in actions:
        if action == "notify":
            monitor.send_alert(
                subject=f"[Risk] {control_name} triggered",
                body=message,
                level=alert_level,
            )
        elif action == "reject":
            logger.warning("[risk] %s: ORDER REJECTED — %s", control_name, message)
            blocked = True
        elif action == "skip":
            logger.warning("[risk] %s: JOB SKIPPED — %s", control_name, message)
            blocked = True
        elif action == "halt":
            _set_state("halt", "1")
            logger.warning("[risk] %s: HALT SET — %s", control_name, message)
            blocked = True
        elif action == "shutdown":
            _set_state("shutdown", "1")
            logger.critical("[risk] %s: SHUTDOWN SET — %s", control_name, message)
            blocked = True
        else:
            logger.warning(
                "[risk] Unknown action %r for control %r", action, control_name
            )
    return not blocked


# ═══════════════════════════════════════════════════════════════════════════════
# Individual control handlers
# Each returns (breached: bool, message: str).
# breached=True → threshold exceeded; actions will fire.
# ═══════════════════════════════════════════════════════════════════════════════

def _check_max_order_value(ctx: dict) -> tuple[bool, str]:
    threshold = config.RISK_MAX_ORDER_VALUE
    if threshold < 0:
        return False, ""
    val = float(ctx.get("order_value", 0))
    if val > threshold:
        return True, (
            f"Order value ${val:,.2f} exceeds limit ${threshold:,.0f}"
        )
    return False, ""


def _check_daily_loss(ctx: dict) -> tuple[bool, str]:
    equity    = float(ctx.get("equity",    0))
    daily_pnl = float(ctx.get("daily_pnl", 0))
    if equity <= 0 or daily_pnl >= 0:
        return False, ""
    loss_pct  = abs(daily_pnl) / equity
    threshold = config.RISK_DAILY_LOSS_PCT
    if loss_pct > threshold:
        return True, (
            f"Daily loss {loss_pct * 100:.2f}% exceeds limit "
            f"{threshold * 100:.1f}%  (P&L: ${daily_pnl:,.2f})"
        )
    return False, ""


def _check_max_drawdown(ctx: dict) -> tuple[bool, str]:
    peak    = float(ctx.get("peak_equity",    0))
    current = float(ctx.get("current_equity", 0))
    if peak <= 0:
        return False, ""
    dd_pct    = (peak - current) / peak
    threshold = config.RISK_MAX_DD_PCT
    if dd_pct > threshold:
        return True, (
            f"Drawdown {dd_pct * 100:.2f}% exceeds limit "
            f"{threshold * 100:.1f}%  "
            f"(peak ${peak:,.0f}, current ${current:,.0f})"
        )
    return False, ""


def _check_margin_breach(ctx: dict) -> tuple[bool, str]:
    margin_pct = float(ctx.get("margin_pct", 1.0))
    threshold  = config.RISK_MARGIN_MIN_PCT
    if margin_pct < threshold:
        return True, (
            f"Margin {margin_pct * 100:.1f}% below minimum "
            f"{threshold * 100:.1f}%"
        )
    return False, ""


def _check_stale_state(ctx: dict) -> tuple[bool, str]:
    last = ctx.get("last_update_date")
    if last is None:
        return True, "last_update_date not provided in context"
    if isinstance(last, str):
        last = date.fromisoformat(last)
    days_old  = (date.today() - last).days
    threshold = config.RISK_STALE_STATE_DAYS
    if days_old > threshold:
        return True, (
            f"State is {days_old} day(s) old (limit: {threshold}); "
            f"last update: {last}"
        )
    return False, ""


def _check_consec_loss_days(ctx: dict) -> tuple[bool, str]:
    n         = int(ctx.get("consec_loss_days", 0))
    threshold = config.RISK_CONSEC_LOSS_DAYS
    if n >= threshold:
        return True, (
            f"{n} consecutive losing day(s) "
            f"(alert threshold: {threshold})"
        )
    return False, ""


def _check_consec_loss_trades(ctx: dict) -> tuple[bool, str]:
    n         = int(ctx.get("consec_loss_trades", 0))
    threshold = config.RISK_CONSEC_LOSS_TRADES
    if n >= threshold:
        return True, (
            f"{n} consecutive losing trade(s) "
            f"(alert threshold: {threshold})"
        )
    return False, ""


def _check_fill_timeout(ctx: dict) -> tuple[bool, str]:
    mins      = float(ctx.get("minutes_pending", 0))
    threshold = config.RISK_FILL_TIMEOUT_MINS
    if mins >= threshold:
        return True, (
            f"Order fill pending for {mins:.1f} min "
            f"(timeout: {threshold} min)"
        )
    return False, ""


def _check_reconcile_mismatch(ctx: dict) -> tuple[bool, str]:
    if ctx.get("mismatch", False):
        detail = ctx.get("detail", "IB positions do not match DB positions")
        return True, detail
    return False, ""


def _check_imbalance(ctx: dict) -> tuple[bool, str]:
    ratio     = float(ctx.get("imbalance_ratio", 0))
    threshold = config.RISK_IMBALANCE_THRESHOLD
    sym       = ctx.get("symbol", "unknown")
    if ratio >= threshold:
        return True, (
            f"Closing auction imbalance ratio {ratio:.2f} >= "
            f"{threshold:.2f} for {sym}"
        )
    return False, ""


# ═══════════════════════════════════════════════════════════════════════════════
# Control registry — maps name → (enabled_key, action_key, handler)
# ═══════════════════════════════════════════════════════════════════════════════

_REGISTRY: dict[str, dict] = {
    "max_order_value": {
        "enabled_key": "RISK_MAX_ORDER_VALUE_ENABLED",
        "action_key":  "RISK_MAX_ORDER_VALUE_ACTION",
        "handler":     _check_max_order_value,
    },
    "daily_loss": {
        "enabled_key": "RISK_DAILY_LOSS_ENABLED",
        "action_key":  "RISK_DAILY_LOSS_ACTION",
        "handler":     _check_daily_loss,
    },
    "max_drawdown": {
        "enabled_key": "RISK_MAX_DD_ENABLED",
        "action_key":  "RISK_MAX_DD_ACTION",
        "handler":     _check_max_drawdown,
    },
    "margin_breach": {
        "enabled_key": "RISK_MARGIN_ENABLED",
        "action_key":  "RISK_MARGIN_ACTION",
        "handler":     _check_margin_breach,
    },
    "stale_state": {
        "enabled_key": "RISK_STALE_STATE_ENABLED",
        "action_key":  "RISK_STALE_STATE_ACTION",
        "handler":     _check_stale_state,
    },
    "consec_loss_days": {
        "enabled_key": "RISK_CONSEC_LOSS_DAYS_ENABLED",
        "action_key":  "RISK_CONSEC_LOSS_DAYS_ACTION",
        "handler":     _check_consec_loss_days,
    },
    "consec_loss_trades": {
        "enabled_key": "RISK_CONSEC_LOSS_TRADES_ENABLED",
        "action_key":  "RISK_CONSEC_LOSS_TRADES_ACTION",
        "handler":     _check_consec_loss_trades,
    },
    "fill_timeout": {
        "enabled_key": "RISK_FILL_TIMEOUT_ENABLED",
        "action_key":  "RISK_FILL_TIMEOUT_ACTION",
        "handler":     _check_fill_timeout,
    },
    "reconcile_mismatch": {
        "enabled_key": "RISK_RECONCILE_ENABLED",
        "action_key":  "RISK_RECONCILE_ACTION",
        "handler":     _check_reconcile_mismatch,
    },
    "imbalance": {
        "enabled_key": "RISK_IMBALANCE_ENABLED",
        "action_key":  "RISK_IMBALANCE_ACTION",
        "handler":     _check_imbalance,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Main interface
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate(control_name: str, context: dict) -> bool:
    """
    Evaluate a named risk control against the provided context.

    Returns True  — execution should proceed.
    Returns False — execution is blocked (reject / skip / halt / shutdown active).

    Persistent halt/shutdown state is always checked first.
    Fails open (returns True) on infrastructure or handler errors so that a DB
    outage does not freeze all trading.
    """
    try:
        _ensure_table()
    except Exception as exc:
        logger.error("[risk] system_state table init failed: %s", exc)
        return True  # Fail open on infrastructure error

    # Persistent gate — checked before every individual control
    if _get_state("shutdown") == "1":
        logger.warning("[risk] SHUTDOWN active — %s blocked", control_name)
        return False
    if _get_state("halt") == "1":
        logger.warning("[risk] HALT active — %s blocked", control_name)
        return False

    reg = _REGISTRY.get(control_name)
    if reg is None:
        logger.error("[risk] Unknown control: %r", control_name)
        return True  # Fail open for unknown names

    if not getattr(config, reg["enabled_key"], True):
        return True  # Control disabled — pass through

    try:
        breached, message = reg["handler"](context)
    except Exception as exc:
        logger.error("[risk] Control %r handler raised: %s", control_name, exc)
        return True  # Fail open on handler error

    if not breached:
        return True

    actions = getattr(config, reg["action_key"], ["notify"])
    return _execute_actions(actions, control_name, message)
