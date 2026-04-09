"""
main.py — Orchestrator.

Wires all modules together into the daily execution flow.  Each scheduled
job is a standalone callable so the scheduler (and manual invocation) can
call them independently.

Module-level state
──────────────────
  bridge          — IBBridge instance, created once at import time.
  _snap_state     — dict that signal_snap() writes and order_submission()
                    reads.  Keys: entry_signals, exit_signals, snap_prices,
                    open_positions, account, snap_date.
  _submitted      — dict that order_submission() writes and
                    fill_reconciliation() reads.  Maps order_id → {symbol,
                    action, pos_id, shares, fill_price, reason}.

All functions use monitor.logger — no print() statements.
"""

import csv
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

import config
import db
import indicators
import monitor
import order_manager
import portfolio_state
import risk_engine
import signals
import ib_data
import td_data
import universe
from ib_exec import (
    IBBridge, IBCController, OrderRejectedError,
    detect_splits, get_account_summary, get_filled_orders, get_ib_positions, submit_order,
)

logger = logging.getLogger("murphy")

# ── Module-level IB bridge (created once, reused across all jobs) ─────────────
bridge: IBBridge = IBBridge()

# ── Inter-job state (signal_snap → order_submission → fill_reconciliation) ────
_snap_state: dict[str, Any] = {}
_submitted:  dict[int, dict] = {}


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _load_universe() -> list[str]:
    """Read symbol list from config.UNIVERSE_CSV. Returns [] if file is absent."""
    path = Path(config.UNIVERSE_CSV)
    if not path.exists():
        logger.warning("[main] Universe file not found: %s", path)
        return []
    symbols: list[str] = []
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if row and row[0].strip() and not row[0].strip().startswith("#"):
                symbols.append(row[0].strip())
    logger.info("[main] Universe: %d symbols loaded from %s", len(symbols), path)
    return symbols


def _load_history_from_db(symbol: str) -> pd.DataFrame | None:
    """Load all daily bars for *symbol* from daily_bars, sorted by date ascending."""
    p = db.ph()
    try:
        with db.connect() as conn:
            rows = conn.execute(
                f"SELECT date, open, high, low, close, volume "
                f"FROM daily_bars WHERE symbol = {p} ORDER BY date",
                (symbol,),
            ).fetchall()
    except Exception as exc:
        logger.error("[main] DB read failed for %s: %s", symbol, exc)
        return None

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").astype(float)
    return df


def _load_watchlist_from_db() -> list[str]:
    """Return symbols currently in the watchlist table."""
    try:
        with db.connect() as conn:
            rows = conn.execute("SELECT symbol FROM watchlist").fetchall()
        return [row[0] for row in rows]
    except Exception as exc:
        logger.warning("[main] Could not read watchlist: %s", exc)
        return []


def _fetch_ib_snapshot(symbols: list[str]) -> dict[str, dict]:
    """Delegate to ib_data.fetch_snapshot — see ib_data.py for full docs."""
    return ib_data.fetch_snapshot(symbols, bridge)


def _merge_today_bar(
    df: pd.DataFrame,
    snap: dict,
    today: pd.Timestamp,
) -> pd.DataFrame:
    """
    If *snap* contains a valid close, append or overwrite today's bar in *df*.
    Returns df unchanged if snap is empty or close <= 0.
    """
    close = snap.get("close", 0)
    if not close or close <= 0:
        return df
    bar = pd.DataFrame(
        [{
            "open":   snap.get("open",   close),
            "high":   snap.get("high",   close),
            "low":    snap.get("low",    close),
            "close":  float(close),
            "volume": snap.get("volume", 0),
        }],
        index=[today],
    )
    bar.index.name = "date"
    if today in df.index:
        df = df.drop(today)
    return pd.concat([df, bar]).sort_index()


def _consec_loss_stats() -> tuple[int, int]:
    """
    Compute consecutive losing days and consecutive losing trades from trade_log.
    Returns (consec_loss_days, consec_loss_trades).
    Both reset on the first profitable entry encountered when scanning backwards.
    """
    p = db.ph()
    try:
        with db.connect() as conn:
            rows = conn.execute(
                "SELECT exit_date, pnl FROM trade_log ORDER BY exit_date DESC, rowid DESC"
            ).fetchall()
    except Exception:
        return 0, 0

    # Consecutive losing trades (scan backwards until first winner)
    consec_trades = 0
    for row in rows:
        pnl = row[1] if row[1] is not None else 0.0
        if pnl <= 0:
            consec_trades += 1
        else:
            break

    # Consecutive losing days (aggregate by exit_date, scan backwards)
    days: dict[str, float] = {}
    for row in rows:
        d = str(row[0])[:10]
        days[d] = days.get(d, 0.0) + (row[1] or 0.0)
    consec_days = 0
    for d_pnl in sorted(days.items(), key=lambda x: x[0], reverse=True):
        if d_pnl[1] <= 0:
            consec_days += 1
        else:
            break

    return consec_days, consec_trades


def _reconcile_with_ib(
    positions: list[dict],
    ib_positions: list[dict],
) -> tuple[bool, str]:
    """
    Compare DB open position symbols against the full IB position report.
    Using the live IB position list catches pre-existing discrepancies from
    prior sessions, not just today's fills.
    Returns (mismatch: bool, detail: str).
    """
    ib_syms  = {p["symbol"] for p in ib_positions}
    db_syms  = {p["symbol"] for p in positions}
    extra_ib = ib_syms - db_syms
    extra_db = db_syms - ib_syms
    if extra_ib or extra_db:
        detail = (
            f"IB has extra: {sorted(extra_ib)}; DB has extra: {sorted(extra_db)}"
        )
        return True, detail
    return False, ""


# ═══════════════════════════════════════════════════════════════════════════════
# Job: nightly_sync
# ═══════════════════════════════════════════════════════════════════════════════

def nightly_sync() -> None:
    """
    ~20:00 ET — TwelveData incremental bar update + watchlist precompute.

    1. Stale-state risk check (skip if last DB update is too old).
    2. Fetch last few days of bars from TwelveData for every universe symbol.
    3. Upsert into daily_bars.
    4. Run precompute_watchlist().
    """
    logger.info("[main] nightly_sync: starting")
    monitor.setup_logging()

    # ── Risk: stale state check ───────────────────────────────────────────────
    ok = risk_engine.evaluate(
        "stale_state",
        {"last_update_date": date.today()},   # today's sync counts as fresh
    )
    if not ok:
        logger.warning("[main] nightly_sync: stale_state check blocked — aborting")
        return

    symbols = _load_universe()
    if not symbols:
        logger.error("[main] nightly_sync: empty universe, nothing to sync")
        return

    db.init_db()
    total_upserted = 0

    for sym in symbols:
        rows = td_data.fetch_bars(sym, config.TWELVEDATA_INCREMENTAL_DAYS)
        if rows:
            n = db.upsert_daily_bars(rows)
            total_upserted += n

    logger.info("[main] nightly_sync: upserted %d bar rows for %d symbols",
                total_upserted, len(symbols))

    # ── Safety net: full history fetch for any symbols still lacking bars ─────
    new_syms = universe.get_new_symbols()
    if new_syms:
        logger.info("[main] nightly_sync: %d symbol(s) still need full history", len(new_syms))
        history_upserted = 0
        for sym in new_syms:
            rows = td_data.fetch_bars(sym, config.TWELVEDATA_HISTORY_DAYS)
            if rows:
                history_upserted += db.upsert_daily_bars(rows)
        logger.info("[main] nightly_sync: full-history upserted %d rows for %d symbol(s)",
                    history_upserted, len(new_syms))

    precompute_watchlist()
    logger.info("[main] nightly_sync: complete")


def precompute_watchlist() -> None:
    """
    Evaluate all price-independent entry conditions across the full universe
    and write passing symbols to the watchlist table.

    Conditions checked (no price-sensitive data required):
      - close > sma200 (using last available close)
      - adv63 > 0 (liquidity data available)
      - QPI rolling window sufficiently warmed up (q_threshold is not NaN)
      - Symbol not already in an open position

    Results written to watchlist table: symbol, updated_date, sma200, q_threshold, adv63.
    """
    logger.info("[main] precompute_watchlist: starting")
    symbols   = _load_universe()
    open_syms = {p["symbol"] for p in portfolio_state.load_positions()}
    today_str = str(date.today())

    p = db.ph()
    if config.DB_DRIVER == "sqlite":
        upsert_sql = (
            f"INSERT OR REPLACE INTO watchlist "
            f"(symbol, updated_date, sma200, q_threshold, adv63) "
            f"VALUES ({p},{p},{p},{p},{p})"
        )
    else:
        upsert_sql = (
            f"INSERT INTO watchlist (symbol, updated_date, sma200, q_threshold, adv63) "
            f"VALUES ({p},{p},{p},{p},{p}) "
            f"ON CONFLICT (symbol) DO UPDATE SET "
            f"updated_date=EXCLUDED.updated_date, sma200=EXCLUDED.sma200, "
            f"q_threshold=EXCLUDED.q_threshold, adv63=EXCLUDED.adv63"
        )

    accepted = 0
    skipped  = 0

    # Clear stale watchlist before recomputing
    with db.connect() as conn:
        conn.execute("DELETE FROM watchlist")

    for sym in symbols:
        if sym in open_syms:
            skipped += 1
            continue

        df = _load_history_from_db(sym)
        if df is None or df.empty:
            skipped += 1
            continue

        df_ind = indicators.compute_indicators(df)
        last = df_ind.iloc[-1]

        sma200      = last.get("sma200")
        q_threshold = last.get("q_threshold")
        adv63       = last.get("adv63")
        close       = last.get("close")

        # Price-independent gate: sma200 must be valid and close above it
        if pd.isna(sma200) or pd.isna(close) or close <= sma200:
            skipped += 1
            continue

        # QPI window must be sufficiently warmed up
        if pd.isna(q_threshold):
            skipped += 1
            continue

        # Liquidity must be available
        if pd.isna(adv63) or adv63 <= 0:
            skipped += 1
            continue

        with db.connect() as conn:
            conn.execute(upsert_sql, (sym, today_str, float(sma200), float(q_threshold), float(adv63)))

        accepted += 1

    logger.info(
        "[main] precompute_watchlist: %d symbols → watchlist (%d skipped)",
        accepted, skipped,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Job: connectivity_check
# ═══════════════════════════════════════════════════════════════════════════════

def connectivity_check() -> None:
    """
    09:00 ET Mon–Fri — Confirm TWS/Gateway is reachable before intraday jobs run.

    1. bridge.heartbeat() — reqCurrentTime() round-trip.
    2. If heartbeat fails: bridge.reconnect() once, then heartbeat() again.
    3. If still failing after reconnect: critical alert + risk_engine halt.
    4. If reconnect succeeds: WARNING log + info alert.
    """
    logger.info("[main] connectivity_check: starting")

    if bridge.heartbeat():
        logger.debug("[main] connectivity_check: IB connection healthy")
        return

    # First heartbeat failed — attempt one reconnect
    logger.warning("[main] connectivity_check: heartbeat failed, attempting reconnect")
    try:
        bridge.reconnect()
    except Exception as exc:
        logger.error("[main] connectivity_check: reconnect raised: %s", exc)

    if bridge.heartbeat():
        logger.warning("[main] connectivity_check: reconnect succeeded")
        monitor.send_alert(
            "IB connectivity restored",
            "Heartbeat failed at 09:00 ET but recovered after one reconnect.",
            level="info",
        )
        return

    # Still down after reconnect
    logger.critical("[main] connectivity_check: IB unreachable after reconnect")
    monitor.send_alert(
        "IB connectivity CRITICAL",
        "Heartbeat failed at 09:00 ET and did not recover after reconnect. "
        "Intraday jobs will not run until connection is restored.",
        level="critical",
    )
    risk_engine.evaluate("reconcile_mismatch", {
        "mismatch": True,
        "detail": "connectivity_check: IB unreachable at 09:00 ET",
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Job: signal_snap
# ═══════════════════════════════════════════════════════════════════════════════

def signal_snap() -> None:
    """
    15:40 ET — IB intraday snapshot + signal evaluation.

    1. Load watchlist from DB.
    2. Fetch today's bar from IB for watchlist symbols only.
    3. Merge today's bar with DB history.
    4. Run compute_indicators() + get_entry_signals() + get_exit_signals().
    5. Store results in _snap_state for order_submission() to consume.
    """
    global _snap_state
    logger.info("[main] signal_snap: starting")

    watchlist_syms = _load_watchlist_from_db()
    if not watchlist_syms:
        logger.warning("[main] signal_snap: watchlist is empty — skipping")
        return

    open_positions = portfolio_state.load_positions()
    open_syms      = [p["symbol"] for p in open_positions]
    # Also snapshot held positions so MTM and exit signals work
    all_snap_syms  = list(set(watchlist_syms) | set(open_syms))

    logger.info("[main] signal_snap: fetching IB snapshot for %d symbols", len(all_snap_syms))
    snaps = _fetch_ib_snapshot(all_snap_syms)

    today = pd.Timestamp(date.today())
    loaded_data: dict[str, pd.DataFrame] = {}
    snap_prices: dict[str, float]        = {}

    for sym in all_snap_syms:
        df = _load_history_from_db(sym)
        if df is None:
            continue
        snap = snaps.get(sym, {})
        df   = _merge_today_bar(df, snap, today)
        if df.empty:
            continue
        df_ind            = indicators.compute_indicators(df)
        loaded_data[sym]  = df_ind
        snap_prices[sym]  = float(snap.get("close", df_ind["close"].iloc[-1]))

    logger.info("[main] signal_snap: indicators computed for %d symbols", len(loaded_data))

    # Only evaluate entry signals for watchlist symbols
    watchlist_data = {s: loaded_data[s] for s in watchlist_syms if s in loaded_data}
    entry_sigs = signals.get_entry_signals(watchlist_data, today)
    exit_sigs  = signals.get_exit_signals(open_positions, loaded_data, today)

    logger.info("[main] signal_snap: %d entry signal(s), %d exit signal(s)",
                len(entry_sigs), len(exit_sigs))

    account = {}
    try:
        account = get_account_summary(bridge)
    except Exception as exc:
        logger.warning("[main] signal_snap: get_account_summary failed: %s", exc)

    _snap_state = {
        "entry_signals":    entry_sigs,
        "exit_signals":     exit_sigs,
        "snap_prices":      snap_prices,
        "open_positions":   open_positions,
        "account":          account,
        "snap_date":        date.today(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Job: order_submission
# ═══════════════════════════════════════════════════════════════════════════════

def order_submission() -> None:
    """
    15:44 ET — Risk checks + order construction + IB submission.

    Reads _snap_state written by signal_snap().
    Writes submitted order details to _submitted for fill_reconciliation().
    """
    global _submitted
    logger.info("[main] order_submission: starting")

    if not _snap_state:
        logger.warning("[main] order_submission: no snap_state — signal_snap may not have run")
        return

    entry_signals  = _snap_state.get("entry_signals",  [])
    exit_signals   = _snap_state.get("exit_signals",   [])
    snap_prices    = _snap_state.get("snap_prices",    {})
    open_positions = _snap_state.get("open_positions", [])
    account        = _snap_state.get("account",        {})
    current_equity = float(account.get("net_liquidation", 0))

    # ── Pre-flight risk checks ────────────────────────────────────────────────
    if risk_engine.is_shutdown():
        logger.critical("[main] order_submission: SHUTDOWN active — no orders submitted")
        return

    # Daily loss — unrealised intraday P&L across all open positions
    daily_pnl = sum(
        (snap_prices.get(p["symbol"], p["fill_price"]) - p["fill_price"]) * p["shares"]
        for p in open_positions
    )
    risk_engine.evaluate("daily_loss", {
        "daily_pnl": daily_pnl,
        "equity":    current_equity,
    })

    # Max drawdown — requires peak equity from equity_log
    peak = current_equity  # conservative fallback
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT MAX(equity_eod) FROM equity_log"
            ).fetchone()
        if row and row[0]:
            peak = float(row[0])
    except Exception:
        pass
    risk_engine.evaluate("max_drawdown", {"current_equity": current_equity, "peak_equity": peak})

    if risk_engine.is_halted():
        logger.warning("[main] order_submission: HALT active — entries blocked (exits may proceed)")

    # ── Build and submit exit orders ──────────────────────────────────────────
    exit_orders = order_manager.build_exit_orders(exit_signals, open_positions)
    _submitted  = {}

    for order in exit_orders:
        try:
            oid = submit_order(bridge, order)
            _submitted[oid] = {
                "symbol":     order.symbol,
                "action":     order.action,
                "pos_id":     order.pos_id,
                "shares":     order.quantity,
                "fill_price": snap_prices.get(order.symbol, 0.0),
                "reason":     order.reason,
            }
            logger.info("[main] submitted EXIT order id=%d %s %s qty=%d reason=%s",
                        oid, order.action, order.symbol, order.quantity, order.reason)
        except OrderRejectedError as exc:
            logger.error("[main] EXIT order REJECTED by IB id=%d %s: %s",
                         exc.order_id, order.symbol, exc.message)
            monitor.send_alert(
                f"Order rejected: {order.symbol}",
                f"EXIT order rejected by IB\nSymbol: {order.symbol}\nReason: {exc.message}",
                level="warning",
            )
        except Exception as exc:
            logger.error("[main] EXIT order submission failed for %s: %s", order.symbol, exc)

    # ── Build and submit entry orders (only if not halted) ────────────────────
    if not risk_engine.is_halted():
        entry_orders = order_manager.build_entry_orders(
            entry_signals,
            open_positions,
            current_equity,
            snap_prices,
        )
        for order in entry_orders:
            # Per-order risk: max single order value
            notional = order.quantity * snap_prices.get(order.symbol, 0.0)
            ok = risk_engine.evaluate("max_order_value", {"order_value": notional})
            if not ok:
                logger.info("[main] entry order for %s rejected by max_order_value", order.symbol)
                continue
            try:
                oid = submit_order(bridge, order)
                _submitted[oid] = {
                    "symbol":      order.symbol,
                    "action":      order.action,
                    "pos_id":      "",
                    "shares":      order.quantity,
                    "fill_price":  snap_prices.get(order.symbol, 0.0),
                    "reason":      order.reason,
                    "order_type":  order.order_type,
                    "limit_price": order.limit_price,
                }
                logger.info("[main] submitted ENTRY order id=%d %s %s qty=%d type=%s",
                            oid, order.action, order.symbol, order.quantity, order.order_type)
            except OrderRejectedError as exc:
                logger.error("[main] ENTRY order REJECTED by IB id=%d %s: %s",
                             exc.order_id, order.symbol, exc.message)
                monitor.send_alert(
                    f"Order rejected: {order.symbol}",
                    f"ENTRY order rejected by IB\nSymbol: {order.symbol}\nReason: {exc.message}",
                    level="warning",
                )
            except Exception as exc:
                logger.error("[main] ENTRY order submission failed for %s: %s", order.symbol, exc)
    else:
        logger.info("[main] order_submission: HALT active — %d entry signal(s) suppressed",
                    len(entry_signals))

    logger.info("[main] order_submission: %d order(s) submitted", len(_submitted))


# ═══════════════════════════════════════════════════════════════════════════════
# Job: fill_reconciliation
# ═══════════════════════════════════════════════════════════════════════════════

def fill_reconciliation() -> None:
    """
    16:10 ET — Confirm fills, update positions, snapshot equity, reconcile with IB.

    1. get_filled_orders() for all submitted order IDs.
    2. detect_splits() — compare IB vs DB share counts; update positions table
       for any detected splits; exclude them from the reconciliation check.
    3. For new entries: save_position() with fill details.
    4. For exits: close_position() → trade_log.
    5. append_equity_snapshot().
    6. reconcile_with_ib() → risk_engine on mismatch (split symbols excluded).
    7. export_positions_json() if enabled.
    """
    logger.info("[main] fill_reconciliation: starting")

    order_ids = list(_submitted.keys())
    if not order_ids:
        logger.info("[main] fill_reconciliation: no submitted orders to reconcile")

    filled: dict[int, dict] = {}
    try:
        filled = get_filled_orders(bridge, order_ids)
    except Exception as exc:
        logger.error("[main] fill_reconciliation: get_filled_orders failed: %s", exc)

    open_positions = portfolio_state.load_positions()
    account        = _snap_state.get("account", {})
    current_equity = float(account.get("net_liquidation", 0))
    snap_prices    = _snap_state.get("snap_prices", {})
    today          = date.today()

    # ── Split detection + fetch live IB positions for later reconciliation ────
    split_symbols: set[str] = set()
    ib_pos: list[dict] = []
    try:
        ib_pos = get_ib_positions(bridge)
        splits = detect_splits(ib_pos, open_positions)
        for sp in splits:
            sym      = sp["symbol"]
            new_shr  = sp["ib_shares"]
            pos_row  = next((p for p in open_positions if p["symbol"] == sym), None)
            if pos_row is None:
                continue
            notional      = float(pos_row["notional"])
            new_fill_price = notional / new_shr if new_shr > 0 else 0.0
            p = db.ph()
            with db.connect() as conn:
                conn.execute(
                    f"UPDATE positions SET shares = {p}, fill_price = {p} WHERE symbol = {p}",
                    (new_shr, new_fill_price, sym),
                )
            logger.warning(
                "[main] fill_reconciliation: SPLIT detected %s — "
                "db_shares=%d ib_shares=%d ratio=%.4f — "
                "updated shares=%d fill_price=%.4f notional=%.2f",
                sym, sp["db_shares"], new_shr, sp["ratio"],
                new_shr, new_fill_price, notional,
            )
            monitor.send_alert(
                f"Split detected: {sym}",
                (
                    f"Symbol: {sym}\n"
                    f"DB shares (pre-split): {sp['db_shares']}\n"
                    f"IB shares (post-split): {new_shr}\n"
                    f"Ratio: {sp['ratio']:.4f}\n"
                    f"Updated fill_price: {new_fill_price:.4f}\n"
                    f"Notional preserved: {notional:.2f}"
                ),
                level="warning",
            )
            split_symbols.add(sym)
        if splits:
            # Reload positions so downstream steps see corrected share counts
            open_positions = portfolio_state.load_positions()
    except Exception as exc:
        logger.warning("[main] fill_reconciliation: split detection failed: %s", exc)

    # ── Process fills ─────────────────────────────────────────────────────────
    for oid, fill in filled.items():
        sub = _submitted.get(oid, {})
        if not sub:
            logger.warning("[main] fill_reconciliation: unknown order id %d", oid)
            continue

        sym        = sub["symbol"]
        action     = sub["action"]
        fill_price = float(fill.get("fill_price", sub.get("fill_price", 0)))
        fill_qty   = int(fill.get("fill_qty",   sub.get("shares", 0)))

        if action == "BUY":
            notional = fill_qty * fill_price
            commission = notional * (config.ROUND_TRIP_COST_BPS / 10_000)
            pos = {
                "pos_id":           f"{sym}_{today.isoformat()}",
                "symbol":           sym,
                "direction":        "long",
                "entry_date":       str(today),
                "fill_price":       fill_price,
                "shares":           fill_qty,
                "notional":         notional,
                "bars_held":        0,
                "equity_at_entry":  current_equity,
                "actual_risk_frac": notional / current_equity if current_equity > 0 else 0,
                "consec_lows":      0,
                "ib_order_id":      oid,
            }
            portfolio_state.save_position(pos)

            # Persist entry metadata not covered by _POS_COLUMNS in portfolio_state
            _entry_sigs_map = {s["symbol"]: s for s in _snap_state.get("entry_signals", [])}
            _sig = _entry_sigs_map.get(sym, {})
            _p = db.ph()
            with db.connect() as conn:
                conn.execute(
                    f"UPDATE positions "
                    f"SET order_type={_p}, limit_price={_p}, "
                    f"qpi_at_entry={_p}, ibs_at_entry={_p} "
                    f"WHERE pos_id={_p}",
                    (
                        sub.get("order_type", config.ENTRY_ORDER_TYPE),
                        sub.get("limit_price"),
                        _sig.get("n_day_ret"),
                        _sig.get("ibs_entry"),
                        pos["pos_id"],
                    ),
                )

            logger.info("[main] fill_reconciliation: saved entry %s qty=%d @ %.4f",
                        sym, fill_qty, fill_price)

        elif action == "SELL":
            pos_id = sub.get("pos_id", "")
            if not pos_id:
                # Try to find by symbol in open positions
                match = next((p for p in open_positions if p["symbol"] == sym), None)
                pos_id = match["pos_id"] if match else ""

            exit_notional = fill_qty * fill_price
            # Compute gross pnl from open position fill price
            pos_row = next((p for p in open_positions if p["pos_id"] == pos_id), {})
            entry_price = float(pos_row.get("fill_price", fill_price))
            gross_pnl   = fill_qty * (fill_price - entry_price)
            commission  = float(pos_row.get("notional", exit_notional)) * (config.ROUND_TRIP_COST_BPS / 10_000)
            net_pnl     = gross_pnl - commission

            portfolio_state.close_position(pos_id, {
                "exit_price":  fill_price,
                "exit_date":   str(today),
                "exit_reason": sub.get("reason", "exit"),
                "pnl":         net_pnl,
                "commission":  commission,
            })
            logger.info("[main] fill_reconciliation: closed %s (%s) qty=%d @ %.4f pnl=%.2f",
                        sym, sub.get("reason"), fill_qty, fill_price, net_pnl)

    # ── Equity snapshot ───────────────────────────────────────────────────────
    positions_now = portfolio_state.load_positions()
    open_equity   = portfolio_state.get_open_equity(positions_now, snap_prices)
    deployed_pct  = open_equity / current_equity if current_equity > 0 else 0.0

    # BOD equity from yesterday's eod (if available)
    bod = current_equity
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT equity_eod FROM equity_log ORDER BY date DESC LIMIT 1"
            ).fetchone()
        if row and row[0]:
            bod = float(row[0])
    except Exception:
        pass

    portfolio_state.append_equity_snapshot(
        today, bod, current_equity, len(positions_now), deployed_pct,
    )

    # ── IB reconciliation (exclude already-handled splits) ───────────────────
    non_split_positions = [p for p in positions_now if p["symbol"] not in split_symbols]
    mismatch, detail = _reconcile_with_ib(non_split_positions, ib_pos)
    if mismatch:
        if config.RISK_RECONCILE_HALT:
            alert_body = (
                f"A TRADING HALT has been set. Trading is suspended until manually "
                f"cleared via risk_engine.clear_halt().\n\nMismatch detail:\n{detail}"
            )
        else:
            alert_body = (
                f"RISK_RECONCILE_HALT is False — no trading halt set. "
                f"Manual review recommended.\n\nMismatch detail:\n{detail}"
            )
        monitor.send_alert(
            subject="🚨 TRADING HALT — Position Reconciliation Mismatch",
            body=alert_body,
            level="critical",
        )
        risk_engine.evaluate("reconcile_mismatch", {"mismatch": True, "detail": detail})
        logger.warning("[main] fill_reconciliation: IB mismatch — %s", detail)

    # ── JSON export ───────────────────────────────────────────────────────────
    if config.EXPORT_STATE_JSON:
        portfolio_state.export_positions_json()

    logger.info("[main] fill_reconciliation: complete — %d open positions, deployed=%.1f%%",
                len(positions_now), deployed_pct * 100)


# ═══════════════════════════════════════════════════════════════════════════════
# Job: daily_report
# ═══════════════════════════════════════════════════════════════════════════════

def daily_report() -> None:
    """
    16:15 ET — Build and send daily (and optionally weekly) report.
    """
    logger.info("[main] daily_report: building report")
    today = date.today()

    # ── Gather data from DB ───────────────────────────────────────────────────
    equity_bod = 0.0
    equity_eod = 0.0
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT equity_bod, equity_eod FROM equity_log WHERE date = ?", (str(today),)
            ).fetchone()
        if row:
            equity_bod = float(row[0] or 0)
            equity_eod = float(row[1] or 0)
    except Exception as exc:
        logger.warning("[main] daily_report: equity_log read failed: %s", exc)

    exits_today:   list[dict] = []
    entries_today: list[dict] = []
    try:
        with db.connect() as conn:
            exits_today = [
                dict(r) for r in conn.execute(
                    "SELECT symbol, exit_reason, pnl, bars_held FROM trade_log WHERE exit_date = ?",
                    (str(today),),
                ).fetchall()
            ]
            entries_today = [
                {
                    "symbol":      r["symbol"],
                    "fill_price":  r["fill_price"],
                    "shares":      r["shares"],
                    "order_type":  r["order_type"] if r["order_type"] is not None
                                   else config.ENTRY_ORDER_TYPE,
                    "limit_price": r["limit_price"],   # None for MOC — handled by build_daily_report
                    "qpi":         r["qpi_at_entry"]  if r["qpi_at_entry"]  is not None else 0.0,
                    "ibs":         r["ibs_at_entry"]  if r["ibs_at_entry"]  is not None else 0.0,
                }
                for r in conn.execute(
                    "SELECT symbol, fill_price, shares, "
                    "order_type, limit_price, qpi_at_entry, ibs_at_entry "
                    "FROM positions WHERE entry_date = ?",
                    (str(today),),
                ).fetchall()
            ]
    except Exception as exc:
        logger.warning("[main] daily_report: trade_log read failed: %s", exc)

    open_positions = portfolio_state.load_positions()
    account        = _snap_state.get("account", {})
    current_equity = float(account.get("net_liquidation", equity_eod))
    snap_prices    = _snap_state.get("snap_prices", {})
    deployed_pct   = 0.0
    if current_equity > 0:
        deployed_pct = portfolio_state.get_open_equity(open_positions, snap_prices) / current_equity

    # YTD P&L from equity_log
    ytd_pnl = 0.0
    ytd_pct  = 0.0
    try:
        with db.connect() as conn:
            first = conn.execute(
                "SELECT equity_eod FROM equity_log ORDER BY date ASC LIMIT 1"
            ).fetchone()
        if first and first[0]:
            start_eq = float(first[0])
            ytd_pnl  = equity_eod - start_eq
            ytd_pct  = ytd_pnl / start_eq if start_eq > 0 else 0.0
    except Exception:
        pass

    report_data = {
        "date":         today,
        "equity_bod":   equity_bod,
        "equity_eod":   equity_eod,
        "exits":        exits_today,
        "entries":      entries_today,
        "n_open":       len(open_positions),
        "deployed_pct": deployed_pct,
        "ytd_pnl":      ytd_pnl,
        "ytd_pnl_pct":  ytd_pct,
    }

    if config.REPORT_DAILY:
        report_text = monitor.build_daily_report(report_data)
        monitor.send_report(report_text, is_weekly=False)
        logger.info("[main] daily_report: sent daily report")

    if config.REPORT_WEEKLY and today.strftime("%A").lower() == config.REPORT_WEEKLY_DAY.lower():
        monitor.send_report(monitor.build_daily_report(report_data), is_weekly=True)
        logger.info("[main] daily_report: sent weekly report")


# ═══════════════════════════════════════════════════════════════════════════════
# Job: sunday_universe_update
# ═══════════════════════════════════════════════════════════════════════════════

def sunday_universe_update() -> None:
    """
    Sunday config.SP500_UPDATE_TIME ET — refresh the S&P 500 universe file and
    fetch full history for any newly added symbols.

    Flow:
      1. universe.update_universe() — fetch current S&P 500 list, diff against
         universe.csv, rewrite file.
      2. For each newly added symbol: fetch TWELVEDATA_HISTORY_DAYS of daily bars
         from TwelveData and upsert into daily_bars.
      3. send_alert() summarising changes.
    """
    logger.info("[main] sunday_universe_update: starting")
    monitor.setup_logging()

    try:
        result = universe.update_universe()
    except Exception as exc:
        logger.error("[main] sunday_universe_update: update_universe failed: %s", exc)
        monitor.send_alert(
            subject="[Murphy] Sunday universe update FAILED",
            body=str(exc),
            level="warning",
        )
        return

    added   = result["added"]
    removed = result["removed"]
    total   = result["total"]

    history_upserted = 0

    if added:
        logger.info("[main] sunday_universe_update: fetching full history for %d new symbol(s)", len(added))
        for sym in added:
            rows = td_data.fetch_bars(sym, config.TWELVEDATA_HISTORY_DAYS)
            if rows:
                history_upserted += db.upsert_daily_bars(rows)
        logger.info("[main] sunday_universe_update: upserted %d bar rows for new symbols",
                    history_upserted)

    body_lines = [
        f"S&P 500 universe updated — {total} total symbols.",
        f"Added   ({len(added)}): {', '.join(added) or 'none'}",
        f"Removed ({len(removed)}): {', '.join(removed) or 'none'}",
    ]
    if added:
        body_lines.append(f"Full history fetched: {history_upserted} bar rows upserted.")

    monitor.send_alert(
        subject="[Murphy] Sunday universe update complete",
        body="\n".join(body_lines),
        level="info",
    )

    # Record completion time so startup_catchup() can detect stale universe.
    try:
        p = db.ph()
        with db.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS system_state "
                "(key TEXT PRIMARY KEY, value TEXT)"
            )
            conn.execute(
                f"INSERT OR REPLACE INTO system_state (key, value) VALUES ({p}, {p})",
                ("last_universe_update", datetime.now(config.TZ).isoformat()),
            )
    except Exception as exc:
        logger.warning("[main] sunday_universe_update: could not write last_universe_update: %s", exc)

    logger.info("[main] sunday_universe_update: complete")


# ═══════════════════════════════════════════════════════════════════════════════
# Job: sunday_reauth
# ═══════════════════════════════════════════════════════════════════════════════

def sunday_reauth() -> None:
    """
    Sunday IBC_2FA_TIME ET — Stop/start IB Gateway, handle 2FA via IBC.

    Flow:
      1. IBCController.stop_gateway()
      2. Wait up to IBC_RESTART_TIMEOUT for API port to close
      3. IBCController.start_gateway()
      4. IBCController.wait_for_api(IBC_RESTART_TIMEOUT)
      5. IBBridge.reconnect()
      6. send_alert on success or failure
    """
    logger.info("[main] sunday_reauth: starting IBC restart sequence")
    ctrl = IBCController()

    ctrl.stop_gateway()
    logger.info("[main] sunday_reauth: stop_gateway sent; waiting %ds", config.IBC_RESTART_TIMEOUT)
    time.sleep(min(30, config.IBC_RESTART_TIMEOUT))   # brief grace period for shutdown

    ctrl.start_gateway()
    logger.info("[main] sunday_reauth: start_gateway sent; polling API port")

    api_up = ctrl.wait_for_api(timeout=config.IBC_RESTART_TIMEOUT)

    if api_up:
        try:
            bridge.reconnect()
            monitor.send_alert(
                subject="[Murphy] Sunday reauth succeeded",
                body="IBC restart completed. API is live. IBBridge reconnected.",
                level="info",
            )
            logger.info("[main] sunday_reauth: complete — bridge reconnected")
        except Exception as exc:
            monitor.send_alert(
                subject="[Murphy] Sunday reauth: bridge reconnect failed",
                body=str(exc),
                level="warning",
            )
            logger.error("[main] sunday_reauth: bridge reconnect failed: %s", exc)
    else:
        monitor.send_alert(
            subject="[Murphy] Sunday reauth FAILED — API port did not open",
            body=f"Waited {config.IBC_RESTART_TIMEOUT}s for {config.IB_HOST}:{config.IB_PORT}",
            level="critical",
        )
        logger.critical("[main] sunday_reauth: API port never opened")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point (direct invocation — not normally used; scheduler is the runner)
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    monitor.setup_logging()
    logger.info("[main] Direct invocation — run scheduler.py for production use")
