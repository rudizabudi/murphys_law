"""
order_manager.py — Order construction.

Direct port of the sizing and gate logic from simulate_portfolio() in
reference/v300.py.  All thresholds read from config — no hardcoded values.

Public API
──────────
  build_exit_orders(exit_signals, positions)                    → list[Order]
  build_entry_orders(entry_signals, positions, current_equity,
                     snap_prices, imbalance_data=None)          → list[Order]

Order dataclass is imported from ib_exec — defined once, used everywhere.
"""

import logging

import config
import risk_engine
from ib_exec import Order

logger = logging.getLogger("murphy")


# ═══════════════════════════════════════════════════════════════════════════════
# Exit order construction
# ═══════════════════════════════════════════════════════════════════════════════

def build_exit_orders(
    exit_signals: list[dict],
    positions: list[dict],
) -> list[Order]:
    """
    Build one plain MOC SELL order for every flagged exit.

    Exit type is always MOC regardless of config.EXIT_ORDER_TYPE — the ROADMAP
    notes that "non-execution risk is too high" for limit exit orders.

    Quantity comes from position state (shares field) — no recalculation.
    pos_id is carried through for downstream reconciliation.

    Parameters
    ----------
    exit_signals : list[dict]
        Output of signals.get_exit_signals().  Each dict is a copy of the
        position dict augmented with an 'exit_reason' field.
    positions : list[dict]
        Current open positions (used to resolve shares when the exit signal
        dict does not already carry them; in practice exit_signals already
        contain all position fields, but this keeps the function robust).
    """
    pos_by_id = {p["pos_id"]: p for p in positions}
    orders: list[Order] = []

    for sig in exit_signals:
        pos_id = sig.get("pos_id", "")
        # Prefer shares from the signal (which is a copy of the position dict);
        # fall back to the live positions table.
        shares = int(sig.get("shares") or pos_by_id.get(pos_id, {}).get("shares", 0))

        if shares < 1:
            logger.warning("[om] build_exit_orders: zero shares for %s, skipping", sig.get("symbol"))
            continue

        orders.append(Order(
            symbol      = sig["symbol"],
            action      = "SELL",
            order_type  = "MOC",
            quantity    = shares,
            limit_price = None,
            reason      = sig.get("exit_reason", "exit"),
            pos_id      = pos_id,
        ))
        logger.debug("[om] exit order: SELL %d %s MOC (%s)",
                     shares, sig["symbol"], sig.get("exit_reason"))

    return orders


# ═══════════════════════════════════════════════════════════════════════════════
# Entry order construction
# ═══════════════════════════════════════════════════════════════════════════════

def build_entry_orders(
    entry_signals: list[dict],
    positions: list[dict],
    current_equity: float,
    snap_prices: dict[str, float],
    imbalance_data: dict[str, float] | None = None,
    exit_orders: list[Order] | None = None,
) -> list[Order]:
    """
    Direct port of the entry sizing and gate logic from simulate_portfolio().

    Steps (identical to backtest):
      1. Remove signals for symbols already held
      2. Sort by config.RANK_BY (deepest dislocation first)
      3. Per candidate (up to free slots):
         a. target_shares  = int((equity × MAX_TOTAL_NOTIONAL / MAX_POSITIONS) / snap_price)
         b. max_cap_shares = int((equity × MAX_NOTIONAL) / snap_price)
         c. shares = min(target, cap); skip if < 1
         d. notional = shares × snap_price
         e. Liquidity gate: notional ≤ adv63 × LIQUIDITY_ADV_MAX_PCT
         f. Total notional gate: (deployed_mtm + notional) / equity ≤ MAX_TOTAL_NOTIONAL
         g. Optional imbalance check via risk_engine
         h. Build LOC or MOC per config.ENTRY_ORDER_TYPE

    Parameters
    ----------
    entry_signals : list[dict]
        Output of signals.get_entry_signals().
        Each dict: symbol, fill_price, n_day_ret, ibs_entry, adv63, …
    positions : list[dict]
        Current open positions (post-exit, pre-entry).
    current_equity : float
        Sizing equity — IB net_liquidation or computed equivalent.
    snap_prices : dict[str, float]
        15:40 snapshot prices keyed by symbol.  Used for MTM of open positions
        and as the fill_price proxy for sizing.
    imbalance_data : dict[str, float] | None
        Optional per-symbol imbalance ratios.  Required when
        config.RISK_IMBALANCE_ENABLED is True; if None, the imbalance check
        is skipped even when enabled.
    exit_orders : list[Order] | None
        Exit orders built in the same session.  Their notional is subtracted
        from the deployed total before the gate — these positions close at the
        same MOC, so their capital is effectively freed for new entries.
    """
    if current_equity <= 0:
        logger.warning("[om] build_entry_orders: current_equity <= 0, no orders built")
        return []

    # ── 1. Filter out already-held symbols ───────────────────────────────────
    open_syms   = {p["symbol"] for p in positions}
    candidates  = [s for s in entry_signals if s["symbol"] not in open_syms]
    slots_free  = config.MAX_POSITIONS - len(positions)

    if not candidates:
        logger.debug("[om] build_entry_orders: no candidates after held-symbol filter")
        return []

    if slots_free <= 0:
        logger.debug("[om] build_entry_orders: no free slots (%d positions open)", len(positions))
        return []

    # ── 2. Sort: deepest dislocation first ───────────────────────────────────
    if config.RANK_BY == "ibs":
        candidates.sort(key=lambda s: s["ibs_entry"])
    else:
        candidates.sort(key=lambda s: s["n_day_ret"])

    # ── 3. Build orders ───────────────────────────────────────────────────────
    # Track the simulated open-position list so the total-notional gate is
    # accurate across multiple new entries built in the same call.
    sim_positions  = list(positions)   # shallow copy; we only append, never mutate
    orders: list[Order] = []

    # Pending-exit credit: exits fire at the same MOC, so those positions'
    # capital is freed.  Subtract from deployed before the total-notional gate.
    exit_credit = (
        sum(o.quantity * snap_prices.get(o.symbol, 0.0) for o in exit_orders)
        if exit_orders else 0.0
    )

    for sig in candidates:
        if len(sim_positions) >= config.MAX_POSITIONS:
            logger.debug("[om] max positions reached, stopping entry order build")
            break

        sym        = sig["symbol"]
        snap_price = snap_prices.get(sym, sig["fill_price"])

        if snap_price <= 0:
            logger.debug("[om] %s: snap_price <= 0, skipping", sym)
            continue

        adv63 = sig.get("adv63", 0.0) or 0.0

        # ── a+b. Sizing (direct port of backtest formula) ─────────────────────
        target_shares  = int((current_equity * config.MAX_TOTAL_NOTIONAL / config.MAX_POSITIONS) / snap_price)
        max_cap_shares = int((current_equity * config.MAX_NOTIONAL) / snap_price)
        shares         = min(target_shares, max_cap_shares)

        if shares < 1:
            logger.debug("[om] %s: shares < 1 (target=%d cap=%d), skipping", sym, target_shares, max_cap_shares)
            continue

        notional = shares * snap_price

        # ── e. Liquidity gate ────────────────────────────────────────────────
        if config.LIQUIDITY_ADV_MAX_PCT > 0 and adv63 > 0:
            if notional > adv63 * config.LIQUIDITY_ADV_MAX_PCT:
                logger.debug("[om] %s: liquidity gate (notional=%.0f adv63=%.0f)", sym, notional, adv63)
                continue

        # ── f. Total notional gate (mark-to-market) ──────────────────────────
        deployed = sum(
            int(p["shares"]) * float(snap_prices.get(p["symbol"], p["fill_price"]))
            for p in sim_positions
        )
        effective_deployed = max(0.0, deployed - exit_credit)
        if (effective_deployed + notional) / current_equity > config.MAX_TOTAL_NOTIONAL:
            logger.debug("[om] %s: total notional gate (deployed=%.0f credit=%.0f new=%.0f equity=%.0f)",
                         sym, deployed, exit_credit, notional, current_equity)
            continue

        # ── g. Optional imbalance filter ─────────────────────────────────────
        if config.RISK_IMBALANCE_ENABLED and imbalance_data is not None:
            ratio = imbalance_data.get(sym, 0.0)
            ok = risk_engine.evaluate("imbalance", {"imbalance_ratio": ratio, "symbol": sym})
            if not ok:
                logger.debug("[om] %s: imbalance filter blocked (ratio=%.2f)", sym, ratio)
                continue

        # ── h. Build LOC or MOC order ─────────────────────────────────────────
        if config.ENTRY_ORDER_TYPE == "LOC":
            limit_price = snap_price * (1.0 + config.ENTRY_LOC_BUFFER_PCT)
            order_type  = "LOC"
        else:
            limit_price = None
            order_type  = "MOC"

        orders.append(Order(
            symbol      = sym,
            action      = "BUY",
            order_type  = order_type,
            quantity    = shares,
            limit_price = limit_price,
            reason      = "entry",
            pos_id      = "",   # filled in after IB fill confirmation
        ))
        logger.debug("[om] entry order: BUY %d %s %s%s",
                     shares, sym, order_type,
                     f" lmt={limit_price:.4f}" if limit_price is not None else "")

        # Track this candidate as a simulated open position so subsequent
        # candidates use the correct deployed capital.
        sim_positions.append({
            "symbol":     sym,
            "shares":     shares,
            "fill_price": snap_price,
        })

    return orders
