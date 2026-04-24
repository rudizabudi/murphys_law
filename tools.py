"""
tools.py — Operator CLI for Murphy's Law.

Usage (from repo root):
    uv run python tools.py status        # halt/shutdown status, open positions, last DB date
    uv run python tools.py clear-halt    # clear halt/shutdown with confirmation prompt
    uv run python tools.py positions     # print all open positions in a table
    uv run python tools.py sync          # manually trigger nightly_sync()
    uv run python tools.py watchlist     # print watchlist symbol count and symbols
"""

import argparse
import sys
from datetime import date

import config
import db
import monitor
import portfolio_state
import risk_engine

_RULE = "─" * 50


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _flag(active: bool) -> str:
    return "ACTIVE ⚠" if active else "clear"


def _last_db_date() -> str:
    """Return the most recent date in daily_bars, or 'n/a'."""
    try:
        db.init_db()
        with db.connect() as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM daily_bars"
            ).fetchone()
        return str(row[0]) if row and row[0] else "n/a"
    except Exception as exc:
        monitor.logger.debug("[tools] _last_db_date failed: %s", exc)
        return "n/a"


def _last_equity_date() -> str:
    """Return the most recent date in equity_log, or 'n/a'."""
    try:
        db.init_db()
        with db.connect() as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM equity_log"
            ).fetchone()
        return str(row[0]) if row and row[0] else "n/a"
    except Exception as exc:
        monitor.logger.debug("[tools] _last_equity_date failed: %s", exc)
        return "n/a"


# ═══════════════════════════════════════════════════════════════════════════════
# Commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_status() -> None:
    """Print system status: halt/shutdown flags, open position count, last DB date."""
    halted   = risk_engine.is_halted()
    shutdown = risk_engine.is_shutdown()
    # is_halted() is True for halt OR shutdown; isolate the pure halt flag
    halt_only = halted and not shutdown

    positions   = portfolio_state.load_positions()
    last_bar    = _last_db_date()
    last_equity = _last_equity_date()

    print()
    print(f"Murphy's Law — Status  {date.today()}")
    print(_RULE)
    print(f"Halt:         {_flag(halt_only)}")
    print(f"Shutdown:     {_flag(shutdown)}")
    print()
    print(f"Open positions:   {len(positions)} / {config.MAX_POSITIONS}")
    print(f"Last bar date:    {last_bar}")
    print(f"Last equity date: {last_equity}")
    print(_RULE)
    print()


def cmd_clear_halt() -> None:
    """Show current halt/shutdown status, prompt for confirmation, then clear."""
    halted   = risk_engine.is_halted()
    shutdown = risk_engine.is_shutdown()
    halt_only = halted and not shutdown

    print()
    print("Current status:")
    print(f"  Halt:     {_flag(halt_only)}")
    print(f"  Shutdown: {_flag(shutdown)}")
    print()

    if not halted and not shutdown:
        print("No active halt or shutdown — nothing to clear.")
        print()
        return

    try:
        answer = input("Clear halt/shutdown flags? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        print("Aborted.")
        print()
        return

    if answer != "y":
        print("Aborted.")
        print()
        return

    risk_engine.clear_halt()
    print("Halt/shutdown flags cleared.")
    print()


def cmd_positions() -> None:
    from collections import defaultdict
    """Print all open positions as a formatted table."""
    positions = portfolio_state.load_positions()

    print(f"Open positions: {len(positions)} / {config.MAX_POSITIONS}")

    if not positions:
        print("  (none)")
        print()
        return
    
        
    total = defaultdict(int)
    last_update = _last_equity_date()

    print(_RULE)

    # Header
    print(
        f"{'ENTRY':>14}"
        f"{'CURRENT':>49}"
    )
    print(
        f"  {'SYMBOL':<7}"
        f"{'DATE':<8}"
        f"{'SHARES':>12}"
        f"{'PRICE':>9}"
        f"{'NOTIONAL':>10}"
        f"{'PRICE':>9}"
        f"{'NOTIONAL':>11}"
        f"{'rel':>10}"
        f"{'abs':>9}"
        f"{'Days':>7}"
        f"{'TYPE':>5}"
        f"{'QPI':>9}"
        f"{'IBS':>4}"
    )
    print("  " + "─" * 85)
    for pos in positions:
        qpi = pos.get("qpi_at_entry")
        ibs = pos.get("ibs_at_entry")

        if (symbol := pos.get('symbol', None)):
            try:
                db.init_db()
                with db.connect() as conn:
                    row = conn.execute(
                        "SELECT close " \
                        "FROM daily_bars " \
                        "WHERE symbol = ? " \
                        "AND date = (SELECT MAX(date) " \
                        "            FROM daily_bars " \
                        "            WHERE symbol = ?)",
                        (symbol, symbol)
                    ).fetchone()
                pos["current_price"] = float(row[0]) if row and row[0] else 0

            except Exception as exc:
                monitor.logger.debug("[tools] failed to request price: %s", exc)

        total["entry_notional"] += pos.get('notional', 0)


        print(
            f"  {pos.get('symbol', ''):<7}"
            f"{str(pos.get('entry_date', '')):<8}"
            f"{'|':^5}"
            f"{int(pos.get('shares', 0)):>5,}"
            f"{float(pos.get('fill_price', 0)):>9.2f}"
            f"{float(pos.get('notional', 0)):>10,.0f}"
            f"{'|':^3}"
            f"{pos["current_price"]:>6,.2f}"
            f"{pos["current_price"] * int(pos.get('shares', 0)):>11,.0f}"
            f"{'|':^3}"
            f"{(pos['current_price'] * int(pos.get('shares', 0)) / float(pos.get('notional', 0))) - 1:>+7,.2%}"

            f"{pos['current_price'] * int(pos.get('shares', 0)) - float(pos.get('notional', 0)):>+9,.0f}"
            f"{'|':>3}"
            f"{int(pos.get('bars_held', 0)):>3}"
            f"  {str(pos.get('order_type') or ''):>4}"
            f"{'|':^5}"
            f"  {(f'{qpi:.2f}' if qpi is not None else '—'):>5}"
            f"  {(f'{ibs:.2f}' if ibs is not None else '—'):>0}"
        )
    
    print("  " + "─" * 85)
    print(
        f"  {'TOTAL':<7}"
        f"{total.get("entry_notional", 0):>44,.0f}"
    )

    print()


def cmd_sync() -> None:
    """Manually trigger nightly_sync()."""
    # Import lazily so the IBBridge module-level object is created here, not
    # at import time of tools.py (avoids any side-effects when running other
    # commands that don't need IB at all).
    import main as _main

    print()
    print("Running nightly_sync()…")
    print(_RULE)
    try:
        _main.nightly_sync()
        print(_RULE)
        print("nightly_sync() complete.")
    except Exception as exc:
        print(_RULE)
        print(f"nightly_sync() raised: {exc}")
        sys.exit(1)
    print()


def cmd_watchlist() -> None:
    """Print the current watchlist: symbol count and sorted symbol list."""
    try:
        db.init_db()
        with db.connect() as conn:
            rows = conn.execute(
                "SELECT symbol FROM watchlist ORDER BY symbol"
            ).fetchall()
        symbols = [row[0] for row in rows]
    except Exception as exc:
        monitor.logger.debug("[tools] watchlist query failed: %s", exc)
        symbols = []

    print()
    print(f"Watchlist: {len(symbols)} symbol(s)")

    if symbols:
        print(_RULE)
        # Print in rows of 10
        for i in range(0, len(symbols), 10):
            print("  " + "  ".join(f"{s:<6}" for s in symbols[i : i + 10]))
    else:
        print("  (empty — run nightly_sync to populate)")

    print()


# ═══════════════════════════════════════════════════════════════════════════════
# CLI entry-point
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    monitor.setup_logging()

    parser = argparse.ArgumentParser(
        prog="tools.py",
        description="Murphy's Law operator tools",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    sub.add_parser("status",     help="print halt/shutdown status, open positions, last DB date")
    sub.add_parser("clear-halt", help="clear halt/shutdown flags (with confirmation)")
    sub.add_parser("positions",  help="print all open positions in a table")
    sub.add_parser("sync",       help="manually trigger nightly_sync()")
    sub.add_parser("watchlist",  help="print watchlist symbol count and symbols")

    args = parser.parse_args()

    _COMMANDS = {
        "status":     cmd_status,
        "clear-halt": cmd_clear_halt,
        "positions":  cmd_positions,
        "sync":       cmd_sync,
        "watchlist":  cmd_watchlist,
    }
    _COMMANDS[args.command]()


if __name__ == "__main__":
    main()
