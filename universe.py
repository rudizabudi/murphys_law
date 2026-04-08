"""
universe.py — S&P 500 constituent management.

Public API
──────────
  fetch_sp500_symbols()   → list[str]   fetch current S&P 500 tickers via HTTP
  update_universe()       → dict        diff + rewrite universe.csv
  get_new_symbols()       → list[str]   symbols in universe.csv lacking DB history
"""

import csv
import io
import logging
from pathlib import Path

import config
import db

logger = logging.getLogger("murphy")

# ─────────────────────────────────────────────────────────────────────────────
# Ticker normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_ticker(ticker: str) -> str:
    """
    Convert S&P 500 ticker to IB-compatible format.
    IB uses no dots: BRK.B → BRKB, BF.B → BFB, etc.
    """
    return ticker.strip().replace(".", "")


# ─────────────────────────────────────────────────────────────────────────────
# fetch_sp500_symbols
# ─────────────────────────────────────────────────────────────────────────────

def fetch_sp500_symbols() -> list[str]:
    """
    Fetch the current S&P 500 constituent list from config.SP500_CSV_URL.

    The CSV is expected to have a 'Symbol' column (standard for the
    datasets/s-and-p-500-companies GitHub dataset).

    Returns a deduplicated, sorted list of IB-normalised ticker strings.
    Raises RuntimeError if the fetch or parse fails.
    """
    import httpx

    logger.info("[universe] fetching S&P 500 list from %s", config.SP500_CSV_URL)
    try:
        resp = httpx.get(config.SP500_CSV_URL, timeout=15, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"fetch_sp500_symbols: HTTP request failed: {exc}") from exc

    try:
        text = resp.text
        reader = csv.DictReader(io.StringIO(text))
        symbols: list[str] = []
        for row in reader:
            raw = row.get("Symbol", "").strip()
            if raw:
                symbols.append(_normalise_ticker(raw))
    except Exception as exc:
        raise RuntimeError(f"fetch_sp500_symbols: CSV parse failed: {exc}") from exc

    symbols = sorted(set(symbols))
    logger.info("[universe] fetched %d S&P 500 symbols", len(symbols))
    return symbols


# ─────────────────────────────────────────────────────────────────────────────
# update_universe
# ─────────────────────────────────────────────────────────────────────────────

def _read_universe_csv() -> list[str]:
    """Read current universe.csv; return [] if absent or empty.

    Symbols present in config.SYMBOL_BLACKLIST are excluded on read so
    callers never operate on blacklisted tickers, even if they exist on disk.
    """
    path = Path(config.UNIVERSE_CSV)
    if not path.exists():
        return []
    blacklist = set(config.SYMBOL_BLACKLIST)
    symbols: list[str] = []
    excluded: list[str] = []
    with open(path, newline="") as fh:
        for row in csv.reader(fh):
            if row and row[0].strip() and not row[0].strip().startswith("#"):
                sym = row[0].strip()
                if sym in blacklist:
                    excluded.append(sym)
                else:
                    symbols.append(sym)
    if excluded:
        logger.info("[universe] _read_universe_csv: excluded blacklisted symbols: %s", excluded)
    return symbols


def _write_universe_csv(symbols: list[str]) -> None:
    """Overwrite universe.csv with *symbols* (one per line, sorted)."""
    path = Path(config.UNIVERSE_CSV)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        for sym in sorted(set(symbols)):
            writer.writerow([sym])


def update_universe() -> dict:
    """
    Fetch current S&P 500 list, merge with config.SYMBOL_WHITELIST, diff against
    the existing universe.csv, and rewrite the file.

    Returns {"added": [...], "removed": [...], "total": int}.

    'removed' contains symbols that were in the previous universe.csv but are no
    longer in the S&P 500 AND are not in config.SYMBOL_WHITELIST.  Whitelist
    symbols are always kept.
    """
    sp500 = fetch_sp500_symbols()

    whitelist = [_normalise_ticker(s) for s in config.SYMBOL_WHITELIST if s.strip()]
    blacklist = set(config.SYMBOL_BLACKLIST)
    new_universe = sorted((set(sp500) | set(whitelist)) - blacklist)
    excluded = sorted((set(sp500) | set(whitelist)) & blacklist)
    if excluded:
        logger.info("[universe] update_universe: excluded blacklisted symbols: %s", excluded)

    old_universe = _read_universe_csv()
    old_set      = set(old_universe)
    new_set      = set(new_universe)

    added   = sorted(new_set - old_set)
    removed = sorted(old_set - new_set)

    _write_universe_csv(new_universe)

    logger.info(
        "[universe] update_universe: total=%d added=%d removed=%d",
        len(new_universe), len(added), len(removed),
    )
    if added:
        logger.info("[universe] added: %s", added)
    if removed:
        logger.info("[universe] removed: %s", removed)

    return {"added": added, "removed": removed, "total": len(new_universe)}


# ─────────────────────────────────────────────────────────────────────────────
# get_new_symbols
# ─────────────────────────────────────────────────────────────────────────────

def get_new_symbols() -> list[str]:
    """
    Return symbols from universe.csv that have fewer than config.MIN_BARS_REQUIRED
    daily bars in the daily_bars table (i.e. need a full history build).

    Uses a single COUNT query per symbol to avoid loading full DataFrames.
    """
    symbols = _read_universe_csv()
    if not symbols:
        return []

    db.init_db()
    p        = db.ph()
    new_syms: list[str] = []

    for sym in symbols:
        try:
            with db.connect() as conn:
                row = conn.execute(
                    f"SELECT COUNT(*) FROM daily_bars WHERE symbol = {p}",
                    (sym,),
                ).fetchone()
            count = int(row[0]) if row else 0
        except Exception as exc:
            logger.warning("[universe] bar count query failed for %s: %s", sym, exc)
            count = 0

        if count < config.MIN_BARS_REQUIRED:
            new_syms.append(sym)

    logger.info(
        "[universe] get_new_symbols: %d of %d symbols need full history build",
        len(new_syms), len(symbols),
    )
    return new_syms
