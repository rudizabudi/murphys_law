"""
monitor.py — Logging, alerting, and daily/weekly reporting.

Public API
──────────
  setup_logging()              configure the 'murphy' logger (call once at startup)
  send_alert(subject, body, level)  dispatch to all configured channels
  send_report(report_text, is_weekly)  thin wrapper over send_alert()
  build_daily_report(data)     structured dict → formatted plain-text report string

The module-level `logger` instance (logging.getLogger("murphy")) is shared across
all modules. Other modules may either `from monitor import logger` or call
`logging.getLogger("murphy")` directly — they resolve to the same object.
No print() statements anywhere in the codebase.
"""

import logging
import logging.handlers
import smtplib
import ssl
from datetime import date
from email.mime.text import MIMEText
from pathlib import Path

import config

# ── Shared logger instance ────────────────────────────────────────────────────
logger = logging.getLogger("murphy")

# ── Report formatting constants ───────────────────────────────────────────────
_RULE   = "─" * 40
_DISCORD_MAX   = 2000
_CODE_OVERHEAD = 8   # len("```\n") + len("\n```")


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging() -> None:
    """
    Configure the 'murphy' logger.  Safe to call once at startup.
    Guards against duplicate handlers if called a second time.
    """
    if logger.handlers:
        return

    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Always: stdout
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # Conditional: rotating file
    if config.LOG_TO_FILE:
        log_dir = Path(config._BASE) / "logs"
        log_dir.mkdir(exist_ok=True)
        fh = logging.handlers.TimedRotatingFileHandler(
            filename=str(log_dir / f"murphy_{date.today():%Y%m%d}.log"),
            when="midnight",
            backupCount=90,
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)


# ═══════════════════════════════════════════════════════════════════════════════
# ALERT DISPATCHER
# ═══════════════════════════════════════════════════════════════════════════════

def send_alert(subject: str, body: str, level: str = "info") -> None:
    """
    Dispatch an alert to all configured channels.

    level : "info" | "warning" | "critical"
    Channels are additive — all configured channels receive every alert.
    Failures in one channel do not suppress others.
    """
    if config.ALERT_EMAIL:
        _send_email(subject, body, level)
    if config.DISCORD_WEBHOOK_URL:
        _send_discord(subject, body, level)


def _send_email(subject: str, body: str, level: str) -> None:
    """
    Send a plain-text email via stdlib smtplib (no extra dependencies).
    Subject is prefixed with [WARNING] or [CRITICAL] on elevated levels.
    """
    if level == "critical":
        subject = f"[CRITICAL] {subject}"
    elif level == "warning":
        subject = f"[WARNING] {subject}"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = config.SMTP_USER
    msg["To"]      = config.ALERT_EMAIL

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls(context=ctx)
            smtp.login(config.SMTP_USER, config.SMTP_PASSWORD)
            smtp.sendmail(config.SMTP_USER, config.ALERT_EMAIL, msg.as_string())
    except Exception as exc:
        logger.error("Email alert failed: %s", exc)


def _send_discord(subject: str, body: str, level: str) -> None:
    """
    POST to Discord webhook via httpx.

    - Body wrapped in a triple-backtick code block for monospace rendering.
    - DISCORD_ALERT_MENTIONS prepended on 'critical' level only, first post only.
    - Messages > 2000 chars split into sequential posts at line boundaries.
    """
    import httpx  # lazy — only needed when Discord is configured

    mention_line = ""
    if level == "critical" and config.DISCORD_ALERT_MENTIONS:
        mention_line = f"{config.DISCORD_ALERT_MENTIONS}\n"

    header = f"**{subject}**\n"

    # Capacity available for raw body text per post
    first_cap = _DISCORD_MAX - len(mention_line) - len(header) - _CODE_OVERHEAD
    rest_cap  = _DISCORD_MAX - _CODE_OVERHEAD

    chunks = _split_lines(body, first_cap, rest_cap)

    for i, chunk in enumerate(chunks):
        if i == 0:
            content = f"{mention_line}{header}```\n{chunk}\n```"
        else:
            content = f"```\n{chunk}\n```"

        try:
            resp = httpx.post(
                config.DISCORD_WEBHOOK_URL,
                json={"content": content},
                timeout=10.0,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.error("Discord alert failed (chunk %d): %s", i, exc)


def _split_lines(text: str, first_cap: int, rest_cap: int) -> list[str]:
    """
    Split *text* into a list of strings where each fits within its capacity.
    Splits only at line boundaries — never mid-line.
    The first chunk uses first_cap; all subsequent chunks use rest_cap.
    """
    lines   = text.splitlines()
    chunks: list[str]      = []
    current: list[str]     = []
    current_len = 0
    cap = max(first_cap, 1)

    for line in lines:
        line_len = len(line) + 1   # +1 accounts for the joining newline
        if current_len + line_len > cap and current:
            chunks.append("\n".join(current))
            current     = [line]
            current_len = line_len
            cap         = max(rest_cap, 1)
        else:
            current.append(line)
            current_len += line_len

    if current:
        chunks.append("\n".join(current))

    return chunks or [""]


# ═══════════════════════════════════════════════════════════════════════════════
# REPORTING
# ═══════════════════════════════════════════════════════════════════════════════

def send_report(report_text: str, is_weekly: bool = False) -> None:
    """Deliver a formatted daily or weekly report via all configured channels."""
    subject = (
        "Murphy's Law \u2014 Weekly Report"
        if is_weekly
        else "Murphy's Law \u2014 Daily Report"
    )
    send_alert(subject, report_text, level="info")


def build_daily_report(data: dict) -> str:
    """
    Render structured data into the plain-text daily report format.

    Expected keys in *data*
    ───────────────────────
    date          : datetime.date or str  (defaults to today)
    equity_bod    : float
    equity_eod    : float
    exits         : list of {symbol, exit_reason, pnl, bars_held}
    entries       : list of {symbol, order_type, shares, limit_price | None, qpi, ibs}
    n_open        : int   (current open position count)
    deployed_pct  : float (e.g. 132.4 → "132.4% of equity")
    ytd_pnl       : float (absolute YTD P&L in $)
    ytd_pnl_pct   : float (e.g. 12.3 → "12.3%")

    No DB calls inside this function — structured data in, string out.
    """
    report_date   = data.get("date", date.today())
    equity_bod    = float(data["equity_bod"])
    equity_eod    = float(data["equity_eod"])
    eq_change     = equity_eod - equity_bod
    eq_change_pct = (eq_change / equity_bod * 100.0) if equity_bod else 0.0

    exits        = data.get("exits",   [])
    entries      = data.get("entries", [])
    n_open       = data.get("n_open",  0)
    deployed_pct = float(data.get("deployed_pct", 0.0))
    ytd_pnl      = float(data.get("ytd_pnl",      0.0))
    ytd_pnl_pct  = float(data.get("ytd_pnl_pct",  0.0))

    lines = [
        f"Murphy's Law \u2014 Daily Report {report_date}",
        _RULE,
        f"Equity (BOD):       ${equity_bod:,.0f}",
        (
            f"Equity (EOD):       ${equity_eod:,.0f}"
            f"   {_usd(eq_change)}"
            f"  ({_pct(eq_change_pct, 2)})"
        ),
        "",
        f"Exits today:        {len(exits)}",
    ]

    for ex in exits:
        lines.append(
            f"  {ex['symbol']:<6}"
            f"  {ex['exit_reason']:<20}"
            f"  {_usd(ex['pnl'])}"
            f"   ({ex['bars_held']} bars)"
        )

    lines.append("")
    lines.append(f"Entries today:      {len(entries)}")

    for en in entries:
        lp = en.get("limit_price")
        price_str = f"@ limit ${lp:.2f}" if lp is not None else "(MOC)"
        lines.append(
            f"  {en['symbol']:<6}"
            f"  {en['order_type']} buy"
            f"  {en['shares']:>4} shares"
            f" {price_str}"
            f"  [QPI={en['qpi']:.2f}, IBS={en['ibs']:.2f}]"
        )

    lines += [
        "",
        f"Open positions:     {n_open} / {config.MAX_POSITIONS}",
        f"Total deployed:     {deployed_pct:.1f}% of equity",
        "",
        f"YTD P\u0026L:  {_usd(ytd_pnl)}  ({_pct(ytd_pnl_pct, 1)})",
        _RULE,
    ]

    return "\n".join(lines)


# ── Formatting helpers ────────────────────────────────────────────────────────

def _usd(v: float) -> str:
    """Format as +$1,234 or -$1,234 (no decimal places)."""
    return f"+${v:,.0f}" if v >= 0 else f"-${abs(v):,.0f}"


def _pct(v: float, decimals: int) -> str:
    """Format as +0.74% or -1.23% with the requested decimal places."""
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.{decimals}f}%"
