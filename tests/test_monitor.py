"""
tests/test_monitor.py — Tests for monitor.py.

Covers:
  - setup_logging(): file handler on/off, stdout always present, level respected
  - send_alert():    correct channels called based on config; both, one, or neither
  - _send_discord(): mention on critical only; long body splits; httpx.post called
  - _send_email():   smtplib flow (ehlo/starttls/login/sendmail); subject prefixes
  - send_report():   routes through send_alert with correct subject
  - build_daily_report(): all fields rendered; positive/negative formatting
"""

import logging
import logging.handlers
import smtplib
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import monitor
from monitor import (
    _pct,
    _split_lines,
    _usd,
    build_daily_report,
    build_weekly_report,
    send_alert,
    send_report,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def reset_logger():
    """
    Isolate each test: remove all handlers from the 'murphy' logger before
    and after the test, close file handles cleanly.
    """
    log = logging.getLogger("murphy")
    saved = log.handlers[:]
    for h in log.handlers[:]:
        h.close()
    log.handlers.clear()
    log.propagate = False      # prevent bleed into root logger during tests
    yield log
    for h in log.handlers[:]:
        h.close()
    log.handlers.clear()
    log.handlers.extend(saved)
    log.propagate = True


# ═══════════════════════════════════════════════════════════════════════════════
# setup_logging()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSetupLogging:

    def test_always_adds_stream_handler(self, monkeypatch):
        monkeypatch.setattr(config, "LOG_TO_FILE", False)
        monkeypatch.setattr(config, "LOG_LEVEL",   "INFO")
        monitor.setup_logging()
        log = logging.getLogger("murphy")
        stream_handlers = [h for h in log.handlers if isinstance(h, logging.StreamHandler)
                           and not isinstance(h, logging.handlers.TimedRotatingFileHandler)]
        assert len(stream_handlers) >= 1

    def test_file_handler_when_enabled(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "LOG_TO_FILE", True)
        monkeypatch.setattr(config, "LOG_LEVEL",   "DEBUG")
        monkeypatch.setattr(config, "_BASE", tmp_path)
        monitor.setup_logging()
        log = logging.getLogger("murphy")
        file_handlers = [h for h in log.handlers
                         if isinstance(h, logging.handlers.TimedRotatingFileHandler)]
        assert len(file_handlers) == 1

    def test_file_handler_absent_when_disabled(self, monkeypatch):
        monkeypatch.setattr(config, "LOG_TO_FILE", False)
        monkeypatch.setattr(config, "LOG_LEVEL",   "INFO")
        monitor.setup_logging()
        log = logging.getLogger("murphy")
        file_handlers = [h for h in log.handlers
                         if isinstance(h, logging.handlers.TimedRotatingFileHandler)]
        assert len(file_handlers) == 0

    def test_log_level_set_correctly(self, monkeypatch):
        monkeypatch.setattr(config, "LOG_TO_FILE", False)
        monkeypatch.setattr(config, "LOG_LEVEL",   "WARNING")
        monitor.setup_logging()
        assert logging.getLogger("murphy").level == logging.WARNING

    def test_idempotent_no_duplicate_handlers(self, monkeypatch):
        monkeypatch.setattr(config, "LOG_TO_FILE", False)
        monkeypatch.setattr(config, "LOG_LEVEL",   "INFO")
        monitor.setup_logging()
        monitor.setup_logging()   # second call must be a no-op
        log = logging.getLogger("murphy")
        stream_handlers = [h for h in log.handlers
                           if isinstance(h, logging.StreamHandler)
                           and not isinstance(h, logging.handlers.TimedRotatingFileHandler)]
        assert len(stream_handlers) == 1

    def test_file_written_to_correct_path(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "LOG_TO_FILE", True)
        monkeypatch.setattr(config, "LOG_LEVEL",   "INFO")
        monkeypatch.setattr(config, "_BASE", tmp_path)
        monitor.setup_logging()
        log = logging.getLogger("murphy")
        fh = next(h for h in log.handlers
                  if isinstance(h, logging.handlers.TimedRotatingFileHandler))
        expected_dir = str(tmp_path / "logs")
        assert fh.baseFilename.startswith(expected_dir)

    def test_logger_name_is_murphy(self):
        assert logging.getLogger("murphy") is monitor.logger


# ═══════════════════════════════════════════════════════════════════════════════
# send_alert() dispatch logic
# ═══════════════════════════════════════════════════════════════════════════════

class TestSendAlertDispatch:

    def _patch(self, monkeypatch, email="", discord=""):
        monkeypatch.setattr(config, "ALERT_EMAIL",          email)
        monkeypatch.setattr(config, "DISCORD_WEBHOOK_URL",  discord)

    @patch("monitor._send_email")
    @patch("monitor._send_discord")
    def test_both_channels_called_when_configured(self, mock_disc, mock_email, monkeypatch):
        self._patch(monkeypatch, email="a@b.com", discord="https://discord.test")
        send_alert("subj", "body", "info")
        mock_email.assert_called_once_with("subj", "body", "info")
        mock_disc.assert_called_once_with("subj", "body", "info")

    @patch("monitor._send_email")
    @patch("monitor._send_discord")
    def test_only_email_when_no_discord(self, mock_disc, mock_email, monkeypatch):
        self._patch(monkeypatch, email="a@b.com", discord="")
        send_alert("subj", "body", "warning")
        mock_email.assert_called_once()
        mock_disc.assert_not_called()

    @patch("monitor._send_email")
    @patch("monitor._send_discord")
    def test_only_discord_when_no_email(self, mock_disc, mock_email, monkeypatch):
        self._patch(monkeypatch, email="", discord="https://discord.test")
        send_alert("subj", "body", "critical")
        mock_email.assert_not_called()
        mock_disc.assert_called_once()

    @patch("monitor._send_email")
    @patch("monitor._send_discord")
    def test_no_channels_called_when_none_configured(self, mock_disc, mock_email, monkeypatch):
        self._patch(monkeypatch, email="", discord="")
        send_alert("subj", "body", "info")
        mock_email.assert_not_called()
        mock_disc.assert_not_called()

    @patch("monitor._send_email")
    @patch("monitor._send_discord")
    def test_level_forwarded_correctly(self, mock_disc, mock_email, monkeypatch):
        self._patch(monkeypatch, email="a@b.com", discord="https://discord.test")
        send_alert("s", "b", "critical")
        _, _, level_email   = mock_email.call_args.args
        _, _, level_discord = mock_disc.call_args.args
        assert level_email   == "critical"
        assert level_discord == "critical"


# ═══════════════════════════════════════════════════════════════════════════════
# _send_discord()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSendDiscord:

    @pytest.fixture
    def discord_cfg(self, monkeypatch):
        monkeypatch.setattr(config, "DISCORD_WEBHOOK_URL",   "https://discord.test/hook")
        monkeypatch.setattr(config, "DISCORD_ALERT_MENTIONS", "<@123>")

    @patch("httpx.post")
    def test_single_post_for_short_body(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        monitor._send_discord("Subject", "short body", "info")
        mock_post.assert_called_once()

    @patch("httpx.post")
    def test_mention_prepended_on_critical(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        monitor._send_discord("Subj", "body", "critical")
        content = mock_post.call_args.kwargs["json"]["content"]
        assert content.startswith("<@123>")

    @patch("httpx.post")
    def test_no_mention_on_info(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        monitor._send_discord("Subj", "body", "info")
        content = mock_post.call_args.kwargs["json"]["content"]
        assert "<@123>" not in content

    @patch("httpx.post")
    def test_no_mention_on_warning(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        monitor._send_discord("Subj", "body", "warning")
        content = mock_post.call_args.kwargs["json"]["content"]
        assert "<@123>" not in content

    @patch("httpx.post")
    def test_body_wrapped_in_code_block(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        monitor._send_discord("Subj", "some content", "info")
        content = mock_post.call_args.kwargs["json"]["content"]
        assert "```" in content
        assert "some content" in content

    @patch("httpx.post")
    def test_long_body_split_into_multiple_posts(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        # Build a body that is definitely > 2000 chars
        long_body = "\n".join(f"Line {i:04d}: " + "x" * 60 for i in range(40))
        monitor._send_discord("Subj", long_body, "info")
        assert mock_post.call_count >= 2

    @patch("httpx.post")
    def test_each_discord_post_under_2000_chars(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        long_body = "\n".join(f"Line {i:04d}: " + "x" * 60 for i in range(40))
        monitor._send_discord("Subj", long_body, "info")
        for c in mock_post.call_args_list:
            content = c.kwargs["json"]["content"]
            assert len(content) <= 2000, f"Discord post exceeds 2000 chars: {len(content)}"

    @patch("httpx.post")
    def test_mention_only_in_first_post(self, mock_post, discord_cfg):
        mock_post.return_value = MagicMock(status_code=204, raise_for_status=lambda: None)
        long_body = "\n".join(f"Line {i:04d}: " + "x" * 60 for i in range(40))
        monitor._send_discord("Subj", long_body, "critical")
        calls = mock_post.call_args_list
        assert calls[0].kwargs["json"]["content"].startswith("<@123>")
        for c in calls[1:]:
            assert "<@123>" not in c.kwargs["json"]["content"]

    @patch("httpx.post")
    def test_http_error_is_logged_not_raised(self, mock_post, discord_cfg):
        mock_post.side_effect = Exception("connection refused")
        # Must not propagate the exception
        monitor._send_discord("Subj", "body", "info")


# ═══════════════════════════════════════════════════════════════════════════════
# _split_lines()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSplitLines:

    def test_short_text_single_chunk(self):
        chunks = _split_lines("line1\nline2", 500, 500)
        assert chunks == ["line1\nline2"]

    def test_empty_text_returns_one_empty_chunk(self):
        chunks = _split_lines("", 500, 500)
        assert chunks == [""]

    def test_first_chunk_uses_first_cap(self):
        # 5 lines of 10 chars each = 55 chars total; first_cap=20 → must split
        body = "\n".join("x" * 10 for _ in range(5))
        chunks = _split_lines(body, first_cap=20, rest_cap=500)
        assert len(chunks) > 1
        assert len(chunks[0]) <= 20

    def test_each_chunk_within_capacity(self):
        body = "\n".join(f"Line {i}: " + "a" * 30 for i in range(30))
        chunks = _split_lines(body, first_cap=100, rest_cap=100)
        for i, chunk in enumerate(chunks):
            cap = 100 if i == 0 else 100
            assert len(chunk) <= cap + 1   # +1 tolerance for the last joined newline

    def test_no_line_is_split_mid_content(self):
        body = "\n".join(f"distinct-line-{i}" for i in range(20))
        chunks = _split_lines(body, first_cap=50, rest_cap=50)
        # Reconstruct and verify no line was broken
        recovered = "\n".join(chunks)
        for i in range(20):
            assert f"distinct-line-{i}" in recovered


# ═══════════════════════════════════════════════════════════════════════════════
# _send_email()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSendEmail:

    @pytest.fixture(autouse=True)
    def email_cfg(self, monkeypatch):
        monkeypatch.setattr(config, "ALERT_EMAIL",    "to@example.com")
        monkeypatch.setattr(config, "SMTP_HOST",      "smtp.example.com")
        monkeypatch.setattr(config, "SMTP_PORT",      587)
        monkeypatch.setattr(config, "SMTP_USER",      "user@example.com")
        monkeypatch.setattr(config, "SMTP_PASSWORD",  "secret")

    @patch("smtplib.SMTP")
    def test_starttls_login_sendmail_called(self, mock_smtp_cls):
        smtp_inst = MagicMock()
        mock_smtp_cls.return_value.__enter__ = lambda s: smtp_inst
        mock_smtp_cls.return_value.__exit__  = MagicMock(return_value=False)
        monitor._send_email("Subject", "Body", "info")
        smtp_inst.ehlo.assert_called_once()
        smtp_inst.starttls.assert_called_once()
        smtp_inst.login.assert_called_once_with("user@example.com", "secret")
        smtp_inst.sendmail.assert_called_once()

    @patch("smtplib.SMTP")
    def test_critical_prefix_in_subject(self, mock_smtp_cls):
        smtp_inst = MagicMock()
        mock_smtp_cls.return_value.__enter__ = lambda s: smtp_inst
        mock_smtp_cls.return_value.__exit__  = MagicMock(return_value=False)
        monitor._send_email("Fire", "Body", "critical")
        _, _, raw_msg = smtp_inst.sendmail.call_args.args
        assert "[CRITICAL]" in raw_msg

    @patch("smtplib.SMTP")
    def test_warning_prefix_in_subject(self, mock_smtp_cls):
        smtp_inst = MagicMock()
        mock_smtp_cls.return_value.__enter__ = lambda s: smtp_inst
        mock_smtp_cls.return_value.__exit__  = MagicMock(return_value=False)
        monitor._send_email("Warn", "Body", "warning")
        _, _, raw_msg = smtp_inst.sendmail.call_args.args
        assert "[WARNING]" in raw_msg

    @patch("smtplib.SMTP")
    def test_info_has_no_prefix(self, mock_smtp_cls):
        smtp_inst = MagicMock()
        mock_smtp_cls.return_value.__enter__ = lambda s: smtp_inst
        mock_smtp_cls.return_value.__exit__  = MagicMock(return_value=False)
        monitor._send_email("Daily Report", "Body", "info")
        _, _, raw_msg = smtp_inst.sendmail.call_args.args
        assert "[CRITICAL]" not in raw_msg
        assert "[WARNING]"  not in raw_msg

    @patch("smtplib.SMTP")
    def test_smtp_error_logged_not_raised(self, mock_smtp_cls):
        mock_smtp_cls.side_effect = smtplib.SMTPException("failed")
        # Must not propagate
        monitor._send_email("Subject", "Body", "info")


# ═══════════════════════════════════════════════════════════════════════════════
# send_report()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSendReport:

    @patch("monitor.send_alert")
    def test_daily_subject(self, mock_alert):
        send_report("report text", is_weekly=False)
        args   = mock_alert.call_args.args
        kwargs = mock_alert.call_args.kwargs
        subject, body = args[0], args[1]
        level = kwargs.get("level", args[2] if len(args) > 2 else "info")
        assert "Daily"  in subject
        assert "Weekly" not in subject
        assert body  == "report text"
        assert level == "info"

    @patch("monitor.send_alert")
    def test_weekly_subject(self, mock_alert):
        send_report("weekly text", is_weekly=True)
        subject = mock_alert.call_args.args[0]
        assert "Weekly" in subject
        assert "Daily"  not in subject

    @patch("monitor.send_alert")
    def test_routes_through_send_alert(self, mock_alert):
        send_report("x")
        mock_alert.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# build_daily_report()
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildDailyReport:

    @pytest.fixture
    def sample_data(self):
        return {
            "date":         date(2026, 3, 28),
            "equity_bod":   845_210.0,
            "equity_eod":   851_430.0,
            "exits": [
                {"symbol": "NVDA", "exit_reason": "ibs_exit",  "pnl": 1842.0,  "bars_held": 3},
                {"symbol": "AAPL", "exit_reason": "time_stop", "pnl": -2105.0, "bars_held": 15},
                {"symbol": "META", "exit_reason": "ibs_exit",  "pnl": 844.0,   "bars_held": 5},
            ],
            "entries": [
                {"symbol": "PLTR", "order_type": "LOC", "shares": 142,
                 "limit_price": 45.23, "qpi": 0.08, "ibs": 0.14},
                {"symbol": "SMCI", "order_type": "LOC", "shares": 87,
                 "limit_price": 28.11, "qpi": 0.06, "ibs": 0.11},
            ],
            "n_open":        13,
            "deployed_pct":  1.324,
            "ytd_pnl":       92_500.0,
            "ytd_pnl_pct":   12.3,
        }

    def test_contains_date(self, sample_data):
        report = build_daily_report(sample_data)
        assert "2026-03-28" in report

    def test_contains_equity_bod(self, sample_data):
        report = build_daily_report(sample_data)
        assert "845,210" in report

    def test_contains_equity_eod(self, sample_data):
        report = build_daily_report(sample_data)
        assert "851,430" in report

    def test_equity_change_positive_sign(self, sample_data):
        report = build_daily_report(sample_data)
        assert "+$6,220" in report

    def test_equity_change_pct(self, sample_data):
        report = build_daily_report(sample_data)
        # 6220 / 845210 * 100 ≈ 0.74%
        assert "0.74%" in report

    def test_exit_symbols_present(self, sample_data):
        report = build_daily_report(sample_data)
        for sym in ("NVDA", "AAPL", "META"):
            assert sym in report

    def test_exit_reasons_present(self, sample_data):
        report = build_daily_report(sample_data)
        assert "ibs_exit"  in report
        assert "time_stop" in report

    def test_negative_pnl_formatted_correctly(self, sample_data):
        report = build_daily_report(sample_data)
        assert "-$2,105" in report

    def test_entry_symbols_present(self, sample_data):
        report = build_daily_report(sample_data)
        assert "PLTR" in report
        assert "SMCI" in report

    def test_entry_limit_price(self, sample_data):
        report = build_daily_report(sample_data)
        assert "45.23" in report
        assert "28.11" in report

    def test_entry_qpi_ibs_shown(self, sample_data):
        report = build_daily_report(sample_data)
        assert "QPI=0.08" in report
        assert "IBS=0.14" in report

    def test_open_positions_shown(self, sample_data):
        report = build_daily_report(sample_data)
        assert "13" in report
        assert str(config.MAX_POSITIONS) in report

    def test_deployed_pct_shown(self, sample_data):
        report = build_daily_report(sample_data)
        assert "132.4%" in report

    def test_deployed_pct_fraction_renders_as_percentage(self):
        """deployed_pct=1.193 (fraction) must render as 119.3% of NLV, not 1.2%."""
        report = build_daily_report({
            "date":         date(2026, 4, 14),
            "equity_bod":   1_000_000.0,
            "equity_eod":   1_010_000.0,
            "exits":        [],
            "entries":      [],
            "n_open":       10,
            "deployed_pct": 1.193,
            "ytd_pnl":      10_000.0,
            "ytd_pnl_pct":  1.0,
        })
        assert "119.3% of NLV" in report
        assert "1.2%" not in report

    def test_ytd_pnl_shown(self, sample_data):
        report = build_daily_report(sample_data)
        assert "92,500" in report
        assert "12.3%" in report

    def test_rule_lines_present(self, sample_data):
        report = build_daily_report(sample_data)
        assert "─" * 10 in report   # at least part of the rule line

    def test_zero_exits_zero_entries(self):
        report = build_daily_report({
            "date": date(2026, 1, 2),
            "equity_bod":   100_000.0,
            "equity_eod":   100_000.0,
            "exits":        [],
            "entries":      [],
            "n_open":       0,
            "deployed_pct": 0.0,
            "ytd_pnl":      0.0,
            "ytd_pnl_pct":  0.0,
        })
        assert "Exits today:        0" in report
        assert "Entries today:      0" in report

    def test_negative_ytd_pnl_formatted_with_minus(self):
        report = build_daily_report({
            "date":         date(2026, 1, 15),
            "equity_bod":   90_000.0,
            "equity_eod":   88_000.0,
            "exits":        [],
            "entries":      [],
            "n_open":       2,
            "deployed_pct": 0.30,
            "ytd_pnl":      -5_000.0,
            "ytd_pnl_pct":  -5.0,
        })
        assert "-$5,000" in report
        assert "-5.0%"   in report

    def test_moc_entry_shows_moc_not_limit(self):
        report = build_daily_report({
            "date":         date(2026, 2, 1),
            "equity_bod":   100_000.0,
            "equity_eod":   100_000.0,
            "exits":        [],
            "entries": [
                {"symbol": "TSLA", "order_type": "MOC", "shares": 50,
                 "limit_price": None, "qpi": 0.05, "ibs": 0.12},
            ],
            "n_open":       1,
            "deployed_pct": 0.05,
            "ytd_pnl":      0.0,
            "ytd_pnl_pct":  0.0,
        })
        assert "MOC" in report
        assert "@ limit" not in report


# ═══════════════════════════════════════════════════════════════════════════════
# TestPctFormatting
# ═══════════════════════════════════════════════════════════════════════════════

class TestPctFormatting:
    """Verify updated _pct() behaviour: -0.0 guard, tiny-value 2dp override."""

    def test_zero_returns_zero_string(self):
        assert _pct(0.0) == "0.0%"

    def test_negative_zero_returns_zero_string(self):
        # -0.0 == 0.0 in Python; must not render as '-0.0%'
        assert _pct(-0.0) == "0.0%"

    def test_positive_value_has_plus_sign(self):
        assert _pct(1.5).startswith("+")

    def test_negative_value_has_minus_sign(self):
        assert _pct(-1.5).startswith("-")

    def test_value_below_threshold_uses_two_decimals(self):
        # abs(0.03) < 0.05 → must show 2 dp
        result = _pct(0.03)
        assert result == "+0.03%"

    def test_value_exactly_at_threshold_uses_passed_decimals(self):
        # abs(0.05) is NOT < 0.05 → use default 1 dp
        result = _pct(0.05)
        assert result == "+0.1%"

    def test_value_above_threshold_uses_passed_decimals(self):
        result = _pct(12.3, 1)
        assert result == "+12.3%"

    def test_large_value_explicit_two_decimals(self):
        result = _pct(0.74, 2)
        assert result == "+0.74%"

    def test_negative_tiny_value_uses_two_decimals(self):
        result = _pct(-0.03)
        assert result == "-0.03%"

    def test_default_decimals_is_one(self):
        # No decimals arg → default 1 (unless tiny override kicks in)
        result = _pct(5.5)
        assert result == "+5.5%"


# ═══════════════════════════════════════════════════════════════════════════════
# TestBuildWeeklyReport
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildWeeklyReport:

    @pytest.fixture
    def week_data(self):
        return {
            "week_start":   date(2026, 4,  6),
            "week_end":     date(2026, 4, 10),
            "equity_start": 840_000.0,
            "equity_end":   855_000.0,
            "exits": [
                {"symbol": "NVDA", "exit_reason": "ibs_exit",  "pnl": 1842.0,  "bars_held": 3},
                {"symbol": "AAPL", "exit_reason": "time_stop", "pnl": -2105.0, "bars_held": 15},
            ],
            "entries": [
                {"symbol": "PLTR", "order_type": "LOC", "shares": 142,
                 "limit_price": 45.23, "qpi": 0.08, "ibs": 0.14},
            ],
            "n_open":       12,
            "deployed_pct": 1.185,
            "ytd_pnl":      15_000.0,
            "ytd_pnl_pct":  1.8,
        }

    def test_contains_week_start_and_end(self, week_data):
        report = build_weekly_report(week_data)
        assert "2026-04-06" in report
        assert "2026-04-10" in report

    def test_contains_equity_start(self, week_data):
        report = build_weekly_report(week_data)
        assert "840,000" in report

    def test_contains_equity_end(self, week_data):
        report = build_weekly_report(week_data)
        assert "855,000" in report

    def test_equity_change_positive(self, week_data):
        report = build_weekly_report(week_data)
        assert "+$15,000" in report

    def test_exit_symbols_present(self, week_data):
        report = build_weekly_report(week_data)
        assert "NVDA" in report
        assert "AAPL" in report

    def test_exit_reasons_present(self, week_data):
        report = build_weekly_report(week_data)
        assert "ibs_exit"  in report
        assert "time_stop" in report

    def test_negative_exit_pnl_formatted(self, week_data):
        report = build_weekly_report(week_data)
        assert "-$2,105" in report

    def test_entry_symbol_present(self, week_data):
        report = build_weekly_report(week_data)
        assert "PLTR" in report

    def test_entry_limit_price_shown(self, week_data):
        report = build_weekly_report(week_data)
        assert "45.23" in report

    def test_entry_qpi_ibs_shown(self, week_data):
        report = build_weekly_report(week_data)
        assert "QPI=0.08" in report
        assert "IBS=0.14" in report

    def test_open_positions_shown(self, week_data):
        report = build_weekly_report(week_data)
        assert "12" in report
        assert str(config.MAX_POSITIONS) in report

    def test_deployed_pct_shown(self, week_data):
        report = build_weekly_report(week_data)
        assert "118.5%" in report

    def test_ytd_pnl_shown(self, week_data):
        report = build_weekly_report(week_data)
        assert "15,000" in report
        assert "1.8%" in report

    def test_rule_lines_present(self, week_data):
        report = build_weekly_report(week_data)
        assert "─" * 10 in report

    def test_weekly_header_in_report(self, week_data):
        report = build_weekly_report(week_data)
        assert "Weekly Report" in report

    def test_zero_exits_zero_entries(self):
        report = build_weekly_report({
            "week_start":   date(2026, 1, 5),
            "week_end":     date(2026, 1, 9),
            "equity_start": 100_000.0,
            "equity_end":   100_000.0,
            "exits":        [],
            "entries":      [],
            "n_open":       0,
            "deployed_pct": 0.0,
            "ytd_pnl":      0.0,
            "ytd_pnl_pct":  0.0,
        })
        assert "Exits this week:    0" in report
        assert "Entries this week:  0" in report

    def test_moc_entry_shows_moc(self):
        report = build_weekly_report({
            "week_start":   date(2026, 2, 2),
            "week_end":     date(2026, 2, 6),
            "equity_start": 100_000.0,
            "equity_end":   100_000.0,
            "exits":        [],
            "entries": [
                {"symbol": "TSLA", "order_type": "MOC", "shares": 50,
                 "limit_price": None, "qpi": 0.05, "ibs": 0.12},
            ],
            "n_open":       1,
            "deployed_pct": 0.05,
            "ytd_pnl":      0.0,
            "ytd_pnl_pct":  0.0,
        })
        assert "MOC" in report
        assert "@ limit" not in report


# ═══════════════════════════════════════════════════════════════════════════════
# TestReportEnhancements — positions table, APY, drawdown, accrued interest
# ═══════════════════════════════════════════════════════════════════════════════

class TestReportEnhancements:
    """Covers the enriched fields added to both daily and weekly reports."""

    @pytest.fixture
    def base_data(self):
        return {
            "date":         date(2026, 4, 14),
            "equity_bod":   1_000_000.0,
            "equity_eod":   1_010_000.0,
            "exits":        [],
            "entries":      [],
            "n_open":       2,
            "deployed_pct": 0.80,
            "ytd_pnl":      10_000.0,
            "ytd_pnl_pct":  1.0,
        }

    @pytest.fixture
    def two_positions(self):
        return [
            {
                "symbol":             "XOM",
                "entry_date":         "2026-04-09",
                "days_held":          5,
                "shares":             100,
                "fill_price":         155.17,
                "notional":           15_517.0,
                "current_price":      161.20,
                "unrealised_pnl":     603.0,
                "unrealised_pnl_pct": 3.88,
            },
            {
                "symbol":             "CVX",
                "entry_date":         "2026-04-07",
                "days_held":          7,
                "shares":             50,
                "fill_price":         200.00,
                "notional":           10_000.0,
                "current_price":      195.00,
                "unrealised_pnl":     -250.0,
                "unrealised_pnl_pct": -2.5,
            },
        ]

    # ── Positions table ───────────────────────────────────────────────────────

    def test_positions_table_symbols_shown(self, base_data, two_positions):
        base_data["open_positions_enriched"] = two_positions
        report = build_daily_report(base_data)
        assert "XOM" in report
        assert "CVX" in report

    def test_positions_table_omitted_when_no_enriched_key(self, base_data):
        report = build_daily_report(base_data)
        assert "Total unrealised" not in report

    def test_positions_table_omitted_when_empty_list(self, base_data):
        base_data["open_positions_enriched"] = []
        report = build_daily_report(base_data)
        assert "Total unrealised" not in report

    def test_total_unrealised_pnl_shown(self, base_data, two_positions):
        base_data["open_positions_enriched"] = two_positions
        report = build_daily_report(base_data)
        # 603 + (-250) = 353
        assert "Total unrealised" in report
        assert "+$353" in report

    def test_negative_unrealised_formatted_correctly(self, base_data):
        base_data["open_positions_enriched"] = [
            {
                "symbol":             "AAPL",
                "entry_date":         "2026-04-01",
                "days_held":          13,
                "shares":             50,
                "fill_price":         200.0,
                "notional":           10_000.0,
                "current_price":      190.0,
                "unrealised_pnl":     -500.0,
                "unrealised_pnl_pct": -5.0,
            }
        ]
        report = build_daily_report(base_data)
        assert "-$500" in report

    def test_positions_cash_columns_right_aligned(self, base_data, two_positions):
        """_usd(603.0) right-justified to _COL_WIDTH must appear in the report."""
        base_data["open_positions_enriched"] = two_positions
        report = build_daily_report(base_data)
        expected = _usd(603.0).rjust(monitor._COL_WIDTH)
        assert expected in report

    def test_total_unrealised_negative_when_all_positions_down(self, base_data):
        base_data["open_positions_enriched"] = [
            {
                "symbol":             "TSLA",
                "entry_date":         "2026-04-10",
                "days_held":          4,
                "shares":             20,
                "fill_price":         250.0,
                "notional":           5_000.0,
                "current_price":      240.0,
                "unrealised_pnl":     -200.0,
                "unrealised_pnl_pct": -4.0,
            }
        ]
        report = build_daily_report(base_data)
        assert "-$200" in report

    # ── APY ───────────────────────────────────────────────────────────────────

    def test_apy_inception_shown_when_provided(self, base_data):
        base_data["apy_inception"] = 43.2
        report = build_daily_report(base_data)
        assert "APY (inception)" in report
        assert "+43.2%" in report

    def test_apy_inception_shows_na_when_none(self, base_data):
        base_data["apy_inception"] = None
        report = build_daily_report(base_data)
        assert "APY (inception)" in report
        assert "n/a" in report

    def test_apy_7d_shows_na_when_none(self, base_data):
        base_data["apy_7d"] = None
        report = build_daily_report(base_data)
        assert "n/a" in report

    def test_apy_30d_shown(self, base_data):
        base_data["apy_30d"] = 51.4
        report = build_daily_report(base_data)
        assert "+51.4%" in report

    def test_apy_90d_shown(self, base_data):
        base_data["apy_90d"] = 38.1
        report = build_daily_report(base_data)
        assert "+38.1%" in report

    def test_apy_all_four_windows_present(self, base_data):
        """All four APY label lines must appear even when all values are n/a."""
        report = build_daily_report(base_data)
        assert "APY (inception)" in report
        assert "APY (7d)"        in report
        assert "APY (30d)"       in report
        assert "APY (90d)"       in report

    def test_apy_negative_value_formatted(self, base_data):
        base_data["apy_inception"] = -12.3
        report = build_daily_report(base_data)
        assert "-12.3%" in report

    # ── Drawdown ──────────────────────────────────────────────────────────────

    def test_drawdown_shown_when_not_none(self, base_data):
        base_data["drawdown_pct"] = -4.2
        base_data["ath"]          = 1_052_340.0
        report = build_daily_report(base_data)
        assert "Drawdown" in report
        assert "-4.2%"    in report
        assert "1,052,340" in report

    def test_drawdown_hidden_when_none(self, base_data):
        base_data["drawdown_pct"] = None
        report = build_daily_report(base_data)
        assert "Drawdown" not in report

    def test_drawdown_absent_key_hidden(self, base_data):
        # drawdown_pct key not set → hidden
        report = build_daily_report(base_data)
        assert "Drawdown" not in report

    # ── Accrued interest ──────────────────────────────────────────────────────

    def test_accrued_interest_shown_when_nonzero(self, base_data):
        base_data["accrued_interest"] = 124.50
        report = build_daily_report(base_data)
        assert "Accrued interest" in report
        assert "124.50"           in report
        assert "month-to-date"    in report

    def test_accrued_interest_hidden_when_zero(self, base_data):
        base_data["accrued_interest"] = 0.0
        report = build_daily_report(base_data)
        assert "Accrued interest" not in report

    def test_accrued_interest_absent_key_hidden(self, base_data):
        report = build_daily_report(base_data)
        assert "Accrued interest" not in report

    def test_accrued_interest_negative_shown(self, base_data):
        base_data["accrued_interest"] = -37.80
        report = build_daily_report(base_data)
        assert "Accrued interest" in report
        assert "37.80" in report

    # ── Weekly report parity ──────────────────────────────────────────────────

    def test_weekly_positions_table_shown(self, two_positions):
        data = {
            "week_start":   date(2026, 4, 7),
            "week_end":     date(2026, 4, 11),
            "equity_start": 1_000_000.0,
            "equity_end":   1_010_000.0,
            "exits":        [],
            "entries":      [],
            "n_open":       2,
            "deployed_pct": 0.80,
            "ytd_pnl":      10_000.0,
            "ytd_pnl_pct":  1.0,
            "open_positions_enriched": two_positions,
        }
        report = build_weekly_report(data)
        assert "XOM"              in report
        assert "Total unrealised" in report
        assert "+$353"            in report

    def test_weekly_apy_and_drawdown(self):
        data = {
            "week_start":    date(2026, 4, 7),
            "week_end":      date(2026, 4, 11),
            "equity_start":  1_000_000.0,
            "equity_end":    1_010_000.0,
            "exits":         [],
            "entries":       [],
            "n_open":        0,
            "deployed_pct":  0.0,
            "ytd_pnl":       0.0,
            "ytd_pnl_pct":   0.0,
            "apy_inception": 40.0,
            "apy_7d":        None,
            "apy_30d":       38.0,
            "apy_90d":       35.0,
            "drawdown_pct":  -3.1,
            "ath":           1_050_000.0,
        }
        report = build_weekly_report(data)
        assert "APY (inception)" in report
        assert "+40.0%"          in report
        assert "n/a"             in report
        assert "Drawdown"        in report
        assert "-3.1%"           in report
        assert "1,050,000"       in report

    def test_weekly_accrued_interest_shown(self):
        data = {
            "week_start":       date(2026, 4, 7),
            "week_end":         date(2026, 4, 11),
            "equity_start":     1_000_000.0,
            "equity_end":       1_010_000.0,
            "exits":            [],
            "entries":          [],
            "n_open":           0,
            "deployed_pct":     0.0,
            "ytd_pnl":          0.0,
            "ytd_pnl_pct":      0.0,
            "accrued_interest": 250.75,
        }
        report = build_weekly_report(data)
        assert "Accrued interest" in report
        assert "250.75"           in report
