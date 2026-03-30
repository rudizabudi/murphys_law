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
    _split_lines,
    build_daily_report,
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
            "deployed_pct":  132.4,
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
            "deployed_pct": 30.0,
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
            "deployed_pct": 5.0,
            "ytd_pnl":      0.0,
            "ytd_pnl_pct":  0.0,
        })
        assert "MOC" in report
        assert "@ limit" not in report
