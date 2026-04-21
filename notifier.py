"""
Email notifier for scan results.

v3.5.6 — sends an HTML email with full strong-signal details after each
scheduled scan, but only when one or more strong (composite >= 60) signals
fire. Weak-only and empty scans stay silent.

v3.5.5 change: force IPv4 resolution (_IPv4SMTPS subclass). Railway's
containers advertise IPv6 addressing but have no IPv6 egress route, so
Python's default dual-stack `create_connection` hit `[Errno 101] Network
is unreachable` on every Gmail send. Restricting resolution to A records
works around this without any external dependency.

v3.5.6 change: switch from STARTTLS on port 587 to implicit TLS on port
465 (``smtplib.SMTP_SSL``). Port 587 was silently dropped by Railway's
egress — after the IPv4 fix the connect succeeded but the TLS upgrade
stalled until the socket timed out. Port 465 (SMTPS, wrapped in TLS from
the first byte) goes through cleanly. The IPv4-only override is preserved
by subclassing ``SMTP_SSL`` instead of ``SMTP``.

Configured via environment variables (Railway):
    GMAIL_USER          — sending Gmail address (e.g. scanner@yourdomain.com
                          or a dedicated personal Gmail). Required.
    GMAIL_APP_PASSWORD  — 16-char Gmail app password (not the account
                          password). Required. See
                          https://myaccount.google.com/apppasswords.
    NOTIFY_EMAIL        — comma-separated recipient list. Defaults to
                          GMAIL_USER if unset.
    NOTIFY_ENABLED      — "true"/"false". Defaults to "true". Set to
                          "false" to hard-disable notifications.
    NOTIFY_MIN_SIGNALS  — int, minimum strong-signal count to trigger an
                          email. Defaults to 1.
    DASHBOARD_URL       — base URL of the live dashboard. Used to embed
                          a "View dashboard" link in the email.

Design guarantees:
    • Never raises out of send_scan_email(). All failures are logged and
      swallowed — the scanner must not be blocked by mail issues.
    • Sends in a background thread so SMTP latency does not delay the
      next scan.
    • No external dependencies beyond Python stdlib (smtplib, email,
      ssl, threading).
"""
from __future__ import annotations

import logging
import os
import smtplib
import socket
import ssl
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

# ── SMTP config ──────────────────────────────────────────────────────
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465  # implicit TLS (SMTPS) — 587/STARTTLS is blocked on Railway


def _ipv4_get_socket(smtp_self, host, port, timeout):
    """_get_socket override used by the IPv4-only SMTP[_SSL] subclasses.

    Railway (and some Docker networks) advertise IPv6 addressing on the
    container but have no IPv6 egress route. Python's default
    ``socket.create_connection`` goes through ``getaddrinfo`` with
    family=0, which on these hosts returns AAAA answers first — the
    kernel then returns ``[Errno 101] Network is unreachable`` before
    Python tries the IPv4 fallback. Forcing AF_INET sidesteps this.

    Hostname is preserved for TLS cert verification and SNI — we only
    change which address family is used for the underlying connect.
    """
    if smtp_self.debuglevel > 0:
        smtp_self._print_debug("connect: to", (host, port), smtp_self.source_address)
    infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    last_exc: Optional[OSError] = None
    for af, sktype, proto, _, sa in infos:
        sock: Optional[socket.socket] = None
        try:
            sock = socket.socket(af, sktype, proto)
            sock.settimeout(timeout)
            if smtp_self.source_address:
                sock.bind(smtp_self.source_address)
            sock.connect(sa)
            return sock
        except OSError as exc:
            last_exc = exc
            if sock is not None:
                sock.close()
    if last_exc is not None:
        raise last_exc
    raise OSError(f"Could not resolve {host!r} to any IPv4 address")


class _IPv4SMTP(smtplib.SMTP):
    """SMTP client (port 587 / STARTTLS) with IPv4-only DNS resolution.

    Kept for completeness in case STARTTLS ever needs to be used again —
    v3.5.6 sends via ``_IPv4SMTPS`` on port 465 by default.
    """

    def _get_socket(self, host, port, timeout):
        return _ipv4_get_socket(self, host, port, timeout)


class _IPv4SMTPS(smtplib.SMTP_SSL):
    """SMTP_SSL client (port 465 / implicit TLS) with IPv4-only DNS
    resolution. See ``_ipv4_get_socket`` for the rationale.

    SMTP_SSL wraps the socket in TLS from the first byte, which bypasses
    the blocked port 587 / STARTTLS path on Railway.
    """

    def _get_socket(self, host, port, timeout):
        # Create the raw IPv4 socket first, then let SMTP_SSL upgrade it
        # to TLS. SMTP_SSL's _get_socket expects to receive an SSL-wrapped
        # socket back, so we delegate the wrapping to its base logic.
        raw = _ipv4_get_socket(self, host, port, timeout)
        return self.context.wrap_socket(raw, server_hostname=self._host)


# ── Env helpers (read at call time so Railway updates take effect) ──
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _enabled() -> bool:
    return _env("NOTIFY_ENABLED", "true").lower() in ("1", "true", "yes", "on")


def _is_configured() -> bool:
    return bool(_env("GMAIL_USER")) and bool(_env("GMAIL_APP_PASSWORD"))


def _recipients() -> List[str]:
    raw = _env("NOTIFY_EMAIL") or _env("GMAIL_USER")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _min_signals() -> int:
    try:
        return max(1, int(_env("NOTIFY_MIN_SIGNALS", "1")))
    except ValueError:
        return 1


def _dashboard_url() -> str:
    return _env("DASHBOARD_URL", "https://trader-v3-production.up.railway.app").rstrip("/")


# ── Rendering ────────────────────────────────────────────────────────
_TIER_COLOR = {
    "primary": "#22c55e",       # green — LEADER/SOLO
    "secondary": "#f59e0b",     # amber — FOLLOWER
    "unclassified": "#94a3b8",  # slate — LAGGARD/UNKNOWN
}
_TIER_LABEL = {
    "primary": "PRIMARY",
    "secondary": "SECONDARY",
    "unclassified": "UNCLASSIFIED",
}


def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "—"
    try:
        return f"{val:+.2f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_money(val: Optional[float]) -> str:
    if val is None:
        return "—"
    try:
        return f"${val:,.2f}"
    except (TypeError, ValueError):
        return "—"


def _render_html(
    signals: List[Dict],
    regime: Optional[Dict],
    scan_time: str,
    dashboard_url: str,
) -> str:
    """Render the HTML body. One row per signal, tier-colored ticker."""
    rows_html: List[str] = []
    for s in signals:
        tier = s.get("leader_tier", "primary")
        color = _TIER_COLOR.get(tier, "#94a3b8")
        tier_label = _TIER_LABEL.get(tier, "—")
        lead_label = (s.get("leadership") or {}).get("label", "")
        earn = s.get("earnings") or {}
        earn_text = ""
        if earn.get("has_earnings") and earn.get("badge_text"):
            lvl = earn.get("badge_level", "")
            if lvl in ("today_amc", "tomorrow"):
                earn_text = f'<span style="color:#ef4444; font-size:11px; margin-left:4px;">⚡ {earn["badge_text"]}</span>'
        rows_html.append(
            f"""
            <tr>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148;">
                    <div style="font-weight:700; color:{color}; font-size:16px; font-family:'SF Mono', Menlo, monospace;">{s.get('ticker', '—')}</div>
                    <div style="font-size:10px; color:#94a3b8; letter-spacing:0.5px; margin-top:2px;">{tier_label} · {lead_label}{earn_text}</div>
                </td>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148; text-align:right; font-family:'SF Mono', Menlo, monospace; color:#e2e8f0;">
                    <div style="font-weight:700; font-size:15px;">{s.get('composite_score', '—')}</div>
                    <div style="font-size:10px; color:#94a3b8;">score</div>
                </td>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148; text-align:right; font-family:'SF Mono', Menlo, monospace; color:#e2e8f0;">
                    {s.get('rvol', '—')}x
                    <div style="font-size:10px; color:#94a3b8;">RVOL</div>
                </td>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148; text-align:right; font-family:'SF Mono', Menlo, monospace; color:#e2e8f0;">
                    {_fmt_money(s.get('entry'))}
                    <div style="font-size:10px; color:#94a3b8;">entry</div>
                </td>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148; text-align:right; font-family:'SF Mono', Menlo, monospace; color:#ef4444;">
                    {_fmt_money(s.get('stop_loss'))}
                    <div style="font-size:10px; color:#94a3b8;">stop</div>
                </td>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148; text-align:right; font-family:'SF Mono', Menlo, monospace; color:#22c55e;">
                    {_fmt_money(s.get('atr_target'))}
                    <div style="font-size:10px; color:#94a3b8;">target</div>
                </td>
                <td style="padding:10px 8px; border-bottom:1px solid #2a3148; text-align:right; font-family:'SF Mono', Menlo, monospace; color:#e2e8f0;">
                    {s.get('risk_reward_ratio', '—')}:1
                    <div style="font-size:10px; color:#94a3b8;">R:R</div>
                </td>
            </tr>
            """
        )

    # Regime banner
    regime_banner = ""
    if regime:
        rg_label = regime.get("regime", "NORMAL")
        rg_vix = regime.get("vix")
        rg_mult = regime.get("size_multiplier", 1.0)
        rg_min = regime.get("effective_min_score", 60)
        rg_color = {"CALM": "#22c55e", "NORMAL": "#3b82f6", "ELEVATED": "#f59e0b", "HIGH": "#ef4444"}.get(rg_label, "#3b82f6")
        regime_banner = f"""
        <div style="background:#1a1f2e; border-left:4px solid {rg_color}; padding:10px 14px; margin:0 0 18px 0; border-radius:4px;">
            <div style="color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase;">Market Regime</div>
            <div style="color:{rg_color}; font-weight:700; font-size:14px; margin-top:2px;">{rg_label}{" · VIX " + f"{rg_vix:.1f}" if rg_vix else ""}</div>
            <div style="color:#94a3b8; font-size:12px; margin-top:4px;">Min score {rg_min} · size multiplier {rg_mult}x</div>
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="margin:0; padding:20px; background:#0a0e17; color:#e2e8f0; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;">
        <div style="max-width:720px; margin:0 auto;">
            <div style="border-bottom:2px solid #2a3148; padding-bottom:14px; margin-bottom:18px;">
                <div style="font-size:22px; font-weight:700; color:#e2e8f0;">Momentum Scanner — {len(signals)} strong signal{"s" if len(signals) != 1 else ""}</div>
                <div style="color:#94a3b8; font-size:13px; margin-top:4px;">Scanned at {scan_time} · v3.4.3</div>
            </div>

            {regime_banner}

            <table style="width:100%; border-collapse:collapse; background:#111827; border-radius:6px; overflow:hidden;">
                <thead>
                    <tr style="background:#1a1f2e;">
                        <th style="padding:10px 8px; text-align:left; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">Ticker</th>
                        <th style="padding:10px 8px; text-align:right; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">Score</th>
                        <th style="padding:10px 8px; text-align:right; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">RVOL</th>
                        <th style="padding:10px 8px; text-align:right; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">Entry</th>
                        <th style="padding:10px 8px; text-align:right; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">Stop</th>
                        <th style="padding:10px 8px; text-align:right; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">Target</th>
                        <th style="padding:10px 8px; text-align:right; color:#94a3b8; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; border-bottom:1px solid #2a3148;">R:R</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows_html)}
                </tbody>
            </table>

            <div style="margin-top:22px; text-align:center;">
                <a href="{dashboard_url}" style="display:inline-block; background:#3b82f6; color:#ffffff; padding:12px 22px; border-radius:6px; text-decoration:none; font-weight:600; font-size:14px;">View dashboard →</a>
            </div>

            <div style="margin-top:24px; padding-top:14px; border-top:1px solid #2a3148; color:#64748b; font-size:11px; line-height:1.5;">
                Sent by the Momentum Scanner. Strong signals only (composite ≥ 60). Weak-only scans stay silent.<br>
                Educational tool — not financial advice. Always verify the entry against the live tape at Fidelity before trading.
            </div>
        </div>
    </body>
    </html>
    """


def _render_plaintext(signals: List[Dict], scan_time: str, dashboard_url: str) -> str:
    """Plain-text fallback — readable in any client, including SMS-to-email bridges."""
    lines = [
        f"Momentum Scanner — {len(signals)} strong signal{'s' if len(signals) != 1 else ''} @ {scan_time}",
        "",
    ]
    for s in signals:
        tier = _TIER_LABEL.get(s.get("leader_tier", "primary"), "—")
        lines.append(
            f"  {s.get('ticker', '—'):6s}  score {s.get('composite_score', '—')}  "
            f"RVOL {s.get('rvol', '—')}x  "
            f"entry {_fmt_money(s.get('entry'))}  "
            f"stop {_fmt_money(s.get('stop_loss'))}  "
            f"target {_fmt_money(s.get('atr_target'))}  "
            f"R:R {s.get('risk_reward_ratio', '—')}:1  "
            f"[{tier}]"
        )
    lines.append("")
    lines.append(f"Dashboard: {dashboard_url}")
    lines.append("")
    lines.append("Educational tool — not financial advice.")
    return "\n".join(lines)


def _build_subject(signals: List[Dict], scan_time: str) -> str:
    n = len(signals)
    top = ", ".join(s.get("ticker", "?") for s in signals[:5])
    if n > 5:
        top += f", +{n - 5} more"
    return f"[MScan] {n} strong signal{'s' if n != 1 else ''} @ {scan_time} — {top}"


# ── SMTP send ────────────────────────────────────────────────────────
def _send_smtp(subject: str, html_body: str, text_body: str, recipients: List[str]) -> None:
    """Send via Gmail SMTP. Raises on failure; callers should catch."""
    user = _env("GMAIL_USER")
    app_pw = _env("GMAIL_APP_PASSWORD")

    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr(("Momentum Scanner", user))
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    ctx = ssl.create_default_context()
    # v3.5.6: implicit-TLS SMTPS on port 465, IPv4-only. See _IPv4SMTPS /
    # _ipv4_get_socket docstrings for the Railway-specific rationale.
    with _IPv4SMTPS(SMTP_HOST, SMTP_PORT, context=ctx, timeout=20) as smtp:
        smtp.ehlo()
        smtp.login(user, app_pw)
        smtp.sendmail(user, recipients, msg.as_string())


def _dispatch(signals: List[Dict], regime: Optional[Dict], scan_time: str) -> None:
    """Build + send the email. Called on a background thread."""
    try:
        dashboard_url = _dashboard_url()
        recipients = _recipients()
        if not recipients:
            logger.warning("notifier: no recipients resolved; skipping send")
            return
        subject = _build_subject(signals, scan_time)
        html = _render_html(signals, regime, scan_time, dashboard_url)
        text = _render_plaintext(signals, scan_time, dashboard_url)
        _send_smtp(subject, html, text, recipients)
        logger.info(
            f"notifier: email sent to {len(recipients)} recipient(s) "
            f"— {len(signals)} strong signal(s)"
        )
    except Exception as exc:
        # Never let SMTP break the scanner.
        logger.error(f"notifier: failed to send email: {exc}")


# ── Public entry point ───────────────────────────────────────────────
def send_scan_email(
    signals: Iterable[Dict],
    regime: Optional[Dict] = None,
    scan_time: Optional[str] = None,
) -> None:
    """
    Send a scan-result email if:
      - NOTIFY_ENABLED is truthy
      - SMTP creds are present
      - at least NOTIFY_MIN_SIGNALS strong signals exist

    This function returns immediately; the SMTP call runs on a daemon
    thread so it can never block the scheduler.
    """
    if not _enabled():
        logger.debug("notifier: NOTIFY_ENABLED=false, skipping")
        return

    strong = [s for s in signals if s.get("signal_strength") == "strong"]
    if len(strong) < _min_signals():
        logger.debug(
            f"notifier: {len(strong)} strong signal(s) "
            f"< NOTIFY_MIN_SIGNALS={_min_signals()}, skipping"
        )
        return

    if not _is_configured():
        logger.warning(
            "notifier: strong signals found but GMAIL_USER / GMAIL_APP_PASSWORD "
            "not set — email skipped. Configure these env vars on Railway to enable."
        )
        return

    ts = scan_time or datetime.now().strftime("%Y-%m-%d %I:%M %p ET")
    thread = threading.Thread(
        target=_dispatch,
        args=(strong, regime, ts),
        daemon=True,
        name="scanner-notifier",
    )
    thread.start()
