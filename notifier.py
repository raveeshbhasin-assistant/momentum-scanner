"""
Email notifier for scan results.

v3.5.8 — sends an HTML email with full strong-signal details after each
scheduled scan, but only when:
  • the scan fired during the US equity regular session (09:30–16:00 ET,
    Mon–Fri) — controlled by NOTIFY_MARKET_HOURS_ONLY, default on
  • at least NOTIFY_MIN_SIGNALS strong signals survive the category filter
  • those signals land in NOTIFY_CATEGORIES (default "A,B" — high-score
    leader + high-score non-leader). Cat C (low-score) and Cat D
    (unclassified) are dropped from the email but stay in the dashboard.

Release history of this file:

    v3.5.2  Introduced smtplib-based notifier (Gmail SMTP on port 587).
    v3.5.5  Forced IPv4 DNS resolution to work around Railway's absent
            IPv6 egress (`[Errno 101] Network is unreachable`).
    v3.5.6  Switched STARTTLS:587 → SMTPS:465 because Railway's egress
            silently dropped 587.
    v3.5.7  Gave up on SMTP entirely — a GET /api/diagnose/egress probe
            confirmed Railway drops outbound 25/465/587 (all 5 s timeouts,
            same IP) while leaving 443 fully open. The notifier now POSTs
            to Resend's HTTPS API on 443. Zero smtplib code remains.
    v3.5.8  (this file) Added market-hours filter and category filter so
            pre-market / after-hours scans and low-quality (Cat C / D)
            signals don't trigger mail. Added send_test_email() for a
            no-filter round-trip check via POST /api/notify/test.

Configuration (env vars):
    RESEND_API_KEY          — API key from https://resend.com/api-keys.
                              Required. Without it the notifier logs and no-ops.
    NOTIFY_FROM             — "From" address or "Name <addr>" form. Defaults
                              to "Momentum Scanner <onboarding@resend.dev>",
                              the Resend sandbox sender. For multi-recipient
                              delivery, this must be on a verified custom domain.
    NOTIFY_EMAIL            — comma-separated recipient list. Required.
    NOTIFY_ENABLED          — "true"/"false". Defaults to "true".
    NOTIFY_MIN_SIGNALS      — int, minimum matching-signal count to trigger
                              an email. Defaults to 1.
    NOTIFY_CATEGORIES       — comma-separated subset of {A,B,C,D}. Defaults
                              to "A,B". Categories are computed per-signal
                              from composite_score + leadership.label using
                              the same rule the Performance page uses.
    NOTIFY_MARKET_HOURS_ONLY — "true"/"false". Defaults to "true". When on,
                              mails are suppressed outside 09:30–16:00 ET
                              Mon–Fri (US equity regular session).
    DASHBOARD_URL           — base URL of the live dashboard. Used to embed
                              a "View dashboard" link in the email.

Legacy vars GMAIL_USER / GMAIL_APP_PASSWORD are no longer read. They can
be safely removed from Railway, though leaving them set causes no harm.

Design guarantees:
    • Never raises out of send_scan_email(). All failures are logged and
      swallowed — the scanner must not be blocked by mail issues.
    • Sends in a background thread so the HTTP call does not delay the
      next scan.
    • Only stdlib + httpx (already a dep via data_backup.py).
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set

import httpx

import config
from performance_engine import assign_category

logger = logging.getLogger(__name__)

# ── Resend config ────────────────────────────────────────────────────
RESEND_API_URL = "https://api.resend.com/emails"
DEFAULT_FROM = "Momentum Scanner <onboarding@resend.dev>"
USER_AGENT = "MomentumScanner-Notifier/3.5.8"

# US equity regular-session boundaries (ET). Used by _in_market_hours.
MARKET_OPEN_HM = (9, 30)   # 09:30 ET
MARKET_CLOSE_HM = (16, 0)  # 16:00 ET


# ── Env helpers (read at call time so Railway updates take effect) ──
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _enabled() -> bool:
    return _env("NOTIFY_ENABLED", "true").lower() in ("1", "true", "yes", "on")


def _is_configured() -> bool:
    return bool(_env("RESEND_API_KEY")) and bool(_recipients())


def _recipients() -> List[str]:
    raw = _env("NOTIFY_EMAIL")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _from_address() -> str:
    return _env("NOTIFY_FROM", DEFAULT_FROM)


def _min_signals() -> int:
    try:
        return max(1, int(_env("NOTIFY_MIN_SIGNALS", "1")))
    except ValueError:
        return 1


def _dashboard_url() -> str:
    return _env(
        "DASHBOARD_URL",
        "https://momentum-scanner-production-20b1.up.railway.app",
    ).rstrip("/")


# ── v3.5.8 filters: market hours + category gate ─────────────────────
def _allowed_categories() -> Set[str]:
    """
    Parse NOTIFY_CATEGORIES (default "A,B"). Silently drops unknown
    tokens. Falls back to {"A","B"} if the env var is empty or invalid
    so a typo never results in zero emails ever being sent.
    """
    raw = _env("NOTIFY_CATEGORIES", "A,B").upper()
    allowed = {c.strip() for c in raw.split(",") if c.strip() in ("A", "B", "C", "D")}
    return allowed or {"A", "B"}


def _market_hours_only() -> bool:
    return _env("NOTIFY_MARKET_HOURS_ONLY", "true").lower() in ("1", "true", "yes", "on")


def _in_market_hours(now_et: Optional[datetime] = None) -> bool:
    """
    True iff `now_et` is Mon–Fri and within 09:30–16:00 ET (inclusive
    on open, inclusive on close). If `now_et` is None, reads the
    current time in America/New_York.
    """
    if now_et is None:
        now_et = datetime.now(config.ET)
    elif now_et.tzinfo is None:
        now_et = now_et.replace(tzinfo=config.ET)
    else:
        now_et = now_et.astimezone(config.ET)
    if now_et.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    oh, om = MARKET_OPEN_HM
    ch, cm = MARKET_CLOSE_HM
    open_t = now_et.replace(hour=oh, minute=om, second=0, microsecond=0)
    close_t = now_et.replace(hour=ch, minute=cm, second=0, microsecond=0)
    return open_t <= now_et <= close_t


def _signal_category(s: Dict) -> str:
    """
    Compute the Performance-page category for a single signal dict.
    Mirrors the rule in performance_engine.assign_category.
    """
    lbl = (s.get("leadership") or {}).get("label")
    return assign_category(s.get("composite_score"), lbl)


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
                <div style="color:#94a3b8; font-size:13px; margin-top:4px;">Scanned at {scan_time} · v3.5.8</div>
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


# ── Resend HTTP send ─────────────────────────────────────────────────
def _send_via_resend(
    subject: str, html_body: str, text_body: str, recipients: List[str]
) -> None:
    """POST to Resend's /emails endpoint. Raises on non-2xx; callers catch."""
    api_key = _env("RESEND_API_KEY")
    payload = {
        "from": _from_address(),
        "to": recipients,
        "subject": subject,
        "html": html_body,
        "text": text_body,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    with httpx.Client(timeout=15.0) as client:
        r = client.post(RESEND_API_URL, json=payload, headers=headers)
    if r.status_code >= 300:
        # Pull the Resend error message if JSON, else raw text.
        try:
            err = r.json()
        except ValueError:
            err = r.text
        raise RuntimeError(f"Resend HTTP {r.status_code}: {err}")


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
        _send_via_resend(subject, html, text, recipients)
        logger.info(
            f"notifier: email sent via Resend to {len(recipients)} recipient(s) "
            f"— {len(signals)} strong signal(s)"
        )
    except Exception as exc:
        # Never let a mail failure break the scanner.
        logger.error(f"notifier: failed to send email: {exc}")


# ── Public entry point ───────────────────────────────────────────────
def send_scan_email(
    signals: Iterable[Dict],
    regime: Optional[Dict] = None,
    scan_time: Optional[str] = None,
) -> None:
    """
    Send a scan-result email if ALL of the following hold:
      - NOTIFY_ENABLED is truthy
      - the scan fired during US equity regular session (Mon–Fri 09:30–
        16:00 ET) — skipped by setting NOTIFY_MARKET_HOURS_ONLY=false
      - RESEND_API_KEY is set and at least one recipient is configured
      - at least one strong signal (composite_score ≥ 60) is in an
        allowed category (NOTIFY_CATEGORIES, default "A,B")
      - that surviving set contains ≥ NOTIFY_MIN_SIGNALS entries

    This function returns immediately; the HTTP call runs on a daemon
    thread so it can never block the scheduler.
    """
    if not _enabled():
        logger.debug("notifier: NOTIFY_ENABLED=false, skipping")
        return

    # Market-hours gate ─────────────────────────────────────────────
    if _market_hours_only() and not _in_market_hours():
        now_et = datetime.now(config.ET)
        logger.info(
            "notifier: outside market hours "
            f"({now_et.strftime('%Y-%m-%d %a %H:%M ET')}) — skipping"
        )
        return

    # Strong filter (composite ≥ 60) — same as before
    strong = [s for s in signals if s.get("signal_strength") == "strong"]
    if not strong:
        logger.debug("notifier: no strong signals, skipping")
        return

    # Category filter — drop Cat C/D (low-score / unclassified) by default
    allowed = _allowed_categories()
    filtered = [s for s in strong if _signal_category(s) in allowed]
    if len(filtered) < _min_signals():
        logger.info(
            f"notifier: {len(strong)} strong → {len(filtered)} after category "
            f"filter {sorted(allowed)} (< NOTIFY_MIN_SIGNALS={_min_signals()}), skipping"
        )
        return

    if not _is_configured():
        logger.warning(
            "notifier: matching signals found but RESEND_API_KEY / NOTIFY_EMAIL "
            "not set — email skipped. Configure these env vars on Railway to enable."
        )
        return

    ts = scan_time or datetime.now(config.ET).strftime("%Y-%m-%d %I:%M %p ET")
    thread = threading.Thread(
        target=_dispatch,
        args=(filtered, regime, ts),
        daemon=True,
        name="scanner-notifier",
    )
    thread.start()


# ── Test-email endpoint helper ───────────────────────────────────────
def send_test_email() -> Dict:
    """
    Synchronous, filter-bypassing round-trip test.

    Sends a single canned email to NOTIFY_EMAIL via Resend so the user
    can verify their Railway env vars + domain verification without
    waiting for a market-hours Cat-A/B scan to fire.

    Returns a dict describing the outcome — never raises. The caller
    (POST /api/notify/test) surfaces this as JSON.
    """
    if not _is_configured():
        missing = []
        if not _env("RESEND_API_KEY"):
            missing.append("RESEND_API_KEY")
        if not _recipients():
            missing.append("NOTIFY_EMAIL")
        return {
            "ok": False,
            "error": f"missing env vars: {', '.join(missing)}",
            "hint": "Set them in Railway → Variables, then redeploy.",
        }

    recipients = _recipients()
    dashboard_url = _dashboard_url()
    now_et = datetime.now(config.ET)
    scan_time = now_et.strftime("%Y-%m-%d %I:%M %p ET")

    # Canned signal — illustrative only, not a real trade.
    demo_signal: Dict = {
        "ticker": "TEST",
        "leader_tier": "primary",
        "leadership": {"label": "LEADER"},
        "composite_score": 72,
        "rvol": 2.3,
        "entry": 100.00,
        "stop_loss": 98.50,
        "atr_target": 103.00,
        "risk_reward_ratio": 2.0,
        "earnings": {},
    }
    demo_regime = {
        "regime": "NORMAL",
        "vix": 14.5,
        "size_multiplier": 1.0,
        "effective_min_score": 60,
    }

    subject = f"[MScan] Test email — round-trip OK @ {scan_time}"
    html = _render_html([demo_signal], demo_regime, scan_time, dashboard_url)
    text = _render_plaintext([demo_signal], scan_time, dashboard_url)

    try:
        _send_via_resend(subject, html, text, recipients)
    except Exception as exc:
        logger.error(f"notifier: test email failed: {exc}")
        return {
            "ok": False,
            "error": str(exc),
            "from": _from_address(),
            "to": recipients,
        }

    logger.info(f"notifier: test email sent to {len(recipients)} recipient(s)")
    return {
        "ok": True,
        "from": _from_address(),
        "to": recipients,
        "subject": subject,
        "sent_at": scan_time,
        "market_hours_only": _market_hours_only(),
        "allowed_categories": sorted(_allowed_categories()),
        "note": "Test email bypasses both the market-hours and category filters.",
    }
