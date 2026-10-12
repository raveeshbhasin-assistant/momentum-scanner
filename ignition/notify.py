"""
ignition/notify.py — email every new FIRE and REFIRE the run of record found.

    python ignition/notify.py          # reads ignition/data/latest.json, mails if anything is new

Runs as the last step of .github/workflows/ignition-daily.yml, after the run
has been committed to the `ignition-data` branch, so a mail goes out once per
new event: scan.py marks as new only the events dated after the previous run's
data date (summary.since), so a same-day re-run, a run refused for partial or
stale data (it never reaches this step) and the themes_web fallback scan (it
never runs this script) all mail nothing. A missed run is not a missed mail:
the next run reports everything since the last published data date as new.

Standalone like the rest of ignition/: stdlib only, imports nothing from the
scanner or themes (notifier.py is the scanner's; this module shares no code
with it). Sends through Resend's HTTPS API, one mail per recipient.

Env (GitHub Actions secrets / variables):
    RESEND_API_KEY          required to send. Unset = not configured: the step
                            prints what it would have sent and exits 0.
    IGNITION_NOTIFY_EMAIL   comma-separated recipients. Defaults to RECIPIENTS.
    IGNITION_NOTIFY_FROM    "Name <addr>". Defaults to Resend's sandbox sender,
                            which can only deliver to the Resend account owner's
                            own address; mailing the other recipients needs a
                            sender on a domain verified in Resend.
    IGNITION_PAGE_URL       link in the mail. Defaults to the live /ignition page.

A send that Resend rejects exits 1 so the Actions run shows red (the data is
already committed by then; only the mail is missing).
"""
from __future__ import annotations

import html
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent
LATEST = HERE / "data" / "latest.json"

RESEND_API_URL = "https://api.resend.com/emails"
RECIPIENTS = ["Raveesh.bhasin@gmail.com", "Meetarao@gmail.com", "shalomthomas@gmail.com"]
DEFAULT_FROM = "Ignition Watch <onboarding@resend.dev>"
DEFAULT_PAGE_URL = "https://alert-youthfulness-production-354a.up.railway.app/ignition"
TIMEOUT_S = 20
TYPES = ("FIRE", "REFIRE")          # what triggers a mail; everything else is context only


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or "").strip() or default


def recipients() -> list[str]:
    raw = _env("IGNITION_NOTIFY_EMAIL")
    return [a.strip() for a in raw.split(",") if a.strip()] if raw else list(RECIPIENTS)


def _position(latest: dict, ev: dict) -> dict | None:
    """The ledger row an event belongs to: a FIRE opens the position whose
    `fired` is the event date; a REFIRE lands on the open position whose
    `last_refire` is. Falls back to the newest open row for the ticker."""
    rows = [p for p in latest.get("positions", []) if p.get("ticker") == ev["ticker"]]
    key = "fired" if ev["type"] == "FIRE" else "last_refire"
    for p in rows:
        if p.get(key) == ev["date"]:
            return p
    open_ = [p for p in rows if p.get("status") != "CLOSED"]
    return max(open_, key=lambda p: p.get("fired") or "") if open_ else None


def collect(latest: dict) -> list[dict]:
    """New FIRE/REFIRE events from the run, each joined to its ledger row."""
    out = []
    for ev in latest.get("summary", {}).get("new_events", []):
        if ev.get("type") in TYPES:
            out.append({"event": ev, "position": _position(latest, ev)})
    out.sort(key=lambda x: (TYPES.index(x["event"]["type"]), x["event"]["ticker"]))
    return out


def _fmt(x, suffix: str = "", sign: bool = False) -> str:
    """Price (2 dp) or, with a suffix, a 1-dp percentage; None renders as a dash."""
    if x is None:
        return "—"
    if sign:
        return f"{x:+.1f}{suffix}"
    return f"{x:.1f}{suffix}" if suffix else f"{x:,.2f}"


def subject(latest: dict, items: list[dict]) -> str:
    fires = [i["event"]["ticker"] for i in items if i["event"]["type"] == "FIRE"]
    refires = [i["event"]["ticker"] for i in items if i["event"]["type"] == "REFIRE"]
    parts = []
    if fires:
        parts.append(("new fire " if len(fires) == 1 else "new fires ") + ", ".join(fires))
    if refires:
        parts.append(("re-fire " if len(refires) == 1 else "re-fires ") + ", ".join(refires))
    return f"Ignition Watch {latest.get('asof', '')}: " + " · ".join(parts)


def _row_fields(item: dict) -> dict:
    ev, p = item["event"], item["position"] or {}
    tags = []
    if p.get("gap"):
        tags.append("gap · pinned" if p.get("pinned") else "gap")
    if p.get("carried"):
        tags.append("carried")
    if ev["type"] == "FIRE":
        plan = (f"bought {_fmt(p.get('entry'))} at the {p.get('entry_date')} open"
                if p.get("entry") else "buy at the next open")
    else:
        plan = (f"held since {p.get('entry_date')} at {_fmt(p.get('entry'))}; "
                f"{_fmt(p.get('ret'), '%', sign=True)} so far" if p.get("entry") else "position open")
    return {
        "type": "NEW FIRE" if ev["type"] == "FIRE" else "RE-FIRE",
        "ticker": ev["ticker"], "date": ev["date"], "detail": ev.get("detail", ""),
        "close": _fmt(p.get("price") if p.get("price") is not None else p.get("fire_close")),
        "base": _fmt(p.get("base")), "to_stop": _fmt(p.get("to_stop"), "%", sign=True),
        "plan": plan, "tags": ", ".join(tags),
    }


def _context_lines(latest: dict) -> list[str]:
    s = latest.get("summary", {})
    lines = []
    if s.get("new_sells"):
        lines.append("Sells this run: " + ", ".join(s["new_sells"]))
    if s.get("new_expired"):
        lines.append("Edge expired (63 sessions, review only): " + ", ".join(s["new_expired"]))
    if s.get("new_checkpoints"):
        lines.append("6-month review flagged: " + ", ".join(s["new_checkpoints"]))
    return lines


def render_text(latest: dict, items: list[dict], page_url: str) -> str:
    lines = [f"Ignition Watch — data as of {latest.get('asof')} (scanned {latest.get('scanned_at')})", ""]
    for it in items:
        r = _row_fields(it)
        lines.append(f"{r['type']}  {r['ticker']}  ({r['date']})" + (f"  [{r['tags']}]" if r["tags"] else ""))
        lines.append(f"    {r['detail']}")
        lines.append(f"    close {r['close']} · pre-ignition base {r['base']} ({r['to_stop']} away) · {r['plan']}")
        lines.append("")
    lines += [
        "Rule: buy at the next open; sell at the next open after a close below the pre-ignition base",
        "(the close five sessions before the fire). Dropping off the signal list is not a sell.",
        "",
    ]
    ctx = _context_lines(latest)
    if ctx:
        lines += ctx + [""]
    s = latest.get("summary", {})
    lines.append(f"Ledger: {s.get('open', 0)} open, {s.get('closed', 0)} closed · {page_url}")
    return "\n".join(lines)


def render_html(latest: dict, items: list[dict], page_url: str) -> str:
    e = html.escape
    cell = 'style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:14px;vertical-align:top"'
    head = 'style="padding:6px 10px;border-bottom:2px solid #111;font-size:12px;text-align:left;color:#555"'
    rows = []
    for it in items:
        r = _row_fields(it)
        color = "#b45309" if r["type"] == "RE-FIRE" else "#15803d"
        tags = f' <span style="color:#6b7280;font-size:12px">[{e(r["tags"])}]</span>' if r["tags"] else ""
        rows.append(
            f"<tr><td {cell}><b style='color:{color}'>{e(r['type'])}</b></td>"
            f"<td {cell}><b>{e(r['ticker'])}</b>{tags}<br><span style='color:#6b7280;font-size:12px'>{e(r['detail'])}</span></td>"
            f"<td {cell}>{e(r['close'])}</td><td {cell}>{e(r['base'])}<br>"
            f"<span style='color:#6b7280;font-size:12px'>{e(r['to_stop'])} away</span></td>"
            f"<td {cell}>{e(r['plan'])}</td></tr>")
    ctx = "".join(f"<p style='margin:4px 0;color:#374151'>{e(l)}</p>" for l in _context_lines(latest))
    s = latest.get("summary", {})
    return f"""<!doctype html><html><body style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#111;max-width:720px;margin:0 auto;padding:16px">
<h2 style="margin:0 0 4px">Ignition Watch</h2>
<p style="margin:0 0 16px;color:#6b7280;font-size:13px">data as of <b>{e(str(latest.get('asof')))}</b> · scanned {e(str(latest.get('scanned_at')))}</p>
<table style="border-collapse:collapse;width:100%">
<thead><tr><th {head}></th><th {head}>Ticker</th><th {head}>Close</th><th {head}>Base (sell below)</th><th {head}>Position</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<p style="margin:16px 0 8px;font-size:13px;color:#374151">Rule: buy at the next open; sell at the next open after a close below the
pre-ignition base (the close five sessions before the fire). Dropping off the signal list is not a sell.</p>
{ctx}
<p style="margin:16px 0 0;font-size:13px;color:#6b7280">Ledger: {s.get('open', 0)} open, {s.get('closed', 0)} closed ·
<a href="{e(page_url)}">open the Ignition page</a></p>
</body></html>"""


def send(api_key: str, sender: str, to: str, subj: str, text: str, body_html: str) -> dict:
    """One POST to Resend. Raises on a non-2xx or transport error."""
    payload = json.dumps({"from": sender, "to": [to], "subject": subj, "html": body_html, "text": text}).encode()
    req = urllib.request.Request(RESEND_API_URL, data=payload, method="POST", headers={
        "Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
        "User-Agent": "IgnitionWatch-Notify/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:     # noqa: S310 - fixed https URL
        return json.loads(resp.read().decode() or "{}")


def main(path: Path = LATEST) -> int:
    if not path.exists():
        print(f"notify: {path} missing; nothing to send", file=sys.stderr)
        return 0
    latest = json.loads(path.read_text(encoding="utf-8"))
    items = collect(latest)
    if not items:
        print(f"notify: asof={latest.get('asof')} since={latest.get('summary', {}).get('since')}: no new fire/refire")
        return 0
    page_url = _env("IGNITION_PAGE_URL", DEFAULT_PAGE_URL)
    subj = subject(latest, items)
    text, body_html = render_text(latest, items, page_url), render_html(latest, items, page_url)
    api_key = _env("RESEND_API_KEY")
    if not api_key:
        print(f"notify: RESEND_API_KEY not set; would have sent to {', '.join(recipients())}:\n  {subj}")
        return 0
    sender, failed = _env("IGNITION_NOTIFY_FROM", DEFAULT_FROM), 0
    for to in recipients():
        try:
            r = send(api_key, sender, to, subj, text, body_html)
            print(f"notify: sent to {to} id={r.get('id')}")
        except urllib.error.HTTPError as err:                            # Resend's reason is in the body
            failed += 1
            print(f"notify: FAILED {to}: HTTP {err.code} {err.read().decode(errors='replace')[:300]}", file=sys.stderr)
        except Exception as err:                                         # noqa: BLE001 - report, keep going
            failed += 1
            print(f"notify: FAILED {to}: {err}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
