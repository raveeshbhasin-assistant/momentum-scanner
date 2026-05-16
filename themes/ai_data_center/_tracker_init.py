"""
One-off tracker initializer for AI Data Center theme.

Takes the 7 names promoted from scoring.md, anchors entry prices from today's
candidates.json, writes:
  - tracker.json   (anchor data — entry $, date added, conviction, notes; survives across runs)
  - tracker.md     (human-readable view — regenerated from tracker.json)

After this initial run, the tracker becomes a live view. The web layer
(Stage 4 in DEFERRED.md) will fetch current prices and overlay them; the
tracker.json stays as the static anchor data.

To add/remove a name post-initial: edit tracker.json directly (manual) or
extend this script. Don't quietly edit entry $ — that's the contract.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Plain-language thesis summary ─────────────────────────
# Displayed at the top of the tracker page. Editable independent of the
# locked thesis.md — this is for human display, not the contract.
PLAIN_SUMMARY = (
    "The AI race has moved from chips to physics. As hyperscalers race to build "
    "gigawatt-scale data centers, the binding constraints have shifted from GPUs to "
    "power distribution, optical networking, and liquid cooling. We're betting that the "
    "boring infrastructure suppliers — heavy electrical equipment, optical interconnect, "
    "and thermal management — outperform flashier picks like pre-revenue nuclear and "
    "pure-play IPPs over the next 12-24 months."
)

# ─── The 7 promoted names ─────────────────────────────────
# 2026-05-16: Swapped GEV → ECL based on supply_chain.md addendum R3+R4 findings.
# GEV is only ~28% AI-relevant revenue (gas turbines dominate). ECL is acquiring CoolIT
# for $4.75B (Q3 2026 close), becoming the cleanest direct-to-chip cooling vehicle.
PROMOTED = ["ETN", "CRDO", "ECL", "ANET", "FN", "MOD", "VRT"]

# Optional per-name conviction overrides. Default = 3 (hold/normal).
# Use 5 (increase) or 1 (trim) only when there's a specific reason at entry.
CONVICTION_OVERRIDES = {}

# Optional per-name notes at entry — captures the "why this one specifically"
# beyond what's in scoring.md
ENTRY_NOTES = {
    "ETN":  "Thesis flagship. Low RS (22) + positive 1M = exactly the asymmetric setup the thesis was built to find. Watch backlog disclosures each quarter as the key re-check trigger.",
    "CRDO": "201% YoY revenue growth + RS inflection sweet spot (RS 57 + positive 1M). High valuation is the main risk — would trim aggressively if growth decelerates.",
    "ANET": "Pure-play DC switching + RS 17 (low) creates asymmetric setup, but 1M -11.8% means we're entering during a breakdown. Position should be smaller until 1M turns positive.",
    "ECL":  "Becoming the cleanest public-market direct-to-chip cooling play via $4.75B CoolIT acquisition (closes Q3 2026). Entered at -19.7% off 52w high after deal-related sell-off — asymmetric setup. RS 0 looks bad but is deal-mechanics noise, not thesis breakdown. Watch for deal close + first-quarter integration commentary.",
    "FN":   "Optical contract manufacturer; high theme purity but margin structure is thin. RS 61 + positive 1M. Customer concentration (NVDA, CIEN) is the key risk.",
    "MOD":  "Thermal management bucket coverage. Airedale DC pure-play growing fast; rest is HVAC. RS 35 + 1M +15.3% = classic inflection. Smallest cap in the tracker — size accordingly.",
    "VRT":  "Highest theme exposure in the entire cohort + spans buckets 2 and 3. Highest concentration risk. Already extended (RS 74, 1M +26%) — entered at near 52w highs. Trim if it gives back >15% from entry.",
}


def main():
    # Load candidates.json for entry prices and metadata
    cand_path = _HERE / "candidates.json"
    if not cand_path.exists():
        raise FileNotFoundError(f"{cand_path} missing — run refresh_data.py first")
    candidates = {c["ticker"]: c for c in json.loads(cand_path.read_text())["candidates"]}

    # Load scoring_log.json for scores
    score_path = _HERE / "scoring_log.json"
    scoring = json.loads(score_path.read_text())["results"] if score_path.exists() else {}

    today = datetime.now().strftime("%Y-%m-%d")
    holdings = []
    for ticker in PROMOTED:
        c = candidates.get(ticker)
        if c is None:
            print(f"WARNING: {ticker} not in candidates.json — skipping")
            continue
        s = scoring.get(ticker, {})
        holdings.append({
            "ticker": ticker,
            "company": c.get("company"),
            "bucket": c.get("bucket"),
            "sub": c.get("sub"),
            "date_added": today,
            "entry_price": c.get("price"),
            "entry_market_cap": c.get("market_cap"),
            "entry_pe": c.get("pe"),
            "entry_ps": c.get("ps"),
            "entry_1y_pct": c.get("1y_pct"),
            "entry_3m_pct": c.get("3m_pct"),
            "entry_1m_pct": c.get("1m_pct"),
            "entry_rs_3m": c.get("rs_3m"),
            "entry_dist_from_52w_high_pct": c.get("dist_from_52w_high_pct"),
            "scoring_total": s.get("normalized_100"),
            "scoring_rationale": ENTRY_NOTES.get(ticker, ""),
            "conviction": CONVICTION_OVERRIDES.get(ticker, 3),  # 1=trim 3=hold 5=increase
            "thesis_status": "Intact",                            # Intact | Watching | Broken
            "event_log": [],                                      # {date, type, note}
        })

    tracker = {
        "theme": "ai_data_center",
        "theme_display_name": "AI Data Center Build-Out",
        "theme_status": "Active",
        "theme_locked_at": "2026-05-11",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            # Pulled from thesis.md anchor block
            "spy_at_theme_lock": 739.30,
            "xlu_at_theme_lock": 45.14,
            "smh_at_theme_lock": 576.31,
            "etn_at_theme_lock": 419.00,
            "ceg_at_theme_lock": 299.69,
        },
        "holdings": holdings,
        "review_cadence": "monthly first, quarterly after 3 months",
    }

    # Save tracker.json
    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))

    # Render tracker.md
    md = render_md(tracker)
    (_HERE / "tracker.md").write_text(md)

    # Print summary
    print(f"\n=== Tracker initialized — {today} ===")
    print(f"Theme: ai_data_center  |  Status: Active  |  Locked: {tracker['theme_locked_at']}")
    print(f"Holdings: {len(holdings)} names\n")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  "
              f"RS 3M {h['entry_rs_3m']:.0f}  conv {h['conviction']}  {h['bucket']}")
    print(f"\nSaved tracker.json and tracker.md")


def render_md(t: dict) -> str:
    L = []
    L.append("# AI Data Center Build-Out — Tracker")
    L.append("")
    L.append(f"**Theme status:** {t['theme_status']}")
    L.append(f"**Theme locked:** {t['theme_locked_at']}")
    L.append(f"**Tracker initialized:** {t['tracker_initialized_at']}")
    L.append(f"**Last reviewed:** {t['last_reviewed_at']}")
    L.append(f"**Review cadence:** {t['review_cadence']}")
    L.append("")
    L.append("> Anchor data (`tracker.json`) is the contract. Entry prices, date added, and notes do not get quietly edited.")
    L.append("> Conviction and thesis-status updates happen via dated event-log entries below the holdings table.")
    L.append("")
    L.append("---")
    L.append("")

    # Benchmarks
    b = t["benchmarks_at_init"]
    L.append("## Benchmarks at theme lock (2026-05-11)")
    L.append("")
    L.append(f"- **SPY:** ${b['spy_at_theme_lock']:.2f}")
    L.append(f"- **XLU:** ${b['xlu_at_theme_lock']:.2f}  ·  **SMH:** ${b['smh_at_theme_lock']:.2f}")
    L.append(f"- **ETN** (key candidate): ${b['etn_at_theme_lock']:.2f}  ·  **CEG** (over-hype reference): ${b['ceg_at_theme_lock']:.2f}")
    L.append("")
    L.append("---")
    L.append("")

    # Holdings table
    L.append("## Holdings — 7 names")
    L.append("")
    L.append("| Ticker | Company | Bucket | Added | Entry $ | Score | RS 3M @ entry | Conviction | Thesis |")
    L.append("|--------|---------|--------|-------|---------|-------|---------------|------------|--------|")
    for h in t["holdings"]:
        L.append(
            f"| **{h['ticker']}** | {h['company']} | {h['bucket']} | {h['date_added']} | "
            f"${h['entry_price']:.2f} | {h['scoring_total']:.1f} | {h['entry_rs_3m']:.0f} | "
            f"{h['conviction']} | {h['thesis_status']} |"
        )
    L.append("")
    L.append("_Conviction: 5 = increase / 4 = lean increase / 3 = hold / 2 = lean trim / 1 = trim._  ")
    L.append("_Thesis status: Intact / Watching (a falsifier is showing early signs) / Broken (a falsifier has fired — sell or addendum)._  ")
    L.append("")
    L.append("---")
    L.append("")

    # Per-name detail
    L.append("## Per-name detail")
    L.append("")
    for h in t["holdings"]:
        L.append(f"### {h['ticker']} — {h['company']}")
        L.append("")
        L.append(f"**Bucket:** {h['bucket']} · **Sub:** {h.get('sub','—')}  ")
        L.append(f"**Date added:** {h['date_added']} at **${h['entry_price']:.2f}**  ")
        L.append(f"**Scoring rank:** {h['scoring_total']:.1f} / 100")
        L.append("")
        L.append(f"**Why this name:**  ")
        L.append(f"_{h['scoring_rationale']}_")
        L.append("")
        L.append("**Entry-day snapshot:**  ")
        L.append(f"Mkt cap: ${(h.get('entry_market_cap') or 0)/1e9:.1f}B · "
                  f"P/E {h.get('entry_pe') or '—'} · P/S {h.get('entry_ps') or '—'} · "
                  f"1Y {h.get('entry_1y_pct') or '—'}% · 3M {h.get('entry_3m_pct') or '—'}% · "
                  f"1M {h.get('entry_1m_pct') or '—'}% · "
                  f"RS 3M {h.get('entry_rs_3m') or '—'} · "
                  f"Δ 52wH {h.get('entry_dist_from_52w_high_pct') or '—'}%")
        L.append("")
        L.append("**Event log:**")
        L.append("")
        if h["event_log"]:
            for ev in h["event_log"]:
                L.append(f"- _{ev.get('date','—')}_ — {ev.get('type','')}: {ev.get('note','')}")
        else:
            L.append("- _No events yet. First post-entry update goes here._")
        L.append("")
        L.append("---")
        L.append("")

    # Theme-level review section
    L.append("## Theme-level review")
    L.append("")
    L.append(f"**Most recent checkpoint:** {t['last_reviewed_at']} (initial — no review yet)")
    L.append("")
    L.append("**Falsifier checklist** (from thesis.md — review each at every checkpoint):")
    L.append("")
    L.append("- [ ] Algorithmic efficiency leaps (SLM / architectural breakthroughs reducing compute/power need)")
    L.append("- [ ] AI bubble bursts (hyperscaler capex cuts for 2+ consecutive quarters)")
    L.append("- [ ] Thesis right, vehicles wrong (private capture / fixed-price contracts squeezing public margins)")
    L.append("- [ ] Supply chain snarls become terminal (specialty steel / copper shortage stalling construction)")
    L.append("")
    L.append("**Performance vs benchmarks** (filled in at first checkpoint):")
    L.append("")
    L.append("- Tracker average vs SPY since theme lock: _TBD_")
    L.append("- Tracker average vs sector proxy (XLU+SMH): _TBD_")
    L.append("- Outperformance vs SPY: _TBD_")
    L.append("")
    L.append("**Names promoted in or trimmed since last review:** none yet")
    L.append("")
    L.append("**Forward outlook:** _TBD at first review_")
    L.append("")
    L.append("---")
    L.append("")
    L.append("_Data source: anchor data in `tracker.json`. Live current prices will be overlaid by the tracker page once the web layer (DEFERRED.md Stage 4) is built. For now, this markdown is the static view._")
    L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    main()
