"""One-off tracker initializer for AI in Healthcare. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "The durable money in healthcare AI accrues to the incumbents that already own the system of "
    "record — the EHR/CRM workflow, the imaging and diagnostics installed base, and the proprietary "
    "molecular-data corpus — not to the pre-revenue 'AI drug discovery' story stocks. AI is the "
    "commodity layer; the distribution it must run inside is the scarce, un-buyable asset. This "
    "basket is real-revenue-weighted and quality-tilted: workflow/data incumbents and imaging owners "
    "form the core, with only a small labeled optionality sleeve in AI-native discovery. The central "
    "risk is 'AI is real but a feature, not a moat' — capital-structure and valuation discipline are "
    "weighted accordingly."
)

# Top 7 from scoring v1 (long-only rubric; capital_structure & valuation weighted for
# commoditisation/dilution risk). Bucket spread:
#   Workflow/Systems-of-record: VEEV, IQV, DOCS
#   Imaging & Diagnostics AI:    GEHC, ISRG
#   Healthcare Data:             TEM, GH
PROMOTED = ["TEM", "VEEV", "GEHC", "IQV", "ISRG", "DOCS", "GH"]

CONVICTION_OVERRIDES = {
    # GH scored top-7 but on peak momentum (RS 100, +240% 1Y) at P/S ~20.7 while still
    # loss-making — the reimbursement path is the swing factor. Lean-Trim at entry.
    "GH": 2,
    # DOCS is the deep-value / turnaround leg — bought after a -62% 1Y derating with growth
    # still stalled and Epic's first-party scribe as an overhang. Lean-Trim until growth reinflects.
    "DOCS": 2,
}

ENTRY_NOTES = {
    "TEM":  "Thesis flagship and the theme's benchmark anchor. Purest expression of the bet: an AI-native multimodal molecular-data corpus that compounds as test volume grows, on the fastest durable revenue growth in the universe (+36% YoY / +83% 2025 headline). Still loss-making, so watch the path to FCF — but the data moat is the point. RS 79 with -42% off the 52w high leaves runway. Top score 79.0.",
    "VEEV": "Core workflow holding. ~80% life-sciences CRM share means AI Agents run inside the record Veeva already owns — the highest bottleneck-specificity name here (5/5). Best-in-class software margins and a -32% 1Y drawdown that reset the multiple to P/E ~34. Watch AI-Agent monetisation (upsell vs. defensive give-away) — that is the thesis's key falsifier. Score 75.0.",
    "GEHC": "Imaging anchor and the deep-value leg. FDA AI-clearance leader (~120 radiology authorizations) on an un-buyable installed scanner base, at the cheapest quality valuation in the basket (P/E ~15.5, P/S ~1.4). The catch: RS 0 — worst momentum in the universe, a falling knife we're catching on valuation + moat. Watch for momentum stabilisation and continued clearance flow. Score 75.0.",
    "IQV":  "Data-and-distribution owner embedding AI across trials and commercial analytics on a proprietary data asset. Healthiest momentum among the incumbents (RS 64, +18.9% 3M) with reasonable valuation (P/E ~26, P/S ~2.1) and room to re-rate. AI accretion is gradual not event-driven — a steady-compounder slot. Score 71.0.",
    "ISRG": "Robotics moat with AI as optionality. The da Vinci/Ion installed base is the irreplaceable piece; AI vision/case analytics is upside on a razor/razorblade lock-in. Strongest durable grower among incumbents (+23% YoY) with best-in-class margins. The tension: premium valuation (P/E ~53, P/S ~14.5) leaves little runway and RS is weak (7). Sized as a quality core, not a bargain. Score 69.0.",
    "DOCS": "The turnaround / deep-value leg — bought after a -62% 1Y derating. Owns the physician distribution AI startups must rent (300k+ clinicians on its AI tools/qtr) and valuation has reset to P/E ~23. But revenue growth stalled to +5% and Epic's first-party scribe is a structural overhang (a re-check trigger: >30% Epic scribe share forces a re-weight). Conviction Lean-Trim until growth reinflects. Score 67.0.",
    "GH":   "AI-native liquid-biopsy data owner on the fastest top-line growth in the universe (+48% YoY). The problem is entry: RS 100 / +240% 1Y / P/S ~20.7 while still loss-making — priced for perfection. The Shield screening reimbursement path is the swing factor. **Conviction Lean-Trim at entry** given the extended run; add on a valuation reset, not here. Score 64.0.",
}


def main():
    cand_path = _HERE / "candidates.json"
    if not cand_path.exists():
        raise FileNotFoundError(f"{cand_path} missing — run _seed_from_universe.py first")
    candidates = {c["ticker"]: c for c in json.loads(cand_path.read_text(encoding="utf-8"))["candidates"]}

    score_path = _HERE / "scoring_log.json"
    scoring = json.loads(score_path.read_text(encoding="utf-8"))["results"] if score_path.exists() else {}

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
            "conviction": CONVICTION_OVERRIDES.get(ticker, 3),
            "thesis_status": "Intact",
            "event_log": [],
        })

    tracker = {
        "theme": "ai_healthcare",
        "theme_display_name": "AI in Healthcare",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "xlv_at_theme_lock": 161.96,
            "tem_at_theme_lock": 60.70,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str), encoding="utf-8")
    print(f"\n=== AI in Healthcare tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
