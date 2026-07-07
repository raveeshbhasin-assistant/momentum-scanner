"""One-off tracker initializer for Climate Adaptation Infrastructure. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "This is climate ADAPTATION, not mitigation — the physical-risk dollars that flow regardless "
    "of emissions policy as water scarcity, flooding, heat, and wildfire get priced into rate bases, "
    "insurance, and capex budgets. Value accrues to the deepest, most-reimbursed pool (regulated water "
    "utilities and water-treatment tech), the analytics layer that prices physical risk, and — more "
    "cyclically — building-envelope resilience and drought/irrigation ag. We anchor on boring, "
    "regulated, real-revenue compounders over hype. The central risk is not solvency but valuation: "
    "the best water and analytics names are premium-multiple quality compounders."
)

# Top 7 from scoring v1. Bucket distribution:
#   3 Water Infra (AWK, XYL, VLTO) · 1 Analytics (VRSK) · 1 Ag irrigation (LNN)
#   + 2 watching-tier promoted for bucket coverage: WTRG (water) and CSL (building envelope)
PROMOTED = ["AWK", "XYL", "VLTO", "VRSK", "LNN", "CSL", "WTRG"]

CONVICTION_OVERRIDES = {
    # LNN and CSL are riding strong RS (93 / 87) into cyclical earnings troughs (rev -5% / -4% YoY).
    # High conviction on the direction, but the entry is momentum-extended, not asymmetric — lean-trim.
    "LNN": 2,  # small-cap irrigation pure-play; RS 93 entry is extended, revenue in an ag-income trough
    "CSL": 2,  # building-envelope cyclical; RS 87 entry after a strong run, revenue still -4% YoY
}

ENTRY_NOTES = {
    "AWK":  "Score #1. The boring, reimbursed anchor — largest US regulated water utility with a geographic monopoly and a rate base that grows as scarcity and resilience capex get recovered through regulated returns. Deepest, most durable adaptation pool. Watch allowed-ROE in rate cases as the reimbursement signal.",
    "XYL":  "The thesis 'what we actually want' reference — purest large-cap water-tech (pumps, Sensus smart metering, analytics). Off -21.8% from its 52w high with 1M momentum turning (+8.9%), so valuation runway has reopened. Watch PFAS/lead capex cadence and metering-upgrade demand.",
    "VLTO": "Highest-specificity water name — Hach/Trojan/ChemTreat water-quality instruments plus a recurring consumables annuity (razor-and-blade). RS 67 with building momentum. The consumables stream scales with every new plant and tighter standard. Watch consumable pull-through as standards tighten.",
    "VRSK": "The sole vehicle that literally prices the theme — catastrophe modeling embedded in insurer underwriting with enormous switching costs. A -38% 1Y drawdown restored valuation runway on a very high-margin subscription franchise. Watch subscription growth and any signal of open-model/AI commoditization (the key falsifier for this bucket).",
    "LNN":  "Purest irrigation pure-play — Zimmatic center-pivot hardware, the literal water-delivery layer for drought-stressed farms. **RS 93 with revenue -5% YoY in an ag-income trough: high conviction on direction, but entry is momentum-extended — Lean Trim.** Watch farm income and drought-year pivot demand.",
    "CSL":  "Best building-envelope proxy — commercial roofing + waterproofing membranes, the layer that keeps storms and water out; re-roof demand is non-discretionary. **RS 87 after a strong run with revenue still -4% YoY on the non-res cycle — Lean Trim at entry.** Watch the re-roof cycle and storm-driven remediation demand.",
    "WTRG": "Second regulated-water utility — Aqua America water rate-base growth (strongest utility revenue at +10% YoY) with a Peoples gas segment that dilutes purity. Cheapest of the water utilities (P/E ~20). Watch water rate cases and whether the gas overhang narrows or widens.",
}


def main():
    cand_path = _HERE / "candidates.json"
    if not cand_path.exists():
        raise FileNotFoundError(f"{cand_path} missing — run _seed_from_universe.py first")
    candidates = {c["ticker"]: c for c in json.loads(cand_path.read_text())["candidates"]}

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
            "conviction": CONVICTION_OVERRIDES.get(ticker, 3),
            "thesis_status": "Intact",
            "event_log": [],
        })

    tracker = {
        "theme": "climate_adaptation",
        "theme_display_name": "Climate Adaptation Infrastructure",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "pho_at_theme_lock": 69.84,
            "xyl_at_theme_lock": 119.42,
            "awk_at_theme_lock": 133.09,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Climate Adaptation tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
