"""One-off tracker initializer for Power Grid Modernization. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "The largest US transmission & distribution capex cycle in generations — driven by "
    "electrification, AI data-center load, renewables interconnection, and aging infrastructure — "
    "forces durable value into the heavy electrical equipment oligopoly and the specialized T&D "
    "construction contractors, not the utilities that spend the money. The physical bottleneck is "
    "transformers with multi-year lead times; the labor bottleneck is scarce high-voltage crews, so "
    "equipment makers with pricing power and contractors with backlog + workforce moats capture the "
    "value. This is the broader grid bet (overlaps the AI Data Center theme on equipment names, but "
    "leans its incremental breadth into the contractor and grid-edge cohorts). The central risk here "
    "is valuation — several names have re-rated hard on the AI-power narrative — so the rubric weights "
    "valuation discipline accordingly."
)

# Top 7 from scoring v1 (valuation_runway weight 20)
# Bucket distribution: 3 Heavy Electrical · 2 T&D Contractors · 1 Grid-Edge · (MYRG 2nd contractor)
PROMOTED = ["ETN", "GEV", "PWR", "ITRI", "EME", "HUBB", "MYRG"]

CONVICTION_OVERRIDES = {
    # GEV scored top-3 but on a P/S ~7.9 after +118% 1Y — valuation is the whole risk.
    # Lean Trim at entry: own it for the purest grid-equipment exposure, but size it as
    # harvest-into-strength, not a full core position.
    "GEV": 2,   # Lean Trim — richest valuation among the equipment anchors
    # ITRI is the contrarian value entry (RS 8, 1Y -35%). Real thesis exposure at the cheapest
    # multiple in the universe, but momentum is broken — half-conviction until it stabilizes.
    "ITRI": 2,  # Lean Trim — cheap + on-thesis but momentum broken; wait for stabilization
}

ENTRY_NOTES = {
    "ETN":  "Score #1 (79). Flagship heavy-electrical anchor — switchgear/breakers/UPS oligopoly with massive backlogs and structural pricing power. RS 38 with +14.8% 3M is constructive, not extended, so the entry is the least stretched of the equipment names. P/E ~41 is full but backlog visibility justifies it. Core position. Watch: any sign transformer/switchgear lead times are clearing (the falsifier).",
    "GEV":  "Score #2 (75). The purest public grid-equipment expression — transformers, HVDC, grid orchestration. This is the single most on-thesis name, but it's also the richest: P/S ~7.9 after +118% 1Y prices continued perfection. **Conviction Lean Trim — own it for the exposure, size it as harvest-into-strength, not full core.** Watch FERC transmission-reform award flow and any AI-capex air-pocket that would deflate the demand stack.",
    "PWR":  "Score #3 (71). Flagship T&D-construction contractor — largest backlog, deepest trained workforce, the labor-bottleneck thesis made concrete. P/E ~93 is the richest in the cohort; the only justification is backlog-driven earnings visibility. Core position but valuation-sensitive. Watch book-to-bill and any commentary that labor availability (not demand) is the binding constraint — that's the upside re-check trigger.",
    "ITRI": "Score #4 (70). The cleanest grid-edge pure-play (smart meters + distribution automation) and NOT in the AI DC theme — genuine incremental breadth. Cheapest quality name in the universe (P/E ~14) but deeply out of favor (RS 8, 1Y -35%, rev -3.3%). **Conviction Lean Trim — contrarian value entry; the drawdown is either the opportunity or a value trap.** Watch for revenue stabilization + smart-meter refresh-cycle commentary before adding.",
    "EME":  "Score #5 (68). Best execution and margin discipline of the contractor group — broad mechanical/electrical construction with real grid leverage. Low-drama compounder: RS 23 but stable, P/E ~26 reasonable for the quality, +19.7% rev. The 'boring' contractor that carries the least valuation froth. Core position. Watch electrical-construction backlog cadence.",
    "HUBB": "Score #6 (67), first watch name. More T&D-utility-levered than most equipment names, and the most reasonably valued of the big-3 equipment names (P/E ~29). RS 15 means it cooled off hard — a constructive entry if you believe the grid-component demand cadence holds. Watch utility capex guidance revisions.",
    "MYRG": "Score #7 (67), second watch name. The cleanest near-pure-play T&D-construction contractor and least-diluted of the contractor cohort (highest specificity). RS 100 / +55% 3M means top-of-cohort momentum — extended, so this is the one to buy on a pullback rather than chase. Watch utility MSA renewals and backlog.",
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
        "theme": "power_grid",
        "theme_display_name": "Power Grid Modernization",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "xlu_at_theme_lock": 45.30,
            "grid_at_theme_lock": 187.42,
            "etn_at_theme_lock": 413.42,
            "pwr_at_theme_lock": 674.04,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Power Grid tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
