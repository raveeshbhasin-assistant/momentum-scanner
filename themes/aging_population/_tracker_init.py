"""One-off tracker initializer for Aging Population Infrastructure. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "The most demographically certain trend in markets: the US 65+ cohort grows for decades and "
    "already exists, while the supply of senior housing, home-care labor, and age-specific medical "
    "devices stays structurally short. Value accrues to senior-housing REITs riding an occupancy-and-"
    "rate recovery against a near-frozen new-supply pipeline, to the ortho/cardio/diabetes device "
    "makers whose volumes scale mechanically with the cohort, to hearing/vision franchises, and to "
    "home-health operators. Long-only, boring demographic-certainty compounders — with the REIT "
    "sleeve's interest-rate sensitivity as the central risk to watch."
)

# Top 7 from scoring v1 (long-only rubric). Spans all four buckets:
# 4 Devices (EW, SYK, ZBH, ISRG) · 1 REIT (WELL) · 1 Home Health (ADUS) · 1 Hearing/Vision (ALC)
PROMOTED = ["EW", "WELL", "SYK", "ADUS", "ZBH", "ALC", "ISRG"]

CONVICTION_OVERRIDES = {
    # WELL is the flagship but near a 52w high on a ~112 P/E — full conviction on the thesis,
    # but flag the rate sensitivity + rich entry. Kept at 3 (Standard).
    # ADUS is a small-cap with thin services margins — Standard, not elevated.
    # ISRG remains premium-priced even after a 28% derating — hold at Standard.
}

ENTRY_NOTES = {
    "EW":   "Top score. TAVR near-monopoly on aortic stenosis — the tightest cohort-linkage device (5/5) with strong margins and, unusually for the sleeve, positive momentum (RS 88, near 52w high). Structural-heart volumes expand as indications broaden to lower-risk patients. Watch trial readouts and any competitive TAVR entrant.",
    "WELL": "Thesis flagship demand vehicle. Highest operating leverage to the senior-housing occupancy-and-rate recovery against a decade-low supply pipeline; rev +38% YoY from SHOP consolidation. The catch: P/E ~112 and near a 52w high, plus REIT rate sensitivity — the sleeve's central falsifier. Buying quality at a full price; watch same-store NOI and the rate regime.",
    "SYK":  "Device anchor. Joint-replacement volumes are a near-mechanical function of the cohort, and Mako robotics deepens the moat. Down ~16% on the year despite the tailwind — a forgiving entry on a durable compounder. Near-term rev soft (+2.6%); the bet is procedure-volume recovery, not this quarter.",
    "ADUS": "Purest aging-in-place vehicle — personal-care labor is the structurally short supply the thesis is built on. Cheap (P/E ~20) with real revenue and healthy momentum (RS 82). Margins are thin and Medicaid-funded, so watch rate updates; sized as a small-cap, not a cornerstone.",
    "ZBH":  "The most cohort-levered large ortho name and the cheapest device pick (P/E ~23). Pure hips/knees exposure, beaten down (-16% off 52w high) alongside SYK. Pricing pressure caps the margin score; the value case rests on the demographic volume recovery.",
    "ALC":  "Global #1 in surgical eyecare — cataract IOLs address one of the most demographically certain procedures on earth. Lagged the market (-21% 1Y), giving a reasonable entry (P/E ~41) on a franchise leader. Watch premium-IOL mix and elective-procedure timing.",
    "ISRG": "da Vinci install-base monopoly with a razor-blade instrument model and an older-skewing procedure mix; +23% rev growth. Derated ~28% off its 52w high, but still premium (P/E ~53) — the lowest-conviction of the seven on entry multiple. Held for the durability of the moat, not the entry price.",
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
        "theme": "aging_population",
        "theme_display_name": "Aging Population Infrastructure",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "xlv_at_theme_lock": 161.96,
            "well_at_theme_lock": 232.69,
            "syk_at_theme_lock": 324.73,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str), encoding="utf-8")
    print(f"\n=== Aging Population tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
