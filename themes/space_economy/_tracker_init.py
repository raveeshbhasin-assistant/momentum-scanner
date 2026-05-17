"""One-off tracker initializer for Space Economy. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "Boring global subsystem suppliers with real revenue and clean balance sheets will outperform "
    "US pure-play space SPACs over 3-5 years, anchored by Space Force / NRO procurement. Cheap "
    "heavy-lift launch shifts the bottleneck from getting things into orbit to building, connecting, "
    "and supporting them once they're there — rad-hard chips, optical inter-satellite links, ground "
    "stations, and component suppliers capture that durable value, not the headline-grabbing launch "
    "and applications layer. Defense-anchored 70/30. The 'thesis right, vehicles wrong' risk is the "
    "central risk for this theme, not a tail risk — capital structure is weighted accordingly."
)

# Top 7 from scoring v1 (capital_structure weight 20)
# Bucket distribution: 2 Comms/Ground · 2 Components · 1 In-Space Logistics · 2 Analytics
PROMOTED = ["LHX", "IRDM", "MRCY", "NOC", "BAESY", "CACI", "BAH"]

CONVICTION_OVERRIDES = {
    # CACI and BAH both flag Debt scores 1/5 and 2/5 respectively — likely acquisition-funded
    # (CACI's recent Mantech-style deals, BAH's pipeline). Real but worth watching.
    "CACI": 2,  # Lean Trim — heavy debt growth (+73.8% YoY) flags integration risk
}

ENTRY_NOTES = {
    "LHX":   "Surprise #1 by score. Perfect CS 5/5 (buyback + 19.5% debt paydown + 0% SBC + healthy FCF) puts L3Harris ahead of IRDM despite being a larger diluted prime. Space comms is its most pure-play segment. Lower-volatility anchor.",
    "IRDM":  "Thesis flagship pure-play. CS 4/5 with -10% shares (buyback) confirms quality. RS 94 / +80% 3M means we're buying after the move — accept this; the thesis is durability not asymmetric entry. Watch for any SpaceX/Starlink direct-to-cellphone moves that would threaten L-band moat.",
    "MRCY":  "Rad-hard chips anchor for Bucket 1. CS 4/5 (modest dilution, modest SBC, FCF 4/5). Defense electronics with growing space exposure. Higher RS (83) suggests momentum sustained, not asymmetric — but durability of the moat justifies entry.",
    "NOC":   "Only operating commercial in-orbit servicer (via SpaceLogistics MEV) — that's the irreplaceable piece. CS 5/5 from buyback + flat debt + 0% SBC. Diluted overall but the niche position + clean balance sheet earn its spot. Watch MEV revenue trajectory in quarterly disclosure.",
    "BAESY": "European defense prime ADR — buyback culture + clean balance sheet (CS 4/5). RS 50 with 3M -8% pullback is the asymmetric setup we want. Real space exposure with EU defense spending tailwind. Watch ADR liquidity if scaling position.",
    "CACI":  "Defense IT pure-play in intelligence community + EW. **CS 4/5 but Debt 1/5 from heavy +73.8% YoY growth — likely acquisition-funded. Conviction Lean Trim at entry until integration is validated.** Watch next earnings for organic vs acquired revenue split.",
    "BAH":   "Government consulting + space mission support. Strong overall CS but Debt 2/5 (+16% significant) flags similar acquisition pattern as CACI. Cleanest of the three defense IT names on balance sheet quality.",
}


def main():
    cand_path = _HERE / "candidates.json"
    if not cand_path.exists():
        raise FileNotFoundError(f"{cand_path} missing — run refresh_data.py first")
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
        "theme": "space_economy",
        "theme_display_name": "Space Economy",
        "theme_status": "Active",
        "theme_locked_at": "2026-05-16",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 739.17,
            "ita_at_theme_lock": 217.27,
            "lmt_at_theme_lock": 516.01,
            "irdm_at_theme_lock": 41.62,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Space tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
