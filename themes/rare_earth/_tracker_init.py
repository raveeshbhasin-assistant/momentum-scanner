"""One-off tracker initializer for Rare Earth & Critical Minerals. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "US/allied industrial policy — Defense Production Act awards, IRA 45X production credits, and "
    "DoD direct equity plus price floors — is rebuilding critical-mineral supply chains outside China "
    "for defense, EVs, robotics, and AI. The bottleneck isn't mining (the West has plenty of ore); "
    "it's the MIDSTREAM separation and magnet-making step China dominates and the West is now "
    "subsidizing. Durable value accrues to the few US/allied names with funded, permitted processing "
    "plus offtake (MP, USAR) and to the copper/diversified anchors riding structural electrification "
    "demand; junior explorers are lottery tickets sized small. The 'thesis right, vehicles wrong' and "
    "junior-dilution risks are central, so capital structure is weighted accordingly."
)

# Top 7 from scoring v1 (long-only, capital_structure weight 20)
# Bucket distribution: 3 Separation & Magnets (MP, USAR, UUUU) · 4 Diversified/Copper (FCX, TECK, SCCO, VALE)
# Note: the pure-play midstream flagships (MP, USAR) carry the thesis; the copper
# anchors scored high on RS + valuation + real cash flow and provide the stable base.
PROMOTED = ["MP", "FCX", "TECK", "SCCO", "USAR", "VALE", "UUUU"]

CONVICTION_OVERRIDES = {
    # USAR is the purest magnet vehicle but pre-cash-flow with an extreme valuation
    # (P/S ~645) — high thesis fit, high blow-up risk. Sized as a conviction-2 sleeve.
    "USAR": 2,  # Lean Trim — pre-revenue, valuation entirely story-priced
    # UUUU cooling hard (RS 8, -22% 3M) and diluted by uranium/vanadium — watch the knife.
    "UUUU": 2,  # Lean Trim — momentum broken, RE-separation optionality unproven
}

ENTRY_NOTES = {
    "MP":   "Thesis flagship and #1 by score. The only US name with the funded, permitted full chain — Mountain Pass separation + Fort Worth magnets — plus the 2025 DoD equity stake, ~$110/kg NdPr price floor, and 10-year magnet offtake. The price floor structurally caps the single biggest risk in mining equities (commodity-price volatility) while the reshored volume ramps. Rich on P/S (~27) — the runway is volume and qualification milestones, not multiple expansion. Watch for a second DoD-template deal (confirms a program) and any dilutive government-equity conversion.",
    "FCX":  "Copper anchor. Largest liquid US-listed copper pure-play, leveraged to the electrification / EV / AI-datacenter-power demand that underpins the whole critical-minerals story. Best relative strength in the anchor sleeve (RS 75, only -16% from 52w high) with real cash flow and a disciplined balance sheet. Lower on the RE-reshoring axis but the structural copper-supply tightness is its own durable thesis. The stable base of the tracker.",
    "TECK": "Strongest momentum in the entire universe (RS 92, +16.5% 3M, +72% YoY revenue as QB2 copper ramps). Post-coal-spin transformation into a copper-growth story gives real, re-rating base-metal exposure at a reasonable multiple (P/E ~23). Diversified rather than pure-play RE, but the copper-transition narrative and possible M&A interest earn its spot. Watch the QB2 ramp trajectory.",
    "SCCO": "Lowest-cost, longest-reserve-life copper producer — best-in-class margins and a structural copper-demand anchor. Grupo Mexico control caps the float and the premium valuation (P/S ~10) limits multiple runway, so it's a quality-and-margin hold rather than a re-rating play. Complements FCX/TECK as the low-cost end of the copper sleeve.",
    "USAR": "Purest US magnet + heavy-RE vehicle — Stillwater magnet plant + Round Top HREE, no dilution from other businesses. Highest thesis fit alongside MP. But pre-cash-flow with an extreme valuation (P/S ~645, P/E ~580) and RS 100 means we're buying after a hot run — entirely optionality-priced. **Conviction Lean Trim (2) at entry until commissioning + first OEM magnet qualification are proven.** The key research item is cash runway vs. commissioning timeline.",
    "VALE": "Deep-value diversified anchor. Nickel and copper give real critical-metal exposure, though iron ore dominates the P&L. P/S ~0.3, P/E ~23, high dividend — cheap and resilient (RS 50, only -6% 3M). Diluted reshoring specificity, but the valuation cushion and yield make it a low-blow-up-risk anchor. Watch iron-ore price and Brazil policy.",
    "UUUU": "The only US mill (White Mesa) doing both uranium and operating RE separation from monazite — genuine crossover optionality on the exact separation bottleneck. But momentum is broken (RS 8, -22% 3M) and the RE story is diluted by uranium/vanadium and not yet proven at scale. **Conviction Lean Trim (2) — track the RE-separation offtake progress; treat as speculative until the separation revenue is real.**",
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
        "theme": "rare_earth",
        "theme_display_name": "Rare Earth & Critical Minerals",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "xme_at_theme_lock": 106.08,
            "mp_at_theme_lock": 53.01,
            "alb_at_theme_lock": 133.80,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Rare Earth tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
