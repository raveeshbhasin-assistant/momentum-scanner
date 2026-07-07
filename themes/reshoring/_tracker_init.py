"""One-off tracker initializer for Reshoring & Industrial Renaissance. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "The US is running its largest manufacturing-construction cycle since the 1970s — CHIPS Act, "
    "IRA, and a durable tariff regime have roughly tripled real construction spending on factory "
    "structures. The bet is that value accrues to the enablers of the build-out, not the "
    "end-manufacturers: the E&C contractors that build the fabs, the automation that equips them, "
    "the local-monopoly aggregates that go into every slab, and the industrial REITs that landlord "
    "the footprint. We screen on backlog visibility and pricing power, and weight capital structure "
    "heavily because several enablers carry acquisition-funded debt."
)

# Top 7 from scoring v1 (capital_structure weight 20).
# Bucket distribution: 3 Materials · 2 E&C · 2 Automation. No REIT in top 7 —
# REITs are the medium-priority, rate-pressured sleeve, consistent with the thesis ranking.
PROMOTED = ["MLM", "EME", "ROK", "VMC", "CRH", "PWR", "PH"]

CONVICTION_OVERRIDES = {
    # PWR earns a spot on backlog visibility but P/E ~93 means the narrative is fully in the price —
    # size it as a Lean Trim entry until a multiple reset or a fresh megaproject-award catalyst.
    "PWR": 2,
}

ENTRY_NOTES = {
    "MLM":   "#1 by score. Aggregates local-monopoly (crushed stone is uneconomic to truck >~50mi) delivers structural above-inflation pricing — the most under-appreciated leg of the thesis. CS is clean (buyback + flat debt + FCF 13.3%). RS 6 means it lags the cohort, which is the asymmetric entry we want on a quality name. Watch VMC/MLM average-selling-price trend as the pricing-power tell.",
    "EME":   "Best risk/reward in the E&C bucket. Record mechanical + electrical backlog with direct factory/data-center exposure, buyback culture, and the most reasonable multiple among the growth contractors (P/E ~27 vs 50-93 for peers). RS 24 (cooled off) is a constructive entry. Watch book-to-bill staying above 1.",
    "ROK":   "The purest US factory-automation monetization name — installed-base + software attach lock-in, ~49% gross margin. Equips reshored plants as they come online. Overlaps AI-DC/robotics themes, so treat as the shared sleeve. RS 94 / +33% 3M means we're buying after a re-rate; accept it for the durability of the moat. Watch automation-capex order cadence.",
    "VMC":   "Largest US aggregates producer — the flagship pricing-power name alongside MLM. Local-monopoly quarries feed every slab, road, and pad in the build-out. Clean CS (buyback + debt paydown). Volume tracks the multi-year construction cycle while pricing compounds independently. Watch annual price resets and shipment volumes.",
    "CRH":   "The value pick of the materials bucket — largest US materials footprint (aggregates + cement + building products) at the cheapest quality multiple in the cohort (P/E ~20). NYSE-primary listing has been pulling in US institutional ownership. Diversified, scaled exposure to the same build-out as VMC/MLM. Watch US organic volume growth.",
    "PWR":   "E&C flagship on backlog visibility — largest specialty electrical/utility contractor with a scarce skilled-labor moat and multi-year backlog on grid + factory power tie-ins. **Conviction Lean Trim at entry: P/E ~93 means the reshoring narrative is fully in the price — size cautiously until a multiple reset or fresh megaproject-award catalyst.** Watch book-to-bill and megaproject conversion.",
    "PH":    "Motion, fluid power, and filtration — the plumbing of automated factories, with broad content per plant and aerospace-quality ~37% gross margin. Cleanest capital structure among the automation names (buyback + debt paydown + FCF 13.2%). RS 41 is mid, not extended — a reasonable entry. Watch factory motion/filtration content growth as reshored plants equip.",
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
        "theme": "reshoring",
        "theme_display_name": "Reshoring & Industrial Renaissance",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "xli_at_theme_lock": 185.56,
            "pwr_at_theme_lock": 674.04,
            "rok_at_theme_lock": 482.87,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Reshoring tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
