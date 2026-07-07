"""One-off tracker initializer for Nuclear Renaissance. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "AI power demand converted nuclear from a decarbonization vibe into a state-backed industrial "
    "buildout with hyperscalers as anchor tenants. The most mispriced layer is not the pre-revenue "
    "SMR developers the market is chasing, but the Western fuel cycle — mining, conversion, and above "
    "all enrichment — where every restart, uprate, PPA, and new reactor bids for the same structurally "
    "scarce pounds and SWU. We anchor the book in names with real revenue and contracted backlog today "
    "(enrichment, components, operating fleets, blue-chip miners) and hold pre-revenue SMR stories only "
    "as small, explicitly speculative optionality. Long-only, 3-5 year horizon."
)

# Top 7 from scoring v1 (long-only, capital_structure weight 20)
# Bucket distribution: 1 Enrichment · 1 Mining · 3 OEM/Components · 2 Operators
PROMOTED = ["LEU", "BWXT", "CEG", "CCJ", "MIR", "TLN", "CW"]

CONVICTION_OVERRIDES = {
    # LEU and CCJ both carry rich valuations (V 2/5) after big runs — real bottleneck
    # premium, but we enter measured until the pullbacks confirm.
    "LEU": 4,  # High — the single hardest-to-substitute input in the theme; DOE anchor contract
    "CEG": 4,  # High — operator flagship with the cheapest large-cap valuation + restart proof point
}

ENTRY_NOTES = {
    "LEU":  "Thesis flagship. The 5/5 bottleneck of the entire theme — the sole US-owned enricher, with HALEU barely existing outside Russia. Fresh $900M fixed-price DOE HALEU contract (2026-06-30) on top of a $2.4B LEU backlog. RS 57 with a mild 3M pullback is a reasonable entry after a huge 1Y run. Valuation is rich (P/E 63) — the bottleneck premium is largely priced, so size measured. Watch Piketon milestones and DOE funding in every budget cycle; a slip is the single biggest leg-weakener.",
    "BWXT": "The component moat that wins regardless of whose reactor design prevails. Sole-source naval reactor manufacturer plus the RPV for Darlington's first BWRX-300 plus TRISO/HALEU fuel work — decades of NRC/Navy qualification nobody can replicate quickly. Rev +26% YoY, turning back up (1M +6.6%) near its 52-week high. Watch BWRX-300 component milestones and SMR fuel awards for the commercial ramp on top of the navy anchor.",
    "CEG":  "Operator flagship and the theme's execution proof-point. Largest US unregulated nuclear fleet — a scarce, non-replicable asset — plus the Crane 2027 restart (Microsoft 837 MW) and Meta's 1.1 GW Clinton PPA. P/E 21 is the most reasonable large-cap valuation in the theme after a 3M pullback. Crane restarting on schedule flips the sector narrative from 'nuclear can't build' to 'nuclear executes.' Watch the restart timeline and any additional hyperscaler PPAs.",
    "CCJ":  "The theme's blue chip: tier-1 pounds, UF6 conversion scarcity, and 49% of Westinghouse in one balance sheet — the hardest miner to replace. RS 21 after a -13% 3M cooldown is a low-RS-but-well-positioned entry. Valuation is full on trailing (P/E 93) because the market discounts price upside from the recontracting cycle at ~$85/lb. Watch McArthur River guidance, Kazatomprom output, and the recontracting cadence.",
    "MIR":  "The quiet razor-blade of the theme: design-agnostic dosimetry and monitoring that every reactor, restart, and fuel facility must buy. Rev +27.5% YoY, the strongest steady grower among the OEMs, yet RS 29 / -18% 1Y means it lags the cohort — the underappreciated pick. Trailing P/E is noisy from post-SPAC amortization; P/S 4.2 is reasonable and runway opens if the recurring-service mix scales. Watch new-build and restart instrumentation order flow.",
    "TLN":  "The most concentrated nuclear-to-datacenter expression: Susquehanna feeding Amazon's 1.92 GW / 17-year PPA. Cheapest operator in the theme (P/E 12.6) with real free cash flow and Rev +97% YoY. RS 93 means we buy after a strong run — accept it for the contracted cash flow. The single-site concentration and merchant exposure outside the PPA are the risks; watch the AWS campus buildout and further capacity monetization.",
    "CW":   "Sole-source reactor coolant pumps, valves, and I&C across the operating fleet plus AP1000 — every restart and uprate is aftermarket revenue. Best-in-class defense-industrial margins (M 5/5), Rev +13% YoY. Theme exposure is diluted by the broad defense/industrial portfolio (TE 3/5) and RS 86 near highs means the entry is extended. Watch restart/uprate aftermarket demand and AP1000 order content for the nuclear-specific ramp.",
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
        "theme": "nuclear_renaissance",
        "theme_display_name": "Nuclear Renaissance",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "ura_at_theme_lock": 43.88,
            "ccj_at_theme_lock": 97.50,
            "leu_at_theme_lock": 174.23,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Nuclear Renaissance tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
