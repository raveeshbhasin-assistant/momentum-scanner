"""One-off tracker initializer for Cybersecurity for Critical Infrastructure.
Promotes the top priceable names from scoring v1.

Note on CYBR: it scored #5 (71.0) on judgment fields but did NOT price in this
build environment (yfinance data-source gap — valid live NASDAQ ticker, FMP will
resolve it on the operator's nightly refresh). A tracker holding requires a real
entry_price / market_cap / multiples, so CYBR is NOT promoted here. It is flagged
in PLAIN_SUMMARY as a would-be top-5 holding awaiting its first priced refresh.
We therefore promote the top 6 fully-priced names (which also happen to span all
three major buckets)."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "Cybersecurity for critical infrastructure stops being discretionary and becomes a regulated, "
    "non-negotiable operating cost over the next 3-5 years — driven by converging OT/IT networks, "
    "nation-state attacks on utilities and pipelines, new CISA/TSA/SEC regulation, and the NIST "
    "post-quantum-crypto migration cycle. The durable value accrues to the FCF-compounding platform "
    "consolidators (endpoint/cloud/edge) and the OT/ICS specialists with sticky critical-infrastructure "
    "installed bases — not the story stocks, and not the thin post-quantum pure-plays. "
    "Long-only. Capital structure is weighted heavily because the platform names carry premium "
    "multiples and the central risk is 'great company, no margin of safety.' "
    "(CYBR scored top-5 on merit but did not price in this build's yfinance-only seed; it is a "
    "would-be holding pending the first FMP-backed refresh and is not yet in the tracker.)"
)

# Top 6 fully-priced names from scoring v1 (CYBR scored #5 but is unpriced — see note above).
# Bucket distribution: 3 Platform · 2 OT/ICS Specialists (ZS is platform; CHKP+TENB are specialists) · PANW platform.
# Effective spread: 3 Platform/Endpoint/Cloud (ZS, CRWD, FTNT, PANW) + 2 OT/ICS (CHKP, TENB).
PROMOTED = ["ZS", "CHKP", "CRWD", "FTNT", "PANW", "TENB"]

CONVICTION_OVERRIDES = {
    # CRWD and PANW are excellent businesses but carry the theme's central 'no margin of safety' risk
    # (P/E 128 / 311, both after huge 3M runs). Lean Trim at entry until a better multiple appears.
    "CRWD": 2,
    "PANW": 2,
}

ENTRY_NOTES = {
    "ZS":   "Co-top score (76). The cheapest premium platform after a brutal -52% 1Y reset (RS 21) — the asymmetric entry we want. Pure-play zero-trust SASE is core infrastructure for OT/IT convergence (secures remote access into plants). Valuation runway 4/5 (P/E ~33, P/S ~8) is the best in the platform group. Watch billings re-acceleration as the reset unwinds.",
    "CHKP": "Co-top score (76) and the balance-sheet anchor of the theme. The 'boring FCF machine' — deeply embedded firewalls, best-in-class margins, strong buyback, and the cheapest quality multiple in the universe (P/E ~14, P/S ~5) after a -37.7% 1Y reset. Grows slowly (~5%), so it earns its spot on capital-structure quality and switching cost, not growth. Watch for structural share loss to the platforms.",
    "CRWD": "The quality-compounder flagship — single-agent Falcon platform with the deepest data-gravity moat and best module-attach economics in the group (specificity 5/5, growth ~26%). **Held at Lean Trim conviction: P/E ~128 / P/S ~40 after a +99.8% 3M run is the theme's 'no margin of safety' risk in one name.** Add on any multiple reset, not at the top.",
    "FTNT": "The real critical-infrastructure angle among the profitable platforms — FortiGate has a large embedded OT/ICS installed base inside industrial and utility networks (specificity 5/5), and it's the best cash generator of the platforms. Full multiple (P/E ~63) after a +96.7% 3M run tempers the size. Watch that OT-franchise growth is holding, not masked by a firewall refresh cycle.",
    "PANW": "The platformization playbook flagship — Prisma + XSIAM module-attach lock-in plus firewalls that give genuine OT/network adjacency into critical infra (growth ~31%, real FCF). **Held at Lean Trim conviction: P/E ~311 / P/S ~28 after a +119% 3M run is peak-momentum pricing.** Structurally core to the thesis; size up only on a reset.",
    "TENB": "The closest holdable proxy to a pure OT-visibility play — Tenable OT is embedded in critical-infra asset inventories (specificity 4/5, theme exposure 5/5). Reasonable multiple (P/E ~19, P/S ~4.5) for the franchise even after a +134.9% 3M run (RS 100 — extended, watch for a pullback entry). Watch the ~10% revenue growth for re-acceleration; deceleration is the main risk here.",
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
        if c.get("price") is None:
            print(f"WARNING: {ticker} has no price — skipping (not tracker-eligible)")
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
        "theme": "cyber_infrastructure",
        "theme_display_name": "Cybersecurity for Critical Infrastructure",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "cibr_at_theme_lock": 92.91,
            "crwd_at_theme_lock": 199.38,
            "panw_at_theme_lock": 357.53,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Cyber Infrastructure tracker initialized — {today} ===")
    for h in holdings:
        print(f"  {h['ticker']:5s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
