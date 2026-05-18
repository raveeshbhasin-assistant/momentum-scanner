"""Tracker initializer for Modern Defense. Top 7 by score (no cross-theme dedup).

Per user direction (2026-05-17): each theme tracker is true to itself. If a name
naturally scores top-7 in both Space and Defense, it's held in both — the
effective 2x position is intentional and documented in the entry note.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "Allied defense buyers (NATO Europe + Israel + Indo-PACOM partners) will procure "
    "cheap autonomous platforms and the EW/HPM systems to defeat them faster than the "
    "slower-moving US doctrine shift. Global subsystem suppliers with multi-year locked "
    "munitions backlogs (Rheinmetall-style) plus EW pure-plays (LHX) capture more durable "
    "per-share value than headline-grabbing US drone pure-plays. Catalyst-vs-structural "
    "framing explicit: drone platforms = 1-2yr catalyst trade, munitions + EW = 3-5yr "
    "structural hold. Position weights skew ~60% Buckets 1+2 / ~25% Bucket 3 / ~15% Bucket 4. "
    "Unique structural risk: can gap down 40-50% on peace — 30% drawdown is hard trim-trigger."
)

# Top 7 by v1 scoring. Cross-theme overlaps (LHX, NOC, CACI) intentionally accepted —
# they are the right vehicles for both themes and the 2x position size is the natural
# consequence of two correct theses pointing to the same name.
PROMOTED = ["RNMBY", "LHX", "AVAV", "SAABY", "NOC", "CACI", "LDOS"]

CONVICTION_OVERRIDES = {
    # CACI debt 1/5 (+73.8% YoY heavy growth from acquisitions) — same concern as Space
    "CACI": 2,
    # AVAV is asymmetric but cash-burning (CS 3/5, negative FCF) — size smaller despite RS 5
    "AVAV": 2,
}

ENTRY_NOTES = {
    "RNMBY":  "Thesis flagship #1 by a wide margin (score 94). Perfect CS 5/5 — massive -34% shares buyback + 14% FCF margin + paying down debt + zero SBC, all while being the dominant European 155mm + propellant supplier with multi-year backlogs. RS 10 at entry = exactly the asymmetric setup we want. The single most thesis-aligned name in the entire Defense cohort.",
    "LHX":    "Tactical EW + space comms; defense prime with CS 5/5 (-1.2% buyback, paying down debt, zero SBC, 12.3% FCF margin). RS 55 with 3M -11.9% pullback = asymmetric. Cross-theme with Space tracker — intentional 2x position because LHX is irreplaceable in BOTH themes (EW for Defense, space comms for Space). Document the concentration.",
    "AVAV":   "The ONE battle-tested US drone pure-play. Switchblade fielded extensively. RS 5 (cohort worst) after -35% 3M pullback = extreme asymmetric setup. **BUT CS 3/5 with negative FCF margin — Lean Trim sizing despite the setup, because dilution risk is real.** Watch DDP contract awards as catalyst.",
    "SAABY":  "Europe pure-play — NLAW + Carl-Gustaf + Gripen + Giraffe radar. Europe rearmament beneficiary. RS 20 at entry after deep -28.5% 3M pullback = asymmetric. CS 3/5 (modest but acceptable). Watch EU procurement awards.",
    "NOC":    "Munitions/propellants slice + B-21 + strategic systems. CS 5/5 (buyback + flat debt + zero SBC). RS 25 LOW after -22.8% 3M pullback = asymmetric setup at depressed price. Cross-theme with Space (where it's the in-orbit servicing anchor) — intentional 2x.",
    "CACI":   "Defense IT + intelligence community + EW services. **CS 4/5 overall BUT Debt 1/5 from +73.8% YoY growth = acquisition-funded — Conviction Lean Trim at entry until integration is validated.** Same Lean Trim applied in Space. Cross-theme overlap accepted at reduced size.",
    "LDOS":   "Largest defense IT contractor. CS 4/5. RS 15 LOW after deep -29.7% 3M pullback = asymmetric setup. Defense-pure (not in Space tracker), promoted as the 7th slot for clean defense IT exposure without yet another Space overlap.",
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
        "theme": "modern_defense",
        "theme_display_name": "Modern Defense & Drones",
        "theme_status": "Active",
        "theme_locked_at": "2026-05-16",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 739.17,
            "ita_at_theme_lock": 217.27,
            "lmt_at_theme_lock": 516.01,
            "rnmby_at_theme_lock": 260.40,
            "avav_at_theme_lock": 158.00,
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter — plus immediate review on any geopolitical de-escalation signal",
        "_cross_theme_overlaps": {
            "LHX": "Also in Space tracker (slot #1). Intentional 2x position — irreplaceable EW + space comms vehicles in both themes.",
            "NOC": "Also in Space tracker (slot #4). Intentional 2x position — irreplaceable propellants + in-orbit servicing in both themes.",
            "CACI": "Also in Space tracker (slot #6). Intentional 2x position — defense IT serves both themes. Lean Trim conviction in BOTH trackers due to debt 1/5 acquisition concern.",
        },
        "position_weight_target": {
            "Bucket 1 (CUAS/EW)": "~30% — LHX heaviest single-name in bucket",
            "Bucket 2 (Munitions)": "~35% — RNMBY as flagship 20%, NOC as supporting 15%",
            "Bucket 3 (Drones)": "~25% — AVAV + SAABY split; AVAV smaller per Lean Trim",
            "Bucket 4 (Software/C2)": "~10% — LDOS + CACI (CACI at smaller Lean Trim size)",
        },
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str))
    print(f"\n=== Modern Defense tracker initialized — {today} ===")
    for h in holdings:
        conv_label = {1:"Trim", 2:"Lean Trim", 3:"Hold", 4:"Lean Inc", 5:"Increase"}.get(h['conviction'], "?")
        print(f"  {h['ticker']:6s}  ${h['entry_price']:8.2f}  score {h['scoring_total']:.1f}  conv {h['conviction']} ({conv_label})  {h['bucket']}")


if __name__ == "__main__":
    main()
