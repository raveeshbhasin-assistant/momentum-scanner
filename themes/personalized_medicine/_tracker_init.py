"""One-off tracker initializer for Personalized Medicine & Genomics. Top 7 from scoring v1."""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

PLAIN_SUMMARY = (
    "As whole-genome sequencing falls below $200, genomics crosses from research curiosity into "
    "standard-of-care infrastructure — and the durable money is made in the picks-and-shovels layer "
    "(sequencers + consumables + DNA synthesis) and the reimbursed molecular-diagnostics franchises "
    "(cancer screening, MRD, liquid biopsy) that earn real, recurring, insurer-paid revenue. We "
    "deliberately do NOT anchor the book in the pre-revenue gene editors: their science is real but "
    "as equities they are binary, dilutive, single-catalyst bets, so they sit in a small optionality "
    "sleeve capped to the best-capitalized names. Long-only, 3-5 year horizon."
)

# Top 7 from scoring v1 (long-only rubric, capital_structure weight 20)
# Bucket distribution: 1 Sequencing · 1 Tools/Consumables · 4 Molecular Dx · 1 Editing (optionality anchor)
PROMOTED = ["ILMN", "NTRA", "GH", "EXAS", "QGEN", "VCYT", "CRSP"]

CONVICTION_OVERRIDES = {
    # CRSP is the single optionality-sleeve promotion — pre-commercial-scale, binary pipeline.
    # Held Lean Trim: earns a spot as the best-capitalized editor with an approved product, but
    # sized small on purpose per the thesis' "cap the editors" discipline.
    "CRSP": 2,
    # ILMN carries single-platform concentration risk (it IS Bucket 1); anchor conviction but
    # flag sizing discipline rather than max conviction.
    "ILMN": 3,
}

ENTRY_NOTES = {
    "ILMN":  "Top score and the picks-and-shovels anchor. Dominant short-read installed base with proprietary consumable pull-through — the razor/razorblade annuity is the durable moat, not instrument placements. Momentum is real (+52% 3M) but partly a recovery off a deep GRAIL-era trough, so this is a durability buy, not an asymmetric entry. Watch: single-platform concentration risk — if open reagents or a long-read competitor crack the consumables lock-in, Bucket 1 thins fast. Size with that in mind.",
    "NTRA":  "Highest-scoring diagnostics name and the MRD-volume flagship. Signatera is compounding ~39% YoY on a large proprietary tumor-informed dataset — the reimbursement + guideline + workflow moat is a toll road, not a commodity lab test. Mid RS (50) with healthy +36% 3M momentum means we're not buying at a blow-off top. Still near breakeven with heavy reinvestment; watch for two consecutive quarters of decelerating test volume as the re-check trigger.",
    "GH":    "Liquid-biopsy + blood-screening flagship (Guardant360 + Shield). Revenue compounding ~48% YoY. The swing factor is reimbursement breadth for Shield blood-based screening — a broad Medicare coverage decision is the biggest upside catalyst in the theme. Still loss-making and richly valued (P/S ~21) after a +241% 1Y run, so accept that we're buying volume growth, not current profitability.",
    "EXAS":  "The flagship reimbursed-screening franchise (Cologuard) plus Oncodetect MRD. **Did not return data from yfinance at lock — entry price/fundamentals are hand-set placeholders and MUST be re-recorded at the first FMP/Railway refresh before any sizing.** Kept in the tracker because it is a core real-revenue diagnostics name whose blood-based screening leg is the next TAM expansion. Watch: MCED/blood-screening coverage decisions.",
    "QGEN":  "The value + quality name in the tools bucket. Sample-to-insight consumables + QuantiFERON are a defensive, real-FCF molecular-prep annuity upstream of every NGS/dx workflow. Cheapest quality name in the universe (P/E ~20, P/S ~3.8) and the only core tools name to clear the tracker — but note RS 0 / -18% 1Y: it's the laggard, so this is a value entry, not momentum. Watch for a turn in molecular-dx menu growth.",
    "VCYT":  "Reimbursed genomic-classifier franchise (Decipher, Afirma). The cleanest P&L in the diagnostics bucket — actually profitable (P/E ~54) with ~22% growth, unlike loss-making GH/NTRA. Narrower TAM keeps it out of the top tier, but the profitability + reimbursed moat earn a promotion. Watch classifier-menu expansion and any MRD entry.",
    "CRSP":  "The single optionality-sleeve promotion and the anchor of the gene-editing bet. Best-capitalized editor with an APPROVED, commercial product (Casgevy) plus a large cash runway and in-vivo pipeline — the only editor that isn't a pure pre-revenue coin-flip. **Held Lean Trim / sized small on purpose:** the thesis caps the editors, and clinical-stage economics are loss-making. Watch Casgevy uptake and in-vivo readouts; treat the whole editing sleeve as one small combined allocation, not per-name.",
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
        "theme": "personalized_medicine",
        "theme_display_name": "Personalized Medicine & Genomics",
        "theme_status": "Active",
        "theme_locked_at": "2026-07-06",
        "tracker_initialized_at": today,
        "last_reviewed_at": today,
        "plain_summary": PLAIN_SUMMARY,
        "benchmarks_at_init": {
            "spy_at_theme_lock": 751.28,
            "xbi_at_theme_lock": 160.81,
            "ilmn_at_theme_lock": 194.33,
            "gh_at_theme_lock": 168.82,
            "exas_at_theme_lock": None,  # yfinance returned no data at lock — record at first FMP refresh
        },
        "holdings": holdings,
        "review_cadence": "monthly first 3 months, quarterly thereafter",
    }

    (_HERE / "tracker.json").write_text(json.dumps(tracker, indent=2, default=str), encoding="utf-8")
    print(f"\n=== Personalized Medicine tracker initialized — {today} ===")
    for h in holdings:
        px = h["entry_price"]
        px_s = f"${px:8.2f}" if isinstance(px, (int, float)) else f"{'n/a':>9s}"
        sc = h["scoring_total"]
        sc_s = f"{sc:.1f}" if isinstance(sc, (int, float)) else "n/a"
        print(f"  {h['ticker']:6s}  {px_s}  score {sc_s}  conv {h['conviction']}  {h['bucket']}")


if __name__ == "__main__":
    main()
