"""
One-off scoring script for Aging Population Infrastructure theme — applies the
long-only rubric to the 18 candidates in candidates.json. Writes scoring.md and
scoring_log.json for the tracker.

Rubric (long-only, adapted from space_economy v1 with two documented tweaks):
1. bottleneck_specificity is read as COHORT-LINKAGE specificity here — how
   directly a name's demand tracks the 65+/80+ population (5 = near-mechanical
   demographic function; 1 = diluted / non-geriatric). Weighted 20 — the tightness
   of the demographic linkage is the whole edge of this theme.
2. capital_structure kept at 20. Unlike space, most vehicles here are cash-
   generative, so the CS auto-score rarely fails — but the REIT rate sensitivity
   and a couple of unprofitable small-caps (RXST) still make balance-sheet quality
   a real discriminator. (The yfinance seed does not populate shares/debt/SBC/FCF,
   so CS falls back to the documented neutral 3/5 for all names until the nightly
   FMP refresh folds this universe in; scoring therefore leans on the judgment
   criteria, as intended for a first pass.)
valuation_runway at 15 does real work here: the sector has lagged the market and
several device compounders are down double digits on the year — cheap entry on a
durable name is a positive, expensive quality (WELL/VTR/COO/ISRG P/E) is a drag.

Reuses the same compute_capital_structure_score and compute_13f_score patterns as
space_economy — only the per-ticker SCORES dict, WEIGHTS rationale, and theme
strings differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Aging Population v1, long-only) ──────────────
WEIGHTS = {
    "bottleneck_specificity": 20,   # read as COHORT-LINKAGE specificity
    "rs_inflection":          10,   # 3-5yr horizon — RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # durability > raw growth
    "margin_durability":       5,
    "valuation_runway":       15,   # sector has lagged — entry multiple matters
    "institutional_13f":       5,
    "capital_structure":      20,   # rate sensitivity + a few unprofitable small-caps
    "catalyst_proximity":      0,   # zeroed — same logic as space/AI DC
}
assert sum(WEIGHTS.values()) == 100

SCORE_TYPE = {
    "bottleneck_specificity": "judgment",
    "rs_inflection":          "quantitative",
    "theme_exposure":         "judgment",
    "revenue_growth":         "quantitative",
    "margin_durability":      "mixed",
    "valuation_runway":       "quantitative",
    "institutional_13f":      "auto-from-13f",
    "capital_structure":      "auto-from-fmp",
    "catalyst_proximity":     "judgment",
}


def compute_capital_structure_score(ticker, cand):
    """Same logic as space_economy/_score_run.py — 4-component avg:
    shares-out YoY · debt YoY · SBC/rev · FCF margin. Neutral 3/5 fallback when
    the yfinance seed leaves these fields empty."""
    shares_yoy = cand.get("shares_growth_yoy")
    debt_yoy = cand.get("debt_growth_yoy")
    sbc_pct = cand.get("sbc_pct_revenue")
    fcf_margin = cand.get("fcf_margin")
    parts = []
    if shares_yoy is not None:
        s = float(shares_yoy)
        score, lbl = (5,"buyback") if s<=0 else (4,"minimal") if s<=0.02 else (3,"moderate") if s<=0.05 else (2,"significant") if s<=0.10 else (1,"heavy")
        parts.append(("Shares", score, f"{s*100:+.1f}% {lbl}"))
    else:
        parts.append(("Shares", 3, "n/a"))
    if debt_yoy is not None:
        d = float(debt_yoy)
        score, lbl = (5,"paying down") if d<=-0.10 else (4,"flat") if d<=0 else (3,"moderate") if d<=0.10 else (2,"significant") if d<=0.25 else (1,"heavy")
        parts.append(("Debt", score, f"{d*100:+.1f}% {lbl}"))
    else:
        parts.append(("Debt", 3, "n/a"))
    if sbc_pct is not None:
        p = float(sbc_pct)
        score, lbl = (5,"minimal") if p<=0.02 else (4,"modest") if p<=0.05 else (3,"moderate") if p<=0.10 else (2,"high") if p<=0.15 else (1,"very high")
        parts.append(("SBC", score, f"{p*100:.1f}% rev {lbl}"))
    else:
        parts.append(("SBC", 3, "n/a"))
    if fcf_margin is not None:
        f = float(fcf_margin)
        score, lbl = (5,"strong") if f>=0.20 else (4,"healthy") if f>=0.10 else (3,"modest") if f>=0.05 else (2,"thin") if f>=0 else (1,"negative")
        parts.append(("FCF margin", score, f"{f*100:.1f}% {lbl}"))
    else:
        parts.append(("FCF margin", 3, "n/a"))

    avg = round(sum(p[1] for p in parts) / 4)
    rationale = " · ".join(f"{name} {s}/5 ({lbl})" for name, s, lbl in parts)
    if all(p[1] == 3 for p in parts) and all(v is None for v in [shares_yoy, debt_yoy, sbc_pct, fcf_margin]):
        rationale = "Capital structure data unavailable — neutral fallback"
    return avg, rationale


def compute_13f_score(ticker):
    """Same logic as space_economy — auto-compute from tracker_live.json, fall
    back to hand-scored."""
    live_path = _HERE / "tracker_live.json"
    if not live_path.exists():
        return _HAND_SCORED_13F.get(ticker, 3), "13F data unavailable — hand-scored fallback"
    try:
        live = json.loads(live_path.read_text())
    except Exception:
        return _HAND_SCORED_13F.get(ticker, 3), "13F load failed — hand-scored fallback"
    per = (live.get("per_ticker") or {}).get(ticker) or {}
    holders = per.get("13f_holders") or []
    if not holders:
        return _HAND_SCORED_13F.get(ticker, 3), "13F empty — hand-scored fallback"
    deltas = [(h.get("delta_pp") or 0) for h in holders if h.get("delta_pp") is not None]
    if not deltas:
        return 3, "No 13F deltas in data — neutral"
    net_pp = sum(deltas)
    positives = sum(1 for d in deltas if d > 0.05)
    negatives = sum(1 for d in deltas if d < -0.05)
    if net_pp >= 1.0 and positives >= 3:
        return 5, f"Strong accumulation: net +{net_pp:.1f}pp across {positives} funds"
    if net_pp >= 0.5:
        return 4, f"Modest accumulation: net +{net_pp:.1f}pp"
    if net_pp >= -0.5 and positives >= negatives:
        return 3, f"Neutral / mixed: net {net_pp:+.1f}pp"
    if net_pp >= -1.0:
        return 2, f"Modest trimming: net {net_pp:+.1f}pp"
    return 1, f"Sustained outflows: net {net_pp:+.1f}pp across {negatives} funds"


# ─── Hand-scored 13F fallback per ticker (Aging-specific) ──
_HAND_SCORED_13F = {
    "WELL":5, "VTR":4, "OHI":4, "NHI":3, "SBRA":3,
    "SYK":5, "ZBH":4, "ISRG":5, "EW":5, "PODD":3, "GMED":3,
    "ALC":4, "COO":4, "RXST":2, "SONVY":2,
    "CHE":4, "ADUS":3, "EHAB":2,
}


# ─── Per-ticker hand scores ───────────────────────────────
# bottleneck_specificity == COHORT-LINKAGE specificity for this theme.
# capital_structure and institutional_13f are auto-computed.
SCORES = {
    # ── Bucket 1: Senior Housing & Healthcare REITs ──
    "WELL": dict(
        bottleneck_specificity=5, _bs="Highest operating leverage to 80+ move-in inflection vs frozen supply; flagship demand vehicle",
        rs_inflection=4,           _rs="RS 76, +15.4% 3M — healthy momentum, near 52w high (-1.5%)",
        theme_exposure=5,          _te="Purest senior-housing occupancy-recovery vehicle",
        revenue_growth=5,          _rg="Rev +38% YoY (SHOP consolidation) — very strong",
        margin_durability=4,       _m="SHOP NOI margins expanding with occupancy",
        valuation_runway=2,        _v="P/E ~112, P/S ~14 — priced for the recovery; near 52w high tightens room",
        institutional_13f=5,       _13f="Core REIT holding, broadly owned",
        catalyst_proximity=4,      _cat="Quarterly same-store NOI + occupancy prints",
    ),
    "VTR": dict(
        bottleneck_specificity=4, _bs="SHOP book + medical office; same recovery, slightly more diluted mix",
        rs_inflection=4,           _rs="RS 65, +11.4% 3M — solid, near 52w high (-0.9%)",
        theme_exposure=4,          _te="Large SHOP exposure to the occupancy recovery",
        revenue_growth=4,          _rg="Rev +22% YoY — strong",
        margin_durability=4,       _m="Improving SHOP margins",
        valuation_runway=2,        _v="P/E ~168 — richest in the sleeve; run-up limits room",
        institutional_13f=4,       _13f="Broadly owned REIT",
        catalyst_proximity=4,      _cat="Same-store occupancy cadence",
    ),
    "OHI": dict(
        bottleneck_specificity=3, _bs="Net-lease SNF/ALF landlord — operator-credit-driven, not occupancy upside",
        rs_inflection=4,           _rs="RS 59, +10.5% 3M — steady, near 52w high",
        theme_exposure=3,          _te="Aging demand but via fixed leases, diluted linkage",
        revenue_growth=3,          _rg="Rev +14% YoY — decent",
        margin_durability=4,       _m="Stable triple-net margins, high payout",
        valuation_runway=4,        _v="P/E ~24 — reasonable, income tilt",
        institutional_13f=4,       _13f="Popular high-yield REIT",
        catalyst_proximity=3,      _cat="Operator-coverage + dividend cadence",
    ),
    "NHI": dict(
        bottleneck_specificity=3, _bs="Diversified senior-housing/SNF net-lease; conservative",
        rs_inflection=2,           _rs="RS 24, -7.5% 3M, -14.9% off 52w high — laggard in the sleeve",
        theme_exposure=3,          _te="Real senior-housing exposure, income-tilted",
        revenue_growth=2,          _rg="Rev -2.6% YoY — flat/declining",
        margin_durability=4,       _m="Conservative balance sheet, stable",
        valuation_runway=4,        _v="P/E ~25 — reasonable, and it has pulled back",
        institutional_13f=3,       _13f="Smaller-cap REIT, moderate ownership",
        catalyst_proximity=3,      _cat="Dividend + lease renewals",
    ),
    "SBRA": dict(
        bottleneck_specificity=2, _bs="SNF-heavy net-lease; higher operator-credit risk, most diluted linkage",
        rs_inflection=3,           _rs="RS 47, +0.8% 3M — middling",
        theme_exposure=3,          _te="Aging demand but SNF operator-credit-sensitive",
        revenue_growth=4,          _rg="Rev +22% YoY — strong",
        margin_durability=3,       _m="Recovering coverage, some operator risk",
        valuation_runway=4,        _v="P/E ~31 but deep-value name if recovery holds",
        institutional_13f=3,       _13f="Small-cap, moderate ownership",
        catalyst_proximity=3,      _cat="Operator-coverage recovery",
    ),

    # ── Bucket 2: Age-Specific Medical Devices ──
    "SYK": dict(
        bottleneck_specificity=5, _bs="Joint-replacement volumes a near-mechanical function of the cohort; Mako moat",
        rs_inflection=3,           _rs="RS 35, -1.8% 3M, -19% off 52w high — beaten down despite tailwind (entry opportunity)",
        theme_exposure=5,          _te="Device anchor; ortho volumes track the cohort directly",
        revenue_growth=2,          _rg="Rev +2.6% YoY — soft near-term",
        margin_durability=4,       _m="Strong, durable med-tech margins",
        valuation_runway=3,        _v="P/E ~38 — full but derated ~16% on the year",
        institutional_13f=5,       _13f="Core large-cap med-tech holding",
        catalyst_proximity=3,      _cat="Procedure-volume recovery, Mako placements",
    ),
    "ZBH": dict(
        bottleneck_specificity=4, _bs="Pure-play hips/knees — most cohort-levered large ortho name",
        rs_inflection=3,           _rs="RS 41, -1.0% 3M, -16% off 52w high — depressed",
        theme_exposure=5,          _te="Purest joint-replacement exposure",
        revenue_growth=3,          _rg="Rev +9.3% YoY — decent",
        margin_durability=3,       _m="Solid but pricing-pressured ortho margins",
        valuation_runway=4,        _v="P/E ~23 — cheapest device name for tight cohort linkage",
        institutional_13f=4,       _13f="Broadly owned med-tech",
        catalyst_proximity=3,      _cat="Procedure recovery + new-product cadence",
    ),
    "ISRG": dict(
        bottleneck_specificity=5, _bs="da Vinci install-base monopoly; razor-blade instruments; older-skewing procedures",
        rs_inflection=2,           _rs="RS 29, -4.3% 3M, -28% off 52w high — heavy derating",
        theme_exposure=4,          _te="Surgical robotics broad but procedure mix skews older",
        revenue_growth=4,          _rg="Rev +23% YoY — strong",
        margin_durability=5,       _m="Best-in-class device margins, recurring instrument revenue",
        valuation_runway=2,        _v="P/E ~53, P/S ~14 — still premium despite the pullback",
        institutional_13f=5,       _13f="Institutional darling",
        catalyst_proximity=3,      _cat="Procedure growth + new-system cycle",
    ),
    "EW": dict(
        bottleneck_specificity=5, _bs="TAVR near-monopoly; aortic stenosis is age-driven, expanding indications",
        rs_inflection=5,           _rs="RS 88, +17.4% 3M, near 52w high (-0.4%) — momentum leader among devices",
        theme_exposure=5,          _te="Purest structural-heart cohort exposure",
        revenue_growth=4,          _rg="Rev +16.7% YoY — strong",
        margin_durability=5,       _m="Excellent structural-heart margins",
        valuation_runway=3,        _v="P/E ~51 — premium but momentum-supported",
        institutional_13f=5,       _13f="Core med-tech holding",
        catalyst_proximity=4,      _cat="TAVR indication expansion + trial readouts",
    ),
    "PODD": dict(
        bottleneck_specificity=4, _bs="Omnipod recurring pods; strong linkage but skews younger via type-2/obesity",
        rs_inflection=1,           _rs="RS 0, -21.8% 3M, -55% off 52w high — severe drawdown",
        theme_exposure=3,          _te="Diabetes tracks aging but not purely geriatric",
        revenue_growth=5,          _rg="Rev +33.9% YoY — fastest grower",
        margin_durability=3,       _m="Improving but pump-margin pressure",
        valuation_runway=4,        _v="P/E ~37 after a 47% 1Y drop — growth at a reset multiple",
        institutional_13f=3,       _13f="Broadly owned med-tech growth name",
        catalyst_proximity=3,      _cat="Type-2 label expansion, pod attach",
    ),
    "GMED": dict(
        bottleneck_specificity=3, _bs="Degenerative spine is age-driven but a competitive segment",
        rs_inflection=2,           _rs="RS 12, -8.4% 3M, -21% off 52w high — weak momentum",
        theme_exposure=3,          _te="Spine + robotics, moderate cohort linkage",
        revenue_growth=5,          _rg="Rev +27% YoY (post-NuVasive) — strong",
        margin_durability=3,       _m="Scaling post-merger margins",
        valuation_runway=5,        _v="P/E ~19 — cheapest device name outright",
        institutional_13f=3,       _13f="Mid-cap, moderate ownership",
        catalyst_proximity=3,      _cat="ExcelsiusGPS placements, merger synergies",
    ),

    # ── Bucket 3: Hearing & Vision ──
    "ALC": dict(
        bottleneck_specificity=5, _bs="Cataract IOLs — among the most demographically certain procedures globally; global #1",
        rs_inflection=2,           _rs="RS 18, -7.9% 3M, -26% off 52w high — laggard, value setup",
        theme_exposure=4,          _te="Surgical eyecare tightly age-driven",
        revenue_growth=3,          _rg="Rev +9.4% YoY — steady",
        margin_durability=4,       _m="Strong surgical-eyecare margins",
        valuation_runway=4,        _v="P/E ~41 but derated ~21% on the year — reasonable for the franchise",
        institutional_13f=4,       _13f="Broadly owned eyecare leader",
        catalyst_proximity=3,      _cat="Cataract volumes + premium-IOL mix",
    ),
    "COO": dict(
        bottleneck_specificity=3, _bs="Lenses skew younger; diversified vision + surgical — diluted geriatric linkage",
        rs_inflection=3,           _rs="RS 53, +3.5% 3M — middling",
        theme_exposure=3,          _te="Vision demand but not purely aging",
        revenue_growth=3,          _rg="Rev +7.9% YoY — steady",
        margin_durability=4,       _m="Durable diversified margins",
        valuation_runway=2,        _v="P/E ~62 — expensive for the diluted linkage",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Myopia-management + lens cadence",
    ),
    "RXST": dict(
        bottleneck_specificity=4, _bs="Light-adjustable IOL pure-play — tight linkage but pre-profit micro-cap",
        rs_inflection=1,           _rs="RS 6, -15.7% 3M, -58% off 52w high — severe drawdown",
        theme_exposure=4,          _te="Pure premium-cataract-upgrade optionality",
        revenue_growth=1,          _rg="Rev -18.5% YoY — contracting",
        margin_durability=1,       _m="Unprofitable (negative P/E)",
        valuation_runway=2,        _v="P/S ~1.8, cheap on sales but no earnings — speculative",
        institutional_13f=2,       _13f="Micro-cap (~$230M), thin ownership",
        catalyst_proximity=3,      _cat="LAL adoption inflection (uncertain)",
    ),
    "SONVY": dict(
        bottleneck_specificity=3, _bs="Hearing loss is age-driven; only holdable hearing-aid proxy (diluted-by-necessity)",
        rs_inflection=4,           _rs="RS 71, +14.8% 3M — momentum building off a low base",
        theme_exposure=4,          _te="Pure hearing-aid exposure via sponsored ADR",
        revenue_growth=3,          _rg="Rev growth n/a in seed — treat neutral, mid-single-digit historically",
        margin_durability=4,       _m="Strong hearing-aid oligopoly margins",
        valuation_runway=4,        _v="P/E ~23 — reasonable after a -11% 1Y",
        institutional_13f=2,       _13f="ADR, thinner US institutional ownership",
        catalyst_proximity=3,      _cat="OTC/prescription hearing-aid cycle",
    ),

    # ── Bucket 4: Home Health & Services ──
    "CHE": dict(
        bottleneck_specificity=3, _bs="VITAS hospice is pure aging vehicle; Roto-Rooter dilutes the linkage",
        rs_inflection=5,           _rs="RS 100, +26.4% 3M, near 52w high (-2.4%) — strongest in the universe",
        theme_exposure=3,          _te="Hospice end-of-life demand certain, but diluted by plumbing segment",
        revenue_growth=2,          _rg="Rev +1.6% YoY — slow",
        margin_durability=5,       _m="Excellent, cash-generative both segments",
        valuation_runway=4,        _v="P/E ~26 — reasonable for the quality",
        institutional_13f=4,       _13f="Broadly owned quality compounder",
        catalyst_proximity=3,      _cat="Hospice census + admissions cadence",
    ),
    "ADUS": dict(
        bottleneck_specificity=4, _bs="Personal-care pure-play — directly the structurally short home-care labor supply",
        rs_inflection=4,           _rs="RS 82, +15.4% 3M — strong momentum",
        theme_exposure=5,          _te="Purest aging-in-place / home-care labor exposure",
        revenue_growth=3,          _rg="Rev +7.7% YoY — steady",
        margin_durability=3,       _m="Thin personal-care-services margins, Medicaid-funded",
        valuation_runway=4,        _v="P/E ~20, P/S ~1.4 — cheap for the pure exposure",
        institutional_13f=3,       _13f="Small-cap, moderate ownership",
        catalyst_proximity=3,      _cat="Medicaid rate updates + acquisitions",
    ),
    "EHAB": dict(
        bottleneck_specificity=2, _bs="Right demand, but returns hinge on the turnaround not the demographic",
        rs_inflection=5,           _rs="RS 94, +23% 3M, +89% 1Y, near 52w high — momentum leader",
        theme_exposure=3,          _te="Home health/hospice demand, leveraged small-cap",
        revenue_growth=2,          _rg="Rev +1.9% YoY — flat",
        margin_durability=2,       _m="Thin, leveraged post-spin",
        valuation_runway=4,        _v="P/E ~21, P/S ~0.7 — deep value on turnaround optionality",
        institutional_13f=2,       _13f="Small-cap, thin ownership",
        catalyst_proximity=4,      _cat="Strategic review / potential sale optionality",
    ),
}


# ─── Compute and render ───────────────────────────────────
def crit_short(crit):
    m = {"bottleneck_specificity":"bs","rs_inflection":"rs","theme_exposure":"te",
         "revenue_growth":"rg","margin_durability":"m","valuation_runway":"v",
         "institutional_13f":"13f","capital_structure":"cap","catalyst_proximity":"cat"}
    return m.get(crit, crit)


def compute(scores):
    out = {}
    candidates = json.loads((_HERE / "candidates.json").read_text())["candidates"]
    meta_by_ticker = {c["ticker"]: c for c in candidates}
    for ticker, s in scores.items():
        components = {}
        raw_total = 0
        auto_13f, auto_13f_rationale = compute_13f_score(ticker)
        auto_cap, auto_cap_rationale = compute_capital_structure_score(ticker, meta_by_ticker.get(ticker, {}))
        for crit, weight in WEIGHTS.items():
            if crit == "institutional_13f":
                score = auto_13f
                rationale = auto_13f_rationale
            elif crit == "capital_structure":
                score = auto_cap
                rationale = auto_cap_rationale
            else:
                score = s.get(crit)
                rationale = s.get(f"_{crit_short(crit)}", "")
            if score is None:
                continue
            contribution = score * weight
            raw_total += contribution
            components[crit] = {
                "score": score, "weight": weight, "type": SCORE_TYPE[crit],
                "contribution": contribution, "rationale": rationale,
            }
        meta = meta_by_ticker.get(ticker, {})
        out[ticker] = {
            "company": meta.get("company"),
            "bucket": meta.get("bucket"),
            "sub": meta.get("sub"),
            "note": meta.get("note"),
            "specificity_meta": meta.get("specificity"),
            "raw_total": raw_total,
            "normalized_100": round(raw_total / 5, 1),
            "components": components,
            "snapshot": {k: meta.get(k) for k in
                          ["price","market_cap","pe","ps","1y_pct","3m_pct","1m_pct",
                           "dist_from_52w_high_pct","rs_3m","revenue_growth_yoy",
                           "shares_growth_yoy","debt_growth_yoy","sbc_pct_revenue","fcf_margin"]},
        }
    return out


def render_md(results):
    today = datetime.now().strftime("%Y-%m-%d")
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    lines = []
    lines.append("# Aging Population Infrastructure — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Long-only rubric v1 (cohort-linkage weighted 20; valuation_runway 15 — the sector has lagged). Source: `candidates.json`. Audit: `scoring_log.json`._")
    lines.append("")
    lines.append("_Note: `bottleneck_specificity` is read here as **cohort-linkage specificity** — how directly demand tracks the 65+/80+ population. `capital_structure` falls back to neutral 3/5 until the FMP nightly refresh folds this universe in._")
    lines.append("")
    lines.append("## Ranked candidates")
    lines.append("")
    lines.append("| Rank | Ticker | Company | Bucket | Score | B | RS | TE | RG | M | V | 13F | Cap |")
    lines.append("|-----:|--------|---------|--------|-------|---|----|----|----|---|---|-----|-----|")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        c = r["components"]
        marker = " 🟢" if i <= 5 else " 🟡" if i <= 7 else ""
        lines.append(f"| {i}{marker} | **{ticker}** | {r['company']} | {str(r['bucket'])[:26]} | **{r['normalized_100']:.1f}** | "
                      f"{c.get('bottleneck_specificity',{}).get('score','—')} | "
                      f"{c.get('rs_inflection',{}).get('score','—')} | "
                      f"{c.get('theme_exposure',{}).get('score','—')} | "
                      f"{c.get('revenue_growth',{}).get('score','—')} | "
                      f"{c.get('margin_durability',{}).get('score','—')} | "
                      f"{c.get('valuation_runway',{}).get('score','—')} | "
                      f"{c.get('institutional_13f',{}).get('score','—')} | "
                      f"{c.get('capital_structure',{}).get('score','—')} |")
    lines.append("")
    lines.append("🟢 = top 5 (tracker)  ·  🟡 = next 2 (watching)")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("_Per-pick detail rendering to be added after the first scoring run is reviewed (matches the space_economy pattern)._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "aging_population", "scored_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": today, "rubric_version": "v1",
        "weights": WEIGHTS, "score_types": SCORE_TYPE, "results": results,
    }
    (_HERE / "scoring_log.json").write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")
    hist = _HERE / "history"
    hist.mkdir(exist_ok=True)
    (hist / f"scoring_log_{today}.json").write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")
    md = render_md(results)
    (_HERE / "scoring.md").write_text(md, encoding="utf-8")
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    print(f"\n=== Aging Population Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:8s}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
