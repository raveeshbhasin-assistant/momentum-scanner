"""
One-off scoring script for Climate Adaptation Infrastructure theme — applies the
long-only rubric to the 16 candidates in candidates.json. Writes scoring.md and
scoring_log.json for the tracker.

Rubric (copied from space_economy v1, one documented adjustment):
- capital_structure kept at 20 — but for THIS theme the seed came from yfinance
  (no shares/debt/SBC/FCF fields yet), so CS lands on the neutral 3/5 fallback for
  every name until the universe is folded into refresh_data.py and FMP fills those
  fields. That is expected and identical to glp1's initial seed. CS therefore does
  not differentiate the cohort at v1 — the water-vs-cyclical split is carried by
  bottleneck_specificity, theme_exposure, and valuation_runway instead.
- rs_inflection kept at 10 (3-5yr horizon, RS matters less than for the scanner).
- revenue_growth kept at 10, margin_durability at 5 (durability > raw growth on a
  cohort that includes ag-cyclicals whose revenue prints swing with the crop cycle).

Reuses the same compute_capital_structure_score and compute_13f_score patterns as
space_economy — only the per-ticker SCORES dict, _HAND_SCORED_13F, and strings differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Climate Adaptation v1) ───────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,
    "theme_exposure":         15,
    "revenue_growth":         10,
    "margin_durability":       5,
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,
    "catalyst_proximity":      0,
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
    fields are absent (yfinance seed — filled once FMP-backed refresh runs)."""
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
        rationale = "Capital structure data unavailable (yfinance seed) — neutral fallback"
    return avg, rationale


def compute_13f_score(ticker):
    """Same logic as space_economy — auto-compute from tracker_live.json, fall back to hand-scored."""
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


# ─── Hand-scored 13F fallback per ticker (Climate-specific) ──
# Large-cap regulated utilities + quality compounders are broadly institutionally
# owned (4-5); small-cap cyclicals and irrigation names less so (2-3).
_HAND_SCORED_13F = {
    "XYL":4, "AWK":5, "WTRG":4, "PNR":4, "ECL":5, "VLTO":4,
    "AOS":3, "AWI":3, "CSL":4, "IBP":3,
    "DE":5, "CTVA":4, "LNN":2, "VMI":3, "NTR":4,
    "VRSK":5,
}


# ─── Per-ticker hand scores ───────────────────────────────
# 7 judgment+quant criteria as 1-5 scores + one-line `_xx` rationales.
# `capital_structure` and `institutional_13f` are auto-computed.
# Quant scores (rs, rg, v) grounded in the REAL fundamentals now in candidates.json.
SCORES = {
    # ── Bucket 1: Water Infrastructure & Treatment ──
    "XYL": dict(
        bottleneck_specificity=5, _bs="Purest large-cap water-tech: pumps, transport, Sensus metering + analytics",
        rs_inflection=4,           _rs="RS 47, 3M -1.1% but 1M +8.9% — cooled-off then turning; constructive setup",
        theme_exposure=5,          _te="Cleanest water-tech pure-play; thesis 'what we actually want' reference",
        revenue_growth=3,          _rg="Rev +2.7% YoY — steady, water-tech is not a hypergrowth story",
        margin_durability=4,       _m="Solid, expanding margins on metering/analytics mix shift",
        valuation_runway=3,        _v="P/E ~30, P/S ~3.1 — full but off its highs (-21.8% from 52w high) leaves room",
        institutional_13f=4,       _13f="Broadly owned water franchise",
        catalyst_proximity=3,      _cat="PFAS/lead capex cycle + metering upgrade demand",
    ),
    "AWK": dict(
        bottleneck_specificity=5, _bs="Largest US regulated water/wastewater utility — geographic monopoly, rate base",
        rs_inflection=3,           _rs="RS 33, 3M -3% but 1M +7.5% — utility recovering, near 52w high (-8%)",
        theme_exposure=5,          _te="Deepest, most-reimbursed adaptation pool; the 'boring reimbursed' anchor",
        revenue_growth=3,          _rg="Rev +5.7% YoY — regulated rate-base growth, exactly as designed",
        margin_durability=4,       _m="Regulated-return margins, highly durable",
        valuation_runway=4,        _v="P/E ~24 — reasonable for a rate-base compounder; near highs but not stretched",
        institutional_13f=5,       _13f="Core utility holding, broadly owned",
        catalyst_proximity=3,      _cat="Rate cases + resilience capex mandates",
    ),
    "WTRG": dict(
        bottleneck_specificity=4, _bs="Regulated water rate-base grower, diluted by Peoples gas segment",
        rs_inflection=3,           _rs="RS 20, 3M -5% / 1M +4.5% — lagging but stabilizing near highs (-6.8%)",
        theme_exposure=4,          _te="Real regulated water exposure; gas overhang caps purity",
        revenue_growth=4,          _rg="Rev +10% YoY — strongest of the utilities",
        margin_durability=4,       _m="Regulated margins, durable",
        valuation_runway=4,        _v="P/E ~20 — cheapest of the water utilities",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Water rate cases + acquisitions",
    ),
    "PNR": dict(
        bottleneck_specificity=4, _bs="Water treatment / filtration / pumps — real scarcity exposure, consumer-cyclical tilt",
        rs_inflection=2,           _rs="RS 13, 3M -11.7%, -33% from 52w high — deep drawdown, no inflection yet",
        theme_exposure=4,          _te="Genuine water treatment exposure diluted by residential/pool",
        revenue_growth=3,          _rg="Rev +2.6% YoY — flattish on soft consumer demand",
        margin_durability=4,       _m="Structurally improved margins post-transformation",
        valuation_runway=4,        _v="P/E ~19 and -33% off highs — valuation offers real runway",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Residential recovery + water-treatment demand",
    ),
    "ECL": dict(
        bottleneck_specificity=4, _bs="Industrial water treatment + hygiene compounder; water-scarcity pricing power",
        rs_inflection=4,           _rs="RS 73, 3M +7.7%, 1M +11.3% — leading the cohort with healthy momentum",
        theme_exposure=4,          _te="Strong industrial water-treatment exposure; premium quality",
        revenue_growth=4,          _rg="Rev +10% YoY — durable organic growth",
        margin_durability=5,       _m="Best-in-class, expanding margins; pricing power",
        valuation_runway=2,        _v="P/E ~38 — premium multiple is the binding constraint here",
        institutional_13f=5,       _13f="Institutional darling quality compounder",
        catalyst_proximity=3,      _cat="Pricing actions + water-scarcity demand",
    ),
    "VLTO": dict(
        bottleneck_specificity=5, _bs="Water-quality instruments + consumables (Hach, Trojan, ChemTreat) — razor-and-blade",
        rs_inflection=4,           _rs="RS 67, 3M +5.2%, 1M +9.4% — momentum building, off its lows",
        theme_exposure=5,          _te="High-specificity water-quality analytics; recurring consumables annuity",
        revenue_growth=3,          _rg="Rev +6.8% YoY — steady recurring growth",
        margin_durability=4,       _m="High-margin razor-and-blade model",
        valuation_runway=3,        _v="P/E ~24, P/S ~4 — reasonable for the recurring-revenue quality",
        institutional_13f=4,       _13f="Well owned post-Danaher spin",
        catalyst_proximity=3,      _cat="Tightening water standards drive consumable pull-through",
    ),

    # ── Bucket 2: Physical-Risk Analytics ──
    "VRSK": dict(
        bottleneck_specificity=5, _bs="Catastrophe modeling embedded in insurer underwriting — enormous switching costs",
        rs_inflection=3,           _rs="RS 60, 3M +1.7%, 1M +5.6% — recovering off a deep -38% 1Y drawdown",
        theme_exposure=5,          _te="The sole vehicle that literally prices the theme; recurring-subscription moat",
        revenue_growth=3,          _rg="Rev +3.9% YoY — steady subscription growth",
        margin_durability=5,       _m="Very high, durable subscription margins",
        valuation_runway=3,        _v="P/E ~29 but -38% off 52w high — the drawdown restores valuation runway",
        institutional_13f=5,       _13f="Widely owned analytics compounder",
        catalyst_proximity=4,      _cat="Insurer repricing/exit cycles drive model demand",
    ),

    # ── Bucket 3: Flood / Storm Resilience & Building Envelope ──
    "CSL": dict(
        bottleneck_specificity=4, _bs="Commercial roofing + waterproofing membranes; non-discretionary re-roof demand",
        rs_inflection=5,           _rs="RS 87, 3M +11.3%, 1M +7.1% — strong momentum inflection off a soft year",
        theme_exposure=4,          _te="Best building-envelope proxy — the layer that keeps storms/water out",
        revenue_growth=2,          _rg="Rev -4% YoY — soft on non-res construction cycle",
        margin_durability=4,       _m="Strong roofing margins, disciplined pricing",
        valuation_runway=4,        _v="P/E ~21 — reasonable, -15% off highs leaves room",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Re-roof cycle + storm-driven remediation demand",
    ),
    "IBP": dict(
        bottleneck_specificity=3, _bs="Insulation + envelope installation; heat/cold resilience but housing-tied",
        rs_inflection=1,           _rs="RS 0, 3M -15.8%, -35% off 52w high — worst-in-cohort drawdown, no inflection",
        theme_exposure=3,          _te="Envelope resilience but demand dominated by housing starts",
        revenue_growth=2,          _rg="Rev -3.5% YoY — soft on housing",
        margin_durability=3,       _m="Moderate installer margins",
        valuation_runway=3,        _v="P/E ~24 despite drawdown — not cheap enough given the cyclicality",
        institutional_13f=3,       _13f="Mid-cap ownership",
        catalyst_proximity=2,      _cat="Housing-starts dependent",
    ),
    "AWI": dict(
        bottleneck_specificity=3, _bs="Ceilings + architectural specialties; resilient interiors, non-res cyclical",
        rs_inflection=2,           _rs="RS 27, 3M -3%, -22% off highs — soft, no clear inflection",
        theme_exposure=3,          _te="Interior resilience is a secondary adaptation driver",
        revenue_growth=4,          _rg="Rev +7.1% YoY — solid despite non-res softness",
        margin_durability=4,       _m="Strong, consistent ceilings margins",
        valuation_runway=3,        _v="P/E ~23 — fair but not compelling",
        institutional_13f=3,       _13f="Mid-cap ownership",
        catalyst_proximity=2,      _cat="Non-res construction cycle",
    ),
    "AOS": dict(
        bottleneck_specificity=3, _bs="Water heating + residential water treatment; building-water, consumer-cyclical",
        rs_inflection=3,           _rs="RS 40, 3M -2.4% but 1M +10.1% — turning off a soft patch",
        theme_exposure=3,          _te="Building-water exposure diluted by consumer/China demand",
        revenue_growth=2,          _rg="Rev -1.9% YoY — soft on China + consumer",
        margin_durability=4,       _m="Durable water-heating margins",
        valuation_runway=4,        _v="P/E ~17 — cheapest building-envelope name, -23% off highs",
        institutional_13f=3,       _13f="Mid-cap ownership",
        catalyst_proximity=3,      _cat="Residential replacement cycle + water treatment cross-sell",
    ),

    # ── Bucket 4: Ag Resilience ──
    "DE": dict(
        bottleneck_specificity=4, _bs="Precision-ag machinery + guidance — water-efficient farming, dominant franchise",
        rs_inflection=4,           _rs="RS 80, 3M +10.6%, near 52w high (-5%) — strong momentum",
        theme_exposure=3,          _te="Precision ag helps drought efficiency, but broad ag-cycle exposure dilutes",
        revenue_growth=2,          _rg="Rev -11% YoY — deep in the ag-equipment down-cycle",
        margin_durability=4,       _m="Structurally higher trough margins than prior cycles",
        valuation_runway=2,        _v="P/E ~36 at a cyclical earnings trough — expensive on depressed EPS",
        institutional_13f=5,       _13f="Core industrial holding",
        catalyst_proximity=3,      _cat="Ag-cycle recovery + precision-ag adoption",
    ),
    "CTVA": dict(
        bottleneck_specificity=5, _bs="Drought-tolerant seed genetics + crop protection — seed-trait moat hard to substitute",
        rs_inflection=3,           _rs="RS 53, 3M +1.1%, 1M +11% — steady, basically at 52w high",
        theme_exposure=4,          _te="The biology of ag adaptation; highest-specificity ag name",
        revenue_growth=4,          _rg="Rev +11% YoY — strongest grower in the ag bucket",
        margin_durability=4,       _m="Improving seed/CP margins, royalty-like traits",
        valuation_runway=2,        _v="P/E ~47 — premium; the trait moat is priced in",
        institutional_13f=4,       _13f="Broadly owned ag major",
        catalyst_proximity=3,      _cat="Seed-trait launches + drought-year demand",
    ),
    "LNN": dict(
        bottleneck_specificity=5, _bs="Zimmatic center-pivot irrigation pure-play — literal drought water-delivery hardware",
        rs_inflection=5,           _rs="RS 93, 3M +14.6% — strong inflection off a weak year (-18% 1Y)",
        theme_exposure=5,          _te="Purest irrigation vehicle; direct drought-adaptation hardware",
        revenue_growth=2,          _rg="Rev -5% YoY — soft farm-income cycle",
        margin_durability=3,       _m="Moderate irrigation-equipment margins",
        valuation_runway=4,        _v="P/E ~23, P/S ~1.9 — reasonable for the pure-play, well off highs",
        institutional_13f=2,       _13f="Small-cap, thinner institutional ownership",
        catalyst_proximity=3,      _cat="Drought events + farm-income recovery drive pivot demand",
    ),
    "VMI": dict(
        bottleneck_specificity=4, _bs="Valley irrigation + infrastructure structures; irrigation blended with utility-poles",
        rs_inflection=1,           _rs="RS 100, 3M +40%, 1Y +68% — extreme run, extended at the top",
        theme_exposure=4,          _te="Irrigation pure-play blended with infrastructure structures",
        revenue_growth=3,          _rg="Rev +6.2% YoY — steady across both segments",
        margin_durability=4,       _m="Solid, improving margins",
        valuation_runway=2,        _v="P/E ~31 after a +68% 1Y run, near 52w high — extended",
        institutional_13f=3,       _13f="Mid-cap ownership",
        catalyst_proximity=3,      _cat="Irrigation + infrastructure (grid-structure) demand",
    ),
    "NTR": dict(
        bottleneck_specificity=3, _bs="Crop nutrients + ag retail; commodity-fertilizer cyclicality dilutes the angle",
        rs_inflection=1,           _rs="RS 7, 3M -13.7%, 1M -5.4% — lagging on fertilizer-price weakness",
        theme_exposure=3,          _te="Input-intensity of resilient yields, but commodity exposure",
        revenue_growth=5,          _rg="Rev +19% YoY — strongest print in the cohort (fertilizer-price led)",
        margin_durability=2,       _m="Commodity-fertilizer margins, cyclical",
        valuation_runway=4,        _v="P/E ~13, P/S ~1.2 — cheapest in the cohort on commodity earnings",
        institutional_13f=4,       _13f="Broadly owned ag major",
        catalyst_proximity=2,      _cat="Fertilizer-price cycle dependent",
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
    lines.append("# Climate Adaptation Infrastructure — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Long-only rubric v1 (capital_structure weighted 20 — on a yfinance seed it lands on the neutral fallback for all names; the water-vs-cyclical split is carried by bottleneck_specificity, theme_exposure, and valuation_runway). Source: `candidates.json`. Audit: `scoring_log.json`._")
    lines.append("")
    lines.append("## Ranked candidates")
    lines.append("")
    lines.append("| Rank | Ticker | Company | Bucket | Score | B | RS | TE | RG | M | V | 13F | Cap |")
    lines.append("|-----:|--------|---------|--------|-------|---|----|----|----|---|---|-----|-----|")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        c = r["components"]
        marker = " 🟢" if i <= 5 else " 🟡" if i <= 7 else ""
        lines.append(f"| {i}{marker} | **{ticker}** | {r['company']} | {r['bucket']} | **{r['normalized_100']:.1f}** | "
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
    lines.append("_Rubric legend: B=bottleneck specificity · RS=relative-strength inflection · TE=theme exposure · RG=revenue growth · M=margin durability · V=valuation runway · 13F=institutional flow · Cap=capital structure. See scoring_log.json for per-criterion rationales and the fundamental snapshot behind each score._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "climate_adaptation", "scored_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": today, "rubric_version": "v1",
        "weights": WEIGHTS, "score_types": SCORE_TYPE, "results": results,
    }
    (_HERE / "scoring_log.json").write_text(json.dumps(log, indent=2, default=str))
    hist = _HERE / "history"
    hist.mkdir(exist_ok=True)
    (hist / f"scoring_log_{today}.json").write_text(json.dumps(log, indent=2, default=str))
    md = render_md(results)
    (_HERE / "scoring.md").write_text(md, encoding="utf-8")
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    print(f"\n=== Climate Adaptation Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER " if i <= 5 else "WATCHING" if i <= 7 else "        "
        print(f"  {i:2d}. {tag}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
