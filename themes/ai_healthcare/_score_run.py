"""
One-off scoring script for AI in Healthcare theme — applies the long-only
rubric to the candidates in candidates.json. Writes scoring.md and
scoring_log.json for the tracker.

Rubric (v1) mirrors Space Economy's long-only weights, with the same
central conviction: for a "real-revenue-weighted" thesis whose primary
risk is *"AI is real but a commoditised feature, not a moat"*, the vehicles
risk is central, not tail. So:
- capital_structure weighted 20 — durability / dilution discipline matters
  because several names (TEM/GH/NTRA/SDGR/RXRX) are loss-making and could
  fund the AI build with dilution.
- revenue_growth reduced to 10 — the universe already skews high-growth
  (diagnostics volume names print 35-48% YoY); durability beats raw growth.
- valuation_runway weighted 15 — the flagship data names (TEM/GH/NTRA) trade
  at extended P/S and negative earnings; runway discipline is the swing factor.

Reuses the same compute_capital_structure_score and compute_13f_score
patterns as Space Economy — only the per-ticker SCORES dict, WEIGHTS, and
theme strings differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (AI in Healthcare v1) ─────────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # 3-5yr horizon — RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # reduced — universe already high-growth
    "margin_durability":       5,
    "valuation_runway":       15,   # data flagships trade extended — discipline matters
    "institutional_13f":       5,
    "capital_structure":      20,   # vehicles risk (commoditisation / dilution) is central
    "catalyst_proximity":      0,   # zeroed — 3-5yr durability bet, not event-driven
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
    shares-out YoY · debt YoY · SBC/rev · FCF margin. Neutral 3/5 fallback
    when FMP-sourced fields are absent (yfinance-only seed)."""
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
    """Same logic as space_economy — auto-compute from tracker_live.json,
    fall back to hand-scored."""
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


# ─── Hand-scored 13F fallback per ticker (AI-Healthcare-specific) ──
_HAND_SCORED_13F = {
    "VEEV":5, "DOCS":3, "IQV":4,
    "GEHC":4, "PHG":3, "ISRG":5, "IDXX":5,
    "TEM":3, "GH":4, "NTRA":4, "EXAS":3, "DGX":4,
    "TMO":5, "A":4, "SDGR":2, "RXRX":2,
}


# ─── Per-ticker hand scores ───────────────────────────────
# 7 judgment+quant criteria as 1-5 scores + one-line `_xx` rationales.
# capital_structure and institutional_13f are auto-computed.
# Quant criteria (rs_inflection, revenue_growth, valuation_runway) grounded
# in the real candidates.json fundamentals seeded 2026-07-06.
SCORES = {
    # ── Bucket 1: Clinical AI Workflow — Systems of Record ──
    "VEEV": dict(
        bottleneck_specificity=5, _bs="~80% life-sciences CRM share; AI Agents run inside the record it owns — hard-to-substitute",
        rs_inflection=3,           _rs="RS 50, 3M +10.4% — mid; -32% 1Y drawdown leaves room, momentum turning",
        theme_exposure=4,          _te="Workflow AI accrues to the system-of-record owner; VEEV is that owner",
        revenue_growth=3,          _rg="Rev +16.3% YoY — steady durable SaaS growth",
        margin_durability=5,       _m="Best-in-class software margins; FCF-rich",
        valuation_runway=3,        _v="P/E ~34, P/S ~9.4 — full but reasonable after the drawdown",
        institutional_13f=5,       _13f="Core software holding, broadly owned",
        catalyst_proximity=4,      _cat="AI Agents for Vault CRM rolling out through 2026",
    ),
    "DOCS": dict(
        bottleneck_specificity=4, _bs="Owns physician distribution AI startups must rent; 300k+ clinicians on AI tools/qtr",
        rs_inflection=2,           _rs="RS 21, -62% 1Y, 3M +0.4% — deeply out of favour; not an inflection yet",
        theme_exposure=4,          _te="Direct clinical-AI adoption story; but Epic first-party scribe is the watch item",
        revenue_growth=2,          _rg="Rev +5.1% YoY — decelerated hard, the source of the drawdown",
        margin_durability=4,       _m="High-margin network model, but growth stall pressures the multiple",
        valuation_runway=4,        _v="P/E ~23, P/S ~6.5 — cheapest it's been; drawdown reset valuation",
        institutional_13f=3,       _13f="Mid-cap, mixed ownership after the derating",
        catalyst_proximity=3,      _cat="AI-tool adoption metrics each quarter; Epic competitive response",
    ),
    "IQV": dict(
        bottleneck_specificity=4, _bs="Proprietary healthcare data + trial/commercial distribution; AI embeds on an un-buyable data asset",
        rs_inflection=4,           _rs="RS 64, 3M +18.9%, +26.8% 1Y — healthy momentum with room",
        theme_exposure=3,          _te="Real AI accretion across trials/analytics, but gradual not event-driven",
        revenue_growth=3,          _rg="Rev +8.4% YoY — steady services growth",
        margin_durability=4,       _m="Durable margins on scaled data/services franchise",
        valuation_runway=4,        _v="P/E ~26, P/S ~2.1 — reasonable for the moat, room to re-rate",
        institutional_13f=4,       _13f="Broadly owned CRO/data name",
        catalyst_proximity=3,      _cat="AI-embedded trial/commercial wins in quarterly cadence",
    ),

    # ── Bucket 2: Medical Imaging & Diagnostics AI ──
    "GEHC": dict(
        bottleneck_specificity=5, _bs="FDA AI-clearance leader (~120 radiology authorizations) on an un-buyable installed scanner base",
        rs_inflection=1,           _rs="RS 0, 3M -7.8%, -12.9% 1Y — worst momentum in the universe; deep value / falling knife",
        theme_exposure=4,          _te="Imaging AI is only deployable on the scanner base GEHC owns — high theme purity",
        revenue_growth=3,          _rg="Rev +7.4% YoY — steady hardware+recurring",
        margin_durability=4,       _m="Solid, improving med-device margins post-spin",
        valuation_runway=5,        _v="P/E ~15.5, P/S ~1.4 — cheapest quality name here; large valuation runway",
        institutional_13f=4,       _13f="Broadly owned large-cap med-tech",
        catalyst_proximity=4,      _cat="Continued FDA AI-clearance flow; installed-base AI upsell",
    ),
    "PHG": dict(
        bottleneck_specificity=4, _bs="~50 radiology AI clearances + informatics; sponsored ADR, real installed base",
        rs_inflection=3,           _rs="RS 36, 3M +7.9%, +22.7% 1Y — recovering off lows, mid momentum",
        theme_exposure=3,          _te="Real imaging AI position but turnaround/overhang dilutes the read",
        revenue_growth=2,          _rg="Rev -4.7% YoY — still working through the recall/turnaround",
        margin_durability=3,       _m="Recovering margins; execution risk remains",
        valuation_runway=3,        _v="P/E ~25, P/S ~1.6 — fair for a turnaround, not a bargain given growth",
        institutional_13f=3,       _13f="ADR, less US institutional than GEHC",
        catalyst_proximity=3,      _cat="Turnaround execution + AI clearance cadence",
    ),
    "ISRG": dict(
        bottleneck_specificity=5, _bs="da Vinci/Ion installed base is the moat; AI vision/analytics is upside on a razor/razorblade lock-in",
        rs_inflection=2,           _rs="RS 7, 3M -4.3%, -19% 1Y — extended-then-cooled; weak near-term momentum",
        theme_exposure=3,          _te="AI is optionality on top of a robotics franchise, not the core driver",
        revenue_growth=4,          _rg="Rev +23% YoY — strongest durable grower among the incumbents",
        margin_durability=5,       _m="Best-in-class recurring-revenue margins",
        valuation_runway=2,        _v="P/E ~53, P/S ~14.5 — premium leaves little runway even after pullback",
        institutional_13f=5,       _13f="Institutional core med-tech holding",
        catalyst_proximity=3,      _cat="Procedure-volume growth + AI-assisted case analytics rollout",
    ),
    "IDXX": dict(
        bottleneck_specificity=4, _bs="Vet diagnostic instrument lock-in + AI interpretation; recurring high-margin consumables",
        rs_inflection=2,           _rs="RS 14, 3M -0.8%, +3.6% 1Y — flat-to-weak momentum",
        theme_exposure=3,          _te="AI interpretation on an installed diagnostic base; vet niche insulates pricing",
        revenue_growth=3,          _rg="Rev +14.3% YoY — solid recurring growth",
        margin_durability=5,       _m="Exceptional razor/razorblade margins",
        valuation_runway=2,        _v="P/E ~42, P/S ~10 — quality always dear; limited runway",
        institutional_13f=5,       _13f="Widely owned quality compounder",
        catalyst_proximity=3,      _cat="Instrument placements + AI interpretation adoption",
    ),

    # ── Bucket 3: Healthcare Data & Diagnostics Infrastructure ──
    "TEM": dict(
        bottleneck_specificity=5, _bs="Multimodal molecular-data corpus that compounds with test volume — the un-replicable asset",
        rs_inflection=4,           _rs="RS 79, 3M +28.1% — strong momentum; -42% off 52w high leaves runway",
        theme_exposure=5,          _te="Purest expression of the thesis; AI-native at the model layer",
        revenue_growth=5,          _rg="Rev +36.1% YoY (2025 +83% headline) — fastest durable grower",
        margin_durability=2,       _m="Still loss-making (P/E negative); scaling toward profitability",
        valuation_runway=3,        _v="P/S ~8 with 36%+ growth — full but justifiable for the data moat",
        institutional_13f=3,       _13f="Newer listing, institutional base still forming",
        catalyst_proximity=5,      _cat="Data-moat compounding + reimbursement expansion; the theme's flagship catalyst",
    ),
    "GH": dict(
        bottleneck_specificity=4, _bs="Liquid-biopsy + oncology data moat; Smart Liquid Biopsy AI on Guardant360",
        rs_inflection=1,           _rs="RS 100, +240% 1Y, +80% 3M — extreme run, peak momentum, extended",
        theme_exposure=5,          _te="AI-native diagnostics data owner; high theme purity",
        revenue_growth=5,          _rg="Rev +48.3% YoY — fastest top-line grower in the universe",
        margin_durability=2,       _m="Loss-making (negative P/E); volume scaling not yet FCF-positive",
        valuation_runway=1,        _v="P/S ~20.7 after a triple — priced for perfection, minimal runway",
        institutional_13f=4,       _13f="Momentum darling, heavily accumulated",
        catalyst_proximity=4,      _cat="Screening (Shield) reimbursement + volume; the reimbursement path is the swing factor",
    ),
    "NTRA": dict(
        bottleneck_specificity=4, _bs="Signatera MRD moat on 250k+ patient dataset; AI trained on proprietary longitudinal data",
        rs_inflection=1,           _rs="RS 86, +79% 1Y, +36% 3M — extended run, near 52w high (-1.5%)",
        theme_exposure=5,          _te="MRD data owner; strong AI-native theme purity",
        revenue_growth=5,          _rg="Rev +38.8% YoY — MRD volume compounding",
        margin_durability=2,       _m="Loss-making (negative P/E); volume scaling toward breakeven",
        valuation_runway=1,        _v="P/S ~16.3 near highs — little valuation runway left",
        institutional_13f=4,       _13f="Heavily owned MRD leader",
        catalyst_proximity=4,      _cat="Signatera coverage expansion + volume compounding",
    ),
    "EXAS": dict(
        bottleneck_specificity=3, _bs="Cologuard screening scale + reimbursement moat; more diagnostics-volume than AI-attributable",
        rs_inflection=3,           _rs="Price data unavailable at seed — neutral placeholder",
        theme_exposure=3,          _te="Screening scale with a data/coverage moat; modest AI attribution",
        revenue_growth=3,          _rg="Data unavailable at seed — neutral (historically ~teens growth)",
        margin_durability=2,       _m="Historically loss-making at the net line",
        valuation_runway=3,        _v="Data unavailable at seed — neutral",
        institutional_13f=3,       _13f="Broadly owned screening name",
        catalyst_proximity=3,      _cat="Cologuard/Shield-adjacent coverage; blood-based screening",
    ),
    "DGX": dict(
        bottleneck_specificity=3, _bs="Largest routine-lab data owner; AI is a slow margin lever on unmatched volume",
        rs_inflection=4,           _rs="RS 43, 3M +8.3%, +24.3% 1Y, near 52w high — steady positive momentum",
        theme_exposure=2,          _te="Low theme purity — AI is an operating-margin lever, not a growth driver; reserve/anchor",
        revenue_growth=3,          _rg="Rev +9.2% YoY — steady, acquisition-aided",
        margin_durability=4,       _m="Stable, defensible lab-scale margins",
        valuation_runway=4,        _v="P/E ~24, P/S ~2.1 — reasonable defensive valuation",
        institutional_13f=4,       _13f="Broadly owned defensive lab name",
        catalyst_proximity=2,      _cat="AI-driven operating efficiency; gradual, not event-driven",
    ),

    # ── Bucket 4: AI Drug Discovery & Research Tools ──
    "TMO": dict(
        bottleneck_specificity=3, _bs="Picks-and-shovels to every AI-discovery lab; real revenue, not a clinical bet",
        rs_inflection=2,           _rs="RS 29, 3M +5.4%, +24% 1Y — mild momentum, lagging the tape",
        theme_exposure=2,          _te="Diffuse AI exposure; a safe way to hold discovery upside, low purity",
        revenue_growth=3,          _rg="Rev +6.2% YoY — steady tools/instruments growth",
        margin_durability=5,       _m="Durable scaled tools-and-services margins",
        valuation_runway=4,        _v="P/E ~28, P/S ~4.3 — reasonable for the quality franchise",
        institutional_13f=5,       _13f="Mega-cap institutional core",
        catalyst_proximity=2,      _cat="Bioprocessing recovery; AI-lab demand is diffuse tailwind",
    ),
    "A": dict(
        bottleneck_specificity=3, _bs="Analytical instrument + informatics base that AI-discovery workflows depend on",
        rs_inflection=4,           _rs="RS 57, 3M +13.3%, +10.4% 1Y — building momentum with room",
        theme_exposure=2,          _te="Low theme purity, high durability — instruments under discovery workflows",
        revenue_growth=3,          _rg="Rev +10% YoY — steady instrument/consumables growth",
        margin_durability=4,       _m="Strong, consistent instrument margins",
        valuation_runway=4,        _v="P/E ~26, P/S ~5.1 — fair for the franchise",
        institutional_13f=4,       _13f="Broadly owned life-science tools name",
        catalyst_proximity=2,      _cat="Instrument cycle recovery + AI-lab demand",
    ),
    "SDGR": dict(
        bottleneck_specificity=3, _bs="Physics + AI drug-design software with real licensing revenue; proprietary-pipeline optionality",
        rs_inflection=3,           _rs="RS 93, 3M +46.8% but -17.6% 1Y — sharp bounce off a low base, extended near-term",
        theme_exposure=4,          _te="High narrative AI exposure but loss-making pipeline bet — speculative sleeve",
        revenue_growth=2,          _rg="Rev -1.6% YoY — software revenue flat/declining",
        margin_durability=1,       _m="Deeply loss-making (negative P/E); pipeline cash burn",
        valuation_runway=3,        _v="P/S ~5 but no earnings — valuation is a pipeline option, not a fundamental",
        institutional_13f=2,       _13f="Smaller-cap, speculative ownership",
        catalyst_proximity=3,      _cat="Proprietary-pipeline readouts; software-bookings inflection",
    ),
    "RXRX": dict(
        bottleneck_specificity=2, _bs="Highest narrative exposure, weakest bottleneck; binary decade-long clinical risk",
        rs_inflection=3,           _rs="RS 71, 3M +27.3% but -19.7% 1Y — bounce off lows, still below trend",
        theme_exposure=5,          _te="Purest AI-discovery narrative; but exposure ≠ moat — smallest optionality only",
        revenue_growth=1,          _rg="Rev -56.1% YoY — collapsing/near-pre-revenue",
        margin_durability=1,       _m="Deeply loss-making, heavy cash burn",
        valuation_runway=1,        _v="P/S ~31.7 on shrinking revenue — extreme; a pure clinical option",
        institutional_13f=2,       _13f="Speculative small-cap ownership",
        catalyst_proximity=2,      _cat="AI-enabled clinical proof-of-concept readouts; binary",
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
    lines.append("# AI in Healthcare — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Rubric v1 (long-only; capital_structure & valuation weighted for the commoditisation/dilution risk that is central to this thesis). Source: `candidates.json`. Audit: `scoring_log.json`._")
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
    lines.append("Columns: B=bottleneck_specificity · RS=rs_inflection · TE=theme_exposure · RG=revenue_growth · M=margin_durability · V=valuation_runway · 13F=institutional · Cap=capital_structure (auto).")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("_Capital-structure column is the neutral 3/5 fallback where the yfinance-only seed lacked FMP shares/debt/SBC/FCF fields; it refreshes once the nightly FMP job folds this theme in._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "ai_healthcare", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== AI in Healthcare Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:9s}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
