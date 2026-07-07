"""
One-off scoring script for Reshoring & Industrial Renaissance theme — applies
the long-only rubric to the 18 candidates in candidates.json. Writes scoring.md
and scoring_log.json for the tracker.

Rubric mirrors Space Economy v1 (long-only), with the same reasoning:
1. capital_structure weighted 20 — this theme screens on backlog visibility +
   balance-sheet quality, and several enabler names carry heavy acquisition-
   funded debt (EMR, FIX, PWR, CRH). Clean-balance-sheet compounders should be
   rewarded over levered roll-ups, so CS gets the heavy weight.
2. revenue_growth reduced to 10 — durability + pricing power beat raw growth
   when the cohort already re-rated on the reshoring narrative (STRL +91% rev
   but priced for perfection). rs_inflection reduced to 10 on the 3-5yr horizon.

Reuses the same compute_capital_structure_score and compute_13f_score patterns
as Space Economy — only the per-ticker SCORES dict and strings differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Reshoring v1 — long-only) ────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,   # pricing power / backlog-visibility moat
    "rs_inflection":          10,   # reduced — 3-5yr horizon, RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # reduced — durability > raw growth here
    "margin_durability":       5,
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,   # BUMPED — balance-sheet quality is central
    "catalyst_proximity":      0,   # zeroed — same logic as Space v1
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
    shares-out YoY · debt YoY · SBC/rev · FCF margin."""
    shares_yoy = cand.get("shares_growth_yoy")
    debt_yoy = cand.get("debt_growth_yoy")
    sbc_pct = cand.get("sbc_pct_revenue")
    fcf_margin = cand.get("fcf_margin")
    parts = []
    # Shares
    if shares_yoy is not None:
        s = float(shares_yoy)
        score, lbl = (5,"buyback") if s<=0 else (4,"minimal") if s<=0.02 else (3,"moderate") if s<=0.05 else (2,"significant") if s<=0.10 else (1,"heavy")
        parts.append(("Shares", score, f"{s*100:+.1f}% {lbl}"))
    else:
        parts.append(("Shares", 3, "n/a"))
    # Debt
    if debt_yoy is not None:
        d = float(debt_yoy)
        score, lbl = (5,"paying down") if d<=-0.10 else (4,"flat") if d<=0 else (3,"moderate") if d<=0.10 else (2,"significant") if d<=0.25 else (1,"heavy")
        parts.append(("Debt", score, f"{d*100:+.1f}% {lbl}"))
    else:
        parts.append(("Debt", 3, "n/a"))
    # SBC
    if sbc_pct is not None:
        p = float(sbc_pct)
        score, lbl = (5,"minimal") if p<=0.02 else (4,"modest") if p<=0.05 else (3,"moderate") if p<=0.10 else (2,"high") if p<=0.15 else (1,"very high")
        parts.append(("SBC", score, f"{p*100:.1f}% rev {lbl}"))
    else:
        parts.append(("SBC", 3, "n/a"))
    # FCF
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


# ─── Hand-scored 13F fallback per ticker (Reshoring-specific) ──
# Large-cap, broadly-owned industrials/materials score higher; smaller-cap and
# REITs mid. These are the "broadly institutionally owned?" proxy.
_HAND_SCORED_13F = {
    "PWR":5, "EME":4, "FIX":4, "MTZ":3, "PRIM":3, "STRL":4,
    "ROK":5, "EMR":5, "ETN":5, "CMI":5, "PH":5,
    "VMC":5, "MLM":5, "CRH":4, "BLDR":4,
    "PLD":5, "REXR":4, "STAG":3,
}


# ─── Per-ticker hand scores ───────────────────────────────
# Format: each entry has the 7 judgment+quant criteria as 1-5 scores plus
# one-line rationales (the `_xx` keys). `capital_structure` and
# `institutional_13f` are auto-computed. Grounded in real candidates.json data
# (prices/RS/valuation as of 2026-07-06 seed).
SCORES = {
    # ── Bucket 1: E&C / Contractors ──
    "PWR": dict(
        bottleneck_specificity=5, _bs="Largest specialty electrical/utility E&C; scarce-labor moat + multi-year backlog — the cleanest enabler",
        rs_inflection=3,           _rs="RS 76, 3M +20.2% — healthy momentum, not asymmetric",
        theme_exposure=5,          _te="Direct build-out exposure: grid, substations, factory power tie-ins",
        revenue_growth=4,          _rg="Rev +26.3% YoY — strong",
        margin_durability=3,       _m="Thin gross margin (15%) typical of E&C; backlog quality offsets",
        valuation_runway=2,        _v="P/E ~93 — richly valued, the narrative is in the price",
        institutional_13f=5,       _13f="Broadly owned large-cap industrial",
        catalyst_proximity=4,      _cat="Megaproject awards + book-to-bill above 1",
    ),
    "EME": dict(
        bottleneck_specificity=4, _bs="Mechanical + electrical construction for factories/DCs; record backlog",
        rs_inflection=4,           _rs="RS 24, 3M +4.2% — cooled off; low-RS-but-positioned setup (asymmetric)",
        theme_exposure=4,          _te="Direct factory/industrial mechanical + electrical build",
        revenue_growth=4,          _rg="Rev +19.7% YoY — strong",
        margin_durability=3,       _m="~19% gross margin, improving with mix",
        valuation_runway=4,        _v="P/E ~27 — most reasonable multiple among the growth E&C names",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=4,      _cat="Backlog conversion + buyback",
    ),
    "FIX": dict(
        bottleneck_specificity=4, _bs="Industrial HVAC + modular process piping; direct chip-fab exposure",
        rs_inflection=2,           _rs="RS 88, 3M +26.6%, 1Y +232% — extended, ran very hard",
        theme_exposure=4,          _te="Modular-mechanical niche with direct megaproject exposure",
        revenue_growth=2,          _rg="Rev +1.0% YoY — decelerated after huge prior-year run",
        margin_durability=4,       _m="~25% gross margin, best-in-class for mechanical E&C",
        valuation_runway=2,        _v="P/E ~52 after 232% 1Y — priced for perfection",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Backlog + megaproject cadence",
    ),
    "MTZ": dict(
        bottleneck_specificity=3, _bs="Diversified infra E&C — power delivery + comms + clean energy; more competitively bid",
        rs_inflection=3,           _rs="RS 65, 3M +13.2% — mid-high momentum",
        theme_exposure=3,          _te="Real but diffuse infra exposure",
        revenue_growth=5,          _rg="Rev +34.5% YoY — fastest ex-STRL",
        margin_durability=2,       _m="Thin ~13% gross margin",
        valuation_runway=2,        _v="P/E ~67 — full",
        institutional_13f=3,       _13f="Mid institutional",
        catalyst_proximity=3,      _cat="Power delivery + clean-energy awards",
    ),
    "PRIM": dict(
        bottleneck_specificity=3, _bs="Utility + industrial + renewables construction; smaller-cap, bid-competitive",
        rs_inflection=5,           _rs="RS 0, 3M -38.9%, -56% off high — deeply washed out; max asymmetric-entry setup",
        theme_exposure=3,          _te="Real utility/industrial exposure but diffuse",
        revenue_growth=2,          _rg="Rev -5.4% YoY — contracted",
        margin_durability=2,       _m="Thin ~10% gross margin",
        valuation_runway=5,        _v="P/E ~20 — cheapest E&C; heavily de-rated",
        institutional_13f=3,       _13f="Smaller-cap ownership",
        catalyst_proximity=3,      _cat="Turnaround / backlog recovery uncertain",
    ),
    "STRL": dict(
        bottleneck_specificity=4, _bs="Site development for DCs + manufacturing pads; highest-margin E&C in cohort",
        rs_inflection=1,           _rs="RS 100, 3M +72.2%, 1Y +203% — peak momentum, most extended name",
        theme_exposure=5,          _te="Site-dev pads for exactly the factories/DCs being built",
        revenue_growth=5,          _rg="Rev +91.6% YoY — cohort-leading",
        margin_durability=4,       _m="~23% gross margin, expanding — unusual for E&C",
        valuation_runway=1,        _v="P/E ~64 at all-time highs — priced for perfection",
        institutional_13f=4,       _13f="Increasingly institutional",
        catalyst_proximity=4,      _cat="Data-center + factory site-dev award cadence",
    ),

    # ── Bucket 2: Factory Automation & Capital Goods ──
    "ROK": dict(
        bottleneck_specificity=5, _bs="Purest US factory-automation play; installed-base + software attach lock-in",
        rs_inflection=2,           _rs="RS 94, 3M +32.7% — extended but automation re-rating supported",
        theme_exposure=4,          _te="Monetizes plants as they come online; shared with AI-DC/robotics",
        revenue_growth=3,          _rg="Rev +11.9% YoY — steady",
        margin_durability=5,       _m="~49% gross margin, high-quality recurring attach",
        valuation_runway=2,        _v="P/E ~50 — full for a mid-cycle automation name",
        institutional_13f=5,       _13f="Institutional automation staple",
        catalyst_proximity=3,      _cat="Reshored-plant equipping + software attach",
    ),
    "EMR": dict(
        bottleneck_specificity=4, _bs="Process automation + measurement; post-reshape pure-play automation",
        rs_inflection=3,           _rs="RS 47, 3M +7.9% — mid, room to run",
        theme_exposure=4,          _te="Process-industry automation for reshored chemical/energy plants",
        revenue_growth=2,          _rg="Rev +2.9% YoY — slow",
        margin_durability=5,       _m="~53% gross margin, strong FCF (17.9%)",
        valuation_runway=3,        _v="P/E ~33 — reasonable for the quality",
        institutional_13f=5,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Automation-capex cycle",
    ),
    "ETN": dict(
        bottleneck_specificity=4, _bs="Intelligent power management for factories + grid",
        rs_inflection=3,           _rs="RS 71, 3M +14.8% — solid momentum",
        theme_exposure=3,          _te="Real reshoring exposure but overlaps AI-DC theme heavily — shared sleeve",
        revenue_growth=4,          _rg="Rev +16.8% YoY — strong",
        margin_durability=4,       _m="~37% gross margin, healthy FCF",
        valuation_runway=2,        _v="P/E ~40 — full, crowded name",
        institutional_13f=5,       _13f="Mega-cap electrical staple",
        catalyst_proximity=3,      _cat="Power-management backlog",
    ),
    "CMI": dict(
        bottleneck_specificity=3, _bs="Engines + power gen + industrial; backup power for new plants — diffuse",
        rs_inflection=2,           _rs="RS 82, 3M +23.8%, 1Y +110% — extended",
        theme_exposure=2,          _te="Reshoring exposure is a slice; trucking/engine cycle dominates",
        revenue_growth=2,          _rg="Rev +2.7% YoY — slow",
        margin_durability=3,       _m="~26% gross margin",
        valuation_runway=3,        _v="P/E ~35 — fair",
        institutional_13f=5,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Power-gen + industrial demand",
    ),
    "PH": dict(
        bottleneck_specificity=4, _bs="Motion, fluid power, filtration — the plumbing of automated factories",
        rs_inflection=3,           _rs="RS 41, 3M +7.2% — mid, not extended",
        theme_exposure=3,          _te="Broad content per plant but aerospace/industrial diluted",
        revenue_growth=3,          _rg="Rev +10.6% YoY — steady",
        margin_durability=4,       _m="~37% gross margin, aerospace-quality; FCF 13.2%",
        valuation_runway=3,        _v="P/E ~36 — reasonable for the compounder",
        institutional_13f=5,       _13f="Institutional industrial staple",
        catalyst_proximity=3,      _cat="Factory motion/filtration content growth",
    ),

    # ── Bucket 3: Building Products / Materials ──
    "VMC": dict(
        bottleneck_specificity=5, _bs="Largest US aggregates producer; local-monopoly quarries — pure structural pricing power",
        rs_inflection=3,           _rs="RS 59, 3M +9.1% — mid, room to run",
        theme_exposure=4,          _te="Aggregates into every slab/road/pad of the build-out",
        revenue_growth=3,          _rg="Rev +7.4% YoY — volume steady, pricing compounds",
        margin_durability=4,       _m="~28% gross margin, above-inflation pricing power",
        valuation_runway=3,        _v="P/E ~36 — fair for the local-monopoly moat",
        institutional_13f=5,       _13f="Broadly owned materials staple",
        catalyst_proximity=3,      _cat="Construction volume + annual price resets",
    ),
    "MLM": dict(
        bottleneck_specificity=5, _bs="#2 US aggregates; same local-monopoly economics as VMC",
        rs_inflection=4,           _rs="RS 6, 3M +1.4% — deeply lagging cohort; low-RS asymmetric setup on a quality name",
        theme_exposure=4,          _te="Heavy infra + factory-slab aggregates exposure",
        revenue_growth=4,          _rg="Rev +17.2% YoY — strong for aggregates",
        margin_durability=5,       _m="~30% gross margin, FCF 13.3% — best of the materials names",
        valuation_runway=3,        _v="P/E ~38 — fair",
        institutional_13f=5,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Volume cycle + pricing power",
    ),
    "CRH": dict(
        bottleneck_specificity=4, _bs="Aggregates + cement + building products; largest US materials footprint (NYSE-primary)",
        rs_inflection=4,           _rs="RS 12, 3M +3.2% — lagging; low-RS-but-quality setup",
        theme_exposure=4,          _te="Diversified, scaled materials exposure to US build-out",
        revenue_growth=3,          _rg="Rev +9.1% YoY — steady",
        margin_durability=4,       _m="~36% gross margin, diversified",
        valuation_runway=5,        _v="P/E ~20 — cheapest quality materials name in cohort",
        institutional_13f=4,       _13f="Growing US institutional post NYSE move",
        catalyst_proximity=3,      _cat="US infra + factory materials demand",
    ),
    "BLDR": dict(
        bottleneck_specificity=3, _bs="Structural building products + distribution; more housing-levered than factory-levered",
        rs_inflection=3,           _rs="RS 18, 3M +4.1%, 1Y -34% — beaten down on housing cycle",
        theme_exposure=2,          _te="Reshoring exposure is indirect; residential construction dominates",
        revenue_growth=2,          _rg="Rev -10.1% YoY — housing-cycle contraction",
        margin_durability=3,       _m="~30% gross margin",
        valuation_runway=4,        _v="P/E ~31 but de-rated hard; cheap on normalized earnings",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=2,      _cat="Housing recovery dependent — off-thesis timing",
    ),

    # ── Bucket 4: Industrial REITs ──
    "PLD": dict(
        bottleneck_specificity=4, _bs="Largest logistics REIT; landlords the logistics + light-mfg footprint",
        rs_inflection=3,           _rs="RS 35, 3M +5.6% — mid; rate-sensitive",
        theme_exposure=3,          _te="Broad logistics landlording; some DC-conversion optionality overlap",
        revenue_growth=3,          _rg="Rev +8.3% YoY — steady rent growth",
        margin_durability=5,       _m="~76% gross margin, FCF 51.7% — REIT economics",
        valuation_runway=3,        _v="P/S ~14 rich but REIT-appropriate; re-leasing spreads support",
        institutional_13f=5,       _13f="Largest industrial REIT, broadly owned",
        catalyst_proximity=3,      _cat="Lease-roll mark-to-market + reshoring demand",
    ),
    "REXR": dict(
        bottleneck_specificity=4, _bs="Infill SoCal industrial; irreplaceable land, extreme re-leasing pricing power",
        rs_inflection=3,           _rs="RS 29, 3M +4.8%, 1Y -2.2% — lagging, rate-pressured",
        theme_exposure=3,          _te="Infill industrial land-scarcity premium; SoCal-concentrated",
        revenue_growth=2,          _rg="Rev -2.9% YoY — soft near-term, embedded mark-to-market ahead",
        margin_durability=5,       _m="~77% gross margin; SBC 10.1% is high for a REIT",
        valuation_runway=3,        _v="P/S ~8 — reasonable for infill quality",
        institutional_13f=4,       _13f="Well-owned mid-cap REIT",
        catalyst_proximity=3,      _cat="Lease-roll re-leasing spreads",
    ),
    "STAG": dict(
        bottleneck_specificity=3, _bs="Single-tenant industrial in secondary markets; more diffuse tenant base",
        rs_inflection=3,           _rs="RS 53, 3M +8.1% — mid",
        theme_exposure=3,          _te="Secondary-market distribution/manufacturing landlord",
        revenue_growth=3,          _rg="Rev +9.1% YoY — steady",
        margin_durability=5,       _m="~80% gross margin, FCF 52.9% — clean REIT economics",
        valuation_runway=4,        _v="P/S ~9, higher yield — reasonable",
        institutional_13f=3,       _13f="Mid institutional",
        catalyst_proximity=3,      _cat="Rent escalators + occupancy",
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
    lines.append("# Reshoring & Industrial Renaissance — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Rubric v1 long-only (capital_structure weighted 20 — balance-sheet quality central; several enablers carry acquisition-funded debt). Source: `candidates.json`. Audit: `scoring_log.json`._")
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
    lines.append("Columns: B=bottleneck/pricing-power · RS=rel-strength inflection · TE=theme exposure · RG=rev growth · M=margin durability · V=valuation runway · 13F=institutional · Cap=capital structure (auto).")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("_Top ~5-7 by score promote to the tracker (`_tracker_init.py`). Per-pick detail rendering to be added after first review, matching AI DC scoring.md._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "reshoring", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== Reshoring Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:8s}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
