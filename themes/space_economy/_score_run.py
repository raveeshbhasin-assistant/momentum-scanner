"""
One-off scoring script for Space Economy theme — applies the v1 rubric
to the 22 candidates in candidates.json. Writes scoring.md and
scoring_log.json for the tracker.

Rubric differs from AI Data Center in two ways:
1. capital_structure weighted 20 (vs 10 for AI DC) — vehicles risk is
   CENTRAL for this theme, not tail. The "real revenue today" mandate
   from the locked thesis demands this weight.
2. revenue_growth reduced to 10 (vs 15 for AI DC) — durability beats
   raw growth when the universe includes SPAC-burns that grow revenue
   while diluting shareholders.

Reuses the same compute_capital_structure_score and compute_13f_score
patterns as AI Data Center — only the per-ticker SCORES dict and
WEIGHTS differ. Could be refactored into a shared library later.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Space Economy v1) ────────────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # reduced — 3-5yr horizon, RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # reduced — durability > raw growth here
    "margin_durability":       5,   # reduced — FCF margin captured in CS
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,   # BUMPED — vehicles risk is central
    "catalyst_proximity":      0,   # zeroed — same logic as AI DC v3
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
    """Same logic as ai_data_center/_score_run.py — see that file for full
    component breakdown. 4-component avg: shares-out YoY · debt YoY · SBC/rev · FCF margin."""
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
    """Same logic as ai_data_center — auto-compute from tracker_live.json, fall back to hand-scored."""
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


# ─── Hand-scored 13F fallback per ticker (Space-specific) ──
_HAND_SCORED_13F = {
    "MRCY":4, "CW":4, "HEI":4, "MOG-A":3, "KTOS":3,
    "BAESY":3, "THLEY":3, "SAFRY":3,
    "IRDM":4, "VSAT":3, "LHX":5, "GILT":2, "CMTL":2, "MYNA":1,
    "LDOS":4, "CACI":4, "BAH":5, "PLTR":5,
    "RKLB":4, "NOC":5, "BWXT":4, "ASTSF":1,
}


# ─── Per-ticker hand scores ───────────────────────────────
# Format: each entry has the 7 judgment+quant criteria as 1-5 scores plus
# one-line rationales (the `_xx` keys). `capital_structure` and
# `institutional_13f` are auto-computed.
SCORES = {
    # ── Bucket 1: Mission-Critical Components ──
    "MRCY": dict(
        bottleneck_specificity=4, _bs="Rad-hard chips + secure processors; ~5 global competitors at this tier",
        rs_inflection=3,           _rs="RS 83 high; 3M +11.7% positive — momentum healthy but not asymmetric",
        theme_exposure=4,          _te="Substantial defense electronics + real space exposure",
        revenue_growth=3,          _rg="~10% YoY — steady",
        margin_durability=4,       _m="Solid mid-teens margins, expanding",
        valuation_runway=3,        _v="P/E ~50, P/S ~5 — full but not extreme for the moat",
        institutional_13f=4,       _13f="Broadly owned defense electronics",
        catalyst_proximity=3,      _cat="Earnings cadence + contract awards",
    ),
    "CW": dict(
        bottleneck_specificity=3, _bs="Diversified defense electronics; broader portfolio",
        rs_inflection=4,           _rs="RS 72, 3M +4.2% positive — mid-high with healthy momentum",
        theme_exposure=3,          _te="Space is meaningful but diluted by nuclear / defense / industrial",
        revenue_growth=3,          _rg="~10% growth",
        margin_durability=4,       _m="Strong margins, consistent",
        valuation_runway=3,        _v="P/E ~35 — reasonable",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Quarterly earnings",
    ),
    "HEI": dict(
        bottleneck_specificity=3, _bs="Specialty aerospace compounder — broad portfolio, exceptional execution",
        rs_inflection=3,           _rs="Mid RS — Heico is a quality compounder that rarely shows asymmetric setups",
        theme_exposure=2,          _te="Space is a small slice; mostly broader aerospace aftermarket",
        revenue_growth=4,          _rg="Consistent low-teens growth, very durable",
        margin_durability=5,       _m="Best-in-class specialty margins",
        valuation_runway=1,        _v="P/E ~70 — Heico is always expensive (high-quality always is)",
        institutional_13f=4,       _13f="Broadly owned quality compounder",
        catalyst_proximity=3,      _cat="Tuck-in acquisitions, earnings cadence",
    ),
    "MOG-A": dict(
        bottleneck_specificity=4, _bs="Specialized aerospace motion control + valves",
        rs_inflection=3,           _rs="RS 56, 3M -6.3% — mid with slight pullback (asymmetric setup forming)",
        theme_exposure=3,          _te="Aerospace + defense propulsion; space exposure meaningful",
        revenue_growth=3,          _rg="~10% growth",
        margin_durability=3,       _m="Moderate cyclical margins",
        valuation_runway=3,        _v="P/E ~30 — fair",
        institutional_13f=3,       _13f="Less institutional than mega-caps",
        catalyst_proximity=3,      _cat="Defense propulsion contracts",
    ),
    "KTOS": dict(
        bottleneck_specificity=3, _bs="Small sats + tactical drones + defense electronics; growing niche",
        rs_inflection=3,           _rs="Need RS data — likely mid range",
        theme_exposure=4,          _te="Real space-adjacent revenue + growing",
        revenue_growth=4,          _rg="~15% growth, accelerating",
        margin_durability=2,       _m="Thin defense services margins",
        valuation_runway=2,        _v="P/E high; valuation stretched for growth",
        institutional_13f=3,       _13f="Mid-cap defense play",
        catalyst_proximity=4,      _cat="Contract awards announcements",
    ),
    "BAESY": dict(
        bottleneck_specificity=3, _bs="UK defense prime; real space business",
        rs_inflection=4,           _rs="RS 50, 3M -8% — cooling-off period; classic low-RS-but-positioned setup",
        theme_exposure=3,          _te="Diluted but real space exposure",
        revenue_growth=3,          _rg="~5-7% organic + acquisitive",
        margin_durability=4,       _m="Improving margins, defense backlog protected",
        valuation_runway=4,        _v="Reasonable for European prime",
        institutional_13f=3,       _13f="Less US institutional given ADR",
        catalyst_proximity=3,      _cat="European defense spend ramp",
    ),
    "THLEY": dict(
        bottleneck_specificity=4, _bs="Thales Alenia Space JV — one of three major EU satellite primes",
        rs_inflection=3,           _rs="Data missing — neutral",
        theme_exposure=4,          _te="Real satellite OEM business",
        revenue_growth=3,          _rg="Steady mid-single-digit",
        margin_durability=3,       _m="Improving",
        valuation_runway=3,        _v="ADR pricing reasonable",
        institutional_13f=3,       _13f="ADR less liquid",
        catalyst_proximity=3,      _cat="EU defense awards",
    ),
    "SAFRY": dict(
        bottleneck_specificity=3, _bs="Propulsion + actuators; space exposure via propulsion",
        rs_inflection=3,           _rs="Need data",
        theme_exposure=2,          _te="Aerospace engines dominate; small space",
        revenue_growth=3,          _rg="Steady mid-single-digit",
        margin_durability=4,       _m="Strong margins from engine business",
        valuation_runway=3,        _v="Reasonable",
        institutional_13f=3,       _13f="ADR mid-liquid",
        catalyst_proximity=3,      _cat="Aerospace cycle dependent",
    ),

    # ── Bucket 2: Comms / Ground ──
    "IRDM": dict(
        bottleneck_specificity=5, _bs="L-band satellite comms near-monopoly; ~$800M real revenue, growing",
        rs_inflection=2,           _rs="RS 94, +80.7% 3M — extended; ran hard but momentum supported by FCF",
        theme_exposure=5,          _te="Pure-play space comms; thesis flagship",
        revenue_growth=3,          _rg="High-single-digit recent growth",
        margin_durability=4,       _m="Improving margins as scale builds",
        valuation_runway=3,        _v="P/E ~20s — reasonable for the quality but run-up tightens room",
        institutional_13f=4,       _13f="Becoming institutional darling",
        catalyst_proximity=4,      _cat="Government D2D + IoT contract cadence",
    ),
    "VSAT": dict(
        bottleneck_specificity=3, _bs="Post-Inmarsat broadband + IFC + maritime + gov; execution challenges",
        rs_inflection=1,           _rs="RS 89, +42% 3M, +536% 1Y — extreme run, extended",
        theme_exposure=5,          _te="Pure-play comms",
        revenue_growth=3,          _rg="Lumpy post-merger",
        margin_durability=2,       _m="Margin pressure from integration",
        valuation_runway=2,        _v="Debt-heavy and dilution risk",
        institutional_13f=3,       _13f="Mid-cap institutional ownership",
        catalyst_proximity=3,      _cat="Maritime + IFC contract awards",
    ),
    "LHX": dict(
        bottleneck_specificity=4, _bs="Tactical comms + space comms — more pure-play than other primes",
        rs_inflection=3,           _rs="Need RS — likely mid",
        theme_exposure=4,          _te="Real space comms business, less diluted than peers",
        revenue_growth=3,          _rg="~5% growth",
        margin_durability=4,       _m="Strong, expanding post-merger synergies",
        valuation_runway=4,        _v="P/E reasonable for defense",
        institutional_13f=5,       _13f="Top defense ETF holding",
        catalyst_proximity=3,      _cat="Defense procurement cycle",
    ),
    "GILT": dict(
        bottleneck_specificity=3, _bs="Israeli ground systems / VSAT terminals / IFC components",
        rs_inflection=3,           _rs="RS 78 high, +10.3% 3M — momentum building but stretched",
        theme_exposure=4,          _te="Pure-play ground systems, $300M+ rev",
        revenue_growth=4,          _rg="Growing double-digits",
        margin_durability=3,       _m="Improving",
        valuation_runway=4,        _v="Cheap small cap",
        institutional_13f=2,       _13f="Less US institutional",
        catalyst_proximity=3,      _cat="Quarterly results",
    ),
    "CMTL": dict(
        bottleneck_specificity=2, _bs="Commoditized ground modems / microwave; troubled execution",
        rs_inflection=1,           _rs="Troubled name",
        theme_exposure=4,          _te="Pure-play but small",
        revenue_growth=1,          _rg="Declining",
        margin_durability=1,       _m="Losses",
        valuation_runway=5,        _v="Trades very cheap — turnaround optionality",
        institutional_13f=2,       _13f="Micro-cap, retail-dominated",
        catalyst_proximity=2,      _cat="Turnaround uncertain",
    ),
    "MYNA": dict(
        bottleneck_specificity=4, _bs="Optical inter-satellite links pure-play (rare specificity)",
        rs_inflection=2,           _rs="Volatile; financial health uncertain",
        theme_exposure=5,          _te="True pure-play in critical sub-technology",
        revenue_growth=4,          _rg="Revenue growing but small base",
        margin_durability=1,       _m="Losses, cash burn",
        valuation_runway=2,        _v="Speculative",
        institutional_13f=1,       _13f="Tiny float",
        catalyst_proximity=4,      _cat="Hyperscaler / DoD design wins",
    ),

    # ── Bucket 3: Analytics ──
    "LDOS": dict(
        bottleneck_specificity=3, _bs="Largest defense IT; deep NRO/Space Force relationships",
        rs_inflection=3,           _rs="Need RS",
        theme_exposure=3,          _te="Diluted but high quality",
        revenue_growth=3,          _rg="Mid-single-digit",
        margin_durability=3,       _m="Stable",
        valuation_runway=4,        _v="P/E reasonable",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Government program awards",
    ),
    "CACI": dict(
        bottleneck_specificity=3, _bs="Defense IT / intel community / electronic warfare",
        rs_inflection=3,           _rs="Need RS",
        theme_exposure=3,          _te="Real defense space exposure but diluted",
        revenue_growth=4,          _rg="Above-peer growth",
        margin_durability=3,       _m="Stable",
        valuation_runway=4,        _v="Reasonable",
        institutional_13f=4,       _13f="Defense ETF holding",
        catalyst_proximity=3,      _cat="Contract awards",
    ),
    "BAH": dict(
        bottleneck_specificity=3, _bs="Government consulting + space mission support",
        rs_inflection=3,           _rs="Need RS",
        theme_exposure=3,          _te="Diluted; some space mission support exposure",
        revenue_growth=3,          _rg="Steady",
        margin_durability=4,       _m="Cleanest balance sheet of the three",
        valuation_runway=4,        _v="Reasonable",
        institutional_13f=5,       _13f="Strong institutional ownership",
        catalyst_proximity=3,      _cat="Government contract flow",
    ),
    "PLTR": dict(
        bottleneck_specificity=3, _bs="Foundry deployed across defense; NRO + Maven program wins",
        rs_inflection=5,           _rs="RS 61, +2% 3M — moderate inflection, not extended",
        theme_exposure=3,          _te="Broad applicability; space-specific revenue is share, not majority",
        revenue_growth=5,          _rg="Very strong growth, accelerating",
        margin_durability=4,       _m="Improving",
        valuation_runway=1,        _v="P/E 200+ — extreme",
        institutional_13f=5,       _13f="Institutional darling",
        catalyst_proximity=4,      _cat="DoD contract cadence; AI narrative",
    ),

    # ── Bucket 4: In-Space Logistics ──
    "RKLB": dict(
        bottleneck_specificity=4, _bs="One of two viable Western alternatives to SpaceX in small launch + Photon bus",
        rs_inflection=1,           _rs="RS 100, +85% 3M, +388% 1Y — extreme run, peak momentum",
        theme_exposure=5,          _te="Pure-play space company with real revenue",
        revenue_growth=5,          _rg="~80% YoY revenue growth",
        margin_durability=1,       _m="Negative margins; -53.5% FCF margin",
        valuation_runway=1,        _v="Priced for perfection at peak",
        institutional_13f=4,       _13f="Hedge fund favorite",
        catalyst_proximity=4,      _cat="Neutron launch cadence + Space Force awards",
    ),
    "NOC": dict(
        bottleneck_specificity=4, _bs="Only commercial in-orbit servicer (SpaceLogistics MEV); B-21; strategic systems",
        rs_inflection=3,           _rs="Defense prime — moderate RS, steady",
        theme_exposure=2,          _te="Space slice is small fraction of total revenue",
        revenue_growth=3,          _rg="~5% growth",
        margin_durability=4,       _m="Strong, expanding",
        valuation_runway=4,        _v="P/E reasonable",
        institutional_13f=5,       _13f="Top defense holding",
        catalyst_proximity=3,      _cat="B-21 ramp + procurement",
    ),
    "BWXT": dict(
        bottleneck_specificity=4, _bs="Nuclear propulsion for space (DARPA DRACO) + navy reactor monopoly",
        rs_inflection=3,           _rs="RS 67, +2.3% 3M — mid",
        theme_exposure=2,          _te="Space rev is small slice; navy nuclear dominates",
        revenue_growth=3,          _rg="Steady",
        margin_durability=4,       _m="Strong, regulated monopoly margins",
        valuation_runway=3,        _v="P/E ~30 — fair for the moat",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="DRACO milestones + AI nuclear narrative overlap",
    ),
    "ASTSF": dict(
        bottleneck_specificity=5, _bs="Only Western pure-play in active debris removal",
        rs_inflection=2,           _rs="Volatile micro-cap; data sparse",
        theme_exposure=5,          _te="Pure-play in-space servicing",
        revenue_growth=4,          _rg="Growing but small base",
        margin_durability=1,       _m="Losses",
        valuation_runway=1,        _v="Speculative; pre-revenue scale",
        institutional_13f=1,       _13f="Tiny float, Japan ADR illiquid",
        catalyst_proximity=4,      _cat="Regulatory mandates for debris removal",
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
    lines.append("# Space Economy — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Rubric v1 (capital_structure weighted 20 — central to thesis). Source: `candidates.json`. Audit: `scoring_log.json`._")
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
    lines.append("_See AI Data Center scoring.md for the per-pick detail rendering pattern (we'll add it here after the first scoring run is reviewed)._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "space_economy", "scored_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": today, "rubric_version": "v1",
        "weights": WEIGHTS, "score_types": SCORE_TYPE, "results": results,
    }
    (_HERE / "scoring_log.json").write_text(json.dumps(log, indent=2, default=str))
    hist = _HERE / "history"
    hist.mkdir(exist_ok=True)
    (hist / f"scoring_log_{today}.json").write_text(json.dumps(log, indent=2, default=str))
    md = render_md(results)
    (_HERE / "scoring.md").write_text(md)
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    print(f"\n=== Space Economy Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "🟢 TRACKER" if i <= 5 else "🟡 WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
