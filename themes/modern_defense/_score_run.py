"""Scoring v1 for Modern Defense — CS weight 20 like Space (vehicles risk medium-high).

Same rubric structure as Space _score_run.py. Per-ticker hand scores for the 21
candidates; capital_structure and institutional_13f auto-computed.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

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
    "bottleneck_specificity": "judgment", "rs_inflection": "quantitative",
    "theme_exposure": "judgment", "revenue_growth": "quantitative",
    "margin_durability": "mixed", "valuation_runway": "quantitative",
    "institutional_13f": "auto-from-13f", "capital_structure": "auto-from-fmp",
    "catalyst_proximity": "judgment",
}


def compute_capital_structure_score(ticker, cand):
    shares_yoy = cand.get("shares_growth_yoy"); debt_yoy = cand.get("debt_growth_yoy")
    sbc_pct = cand.get("sbc_pct_revenue"); fcf_margin = cand.get("fcf_margin")
    parts = []
    if shares_yoy is not None:
        s = float(shares_yoy)
        score, lbl = (5,"buyback") if s<=0 else (4,"minimal") if s<=0.02 else (3,"moderate") if s<=0.05 else (2,"significant") if s<=0.10 else (1,"heavy")
        parts.append(("Shares", score, f"{s*100:+.1f}% {lbl}"))
    else: parts.append(("Shares", 3, "n/a"))
    if debt_yoy is not None:
        d = float(debt_yoy)
        score, lbl = (5,"paying down") if d<=-0.10 else (4,"flat") if d<=0 else (3,"moderate") if d<=0.10 else (2,"significant") if d<=0.25 else (1,"heavy")
        parts.append(("Debt", score, f"{d*100:+.1f}% {lbl}"))
    else: parts.append(("Debt", 3, "n/a"))
    if sbc_pct is not None:
        p = float(sbc_pct)
        score, lbl = (5,"minimal") if p<=0.02 else (4,"modest") if p<=0.05 else (3,"moderate") if p<=0.10 else (2,"high") if p<=0.15 else (1,"very high")
        parts.append(("SBC", score, f"{p*100:.1f}% rev {lbl}"))
    else: parts.append(("SBC", 3, "n/a"))
    if fcf_margin is not None:
        f = float(fcf_margin)
        score, lbl = (5,"strong") if f>=0.20 else (4,"healthy") if f>=0.10 else (3,"modest") if f>=0.05 else (2,"thin") if f>=0 else (1,"negative")
        parts.append(("FCF margin", score, f"{f*100:.1f}% {lbl}"))
    else: parts.append(("FCF margin", 3, "n/a"))
    avg = round(sum(p[1] for p in parts) / 4)
    rationale = " · ".join(f"{name} {s}/5 ({lbl})" for name, s, lbl in parts)
    if all(p[1] == 3 for p in parts) and all(v is None for v in [shares_yoy, debt_yoy, sbc_pct, fcf_margin]):
        rationale = "Capital structure data unavailable — neutral fallback"
    return avg, rationale


def compute_13f_score(ticker):
    live_path = _HERE / "tracker_live.json"
    if not live_path.exists():
        return _HAND_SCORED_13F.get(ticker, 3), "13F data unavailable — hand-scored fallback"
    try: live = json.loads(live_path.read_text())
    except: return _HAND_SCORED_13F.get(ticker, 3), "13F load failed — hand-scored fallback"
    per = (live.get("per_ticker") or {}).get(ticker) or {}
    holders = per.get("13f_holders") or []
    if not holders: return _HAND_SCORED_13F.get(ticker, 3), "13F empty — hand-scored fallback"
    deltas = [(h.get("delta_pp") or 0) for h in holders if h.get("delta_pp") is not None]
    if not deltas: return 3, "No 13F deltas in data — neutral"
    net_pp = sum(deltas)
    positives = sum(1 for d in deltas if d > 0.05)
    negatives = sum(1 for d in deltas if d < -0.05)
    if net_pp >= 1.0 and positives >= 3: return 5, f"Strong accumulation: net +{net_pp:.1f}pp across {positives} funds"
    if net_pp >= 0.5: return 4, f"Modest accumulation: net +{net_pp:.1f}pp"
    if net_pp >= -0.5 and positives >= negatives: return 3, f"Neutral / mixed: net {net_pp:+.1f}pp"
    if net_pp >= -1.0: return 2, f"Modest trimming: net {net_pp:+.1f}pp"
    return 1, f"Sustained outflows: net {net_pp:+.1f}pp across {negatives} funds"


_HAND_SCORED_13F = {
    "LHX":5, "MRCY":4, "BAESY":3, "DRS":3, "RTX":4, "HII":3,
    "RNMBY":3, "NOC":5, "GD":5, "LMT":5,
    "AVAV":3, "ESLT":3, "SAABY":3, "KTOS":3, "TXT":4, "ONDS":1, "RCAT":1,
    "PLTR":5, "CACI":4, "LDOS":4, "BAH":5,
}

SCORES = {
    # Bucket 1: Counter-Drone / EW
    "LHX": dict(
        bottleneck_specificity=4, _bs="Most EW pure-play among defense primes; tactical comms + space comms",
        rs_inflection=4, _rs="RS 55, 3M -11.9% — moderate pullback creates asymmetric setup",
        theme_exposure=4, _te="EW + tactical comms = real defense exposure",
        revenue_growth=3, _rg="~5% — defense steady",
        margin_durability=4, _m="Strong, expanding post-merger",
        valuation_runway=4, _v="P/E reasonable for defense quality",
        institutional_13f=5, _13f="Top defense ETF holding",
        catalyst_proximity=3, _cat="Earnings cadence",
    ),
    "MRCY": dict(
        bottleneck_specificity=4, _bs="Rad-hard edge compute for autonomous systems — specialized",
        rs_inflection=2, _rs="RS 95 extended; already ran 99.5% YoY",
        theme_exposure=4, _te="Defense electronics + autonomous compute very relevant",
        revenue_growth=3, _rg="~10% YoY",
        margin_durability=4, _m="Solid mid-teens margins",
        valuation_runway=3, _v="P/E 60 — full",
        institutional_13f=4, _13f="Broadly owned",
        catalyst_proximity=3, _cat="Contract awards",
    ),
    "BAESY": dict(
        bottleneck_specificity=3, _bs="UK prime — EW + targeting + radar; diluted but real",
        rs_inflection=3, _rs="RS 70 mid, 3M -8% — moderate pullback",
        theme_exposure=4, _te="Real defense pure-play (vs space-only diluted)",
        revenue_growth=3, _rg="~5-7% organic + acquisitive",
        margin_durability=4, _m="Improving",
        valuation_runway=4, _v="Reasonable European prime",
        institutional_13f=3, _13f="Less US institutional",
        catalyst_proximity=3, _cat="European defense ramp",
    ),
    "DRS": dict(
        bottleneck_specificity=4, _bs="Counter-UAS + force protection + ISR; US arm of Leonardo",
        rs_inflection=2, _rs="RS 85 extended despite flat 1Y",
        theme_exposure=5, _te="Defense pure-play with real Army contracts",
        revenue_growth=4, _rg="Solid growth, accelerating",
        margin_durability=3, _m="Improving",
        valuation_runway=2, _v="P/E elevated for the growth",
        institutional_13f=3, _13f="Leonardo-controlled limits float",
        catalyst_proximity=4, _cat="C-UAS contract awards",
    ),
    "RTX": dict(
        bottleneck_specificity=2, _bs="Legacy kinetic — thesis explicitly says uneconomic vs cheap drones",
        rs_inflection=3, _rs="RS 50 mid, 3M -14% — pullback but thesis-misaligned",
        theme_exposure=2, _te="Commercial aero + Patriot dilution; wrong vehicle",
        revenue_growth=3, _rg="~5%",
        margin_durability=3, _m="Mixed across segments",
        valuation_runway=3, _v="Fair",
        institutional_13f=4, _13f="Top defense ETF holding",
        catalyst_proximity=3, _cat="Patriot orders (thesis-skeptical)",
    ),
    "HII": dict(
        bottleneck_specificity=2, _bs="Ship-based defense — thesis-misaligned legacy",
        rs_inflection=4, _rs="RS 30 low; deep pullback but wrong-vehicle context",
        theme_exposure=1, _te="Shipbuilding dominates — wrong category",
        revenue_growth=2, _rg="Slow",
        margin_durability=3, _m="Ship margins compressed",
        valuation_runway=4, _v="Cheap absolute but thesis-misaligned",
        institutional_13f=3, _13f="Defense ETF presence",
        catalyst_proximity=2, _cat="Navy program awards",
    ),

    # Bucket 2: Munitions
    "RNMBY": dict(
        bottleneck_specificity=5, _bs="Dominant European 155mm + propellant supplier; multi-year backlog",
        rs_inflection=5, _rs="RS 10 LOW with -34% shares BUYBACK + 14% FCF — extreme asymmetric setup",
        theme_exposure=5, _te="Pure-play allied munitions ramp; theme flagship",
        revenue_growth=5, _rg="Accelerating from Europe rearmament",
        margin_durability=4, _m="Expanding as ramp scales",
        valuation_runway=4, _v="Reasonable for the backlog visibility",
        institutional_13f=3, _13f="ADR less US-institutional",
        catalyst_proximity=4, _cat="Quarterly backlog disclosures",
    ),
    "NOC": dict(
        bottleneck_specificity=4, _bs="Munitions + propellants slice is real; otherwise diluted into B-21",
        rs_inflection=4, _rs="RS 25 low, 3M -22.8% — deep pullback / asymmetric",
        theme_exposure=3, _te="Munitions exposure within broader portfolio",
        revenue_growth=3, _rg="~5%",
        margin_durability=4, _m="Strong",
        valuation_runway=4, _v="Reasonable after pullback",
        institutional_13f=5, _13f="Top defense holding",
        catalyst_proximity=3, _cat="B-21 ramp + munitions awards",
    ),
    "GD": dict(
        bottleneck_specificity=3, _bs="Broad portfolio — armor + Stryker + ordnance + subs",
        rs_inflection=4, _rs="RS 75 high but 3M -3.3% — measured pullback",
        theme_exposure=3, _te="Munitions/armor real but diluted by subs + IT",
        revenue_growth=3, _rg="~5%",
        margin_durability=4, _m="Strong, consistent",
        valuation_runway=4, _v="Reasonable",
        institutional_13f=5, _13f="Defense blue-chip",
        catalyst_proximity=3, _cat="Munitions order flow",
    ),
    "LMT": dict(
        bottleneck_specificity=3, _bs="F-35 dominates + missiles; vehicles-wrong reference anchor",
        rs_inflection=4, _rs="RS 35 low, 3M -20.5% — deep pullback",
        theme_exposure=3, _te="Munitions (Javelin/HIMARS/GMLRS) real but diluted",
        revenue_growth=3, _rg="~5%",
        margin_durability=4, _m="Strong",
        valuation_runway=4, _v="Reasonable after pullback",
        institutional_13f=5, _13f="Top defense holding",
        catalyst_proximity=3, _cat="Sustainment + munitions contracts",
    ),

    # Bucket 3: Drones
    "AVAV": dict(
        bottleneck_specificity=4, _bs="Switchblade + Puma fielded; battle-tested DoD revenue",
        rs_inflection=5, _rs="RS 5 LOW after 3M -35% — extreme pullback / asymmetric setup",
        theme_exposure=5, _te="The ONE battle-tested US pure-play",
        revenue_growth=5, _rg="Strong growth from Switchblade ramp",
        margin_durability=2, _m="Thin / inconsistent",
        valuation_runway=4, _v="Compressed after drawdown",
        institutional_13f=3, _13f="Volatile fund ownership",
        catalyst_proximity=4, _cat="DDP awards + Switchblade restocking",
    ),
    "ESLT": dict(
        bottleneck_specificity=4, _bs="Israeli — Hermes UAVs + EW + ground systems; battle-tested",
        rs_inflection=2, _rs="RS 90 extended after +91% 1Y run",
        theme_exposure=5, _te="High allied-buyer concentration; pure thesis fit",
        revenue_growth=4, _rg="Strong allied demand",
        margin_durability=4, _m="Improving",
        valuation_runway=2, _v="Run-up tightens room",
        institutional_13f=3, _13f="ADR less US-institutional",
        catalyst_proximity=3, _cat="Allied procurement awards",
    ),
    "SAABY": dict(
        bottleneck_specificity=4, _bs="Swedish — NLAW + Carl-Gustaf + Gripen + Giraffe radar",
        rs_inflection=5, _rs="RS 20 LOW, 3M -28.5% — deep pullback / asymmetric",
        theme_exposure=5, _te="Europe-pure-play; rearmament beneficiary",
        revenue_growth=4, _rg="Accelerating Europe orders",
        margin_durability=3, _m="Improving",
        valuation_runway=4, _v="Compressed after drawdown",
        institutional_13f=3, _13f="ADR less US-institutional",
        catalyst_proximity=4, _cat="EU defense procurement",
    ),
    "KTOS": dict(
        bottleneck_specificity=3, _bs="Target drones + Valkyrie XQ-58 + tactical drones",
        rs_inflection=4, _rs="RS 0 (worst) but 1Y +48% — extreme pullback after run",
        theme_exposure=4, _te="Real drone exposure + growing customer base",
        revenue_growth=4, _rg="~15% accelerating",
        margin_durability=2, _m="Thin defense services margins",
        valuation_runway=3, _v="Compressed after pullback",
        institutional_13f=3, _13f="Mid-cap defense play",
        catalyst_proximity=4, _cat="Valkyrie contract milestones",
    ),
    "TXT": dict(
        bottleneck_specificity=2, _bs="Bell + AAI Shadow + Cessna; diluted industrial",
        rs_inflection=4, _rs="RS 60 mid, 3M -9.5% — moderate pullback",
        theme_exposure=2, _te="Drones small slice of conglomerate",
        revenue_growth=3, _rg="~3-5%",
        margin_durability=3, _m="Mixed across segments",
        valuation_runway=4, _v="Reasonable",
        institutional_13f=4, _13f="Broadly owned",
        catalyst_proximity=2, _cat="No specific drone catalyst",
    ),
    "ONDS": dict(
        bottleneck_specificity=2, _bs="Small drones + Replicator participant",
        rs_inflection=1, _rs="RS 100 extreme; +1042% 1Y dominantly speculative",
        theme_exposure=4, _te="Real defense interest",
        revenue_growth=5, _rg="High but on tiny base",
        margin_durability=1, _m="Cash burning",
        valuation_runway=1, _v="Trading on hype",
        institutional_13f=1, _13f="Retail-dominated micro-cap",
        catalyst_proximity=4, _cat="Replicator + contract awards",
    ),
    "RCAT": dict(
        bottleneck_specificity=2, _bs="Small drone manufacturer (SRR Tranche 2)",
        rs_inflection=2, _rs="RS 40 mid, 3M -16.5% — pullback",
        theme_exposure=4, _te="Real US Army drone contract",
        revenue_growth=4, _rg="Growing but small",
        margin_durability=1, _m="Losses",
        valuation_runway=2, _v="Speculative",
        institutional_13f=1, _13f="Micro-cap, retail",
        catalyst_proximity=4, _cat="SRR awards + contract execution",
    ),

    # Bucket 4: Software
    "PLTR": dict(
        bottleneck_specificity=3, _bs="Foundry + Maven program + NRO contracts",
        rs_inflection=4, _rs="RS 80 high but 3M only +2% — moderate momentum",
        theme_exposure=4, _te="Defense space and battlefield C2 is core",
        revenue_growth=5, _rg="Very strong, accelerating",
        margin_durability=4, _m="Improving",
        valuation_runway=1, _v="P/E 200+ — extreme",
        institutional_13f=5, _13f="Institutional darling",
        catalyst_proximity=4, _cat="DoD contract cadence",
    ),
    "CACI": dict(
        bottleneck_specificity=3, _bs="Defense IT + intelligence community + EW services",
        rs_inflection=4, _rs="RS 45 low, 3M -15% — pullback",
        theme_exposure=4, _te="Real defense exposure (vs Space-only diluted)",
        revenue_growth=4, _rg="Above-peer growth",
        margin_durability=3, _m="Stable",
        valuation_runway=4, _v="Reasonable after pullback",
        institutional_13f=4, _13f="Defense ETF holding",
        catalyst_proximity=3, _cat="Government awards",
    ),
    "LDOS": dict(
        bottleneck_specificity=3, _bs="Largest defense IT contractor",
        rs_inflection=4, _rs="RS 15 LOW, 3M -29.7% — deep pullback / asymmetric",
        theme_exposure=4, _te="Real defense IT exposure",
        revenue_growth=3, _rg="Mid-single-digit",
        margin_durability=3, _m="Stable",
        valuation_runway=4, _v="Cheap after drawdown",
        institutional_13f=4, _13f="Broadly owned",
        catalyst_proximity=3, _cat="Government program awards",
    ),
    "BAH": dict(
        bottleneck_specificity=3, _bs="Government consulting + mission support",
        rs_inflection=3, _rs="RS 65 mid, 3M -8.4% — measured pullback",
        theme_exposure=4, _te="Real defense IT exposure",
        revenue_growth=3, _rg="Steady",
        margin_durability=4, _m="Cleanest BS of three defense IT names",
        valuation_runway=4, _v="Reasonable",
        institutional_13f=5, _13f="Strong institutional ownership",
        catalyst_proximity=3, _cat="Government contract flow",
    ),
}


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
        components = {}; raw_total = 0
        auto_13f, auto_13f_rationale = compute_13f_score(ticker)
        auto_cap, auto_cap_rationale = compute_capital_structure_score(ticker, meta_by_ticker.get(ticker, {}))
        for crit, weight in WEIGHTS.items():
            if crit == "institutional_13f": score, rationale = auto_13f, auto_13f_rationale
            elif crit == "capital_structure": score, rationale = auto_cap, auto_cap_rationale
            else: score, rationale = s.get(crit), s.get(f"_{crit_short(crit)}", "")
            if score is None: continue
            contribution = score * weight
            raw_total += contribution
            components[crit] = {"score": score, "weight": weight, "type": SCORE_TYPE[crit], "contribution": contribution, "rationale": rationale}
        meta = meta_by_ticker.get(ticker, {})
        out[ticker] = {
            "company": meta.get("company"), "bucket": meta.get("bucket"),
            "sub": meta.get("sub"), "note": meta.get("note"),
            "specificity_meta": meta.get("specificity"),
            "raw_total": raw_total, "normalized_100": round(raw_total / 5, 1),
            "components": components,
            "snapshot": {k: meta.get(k) for k in
                          ["price","market_cap","pe","ps","1y_pct","3m_pct","1m_pct",
                           "dist_from_52w_high_pct","rs_3m","revenue_growth_yoy",
                           "shares_growth_yoy","debt_growth_yoy","sbc_pct_revenue","fcf_margin"]},
        }
    return out


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {"theme": "modern_defense", "scored_at": datetime.utcnow().isoformat() + "Z",
            "trade_date": today, "rubric_version": "v1",
            "weights": WEIGHTS, "score_types": SCORE_TYPE, "results": results}
    (_HERE / "scoring_log.json").write_text(json.dumps(log, indent=2, default=str))
    hist = _HERE / "history"; hist.mkdir(exist_ok=True)
    (hist / f"scoring_log_{today}.json").write_text(json.dumps(log, indent=2, default=str))
    md = "# Modern Defense — Scoring (v1)\n\n_Generated " + today + ". Rubric v1 (CS weight 20)._\n\n"
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    md += "| R | Ticker | Bucket | Score | CS |\n|--:|--------|--------|------:|---:|\n"
    for i, (tk, r) in enumerate(sorted_rows, 1):
        cs = r['components'].get('capital_structure', {}).get('score', '—')
        md += f"| {i} | **{tk}** | {r.get('bucket','—')[:18]} | {r['normalized_100']:.1f} | {cs}/5 |\n"
    (_HERE / "scoring.md").write_text(md)
    print(f"\n=== Modern Defense Scoring v1 ({today}) ===")
    for i, (tk, r) in enumerate(sorted_rows, 1):
        tag = "🟢 TRACKER" if i <= 5 else "🟡 WATCHING" if i <= 7 else "      "
        cs = r['components'].get('capital_structure', {}).get('score', '?')
        print(f"  {i:2d}. {tag}  {tk:6s} {r['normalized_100']:5.1f}  CS {cs}/5  {r.get('company','')} ({r.get('bucket','')})")


if __name__ == "__main__":
    main()
