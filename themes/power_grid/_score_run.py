"""
One-off scoring script for Power Grid Modernization theme — applies the v1
rubric to the 14 candidates in candidates.json. Writes scoring.md and
scoring_log.json for the tracker.

Rubric rationale (Power Grid v1) — differs from Space Economy in two ways:
1. valuation_runway BUMPED to 20 (vs 15). The central vehicles risk for THIS
   theme is not "the names don't exist" (they're all real, profitable,
   US-listed) — it's VALUATION. GEV / PWR / POWL have re-rated hard on the
   AI-power narrative. Paying a cyclical-peak multiple for a structural story
   is the thesis's #1 way to be wrong on returns, so valuation carries more
   weight here.
2. capital_structure reduced to 10 (vs 20 for Space). Every name here is a
   profitable, cash-generative industrial with a clean-enough balance sheet —
   the SPAC-dilution risk that made CS central for Space is simply absent.
   (Also: FMP fields are unavailable in this seed, so CS auto-scores neutral
   3/5 for all names via the shared fallback — weighting it heavily would just
   add noise.)

Reuses the same compute_capital_structure_score and compute_13f_score
patterns as Space Economy — only the per-ticker SCORES dict and WEIGHTS differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Power Grid v1) ───────────────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # 3-5yr horizon — RS matters but not dominant
    "theme_exposure":         15,
    "revenue_growth":         10,
    "margin_durability":       5,
    "valuation_runway":       20,   # BUMPED — valuation is the central risk here
    "institutional_13f":       5,
    "capital_structure":      10,   # reduced — all names profitable; FMP fields n/a
    "catalyst_proximity":      5,   # small weight — transformer lead-time catalyst is real
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
    when FMP fields are unavailable (as in the yfinance-only seed)."""
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
        rationale = "Capital structure data unavailable (yfinance-only seed) — neutral fallback"
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


# ─── Hand-scored 13F fallback per ticker (Power-Grid-specific) ──
_HAND_SCORED_13F = {
    "ETN":4, "GEV":5, "HUBB":4, "POWL":4, "NVT":4,
    "PWR":5, "MTZ":4, "MYRG":3, "PRIM":3, "EME":4,
    "ITRI":3, "DY":3, "ATKR":3, "ENS":3,
}


# ─── Per-ticker hand scores ───────────────────────────────
# 7 judgment+quant criteria as 1-5 scores plus one-line `_xx` rationales.
# capital_structure and institutional_13f are auto-computed.
# Fundamentals (px/pe/ps/RS/rev) are the real 2026-07-06 seed values.
SCORES = {
    # ── Bucket 1: Heavy Electrical Equipment ──
    "ETN": dict(
        bottleneck_specificity=5, _bs="Switchgear/breakers/UPS oligopoly; massive backlogs + pricing power",
        rs_inflection=4,           _rs="RS 38, 3M +14.8% — mid RS but constructive; not extended",
        theme_exposure=4,          _te="Direct T&D + data-center power; core equipment anchor (shared w/ AI DC)",
        revenue_growth=4,          _rg="+16.8% YoY — strong for a mega-cap industrial",
        margin_durability=5,       _m="Best-in-class, expanding electrical margins",
        valuation_runway=3,        _v="P/E ~41, P/S ~5.6 — full but justified by backlog visibility",
        institutional_13f=4,       _13f="Broadly owned industrial",
        catalyst_proximity=4,      _cat="Transformer/switchgear lead-time blowout is the direct catalyst",
    ),
    "GEV": dict(
        bottleneck_specificity=5, _bs="Purest large-scale grid-equipment maker (transformers/HVDC/orchestration)",
        rs_inflection=2,           _rs="RS 77, +28% 3M, +118% 1Y — extended; ran hard on AI-power narrative",
        theme_exposure=5,          _te="Purest public grid-equipment expression; flagship of the equipment bet",
        revenue_growth=4,          _rg="+16.3% YoY, accelerating",
        margin_durability=4,       _m="Rapidly improving post-spin margins",
        valuation_runway=2,        _v="P/E ~34 but P/S ~7.9 after +118% run — priced for continued perfection",
        institutional_13f=5,       _13f="Institutional darling post-spin",
        catalyst_proximity=5,      _cat="Direct transformer/HVDC lead-time + FERC transmission-reform catalyst",
    ),
    "HUBB": dict(
        bottleneck_specificity=3, _bs="Diversified utility T&D components + grid protection; genuinely T&D-facing",
        rs_inflection=2,           _rs="RS 15, 3M +0.6% — lagging; cooled off hard",
        theme_exposure=4,          _te="More T&D-utility-levered than most equipment names",
        revenue_growth=3,          _rg="+11.1% YoY — steady",
        margin_durability=4,       _m="Solid, consistent electrical margins",
        valuation_runway=4,        _v="P/E ~29, P/S ~4.4 — most reasonable of the big-3 equipment names",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Utility grid-component demand cadence",
    ),
    "POWL": dict(
        bottleneck_specificity=4, _bs="Custom engineered-to-order switchgear; long qualification cycles",
        rs_inflection=1,           _rs="RS 92, +36% 3M, +246% 1Y — extreme run, peak momentum",
        theme_exposure=4,          _te="Pure utility/industrial switchgear; narrow customer base but on-thesis",
        revenue_growth=2,          _rg="+6.5% YoY — modest given the multiple",
        margin_durability=3,       _m="Cyclical project margins, currently elevated",
        valuation_runway=1,        _v="P/E ~48, P/S ~8.0 after +246% 1Y — cyclical-peak pricing risk is highest here",
        institutional_13f=4,       _13f="Increasingly institutional small-cap",
        catalyst_proximity=4,      _cat="Switchgear backlog + data-center orders",
    ),
    "NVT": dict(
        bottleneck_specificity=3, _bs="Enclosures + connection/protection; component-level, competitive",
        rs_inflection=2,           _rs="RS 85, +33% 3M, +112% 1Y — extended",
        theme_exposure=3,          _te="Grid-hardening components; real but lower-specificity (shared w/ AI DC)",
        revenue_growth=5,          _rg="+53.5% YoY — very strong (acquisitions + organic)",
        margin_durability=3,       _m="Moderate, improving",
        valuation_runway=2,        _v="P/E ~53, P/S ~5.9 — full after the run",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=3,      _cat="Grid-hardening + data-center demand",
    ),

    # ── Bucket 2: T&D Construction / E&C ──
    "PWR": dict(
        bottleneck_specificity=4, _bs="Dominant US T&D contractor; trained-crew workforce moat + largest backlog",
        rs_inflection=3,           _rs="RS 54, +20% 3M — mid; constructive momentum, not overextended",
        theme_exposure=5,          _te="Purest large-cap T&D-construction expression; the labor-bottleneck flagship",
        revenue_growth=4,          _rg="+26.3% YoY — strong",
        margin_durability=3,       _m="Steady construction margins, mix improving toward higher-margin T&D",
        valuation_runway=2,        _v="P/E ~93 — richest in cohort; backlog visibility is the only justification",
        institutional_13f=5,       _13f="Top infrastructure ETF holding",
        catalyst_proximity=4,      _cat="Backlog conversion + interconnection-queue reform",
    ),
    "MTZ": dict(
        bottleneck_specificity=3, _bs="Power-delivery T&D growing fast; comms + pipeline dilute the mix",
        rs_inflection=2,           _rs="RS 31, +13% 3M — lagging the contractor cohort",
        theme_exposure=4,          _te="Real T&D leverage via power-delivery segment; some non-grid dilution",
        revenue_growth=4,          _rg="+34.5% YoY — strong",
        margin_durability=3,       _m="Improving as power-delivery mix grows",
        valuation_runway=3,        _v="P/E ~67 but P/S ~2.0 — cheaper on sales than PWR",
        institutional_13f=4,       _13f="Broadly owned infrastructure name",
        catalyst_proximity=4,      _cat="Power-delivery backlog ramp",
    ),
    "MYRG": dict(
        bottleneck_specificity=4, _bs="Near-pure-play T&D + C&I electrical construction; cleanest contractor exposure",
        rs_inflection=1,           _rs="RS 100, +55% 3M — top-of-cohort momentum, extended",
        theme_exposure=5,          _te="Highest-specificity T&D-construction pure-play; least diluted contractor",
        revenue_growth=3,          _rg="+20% YoY — solid",
        margin_durability=3,       _m="Small-cap contractor margins, cyclical but improving",
        valuation_runway=3,        _v="P/E ~49, P/S ~1.8 — reasonable on sales despite the run",
        institutional_13f=3,       _13f="Smaller-cap, less institutional",
        catalyst_proximity=4,      _cat="T&D backlog + utility MSA renewals",
    ),
    "PRIM": dict(
        bottleneck_specificity=3, _bs="Diversified E&C with expanding utility/power-delivery mix",
        rs_inflection=1,           _rs="RS 0, 3M -38.9% — sharp drawdown; either broken or the asymmetric entry",
        theme_exposure=3,          _te="Growing utility exposure but still diversified energy-infra",
        revenue_growth=2,          _rg="-5.4% YoY — recent revenue soft; the RS-0 explains itself",
        margin_durability=2,       _m="Thin, project-lumpy margins",
        valuation_runway=5,        _v="P/E ~20, P/S ~0.65 — cheapest in cohort; deep-value if backlog holds",
        institutional_13f=3,       _13f="Mid-cap, moderate ownership",
        catalyst_proximity=3,      _cat="Utility backlog conversion — but momentum broken; wait for stabilization",
    ),
    "EME": dict(
        bottleneck_specificity=3, _bs="Broad mechanical/electrical construction; grid exposure more diluted",
        rs_inflection=3,           _rs="RS 23, +4.2% 3M — lagging but stable; low-drama compounder",
        theme_exposure=3,          _te="Real electrical-construction leverage, diluted by facilities/mechanical",
        revenue_growth=4,          _rg="+19.7% YoY — strong",
        margin_durability=4,       _m="Best execution + margin discipline of the contractor group",
        valuation_runway=4,        _v="P/E ~26, P/S ~2.0 — reasonable for the execution quality",
        institutional_13f=4,       _13f="Broadly owned quality compounder",
        catalyst_proximity=3,      _cat="Electrical-construction backlog cadence",
    ),

    # ── Bucket 3: Grid-Edge Intelligence ──
    "ITRI": dict(
        bottleneck_specificity=3, _bs="Smart meters + grid-edge + distribution automation; incumbency + software moat",
        rs_inflection=2,           _rs="RS 8, 3M -2.2%, 1Y -35% — deeply out of favor; contrarian entry or value trap",
        theme_exposure=5,          _te="Cleanest grid-edge pure-play; NOT in AI DC — genuine incremental breadth",
        revenue_growth=2,          _rg="-3.3% YoY — soft; the RS/1Y drawdown reflects it",
        margin_durability=3,       _m="Recurring software mix improving margins",
        valuation_runway=5,        _v="P/E ~14, P/S ~1.6 — cheapest quality name in universe after the drawdown",
        institutional_13f=3,       _13f="Moderate ownership",
        catalyst_proximity=3,      _cat="Smart-meter refresh cycles + grid-modernization spend",
    ),
    "DY": dict(
        bottleneck_specificity=2, _bs="Primarily telecom/fiber line construction; expanding into utility/grid",
        rs_inflection=3,           _rs="RS 69, +23.8% 3M — constructive momentum",
        theme_exposure=2,          _te="Mostly telecom; grid-edge buildout exposure is emerging, not core",
        revenue_growth=5,          _rg="+56.1% YoY — very strong (telecom-led)",
        margin_durability=3,       _m="Steady specialty-contracting margins",
        valuation_runway=3,        _v="P/E ~41, P/S ~2.1 — full for a contractor",
        institutional_13f=3,       _13f="Moderate ownership",
        catalyst_proximity=3,      _cat="Fiber + utility line-construction demand",
    ),

    # ── Bucket 4: Cable / Wire & Components ──
    "ATKR": dict(
        bottleneck_specificity=2, _bs="Conduit/cable/busbar — commoditized, cyclical pricing",
        rs_inflection=3,           _rs="RS 46, +15% 3M — recovering off a weak 1Y (-2.4%)",
        theme_exposure=3,          _te="Direct buildout-volume exposure but low specificity (shared w/ AI DC)",
        revenue_growth=2,          _rg="+4.2% YoY — soft (pricing normalization off pandemic peak)",
        margin_durability=2,       _m="Margins normalizing down from cyclical peak",
        valuation_runway=5,        _v="P/E ~11, P/S ~0.83 — very cheap; deep-value volume play",
        institutional_13f=3,       _13f="Moderate ownership",
        catalyst_proximity=3,      _cat="Buildout tonnage + data-center conduit demand",
    ),
    "ENS": dict(
        bottleneck_specificity=2, _bs="Industrial stored energy + DC power systems; real but fungible",
        rs_inflection=3,           _rs="RS 62, +20% 3M, +144% 1Y — momentum strong",
        theme_exposure=3,          _te="Utility + telecom + grid-storage exposure; adjacent, not core equipment",
        revenue_growth=2,          _rg="+1.3% YoY — flat",
        margin_durability=3,       _m="Steady industrial margins",
        valuation_runway=4,        _v="P/E ~28, P/S ~2.1 — reasonable after the run",
        institutional_13f=3,       _13f="Moderate ownership",
        catalyst_proximity=3,      _cat="Grid-storage + backup-power demand",
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
    lines.append("# Power Grid Modernization — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Rubric v1 (valuation_runway weighted 20 — the central risk is paying "
                 f"a cyclical-peak multiple for a structural story). Source: `candidates.json`. Audit: `scoring_log.json`._")
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
    lines.append("B=bottleneck · RS=rs_inflection · TE=theme_exposure · RG=rev_growth · M=margin · V=valuation · 13F=institutional · Cap=capital_structure")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("_Capital-structure auto-scores neutral 3/5 across the board in this yfinance-only seed "
                 "(FMP shares/debt/SBC/FCF fields unavailable). Once folded into the nightly FMP refresh, "
                 "CS will differentiate. All names are profitable, cash-generative industrials — CS is "
                 "deliberately down-weighted (10) for this theme vs Space Economy (20)._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "power_grid", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== Power Grid Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:8s}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
