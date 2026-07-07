"""
One-off scoring script for Rare Earth & Critical Minerals theme — applies the
long-only rubric to the 13 candidates in candidates.json. Writes scoring.md and
scoring_log.json (+ history snapshot) for the tracker.

Rubric (copied from space_economy v1, same long-only shape):
1. capital_structure weighted 20 — the "vehicles wrong" + junior-explorer
   dilution risk is CENTRAL for this theme, not tail. The "funded midstream, not
   a resource-and-a-dream" mandate from the locked thesis demands this weight.
2. revenue_growth reduced to 10 — durability + funded capacity beats raw growth
   when the universe includes pre-revenue juniors (USAR/LAC/TMC) that grow
   revenue off a tiny base while diluting, and cyclicals (ALB/SQM) whose growth
   is a commodity-price artifact.

Reuses the same compute_capital_structure_score and compute_13f_score patterns
as space_economy. When FMP capital-structure fields are absent (yfinance-only
seed) the CS score falls back to a neutral 3/5 — same behavior as the initial
glp1 seed. Only the per-ticker SCORES dict, WEIGHTS, and theme strings differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Rare Earth v1, long-only) ────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # reduced — 3-5yr horizon, RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # reduced — durability/funded capacity > raw growth
    "margin_durability":       5,   # reduced — FCF margin captured in CS
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,   # BUMPED — vehicles/dilution risk is central
    "catalyst_proximity":      0,   # zeroed — same logic as space_economy v1
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
    shares-out YoY · debt YoY · SBC/rev · FCF margin. Falls back to neutral
    3/5 when FMP fields are absent (yfinance-only seed)."""
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


# ─── Hand-scored 13F fallback per ticker (Rare-Earth-specific) ──
_HAND_SCORED_13F = {
    "MP":4, "USAR":3, "UUUU":3,
    "ALB":4, "SQM":3, "LAC":3, "SGML":2,
    "FCX":5, "SCCO":4, "VALE":4, "TECK":4,
    "CCJ":4, "TMC":2,
}


# ─── Per-ticker hand scores ───────────────────────────────
# Format: each entry has the 7 judgment+quant criteria as 1-5 scores plus
# one-line rationales (the `_xx` keys). `capital_structure` and
# `institutional_13f` are auto-computed. Judgments use the real fundamentals
# now in candidates.json (yfinance seed: CS fields absent → neutral CS fallback,
# so bottleneck/theme/valuation/RS do the differentiating work).
SCORES = {
    # ── Bucket 1: Separation & Magnets (HIGHEST PRIORITY) ──
    "MP": dict(
        bottleneck_specificity=5, _bs="Only US funded/permitted full chain: separation + Fort Worth magnets + DoD price floor + offtake — the exact China-monopoly step",
        rs_inflection=4,           _rs="RS 83, +6.6% 3M — healthy momentum, not extended; still -47% from 52w high leaves runway",
        theme_exposure=5,          _te="Pure-play flagship — the thesis in one ticker",
        revenue_growth=5,          _rg="+118% YoY as magnet/sep ramps (off small base, but funded capacity)",
        margin_durability=3,       _m="DoD ~$110/kg NdPr floor structurally caps downside; magnet margins ramping",
        valuation_runway=2,        _v="P/S ~27, P/E ~51 — priced for the ramp; the runway is volume not multiple",
        institutional_13f=4,       _13f="Institutional darling of the RE reshoring trade",
        catalyst_proximity=5,      _cat="DoD floor+offtake live; magnet-plant qualification milestones ahead",
    ),
    "USAR": dict(
        bottleneck_specificity=5, _bs="Pure-play NdFeB magnets + Round Top heavy-RE — the highest-value, hardest-to-substitute midstream step",
        rs_inflection=3,           _rs="RS 100, +21.5% 3M — peak momentum, extended; buying after a hot run",
        theme_exposure=5,          _te="Pure-play magnet + HREE — no dilution from other businesses",
        revenue_growth=3,          _rg="Pre-scale revenue — funded but commissioning still ahead",
        margin_durability=1,       _m="Pre-cash-flow; magnet plant not yet at qualified scale",
        valuation_runway=1,        _v="P/S ~645, P/E ~580 — extreme; entirely story/optionality priced",
        institutional_13f=3,       _13f="Newer listing, building institutional base",
        catalyst_proximity=4,      _cat="Stillwater magnet-plant commissioning + first OEM qualification are the catalysts",
    ),
    "UUUU": dict(
        bottleneck_specificity=4, _bs="Only US mill (White Mesa) doing both uranium and operating RE separation from monazite",
        rs_inflection=2,           _rs="RS 8, -22.4% 3M — cooling hard; either falling knife or asymmetric setup forming",
        theme_exposure=4,          _te="Real RE-separation optionality but diluted by uranium/vanadium",
        revenue_growth=4,          _rg="+112% YoY as uranium + RE ramp",
        margin_durability=2,       _m="Thin/volatile margins across uranium + early RE",
        valuation_runway=2,        _v="P/S ~40 — expensive for the diluted exposure",
        institutional_13f=3,       _13f="Mid-cap, retail-heavy uranium+RE crossover",
        catalyst_proximity=4,      _cat="RE-separation offtake + uranium price cycle",
    ),

    # ── Bucket 2: Lithium Chemistry (CYCLICAL) ──
    "ALB": dict(
        bottleneck_specificity=3, _bs="Largest lithium major + conversion; real assets but lithium resource is comparatively abundant",
        rs_inflection=2,           _rs="RS 0, -24.7% 3M — deep in the lithium-price down-cycle; cheap but no momentum",
        theme_exposure=3,          _te="Real critical-mineral exposure but commodity-price-dominated, diluted reshoring specificity",
        revenue_growth=3,          _rg="+33% YoY — recovering off the lithium trough",
        margin_durability=3,       _m="Low-cost position but full commodity-margin whipsaw",
        valuation_runway=4,        _v="P/E ~10, P/S ~2.9 — genuinely cheap at cycle-low, real cash flow",
        institutional_13f=4,       _13f="Broadly owned lithium bellwether",
        catalyst_proximity=3,      _cat="Lithium spot recovery is the swing factor",
    ),
    "SQM": dict(
        bottleneck_specificity=3, _bs="Lowest-cost Atacama brine lithium + specialty chem; abundant resource, cost moat",
        rs_inflection=3,           _rs="RS 33, -10.6% 3M — mid, less beaten-down than ALB",
        theme_exposure=3,          _te="Lithium + iodine/specialty; commodity-driven, Chile political overhang",
        revenue_growth=4,          _rg="+70% YoY off the trough",
        margin_durability=4,       _m="Lowest-cost brine gives best margin resilience in the group",
        valuation_runway=3,        _v="P/E ~26, P/S ~4 — fair; royalty/political discount embedded",
        institutional_13f=3,       _13f="ADR, moderate US institutional ownership",
        catalyst_proximity=3,      _cat="Lithium price + Chilean royalty/JV resolution",
    ),
    "LAC": dict(
        bottleneck_specificity=4, _bs="Thacker Pass — largest US lithium resource; DOE loan + GM offtake give real US-reshoring specificity",
        rs_inflection=3,           _rs="RS 58, -5.5% 3M — mid, holding up better than the lithium majors",
        theme_exposure=4,          _te="Purest US-lithium-reshoring vehicle but pre-major-revenue",
        revenue_growth=2,          _rg="Pre-production ramp — revenue not yet meaningful",
        margin_durability=1,       _m="Pre-cash-flow; negative earnings during build",
        valuation_runway=3,        _v="P/E negative (pre-production); optionality on the US-resource crown jewel",
        institutional_13f=3,       _13f="Owned for the Thacker Pass option",
        catalyst_proximity=4,      _cat="Phase-1 production ramp + DOE loan drawdown milestones",
    ),
    "SGML": dict(
        bottleneck_specificity=3, _bs="Low-cost Brazil hard-rock spodumene; real production but abundant hard-rock supply",
        rs_inflection=2,           _rs="RS 17, -13.9% 3M — weak, tracking lithium down-cycle",
        theme_exposure=3,          _te="Real lithium producer but foreign, commodity-driven",
        revenue_growth=2,          _rg="-11% YoY on lower lithium prices",
        margin_durability=3,       _m="Low-cost position cushions the cycle",
        valuation_runway=3,        _v="P/E ~7 low but P/S ~13 rich — mixed; takeover chatter is the wildcard",
        institutional_13f=2,       _13f="Smaller cap, takeover-speculation ownership",
        catalyst_proximity=3,      _cat="Lithium price recovery + persistent M&A chatter",
    ),

    # ── Bucket 3: Diversified / Copper (ANCHOR) ──
    "FCX": dict(
        bottleneck_specificity=4, _bs="Largest liquid US-listed copper pure-play — copper is the electrification/AI-power backbone metal",
        rs_inflection=4,           _rs="RS 75, ~flat 3M, only -16% from 52w high — best relative strength in the anchor sleeve",
        theme_exposure=4,          _te="High on the copper-demand axis, lower on the RE-reshoring axis; structural anchor",
        revenue_growth=3,          _rg="+9% YoY — steady",
        margin_durability=4,       _m="Real cash flow, quality copper assets, disciplined balance sheet",
        valuation_runway=3,        _v="P/E ~32, P/S ~3.3 — fair for the premier copper name",
        institutional_13f=5,       _13f="Core institutional copper holding",
        catalyst_proximity=3,      _cat="Copper price + electrification/AI-datacenter demand",
    ),
    "SCCO": dict(
        bottleneck_specificity=4, _bs="Lowest-cost, longest-reserve-life copper; structural copper-demand anchor",
        rs_inflection=3,           _rs="RS 67, ~flat 3M — steady, -21% from high",
        theme_exposure=4,          _te="High copper-demand exposure; Grupo Mexico control caps float",
        revenue_growth=3,          _rg="+36% YoY",
        margin_durability=5,       _m="Best-in-class low-cost copper margins in the industry",
        valuation_runway=2,        _v="P/S ~10, P/E ~29 — premium for the quality; limited multiple runway",
        institutional_13f=4,       _13f="Broadly owned, though controlled-company discount",
        catalyst_proximity=3,      _cat="Copper price + reserve-life premium",
    ),
    "VALE": dict(
        bottleneck_specificity=3, _bs="Nickel/copper give critical-metal exposure but iron ore dominates the P&L",
        rs_inflection=3,           _rs="RS 50, -6.8% 3M — mid, cheap and resilient",
        theme_exposure=3,          _te="Diluted critical-metal exposure inside an iron-ore major",
        revenue_growth=2,          _rg="+3% YoY — flattish on iron-ore prices",
        margin_durability=3,       _m="Real cash flow, high yield, but iron-ore-cycle exposed",
        valuation_runway=5,        _v="P/E ~23, P/S ~0.3 — deep-value, high dividend; Brazil/iron-ore discount",
        institutional_13f=4,       _13f="Widely held value/yield name",
        catalyst_proximity=3,      _cat="Iron ore + nickel/copper price + Brazil policy",
    ),
    "TECK": dict(
        bottleneck_specificity=3, _bs="Post-coal-spin copper-growth story (QB2 ramp) + zinc; clean-ish diversified base metals",
        rs_inflection=5,           _rs="RS 92, +16.5% 3M, only -14% from high — strongest momentum in the whole universe",
        theme_exposure=3,          _te="Copper-growth pivot gives real base-metal exposure; still diversified",
        revenue_growth=5,          _rg="+72% YoY as QB2 copper ramps",
        margin_durability=3,       _m="Improving as copper mix rises post coal spin-off",
        valuation_runway=4,        _v="P/E ~23, P/S ~2.4 — reasonable for a copper-growth re-rating story",
        institutional_13f=4,       _13f="Institutional copper-transition favorite",
        catalyst_proximity=4,      _cat="QB2 copper ramp + potential M&A interest",
    ),

    # ── Bucket 4: Strategic / Juniors (SPECULATIVE) ──
    "CCJ": dict(
        bottleneck_specificity=4, _bs="Largest Western uranium producer + fuel services — real strategic-supply-chain bottleneck with cash flow",
        rs_inflection=2,           _rs="RS 25, -13.4% 3M — cooling after a strong run; extended valuation being worked off",
        theme_exposure=3,          _te="Strategic-mineral overlap (nuclear fuel cycle), not core RE but same reshoring tailwind",
        revenue_growth=3,          _rg="+7% YoY — steady",
        margin_durability=4,       _m="Improving as uranium contracts re-price higher",
        valuation_runway=1,        _v="P/E ~93, P/S ~12 — richly valued on the uranium re-rating",
        institutional_13f=4,       _13f="Core institutional uranium holding",
        catalyst_proximity=4,      _cat="Uranium contract cycle + nuclear-buildout / reshoring policy",
    ),
    "TMC": dict(
        bottleneck_specificity=5, _bs="Seabed polymetallic nodules (Ni/Co/Cu/Mn) — extreme specificity IF it ever clears regulatory/technical bar",
        rs_inflection=3,           _rs="RS 42, -7.6% 3M, -36% 1Y — volatile optionality name",
        theme_exposure=4,          _te="Pure frontier-sourcing optionality; not a 3-5yr cash-flow vehicle",
        revenue_growth=1,          _rg="Pre-revenue — no commercial output",
        margin_durability=1,       _m="Pre-cash-flow, cash burn",
        valuation_runway=1,        _v="P/E negative; pure speculative option value",
        institutional_13f=2,       _13f="Retail-heavy, thin institutional base",
        catalyst_proximity=3,      _cat="ISA/NOAA seabed licensing decisions — binary and slow",
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
    lines.append("# Rare Earth & Critical Minerals — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Long-only rubric v1 (capital_structure weighted 20 — vehicles/dilution risk is central to thesis). Source: `candidates.json`. Audit: `scoring_log.json`._")
    lines.append("")
    lines.append("**Note:** this seed is yfinance-only, so capital-structure FMP fields (shares/debt/SBC/FCF) are absent and CS scores fall back to a neutral 3/5 for every name. Bottleneck specificity, theme exposure, valuation, and RS do the differentiating work until FMP data folds in via the nightly refresh.")
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
    lines.append("_B=bottleneck · RS=relative-strength inflection · TE=theme exposure · RG=revenue growth · M=margin durability · V=valuation runway · 13F=institutional · Cap=capital structure (auto)._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "rare_earth", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== Rare Earth Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:8s}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
