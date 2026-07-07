"""
One-off scoring script for Nuclear Renaissance theme — applies the long-only
rubric to the 15 candidates in candidates.json. Writes scoring.md and
scoring_log.json (+ history snapshot) for the tracker.

Rubric mirrors Space Economy v1 (the "vehicles-wrong risk is central" long-only
rubric), because this theme shares that exact shape:
1. capital_structure weighted 20 — the renaissance may happen while value pools
   in private/sovereign hands or dilutive pre-revenue SMR SPACs. Balance-sheet
   quality is the guardrail that keeps us in real-revenue names.
2. revenue_growth reduced to 10 — durability of a licensed bottleneck beats raw
   growth when the universe includes hyper-growth pre-revenue developers that
   dilute shareholders.

Reuses the same compute_capital_structure_score and compute_13f_score patterns
as Space Economy — only the per-ticker SCORES dict and theme strings differ.

NOTE on capital_structure: the yfinance-only seed path does not populate
shares/debt/SBC/FCF fields (those arrive from FMP in the nightly refresh), so
capital_structure falls back to the neutral 3/5 for every name at init. The
per-ticker hand scores on the other seven criteria carry the differentiation
until the FMP refresh backfills balance-sheet data.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Nuclear Renaissance v1 — long-only, vehicles-risk-central) ────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # reduced — 3-5yr horizon, RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # reduced — durability > raw growth here
    "margin_durability":       5,   # reduced — FCF margin captured in CS
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,   # BUMPED — vehicles risk is central
    "catalyst_proximity":      0,   # zeroed — same logic as Space Economy v1
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
    shares-out YoY · debt YoY · SBC/rev · FCF margin. Neutral fallback (3/5)
    when the seed path leaves these None (FMP backfills on nightly refresh)."""
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
        rationale = "Capital structure data unavailable (yfinance seed) — neutral fallback; FMP backfills on nightly refresh"
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


# ─── Hand-scored 13F fallback per ticker (Nuclear-specific) ──
_HAND_SCORED_13F = {
    "LEU":4, "CCJ":5, "UEC":3, "UUUU":3, "NXE":3,
    "BWXT":4, "GEV":5, "CW":4, "MIR":3,
    "CEG":5, "TLN":4, "VST":5, "D":4,
    "SMR":2, "OKLO":3,
}


# ─── Per-ticker hand scores ───────────────────────────────
# Format: each entry has the 7 judgment+quant criteria as 1-5 scores plus
# one-line rationales (the `_xx` keys). `capital_structure` and
# `institutional_13f` are auto-computed.
SCORES = {
    # ── Bucket 1: Enrichment & HALEU ──
    "LEU": dict(
        bottleneck_specificity=5, _bs="Sole US-owned enricher; HALEU barely exists outside Russia — the 5/5 bottleneck of the whole theme",
        rs_inflection=4,           _rs="RS 57, 3M -4.9% — mild pullback off a huge 1Y run; asymmetric re-entry forming",
        theme_exposure=5,          _te="Pure enrichment / HALEU play; nothing dilutes the thesis",
        revenue_growth=3,          _rg="Rev +4.9% YoY — modest; the story is the $2.4B backlog + DOE contract, not trailing growth",
        margin_durability=4,       _m="Contracted fixed-price DOE work + LEU backlog underpin margins",
        valuation_runway=2,        _v="P/E 63, P/S 7.6 — richly priced; the bottleneck premium is largely in",
        institutional_13f=4,       _13f="Increasingly owned as the enrichment pure-play",
        catalyst_proximity=5,      _cat="Piketon HALEU milestones + DOE contract execution due near-term",
    ),

    # ── Bucket 2: Mining & Conversion ──
    "CCJ": dict(
        bottleneck_specificity=4, _bs="Tier-1 pounds + UF6 conversion scarcity + 49% Westinghouse — hardest-to-replace miner",
        rs_inflection=3,           _rs="RS 21, 3M -13.4% — cooled hard off highs; low-RS-but-positioned setup",
        theme_exposure=5,          _te="Purest large-cap fuel-cycle exposure; theme blue chip",
        revenue_growth=3,          _rg="Rev +7.1% YoY — steady; recontracting cycle is the driver, not trailing rev",
        margin_durability=4,       _m="Tier-1 cost curve + conversion scarcity protect margins",
        valuation_runway=2,        _v="P/E 93, P/S 12 — expensive on trailing; discounts a lot of price upside",
        institutional_13f=5,       _13f="Core institutional uranium holding",
        catalyst_proximity=4,      _cat="Recontracting at $85/lb + McArthur guidance + Westinghouse earnings",
    ),
    "UEC": dict(
        bottleneck_specificity=3, _bs="US ISR pounds — real but the most substitutable input in the stack",
        rs_inflection=1,           _rs="RS 7, 3M -22% — heavy drawdown, momentum broken",
        theme_exposure=5,          _te="Pure unhedged spot leverage on the deficit",
        revenue_growth=2,          _rg="Ramping production; trailing revenue tiny (P/S extreme), pre-scale",
        margin_durability=2,       _m="Unhedged spot exposure = high operating leverage both ways; thin now",
        valuation_runway=2,        _v="Negative P/E, P/S 259 — priced on future pounds, not current cash flow",
        institutional_13f=3,       _13f="Retail-heavy uranium name; some institutional",
        catalyst_proximity=3,      _cat="ISR hub restarts + spot-price moves",
    ),
    "UUUU": dict(
        bottleneck_specificity=4, _bs="White Mesa is the only operating conventional US uranium mill — real infra moat",
        rs_inflection=1,           _rs="RS 0, 3M -22.4% — worst momentum in the cohort after a +123% 1Y run",
        theme_exposure=4,          _te="Strong uranium exposure; rare-earth line dilutes purity but adds policy tailwind",
        revenue_growth=5,          _rg="Rev +112% YoY — fastest grower in cohort as milling + RE ramp",
        margin_durability=2,       _m="Milling economics improving but still thin; RE line unproven at scale",
        valuation_runway=2,        _v="P/E 42, P/S 40 — priced richly for a ramping small-cap",
        institutional_13f=3,       _13f="Mid retail/institutional mix",
        catalyst_proximity=3,      _cat="Milling throughput + RE separation milestones",
    ),
    "NXE": dict(
        bottleneck_specificity=3, _bs="World-class Rook I deposit but pre-production; substitutable until it pours",
        rs_inflection=2,           _rs="RS 14, 3M -17.5% — weak momentum; development-stage volatility",
        theme_exposure=5,          _te="Pure Athabasca development optionality on the deficit",
        revenue_growth=1,          _rg="Pre-revenue — no top line yet",
        margin_durability=1,       _m="No operating margins; cash burn through licensing/build",
        valuation_runway=2,        _v="$6.4B cap on an undeveloped mine — a lot of NAV pulled forward",
        institutional_13f=3,       _13f="Institutional uranium-development favorite",
        catalyst_proximity=3,      _cat="Licensing decision + FID — but timeline has slipped",
    ),

    # ── Bucket 3: OEM, Components & Services ──
    "BWXT": dict(
        bottleneck_specificity=5, _bs="Sole-source naval reactor manufacturer + Darlington BWRX-300 RPV + TRISO/HALEU — decades of unreplicable qualification",
        rs_inflection=3,           _rs="RS 43, 3M -8.3% but 1M +6.6% — turning back up; near 52w high",
        theme_exposure=4,          _te="Real, growing commercial-nuclear content; navy work anchors but isn't 'theme'",
        revenue_growth=4,          _rg="Rev +26% YoY — strong for a defense-grade manufacturer",
        margin_durability=4,       _m="Regulated/qualified monopoly margins; durable backlog",
        valuation_runway=3,        _v="P/E 53, P/S 5.3 — full but justified by the moat",
        institutional_13f=4,       _13f="Broadly owned defense/nuclear name",
        catalyst_proximity=4,      _cat="BWRX-300 component milestones + SMR fuel awards",
    ),
    "GEV": dict(
        bottleneck_specificity=4, _bs="Owns the lead Western SMR order book (BWRX-300: Darlington, Clinch River, Poland, Sweden)",
        rs_inflection=1,           _rs="RS 100, 3M +28%, 1Y +118% — peak momentum, near 52w high; extended entry",
        theme_exposure=2,          _te="Nuclear is a slice of a diversified power giant (gas turbines + grid dominate)",
        revenue_growth=4,          _rg="Rev +16% YoY — strong across the power complex",
        margin_durability=4,       _m="Improving margins as backlog converts; scale advantages",
        valuation_runway=1,        _v="P/E 34, P/S 7.9 after a doubling — priced for perfection",
        institutional_13f=5,       _13f="Institutional darling post-spin",
        catalyst_proximity=4,      _cat="BWRX-300 FIDs + gas-turbine order flow",
    ),
    "CW": dict(
        bottleneck_specificity=4, _bs="Sole-source reactor coolant pumps / valves / I&C across the fleet + AP1000 — every restart is aftermarket revenue",
        rs_inflection=2,           _rs="RS 86, 3M +14%, near 52w high — strong momentum, extended",
        theme_exposure=3,          _te="Nuclear content is real but diluted by broad defense/industrial portfolio",
        revenue_growth=4,          _rg="Rev +13% YoY — solid, backlog-driven",
        margin_durability=5,       _m="Best-in-class defense-industrial margins, very consistent",
        valuation_runway=2,        _v="P/E 58, P/S 8.1 near highs — quality fully priced",
        institutional_13f=4,       _13f="Broadly owned quality compounder",
        catalyst_proximity=4,      _cat="Restart/uprate aftermarket + AP1000 order content",
    ),
    "MIR": dict(
        bottleneck_specificity=4, _bs="Design-agnostic dosimetry/monitoring every reactor + fuel facility must buy; recurring razor-blade demand",
        rs_inflection=2,           _rs="RS 29, 3M -11%, 1Y -18% — lagging the cohort; underappreciated",
        theme_exposure=4,          _te="Nuclear-instrumentation pure-play; benefits from every reactor regardless of design",
        revenue_growth=5,          _rg="Rev +27.5% YoY — strongest steady grower among the OEMs",
        margin_durability=3,       _m="Recurring service mix improving; still integrating post-SPAC",
        valuation_runway=3,        _v="P/E 169 (post-SPAC amortization noise) but P/S 4.2 reasonable; runway if margins scale",
        institutional_13f=3,       _13f="Growing institutional coverage",
        catalyst_proximity=3,      _cat="New-build + restart instrumentation orders",
    ),

    # ── Bucket 4: Operators & Restarts ──
    "CEG": dict(
        bottleneck_specificity=4, _bs="Largest US unregulated nuclear fleet — a scarce, non-replicable asset; Crane restart is the execution proof",
        rs_inflection=3,           _rs="RS 36, 3M -9.7% — pulled back off highs; buying into weakness",
        theme_exposure=4,          _te="Fleet + restart + two signed hyperscaler PPAs — the operator flagship",
        revenue_growth=4,          _rg="Rev +64% YoY (Calpine deal + power prices)",
        margin_durability=4,       _m="Contracted PPAs + baseload fleet underwrite durable cash flow",
        valuation_runway=4,        _v="P/E 21 — most reasonable large-cap valuation in the theme after the pullback",
        institutional_13f=5,       _13f="Core institutional utility/AI-power holding",
        catalyst_proximity=5,      _cat="Crane 2027 restart milestones + additional hyperscaler PPAs",
    ),
    "TLN": dict(
        bottleneck_specificity=3, _bs="Susquehanna + AWS campus — real, but a single-site bet vs CEG's fleet",
        rs_inflection=2,           _rs="RS 93, 3M +15% — strong momentum near highs; extended",
        theme_exposure=4,          _te="Most concentrated nuclear-to-datacenter expression via the Amazon PPA",
        revenue_growth=5,          _rg="Rev +97% YoY as PPA + merchant power scale",
        margin_durability=3,       _m="Merchant exposure adds volatility outside the contracted MW",
        valuation_runway=4,        _v="P/E 12.6 — cheapest operator in the theme; real free cash flow",
        institutional_13f=4,       _13f="Hedge-fund favorite AI-power play",
        catalyst_proximity=4,      _cat="AWS campus buildout + further capacity monetization",
    ),
    "VST": dict(
        bottleneck_specificity=3, _bs="Comanche Peak nuclear is real but buried in a gas-heavy generation + retail mix",
        rs_inflection=4,           _rs="RS 64, 3M +4.1% — steady, mid-range; healthier entry than the momentum names",
        theme_exposure=2,          _te="Nuclear is a minority of generation; gas fleet dilutes the theme",
        revenue_growth=4,          _rg="Rev +43% YoY (Energy Harbor nuclear + power prices)",
        margin_durability=3,       _m="Retail + merchant mix; hedged but cyclical",
        valuation_runway=3,        _v="P/E 26 — fair; diversified cash flows",
        institutional_13f=5,       _13f="Top institutional independent-power holding",
        catalyst_proximity=3,      _cat="Data-center PPAs + capacity market prices",
    ),
    "D": dict(
        bottleneck_specificity=2, _bs="Regulated utility with a small rate-based nuclear slice — lowest specificity, diluted-utility reference",
        rs_inflection=4,           _rs="RS 79, 3M +11% near 52w high — steady defensive strength",
        theme_exposure=2,          _te="At the epicenter of Virginia DC load but nuclear is a minor part of a regulated book",
        revenue_growth=4,          _rg="Rev +23% YoY (rate base + load growth)",
        margin_durability=4,       _m="Regulated returns = durable, low-volatility margins",
        valuation_runway=3,        _v="P/E 20 — fair for a regulated utility; limited multiple upside",
        institutional_13f=4,       _13f="Broadly owned regulated utility",
        catalyst_proximity=3,      _cat="Virginia data-center rate cases + load-growth approvals",
    ),

    # ── Bucket 5: SMR Developers (SPECULATIVE) ──
    "SMR": dict(
        bottleneck_specificity=2, _bs="Only NRC-certified SMR design — real, but substitutable vs BWRX-300 + private rivals; revenue years out",
        rs_inflection=3,           _rs="RS 50, 3M -5.3% but 1M -21.7% — rolling over after a brutal 1Y (-74%)",
        theme_exposure=5,          _te="Pure SMR optionality on the 400 GW future",
        revenue_growth=1,          _rg="Rev -96% YoY — effectively pre-revenue; lumpy pilot work only",
        margin_durability=1,       _m="Deep losses; cash burn to first deployment",
        valuation_runway=2,        _v="Negative P/E, P/S 178 — priced on programs, not cash flow",
        institutional_13f=2,       _13f="Retail-dominated post-collapse; light institutional",
        catalyst_proximity=3,      _cat="ENTRA1/TVA program progress — but nothing near revenue",
    ),
    "OKLO": dict(
        bottleneck_specificity=2, _bs="Fast microreactor + fuel recycling — differentiated but unproven; substitutable optionality",
        rs_inflection=3,           _rs="RS 71, 3M +7.7% but 1M -20.5% — volatile; news-driven whipsaw",
        theme_exposure=5,          _te="Pure advanced-reactor optionality; DOE pilot darling",
        revenue_growth=1,          _rg="Pre-revenue — powerhouse-as-a-service model not yet earning",
        margin_durability=1,       _m="No revenue; certain dilution to fund INL build",
        valuation_runway=1,        _v="$9B cap on a pre-revenue developer — the most speculative valuation in the theme",
        institutional_13f=3,       _13f="Momentum + thematic institutional interest",
        catalyst_proximity=4,      _cat="INL first-power attempt + DOE pilot news flow near-term",
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
    lines.append("# Nuclear Renaissance — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Long-only rubric v1 (capital_structure weighted 20 — vehicles-wrong risk is central: renaissance can happen while value pools in private/sovereign/dilutive hands). Source: `candidates.json`. Audit: `scoring_log.json`._")
    lines.append("")
    lines.append("_Note: the yfinance seed leaves capital-structure fields empty, so `capital_structure` uses the neutral 3/5 fallback for every name at init; the FMP nightly refresh backfills it. Differentiation at init comes from the seven hand-scored criteria._")
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
    lines.append("Columns: B=bottleneck · RS=rel-strength inflection · TE=theme exposure · RG=rev growth · M=margin durability · V=valuation runway · 13F=institutional flow · Cap=capital structure.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("_Per-pick detail rendering to be added after the first scoring run is reviewed (matches the Space Economy scoring.md pattern)._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "nuclear_renaissance", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== Nuclear Renaissance Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:8s}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
