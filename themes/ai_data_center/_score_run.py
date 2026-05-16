"""
One-off scoring script for AI Data Center theme — applies the v1 rubric to
the 24 candidates in candidates.json. Writes scoring.md (human-readable) and
scoring_log.json (machine-readable performance-review record).

This script is theme-local (not a generic rubric engine). Each theme will
have its own _score_run.py until we generalize. Keeping it small and explicit
is intentional — scoring is a human judgment exercise, not an algorithm.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric ───────────────────────────────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          15,
    "theme_exposure":         15,
    "revenue_growth":         15,
    "margin_durability":      10,
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":       5,   # added v2 — share dilution + leverage + SBC + FCF
    "catalyst_proximity":      0,   # zeroed v2 — weakest signal, mostly redundant with earnings cadence
}
assert sum(WEIGHTS.values()) == 100

# Quantitative vs qualitative — for retrospective performance analysis
SCORE_TYPE = {
    "bottleneck_specificity": "judgment",
    "rs_inflection":          "quantitative",
    "theme_exposure":         "judgment",
    "revenue_growth":         "quantitative",
    "margin_durability":      "mixed",
    "valuation_runway":       "quantitative",
    "institutional_13f":      "auto-from-13f",     # from tracker_live.json
    "capital_structure":      "auto-from-fmp",     # from candidates.json fields
    "catalyst_proximity":     "judgment",
}


def compute_13f_score(ticker: str) -> tuple[int, str]:
    """
    Derive 1-5 score from tracker_live.json 13F deltas. Returns (score, rationale).
    Heuristic:
      - Sum positive deltas (funds adding) - sum negative deltas (funds trimming)
      - Net positive momentum from concentrated holders → high score
      - Net negative → low score
    Falls back to the hand-scored default if 13F data missing.
    """
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
        return _HAND_SCORED_13F.get(ticker, 3), "13F empty (likely no FMP key at refresh time) — hand-scored fallback"

    deltas = [(h.get("delta_pp") or 0) for h in holders if h.get("delta_pp") is not None]
    if not deltas:
        return 3, "No 13F deltas in data — neutral"

    net_pp = sum(deltas)
    positives = sum(1 for d in deltas if d > 0.05)  # > 5 bps
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


def compute_capital_structure_score(ticker: str, cand: dict) -> tuple[int, str]:
    """
    1-5 score based on 4 components, averaged. Higher = healthier capital structure.

    The bet: companies that grow revenue without diluting shareholders and without
    leveraging up deliver more per-share value over a 12-24 month horizon. Companies
    that fund growth via heavy SBC, equity raises, or debt issuance get penalized
    even when revenue looks fine.

    Components (each scored 1-5):
      1. Shares outstanding YoY growth  — lower is better (buybacks = 5)
      2. Total debt YoY growth          — lower is better
      3. SBC as % of revenue            — lower is better
      4. Free cash flow margin          — higher is better

    Returns (avg_score, rationale_string).
    """
    shares_yoy = cand.get("shares_growth_yoy")
    debt_yoy = cand.get("debt_growth_yoy")
    sbc_pct = cand.get("sbc_pct_revenue")
    fcf_margin = cand.get("fcf_margin")

    parts = []

    # 1. Shares outstanding YoY
    if shares_yoy is not None:
        s = float(shares_yoy)
        if s <= 0:        score_s, lbl_s = 5, "buyback"
        elif s <= 0.02:   score_s, lbl_s = 4, "minimal"
        elif s <= 0.05:   score_s, lbl_s = 3, "moderate"
        elif s <= 0.10:   score_s, lbl_s = 2, "significant"
        else:             score_s, lbl_s = 1, "heavy"
        parts.append(("Shares", score_s, f"{s*100:+.1f}% {lbl_s}"))
    else:
        score_s = 3
        parts.append(("Shares", 3, "n/a"))

    # 2. Total debt YoY
    if debt_yoy is not None:
        d = float(debt_yoy)
        if d <= -0.10:    score_d, lbl_d = 5, "paying down"
        elif d <= 0:      score_d, lbl_d = 4, "flat"
        elif d <= 0.10:   score_d, lbl_d = 3, "moderate"
        elif d <= 0.25:   score_d, lbl_d = 2, "significant"
        else:             score_d, lbl_d = 1, "heavy"
        parts.append(("Debt", score_d, f"{d*100:+.1f}% {lbl_d}"))
    else:
        score_d = 3
        parts.append(("Debt", 3, "n/a"))

    # 3. SBC as % of revenue
    if sbc_pct is not None:
        p = float(sbc_pct)
        if p <= 0.02:     score_sbc, lbl_sbc = 5, "minimal"
        elif p <= 0.05:   score_sbc, lbl_sbc = 4, "modest"
        elif p <= 0.10:   score_sbc, lbl_sbc = 3, "moderate"
        elif p <= 0.15:   score_sbc, lbl_sbc = 2, "high"
        else:             score_sbc, lbl_sbc = 1, "very high"
        parts.append(("SBC", score_sbc, f"{p*100:.1f}% rev {lbl_sbc}"))
    else:
        score_sbc = 3
        parts.append(("SBC", 3, "n/a"))

    # 4. FCF margin (higher is better)
    if fcf_margin is not None:
        f = float(fcf_margin)
        if f >= 0.20:     score_f, lbl_f = 5, "strong"
        elif f >= 0.10:   score_f, lbl_f = 4, "healthy"
        elif f >= 0.05:   score_f, lbl_f = 3, "modest"
        elif f >= 0:      score_f, lbl_f = 2, "thin"
        else:             score_f, lbl_f = 1, "negative"
        parts.append(("FCF margin", score_f, f"{f*100:.1f}% {lbl_f}"))
    else:
        score_f = 3
        parts.append(("FCF margin", 3, "n/a"))

    avg = round((score_s + score_d + score_sbc + score_f) / 4)
    rationale = " · ".join(f"{name} {s}/5 ({lbl})" for name, s, lbl in parts)

    # If no component data was available at all (all neutral 3s), flag it
    if all(p[1] == 3 for p in parts) and shares_yoy is None and debt_yoy is None and sbc_pct is None and fcf_margin is None:
        rationale = "Capital structure data unavailable — neutral fallback"

    return avg, rationale


# Hand-scored 13F fallback values per ticker (the original v1 scores).
# Used only when tracker_live.json doesn't have 13F data (sandbox / pre-deploy).
_HAND_SCORED_13F = {
    "AVGO": 4, "ANET": 4, "CLS": 3, "AAOI": 3, "LITE": 4, "COHR": 3,
    "FN": 3, "MRVL": 4, "CRDO": 3, "ALAB": 3, "AXTI": 2,
    "ETN": 5, "ABB": 4, "GEV": 5, "HUBB": 4, "VRT": 5, "POWL": 3,
    "NVT": 4, "ATKR": 3, "CLF": 3, "CRS": 3,
    "MOD": 3, "ECL": 4, "TT": 5, "FLEX": 3, "AAON": 3, "MMM": 3, "HON": 4,
}

# ─── Per-ticker scores (1-5 each) ──────────────────────────
# Each entry includes a one-line rationale per criterion. This is the audit
# trail — six months from now we'll come back and ask "did this hold up?"
SCORES = {
    # ── Bucket 1: Optical Networking ──
    "AVGO": dict(
        bottleneck_specificity=5, _bs="Switch silicon duopoly with MRVL; Tomahawk dominant",
        rs_inflection=4,           _rs="RS 48 mid-cohort, 1M +6.7% positive — healthy",
        theme_exposure=2,          _te="Only ~30% AI/networking after VMware acquisition; diversified",
        revenue_growth=4,          _rg="~30% YoY, sustained",
        margin_durability=5,       _m="Best-in-class gross margin, expanding",
        valuation_runway=2,        _v="P/E 82, P/S 29 — priced for continued AI tailwind",
        institutional_13f=4,       _13f="Mega-cap core holding broadly; placeholder",
        catalyst_proximity=4,      _cat="Earnings cadence; new Tomahawk/Jericho generations",
    ),
    "ANET": dict(
        bottleneck_specificity=4, _bs="Hyperscaler moat + software differentiation",
        rs_inflection=3,           _rs="RS 17 LOW but 1M -11.8% (breakdown risk)",
        theme_exposure=5,          _te="Data center switching is the entire business",
        revenue_growth=4,          _rg="~35% YoY",
        margin_durability=5,       _m="High and durable; capital-light",
        valuation_runway=3,        _v="P/E 49 / P/S 18 — reasonable given growth",
        institutional_13f=4,       _13f="Top hedge fund holding; placeholder",
        catalyst_proximity=3,      _cat="Hyperscaler capex commentary on each earnings call",
    ),
    "CLS": dict(
        bottleneck_specificity=3, _bs="Contract manufacturer — less moat",
        rs_inflection=3,           _rs="RS 39 mid, 1M -6.2% (cooling)",
        theme_exposure=3,          _te="HPS division is DC-relevant; rest is broader EMS",
        revenue_growth=5,          _rg="53% YoY — fastest in cohort outside CRDO",
        margin_durability=3,       _m="EMS-thin but expanding via HPS mix shift",
        valuation_runway=4,        _v="P/E 44 / P/S 3.0 — cheap relative to growth",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="HPS revenue disclosure improving",
    ),
    "AAOI": dict(
        bottleneck_specificity=3, _bs="Mid-tier transceiver player; share-gain dependent",
        rs_inflection=1,           _rs="RS 96 EXTENDED after 918% 1Y run",
        theme_exposure=4,          _te="DC optics core but business has pivoted before",
        revenue_growth=5,          _rg="51% YoY — recent acceleration",
        margin_durability=2,       _m="Historically volatile; thin",
        valuation_runway=1,        _v="P/E 40, P/S 30, after 918% 1Y — priced for perfection",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="Big hyperscaler program wins are episodic",
    ),
    "LITE": dict(
        bottleneck_specificity=4, _bs="Vertically integrated lasers + InP",
        rs_inflection=2,           _rs="RS 78 — high; 1M +8.9%",
        theme_exposure=4,          _te="Datacom + telecom; majority datacom now",
        revenue_growth=5,          _rg="90% YoY — extreme",
        margin_durability=3,       _m="Cyclical historically, currently expanding",
        valuation_runway=1,        _v="P/E 169 / P/S 30 — extreme",
        institutional_13f=4,       _13f="Broadly owned; placeholder",
        catalyst_proximity=3,      _cat="CPO risk over multi-year horizon",
    ),
    "COHR": dict(
        bottleneck_specificity=3, _bs="Broad portfolio post II-VI merger; less specific",
        rs_inflection=1,           _rs="RS 83 — high",
        theme_exposure=3,          _te="Datacom is one of multiple segments",
        revenue_growth=3,          _rg="20% YoY — moderate",
        margin_durability=4,       _m="Improving post-merger",
        valuation_runway=1,        _v="P/E 182 — extreme",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="Merger integration milestones",
    ),
    "FN": dict(
        bottleneck_specificity=4, _bs="Concentrated optical CM; few global peers at scale",
        rs_inflection=5,           _rs="RS 61, 1M +7.3% — exactly the middle-range + positive 1M sweet spot",
        theme_exposure=5,          _te="Pure-play optical contract manufacturer",
        revenue_growth=4,          _rg="39% YoY",
        margin_durability=2,       _m="CM margins are structurally thin",
        valuation_runway=3,        _v="P/E 62 / P/S 6 — fair given growth",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="Customer concentration (NVDA, CIEN) is also a risk",
    ),
    "MRVL": dict(
        bottleneck_specificity=5, _bs="PAM4 DSP duopoly; custom silicon programs",
        rs_inflection=1,           _rs="RS 91 — extended",
        theme_exposure=4,          _te="Large DC silicon exposure; auto + storage still meaningful",
        revenue_growth=3,          _rg="22% YoY — solid not stunning",
        margin_durability=4,       _m="Improving as DC mix grows",
        valuation_runway=3,        _v="P/E 57 — reasonable for the tier",
        institutional_13f=4,       _13f="Broadly owned; placeholder",
        catalyst_proximity=3,      _cat="Custom silicon program ramps",
    ),
    "CRDO": dict(
        bottleneck_specificity=4, _bs="AEC + SerDes; specialized niche, smaller competition",
        rs_inflection=5,           _rs="RS 57, 1M +8.3% — middle range with positive momentum",
        theme_exposure=5,          _te="Essentially all DC connectivity",
        revenue_growth=5,          _rg="201% YoY — highest in cohort",
        margin_durability=4,       _m="Improving rapidly as scale builds",
        valuation_runway=2,        _v="P/E 94 — priced for continued growth",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="Customer wins are the catalyst signal",
    ),
    "ALAB": dict(
        bottleneck_specificity=4, _bs="PCIe/CXL retimers; specialized",
        rs_inflection=1,           _rs="RS 87 — extended",
        theme_exposure=5,          _te="Pure DC connectivity play",
        revenue_growth=5,          _rg="93% YoY",
        margin_durability=4,       _m="Improving as recent IPO scales",
        valuation_runway=1,        _v="P/E 156 — extreme; recent IPO premium",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=4,      _cat="Lockup expirations + earnings cadence",
    ),
    "AXTI": dict(
        bottleneck_specificity=5, _bs="Upstream InP substrates — extreme physical bottleneck",
        rs_inflection=1,           _rs="RS 100 — most extended in cohort after +8207% 1Y",
        theme_exposure=3,          _te="InP is one of multiple substrate businesses (GaAs, Ge)",
        revenue_growth=4,          _rg="39% YoY",
        margin_durability=2,       _m="Historically thin; quality issues in past",
        valuation_runway=1,        _v="P/E 165, P/S 84 — extreme",
        institutional_13f=2,       _13f="Small cap; less institutional presence",
        catalyst_proximity=2,      _cat="Quality and supply ramp execution",
    ),

    # ── Bucket 2: Heavy Electrical ──
    "ETN": dict(
        bottleneck_specificity=5, _bs="MV transformer + switchgear flagship; 128w lead times",
        rs_inflection=5,           _rs="RS 22 LOW, 1M +2.0% positive — the asymmetry the thesis targets",
        theme_exposure=3,          _te="Eaton is a giant industrial; DC + utility is ~40-50%",
        revenue_growth=3,          _rg="17% YoY — steady not stunning",
        margin_durability=5,       _m="Strong, expanding margins",
        valuation_runway=4,        _v="P/E 39, P/S 5.4 — reasonable given pricing power + backlog",
        institutional_13f=5,       _13f="Heavily owned; many AI ETF inclusions",
        catalyst_proximity=3,      _cat="Backlog updates each quarter",
    ),
    "GEV": dict(
        bottleneck_specificity=4, _bs="Transformers + turbines + grid software — real exposure",
        rs_inflection=5,           _rs="RS 43, 1M +7.2% — middle-range with positive momentum",
        theme_exposure=3,          _te="Multi-segment; DC-relevant pieces are part of mix",
        revenue_growth=3,          _rg="16% YoY",
        margin_durability=4,       _m="Improving post-spin",
        valuation_runway=4,        _v="P/E 31, P/S 7.2 — fair given the re-rating opportunity",
        institutional_13f=5,       _13f="Spin-off + AI infrastructure ETF inclusion driving ownership",
        catalyst_proximity=4,      _cat="Post-spin re-rating + capex cycle commentary",
    ),
    "HUBB": dict(
        bottleneck_specificity=3, _bs="Diversified electrical components",
        rs_inflection=2,           _rs="RS 9 very low, 1M -8% — broken near-term",
        theme_exposure=3,          _te="Grid hardware; DC exposure secondary",
        revenue_growth=2,          _rg="11% YoY — slower",
        margin_durability=3,       _m="Stable",
        valuation_runway=4,        _v="P/E 28 — reasonable given the recent pullback",
        institutional_13f=4,       _13f="Broadly owned utility play; placeholder",
        catalyst_proximity=2,      _cat="No specific near-term",
    ),
    "VRT": dict(
        bottleneck_specificity=5, _bs="Spans Bucket 2 + Bucket 3 — single most concentrated thesis name",
        rs_inflection=2,           _rs="RS 74, 1M +26.1% — strongly trending but extended",
        theme_exposure=5,          _te="Pure-play data center power + thermal",
        revenue_growth=4,          _rg="30% YoY",
        margin_durability=4,       _m="Expanding",
        valuation_runway=2,        _v="P/E 93 / P/S 13 — priced",
        institutional_13f=5,       _13f="Top hedge fund / ETF holding for AI infrastructure",
        catalyst_proximity=4,      _cat="Liquid cooling product cycle, capex disclosures",
    ),
    "POWL": dict(
        bottleneck_specificity=3, _bs="Custom switchgear, narrower customer base",
        rs_inflection=3,           _rs="RS 70, 1M +25.7% — high but momentum",
        theme_exposure=4,          _te="Industrial + DC switchgear",
        revenue_growth=2,          _rg="6.5% YoY — slow",
        margin_durability=3,       _m="Cyclical",
        valuation_runway=2,        _v="P/E 57 / P/S 9.4 — extended for the growth",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="Order book disclosures",
    ),
    "NVT": dict(
        bottleneck_specificity=3, _bs="Cross-bucket but less concentrated than VRT",
        rs_inflection=3,           _rs="RS 65, 1M +30.5% — high but strong",
        theme_exposure=4,          _te="Containment + liquid cooling integration",
        revenue_growth=5,          _rg="54% YoY",
        margin_durability=4,       _m="Expanding via ECM acquisition",
        valuation_runway=3,        _v="P/E 57 / P/S 6.3 — reasonable given growth",
        institutional_13f=4,       _13f="Broadly owned; placeholder",
        catalyst_proximity=3,      _cat="Liquid cooling product launches",
    ),
    "ATKR": dict(
        bottleneck_specificity=2, _bs="Commoditized conduit / busbars",
        rs_inflection=4,           _rs="RS 30, 1M +10.9% — low/mid with positive momentum",
        theme_exposure=3,          _te="DC conduit is meaningful but secondary",
        revenue_growth=2,          _rg="4.2% YoY — slow",
        margin_durability=2,       _m="Margins under pressure",
        valuation_runway=5,        _v="P/E 12, P/S 0.9 — cheapest in cohort",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=2,      _cat="No specific near-term",
    ),
    "CLF": dict(
        bottleneck_specificity=5, _bs="GOES steel — upstream physical bottleneck",
        rs_inflection=4,           _rs="RS 13 very low, 1M +6.1% positive — asymmetry",
        theme_exposure=2,          _te="Electrical steel is small % of overall business",
        revenue_growth=2,          _rg="6.3% YoY — slow",
        margin_durability=2,       _m="Cyclical commodity exposure",
        valuation_runway=5,        _v="P/E 22, P/S 0.3 — very cheap",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="Tariff news, transformer-demand pricing",
    ),
    "CRS": dict(
        bottleneck_specificity=3, _bs="Specialty alloys; less GOES-specific than CLF",
        rs_inflection=2,           _rs="RS 26, 1M -4% — mid-low with negative momentum",
        theme_exposure=2,          _te="DC exposure is small slice of broader specialty",
        revenue_growth=2,          _rg="12% YoY",
        margin_durability=4,       _m="Improving via aerospace + specialty mix",
        valuation_runway=2,        _v="P/E 43 / P/S 6.7 — full",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=2,      _cat="No specific near-term",
    ),

    # ── Bucket 3: Thermal Management ──
    "ECL": dict(
        bottleneck_specificity=5, _bs="Becoming pure-play DTC cooling owner via $4.75B CoolIT acquisition (closes Q3 2026)",
        rs_inflection=4,           _rs="RS 0, 1M -8%: but driven by deal-related sell-off, not thesis breakdown — classic asymmetric setup",
        theme_exposure=3,          _te="CoolIT will add ~$700M-1B to a $15B specialty chemicals base; material but not majority",
        revenue_growth=3,          _rg="10% YoY base + accretive CoolIT — solid not stunning",
        margin_durability=4,       _m="Best-in-class margins for industrials; CoolIT integration could pressure short-term",
        valuation_runway=4,        _v="P/E 33, P/S 4.2 after -20% drawdown — reasonable for the catalyst",
        institutional_13f=4,       _13f="Mega-cap broadly owned; activist interest possible after sell-off",
        catalyst_proximity=5,      _cat="Q3 2026 deal close is the catalyst; quarterly updates between now and then",
    ),
    "TT": dict(
        bottleneck_specificity=4, _bs="LiquidStack acquisition adds immersion cooling; rest is large HVAC base",
        rs_inflection=4,           _rs="RS 23 low, 1M +1.4% positive — mid-low with steady momentum",
        theme_exposure=2,          _te="LiquidStack is small slice; bulk is commercial/residential HVAC",
        revenue_growth=3,          _rg="High-single-digit organic growth; LiquidStack adds optionality",
        margin_durability=5,       _m="Strong margins, expanding pricing power",
        valuation_runway=3,        _v="P/E in low 30s — fair for the quality, no special discount",
        institutional_13f=5,       _13f="Broadly owned industrial blue-chip",
        catalyst_proximity=3,      _cat="LiquidStack integration milestones over next 2-3 quarters",
    ),
    "FLEX": dict(
        bottleneck_specificity=3, _bs="JetCool acquisition adds micro-convective cooling; broad EMS otherwise",
        rs_inflection=1,           _rs="RS 88 extended, 1M +72% — way past entry window",
        theme_exposure=2,          _te="JetCool tiny slice of broad EMS; not a focused bet",
        revenue_growth=4,          _rg="Strong recent results driven by AI/cloud customer wins",
        margin_durability=3,       _m="EMS-thin margins, slowly improving",
        valuation_runway=1,        _v="After 228% 1Y run, valuation no longer cheap",
        institutional_13f=3,       _13f="Mid-cap broadly owned",
        catalyst_proximity=2,      _cat="No specific near-term catalyst beyond earnings cadence",
    ),
    "MOD": dict(
        bottleneck_specificity=4, _bs="CDU + heat exchanger leader (Airedale)",
        rs_inflection=5,           _rs="RS 35, 1M +15.3% — low-mid with strong positive momentum, classic inflection",
        theme_exposure=4,          _te="Airedale is DC pure-play; rest is auto + HVAC",
        revenue_growth=4,          _rg="31% YoY",
        margin_durability=4,       _m="Expanding as DC mix grows",
        valuation_runway=2,        _v="P/E 149 — high; P/S 5 — moderate",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=4,      _cat="Airedale order book disclosures; product launches",
    ),
    "AAON": dict(
        bottleneck_specificity=2, _bs="Industrial HVAC, broad",
        rs_inflection=3,           _rs="RS 52, 1M +48% — strong momentum but mid RS",
        theme_exposure=2,          _te="Limited DC exposure",
        revenue_growth=5,          _rg="54% YoY — fast",
        margin_durability=4,       _m="Solid HVAC margins",
        valuation_runway=2,        _v="P/E 95 — high",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=3,      _cat="DC order disclosures (if any)",
    ),
    "MMM": dict(
        bottleneck_specificity=3, _bs="DECLINING — PFAS exit by end-2025",
        rs_inflection=1,           _rs="RS 0 — worst in cohort, broken",
        theme_exposure=1,          _te="Novec was small % of revenue + exiting",
        revenue_growth=1,          _rg="1.3% YoY",
        margin_durability=2,       _m="Weak; restructuring",
        valuation_runway=3,        _v="P/E 28 — reasonable absolute but no growth",
        institutional_13f=3,       _13f="Placeholder",
        catalyst_proximity=1,      _cat="Legal overhang dominates",
    ),
    "HON": dict(
        bottleneck_specificity=2, _bs="Potential PFAS alternative supplier (speculative)",
        rs_inflection=1,           _rs="RS 4 — broken",
        theme_exposure=1,          _te="Massive multi-segment industrial; thermal materials is tiny",
        revenue_growth=1,          _rg="2.4% YoY — slow",
        margin_durability=3,       _m="Stable",
        valuation_runway=3,        _v="P/E 34 — fair absolute",
        institutional_13f=4,       _13f="Broadly owned mega-cap industrial; placeholder",
        catalyst_proximity=2,      _cat="No specific near-term related to thesis",
    ),
}

# ─── ABB skipped — no data in current candidates.json ──────


# ─── Compute totals ───────────────────────────────────────
def compute(scores: dict) -> dict:
    """Return {ticker: {raw_total, normalized_100, components, ...}}."""
    out = {}
    candidates = json.loads((_HERE / "candidates.json").read_text())["candidates"]
    meta_by_ticker = {c["ticker"]: c for c in candidates}

    for ticker, s in scores.items():
        components = {}
        raw_total = 0
        # Auto-computed criteria — read from the live data files
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
                "score": score,
                "weight": weight,
                "type": SCORE_TYPE[crit],
                "contribution": contribution,
                "rationale": rationale,
            }

        meta = meta_by_ticker.get(ticker, {})
        out[ticker] = {
            "company": meta.get("company"),
            "bucket": meta.get("bucket"),
            "sub": meta.get("sub"),
            "note": meta.get("note"),
            "specificity_meta": meta.get("specificity"),
            "raw_total": raw_total,                       # 0 - 500
            "normalized_100": round(raw_total / 5, 1),    # 0 - 100
            "components": components,
            # Snapshot the candidates.json data for retrospective comparison
            "snapshot": {k: meta.get(k) for k in
                          ["price","market_cap","pe","ps","1y_pct","3m_pct","1m_pct",
                           "dist_from_52w_high_pct","rs_3m","revenue_growth_yoy"]},
        }
    return out


def crit_short(crit: str) -> str:
    """Map criterion name to its rationale-key suffix."""
    m = {
        "bottleneck_specificity": "bs",
        "rs_inflection":          "rs",
        "theme_exposure":         "te",
        "revenue_growth":         "rg",
        "margin_durability":      "m",
        "valuation_runway":       "v",
        "institutional_13f":      "13f",
        "catalyst_proximity":     "cat",
    }
    return m[crit]


# ─── Output ───────────────────────────────────────────────
def render_md(results: dict) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])

    lines = []
    lines.append("# AI Data Center Build-Out — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Rubric v1. Source data: `candidates.json` from same date. "
                  f"Audit trail: `scoring_log.json`._")
    lines.append("")
    lines.append("## Rubric (this theme)")
    lines.append("")
    lines.append("| Criterion | Weight | Type | Notes |")
    lines.append("|-----------|-------:|------|-------|")
    notes = {
        "bottleneck_specificity": "Hard-to-substitute supplier vs commoditized. From supply_chain.md",
        "rs_inflection":          "Reward low-RS-with-positive-momentum (asymmetric). Penalize extended (RS 85+)",
        "theme_exposure":         "% of revenue tied to AI DC build-out. Penalizes diversified mega-caps",
        "revenue_growth":         "YoY revenue growth, accelerating > flat > declining",
        "margin_durability":      "Pricing power and gross margin trend",
        "valuation_runway":       "P/E and P/S vs growth rate — re-rating room left",
        "institutional_13f":      "Auto-computed from quarterly 13F fund deltas (yfinance.Ticker.institutional_holders)",
        "capital_structure":      "Auto-computed from FMP: shares-out growth + debt growth + SBC/revenue + FCF margin",
        "catalyst_proximity":     "Zeroed v2 — weakest signal; mostly redundant with earnings cadence and 13F flow",
    }
    for crit, weight in WEIGHTS.items():
        lines.append(f"| {crit.replace('_', ' ').title()} | {weight} | {SCORE_TYPE[crit]} | {notes[crit]} |")
    lines.append("| **Total** | **100** | | |")
    lines.append("")

    # Master table
    lines.append("## Ranked candidates")
    lines.append("")
    lines.append("Top 5 promoted to tracker. Next 3 watching. Below the line = not in tracker, can be promoted if a top-5 name's thesis breaks.")
    lines.append("")
    lines.append("| Rank | Ticker | Company | Bucket | Score | B | RS | TE | RG | M | V | 13F | Cat |")
    lines.append("|-----:|--------|---------|--------|-------|---|----|----|----|---|---|-----|-----|")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        c = r["components"]
        marker = ""
        if i <= 5: marker = " 🟢"
        elif i <= 8: marker = " 🟡"
        lines.append(
            f"| {i}{marker} | **{ticker}** | {r['company']} | {r['bucket']} | "
            f"**{r['normalized_100']:.1f}** | "
            f"{c.get('bottleneck_specificity',{}).get('score','—')} | "
            f"{c.get('rs_inflection',{}).get('score','—')} | "
            f"{c.get('theme_exposure',{}).get('score','—')} | "
            f"{c.get('revenue_growth',{}).get('score','—')} | "
            f"{c.get('margin_durability',{}).get('score','—')} | "
            f"{c.get('valuation_runway',{}).get('score','—')} | "
            f"{c.get('institutional_13f',{}).get('score','—')} | "
            f"{c.get('catalyst_proximity',{}).get('score','—')} |"
        )
    lines.append("")
    lines.append("🟢 = top 5 (tracker)  ·  🟡 = next 3 (watching)")
    lines.append("")

    # Top 5 detail
    lines.append("## Top 5 — promoted to tracker")
    lines.append("")
    for i, (ticker, r) in enumerate(sorted_rows[:5], 1):
        lines.append(f"### {i}. {ticker} — {r['company']}  ·  Score {r['normalized_100']:.1f}/100")
        lines.append("")
        lines.append(f"**Bucket:** {r['bucket']} · **Sub:** {r['sub']}  ")
        lines.append(f"_{r['note']}_")
        lines.append("")
        lines.append("| Criterion | Score | Rationale |")
        lines.append("|-----------|------:|-----------|")
        for crit in WEIGHTS:
            c = r["components"].get(crit, {})
            lines.append(f"| {crit.replace('_',' ').title()} | {c.get('score','—')} | {c.get('rationale','')} |")
        snap = r["snapshot"]
        lines.append("")
        lines.append(f"**Snapshot at scoring:** price ${snap.get('price','—')} · "
                      f"1Y {snap.get('1y_pct','—')}% · 3M {snap.get('3m_pct','—')}% · "
                      f"RS 3M {snap.get('rs_3m','—')} · P/E {snap.get('pe','—')}")
        lines.append("")
    lines.append("")

    # Next 3 watching
    lines.append("## Next 3 — watching")
    lines.append("")
    for i, (ticker, r) in enumerate(sorted_rows[5:8], 6):
        lines.append(f"**{i}. {ticker}** ({r['company']}) — {r['normalized_100']:.1f}  ·  {r['bucket']}  ")
        lines.append(f"_{r['note']}_")
        # Brief — top 2 highest and lowest components
        comps = [(k, v["score"]) for k, v in r["components"].items()]
        strengths = sorted(comps, key=lambda x: -x[1])[:2]
        weaknesses = sorted(comps, key=lambda x: x[1])[:2]
        lines.append(f"  - Strengths: {', '.join(f'{k}={s}' for k,s in strengths)}")
        lines.append(f"  - Weaknesses: {', '.join(f'{k}={s}' for k,s in weaknesses)}")
        lines.append("")
    lines.append("")

    # Notes on the cut
    lines.append("## Notes on the cut")
    lines.append("")
    lines.append("**Why the top 5 looks the way it does.** The rubric was tuned to reward bottleneck specificity, low-but-rising RS, and reasonable valuation — that's the asymmetric bet the thesis describes. Names that ranked highest combine all three: a real supply-side moat, a stock that *hasn't* run yet, and pricing that doesn't already assume the bull case.")
    lines.append("")
    lines.append("**Bucket distribution of the top 5:** check the table. If thermal is under-represented and you want explicit bucket coverage, consider whether to bump MOD into the tracker in place of the 5th-ranked name. The default top-5-by-score is in `tracker.md` to start; rebalance is your call.")
    lines.append("")
    lines.append("**Names that missed but might re-enter:**")
    lines.append("- VRT: highest theme exposure in the cohort, dropped by valuation + extension. If valuation compresses 15-20%, promote.")
    lines.append("- MOD: pure thermal exposure; promote if you want explicit bucket-3 coverage.")
    lines.append("- AVGO: switch silicon king but penalty for VMware-driven diversification. If software contribution disclosed lower, promote.")
    lines.append("")
    lines.append("**Names explicitly NOT in the tracker (and why):**")
    lines.append("- AAOI, LITE, ALAB, AXTI, COHR, MRVL: all extended (RS 78-100). The thesis says these will likely fall faster than the picks-and-shovels in a drawdown.")
    lines.append("- MMM, HON: thermal-related but failing the thesis on revenue trajectory and RS — the PFAS exit risk is real but doesn't make MMM a tracker candidate, just a watch-list.")
    lines.append("- CRS, CLF, ATKR: low theme exposure or commoditized. CLF is the most interesting of the three — the upstream GOES story is real but expressed at the 'AI play' level it's diluted.")
    lines.append("")

    # Reproducibility footer
    lines.append("---")
    lines.append("")
    lines.append("_Methodology notes: scoring criteria marked `quantitative` are derived from `candidates.json` numerics; "
                  "`judgment` criteria are this session's hand-scoring. The `institutional_13f` weight is a placeholder pending "
                  "the tracker-build 13F integration (see `themes/DEFERRED.md` D1). To re-score with different weights, "
                  "edit `_score_run.py` and re-run; the previous scoring is preserved in dated files under `history/`._")
    lines.append("")
    return "\n".join(lines)


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")

    # Save scoring_log.json (audit trail)
    log = {
        "theme": "ai_data_center",
        "scored_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": today,
        "rubric_version": "v1",
        "weights": WEIGHTS,
        "score_types": SCORE_TYPE,
        "results": results,
    }
    (_HERE / "scoring_log.json").write_text(json.dumps(log, indent=2, default=str))

    # Save dated history
    hist = _HERE / "history"
    hist.mkdir(exist_ok=True)
    (hist / f"scoring_log_{today}.json").write_text(json.dumps(log, indent=2, default=str))

    # Render markdown
    md = render_md(results)
    (_HERE / "scoring.md").write_text(md)

    # Print summary
    sorted_rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    print(f"\n=== AI Data Center Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "🟢 TRACKER" if i <= 5 else ("🟡 WATCHING" if i <= 8 else "      ")
        print(f"  {i:2d}. {tag}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
