"""
One-off scoring script for Cybersecurity for Critical Infrastructure theme —
applies the long-only rubric to the 16 candidates in candidates.json. Writes
scoring.md and scoring_log.json for the tracker.

Rubric (copied from space_economy v1 and kept, with the same reasoning):
1. capital_structure weighted 20 — the "great company, no margin of safety"
   risk is CENTRAL for this theme: the platform consolidators carry premium
   multiples and the thesis mandate is "durable FCF," so balance-sheet /
   cash-flow quality must be weighted heavily, not treated as tail.
2. revenue_growth reduced to 10 — durability beats raw growth when the
   universe mixes FCF compounders with faster-growing but cash-burning
   challengers (S) and a speculative micro-cap (ARQQ) that grows revenue
   while diluting.

NOTE on data source: this theme was seeded via yfinance only (no FMP key in
the build environment), so the four capital-structure inputs
(shares_growth_yoy / debt_growth_yoy / sbc_pct_revenue / fcf_margin) are
absent. compute_capital_structure_score handles this with a neutral 3/5
fallback per component (documented behavior). Once the operator folds this
universe into refresh_data.py, the nightly FMP refresh populates those fields
and re-running this script sharpens the capital_structure scores.

Reuses the same compute_capital_structure_score and compute_13f_score patterns
as space_economy — only the per-ticker SCORES dict and _HAND_SCORED_13F differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Cyber Infrastructure v1) ─────────────────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # 3-5yr horizon — RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # durability > raw growth here
    "margin_durability":       5,
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,   # BUMPED — "no margin of safety" risk is central
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
    shares-out YoY · debt YoY · SBC/rev · FCF margin. Neutral 3/5 fallback
    per component when the FMP-sourced field is absent."""
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
        rationale = "Capital structure data unavailable (yfinance-only seed) — neutral fallback; refresh via FMP to sharpen"
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


# ─── Hand-scored 13F fallback per ticker (Cyber-specific) ──
_HAND_SCORED_13F = {
    "CRWD":5, "PANW":5, "ZS":4, "FTNT":4, "S":3,
    "TENB":3, "VRNS":3, "RDWR":2, "NET":5, "CHKP":4,
    "OKTA":3, "CYBR":4,
    "ARQQ":1, "IBM":4, "BAH":4, "LDOS":4,
}


# ─── Per-ticker hand scores ───────────────────────────────
# 7 judgment+quant criteria (1-5) + one-line _xx rationales.
# capital_structure and institutional_13f are auto-computed.
SCORES = {
    # ── Bucket 1: Platform / Endpoint / Cloud ──
    "CRWD": dict(
        bottleneck_specificity=5, _bs="Single-agent Falcon platform + data-gravity lock-in; the deepest moat in the group",
        rs_inflection=2,           _rs="RS 79, +99.8% 3M — ran hard; extended but momentum FCF-supported",
        theme_exposure=5,          _te="The platform-consolidator flagship — endpoint→cloud→identity→SIEM",
        revenue_growth=5,          _rg="~26% YoY, durable at scale",
        margin_durability=5,       _m="Strong FCF margins, best module-attach economics",
        valuation_runway=1,        _v="P/E ~128, P/S ~40 — premium; the 'no margin of safety' risk in one name",
        institutional_13f=5,       _13f="Institutional darling; top cyber holding",
        catalyst_proximity=3,      _cat="Module-attach + regulation-driven budget expansion",
    ),
    "PANW": dict(
        bottleneck_specificity=4, _bs="Platformization (Prisma + XSIAM) module-attach lock-in + firewall network position",
        rs_inflection=1,           _rs="RS 93, +119% 3M, +77.5% 1Y — extreme run, peak momentum",
        theme_exposure=5,          _te="Platformization playbook flagship; firewalls give real OT/network adjacency",
        revenue_growth=4,          _rg="~31% YoY (incl. platformization mix)",
        margin_durability=5,       _m="Real FCF, expanding; strong operating leverage",
        valuation_runway=1,        _v="P/E ~311, P/S ~28 — priced for perfection",
        institutional_13f=5,       _13f="Top cyber institutional holding",
        catalyst_proximity=3,      _cat="Platformization deal cadence + regulation floor",
    ),
    "ZS": dict(
        bottleneck_specificity=4, _bs="Zero-trust SASE — inline traffic dependency once deployed; secures OT/IT remote access",
        rs_inflection=4,           _rs="RS 21, +8.6% 3M but -52% 1Y — deeply reset; classic low-RS-but-positioned setup",
        theme_exposure=4,          _te="Pure-play zero-trust edge; core to OT/IT convergence",
        revenue_growth=4,          _rg="~25% YoY billings-led growth",
        margin_durability=4,       _m="Improving FCF margins as scale builds",
        valuation_runway=4,        _v="P/E ~33, P/S ~8 — the cheapest premium platform after the 1Y reset",
        institutional_13f=4,       _13f="Broadly owned SASE leader",
        catalyst_proximity=3,      _cat="SASE consolidation + zero-trust mandates",
    ),
    "FTNT": dict(
        bottleneck_specificity=5, _bs="Genuine OT/ICS installed base inside industrial/utility networks — the real critical-infra angle",
        rs_inflection=2,           _rs="RS 71, +96.7% 3M — ran hard, extended but supported",
        theme_exposure=5,          _te="The one profitable platform with a true critical-infra OT franchise",
        revenue_growth=4,          _rg="~20% YoY, durable",
        margin_durability=5,       _m="Highly profitable with real FCF — best cash generation among platforms",
        valuation_runway=2,        _v="P/E ~63, P/S ~17 — full after the run",
        institutional_13f=4,       _13f="Broadly owned profitable platform",
        catalyst_proximity=4,      _cat="OT/ICS regulation + firewall refresh cycle",
    ),
    "S": dict(
        bottleneck_specificity=3, _bs="#2 endpoint (Singularity) — real tech but less data gravity than CRWD",
        rs_inflection=3,           _rs="RS 50, +36% 3M — mid, momentum building",
        theme_exposure=4,          _te="Endpoint platform challenger; real but not the leader",
        revenue_growth=4,          _rg="~21% YoY, faster than leaders but decelerating",
        margin_durability=2,       _m="Not consistently FCF-positive — the capital-structure penalty name",
        valuation_runway=3,        _v="P/E ~37, P/S ~6 — cheaper than leaders but for a reason",
        institutional_13f=3,       _13f="Mid institutional ownership",
        catalyst_proximity=3,      _cat="Endpoint displacement wins; path to FCF",
    ),

    # ── Bucket 2: OT / ICS / Critical-Infra Specialists ──
    "TENB": dict(
        bottleneck_specificity=4, _bs="Tenable OT embedded in critical-infra asset inventories — closest holdable OT-visibility proxy",
        rs_inflection=1,           _rs="RS 100, +134.9% 3M — extreme run, peak momentum",
        theme_exposure=5,          _te="Purest OT/exposure-management exposure that's publicly holdable",
        revenue_growth=3,          _rg="~10% YoY — decelerated, watch",
        margin_durability=3,       _m="Modest FCF; improving profitability",
        valuation_runway=3,        _v="P/E ~19, P/S ~4.5 — reasonable for the OT franchise after run",
        institutional_13f=3,       _13f="Mid-cap institutional ownership",
        catalyst_proximity=4,      _cat="OT/ICS regulation + exposure-management mandates",
    ),
    "VRNS": dict(
        bottleneck_specificity=3, _bs="Data security / DSPM — sticky where data governance is mandated (utilities/healthcare)",
        rs_inflection=2,           _rs="RS 86, +102.7% 3M but -10.6% 1Y — ran hard off a reset base",
        theme_exposure=4,          _te="Data-centric security relevant to regulated critical-infra data",
        revenue_growth=4,          _rg="~27% YoY (SaaS transition inflating ARR optics)",
        margin_durability=2,       _m="FCF pressured mid SaaS transition — watch",
        valuation_runway=2,        _v="P/E ~119, P/S ~8 — full during the transition",
        institutional_13f=3,       _13f="Mid institutional ownership",
        catalyst_proximity=3,      _cat="Data-governance mandates; SaaS transition inflection",
    ),
    "RDWR": dict(
        bottleneck_specificity=3, _bs="Application / DDoS / network security — protects critical-infra web/app layer",
        rs_inflection=3,           _rs="RS 29, +11.5% 3M — mid-low, quiet base",
        theme_exposure=3,          _te="Network-layer protection; real but a competitive segment",
        revenue_growth=3,          _rg="~11% YoY, steady",
        margin_durability=3,       _m="Profitable with real cash flow; smaller and durable",
        valuation_runway=4,        _v="P/E ~69 but P/S ~4 — cheap small-cap on sales",
        institutional_13f=2,       _13f="Smaller-cap, lighter institutional ownership",
        catalyst_proximity=3,      _cat="DDoS/app-security demand from attacks",
    ),
    "NET": dict(
        bottleneck_specificity=3, _bs="Infrastructure edge / DDoS / Zero Trust — sticky at scale, somewhat substitutable",
        rs_inflection=3,           _rs="RS 36, +16.9% 3M — mid, steady",
        theme_exposure=4,          _te="The internet edge increasingly a security platform for critical infra",
        revenue_growth=5,          _rg="~34% YoY — strongest growth in the universe",
        margin_durability=4,       _m="Improving FCF margins as it scales",
        valuation_runway=1,        _v="P/E ~157, P/S ~38 — extreme premium",
        institutional_13f=5,       _13f="Institutional favorite",
        catalyst_proximity=3,      _cat="Zero-trust + edge-security adoption",
    ),
    "CHKP": dict(
        bottleneck_specificity=4, _bs="Deeply embedded firewalls; high switching cost — the boring FCF/buyback machine",
        rs_inflection=4,           _rs="RS 14, -6.3% 3M, -37.7% 1Y — reset hard; low-RS value setup",
        theme_exposure=4,          _te="Firewall/network security incumbent with real critical-infra footprint",
        revenue_growth=2,          _rg="~5% YoY — the growth drag",
        margin_durability=5,       _m="Best-in-class margins + strong buyback culture — balance-sheet anchor",
        valuation_runway=5,        _v="P/E ~14, P/S ~5 — the cheapest quality name in the universe",
        institutional_13f=4,       _13f="Broadly owned profitable ADR",
        catalyst_proximity=2,      _cat="Buyback + slow-and-steady; low near-term catalyst",
    ),

    # ── Bucket 3: Identity & Access ──
    "OKTA": dict(
        bottleneck_specificity=3, _bs="Workforce/CIAM identity — critical role but Entra-bundle-contested",
        rs_inflection=2,           _rs="RS 64, +85.3% 3M — ran hard off a low base",
        theme_exposure=4,          _te="Identity-as-perimeter leader; held for structure, breach-scar risk",
        revenue_growth=3,          _rg="~11% YoY — decelerated",
        margin_durability=4,       _m="FCF-positive now; margins improving",
        valuation_runway=2,        _v="P/E ~107, P/S ~9 — full after the 3M run",
        institutional_13f=3,       _13f="Mid institutional; breach overhang",
        catalyst_proximity=3,      _cat="Zero-trust identity mandates; competitive pressure",
    ),
    "CYBR": dict(
        bottleneck_specificity=4, _bs="PAM leader — privileged control-system access is the crown-jewel attack surface",
        rs_inflection=3,           _rs="No live price this seed (data-source gap) — neutral RS",
        theme_exposure=4,          _te="PAM directly relevant to critical-infra privileged access; better-positioned than OKTA",
        revenue_growth=4,          _rg="Strong ~25%+ growth (from prior data) — not in this seed",
        margin_durability=4,       _m="Improving FCF; quality identity franchise",
        valuation_runway=3,        _v="Premium but not extreme (est.) — no live multiple this seed",
        institutional_13f=4,       _13f="Broadly owned PAM leader",
        catalyst_proximity=3,      _cat="Machine-identity + privileged-access mandates",
    ),

    # ── Bucket 4: Post-Quantum / Encryption ──
    "ARQQ": dict(
        bottleneck_specificity=4, _bs="Genuine PQC pure-play (rare specificity) but micro-cap solvency dominates",
        rs_inflection=3,           _rs="RS 57, +58.4% 3M but -38.4% 1Y — volatile speculative",
        theme_exposure=5,          _te="Only clean public PQC pure-play — but immaterial-optionality sizing only",
        revenue_growth=2,          _rg="Revenue tiny/erratic off ~$0 base — not meaningful",
        margin_durability=1,       _m="Losses, cash burn, going-concern risk",
        valuation_runway=1,        _v="P/S ~355, negative earnings — speculative micro-cap",
        institutional_13f=1,       _13f="Tiny float, retail-dominated",
        catalyst_proximity=4,      _cat="PQC-mandate / NIST / NSA headlines — episodic optionality",
    ),
    "IBM": dict(
        bottleneck_specificity=3, _bs="Co-authored NIST PQC standards; real quantum-safe capability but diffuse in a mega-cap",
        rs_inflection=3,           _rs="RS 43, +21.6% 3M — mid, steady",
        theme_exposure=2,          _te="PQC is a tiny slice of a diversified mega-cap — diffuse exposure",
        revenue_growth=2,          _rg="~9.5% YoY — mega-cap slow growth",
        margin_durability=4,       _m="Real FCF, durable — the balance sheet under the optionality",
        valuation_runway=4,        _v="P/E ~27, P/S ~4 — reasonable for a cash-generative mega-cap",
        institutional_13f=4,       _13f="Broadly owned mega-cap",
        catalyst_proximity=3,      _cat="Federal PQC migration services; quantum roadmap milestones",
    ),
    "BAH": dict(
        bottleneck_specificity=3, _bs="Federal cyber + PQC migration services — procurement-framework moat",
        rs_inflection=2,           _rs="RS 7, -24.5% 3M, -42% 1Y — deep drawdown",
        theme_exposure=3,          _te="Implements federal directives + PQC migration; cross-theme, small",
        revenue_growth=2,          _rg="-6.5% YoY — federal budget softness the drag",
        margin_durability=4,       _m="Real FCF; clean balance sheet",
        valuation_runway=5,        _v="P/E ~9, P/S ~0.7 — very cheap after the drawdown",
        institutional_13f=4,       _13f="Broadly owned government consultant",
        catalyst_proximity=3,      _cat="Federal cyber contract flow; PQC migration mandates",
    ),
    "LDOS": dict(
        bottleneck_specificity=3, _bs="Federal IT/cyber integrator with critical-infra contracts — procurement moat",
        rs_inflection=2,           _rs="RS 0, -31.3% 3M, -33% 1Y — deep drawdown, worst RS in universe",
        theme_exposure=3,          _te="Critical-infra IT/cyber services; cross-theme, small",
        revenue_growth=2,          _rg="~4% YoY — slow",
        margin_durability=3,       _m="Steady margins, real cash flow",
        valuation_runway=5,        _v="P/E ~10, P/S ~0.8 — cheapest on earnings in the universe",
        institutional_13f=4,       _13f="Broadly owned defense/IT name",
        catalyst_proximity=3,      _cat="Federal program awards; critical-infra IT spend",
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
    lines.append("# Cybersecurity for Critical Infrastructure — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Rubric v1 (capital_structure weighted 20 — 'no margin of safety' risk is central). Source: `candidates.json`. Audit: `scoring_log.json`._")
    lines.append("")
    lines.append("_Note: seeded yfinance-only, so capital_structure scores fall back to neutral 3/5 until the operator's FMP refresh populates shares/debt/SBC/FCF fields. CYBR did not price in this environment (data-source gap) — it scores on judgment fields only and is not tracker-eligible until priced._")
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
    lines.append("_Criteria: B=bottleneck/switching-cost specificity · RS=relative-strength inflection · TE=theme exposure · RG=revenue growth · M=margin durability · V=valuation runway · 13F=institutional flow · Cap=capital structure (auto)._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "cyber_infrastructure", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== Cyber Infrastructure Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "TRACKER" if i <= 5 else "WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag:8s}  {ticker:5s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
