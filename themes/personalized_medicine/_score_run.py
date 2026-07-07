"""
One-off scoring script for Personalized Medicine & Genomics theme — applies
the long-only rubric to the 16 candidates in candidates.json. Writes scoring.md
and scoring_log.json for the tracker.

Rubric (long-only, copied from space_economy v1) reflects this theme's central
discipline: the durable value is in the real-revenue picks-and-shovels tools/
consumables layer and the reimbursed molecular-dx franchises — NOT the pre-revenue,
dilutive gene editors. So:
1. capital_structure weighted 20 — "don't let cash-burning editors crowd out the
   durable arms dealers" is the CENTRAL risk. NOTE: on the local yfinance-only seed
   the CS component fields (shares/debt/SBC/FCF) are None, so compute_capital_structure_score
   returns a neutral 3/5 "data unavailable" fallback for every name until this universe
   is folded into refresh_data.py and FMP populates them on Railway. Until then the
   real-revenue vs. cash-burn distinction is carried by margin_durability + valuation_runway.
2. revenue_growth reduced to 10 — durability and reimbursed recurring revenue beat raw
   growth when the universe includes pre-revenue editors that grow revenue off a zero base
   while diluting shareholders.

Reuses the same compute_capital_structure_score and compute_13f_score patterns as
space_economy — only the per-ticker SCORES dict, WEIGHTS strings, and theme name differ.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (Personalized Medicine v1 — long-only) ────────
WEIGHTS = {
    "bottleneck_specificity": 20,
    "rs_inflection":          10,   # 3-5yr horizon, RS matters less
    "theme_exposure":         15,
    "revenue_growth":         10,   # reduced — durability > raw growth here
    "margin_durability":       5,
    "valuation_runway":       15,
    "institutional_13f":       5,
    "capital_structure":      20,   # central — real revenue vs dilutive editors
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
    shares-out YoY · debt YoY · SBC/rev · FCF margin. Falls back to neutral 3/5
    when the yfinance-only seed leaves these fields None (FMP populates on Railway)."""
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
        rationale = "Capital structure data unavailable (yfinance-only seed) — neutral fallback; FMP populates on Railway fold-in"
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


# ─── Hand-scored 13F fallback per ticker (theme-specific) ──
_HAND_SCORED_13F = {
    # Sequencing platforms
    "ILMN":4, "PACB":2,
    # Tools / consumables / synthesis
    "TXG":3, "TWST":3, "QGEN":4, "A":4, "TMO":5,
    # Molecular diagnostics / liquid biopsy
    "EXAS":4, "GH":4, "NTRA":4, "VCYT":3,
    # Gene editing & mRNA (optionality)
    "CRSP":4, "NTLA":3, "BEAM":3, "MRNA":4, "BNTX":3,
}


# ─── Per-ticker hand scores ───────────────────────────────
# 7 judgment+quant criteria as 1-5 scores plus one-line `_xx` rationales.
# `capital_structure` and `institutional_13f` are auto-computed.
SCORES = {
    # ── Bucket 1: Sequencing Platforms ──
    "ILMN": dict(
        bottleneck_specificity=5, _bs="Dominant short-read installed base + proprietary consumable pull-through; the picks-and-shovels anchor",
        rs_inflection=3,           _rs="RS 64, 3M +52.6% — strong recovery off a deep trough; momentum real but partly rebound, not asymmetric",
        theme_exposure=5,          _te="Pure sequencing-platform play; the sub-$200-genome enabler",
        revenue_growth=2,          _rg="~5% YoY — mature/recovering, not a growth story right now",
        margin_durability=4,       _m="High consumables-driven gross margin; the razor/razorblade annuity",
        valuation_runway=3,        _v="P/E ~35, P/S ~6.7 — full after a +100% 1Y run; installed-base moat justifies but tightens room",
        institutional_13f=4,       _13f="Broadly owned genomics bellwether",
        catalyst_proximity=3,      _cat="NovaSeq X ramp + post-GRAIL simplification",
    ),
    "PACB": dict(
        bottleneck_specificity=4, _bs="Long-read HiFi differentiation (Revio) — real tech, but small commercial base",
        rs_inflection=2,           _rs="RS 29, 3M +23.9% but off a sub-$2 micro-cap base — noisy, not a clean inflection",
        theme_exposure=5,          _te="Pure-play long-read sequencing",
        revenue_growth=1,          _rg="~0% YoY — flat revenue, no growth traction",
        margin_durability=1,       _m="Negative margins, cash-burning micro-cap",
        valuation_runway=2,        _v="$1.66 micro-cap; P/E negative, P/S ~3.2 — speculative, dilution risk",
        institutional_13f=2,       _13f="Micro-cap, thin institutional ownership",
        catalyst_proximity=3,      _cat="Revio placements + long-read clinical adoption",
    ),

    # ── Bucket 2: Tools / Consumables / Synthesis ──
    "TWST": dict(
        bottleneck_specificity=5, _bs="Silicon DNA synthesis — hard-to-substitute upstream input for synbio + NGS; few qualified suppliers",
        rs_inflection=1,           _rs="RS 100, 3M +99%, 1Y +181% — peak momentum, extremely extended",
        theme_exposure=4,          _te="Core arms dealer; synthesis underpins editing + NGS panels",
        revenue_growth=5,          _rg="~19% YoY, accelerating on synthesis + NGS",
        margin_durability=2,       _m="Improving but still pre-profit; gross margin scaling",
        valuation_runway=1,        _v="P/S ~15, P/E negative — priced for perfection at peak",
        institutional_13f=3,       _13f="Growing institutional interest",
        catalyst_proximity=4,      _cat="Synbio + NGS panel demand; gross-margin inflection",
    ),
    "TXG": dict(
        bottleneck_specificity=4, _bs="Single-cell + spatial reagent consumables franchise; recurring pull-through",
        rs_inflection=3,           _rs="RS 79, 3M +79% — strong momentum but stretched",
        theme_exposure=4,          _te="Core research-tools consumables; broad exposure to genomics labs",
        revenue_growth=2,          _rg="~-3% YoY — revenue decelerated/contracting; the growth overhang",
        margin_durability=3,       _m="High gross margin but opex-heavy; profitability elusive",
        valuation_runway=2,        _v="P/S ~8 after +236% 1Y run; IP-litigation overhang",
        institutional_13f=3,       _13f="Mid institutional ownership",
        catalyst_proximity=3,      _cat="Spatial platform adoption; litigation resolution",
    ),
    "QGEN": dict(
        bottleneck_specificity=4, _bs="Sample-to-insight consumables + QuantiFERON; defensive molecular-prep annuity",
        rs_inflection=1,           _rs="RS 0, 3M -4.6%, 1Y -17.6% — the laggard; downtrend, oversold-but-not-turning",
        theme_exposure=4,          _te="Core consumables; sample prep is upstream of every NGS/dx workflow",
        revenue_growth=2,          _rg="~2% YoY — slow but steady",
        margin_durability=4,       _m="Real FCF, durable consumables margins",
        valuation_runway=5,        _v="P/E ~20, P/S ~3.8 — cheapest quality name in the tools bucket; real value",
        institutional_13f=4,       _13f="Broadly owned ADR",
        catalyst_proximity=3,      _cat="QuantiFERON growth + molecular-dx menu expansion",
    ),
    "A": dict(
        bottleneck_specificity=3, _bs="Analytical instruments + genomics reagents; diversified, hard to displace but diffuse",
        rs_inflection=2,           _rs="RS 21, 3M +13.3% — mild recovery, low relative strength",
        theme_exposure=2,          _te="Genomics is one slice; broad analytical-instrument base",
        revenue_growth=3,          _rg="~10% YoY — steady",
        margin_durability=4,       _m="Durable margins + FCF; quality compounder",
        valuation_runway=4,        _v="P/E ~26, P/S ~5.1 — reasonable for the quality",
        institutional_13f=4,       _13f="Broadly owned",
        catalyst_proximity=2,      _cat="Instrument replacement cycle; NASD/genomics reagent growth",
    ),
    "TMO": dict(
        bottleneck_specificity=3, _bs="Instruments + reagents to every genomics lab; diffuse but structurally durable",
        rs_inflection=2,           _rs="RS 14, 3M +5.4% — muted, low relative strength",
        theme_exposure=2,          _te="Genomics tools are a slice of a vast life-sciences franchise",
        revenue_growth=3,          _rg="~6% YoY — steady mega-cap growth",
        margin_durability=4,       _m="Strong, durable margins + FCF; the safe picks-and-shovels ballast",
        valuation_runway=4,        _v="P/E ~28, P/S ~4.3 — reasonable for the quality/breadth",
        institutional_13f=5,       _13f="Top-tier institutional ownership",
        catalyst_proximity=2,      _cat="Bioprocessing/reagent recovery; capital-equipment cycle",
    ),

    # ── Bucket 3: Molecular Diagnostics / Liquid Biopsy ──
    "EXAS": dict(
        bottleneck_specificity=4, _bs="Reimbursed screening scale (Cologuard) + Oncodetect MRD; reimbursement/guideline moat",
        rs_inflection=3,           _rs="Did not price at seed — hand-set neutral; historically strong screening-volume franchise",
        theme_exposure=4,          _te="Flagship real-revenue molecular-dx; blood-based screening is the next TAM leg",
        revenue_growth=4,          _rg="Screening volume compounding double-digits (hand-set; no seed data)",
        margin_durability=3,       _m="Improving toward profitability on screening scale",
        valuation_runway=3,        _v="Reimbursed franchise; valuation reasonable vs. TAM (hand-set — no seed price)",
        institutional_13f=4,       _13f="Broadly owned diagnostics name",
        catalyst_proximity=4,      _cat="Blood-based screening + MCED coverage decisions",
    ),
    "GH": dict(
        bottleneck_specificity=4, _bs="Guardant360 liquid biopsy + Shield blood screening + MRD; reimbursement is the swing factor",
        rs_inflection=3,           _rs="RS 93, 3M +79.9% — strong momentum, extended after a big run",
        theme_exposure=5,          _te="Pure-play liquid biopsy + blood screening; thesis flagship dx name",
        revenue_growth=5,          _rg="~48% YoY — volume compounding fast",
        margin_durability=2,       _m="Still loss-making (P/E negative); scaling toward operating leverage",
        valuation_runway=2,        _v="P/S ~21 after +241% 1Y — priced for continued volume growth",
        institutional_13f=4,       _13f="Institutional favorite in liquid biopsy",
        catalyst_proximity=4,      _cat="Shield reimbursement + guideline inclusion",
    ),
    "NTRA": dict(
        bottleneck_specificity=4, _bs="Tumor-informed MRD (Signatera) on a large proprietary dataset; category leader",
        rs_inflection=4,           _rs="RS 50, 3M +36.5% — mid RS with healthy momentum; not over-extended",
        theme_exposure=5,          _te="Pure-play MRD + molecular dx; the compounding-volume flagship",
        revenue_growth=5,          _rg="~39% YoY — Signatera compounding ~40%",
        margin_durability=2,       _m="Near/at breakeven; heavy reinvestment (P/E deeply negative on small loss)",
        valuation_runway=2,        _v="P/S ~16 — priced for continued MRD compounding",
        institutional_13f=4,       _13f="Broadly owned MRD leader",
        catalyst_proximity=4,      _cat="MRD guideline inclusion + Medicare coverage expansion",
    ),
    "VCYT": dict(
        bottleneck_specificity=3, _bs="Genomic classifiers (Decipher, Afirma); reimbursed but narrower TAM",
        rs_inflection=3,           _rs="RS 86, 3M +79.8% — strong momentum, stretched",
        theme_exposure=4,          _te="Reimbursed genomic-classifier franchise; smaller but real",
        revenue_growth=4,          _rg="~22% YoY — solid double-digit growth",
        margin_durability=3,       _m="Profitable (P/E ~54); cleaner P&L than GH/NTRA",
        valuation_runway=3,        _v="P/E ~54, P/S ~8.5 — full but supported by profitability + growth",
        institutional_13f=3,       _13f="Mid institutional ownership",
        catalyst_proximity=3,      _cat="Classifier menu expansion; MRD entry",
    ),

    # ── Bucket 4: Gene Editing & mRNA (Optionality — SMALL) ──
    "CRSP": dict(
        bottleneck_specificity=4, _bs="Best-capitalized editor; Casgevy approved/commercial + in-vivo pipeline",
        rs_inflection=2,           _rs="RS 36, 3M +25% — mid-low RS, moderate momentum",
        theme_exposure=4,          _te="Editing optionality anchor; has an approved product, unlike peers",
        revenue_growth=5,          _rg="~69% YoY off a small Casgevy base — early commercial ramp",
        margin_durability=1,       _m="Loss-making clinical-stage economics; large cash offsets",
        valuation_runway=2,        _v="P/S distorted by tiny revenue; valued on cash + pipeline optionality",
        institutional_13f=4,       _13f="Best-owned editor; large cash pile attracts institutions",
        catalyst_proximity=4,      _cat="Casgevy uptake + in-vivo clinical readouts",
    ),
    "MRNA": dict(
        bottleneck_specificity=3, _bs="mRNA platform + INT personalized cancer vaccine (w/ Merck)",
        rs_inflection=4,           _rs="RS 71, 3M +66% — strong momentum off a beaten-down base",
        theme_exposure=3,          _te="mRNA optionality; personalized-oncology INT is the theme-relevant piece",
        revenue_growth=1,          _rg="Post-COVID revenue cliff — reported growth noisy off collapsing base",
        margin_durability=1,       _m="Loss-making post-COVID; large cash pile cushions",
        valuation_runway=3,        _v="P/S ~15 but trades near cash; optionality-priced",
        institutional_13f=4,       _13f="Broadly owned large-cap biotech",
        catalyst_proximity=4,      _cat="INT (mRNA-4157) melanoma/other Phase 3 readouts with Merck",
    ),
    "BNTX": dict(
        bottleneck_specificity=3, _bs="mRNA individualized cancer immunotherapy pipeline; strong balance sheet",
        rs_inflection=1,           _rs="RS 7, 3M +3% — laggard, low relative strength",
        theme_exposure=3,          _te="mRNA optionality; individualized immunotherapy is theme-relevant",
        revenue_growth=1,          _rg="~-35% YoY — post-COVID revenue decline",
        margin_durability=1,       _m="Loss-making now; very large cash pile",
        valuation_runway=4,        _v="P/S ~8.5 and trades close to net cash — cheapest optionality in the sleeve",
        institutional_13f=3,       _13f="Owned but ADR reduces US institutional depth",
        catalyst_proximity=3,      _cat="Individualized immunotherapy readouts; pipeline milestones",
    ),
    "NTLA": dict(
        bottleneck_specificity=4, _bs="In-vivo CRISPR leader (NTLA-2001/2002) — rare platform capability",
        rs_inflection=3,           _rs="RS 43, 3M +35.9% — mid RS, moderate momentum",
        theme_exposure=4,          _te="Pure-play in-vivo editing optionality",
        revenue_growth=1,          _rg="~-10% YoY — collaboration revenue, no product revenue",
        margin_durability=1,       _m="Loss-making, cash-burning clinical-stage",
        valuation_runway=1,        _v="P/S ~38; pre-revenue, single-catalyst, dilution-financed — smallest sizing",
        institutional_13f=3,       _13f="Owned by specialists; binary-risk name",
        catalyst_proximity=4,      _cat="ATTR + HAE in-vivo Phase 3 readouts",
    ),
    "BEAM": dict(
        bottleneck_specificity=4, _bs="Base-editing platform differentiation — distinct editing modality",
        rs_inflection=3,           _rs="RS 57, 3M +43.2% — mid RS with momentum",
        theme_exposure=4,          _te="Pure-play base-editing optionality",
        revenue_growth=5,          _rg="Revenue up sharply off a near-zero collaboration base — not meaningful",
        margin_durability=1,       _m="Loss-making, cash-burning pre-revenue",
        valuation_runway=1,        _v="P/S ~22; pre-revenue, dilutive — optionality only",
        institutional_13f=3,       _13f="Owned by editing specialists",
        catalyst_proximity=3,      _cat="Base-editing clinical readouts; platform validation",
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
    lines.append("# Personalized Medicine & Genomics — Scoring (v1)")
    lines.append("")
    lines.append(f"_Generated **{today}**. Long-only rubric v1 (capital_structure weighted 20 — central to thesis; "
                 f"neutral fallback on this yfinance-only seed until FMP folds in). Source: `candidates.json`. Audit: `scoring_log.json`._")
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
    lines.append("_Rubric legend: B=bottleneck specificity · RS=relative-strength inflection · TE=theme exposure · "
                 "RG=revenue growth · M=margin durability · V=valuation runway · 13F=institutional flow · Cap=capital structure._")
    return "\n".join(lines) + "\n"


def main():
    results = compute(SCORES)
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "personalized_medicine", "scored_at": datetime.utcnow().isoformat() + "Z",
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
    print(f"\n=== Personalized Medicine Scoring v1 ({today}) ===")
    for i, (ticker, r) in enumerate(sorted_rows, 1):
        tag = "🟢 TRACKER" if i <= 5 else "🟡 WATCHING" if i <= 7 else "      "
        print(f"  {i:2d}. {tag}  {ticker:6s} {r['normalized_100']:5.1f}  {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md, scoring_log.json, history/scoring_log_{today}.json")


if __name__ == "__main__":
    main()
