"""
Scoring script for GLP-1 Mass Adoption — a Long/Short, short-weighted theme.

WHY THIS DIFFERS FROM THE LONG-ONLY THEMES
------------------------------------------
The other themes (Space, AI DC) rank a long-only universe: high momentum +
clean capital structure = high score. That logic is WRONG for our short book.

For a SHORT candidate, the ideal setup is the mirror image:
  - the stock still trades at a FULL multiple (market hasn't re-priced the
    GLP-1 demand-destruction risk yet) — maximum downside room, and
  - momentum is already ROLLING OVER (RS weak, below 52w high, negative 3M) —
    the thesis is starting to play out and confirms the short.
A short that has ALREADY de-rated hard is LESS interesting (thesis partly
spent). A short still near its highs on a deteriorating volume story is the
prize.

So `side` (from candidates.json) flips the interpretation of the momentum and
valuation criteria:
  - LONG  candidates: high RS + reasonable valuation + growth  -> high score
  - SHORT candidates: weak/rolling-over RS + FULL valuation + slowing growth
                      -> high score (best short asymmetry)

The output shape (weights, score_types, results{components,raw_total,
normalized_100,snapshot}) is IDENTICAL to the long-only scripts so scoring.html
renders unchanged.

Capital-structure sub-scores need FMP-only fields (SBC%, FCF margin, debt/
share growth) which the current yfinance seed lacks — they fall back to a
neutral 3 until a keyed FMP refresh runs on Railway.
"""
import json
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).parent

# ─── Rubric (GLP-1 v1) ────────────────────────────────────
# Short-weighted theme: the "setup quality" criteria (momentum inflection +
# valuation room) carry the most weight because timing/asymmetry is the whole
# game for a structural short. thesis_conviction is the analyst's judgment on
# how cleanly the name expresses the demand-destruction (or beneficiary) bet.
WEIGHTS = {
    "thesis_conviction":     25,   # judgment: how purely does this name express the bet
    "setup_asymmetry":       25,   # side-aware: short=full-mult+rolling-over; long=momentum+room
    "valuation_room":        15,   # side-aware: short=expensive is GOOD; long=reasonable is good
    "momentum_inflection":   15,   # side-aware: short=weakening RS is GOOD; long=strengthening
    "fundamental_trend":     10,   # side-aware: short=slowing growth is GOOD; long=accelerating
    "liquidity_borrow":      10,   # shorts need borrowable float + liquidity; longs = liquidity
}
assert sum(WEIGHTS.values()) == 100

SCORE_TYPE = {
    "thesis_conviction":   "judgment",
    "setup_asymmetry":     "mixed",
    "valuation_room":      "quantitative",
    "momentum_inflection": "quantitative",
    "fundamental_trend":   "quantitative",
    "liquidity_borrow":    "judgment",
}

# ─── Analyst judgment scores (thesis_conviction + liquidity_borrow + setup notes) ──
# thesis_conviction: how cleanly does this ticker express the GLP-1 bet (1-5)
# liquidity_borrow:  US-listed liquid + borrowable (shorts) / liquid (longs) (1-5)
JUDGMENT = {
    # SHORTS — Med-Tech demand destruction
    "RMD":  dict(tc=4, lb=5, _tc="Direct: weight loss cuts OSA severity → CPAP demand. First-line displacement is the cleanest med-tech short", _lb="Large-cap NYSE, deep liquidity, easy borrow"),
    "DXCM": dict(tc=4, lb=5, _tc="Fewer progressing diabetics shrinks CGM funnel; strong second-order read", _lb="Large-cap, liquid, borrowable"),
    "MDT":  dict(tc=3, lb=5, _tc="Bariatric + ortho exposure real but diluted across a mega-cap med-tech portfolio", _lb="Mega-cap, maximal liquidity"),
    # SHORTS — Food & Beverage
    "MDLZ": dict(tc=4, lb=5, _tc="Impulse snacking + confection core to volume; craving-modulation hits directly", _lb="Mega-cap staple, deep borrow"),
    "PEP":  dict(tc=4, lb=5, _tc="Frito-Lay + sugary beverage caloric volume; broad exposure to the calorie-cut", _lb="Mega-cap, maximal liquidity"),
    "HSY":  dict(tc=5, lb=5, _tc="Purest sugar/impulse name in the book — nearly all revenue is discretionary confection", _lb="Large-cap, liquid, borrowable"),
    # SHORTS — Stranded injectable supply
    "WST":  dict(tc=4, lb=4, _tc="Glass syringe / auto-injector capex stranded by the oral pivot; the 'dead bottleneck' thesis", _lb="Large-cap but watch borrow as thesis gets crowded"),
    "STVN": dict(tc=4, lb=3, _tc="Same stranded-capex thesis, purer play; ADR so watch borrow depth", _lb="ADR, smaller float — borrow/liquidity caution"),
    # LONGS — Beneficiaries
    "CAVA": dict(tc=3, lb=4, _tc="Health-forward QSR beneficiary of spend shift; indirect", _lb="Liquid mid-cap"),
    "LULU": dict(tc=2, lb=5, _tc="Lifestyle beneficiary but very diffuse — weakest thesis link in the book", _lb="Large-cap, liquid"),
    "CELH": dict(tc=3, lb=4, _tc="Zero-sugar functional bev — substitution winner vs sugary drinks", _lb="Liquid mid-cap"),
    "TMO":  dict(tc=3, lb=5, _tc="Small-molecule solid-dose API/CDMO layer that scales the oral pill; diluted large-cap", _lb="Mega-cap, maximal liquidity"),
}


def _score_valuation_room(side, pe):
    """SHORT: expensive (high P/E) = more room to fall = HIGH score.
       LONG:  reasonable P/E = HIGH score."""
    if pe is None:
        return 3, "P/E n/a — neutral"
    pe = float(pe)
    if side == "short":
        if pe >= 40:  return 5, f"P/E {pe:.0f} — rich; maximum de-rate room"
        if pe >= 30:  return 4, f"P/E {pe:.0f} — full; good downside room"
        if pe >= 22:  return 3, f"P/E {pe:.0f} — moderate room"
        if pe >= 15:  return 2, f"P/E {pe:.0f} — already cheap; less short room"
        return 1, f"P/E {pe:.0f} — cheap; poor short asymmetry"
    else:  # long
        if pe <= 20:  return 5, f"P/E {pe:.0f} — attractive"
        if pe <= 30:  return 4, f"P/E {pe:.0f} — reasonable"
        if pe <= 50:  return 3, f"P/E {pe:.0f} — full"
        if pe <= 90:  return 2, f"P/E {pe:.0f} — expensive"
        return 1, f"P/E {pe:.0f} — extreme"


def _score_momentum_inflection(side, rs_3m, three_m, dist_52w):
    """SHORT: weak/rolling-over momentum = thesis confirming = HIGH score.
       LONG:  strengthening momentum = HIGH score.
       Uses RS_3m (0-100 cohort percentile), 3M return, distance from 52w high."""
    if rs_3m is None:
        return 3, "RS n/a — neutral"
    rs = float(rs_3m)
    if side == "short":
        # low RS (already weak) is good, but not TOO low (partly spent).
        # Best short: mid-low RS + still near highs (dist_52w near 0) = full but rolling.
        base = 5 if rs <= 25 else 4 if rs <= 45 else 3 if rs <= 65 else 2 if rs <= 85 else 1
        near_high = (dist_52w is not None and float(dist_52w) > -12)
        lbl = f"RS3M {rs:.0f} (weak=confirming)"
        if near_high and base < 5:
            base = min(5, base + 1); lbl += " + still near 52w high (full, rolling over)"
        return base, lbl
    else:  # long
        base = 5 if rs >= 75 else 4 if rs >= 55 else 3 if rs >= 40 else 2 if rs >= 20 else 1
        return base, f"RS3M {rs:.0f} (strength)"


def _score_fundamental_trend(side, rev_growth):
    """SHORT: slowing/low revenue growth = HIGH score (demand already softening).
       LONG:  accelerating/high growth = HIGH score."""
    if rev_growth is None:
        return 3, "rev growth n/a — neutral"
    g = float(rev_growth)
    if side == "short":
        if g <= 0.04:  return 5, f"rev +{g*100:.0f}% — near-stall; volume softening"
        if g <= 0.08:  return 4, f"rev +{g*100:.0f}% — slow"
        if g <= 0.12:  return 3, f"rev +{g*100:.0f}% — moderate"
        if g <= 0.20:  return 2, f"rev +{g*100:.0f}% — still growing; thesis early"
        return 1, f"rev +{g*100:.0f}% — strong growth fights the short"
    else:  # long
        if g >= 0.25:  return 5, f"rev +{g*100:.0f}% — high growth"
        if g >= 0.12:  return 4, f"rev +{g*100:.0f}% — solid"
        if g >= 0.06:  return 3, f"rev +{g*100:.0f}% — moderate"
        if g >= 0.02:  return 2, f"rev +{g*100:.0f}% — sluggish"
        return 1, f"rev +{g*100:.0f}% — stalling"


def _score_setup_asymmetry(side, val_score, mom_score):
    """Composite of valuation room + momentum inflection — the core short/long
    'is this the right entry' read. Average of the two side-aware sub-scores."""
    avg = round((val_score + mom_score) / 2)
    if side == "short":
        return avg, f"short setup: valuation room {val_score}/5 + rolling-over {mom_score}/5"
    return avg, f"long setup: value {val_score}/5 + momentum {mom_score}/5"


def compute():
    cands = json.loads((_HERE / "candidates.json").read_text())["candidates"]
    meta = {c["ticker"]: c for c in cands}
    out = {}
    for ticker, j in JUDGMENT.items():
        c = meta.get(ticker, {})
        side = c.get("side", "long")
        pe = c.get("pe"); rs = c.get("rs_3m"); tm = c.get("3m_pct")
        d52 = c.get("dist_from_52w_high_pct"); rg = c.get("revenue_growth_yoy")

        val_s, val_r = _score_valuation_room(side, pe)
        mom_s, mom_r = _score_momentum_inflection(side, rs, tm, d52)
        fun_s, fun_r = _score_fundamental_trend(side, rg)
        setup_s, setup_r = _score_setup_asymmetry(side, val_s, mom_s)

        scored = {
            "thesis_conviction":   (j["tc"], j["_tc"]),
            "setup_asymmetry":     (setup_s, setup_r),
            "valuation_room":      (val_s, val_r),
            "momentum_inflection": (mom_s, mom_r),
            "fundamental_trend":   (fun_s, fun_r),
            "liquidity_borrow":    (j["lb"], j["_lb"]),
        }
        components = {}
        raw_total = 0
        for crit, weight in WEIGHTS.items():
            score, rationale = scored[crit]
            contribution = score * weight
            raw_total += contribution
            components[crit] = {
                "score": score, "weight": weight, "type": SCORE_TYPE[crit],
                "contribution": contribution, "rationale": rationale,
            }
        out[ticker] = {
            "company": c.get("company"),
            "bucket": c.get("bucket"),
            "sub": c.get("sub"),
            "side": side,
            "note": c.get("note"),
            "raw_total": raw_total,
            "normalized_100": round(raw_total / 5, 1),
            "components": components,
            "snapshot": {k: c.get(k) for k in
                         ["price","market_cap","pe","ps","side","1y_pct","3m_pct","1m_pct",
                          "dist_from_52w_high_pct","rs_3m","revenue_growth_yoy"]},
        }
    return out


def render_md(results):
    today = datetime.now().strftime("%Y-%m-%d")
    rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    L = ["# GLP-1 Mass Adoption — Scoring (v1, side-aware)", "",
         f"_Generated **{today}**. Long/Short rubric: SHORT candidates score high on full valuation + rolling-over momentum + slowing growth (max short asymmetry); LONG candidates score high on momentum + reasonable valuation + growth. Source: `candidates.json`. Audit: `scoring_log.json`._", "",
         "## Ranked candidates", "",
         "| Rank | Ticker | Side | Company | Bucket | Score | Conv | Setup | Val | Mom | Fund | Liq |",
         "|-----:|--------|------|---------|--------|------:|-----|------|-----|-----|------|-----|"]
    for i, (tk, r) in enumerate(rows, 1):
        c = r["components"]; mk = " 🟢" if i <= 5 else " 🟡" if i <= 7 else ""
        L.append(f"| {i}{mk} | **{tk}** | {r['side']} | {r['company']} | {r['bucket']} | **{r['normalized_100']:.0f}** | "
                 f"{c['thesis_conviction']['score']} | {c['setup_asymmetry']['score']} | "
                 f"{c['valuation_room']['score']} | {c['momentum_inflection']['score']} | "
                 f"{c['fundamental_trend']['score']} | {c['liquidity_borrow']['score']} |")
    L += ["", "🟢 = top 5 (tracker)  ·  🟡 = next 2 (watch)", "",
          "_SHORT names dominate the top of the book by design — the thesis is short-weighted._"]
    return "\n".join(L) + "\n"


def main():
    results = compute()
    today = datetime.now().strftime("%Y-%m-%d")
    log = {
        "theme": "glp1", "scored_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": today, "rubric_version": "v1-sideaware",
        "weights": WEIGHTS, "score_types": SCORE_TYPE, "results": results,
    }
    (_HERE / "scoring_log.json").write_text(json.dumps(log, indent=2, default=str))
    hist = _HERE / "history"; hist.mkdir(exist_ok=True)
    (hist / f"scoring_log_{today}.json").write_text(json.dumps(log, indent=2, default=str))
    (_HERE / "scoring.md").write_text(render_md(results))
    rows = sorted(results.items(), key=lambda kv: -kv[1]["raw_total"])
    print(f"\n=== GLP-1 Scoring v1 side-aware ({today}) ===")
    for i, (tk, r) in enumerate(rows, 1):
        tag = "🟢 TRACKER" if i <= 5 else "🟡 WATCH  " if i <= 7 else "         "
        print(f"  {i:2d}. {tag} {tk:5s} {r['normalized_100']:5.0f}  [{r['side']:5s}] {r['company']} ({r['bucket']})")
    print(f"\nSaved scoring.md + scoring_log.json + history snapshot")


if __name__ == "__main__":
    main()
