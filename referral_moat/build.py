"""Referral-Moat pipeline: fetch fundamentals -> compute referral-economics
metrics -> score within industry group -> (separately) compute returns.

DESIGN INVARIANT — theory purity:
  The scoring function receives ONLY fundamental statement data. Price
  returns are computed in a separate pass and attached to the output as an
  *evaluation* field. Nothing in the score can see a stock price.

Run:  python referral_moat/build.py
Out:  referral_moat/data/scorecards.json
      referral_moat/data/snapshots/<date>.json   (for forward tracking)
"""

from __future__ import annotations

import json
import math
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import yfinance as yf

from universe import UNIVERSE, ALL_TICKERS, BENCHMARK

OUT_DIR = Path(__file__).parent / "data"

# ── Pillar weights (sum = 100). See README.md for the rationale of each. ──
WEIGHTS = {
    "growth_efficiency": 25,   # new revenue per $ of S&M ("magic number")
    "sales_intensity": 20,     # S&M % of revenue: level + improving slope
    "growth_persistence": 20,  # multi-year CAGR + steadiness of growth
    "gross_quality": 15,       # gross margin level + trend (pricing power)
    "operating_leverage": 10,  # incremental operating margin
    "rule_of_40": 10,          # growth + FCF margin (efficient growth)
}


# ─────────────────────────── fetch ───────────────────────────

def _row(df: pd.DataFrame, *names):
    """First matching row from a yfinance statement, as {year: value}."""
    if df is None or df.empty:
        return {}
    for n in names:
        if n in df.index:
            s = df.loc[n].dropna()
            return {c.year: float(v) for c, v in s.items()}
    return {}


def fetch_one(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        inc = t.income_stmt
        cf = t.cash_flow
        if inc is None or inc.empty:
            return None
        info = {}
        try:
            info = t.info or {}
        except Exception:
            pass
        first_trade = (info.get("firstTradeDateEpochUtc")
                       or (info.get("firstTradeDateMilliseconds") or 0) / 1000
                       or None)
        return {
            "ticker": ticker,
            "name": info.get("shortName") or ticker,
            "market_cap": info.get("marketCap"),
            "listed_years": round((dt.datetime.now(dt.timezone.utc).timestamp()
                                   - first_trade) / (365.25 * 86400), 1)
                            if first_trade else None,
            "revenue": _row(inc, "Total Revenue", "Operating Revenue"),
            "gross_profit": _row(inc, "Gross Profit"),
            "op_income": _row(inc, "Operating Income",
                              "Total Operating Income As Reported"),
            "sm": _row(inc, "Selling And Marketing Expense"),
            "sga": _row(inc, "Selling General And Administration"),
            "rnd": _row(inc, "Research And Development"),
            "fcf": _row(cf, "Free Cash Flow"),
        }
    except Exception as e:
        print(f"  !! {ticker}: {e}")
        return None


def fetch_all(tickers: list[str]) -> dict[str, dict]:
    out = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_one, tk): tk for tk in tickers}
        for i, f in enumerate(as_completed(futs), 1):
            tk = futs[f]
            r = f.result()
            if r and len(r["revenue"]) >= 3:
                out[tk] = r
            else:
                print(f"  -- dropped {tk} (insufficient data)")
            if i % 25 == 0:
                print(f"  fetched {i}/{len(tickers)}")
    return out


# ─────────────────────── per-year metrics ───────────────────────

def yearly_metrics(c: dict) -> list[dict]:
    """Per-fiscal-year raw metric series (oldest -> newest)."""
    years = sorted(c["revenue"])
    rows = []
    for y in years:
        rev = c["revenue"].get(y)
        prev = c["revenue"].get(y - 1)
        sm = c["sm"].get(y) or c["sga"].get(y)  # S&M, else SG&A fallback
        gp, oi, fcf = (c[k].get(y) for k in ("gross_profit", "op_income", "fcf"))
        oi_prev = c["op_income"].get(y - 1)
        row = {"year": y, "revenue": rev}
        if rev and prev:
            row["rev_growth"] = rev / prev - 1
            if sm and sm > 0:
                row["magic"] = (rev - prev) / sm
        if rev and sm:
            row["sm_pct"] = sm / rev
            row["sm_is_sga"] = y not in c["sm"]
        if rev and gp is not None:
            row["gross_margin"] = gp / rev
        if rev and oi is not None:
            row["op_margin"] = oi / rev
        if rev and prev and oi is not None and oi_prev is not None \
                and (rev - prev) > 0:
            row["incr_margin"] = (oi - oi_prev) / (rev - prev)
        if rev and fcf is not None:
            row["fcf_margin"] = fcf / rev
        rows.append(row)
    return rows


def _slope(pairs: list[tuple[int, float]]) -> float | None:
    """OLS slope per year over (year, value) pairs."""
    if len(pairs) < 3:
        return None
    xs, ys = zip(*pairs)
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den


def _series(rows, key):
    return [(r["year"], r[key]) for r in rows if key in r and r[key] is not None]


def pillar_inputs(rows: list[dict]) -> dict:
    """Collapse the per-year series into the six pillar raw values."""
    out = {}
    magic = [v for _, v in _series(rows, "magic")][-2:]
    if magic:
        out["growth_efficiency"] = sum(magic) / len(magic)

    smp = _series(rows, "sm_pct")
    if smp:
        level = smp[-1][1]
        slope = _slope(smp[-4:])
        # lower intensity better; improving (negative slope) better
        out["sales_intensity"] = -(level + (slope * 3 if slope is not None else 0))
        out["_sm_pct"] = level
        out["_sm_slope"] = slope

    growth = [v for _, v in _series(rows, "rev_growth")]
    if growth:
        window = growth[-3:]
        cagr = sum(window) / len(window)
        vol = (sum((g - cagr) ** 2 for g in window) / len(window)) ** 0.5 \
            if len(window) > 1 else 0.0
        out["growth_persistence"] = cagr - 0.5 * vol
        out["_rev_cagr3"] = cagr
        out["_rev_latest"] = growth[-1]

    gm = _series(rows, "gross_margin")
    if gm:
        slope = _slope(gm[-4:]) or 0.0
        out["gross_quality"] = gm[-1][1] + 3 * slope
        out["_gross_margin"] = gm[-1][1]

    im = [min(max(v, -1.0), 1.5) for _, v in _series(rows, "incr_margin")][-2:]
    if im:
        out["operating_leverage"] = sum(im) / len(im)

    fm = _series(rows, "fcf_margin")
    if fm and growth:
        out["rule_of_40"] = growth[-1] + fm[-1][1]
    return out


# ────────────────────── flywheel gate (picks) ──────────────────────
# "Pick growing companies that are growing BECAUSE OF the referral
# signal." RES ranks quality within a group; the flywheel gate is the
# picking overlay — all three must hold, from fundamentals only:
#   1. growing            — latest rev growth ≥ 8% AND 3y avg ≥ 10%
#   2. efficient_acquisition — magic (ΔRev ÷ S&M, 2y avg) ≥ 1.0: each
#      sales dollar returns ≥ $1 of new annual revenue, i.e. growth is
#      not being bought at accelerating cost
#   3. intensity_not_rising  — S&M% slope ≤ +0.5pp/yr (falling/flat
#      sales intensity while growing = customers arriving on their own);
#      if no slope is measurable, S&M% must be at/below group median
FLYWHEEL = {"min_rev_latest": 0.08, "min_rev_cagr3": 0.10,
            "min_magic": 1.0, "max_sm_slope": 0.005}


def flywheel_test(pins: dict, group_sm_median: float | None) -> dict:
    g_latest, g_cagr = pins.get("_rev_latest"), pins.get("_rev_cagr3")
    growing = (g_latest is not None and g_cagr is not None
               and g_latest >= FLYWHEEL["min_rev_latest"]
               and g_cagr >= FLYWHEEL["min_rev_cagr3"])
    magic = pins.get("growth_efficiency")
    efficient = magic is not None and magic >= FLYWHEEL["min_magic"]
    slope, level = pins.get("_sm_slope"), pins.get("_sm_pct")
    if slope is not None:
        not_rising = slope <= FLYWHEEL["max_sm_slope"]
    elif level is not None and group_sm_median is not None:
        not_rising = level <= group_sm_median
    else:
        not_rising = None  # unmeasurable — cannot pass
    checks = {"growing": growing, "efficient_acquisition": efficient,
              "intensity_not_rising": not_rising}
    return {"pass": all(v is True for v in checks.values()), "checks": checks}


# ──────────────── early indicators (young / pivoting) ────────────────
# The flywheel gate structurally favors incumbents: it demands 3y of
# proven history, which recent IPOs and mid-pivot companies cannot show.
# The Early Indicators gate looks for the referral fingerprint *forming*:
# current-year signals plus direction of travel, not multi-year averages.
#
# Eligibility (one of):
#   young          — listed ≤ 5 years (real IPO date from exchange
#                    metadata; statement count is NOT a youth signal —
#                    Yahoo sometimes returns only 4 years for mega-caps).
#                    Fallback when listing date is unknown: ≤ 3 fiscal
#                    years of revenue on record.
#   reacceleration — growth was < 10% two FYs ago, now ≥ 15% (a pivot
#                    catching on: the trailing average hides the new
#                    engine) AND revenue < $10B — a giant re-accelerating
#                    is not "early", and revenue (not market cap) keeps
#                    the size test free of price data.
# Gates (all three, latest data only):
#   fast_growth      — latest rev growth ≥ 15% (higher bar than flywheel's
#                      8%: with no history, the present must be loud)
#   efficient_latest — latest-year magic ≥ 1.0
#   engine_improving — S&M% fell vs prior year OR magic rose vs prior year
EARLY = {"max_listed_years": 5.0, "fallback_max_years_data": 3,
         "reaccel_from": 0.10, "reaccel_to": 0.15, "reaccel_max_rev": 10e9,
         "min_rev_latest": 0.15, "min_magic_latest": 1.0}


def early_test(rows: list[dict], listed_years: float | None) -> dict:
    growth = _series(rows, "rev_growth")
    magic = _series(rows, "magic")
    smp = _series(rows, "sm_pct")
    years = len([r for r in rows if r.get("revenue") is not None])
    if listed_years is not None:
        young = listed_years <= EARLY["max_listed_years"]
    else:
        young = years <= EARLY["fallback_max_years_data"]
    latest_rev = next((r["revenue"] for r in reversed(rows)
                       if r.get("revenue") is not None), None)
    reaccel = (len(growth) >= 3 and growth[-3][1] < EARLY["reaccel_from"]
               and growth[-1][1] >= EARLY["reaccel_to"]
               and latest_rev is not None
               and latest_rev < EARLY["reaccel_max_rev"])
    eligible = young or reaccel
    fast = bool(growth) and growth[-1][1] >= EARLY["min_rev_latest"]
    efficient = bool(magic) and magic[-1][1] >= EARLY["min_magic_latest"]
    improving = ((len(smp) >= 2 and smp[-1][1] < smp[-2][1])
                 or (len(magic) >= 2 and magic[-1][1] > magic[-2][1]))
    checks = {"fast_growth": fast, "efficient_latest": efficient,
              "engine_improving": improving}
    return {"eligible": eligible,
            "why": ("young" if young else "reacceleration") if eligible else None,
            "years_of_data": years, "listed_years": listed_years,
            "pass": eligible and all(checks.values()), "checks": checks}


# ─────────────────────────── scoring ───────────────────────────

def pct_rank(values: dict[str, float]) -> dict[str, float]:
    """ticker -> percentile (0-100) among non-null peers."""
    items = sorted(values.items(), key=lambda kv: kv[1])
    n = len(items)
    if n == 1:
        return {items[0][0]: 50.0}
    return {tk: 100.0 * i / (n - 1) for i, (tk, _) in enumerate(items)}


def score_group(members: dict[str, dict]) -> dict[str, dict]:
    """members: ticker -> pillar_inputs. Returns ticker -> pillar pct + RES."""
    ranks: dict[str, dict[str, float]] = {tk: {} for tk in members}
    for pillar in WEIGHTS:
        vals = {tk: p[pillar] for tk, p in members.items()
                if p.get(pillar) is not None and not math.isnan(p[pillar])}
        for tk, pct in pct_rank(vals).items():
            ranks[tk][pillar] = pct
    out = {}
    for tk, r in ranks.items():
        avail = {p: w for p, w in WEIGHTS.items() if p in r}
        tot = sum(avail.values())
        res = sum(r[p] * w for p, w in avail.items()) / tot if tot else None
        out[tk] = {"pillars": r, "res": round(res, 1) if res is not None else None,
                   "coverage": round(tot / sum(WEIGHTS.values()), 2)}
    return out


# ────────────── returns (OUTPUT ONLY — never fed to scoring) ──────────────

def compute_returns(tickers: list[str]) -> dict[str, dict]:
    px = yf.download(tickers + [BENCHMARK], period="6y", interval="1mo",
                     auto_adjust=True, progress=False)["Close"]
    px = px.dropna(how="all")
    out = {}
    spy = px[BENCHMARK].dropna()

    def tr(s: pd.Series, months: int):
        s = s.dropna()
        if len(s) < months + 1:
            return None
        return float(s.iloc[-1] / s.iloc[-(months + 1)] - 1)

    for tk in tickers:
        if tk not in px.columns:
            continue
        s = px[tk]
        r = {}
        for label, m in (("r1y", 12), ("r3y", 36), ("r5y", 60)):
            v, b = tr(s, m), tr(spy, m)
            if v is not None:
                r[label] = round(v, 4)
                if b is not None:
                    yrs = m / 12
                    r[label + "_alpha"] = round(
                        (1 + v) ** (1 / yrs) - (1 + b) ** (1 / yrs), 4)
        out[tk] = r
    return out


def spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 5:
        return None
    def ranks(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        for pos, i in enumerate(order):
            r[i] = pos
        return r
    rx, ry = ranks(xs), ranks(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = (sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry)) ** 0.5
    return num / den if den else None


# ─────────────────────────── main ───────────────────────────

def main():
    print(f"Fetching fundamentals for {len(ALL_TICKERS)} tickers …")
    raw = fetch_all(ALL_TICKERS)
    print(f"Got usable data for {len(raw)} tickers.")

    companies = {}
    for group, tickers in UNIVERSE.items():
        members = {}
        for tk in tickers:
            if tk not in raw:
                continue
            rows = yearly_metrics(raw[tk])
            pins = pillar_inputs(rows)
            members[tk] = pins
            early = early_test(rows, raw[tk].get("listed_years"))
            companies[tk] = {
                "early": early,
                "ticker": tk,
                "name": raw[tk]["name"],
                "group": group,
                "market_cap": raw[tk]["market_cap"],
                "years": rows,
                "pillar_raw": {k: v for k, v in pins.items()},
            }
        for tk, sc in score_group(members).items():
            companies[tk].update(sc)
        # flywheel gate — fundamentals only, needs the group's median S&M%
        sm_levels = sorted(p["_sm_pct"] for p in members.values()
                           if p.get("_sm_pct") is not None)
        sm_median = sm_levels[len(sm_levels) // 2] if sm_levels else None
        for tk, pins in members.items():
            companies[tk]["flywheel"] = flywheel_test(pins, sm_median)

    # global rank across the whole universe (by RES)
    ranked = sorted((c for c in companies.values() if c["res"] is not None),
                    key=lambda c: -c["res"])
    for i, c in enumerate(ranked, 1):
        c["universe_rank"] = i

    # picks = flywheel passers with decent measurability, outside controls,
    # ordered by RES. Chosen strictly BEFORE the returns pass below.
    picks = [c["ticker"] for c in ranked
             if c["flywheel"]["pass"] and c["coverage"] >= 0.5
             and not c["group"].startswith("Control")]

    # early indicators: young/pivoting names where the fingerprint is
    # forming. Same RES order; overlap with `picks` is allowed and marked
    # in the app (a young company passing both is best-of-breed).
    early_picks = [c["ticker"] for c in ranked
                   if c["early"]["pass"]
                   and not c["group"].startswith("Control")]

    # ── returns pass: strictly after scoring, output-only ──
    print("Computing returns (output-only evaluation) …")
    rets = compute_returns(list(companies))
    for tk, r in rets.items():
        companies[tk]["returns"] = r

    # theory check: does RES associate with 3y alpha? (concurrent, not forward)
    checks = {}
    def corr_for(tks):
        pairs = [(companies[t]["res"], companies[t]["returns"].get("r3y_alpha"))
                 for t in tks if companies[t].get("res") is not None
                 and companies[t].get("returns", {}).get("r3y_alpha") is not None]
        if len(pairs) < 5:
            return None
        return spearman([p[0] for p in pairs], [p[1] for p in pairs])
    rho = corr_for(list(companies))
    checks["all"] = {"spearman_res_vs_3y_alpha": round(rho, 3) if rho else None,
                     "n": len(companies)}
    for group, tickers in UNIVERSE.items():
        rho = corr_for([t for t in tickers if t in companies])
        if rho is not None:
            checks[group] = {"spearman_res_vs_3y_alpha": round(rho, 3)}

    payload = {
        "as_of": dt.date.today().isoformat(),
        "weights": WEIGHTS,
        "flywheel_thresholds": FLYWHEEL,
        "early_thresholds": EARLY,
        "groups": list(UNIVERSE.keys()),
        "picks": picks,
        "early_picks": early_picks,
        "companies": companies,
        "theory_checks": checks,
        "note": ("Scores computed from fundamentals only; returns attached "
                 "afterwards as evaluation. 3y-alpha check is concurrent "
                 "association, not a forward test — forward test starts at "
                 "the first snapshot date."),
    }
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "scorecards.json").write_text(json.dumps(payload), encoding="utf-8")
    snap = OUT_DIR / "snapshots"
    snap.mkdir(exist_ok=True)
    slim = {tk: {"res": c["res"], "group": c["group"],
                 "flywheel": c["flywheel"]["pass"],
                 "early": c["early"]["pass"]}
            for tk, c in companies.items()}
    (snap / f"{payload['as_of']}.json").write_text(json.dumps(slim), encoding="utf-8")
    print(f"Wrote {OUT_DIR / 'scorecards.json'} ({len(companies)} companies).")


if __name__ == "__main__":
    main()
