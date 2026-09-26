"""
portfolio_B.py -- INDEPENDENT BUILDER B for PORTFOLIO_PREREG.md.

Capital-constrained portfolio simulation of the Ignition Watch exit rules R0-R6.
Built from the raw yfinance price pickles (../px/c*.pkl). Does NOT import or copy
engine.py: the ignition signal, fresh entries, floor and exit triggers are
re-derived here from the spec + the engine docstring (+ the signal formula of the
original study, research/ignition_exits/exit_study.py, which the docstring says
the entries are identical to).

Run:  python portfolio_B.py --period train|holdout [--verify]
Writes portfolio_B_<period>.json next to this file.

SEAL: with --period train the price panel is cut at 2021-12-31 BEFORE anything is
computed, so no post-2021 price can influence (or appear in) any train number.
All indicators are trailing, so the cut changes nothing on or before 2021-12-31.

Definitions
  signal   r5 > 12%  AND  vol(21d avg)/vol(126d avg) > 1.5  AND  MA50 > MA200
           AND close > $3 AND 21d avg dollar volume > $5M AND 252d return exists
  entry    a fire at session t with no fire in sessions t-20..t-1 (fresh), buy at
           the open of t+1 (open must exist, as in the study)
  path     k = 0..252 sessions after entry (k=0 = entry day), closes ffilled
  floor    close < base, base = raw close 5 sessions before the fire
  ret      close / entry_open - 1 (raw prices, costs excluded, as in the engine)
  since_fire  k - (last path index with a fire), or k+1 if none yet on the path
  refired  True from the first fire at k >= 5 onwards
  exits    a trigger on close k (k = 0..251) sells at the open of k+1
           (missing open -> that day's close -> last close); no trigger by k=251
           -> sell at the close of k=252 (hard max hold).
  skipped  candidates skipped for lack of a SLOT (K positions already open), the
           spec's literal metric; cash-short (<50% of a slot) and already-held skips
           are reported separately as skipped_cash / skipped_held.
  CAGR     calendar years from the start-flat anchor (close before the first
           session) to the last session, /365.25 (same convention as builder A).
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PX = os.environ.get("IGN_PX", os.path.normpath(os.path.join(HERE, "..", "px")))

H = 252           # hard max hold (sessions after entry)
GAP = 20          # fresh fire: no fire in the prior 20 sessions
KS = (20, 10)     # primary K first
COSTS = (10, 0)   # bps per side, primary first
INIT = 1.0

# Row labels are shared with builder A (portfolio_A.py) so the two result sets key identically.
RULES = {
    "R0_current": "current: floor",
    "R1_hold252": "hold252: no floor (reference, not eligible)",
    "R2_A10": "A10: floor | (k>=126 & 0<ret<0.30)",
    "R3_A09": "A09: floor | (k>=63 & 0<ret<0.30)",
    "R4_A08": "A08: floor | (since_fire>=63 & ret>0)",
    "R5_A02": "A02: floor | (k>=63 & ret>0)",
    "R6_A06": "A06: floor | (k>=63 & ret>0 & ~refired)",
}
ELIGIBLE = ("R2_A10", "R3_A09", "R4_A08", "R5_A02", "R6_A06")

PERIODS = {
    # cutoff = last price date loaded (seal); start/end = simulated sessions;
    # fire_from/fire_to = window for the fire date of an entry candidate.
    "train": dict(cutoff="2021-12-31", start="2016-01-01", end="2021-12-31",
                  fire_from="2016-01-01", fire_to="2021-12-31"),
    "holdout": dict(cutoff=None, start="2022-01-01", end=None,
                    fire_from="2021-12-31", fire_to=None),
}


# --------------------------------------------------------------------------- data
def load_prices(cutoff: str | None):
    parts = [pd.read_pickle(f) for f in sorted(glob.glob(os.path.join(PX, "c*.pkl")))]
    out = {}
    for fld in ("Open", "Close", "Volume"):
        df = pd.concat([p[fld] for p in parts], axis=1)
        df = df.loc[:, ~df.columns.duplicated()].sort_index()
        if cutoff is not None:
            df = df.loc[:pd.Timestamp(cutoff)]
        out[fld] = df
    cols = out["Close"].columns
    for fld in out:
        out[fld] = out[fld].reindex(columns=cols).astype(float)
    return out


def build_signal(px):
    C, V = px["Close"], px["Volume"]
    r5 = C.pct_change(5, fill_method=None)
    r252 = C.pct_change(252, fill_method=None)
    volr = V.rolling(21).mean() / V.rolling(126).mean()
    golden = C.rolling(50).mean() > C.rolling(200).mean()
    dv = (C * V).rolling(21).mean()
    liquid = (C > 3) & (dv > 5e6)
    valid = liquid & r252.notna()
    sig = ((r5 > 0.12) & (volr > 1.5) & golden & valid).to_numpy()
    return sig, r5.to_numpy(), liquid.to_numpy()


def fresh_entries(sig, Ov):
    """(t, j): fire at t, no fire in t-20..t-1, next session exists with a finite open."""
    n_days, n_tk = sig.shape
    out = []
    for j in range(n_tk):
        last = -10_000
        for t in np.flatnonzero(sig[:, j]):
            if t - last > GAP and t + 1 < n_days and np.isfinite(Ov[t + 1, j]):
                out.append((int(t), j))
            last = t
    return out


# --------------------------------------------------------------------------- exits
def rule_trigger(rule, c, k, ret, base, since_fire, refired):
    rule = rule.split("_")[0]          # "R0_current" -> "R0"
    floor = c < base
    if rule == "R0":
        return floor
    if rule == "R1":
        return np.zeros_like(floor)
    if rule == "R2":
        return floor | ((k >= 126) & (ret > 0) & (ret < 0.30))
    if rule == "R3":
        return floor | ((k >= 63) & (ret > 0) & (ret < 0.30))
    if rule == "R4":
        return floor | ((since_fire >= 63) & (ret > 0))
    if rule == "R5":
        return floor | ((k >= 63) & (ret > 0))
    if rule == "R6":
        return floor | ((k >= 63) & (ret > 0) & ~refired)
    raise ValueError(rule)


def exit_plan(t, j, rule, sig, Ov, Cv, Cff):
    """Deterministic exit of one trade under one rule (independent of the portfolio).
    Returns (exit_idx, kind, price) with kind 'open' or 'close', or None if the
    trade is still open at the end of the loaded data."""
    n_days = Cv.shape[0]
    e = t + 1
    end = min(e + H, n_days - 1)
    entry = Ov[e, j]
    c = Cff[e:end + 1, j]
    k = np.arange(len(c))
    ret = c / entry - 1.0
    base = Cv[t - 5, j]
    s = sig[e:end + 1, j]
    last_fire = np.maximum.accumulate(np.where(s, k, -1))
    since_fire = np.where(last_fire >= 0, k - last_fire, k + 1)
    refired = np.logical_or.accumulate(s & (k >= 5))
    with np.errstate(invalid="ignore"):
        trig = np.asarray(rule_trigger(rule, c, k, ret, base, since_fire, refired), dtype=bool)
    trig = trig[:len(c) - 1]          # needs a next session; k <= 251 when complete
    hit = np.flatnonzero(trig)
    if len(hit):
        xi = e + int(hit[0]) + 1
        xp = Ov[xi, j]
        if not np.isfinite(xp):
            xp = Cv[xi, j] if np.isfinite(Cv[xi, j]) else Cff[xi, j]
        return xi, "open", float(xp)
    if e + H <= n_days - 1:
        return e + H, "close", float(Cff[e + H, j])
    return None


# --------------------------------------------------------------------------- portfolio
def simulate(rule, K, cost_bps, cands, plans, ps, pe, Ov, Cff, fire_of, tickers):
    cst = cost_bps / 1e4
    cash = INIT
    e_prev = INIT
    pos = {}                     # j -> dict(sh, ei, t, xi, xk, xp)
    eq, expo = [], []
    trades = sk_slot = sk_cash = sk_held = 0
    closed = []                  # (ticker, fire_idx, entry_idx, exit_idx)
    checks = dict(min_cash=np.inf, min_equity=np.inf, max_positions=0,
                  bad_entry_timing=0, bad_exit_timing=0, double_holdings=0)
    for d in range(ps, pe + 1):
        # 1) exits at the open (triggered by the previous close)
        for j in [j for j, p in pos.items() if p["xk"] == "open" and p["xi"] == d]:
            p = pos.pop(j)
            if not d > p["ei"]:
                checks["bad_exit_timing"] += 1
            cash += p["sh"] * p["xp"] * (1 - cst)
            closed.append((tickers[j], p["t"], p["ei"], d))
        checks["min_cash"] = min(checks["min_cash"], cash)
        # 2) entries at the open, ranked by 5-day return at the fire
        slot = e_prev / K
        for _, _, t, j in cands.get(d, ()):
            if j in pos:
                sk_held += 1
                continue
            if len(pos) >= K:
                sk_slot += 1
                continue
            size = min(slot, cash)
            if size < 0.5 * slot:
                sk_cash += 1
                continue
            if not d >= t + 1:
                checks["bad_entry_timing"] += 1
            xi, xk, xp = plans[(t, j)] if plans[(t, j)] is not None else (10**9, "none", np.nan)
            sh = size / (Ov[d, j] * (1 + cst))     # cost paid out of the slot
            cash -= size
            pos[j] = dict(sh=sh, ei=d, t=t, xi=xi, xk=xk, xp=xp, px=float(Ov[d, j]))
            trades += 1
            checks["min_cash"] = min(checks["min_cash"], cash)
        checks["max_positions"] = max(checks["max_positions"], len(pos))
        # 3) hard max-hold exits at the close
        for j in [j for j, p in pos.items() if p["xk"] == "close" and p["xi"] == d]:
            p = pos.pop(j)
            if not d > p["ei"]:
                checks["bad_exit_timing"] += 1
            cash += p["sh"] * p["xp"] * (1 - cst)
            closed.append((tickers[j], p["t"], p["ei"], d))
        # 4) mark to market at the close (ffilled)
        inv = float(sum(p["sh"] * (Cff[d, j] if np.isfinite(Cff[d, j]) else p["px"])
                        for j, p in pos.items()))
        eq_d = cash + inv
        eq.append(eq_d)
        expo.append(inv / eq_d if eq_d > 0 else 0.0)
        checks["min_cash"] = min(checks["min_cash"], cash)
        checks["min_equity"] = min(checks["min_equity"], eq_d)
        e_prev = eq_d
    # overlapping holdings of the same ticker (closed + still open)
    spans = {}
    for tk, t, ei, xi in closed + [(tickers[j], p["t"], p["ei"], pe + 1) for j, p in pos.items()]:
        spans.setdefault(tk, []).append((ei, xi))
    for tk, sp in spans.items():
        sp.sort()
        for (a0, a1), (b0, b1) in zip(sp, sp[1:]):
            if b0 < a1:            # next entry before previous exit day
                checks["double_holdings"] += 1
    # `skipped` = lack of a slot (K positions open), the spec's literal metric; cash-short and
    # already-held skips are reported separately.
    return dict(eq=np.array(eq), expo=np.array(expo), trades=trades, skipped=sk_slot,
                skipped_slot=sk_slot, skipped_cash=sk_cash, skipped_held=sk_held, open_at_end=len(pos),
                closed=len(closed), checks=checks)


def perf(eq, years):
    full = np.concatenate([[INIT], eq])
    r = full[1:] / full[:-1] - 1.0
    cagr = (full[-1] / INIT) ** (1.0 / years) - 1.0
    dd = full / np.maximum.accumulate(full) - 1.0
    maxdd = float(dd.min())
    sd = r.std(ddof=1)
    sharpe = float(r.mean() / sd * math.sqrt(252)) if sd > 0 else float("nan")
    mar = cagr / abs(maxdd) if maxdd < 0 else float("nan")
    return dict(cagr=float(cagr), maxdd=maxdd, sharpe=sharpe, mar=float(mar),
                final_equity=float(full[-1]))


# --------------------------------------------------------------------------- driver
def run(period: str, cutoff_override: str | None = "__default__", verbose=True):
    cfg = PERIODS[period]
    cutoff = cfg["cutoff"] if cutoff_override == "__default__" else cutoff_override
    px = load_prices(cutoff)
    O, C = px["Open"], px["Close"]
    dates = C.index
    tickers = list(C.columns)
    Ov, Cv = O.to_numpy(), C.to_numpy()
    Cff = C.ffill().to_numpy()
    sig, r5, liquid = build_signal(px)
    ents = fresh_entries(sig, Ov)

    start = pd.Timestamp(cfg["start"])
    end = pd.Timestamp(cfg["end"]) if cfg["end"] else dates[-1]
    ps = int(np.searchsorted(dates, start, side="left"))
    pe = int(np.searchsorted(dates, end, side="right")) - 1
    assert 0 < ps <= pe < len(dates)
    anchor = dates[ps - 1]                      # prior close = "start flat" point
    years = (dates[pe] - anchor).days / 365.25
    ff = pd.Timestamp(cfg["fire_from"])
    ft = pd.Timestamp(cfg["fire_to"]) if cfg["fire_to"] else dates[-1]

    # entry candidates for this period
    cand_list = [(t, j) for t, j in ents
                 if ff <= dates[t] <= ft and ps <= t + 1 <= pe]
    cands = {}
    for t, j in cand_list:
        cands.setdefault(t + 1, []).append((-r5[t, j], tickers[j], t, j))
    for d in cands:
        cands[d].sort()
    plans = {r: {(t, j): exit_plan(t, j, r, sig, Ov, Cv, Cff) for t, j in cand_list} for r in RULES}

    # equal-weight benchmark: mean close-to-close return of tickers liquid at the prior close
    bench_r = []
    for d in range(ps, pe + 1):
        m = liquid[d - 1] & np.isfinite(Cv[d]) & np.isfinite(Cv[d - 1])
        bench_r.append(float(np.mean(Cv[d, m] / Cv[d - 1, m] - 1.0)))
    bench_eq = INIT * np.cumprod(1.0 + np.array(bench_r))
    bench = perf(bench_eq, years)

    rows, all_checks = [], {}
    for r in RULES:
        for K in KS:
            for cb in COSTS:
                s = simulate(r, K, cb, cands, plans[r], ps, pe, Ov, Cff, None, tickers)
                p = perf(s["eq"], years)
                rows.append(dict(rule=r, desc=RULES[r], K=K, cost_bps=cb, **p,
                                 exposure=float(s["expo"].mean()), trades=s["trades"],
                                 skipped=s["skipped"], skipped_slot=s["skipped_slot"],
                                 skipped_cash=s["skipped_cash"],
                                 skipped_held=s["skipped_held"], open_at_end=s["open_at_end"]))
                all_checks[f"{r}|{K}|{cb}"] = s["checks"]

    sanity = dict(
        min_cash=min(c["min_cash"] for c in all_checks.values()),
        min_equity=min(c["min_equity"] for c in all_checks.values()),
        max_positions_vs_K=all(c["max_positions"] <= int(key.split("|")[1])
                               for key, c in all_checks.items()),
        bad_entry_timing=sum(c["bad_entry_timing"] for c in all_checks.values()),
        bad_exit_timing=sum(c["bad_exit_timing"] for c in all_checks.values()),
        double_holdings=sum(c["double_holdings"] for c in all_checks.values()),
        candidates_in_window=len(cand_list),
        candidate_fire_first=str(min(dates[t] for t, _ in cand_list).date()),
        candidate_fire_last=str(max(dates[t] for t, _ in cand_list).date()),
    )
    sanity["all_pass"] = bool(sanity["min_cash"] >= -1e-12 and sanity["min_equity"] > 0
                              and sanity["max_positions_vs_K"] and sanity["bad_entry_timing"] == 0
                              and sanity["bad_exit_timing"] == 0 and sanity["double_holdings"] == 0)

    out = dict(builder="B", period=period,
               sessions=f"{dates[ps].date()} .. {dates[pe].date()}", n_sessions=pe - ps + 1,
               years=years, data_cutoff=str(dates[-1].date()),
               benchmark=bench, rows=rows, sanity=sanity)
    if period == "train":
        out["decision_train"] = decide(rows)
    return out, dict(ents=ents, dates=dates, tickers=tickers, sig=sig, Ov=Ov, Cv=Cv, Cff=Cff, pe=pe)


def decide(rows):
    g = {(r["rule"], r["K"], r["cost_bps"]): r for r in rows}
    r0_20, r0_10 = g[("R0_current", 20, 10)], g[("R0_current", 10, 10)]
    res = {}
    for r in ELIGIBLE:
        a20, a10 = g[(r, 20, 10)], g[(r, 10, 10)]
        p1 = a20["cagr"] > r0_20["cagr"]
        p2 = a20["maxdd"] >= r0_20["maxdd"] - 0.05
        p3 = a10["cagr"] > r0_10["cagr"]
        res[r] = dict(P1=bool(p1), P2=bool(p2), P3=bool(p3), passes=bool(p1 and p2 and p3),
                      cagr_K20=a20["cagr"])
    passers = [r for r in res if res[r]["passes"]]
    res["locked_winner"] = max(passers, key=lambda r: res[r]["cagr_K20"]) if passers else None
    return res


def print_table(out):
    print(f"\nPORTFOLIO B  period={out['period']}  sessions {out['sessions']} "
          f"({out['n_sessions']}, {out['years']:.3f} y)  data cut {out['data_cutoff']}")
    hdr = f"{'rule':11} {'K':>3} {'bps':>4} {'CAGR':>8} {'maxDD':>8} {'Sharpe':>7} {'MAR':>6} {'expo':>6} {'trades':>6} {'skip':>5} {'skSlot':>6} {'skCash':>6} {'skHeld':>6} {'open':>4}"
    print(hdr)
    print("-" * len(hdr))
    for r in out["rows"]:
        print(f"{r['rule']:11} {r['K']:>3} {r['cost_bps']:>4} {r['cagr']:>8.2%} {r['maxdd']:>8.2%} "
              f"{r['sharpe']:>7.2f} {r['mar']:>6.2f} {r['exposure']:>6.1%} {r['trades']:>6} "
              f"{r['skipped']:>5} {r['skipped_slot']:>6} {r['skipped_cash']:>6} {r['skipped_held']:>6} {r['open_at_end']:>4}")
    b = out["benchmark"]
    print(f"{'EW':11} {'':>3} {'':>4} {b['cagr']:>8.2%} {b['maxdd']:>8.2%} {b['sharpe']:>7.2f} {b['mar']:>6.2f}")
    print("\nsanity:", json.dumps(out["sanity"]))
    if "decision_train" in out:
        print("decision (train, K=20, 10 bps):", json.dumps(out["decision_train"]))


def verify_train(aux):
    """Cross-check fresh entries and per-trade exits against the original study's
    trade file (TRAIN split only; data is cut at 2021-12-31, so only exits that
    land on or before the cut are compared)."""
    tr = pd.read_pickle(os.path.join(PX, "exit_trades.pkl"))
    tr = tr[tr.split == "train"]
    dates, tickers, ents = aux["dates"], aux["tickers"], aux["ents"]
    mine = {(tickers[j], dates[t]) for t, j in ents if dates[t] < pd.Timestamp("2022-01-01")}
    theirs = {(r.ticker, pd.Timestamp(r.fire)) for r in tr.itertuples()}
    print(f"\nverify: my train entries {len(mine)}  study train entries {len(theirs)}  "
          f"only-mine {len(mine - theirs)}  only-study {len(theirs - mine)}")
    tix = {tk: i for i, tk in enumerate(tickers)}
    didx = {d: i for i, d in enumerate(dates)}
    pairs = {"base_fail": "R0_current", "hold252": "R1_hold252"}
    for col, rule in pairs.items():
        n = bad = unresolved = 0
        for r in tr.itertuples():
            t, j = didx[pd.Timestamp(r.fire)], tix[r.ticker]
            ret, held, early, _ = getattr(r, col)
            xi_study = t + 1 + int(held)
            plan = exit_plan(t, j, rule, aux["sig"], aux["Ov"], aux["Cv"], aux["Cff"])
            if xi_study > aux["pe"]:
                unresolved += 1
                if plan is not None:
                    bad += 1
                continue
            n += 1
            my_ret = plan[2] / aux["Ov"][t + 1, j] - 1 if plan else np.nan
            if plan is None or plan[0] != xi_study or not np.isclose(my_ret, ret, atol=1e-9):
                bad += 1
        print(f"verify {col:10s} vs {rule}: compared {n}, mismatches {bad}, exit after cut {unresolved}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=("train", "holdout"), required=True)
    ap.add_argument("--verify", action="store_true", help="train only: check vs study trade file")
    a = ap.parse_args()
    out, aux = run(a.period)
    print_table(out)
    path = os.path.join(HERE, f"portfolio_B_{a.period}.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=1, default=float)
    print("wrote", path)
    if a.verify and a.period == "train":
        verify_train(aux)


if __name__ == "__main__":
    main()
