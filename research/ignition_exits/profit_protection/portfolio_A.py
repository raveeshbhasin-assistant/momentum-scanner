"""
Portfolio-level test of the profit-taking rules (BUILDER A, built on engine.py).

Spec: PORTFOLIO_PREREG.md (binding). Rules R0-R6 are the per-trade rules reused
verbatim from family_A.VARIANTS; entries, entry prices, exit triggers and exit
prices come from engine._load()["trades"] (each trade's `e`, `j`, `p` path).
Only the capital-constrained accounting is written here.

    python portfolio_A.py --period train      (holdout is SEALED until the train decision is locked)
    python portfolio_A.py --period holdout

Mechanics (see PORTFOLIO_PREREG.md; interpretation choices are listed in INTERP):
  * Candidates: engine fresh-ignition trades whose fire date is in the period's fire
    window and whose entry session (fire + 1) is inside the simulated window.
  * Session s order: (1) close-triggered exits from the previous close sell at the
    open of s; (2) entries at the open of s, ranked by 5-day return at the fire
    (desc, ties by ticker), skipped if ticker already held, if K positions are open,
    or if min(equity_prev_close/K, cash) < 0.5*equity_prev_close/K;
    (3) 252-session max-hold exits sell at the close of s (engine: exit at c[252]);
    (4) mark-to-market at the close (forward-filled closes).
  * Costs: `cost` per side on traded notional. A buy spends exactly `size` cash
    (shares = size / (price*(1+cost))); a sale credits shares*price*(1-cost).
  * Positions still open at the period end are marked at the final close (no exit cost).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time

import numpy as np
import pandas as pd

import engine as E
import family_A as FA

HERE = os.path.dirname(os.path.abspath(__file__))

PERIODS = {
    # sim window [start, end] (sessions), fire window [fire_lo, fire_hi] (inclusive)
    "train": dict(start="2016-01-01", end="2021-12-31", fire_lo="2016-01-01", fire_hi="2021-12-31"),
    "holdout": dict(start="2022-01-01", end=None, fire_lo="2021-12-31", fire_hi=None),
}

_FA = {n: r for n, _, r in FA.VARIANTS}
RULES = [
    # name, floor, trigger fn (from the per-trade study, unchanged)
    ("R0_current", True, E.never),
    ("R1_hold252", False, E.never),
    ("R2_A10", True, _FA["A10_after126_band0_30"]),
    ("R3_A09", True, _FA["A09_after63_band0_30"]),
    ("R4_A08", True, _FA["A08_edge_expired63_pos"]),
    ("R5_A02", True, _FA["A02_after63_pos"]),
    ("R6_A06", True, _FA["A06_after63_pos_norefire"]),
]
ELIGIBLE = ["R2_A10", "R3_A09", "R4_A08", "R5_A02", "R6_A06"]
KS = (10, 20)
COSTS_BPS = (0, 10)
MIN_FILL = 0.5

INTERP = [
    "Fire window: train = fires 2016-01-01..2021-12-31 whose entry session (fire+1) falls on/before "
    "2021-12-31 (a 2021-12-31 fire enters 2022-01-03, outside train, and belongs to holdout); "
    "holdout = fires >= 2021-12-31, sim from the first 2022 session to the last data date.",
    "Max hold: as engine.py, a trade with no trigger on closes k=0..251 exits at the CLOSE of k=252 "
    "(cash usable next morning); triggers are only acted on for k < last, so a trigger on the last "
    "path day is never sold. Trades whose 252-session window runs past the data end are simply held "
    "and marked at the final close.",
    "Exit price for a close trigger on day k=d is p['o'][d+1] (engine open, missing -> that day's close, "
    "forward-filled within the trade path).",
    "Held-ticker check is done AFTER the same morning's exits, so a ticker sold at today's open can be "
    "re-bought the same morning if it has a fresh candidate.",
    "Sizing: slot = equity at prior close / K (first session: initial capital 1.0); size = min(slot, cash "
    "after this morning's exits and earlier entries); skip if size < 50% of slot. Cost is taken out of "
    "the size (buy spends exactly `size`), so cash can never go negative.",
    "Rule row labels (R0_current, R1_hold252, R2_A10, R3_A09, R4_A08, R5_A02, R6_A06) are shared "
    "verbatim with builder B so the two result sets key identically.",
    "`skipped` = candidates skipped for lack of a slot (K positions already open). Candidates with a "
    "free slot but insufficient cash are reported separately as skipped_cash, and candidates whose "
    "ticker was already held as skipped_held.",
    "Same-day candidate ranking: 5-day return at the fire, C[t]/C[t-5]-1 (the engine's r5), highest "
    "first; exact ties broken by ticker symbol ascending.",
    "Metrics: equity series = initial 1.0 (at the close before the first session, the 'start flat' anchor) "
    "followed by each session's close; CAGR = final^(1/years)-1 with CALENDAR years = (last session date - "
    "anchor date)/365.25 (train: 2015-12-31 -> 2021-12-31 = 6.001 y; harmonised with builder B); maxDD on "
    "that daily series; Sharpe = mean/std(ddof=1) of daily returns "
    "x sqrt(252); MAR = CAGR/|maxDD|; exposure = mean over sessions of invested value/equity at the close.",
    "Open positions at the period end are marked to market at the final close with no exit cost.",
    "Benchmark: equal-weight mean of close-to-close returns over tickers that are liquid at the PRIOR "
    "close (engine liquidity filter: close > $3 and 21-day mean dollar volume > $5M), both closes present; "
    "no costs; compounded over the same sessions.",
]


# ---------------------------------------------------------------- data
def _load_panel():
    """Dates, tickers, close and volume matrices aligned exactly as engine._load()."""
    parts = [pd.read_pickle(f) for f in sorted(glob.glob(f"{E.PX}/c*.pkl"))]
    F = {}
    for fld in ("Close", "Volume"):
        df = pd.concat([p[fld] for p in parts], axis=1)
        F[fld] = df.loc[:, ~df.columns.duplicated()].sort_index()
    return F["Close"], F["Volume"]


def _exit_plan(tr, trig, floor):
    """(exit_session, kind, price, reason) for one trade under one rule, from the engine path.
    kind 'open' = close-trigger on day d sold at next open; 'close' = 252-session max-hold;
    None = the path ends at the data end before 252 sessions (held, marked to market)."""
    p = tr["p"]; c = p["c"]; last = len(c) - 1; e = tr["e"]
    trig = np.asarray(trig, dtype=bool)
    fl = (c < p["base"]) if floor else np.zeros(len(c), bool)
    full = trig | fl
    hit = np.flatnonzero(full[:last])
    if len(hit):
        d = int(hit[0])
        return e + d + 1, "open", float(p["o"][d + 1]), ("floor" if fl[d] else "rule")
    if last == E.H:
        return e + last, "close", float(c[last]), "maxhold"
    return None, None, None, "data_end"


# ---------------------------------------------------------------- simulation
def simulate(cands, plan, K, cost, sess, Cff, Ov, entry_px):
    """cands: {session -> [trade ids ranked]}; plan: {tid -> (xs, kind, xp, reason)}."""
    cash, eq_prev = 1.0, 1.0
    pos = {}                      # j -> dict
    eq, inv_s, cash_s, npos_s = [], [], [], []
    log = []
    n_taken = sk_slot = sk_cash = sk_held = 0
    reasons = {"floor": 0, "rule": 0, "maxhold": 0, "open_at_end": 0}
    for s in sess:
        # (1) exits at the open
        for j in [j for j, q in pos.items() if q["xs"] == s and q["kind"] == "open"]:
            q = pos.pop(j)
            proceeds = q["sh"] * q["xp"] * (1 - cost)
            cash += proceeds
            q["rec"].update(exit_s=int(s), exit_px=q["xp"], proceeds=proceeds)
            reasons[q["reason"]] += 1
        # (2) entries at the open
        for tid, j in cands.get(s, ()):
            if j in pos:
                sk_held += 1; continue
            if len(pos) >= K:
                sk_slot += 1; continue
            slot = eq_prev / K
            size = min(slot, cash)
            if size < MIN_FILL * slot:
                sk_cash += 1; continue
            px = entry_px[tid]
            sh = size / (px * (1 + cost))
            cash -= size
            xs, kind, xp, reason = plan[tid]
            rec = dict(tid=tid, j=int(j), entry_s=int(s), entry_px=px, cost_in=size, shares=sh,
                       exit_s=None, exit_px=None, proceeds=None, planned_xs=xs, kind=kind)
            pos[j] = dict(sh=sh, xs=xs, kind=kind, xp=xp, reason=reason, rec=rec)
            log.append(rec)
            n_taken += 1
            assert len(pos) <= K and cash >= -1e-12
        # (3) 252-session max-hold exits at the close
        for j in [j for j, q in pos.items() if q["xs"] == s and q["kind"] == "close"]:
            q = pos.pop(j)
            proceeds = q["sh"] * q["xp"] * (1 - cost)
            cash += proceeds
            q["rec"].update(exit_s=int(s), exit_px=q["xp"], proceeds=proceeds)
            reasons[q["reason"]] += 1
        # (4) mark to market
        inv = 0.0
        for j, q in pos.items():
            m = Cff[s, j]
            if not np.isfinite(m):
                m = q["rec"]["entry_px"]
            inv += q["sh"] * m
        e_ = cash + inv
        eq.append(e_); inv_s.append(inv); cash_s.append(cash); npos_s.append(len(pos))
        eq_prev = e_
    reasons["open_at_end"] = len(pos)
    last_s = sess[-1]
    for j, q in pos.items():
        m = Cff[last_s, j]
        q["rec"]["mark_px"] = float(m if np.isfinite(m) else q["rec"]["entry_px"])
    return dict(eq=np.array(eq), inv=np.array(inv_s), cash=np.array(cash_s), npos=np.array(npos_s),
                log=log, taken=n_taken, sk_slot=sk_slot, sk_cash=sk_cash, sk_held=sk_held,
                reasons=reasons, open_end=pos)


def perf(eq, years, inv=None):
    """years = calendar years from the start-flat anchor (close before the first session) to the last
    session: (last_date - anchor_date).days / 365.25."""
    full = np.r_[1.0, np.asarray(eq, float)]
    r = full[1:] / full[:-1] - 1
    n = len(r)
    cagr = full[-1] ** (1.0 / years) - 1
    mdd = float((full / np.maximum.accumulate(full) - 1).min())
    sd = r.std(ddof=1)
    out = dict(cagr=float(cagr), maxdd=mdd, sharpe=float(r.mean() / sd * np.sqrt(252)) if sd > 0 else float("nan"),
               mar=float(cagr / abs(mdd)) if mdd < 0 else float("nan"), final_equity=float(full[-1]),
               n_sessions=n)
    if inv is not None:
        out["exposure"] = float(np.mean(np.asarray(inv) / np.asarray(eq)))
    return out


# ---------------------------------------------------------------- sanity checks
def sanity(res, K, cost, sess, trades, fire_idx, Cff):
    errs = []
    eq, cash, npos = res["eq"], res["cash"], res["npos"]
    if not (eq > 0).all():
        errs.append("equity <= 0")
    if not (cash >= -1e-12).all():
        errs.append("negative cash")
    if not (npos <= K).all():
        errs.append("positions > K")
    s0, s1 = sess[0], sess[-1]
    by_tk = {}
    realized = 0.0
    for rec in res["log"]:
        tr = trades[rec["tid"]]
        if rec["entry_s"] != fire_idx[rec["tid"]] + 1 or rec["entry_s"] != tr["e"]:
            errs.append(f"entry not at fire+1: tid {rec['tid']}")
        if not (s0 <= rec["entry_s"] <= s1):
            errs.append(f"entry outside window: tid {rec['tid']}")
        if rec["exit_s"] is not None:
            if rec["exit_s"] <= rec["entry_s"]:
                errs.append(f"exit not after entry: tid {rec['tid']}")
            if rec["exit_s"] > s1:
                errs.append(f"exit after window: tid {rec['tid']}")
            realized += rec["proceeds"] - rec["cost_in"]
        else:
            realized += rec["shares"] * rec["mark_px"] - rec["cost_in"]
        end = rec["exit_s"] if rec["exit_s"] is not None else s1 + 1
        by_tk.setdefault(rec["j"], []).append((rec["entry_s"], end, rec["kind"] if rec["exit_s"] is not None else "held"))
    for j, iv in by_tk.items():
        iv.sort()
        for (a0, a1, k0), (b0, b1, _) in zip(iv, iv[1:]):
            # an open-exit at session a1 frees the ticker for a same-morning re-entry (b0 == a1)
            if b0 < a1 or (b0 == a1 and k0 != "open"):
                errs.append(f"double holding ticker idx {j}: {(a0, a1)} vs {(b0, b1)}")
    # accounting identity: final equity = 1 + sum of trade P&L (realized + marked)
    if abs((1.0 + realized) - eq[-1]) > 1e-8:
        errs.append(f"P&L does not reconcile: {1 + realized} vs {eq[-1]}")
    return errs


# ---------------------------------------------------------------- driver
def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=("train", "holdout"), required=True)
    a = ap.parse_args(argv)
    P = PERIODS[a.period]
    t0 = time.time()

    D = E._load()
    trades, Ov, Cv = D["trades"], D["Ov"], D["Cv"]
    C, V = _load_panel()
    assert C.shape == Cv.shape and np.array_equal(np.isnan(C.values), np.isnan(Cv))
    dates, tick = C.index, C.columns
    n_days = len(dates)
    Cff = C.ffill().values

    start = pd.Timestamp(P["start"])
    end = pd.Timestamp(P["end"]) if P["end"] else dates[-1]
    sess = np.flatnonzero((dates >= start) & (dates <= end))
    s0, s1 = int(sess[0]), int(sess[-1])
    years = (dates[s1] - dates[s0 - 1]).days / 365.25     # calendar years from the start-flat anchor
    fire_lo = pd.Timestamp(P["fire_lo"])
    fire_hi = pd.Timestamp(P["fire_hi"]) if P["fire_hi"] else dates[-1]

    # candidates: fire in fire window AND entry session inside the sim window
    fire_idx, r5, entry_px = {}, {}, {}
    per_sess = {}
    n_cand = 0
    for tid, tr in enumerate(trades):
        t = tr["e"] - 1
        assert dates[t] == tr["fire"]
        fire_idx[tid] = t
        if not (fire_lo <= tr["fire"] <= fire_hi and s0 <= tr["e"] <= s1):
            continue
        j = tr["j"]
        r5[tid] = Cv[t, j] / Cv[t - 5, j] - 1
        entry_px[tid] = float(tr["p"]["entry"])
        assert entry_px[tid] == Ov[tr["e"], j] and np.isfinite(entry_px[tid])
        per_sess.setdefault(tr["e"], []).append((tid, j))
        n_cand += 1
    cands = {s: sorted(v, key=lambda x: (-r5[x[0]], tick[x[1]])) for s, v in per_sess.items()}

    # exit plans per rule (independent of portfolio state)
    plans = {}
    for name, floor, fn in RULES:
        plans[name] = {tid: _exit_plan(trades[tid], fn(trades[tid]["p"]), floor) for tid in entry_px}

    rows, errs_all, detail = [], {}, {}
    for name, floor, fn in RULES:
        for K in KS:
            for cb in COSTS_BPS:
                res = simulate(cands, plans[name], K, cb / 1e4, sess, Cff, Ov, entry_px)
                m = perf(res["eq"], years, res["inv"])
                errs = sanity(res, K, cb / 1e4, sess, trades, fire_idx, Cff)
                if errs:
                    errs_all[f"{name}|K{K}|{cb}"] = errs[:10]
                closed = [r for r in res["log"] if r["exit_s"] is not None]
                hold = [r["exit_s"] - r["entry_s"] for r in closed]
                win = [r["proceeds"] > r["cost_in"] for r in closed]
                rows.append(dict(rule=name, K=K, cost_bps=cb, **m, trades=res["taken"], skipped=res["sk_slot"],
                                 skipped_cash=res["sk_cash"], skipped_held=res["sk_held"], candidates=n_cand,
                                 max_positions=int(res["npos"].max()), avg_positions=float(res["npos"].mean()),
                                 min_cash=float(res["cash"].min()),
                                 avg_hold_closed=float(np.mean(hold)) if hold else float("nan"),
                                 win_rate_closed=float(np.mean(win)) if win else float("nan"),
                                 exits=res["reasons"]))
                if K == 20 and cb == 10:
                    detail[name] = dict(dates=[d.strftime("%Y-%m-%d") for d in dates[sess]],
                                        equity=[round(float(x), 8) for x in res["eq"]])

    # benchmark: equal-weight liquid universe, close-to-close
    dv = (C * V).rolling(21).mean()
    liq = ((C > 3) & (dv > 5e6)).shift(1, fill_value=False).values
    rets = (C / C.shift(1) - 1).values
    ok = liq & np.isfinite(rets)
    bret = np.where(ok, rets, 0.0).sum(1) / np.maximum(ok.sum(1), 1)
    beq = np.cumprod(1 + bret[sess])
    bench = perf(beq, years)
    bench["avg_names"] = float(ok[sess].sum(1).mean())

    # ------------------------------------------------------------ print
    print(f"PERIOD {a.period}: {dates[s0].date()} .. {dates[s1].date()}  ({len(sess)} sessions), "
          f"fires {fire_lo.date()} .. {fire_hi.date()}, candidates {n_cand}")
    hdr = (f"{'rule':<11}{'K':>3}{'bps':>4}{'CAGR':>8}{'maxDD':>8}{'Sharpe':>7}{'MAR':>6}{'expo':>6}"
           f"{'trades':>7}{'skip':>6}{'skCash':>7}{'skHeld':>7}{'final':>7}{'hold':>6}{'win':>6}"
           f"  exits floor/rule/max/open")
    print(hdr); print("-" * len(hdr))
    for r in sorted(rows, key=lambda r: (r["K"], r["cost_bps"], r["rule"]), reverse=False):
        x = r["exits"]
        print(f"{r['rule']:<11}{r['K']:>3}{r['cost_bps']:>4}{r['cagr']*100:>7.2f}%{r['maxdd']*100:>7.1f}%"
              f"{r['sharpe']:>7.2f}{r['mar']:>6.2f}{r['exposure']:>6.2f}{r['trades']:>7}{r['skipped']:>6}"
              f"{r['skipped_cash']:>7}{r['skipped_held']:>7}{r['final_equity']:>7.2f}{r['avg_hold_closed']:>6.0f}"
              f"{r['win_rate_closed']:>6.2f}  {x['floor']}/{x['rule']}/{x['maxhold']}/{x['open_at_end']}")
    print(f"{'EW bench':<18}{bench['cagr']*100:>7.2f}%{bench['maxdd']*100:>7.1f}%{bench['sharpe']:>7.2f}"
          f"{bench['mar']:>6.2f}   (avg {bench['avg_names']:.0f} liquid names/day)")

    # decision / confirmation vs R0 (spec: K=20 & K=10 at 10 bps)
    get = {(r["rule"], r["K"], r["cost_bps"]): r for r in rows}
    decision = {}
    r0_20, r0_10 = get[("R0_current", 20, 10)], get[("R0_current", 10, 10)]
    for nm in ELIGIBLE:
        a20, a10 = get[(nm, 20, 10)], get[(nm, 10, 10)]
        p1 = a20["cagr"] > r0_20["cagr"]
        p2 = a20["maxdd"] >= r0_20["maxdd"] - 0.05
        p3 = a10["cagr"] > r0_10["cagr"]
        decision[nm] = dict(P1=bool(p1), P2=bool(p2), P3=bool(p3), passes=bool(p1 and p2 and p3),
                            cagr_k20=a20["cagr"], d_cagr_k20=a20["cagr"] - r0_20["cagr"],
                            d_maxdd_k20=a20["maxdd"] - r0_20["maxdd"], d_cagr_k10=a10["cagr"] - r0_10["cagr"])
    passers = [nm for nm in ELIGIBLE if decision[nm]["passes"]]
    winner = max(passers, key=lambda nm: decision[nm]["cagr_k20"]) if passers else None
    label = "TRAIN decision (P1 CAGR>R0 @K20, P2 maxDD within 5pp @K20, P3 CAGR>R0 @K10; 10 bps)" \
        if a.period == "train" else "HOLDOUT vs R0 (same tests; the locked rule is the one that matters)"
    print(f"\n{label}")
    for nm in ELIGIBLE:
        d = decision[nm]
        print(f"  {nm:<8} dCAGR20 {d['d_cagr_k20']*100:+6.2f}pp  dMaxDD20 {d['d_maxdd_k20']*100:+6.2f}pp  "
              f"dCAGR10 {d['d_cagr_k10']*100:+6.2f}pp  "
              f"{'P1' if d['P1'] else '--'}{'P2' if d['P2'] else '--'}{'P3' if d['P3'] else '--'}"
              f"{'  PASS' if d['passes'] else ''}")
    if a.period == "train":
        print(f"  winner: {winner or 'NONE - capital recycling does not rescue profit-taking'}")

    print("\nSANITY:", "all checks passed (equity>0, cash>=0, positions<=K, entry=fire+1 inside window, "
          "exit>entry, no same-ticker overlap, P&L reconciles)" if not errs_all else errs_all)

    out = dict(builder="A", period=a.period, window=[str(dates[s0].date()), str(dates[s1].date())],
               n_sessions=len(sess), years=years, fire_window=[str(fire_lo.date()), str(fire_hi.date())],
               candidates=n_cand, rows=rows, benchmark=bench, decision=decision,
               passers=passers, winner=winner if a.period == "train" else None,
               sanity_ok=not errs_all, sanity_errors=errs_all, interpretations=INTERP,
               equity_K20_10bps=detail, runtime_s=round(time.time() - t0, 1))
    fn = os.path.join(HERE, f"portfolio_A_{a.period}.json")
    with open(fn, "w") as f:
        json.dump(out, f, indent=1, default=float)
    print(f"\nwrote {fn}  ({time.time() - t0:.1f}s)")
    return out


if __name__ == "__main__":
    main()
