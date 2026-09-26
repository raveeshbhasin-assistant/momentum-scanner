"""
SKEPTIC harness for the portfolio pre-registration (PORTFOLIO_PREREG.md).

Imports portfolio_A (simulate / perf / _exit_plan / RULES are reused unchanged; portfolio_A's
default behaviour is not touched and its output JSONs are never written from here).

    python portfolio_skeptic.py base          # reproduce the impl-A table (must match exactly)
    python portfolio_skeptic.py all           # every perturbation -> portfolio_skeptic_results.json
    python portfolio_skeptic.py trunc_probe   # (run with IGN_PX=<truncated px dir>) look-ahead probe

Interpretation choices (the spec is silent on these perturbations):
  * Tie-break modes: r5 (spec: 5-day return desc, ties by ticker), alpha (ticker asc), r5_asc
    (adversarial: lowest 5-day return first), random seeds 1/2/3 (per session: alphabetical then a
    seeded permutation, sessions visited in time order).
  * K=15 / K=30: P1/P2 evaluated at the perturbed K (it replaces the primary K=20); P3 stays at K=10.
  * 25 bps: both the K=20 (P1,P2) and the K=10 (P3) legs at 25 bps.
  * Start shifts: train start 2016-07-01 / 2017-01-01 with fire_lo = start date (as the spec's
    train uses fire_lo = start); holdout start 2022-07-01 with fire_lo = the last session before it
    (as the spec's holdout uses fire_lo = 2021-12-31). End dates unchanged.
  * Drop-best: "best trade" = largest portfolio $ P&L (realised proceeds - cost, or marked value -
    cost) in that rule's own run at that K; the trade is removed from the candidate pool and the
    whole simulation re-run.
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import pandas as pd

import engine as E
import portfolio_A as PA

HERE = os.path.dirname(os.path.abspath(__file__))

D = E._load()
TRADES, OV, CV = D["trades"], D["Ov"], D["Cv"]
C, V = PA._load_panel()
DATES, TICK = C.index, C.columns
CFF = C.ffill().values
RULE_NAMES = [n for n, _, _ in PA.RULES]
ELIG = list(PA.ELIGIBLE)
PLANS = {name: {tid: PA._exit_plan(tr, fn(tr["p"]), floor) for tid, tr in enumerate(TRADES)}
         for name, floor, fn in PA.RULES}


def window(start, end=None, fire_lo=None, fire_hi=None):
    start = pd.Timestamp(start)
    end = pd.Timestamp(end) if end else DATES[-1]
    sess = np.flatnonzero((DATES >= start) & (DATES <= end))
    s0, s1 = int(sess[0]), int(sess[-1])
    years = (DATES[s1] - DATES[s0 - 1]).days / 365.25
    flo = pd.Timestamp(fire_lo) if fire_lo else start
    fhi = pd.Timestamp(fire_hi) if fire_hi else DATES[-1]
    cands = []
    for tid, tr in enumerate(TRADES):
        if flo <= tr["fire"] <= fhi and s0 <= tr["e"] <= s1:
            t, j = tr["e"] - 1, tr["j"]
            cands.append((tid, j, Cv_r5(t, j), TICK[j], tr["e"]))
    epx = {c[0]: float(TRADES[c[0]]["p"]["entry"]) for c in cands}
    return dict(sess=sess, s0=s0, s1=s1, years=years, cands=cands, entry_px=epx,
                label=f"{DATES[s0].date()}..{DATES[s1].date()} fires {flo.date()}..{fhi.date()}")


def Cv_r5(t, j):
    return CV[t, j] / CV[t - 5, j] - 1


WIN = {
    "train": dict(start="2016-01-01", end="2021-12-31", fire_lo="2016-01-01", fire_hi="2021-12-31"),
    "holdout": dict(start="2022-01-01", end=None, fire_lo="2021-12-31", fire_hi=None),
    "train_2016H2": dict(start="2016-07-01", end="2021-12-31", fire_lo="2016-07-01", fire_hi="2021-12-31"),
    "train_2017": dict(start="2017-01-01", end="2021-12-31", fire_lo="2017-01-01", fire_hi="2021-12-31"),
    "holdout_2022H2": dict(start="2022-07-01", end=None, fire_lo="2022-06-30", fire_hi=None),
}
_WCACHE = {}


def W(name):
    if name not in _WCACHE:
        _WCACHE[name] = window(**WIN[name])
    return _WCACHE[name]


def ranked(Wd, mode="r5", seed=None, exclude=()):
    per = {}
    for c in Wd["cands"]:
        if c[0] in exclude:
            continue
        per.setdefault(c[4], []).append(c)
    rng = np.random.default_rng(seed) if mode == "random" else None
    out = {}
    for s in sorted(per):
        v = per[s]
        if mode == "r5":
            v = sorted(v, key=lambda x: (-x[2], x[3]))
        elif mode == "r5_asc":
            v = sorted(v, key=lambda x: (x[2], x[3]))
        elif mode == "alpha":
            v = sorted(v, key=lambda x: x[3])
        elif mode == "random":
            v = sorted(v, key=lambda x: x[3])
            v = [v[i] for i in rng.permutation(len(v))]
        else:
            raise ValueError(mode)
        out[s] = [(x[0], x[1]) for x in v]
    return out


def run(wname, rule, K, bps=10, mode="r5", seed=None, exclude=(), keep=False):
    Wd = W(wname)
    cands = ranked(Wd, mode, seed, exclude)
    res = PA.simulate(cands, PLANS[rule], K, bps / 1e4, Wd["sess"], CFF, OV, Wd["entry_px"])
    m = PA.perf(res["eq"], Wd["years"], res["inv"])
    eq = np.r_[1.0, res["eq"]]
    dd = eq / np.maximum.accumulate(eq) - 1
    tr_i = int(np.argmin(dd)); pk_i = int(np.argmax(eq[:tr_i + 1]))
    sd = Wd["sess"]
    m.update(trades=res["taken"], sk_slot=res["sk_slot"], sk_cash=res["sk_cash"], sk_held=res["sk_held"],
             n_cand=len(Wd["cands"]) - len(exclude),
             frac_days_full_slots=float((res["npos"] >= K).mean()),
             frac_days_cash_lt_halfslot=float((res["cash"] < PA.MIN_FILL * res["eq"] / K).mean()),
             avg_pos=float(res["npos"].mean()),
             dd_peak=str(DATES[sd[pk_i - 1]].date()) if pk_i > 0 else str(DATES[sd[0] - 1].date()),
             dd_trough=str(DATES[sd[tr_i - 1]].date()) if tr_i > 0 else "start")
    if keep:
        m["_res"] = res
    return m


def gates(prim, rob):
    """prim/rob: {rule: metrics} at the primary K and the robustness K (same period/cost)."""
    out = {}
    r0p, r0r = prim["R0_current"], rob["R0_current"]
    for nm in ELIG:
        a, b = prim[nm], rob[nm]
        p1 = a["cagr"] > r0p["cagr"]; p2 = a["maxdd"] >= r0p["maxdd"] - 0.05; p3 = b["cagr"] > r0r["cagr"]
        out[nm] = dict(P1=bool(p1), P2=bool(p2), P3=bool(p3), passes=bool(p1 and p2 and p3),
                       dcagr=round(a["cagr"] - r0p["cagr"], 4), ddd=round(a["maxdd"] - r0p["maxdd"], 4),
                       dcagr_rob=round(b["cagr"] - r0r["cagr"], 4))
    passers = [nm for nm in ELIG if out[nm]["passes"]]
    winner = max(passers, key=lambda nm: prim[nm]["cagr"]) if passers else None
    order = sorted(RULE_NAMES, key=lambda nm: -prim[nm]["cagr"])
    return dict(gates=out, passers=passers, winner=winner, order_by_cagr=order)


def scenario(wname, Kp=20, Kr=10, bps=10, mode="r5", seed=None, exclude_by=None):
    """exclude_by: {(rule, K): set(tids)}"""
    prim, rob = {}, {}
    for nm in RULE_NAMES:
        exP = exclude_by.get((nm, Kp), ()) if exclude_by else ()
        exR = exclude_by.get((nm, Kr), ()) if exclude_by else ()
        prim[nm] = run(wname, nm, Kp, bps, mode, seed, exP)
        rob[nm] = run(wname, nm, Kr, bps, mode, seed, exR)
    g = gates(prim, rob)
    slim = lambda m: {k: (round(v, 4) if isinstance(v, float) else v) for k, v in m.items() if not k.startswith("_")}
    return dict(window=W(wname)["label"], Kp=Kp, Kr=Kr, bps=bps, mode=mode, seed=seed,
                prim={k: slim(v) for k, v in prim.items()}, rob={k: slim(v) for k, v in rob.items()}, **g)


def pnl_by_trade(res):
    out = {}
    for rec in res["log"]:
        if rec["exit_s"] is not None:
            out[rec["tid"]] = rec["proceeds"] - rec["cost_in"]
        else:
            out[rec["tid"]] = rec["shares"] * rec["mark_px"] - rec["cost_in"]
    return out


def fmt_scn(s, title):
    print(f"\n=== {title}  [{s['window']}  Kp={s['Kp']} Kr={s['Kr']} {s['bps']}bps order={s['mode']}"
          f"{'' if s['seed'] is None else ' seed=' + str(s['seed'])}]")
    for nm in RULE_NAMES:
        a, b = s["prim"][nm], s["rob"][nm]
        g = s["gates"].get(nm)
        gs = "" if g is None else (("P1" if g["P1"] else "--") + ("P2" if g["P2"] else "--") +
                                   ("P3" if g["P3"] else "--") + (" PASS" if g["passes"] else ""))
        print(f"  {nm:<11} CAGR{s['Kp']:>2} {a['cagr']*100:6.2f}% DD {a['maxdd']*100:6.1f}%  "
              f"CAGR{s['Kr']:>2} {b['cagr']*100:6.2f}% DD {b['maxdd']*100:6.1f}%  {gs}")
    print(f"  order by CAGR@K{s['Kp']}: {' > '.join(x.split('_')[0] for x in s['order_by_cagr'])}   "
          f"passers: {s['passers'] or 'NONE'}")


# ---------------------------------------------------------------- audits
def recompute_equity(res, Wd, cost):
    """Rebuild the daily equity series from the trade log alone (independent of simulate's state)."""
    sess = Wd["sess"]; idx = {s: i for i, s in enumerate(sess)}
    n = len(sess)
    cashflow = np.zeros(n)
    hold = {}                      # (j, sh) intervals
    for r in res["log"]:
        i0 = idx[r["entry_s"]]
        cashflow[i0] -= r["cost_in"]
        if r["exit_s"] is not None:
            i1 = idx[r["exit_s"]]
            cashflow[i1] += r["proceeds"]
            # open exit: sold before the close of i1; max-hold exit: sold AT the close of i1 (proceeds in cash)
            last_marked = i1 - 1
        else:
            last_marked = n - 1
        hold.setdefault(r["j"], []).append((i0, last_marked, r["shares"], r["kind"] if r["exit_s"] is not None else None))
    cash = 1.0 + np.cumsum(cashflow)
    inv = np.zeros(n)
    for j, iv in hold.items():
        for i0, i1, sh, kind in iv:
            m = CFF[sess[i0:i1 + 1], j]
            inv[i0:i1 + 1] += sh * m
    return cash + inv


def audit(wname="train", K=20, bps=10):
    out = {}
    Wd = W(wname)
    for nm in RULE_NAMES:
        m = run(wname, nm, K, bps, keep=True)
        res = m["_res"]
        eq2 = recompute_equity(res, Wd, bps / 1e4)
        diff = float(np.max(np.abs(eq2 - res["eq"])))
        # exit-timing checks
        bad = []
        n_missing_open_exit = 0
        for r in res["log"]:
            tr = TRADES[r["tid"]]
            xs, kind, xp, reason = PLANS[nm][r["tid"]]
            if r["exit_s"] is None:
                continue
            if kind == "open":
                d = xs - tr["e"] - 1
                # trigger day d must be strictly before the exit session, and exit price = open (or close if no open)
                if not (tr["e"] + d < r["exit_s"]):
                    bad.append(("exit_not_after_trigger", r["tid"]))
                o = OV[xs, tr["j"]]
                if not np.isfinite(o):
                    n_missing_open_exit += 1
                    if abs(xp - CV[xs, tr["j"]]) > 1e-9 and np.isfinite(CV[xs, tr["j"]]):
                        bad.append(("missing_open_px", r["tid"]))
                elif abs(xp - o) > 1e-9:
                    bad.append(("exit_px_not_open", r["tid"]))
            else:
                if abs(xp - CFF[xs, tr["j"]]) > 1e-9:
                    bad.append(("maxhold_px", r["tid"]))
                if xs - tr["e"] != E.H:
                    bad.append(("maxhold_len", r["tid"]))
            # entry: fire day t = e-1; ranking info r5 uses closes t and t-5 only
            if r["entry_px"] != OV[tr["e"], tr["j"]]:
                bad.append(("entry_px", r["tid"]))
        out[nm] = dict(max_abs_equity_recompute_diff=diff, bad=bad[:10], n_bad=len(bad),
                       exits_with_missing_open=n_missing_open_exit)
    return out


def trunc_probe():
    """Run under IGN_PX=<truncated px>. Prints K20/K10 10bps metrics for train (and holdout to cut)."""
    cut = DATES[-1]
    res = {}
    for nm in RULE_NAMES:
        for K in (20, 10):
            a = run("train", nm, K, 10)
            res[f"train|{nm}|{K}"] = [a["cagr"], a["maxdd"], a["final_equity"], a["trades"]]
    if cut > pd.Timestamp("2022-06-01"):
        _WCACHE["hcut"] = window(start="2022-01-01", end=str(cut.date()), fire_lo="2021-12-31", fire_hi=None)
        for nm in RULE_NAMES:
            for K in (20, 10):
                a = run("hcut", nm, K, 10)
                res[f"hcut|{nm}|{K}"] = [a["cagr"], a["maxdd"], a["final_equity"], a["trades"]]
    print("TRUNC_JSON " + json.dumps(dict(cut=str(cut.date()), res=res)))


# ---------------------------------------------------------------- driver
def base_check():
    ref = json.load(open(os.path.join(HERE, "portfolio_A_train.json")))
    refh = json.load(open(os.path.join(HERE, "portfolio_A_holdout.json")))
    worst = 0.0
    for per, R in (("train", ref), ("holdout", refh)):
        for r in R["rows"]:
            m = run(per, r["rule"], r["K"], r["cost_bps"])
            for k in ("cagr", "maxdd", "sharpe", "exposure", "final_equity"):
                worst = max(worst, abs(m[k] - r[k]))
            assert m["trades"] == r["trades"] and m["sk_slot"] == r["skipped"] and m["sk_cash"] == r["skipped_cash"], (per, r["rule"])
    print(f"base reproduction vs portfolio_A_*.json: max abs metric diff {worst:.2e}")
    return worst


def main_all():
    t0 = time.time()
    OUT = {}
    OUT["base_repro_maxdiff"] = base_check()
    periods = ("train", "holdout")

    # (a) tie-break order
    OUT["a_tiebreak"] = {}
    for per in periods:
        for mode, seed in (("r5", None), ("alpha", None), ("random", 1), ("random", 2), ("random", 3), ("r5_asc", None)):
            s = scenario(per, mode=mode, seed=seed)
            OUT["a_tiebreak"][f"{per}|{mode}|{seed}"] = s
            fmt_scn(s, f"(a) tie-break {per}")
    # extra: 30 random seeds, summarised (distribution of gate outcomes)
    OUT["a_tiebreak_30seeds"] = {}
    for per in periods:
        agg = {nm: dict(P1=0, P2=0, P3=0, passes=0, dcagr=[], ddd=[], dcagr_rob=[]) for nm in ELIG}
        r0dd, r0cagr = [], []
        for seed in range(100, 130):
            s = scenario(per, mode="random", seed=seed)
            r0dd.append(s["prim"]["R0_current"]["maxdd"]); r0cagr.append(s["prim"]["R0_current"]["cagr"])
            for nm in ELIG:
                g = s["gates"][nm]
                for k in ("P1", "P2", "P3", "passes"):
                    agg[nm][k] += int(g[k])
                for k in ("dcagr", "ddd", "dcagr_rob"):
                    agg[nm][k].append(g[k])
        summ = {nm: dict(P1=a["P1"], P2=a["P2"], P3=a["P3"], passes=a["passes"],
                         dcagr_med=float(np.median(a["dcagr"])), dcagr_min=float(np.min(a["dcagr"])),
                         dcagr_max=float(np.max(a["dcagr"])),
                         ddd_med=float(np.median(a["ddd"])), ddd_min=float(np.min(a["ddd"])), ddd_max=float(np.max(a["ddd"])),
                         dcagr10_med=float(np.median(a["dcagr_rob"])), dcagr10_min=float(np.min(a["dcagr_rob"])),
                         dcagr10_max=float(np.max(a["dcagr_rob"])))
                for nm, a in agg.items()}
        summ["R0"] = dict(maxdd_min=float(np.min(r0dd)), maxdd_max=float(np.max(r0dd)),
                          cagr_min=float(np.min(r0cagr)), cagr_max=float(np.max(r0cagr)))
        OUT["a_tiebreak_30seeds"][per] = summ
        print(f"\n=== (a+) 30 random tie-break seeds, {per} (counts of seeds passing each gate / 30)")
        print(f"  R0 K20: CAGR {summ['R0']['cagr_min']*100:.2f}..{summ['R0']['cagr_max']*100:.2f}%  "
              f"maxDD {summ['R0']['maxdd_min']*100:.1f}..{summ['R0']['maxdd_max']*100:.1f}%")
        for nm in ELIG:
            x = summ[nm]
            print(f"  {nm:<8} P1 {x['P1']:>2} P2 {x['P2']:>2} P3 {x['P3']:>2} PASS {x['passes']:>2} | "
                  f"dCAGR20 med {x['dcagr_med']*100:+.1f} [{x['dcagr_min']*100:+.1f},{x['dcagr_max']*100:+.1f}]  "
                  f"dDD20 med {x['ddd_med']*100:+.1f} [{x['ddd_min']*100:+.1f},{x['ddd_max']*100:+.1f}]  "
                  f"dCAGR10 med {x['dcagr10_med']*100:+.1f} [{x['dcagr10_min']*100:+.1f},{x['dcagr10_max']*100:+.1f}]")

    # (b) K = 15 / 30 as primary; P3 at K=10
    OUT["b_K"] = {}
    for per in periods:
        for Kp in (15, 30):
            s = scenario(per, Kp=Kp, Kr=10)
            OUT["b_K"][f"{per}|K{Kp}"] = s
            fmt_scn(s, f"(b) K={Kp} {per}")

    # (c) 25 bps
    OUT["c_cost"] = {}
    for per in periods:
        s = scenario(per, bps=25)
        OUT["c_cost"][per] = s
        fmt_scn(s, f"(c) 25 bps {per}")

    # (d) start shifts
    OUT["d_start"] = {}
    for per in ("train_2016H2", "train_2017", "holdout_2022H2"):
        s = scenario(per)
        OUT["d_start"][per] = s
        fmt_scn(s, f"(d) start shift {per}")

    # (e) drop best trade(s)
    OUT["e_dropbest"] = {}
    for per in periods:
        best1, best2, share = {}, {}, {}
        for nm in RULE_NAMES:
            for K in (20, 10):
                m = run(per, nm, K, 10, keep=True)
                pl = pnl_by_trade(m["_res"])
                srt = sorted(pl.items(), key=lambda kv: -kv[1])
                best1[(nm, K)] = {srt[0][0]}
                best2[(nm, K)] = {srt[0][0], srt[1][0]}
                tot = m["final_equity"] - 1.0
                tk = lambda tid: f"{TICK[TRADES[tid]['j']]}@{TRADES[tid]['fire'].date()}"
                share[f"{nm}|K{K}"] = dict(total_pnl=round(tot, 4),
                                           top1=[tk(srt[0][0]), round(srt[0][1], 4), round(srt[0][1] / tot, 3)],
                                           top2=[tk(srt[1][0]), round(srt[1][1], 4), round(srt[1][1] / tot, 3)])
        s1 = scenario(per, exclude_by=best1)
        s2 = scenario(per, exclude_by=best2)
        # symmetric: drop the union of every rule's K20 best trade from ALL rules
        union = set().union(*[best1[(nm, 20)] for nm in RULE_NAMES]) | set().union(*[best1[(nm, 10)] for nm in RULE_NAMES])
        s3 = scenario(per, exclude_by={(nm, K): union for nm in RULE_NAMES for K in (20, 10)})
        OUT["e_dropbest"][per] = dict(share=share, drop1=s1, drop2=s2, drop_union=s3,
                                      union=[f"{TICK[TRADES[t]['j']]}@{TRADES[t]['fire'].date()}" for t in sorted(union)])
        print(f"\n=== (e) best-trade concentration {per} (portfolio $ P&L, initial equity 1.0)")
        for k, v in share.items():
            print(f"  {k:<16} total {v['total_pnl']:+7.3f}  top1 {v['top1']}  top2 {v['top2']}")
        fmt_scn(s1, f"(e) drop own best 1 trade {per}")
        fmt_scn(s2, f"(e) drop own best 2 trades {per}")
        fmt_scn(s3, f"(e) drop union of best trades from all rules {per} ({len(union)} trades)")

    # (f) utilisation
    OUT["f_util"] = {}
    eng = E.run(E.never, floor=True, splits=("train", "holdout", "live"))
    assert len(eng) == len(TRADES)
    r0ret = eng["ret"].values
    for per in periods:
        Wd = W(per)
        util = {}
        for nm in RULE_NAMES:
            for K in (20, 10):
                m = run(per, nm, K, 10, keep=True)
                res = m["_res"]
                taken = {r["tid"] for r in res["log"]}
                allc = [c[0] for c in Wd["cands"]]
                skipped = [t for t in allc if t not in taken]
                util[f"{nm}|K{K}"] = dict(
                    candidates=len(allc), taken=m["trades"], sk_slot=m["sk_slot"], sk_cash=m["sk_cash"],
                    sk_held=m["sk_held"], capacity_skip_frac=round((m["sk_slot"] + m["sk_cash"]) / len(allc), 3),
                    exposure=round(m["exposure"], 3), avg_pos=round(m["avg_pos"], 2),
                    days_full_slots=round(m["frac_days_full_slots"], 3),
                    days_cash_lt_halfslot=round(m["frac_days_cash_lt_halfslot"], 3),
                    R0rule_ret_taken=round(float(np.mean([r0ret[t] for t in taken])), 4),
                    R0rule_ret_skipped=round(float(np.mean([r0ret[t] for t in skipped])), 4) if skipped else None)
        OUT["f_util"][per] = util
        print(f"\n=== (f) utilisation {per} (10 bps); ret columns = per-trade return under the R0 exit rule")
        for k, v in util.items():
            print(f"  {k:<16} cand {v['candidates']} taken {v['taken']:>3} skSlot {v['sk_slot']:>3} skCash {v['sk_cash']:>3} "
                  f"skHeld {v['sk_held']:>2} capSkip {v['capacity_skip_frac']:.2f} expo {v['exposure']:.2f} "
                  f"avgPos {v['avg_pos']:>5.2f} daysFull {v['days_full_slots']:.2f} daysCashBound {v['days_cash_lt_halfslot']:.2f} "
                  f"R0ret taken {v['R0rule_ret_taken']:+.3f} skipped {v['R0rule_ret_skipped']}")

    # drawdown episodes (what drives P2)
    OUT["dd_episodes"] = {}
    for per in periods + ("train_2016H2", "train_2017", "holdout_2022H2"):
        OUT["dd_episodes"][per] = {}
        for nm in RULE_NAMES:
            for K in (20, 10):
                m = run(per, nm, K, 10)
                OUT["dd_episodes"][per][f"{nm}|K{K}"] = (round(m["maxdd"], 4), m["dd_peak"], m["dd_trough"])
    print("\n=== drawdown episodes (K20 / K10, 10 bps): maxDD peak -> trough")
    for per, v in OUT["dd_episodes"].items():
        print(f"  {per}")
        for k, x in v.items():
            print(f"    {k:<16} {x[0]*100:6.1f}%  {x[1]} -> {x[2]}")

    # (g) audit
    OUT["g_audit"] = {per: audit(per) for per in periods}
    print("\n=== (g) audit: equity recomputed from the trade log; exit/entry price & timing checks")
    for per, v in OUT["g_audit"].items():
        for nm, x in v.items():
            print(f"  {per:<8} {nm:<11} max|eq diff| {x['max_abs_equity_recompute_diff']:.2e}  bad {x['n_bad']}  "
                  f"missing-open exits {x['exits_with_missing_open']}")

    fn = os.path.join(HERE, "portfolio_skeptic_results.json")
    with open(fn, "w") as f:
        json.dump(OUT, f, indent=1, default=str)
    print(f"\nwrote {fn} ({time.time() - t0:.0f}s)")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "base":
        base_check()
    elif cmd == "trunc_probe":
        trunc_probe()
    else:
        main_all()
