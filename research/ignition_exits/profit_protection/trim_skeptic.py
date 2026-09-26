"""
SKEPTIC harness for TRIM_PREREG.md (trimming winners). Tries to REFUTE the locked train decision
"T5 (trim to fund) passes on train" and stress-tests the holdout confirmation.

Imports trim_A.py (build_context / simulate / sanity are reused UNCHANGED; trim_A's default
behaviour is not touched and trim_A_*.json are never written from here).

    python trim_skeptic.py                   # everything -> trim_skeptic_results.json (+ printout)
    python trim_skeptic.py trunc_probe <base_period> <cut> <out.npz>
                                             # internal: run with IGN_PX=<truncated px dir>

Interpretation choices (the spec is silent on these perturbations; documented here and in the JSON):
  * Tie-break orders: spec (5-day return desc, exact ties by ticker), alpha (ticker asc), 10 random
    orders (seeds 1..10: per session, alphabetical then a seeded permutation; one RNG per order,
    sessions visited in time order). no_gme = the same order with GME removed.
  * K=15 / K=30: the P1/P2/P4 legs are evaluated at the perturbed K (replacing K=20); P3 stays at K=10.
  * 25 bps: every leg (K=20, K=10, no-GME) at 25 bps, T0 also at 25 bps.
  * Start shifts: train starts 2016-07-01 / 2017-01-01 with fire_lo = the start date (the spec's train
    uses fire_lo = start); holdout start 2022-07-01 with fire_lo = 2022-06-30, the last session before it
    (the spec's holdout uses fire_lo = 2021-12-31). End dates unchanged; every run starts flat.
  * Drop-best: best trade = largest portfolio $ P&L (exit proceeds or end mark + trim proceeds - cost in)
    in that variant's OWN run at that K/universe/period; its candidate is removed from the pool and the
    whole simulation re-run. T0 is treated the same way (its own best trade dropped). Also a sequential
    top-3 drop for T0 and T5, and a common drop (T0's and T5's best trades removed from every variant).
  * Look-ahead probe: the price pickles are truncated at a cut date (nothing after the cut exists), the
    whole pipeline (engine signal, trades, exit plans, trim_A.simulate) is re-run in a subprocess and the
    equity curve through the cut must be bit-identical to the full-data run.
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import time
from collections import defaultdict

import numpy as np
import pandas as pd

import engine as E
import portfolio_A as PA
import trim_A as TA

HERE = os.path.dirname(os.path.abspath(__file__))
SCR = os.path.normpath(os.path.join(HERE, ".."))
VARS = list(TA.VARIANTS)
ELIG = list(TA.ELIGIBLE)
SANITY_FAIL = []
N_SIMS = [0]

PA.PERIODS.update({
    "train_1607": dict(start="2016-07-01", end="2021-12-31", fire_lo="2016-07-01", fire_hi="2021-12-31"),
    "train_1701": dict(start="2017-01-01", end="2021-12-31", fire_lo="2017-01-01", fire_hi="2021-12-31"),
    "holdout_2207": dict(start="2022-07-01", end=None, fire_lo="2022-06-30", fire_hi=None),
})

_CTX = {}


def ctx(name):
    if name not in _CTX:
        _CTX[name] = TA.build_context(name)
    return _CTX[name]


def filt_gme(c, cands):
    out = {s: [x for x in v if x[1] != c["gme_j"]] for s, v in cands.items()}
    return {s: v for s, v in out.items() if v}


def drop(cands, tids):
    out = {s: [x for x in v if x[0] not in tids] for s, v in cands.items()}
    return {s: v for s, v in out.items() if v}


def sim(c, v, K, cb=10, cands=None, uni="all", exits="R0", check=True):
    cd = cands if cands is not None else c["cands"][uni]
    res = TA.simulate(cd, c["plans"][exits], K, cb / 1e4, c["sess"], c["Cff"], c["entry_px"], c["trades"],
                      c["dates"], v)
    N_SIMS[0] += 1
    m = PA.perf(res["eq"], c["years"], res["inv"])
    if check:
        errs = TA.sanity(res, K, cb / 1e4, v, c)
        if errs:
            SANITY_FAIL.append(dict(period=c["period"], v=v, K=K, cb=cb, errs=errs[:3]))
    sells = [e for e in res["events"] if e["side"] == "sell"]
    row = dict(variant=v if exits == "R0" else f"{v}+A10", K=K, cost_bps=cb, universe=uni,
               cagr=m["cagr"], maxdd=m["maxdd"], sharpe=m["sharpe"], exposure=m["exposure"],
               final_equity=m["final_equity"], trades=res["taken"], skipped_cash=res["sk_cash"],
               skipped_slot=res["sk_slot"], trims=len(sells))
    return row, res


def grid(c, Ks=(10, 20), cb=10, unis=("all", "no_gme"), variants=VARS, cands_by_uni=None):
    g = {}
    for uni in unis:
        cd = cands_by_uni[uni] if cands_by_uni else None
        for K in Ks:
            for v in variants:
                g[(v, K, uni)] = sim(c, v, K, cb, cands=cd, uni=uni)[0]
    return g


def decide(g, Kp=20, Kr=10):
    """TRIM_PREREG decision on a grid {(v, K, uni): row}; Kp replaces K=20 (P1, P2, P4), Kr is P3's K."""
    t20, t10, tng = g[("T0", Kp, "all")], g[("T0", Kr, "all")], g[("T0", Kp, "no_gme")]
    dec = {}
    for v in ELIG:
        a20, a10, ang = g[(v, Kp, "all")], g[(v, Kr, "all")], g[(v, Kp, "no_gme")]
        p = dict(P1=a20["cagr"] > t20["cagr"], P2=a20["maxdd"] >= t20["maxdd"] - 0.05,
                 P3=a10["cagr"] > t10["cagr"], P4=ang["cagr"] > tng["cagr"])
        dec[v] = dict(**{k: bool(x) for k, x in p.items()}, passes=bool(all(p.values())),
                      d_cagr_p=a20["cagr"] - t20["cagr"], d_maxdd_p=a20["maxdd"] - t20["maxdd"],
                      d_cagr_r=a10["cagr"] - t10["cagr"], d_cagr_ng=ang["cagr"] - tng["cagr"])
    passers = [v for v in ELIG if dec[v]["passes"]]
    winner = max(passers, key=lambda v: g[(v, Kp, "all")]["cagr"]) if passers else None
    best = max(ELIG, key=lambda v: g[(v, Kp, "all")]["cagr"])
    return dict(per_variant=dec, passers=passers, winner=winner, best_cagr=best,
                T5_passes=dec["T5"]["passes"], T5_flags="".join(k if dec["T5"][k] else "--" for k in ("P1", "P2", "P3", "P4")))


def confirm(g, v="T5", K=20):
    t, w = g[("T0", K, "all")], g[(v, K, "all")]
    return dict(variant=v, K=K, d_cagr=w["cagr"] - t["cagr"], d_maxdd=w["maxdd"] - t["maxdd"],
                confirmed=bool(w["cagr"] > t["cagr"] and w["maxdd"] >= t["maxdd"] - 0.05))


def short(r):
    return dict(cagr=round(r["cagr"], 6), maxdd=round(r["maxdd"], 6), sharpe=round(r["sharpe"], 4),
                trades=r["trades"], skipped_cash=r["skipped_cash"], trims=r["trims"])


def pnl(rec):
    val = rec["proceeds"] if rec["exit_s"] is not None else rec["shares_end"] * rec["mark_px"]
    return val + rec["proceeds_trim"] - rec["cost_in"]


def pxret(rec):
    px = rec["exit_px"] if rec["exit_s"] is not None else rec["mark_px"]
    return px / rec["entry_px"] - 1


# ================================================================ 0) base reproduction
def base_check():
    worst, n = 0.0, 0
    for per in ("train", "holdout"):
        c = ctx(per)
        ref = json.load(open(os.path.join(HERE, f"trim_A_{per}.json")))["rows"]
        for r in ref:
            if r["cost_bps"] != 10:
                continue
            m = sim(c, r["base_variant"], r["K"], 10, uni=r["universe"], exits=r["exits"])[0]
            for k in ("cagr", "maxdd", "sharpe", "exposure", "final_equity"):
                worst = max(worst, abs(m[k] - r[k]))
            assert (m["trades"], m["skipped_cash"], m["trims"]) == (r["trades"], r["skipped_cash"], r["trims"]), r["variant"]
            n += 1
    # T0 holdout == portfolio_A_holdout.json R0 rows (the portfolio study's holdout)
    pa = json.load(open(os.path.join(HERE, "portfolio_A_holdout.json")))["rows"]
    c = ctx("holdout")
    t0_vs_pa = []
    for K in (10, 20):
        ref = next(r for r in pa if r["rule"] == "R0_current" and r["K"] == K and r["cost_bps"] == 10)
        m = sim(c, "T0", K)[0]
        t0_vs_pa.append(dict(K=K, exact=bool(m["cagr"] == ref["cagr"] and m["maxdd"] == ref["maxdd"]
                                             and m["trades"] == ref["trades"])))
    return dict(rows_checked=n, max_abs_metric_diff=worst, t0_holdout_equals_portfolio_A_R0=t0_vs_pa)


# ================================================================ a) tie-break orders
def orders(c, mode, seed=None):
    base = c["cands"]["all"]
    tick = c["tick"]
    rng = np.random.default_rng(seed) if mode == "random" else None
    out = {}
    for s in sorted(base):
        v = list(base[s])
        if mode == "alpha":
            v.sort(key=lambda x: tick[x[1]])
        elif mode == "random":
            v.sort(key=lambda x: tick[x[1]])
            v = [v[i] for i in rng.permutation(len(v))]
        elif mode == "r5_asc":
            v = v[::-1]
        out[s] = v
    return {"all": out, "no_gme": filt_gme(c, out)}


def check_tiebreak():
    modes = [("spec", None), ("alpha", None), ("r5_asc", None)] + [("random", s) for s in range(1, 11)]
    out = []
    ct, ch = ctx("train"), ctx("holdout")
    for mode, seed in modes:
        gt = grid(ct, cands_by_uni=orders(ct, mode, seed))
        gh = grid(ch, cands_by_uni=orders(ch, mode, seed), unis=("all",))
        d = decide(gt)
        out.append(dict(order=mode if seed is None else f"random{seed}", train_passers=d["passers"],
                        train_winner=d["winner"], train_best=d["best_cagr"], T5_flags=d["T5_flags"],
                        T5_d_cagr20_train=d["per_variant"]["T5"]["d_cagr_p"],
                        T5_d_cagr10_train=d["per_variant"]["T5"]["d_cagr_r"],
                        T5_d_maxdd20_train=d["per_variant"]["T5"]["d_maxdd_p"],
                        T0_cagr20_train=gt[("T0", 20, "all")]["cagr"], T5_cagr20_train=gt[("T5", 20, "all")]["cagr"],
                        T0_cagr20_hold=gh[("T0", 20, "all")]["cagr"], T5_cagr20_hold=gh[("T5", 20, "all")]["cagr"],
                        T5_hold_confirm=confirm(gh)["confirmed"],
                        T5_d_cagr10_hold=gh[("T5", 10, "all")]["cagr"] - gh[("T0", 10, "all")]["cagr"],
                        best_hold_K20=max(ELIG, key=lambda v: gh[(v, 20, "all")]["cagr"]),
                        rank_T5_train_K20=1 + sorted([gt[(v, 20, "all")]["cagr"] for v in VARS], reverse=True).index(
                            gt[("T5", 20, "all")]["cagr"])))
    return out


# ================================================================ b) K = 15 / 30
def check_k():
    out = {}
    for Kp in (15, 30):
        gt = grid(ctx("train"), Ks=(10, Kp))
        gh = grid(ctx("holdout"), Ks=(Kp,), unis=("all",))
        d = decide(gt, Kp=Kp, Kr=10)
        out[f"K{Kp}"] = dict(
            train_passers=d["passers"], train_winner=d["winner"], T5_flags=d["T5_flags"],
            train={v: short(gt[(v, Kp, "all")]) for v in VARS},
            holdout={v: short(gh[(v, Kp, "all")]) for v in VARS},
            T5_hold_confirm=confirm(gh, K=Kp),
            best_train=max(ELIG, key=lambda v: gt[(v, Kp, "all")]["cagr"]),
            best_hold=max(ELIG, key=lambda v: gh[(v, Kp, "all")]["cagr"]))
    return out


def check_k_sweep(Ks=range(8, 41), seeds=range(1, 11)):
    """Extra (b): every K from 8 to 40, all variants, spec order; plus T0/T5 under the 10 random orders."""
    out = {}
    for per in ("train", "holdout"):
        c = ctx(per)
        rows = {}
        for K in Ks:
            for v in VARS:
                rows[(v, K)] = sim(c, v, K)[0]
        d5 = {K: rows[("T5", K)]["cagr"] - rows[("T0", K)]["cagr"] for K in Ks}
        dd5 = {K: rows[("T5", K)]["maxdd"] - rows[("T0", K)]["maxdd"] for K in Ks}
        beat = {v: sum(rows[(v, K)]["cagr"] > rows[("T0", K)]["cagr"] for K in Ks) for v in ELIG}
        med = {v: float(np.median([rows[(v, K)]["cagr"] - rows[("T0", K)]["cagr"] for K in Ks])) for v in ELIG}
        rnd = []
        for sd in seeds:
            cd = orders(c, "random", sd)["all"]
            for K in Ks:
                a = sim(c, "T0", K, cands=cd)[0]["cagr"]
                b = sim(c, "T5", K, cands=cd)[0]["cagr"]
                rnd.append(b - a)
        rnd = np.array(rnd)
        out[per] = dict(K_values=[int(k) for k in Ks], T5_minus_T0_cagr={int(k): x for k, x in d5.items()},
                        T5_minus_T0_maxdd={int(k): x for k, x in dd5.items()},
                        T0_cagr={int(K): rows[("T0", K)]["cagr"] for K in Ks},
                        T5_cagr={int(K): rows[("T5", K)]["cagr"] for K in Ks},
                        T5_minus_T0_sharpe={int(K): rows[("T5", K)]["sharpe"] - rows[("T0", K)]["sharpe"] for K in Ks},
                        n_K_variant_better_sharpe={v: sum(rows[(v, K)]["sharpe"] > rows[("T0", K)]["sharpe"] for K in Ks)
                                                   for v in ELIG},
                        n_K_variant_shallower_maxdd={v: sum(rows[(v, K)]["maxdd"] > rows[("T0", K)]["maxdd"] for K in Ks)
                                                     for v in ELIG},
                        n_K_variant_beats_T0=beat, median_d_cagr_vs_T0=med,
                        random_orders_x_K=dict(n=int(len(rnd)), frac_T5_beats_T0=float((rnd > 0).mean()),
                                               mean=float(rnd.mean()), median=float(np.median(rnd)),
                                               p10=float(np.percentile(rnd, 10)), p90=float(np.percentile(rnd, 90))))
    return out


# ================================================================ c) 25 bps
def check_cost():
    gt = grid(ctx("train"), cb=25)
    gh = grid(ctx("holdout"), cb=25)
    d = decide(gt)
    return dict(train_passers=d["passers"], train_winner=d["winner"], T5_flags=d["T5_flags"],
                per_variant=d["per_variant"],
                train={f"{v}|K{K}|{u}": short(gt[(v, K, u)]) for v, K, u in gt if u == "all"},
                holdout={f"{v}|K{K}|{u}": short(gh[(v, K, u)]) for v, K, u in gh if u == "all"},
                T5_hold_confirm_K20=confirm(gh), T5_hold_d_cagr10=gh[("T5", 10, "all")]["cagr"] - gh[("T0", 10, "all")]["cagr"],
                best_hold_K20=max(ELIG, key=lambda v: gh[(v, 20, "all")]["cagr"]))


# ================================================================ d) start shifts
def check_start():
    out = {}
    for per in ("train_1607", "train_1701"):
        c = ctx(per)
        g = grid(c)
        d = decide(g)
        out[per] = dict(window=[str(c["dates"][c["s0"]].date()), str(c["dates"][c["s1"]].date())],
                        candidates=c["n_cand"], passers=d["passers"], winner=d["winner"], T5_flags=d["T5_flags"],
                        per_variant={v: {k: (round(x, 5) if isinstance(x, float) else x) for k, x in dd.items()}
                                     for v, dd in d["per_variant"].items()},
                        T0={f"K{K}": short(g[("T0", K, "all")]) for K in (10, 20)},
                        T5={f"K{K}": short(g[("T5", K, "all")]) for K in (10, 20)},
                        best=d["best_cagr"])
    c = ctx("holdout_2207")
    g = grid(c)
    out["holdout_2207"] = dict(window=[str(c["dates"][c["s0"]].date()), str(c["dates"][c["s1"]].date())],
                               candidates=c["n_cand"], T5_confirm_K20=confirm(g), T5_confirm_K10=confirm(g, K=10),
                               rows={f"{v}|K{K}": short(g[(v, K, "all")]) for v in VARS for K in (10, 20)},
                               best_K20=max(ELIG, key=lambda v: g[(v, 20, "all")]["cagr"]),
                               T5_nogme_d_cagr20=g[("T5", 20, "no_gme")]["cagr"] - g[("T0", 20, "no_gme")]["cagr"])
    return out


# ================================================================ e) drop best trade
def best_trade(c, res):
    r = max(res["log"], key=pnl)
    return r["tid"], dict(ticker=str(c["tick"][r["j"]]), entry=str(c["dates"][r["entry_s"]].date()),
                          pnl=pnl(r), pxret=pxret(r), share_of_gain=pnl(r) / (res["eq"][-1] - 1.0))


def check_drop_best():
    out = {}
    for per in ("train", "holdout"):
        c = ctx(per)
        unis = ("all", "no_gme") if per == "train" else ("all",)
        g, info = {}, {}
        for uni in unis:
            for K in (10, 20):
                for v in VARS:
                    _, res = sim(c, v, K, uni=uni)
                    tid, inf = best_trade(c, res)
                    row = sim(c, v, K, cands=drop(c["cands"][uni], {tid}), uni=uni)[0]
                    g[(v, K, uni)] = row
                    inf.update(cagr_after=row["cagr"], maxdd_after=row["maxdd"])
                    info[f"{v}|K{K}|{uni}"] = inf
        o = dict(best_trades=info)
        if per == "train":
            d = decide(g)
            o.update(passers=d["passers"], winner=d["winner"], T5_flags=d["T5_flags"],
                     T5=d["per_variant"]["T5"])
        else:
            o.update(T5_confirm_K20=confirm(g), T5_d_cagr10=g[("T5", 10, "all")]["cagr"] - g[("T0", 10, "all")]["cagr"])
        # sequential top-3 drop, T0 and T5
        seq = {}
        for v in ("T0", "T5"):
            for K in (10, 20):
                dropped, path = set(), []
                cd = c["cands"]["all"]
                for n in range(4):
                    row, res = sim(c, v, K, cands=drop(cd, dropped))
                    path.append(dict(n_dropped=n, cagr=row["cagr"], maxdd=row["maxdd"]))
                    tid, inf = best_trade(c, res)
                    path[-1]["next_best"] = f"{inf['ticker']} {inf['entry']}"
                    dropped.add(tid)
                seq[f"{v}|K{K}"] = path
        o["sequential_top3"] = seq
        o["T5_minus_T0_after_k_drops"] = {f"K{K}": [seq[f"T5|K{K}"][n]["cagr"] - seq[f"T0|K{K}"][n]["cagr"] for n in range(4)]
                                          for K in (10, 20)}
        # common drop: T0's and T5's best trades (K=20 and K=10 runs) removed from every variant
        common = set()
        for v in ("T0", "T5"):
            for K in (10, 20):
                common.add(best_trade(c, sim(c, v, K)[1])[0])
        cd = drop(c["cands"]["all"], common)
        gc = {(v, K, "all"): sim(c, v, K, cands=cd)[0] for v in VARS for K in (10, 20)}
        o["common_drop"] = dict(n_removed=len(common),
                                removed=[f"{c['tick'][c['trades'][t]['j']]} {c['trades'][t]['fire'].date()}" for t in sorted(common)],
                                d_cagr20={v: gc[(v, 20, "all")]["cagr"] - gc[("T0", 20, "all")]["cagr"] for v in ELIG},
                                d_cagr10={v: gc[(v, 10, "all")]["cagr"] - gc[("T0", 10, "all")]["cagr"] for v in ELIG},
                                d_maxdd20_T5=gc[("T5", 20, "all")]["maxdd"] - gc[("T0", 20, "all")]["maxdd"])
        out[per] = o
    return out


# ================================================================ f) anatomy of trimming
def year_returns(c, eq):
    d = c["dates"][c["sess"]]
    full = np.r_[1.0, eq]
    out, prev = {}, 1.0
    for y in sorted(set(d.year)):
        last = np.flatnonzero(d.year == y)[-1]
        out[int(y)] = float(full[last + 1] / prev - 1)
        prev = full[last + 1]
    return out


def anatomy(c, v, K, cb=10):
    row, res = sim(c, v, K, cb)
    _, r0 = sim(c, "T0", K, cb)
    dates, tick = c["dates"], c["tick"]
    eqm = float(np.mean(res["eq"]))
    sells = [e for e in res["events"] if e["side"] == "sell"]
    buys = [e for e in res["events"] if e["side"] == "buy"]
    by_tid = {r["tid"]: r for r in res["log"]}
    # where / when
    by_year = defaultdict(lambda: [0, 0.0])
    for e in sells:
        y = int(dates[e["s"]].year)
        by_year[y][0] += 1
        by_year[y][1] += e["notional"] / eqm
    burst = [e for e in sells if pd.Timestamp("2020-03-01") <= dates[e["s"]] <= pd.Timestamp("2020-04-30")]
    by_tk = defaultdict(float)
    for e in sells:
        by_tk[str(tick[e["j"]])] += e["notional"]
    tot_n = sum(by_tk.values()) or 1.0
    top_tk = sorted(by_tk.items(), key=lambda x: -x[1])[:6]
    # turnover (one-sided traded notional / mean equity / year)
    entry_n = sum(r["cost_in"] - r["topup_cost"] for r in res["log"])
    exit_n = sum(r["shares_exit"] * r["exit_px"] for r in res["log"] if r["exit_s"] is not None)
    trim_n = sum(e["notional"] for e in sells)
    top_n = sum(e["notional"] for e in buys)
    yrs = c["years"]
    # donor forgone gain: trimmed shares held to the donor's actual exit (or end mark)
    forgone, don_ret = 0.0, []
    for e in sells:
        rec = by_tid[e["tid"]]
        fin = rec["exit_px"] if rec["exit_s"] is not None else rec["mark_px"]
        sold = e["sh_before"] - e["sh_after"]
        forgone += sold * (fin - e["px"])
        don_ret.append(fin / e["px"] - 1)
    # T5 funded entries
    funded = []
    if v == "T5":
        for f in res["fund_events"]:
            rec = by_tid.get(f["tid"])
            taken = rec is not None and rec["entry_s"] == f["s"]
            raised = f["cash_after"] - f["cash_before"]
            funded.append(dict(s=f["s"], taken=taken, raised=raised,
                               frac_trim=raised / rec["cost_in"] if taken else None,
                               pxret=pxret(rec) if taken else None, pnl=pnl(rec) if taken else None))
    ft = [f for f in funded if f["taken"]]
    funded_gain = sum(f["raised"] * f["pxret"] for f in ft)   # $ raised by trims x the funded entry's price return
    t5_tids = {r["tid"] for r in res["log"]}
    t0_tids = {r["tid"] for r in r0["log"]}
    t0_by = {r["tid"]: r for r in r0["log"]}
    only_v = [pxret(by_tid[t]) for t in t5_tids - t0_tids]
    only_0 = [pxret(t0_by[t]) for t in t0_tids - t5_tids]
    ft_rets = [f["pxret"] for f in ft]
    q = lambda a: dict(n=len(a), mean=float(np.mean(a)) if a else None, median=float(np.median(a)) if a else None,
                       win=float(np.mean(np.array(a) > 0)) if a else None)
    return dict(
        period=c["period"], variant=v, K=K, cagr=row["cagr"], T0_cagr=PA.perf(r0["eq"], c["years"])["cagr"],
        trims=len(sells), trim_sessions=len({e["s"] for e in sells}),
        trims_by_year={y: dict(n=n, notional_over_mean_eq=round(x, 4)) for y, (n, x) in sorted(by_year.items())},
        trims_in_mar_apr_2020=len(burst), top_trimmed_tickers=[(t, round(x / tot_n, 3)) for t, x in top_tk],
        turnover_per_year=dict(entries=entry_n / eqm / yrs, exits=exit_n / eqm / yrs, trims=trim_n / eqm / yrs,
                               topups=top_n / eqm / yrs),
        T0_turnover_per_year=dict(entries=sum(r["cost_in"] for r in r0["log"]) / float(np.mean(r0["eq"])) / yrs,
                                  exits=sum(r["shares_exit"] * r["exit_px"] for r in r0["log"] if r["exit_s"] is not None)
                                  / float(np.mean(r0["eq"])) / yrs),
        trim_cost_paid=float(sum(e["cost_paid"] for e in res["events"])),
        donor_trimmed_slices=dict(forgone_gain_to_donor_exit=forgone, **q(don_ret)),
        fund_events=len(funded), fund_events_taken=len(ft), fund_trim_then_skip=res["fund_trim_then_skip"],
        funded_entries=q(ft_rets), funded_entries_pnl=float(sum(f["pnl"] for f in ft)) if ft else 0.0,
        trim_dollars_x_funded_return=funded_gain,
        mean_frac_of_entry_funded_by_trims=float(np.mean([f["frac_trim"] for f in ft])) if ft else None,
        entries_only_in_variant=q(only_v), entries_only_in_T0=q(only_0),
        year_returns=year_returns(c, res["eq"]), T0_year_returns=year_returns(c, r0["eq"]),
        final_equity=float(res["eq"][-1]), T0_final_equity=float(r0["eq"][-1]),
        skipped_cash=res["sk_cash"], T0_skipped_cash=r0["sk_cash"], skipped_slot=res["sk_slot"],
        T0_skipped_slot=r0["sk_slot"], trades=res["taken"], T0_trades=r0["taken"])


def check_anatomy():
    out = {}
    for per in ("train", "holdout"):
        for K in (20, 10):
            for v in ("T5", "T3", "T7"):
                out[f"{per}|{v}|K{K}"] = anatomy(ctx(per), v, K)
    return out


# ================================================================ g) audit
def replay(c, res, K, cost, exits="R0"):
    """Independent equity reconstruction from the trade ledger + raw prices only (not simulator state)."""
    E_ = E._load()
    Ov, Cv = E_["Ov"], E_["Cv"]
    sess, Cff = c["sess"], c["Cff"]
    idx = {int(s): i for i, s in enumerate(sess)}
    n = len(sess)
    cashflow = np.zeros(n)
    errs = dict(entry_px=0, entry_cost=0, exit_px=0, exit_proceeds=0, trim_px=0, trim_cash=0, exit_trigger=0,
                trim_open_missing=0)
    dsh = defaultdict(list)
    for e in res["events"]:
        s, j = e["s"], e["j"]
        tr = c["trades"][e["tid"]]
        o = Ov[s, j]
        if not np.isfinite(o):
            errs["trim_open_missing"] += 1
            o = tr["p"]["c"][s - tr["e"]]
        if e["px"] != o:
            errs["trim_px"] += 1
        d = e["sh_after"] - e["sh_before"]
        cd = -d * o * (1 - cost) if d < 0 else -d * o * (1 + cost)
        if abs(cd - e["cash_delta"]) > 1e-14:
            errs["trim_cash"] += 1
        cashflow[idx[s]] += cd
        dsh[e["tid"]].append((s, d))
    for r in res["log"]:
        tr = c["trades"][r["tid"]]
        j, es = r["j"], r["entry_s"]
        if r["entry_px"] != Ov[es, j]:
            errs["entry_px"] += 1
        size = r["cost_in"] - r["topup_cost"]
        if abs(r["shares"] * Ov[es, j] * (1 + cost) - size) > 1e-12 * max(size, 1e-9):
            errs["entry_cost"] += 1
        cashflow[idx[es]] -= size
        if r["exit_s"] is not None:
            xs = r["exit_s"]
            if r["kind"] == "open":
                xp = Ov[xs, j] if np.isfinite(Ov[xs, j]) else tr["p"]["c"][xs - tr["e"]]
                # exit decided on the PRIOR close (raw ffilled closes): trigger at close xs-1 and never before
                cl = Cff[es:xs, j]
                kk = np.arange(len(cl))
                trig = cl < tr["p"]["base"]
                if exits == "R2":
                    ret = cl / r["entry_px"] - 1
                    trig = trig | ((kk >= 126) & (ret > 0) & (ret < 0.30))
                if not (trig[-1] and not trig[:-1].any()):
                    errs["exit_trigger"] += 1
            else:
                xp = tr["p"]["c"][xs - tr["e"]]
                if xs - tr["e"] != E.H:
                    errs["exit_trigger"] += 1
            if xp != r["exit_px"]:
                errs["exit_px"] += 1
            pr = r["shares_exit"] * xp * (1 - cost)
            if abs(pr - r["proceeds"]) > 1e-14:
                errs["exit_proceeds"] += 1
            cashflow[idx[xs]] += pr
    cash = 1.0 + np.cumsum(cashflow)
    inv = np.zeros(n)
    for r in res["log"]:
        j, es = r["j"], r["entry_s"]
        end = r["exit_s"] if r["exit_s"] is not None else int(sess[-1]) + 1
        ch = sorted(dsh.get(r["tid"], []))
        sh = r["shares"]
        k = 0
        for s in range(es, end):
            while k < len(ch) and ch[k][0] <= s:
                sh += ch[k][1]; k += 1
            m = Cff[s, j] if np.isfinite(Cff[s, j]) else r["entry_px"]
            inv[idx[s]] += sh * m
    eq = cash + inv
    return dict(max_rel_equity_err=float(np.max(np.abs(eq - res["eq"]) / res["eq"])),
                min_cash_replayed=float(cash.min()), errors=errs, n_events=len(res["events"]),
                n_trades=len(res["log"]))


def make_cut(cut):
    d = os.path.join(SCR, f"px_cut_{cut}")
    if not os.path.isdir(d):
        os.makedirs(d)
        for f in sorted(glob.glob(os.path.join(E.PX, "c*.pkl"))):
            pd.read_pickle(f).loc[:pd.Timestamp(cut)].to_pickle(os.path.join(d, os.path.basename(f)))
    return d


def trunc_probe(base, cut, outfn):
    P = dict(PA.PERIODS[base])
    P["end"], P["fire_hi"] = cut, cut
    PA.PERIODS["cut"] = P
    c = TA.build_context("cut")
    assert c["dates"][-1] <= pd.Timestamp(cut)
    arrs = {}
    for v in VARS:
        for K in (10, 20):
            arrs[f"{v}|{K}|R0"] = sim(c, v, K)[1]["eq"]
    for K in (10, 20):
        arrs[f"T5|{K}|R2"] = sim(c, "T5", K, exits="R2")[1]["eq"]
    np.savez(outfn, **arrs)
    print("probe ok", base, cut, len(c["sess"]), "sessions, data end", c["dates"][-1].date(),
          "sanity fails", len(SANITY_FAIL))


def check_audit():
    out = {}
    # (1) ledger replay, every variant, both periods, K 10/20, R0 (+ T5+A10)
    rep = {}
    for per in ("train", "holdout"):
        c = ctx(per)
        for K in (10, 20):
            for v in VARS:
                rep[f"{per}|{v}|K{K}"] = replay(c, sim(c, v, K)[1], K, 1e-3)
            rep[f"{per}|T5+A10|K{K}"] = replay(c, sim(c, "T5", K, exits="R2")[1], K, 1e-3, exits="R2")
    out["ledger_replay_max_rel_err"] = max(x["max_rel_equity_err"] for x in rep.values())
    out["ledger_replay_error_counts"] = {k: sum(x["errors"][k] for x in rep.values()) for k in
                                         next(iter(rep.values()))["errors"]}
    out["ledger_replay_min_cash"] = min(x["min_cash_replayed"] for x in rep.values())
    # (2) truncation look-ahead probe
    probes = []
    for base, cut in (("train", "2018-12-31"), ("train", "2020-04-30"), ("train", "2021-06-30"),
                      ("holdout", "2024-06-28")):
        d = make_cut(cut)
        fn = os.path.join(SCR, f"trunc_{cut}.npz")
        env = dict(os.environ, IGN_PX=d)
        p = subprocess.run([sys.executable, os.path.join(HERE, "trim_skeptic.py"), "trunc_probe", base, cut, fn],
                           env=env, capture_output=True, text=True, cwd=HERE)
        if p.returncode != 0:
            probes.append(dict(cut=cut, error=p.stderr[-2000:]))
            continue
        z = np.load(fn)
        c = ctx(base)
        res = {}
        for k in z.files:
            v, K, ex = k.split("|")
            full = sim(c, v, int(K), exits=ex)[1]["eq"]
            t = z[k]
            res[k] = dict(n=len(t), bit_identical=bool(np.array_equal(full[:len(t)], t)),
                          max_abs=float(np.max(np.abs(full[:len(t)] - t))))
        probes.append(dict(base=base, cut=cut, stdout=p.stdout.strip(), sessions=len(z[z.files[0]]),
                           all_bit_identical=all(x["bit_identical"] for x in res.values()),
                           max_abs=max(x["max_abs"] for x in res.values())))
    out["truncation_probe"] = probes
    return out


# ================================================================ driver
def main():
    t0 = time.time()
    R = dict(interpretations=__doc__)
    print("base reproduction ...", flush=True)
    R["base_check"] = base_check()
    print(json.dumps(R["base_check"]), flush=True)
    for name, fn in (("a_tiebreak", check_tiebreak), ("b_K15_K30", check_k), ("b2_K_sweep", check_k_sweep),
                     ("c_25bps", check_cost),
                     ("d_start_shift", check_start), ("e_drop_best", check_drop_best),
                     ("f_anatomy", check_anatomy), ("g_audit", check_audit)):
        t = time.time()
        print(f"\n=== {name} ...", flush=True)
        R[name] = fn()
        print(f"   done {time.time() - t:.1f}s", flush=True)
    R["sanity_failures"] = SANITY_FAIL
    R["n_simulations"] = N_SIMS[0]
    R["runtime_s"] = round(time.time() - t0, 1)
    fn = os.path.join(HERE, "trim_skeptic_results.json")
    with open(fn, "w") as f:
        json.dump(R, f, indent=1, default=lambda x: x.item() if hasattr(x, "item") else str(x))
    print(f"\nsims {N_SIMS[0]}, sanity failures {len(SANITY_FAIL)}, wrote {fn} ({time.time() - t0:.1f}s)")
    return R


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "trunc_probe":
        trunc_probe(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main()
