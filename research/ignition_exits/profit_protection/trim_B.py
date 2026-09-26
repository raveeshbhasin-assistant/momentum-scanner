"""
trim_B.py -- INDEPENDENT BUILDER B for TRIM_PREREG.md (trimming winners).

Imports portfolio_B.py and reuses its data loading (with the train seal: the price
panel is cut at 2021-12-31 before anything is computed), signal, fresh entries,
exit plans (R0 / R2) and metrics (perf). The simulate loop is a copy of
portfolio_B.simulate extended with trimming; portfolio_B.py is not edited.

Run:
  python trim_B.py --period train|holdout [--no-gme] [--k 20] [--cost 10] [--exits R0|R2]
      [--variants T0,T3] [--universe all|no_gme|both] [--fresh]
  Without --k both K=20 and K=10 are run. Rows are MERGED into trim_B_<period>.json
  (keyed by variant, K, cost, universe, exits); --fresh starts the file over.
  python trim_B.py --period train --grid
      runs the whole pre-registered train grid in one process (T0-T7 x K{20,10} x
      10 bps x universe {all, no_gme}, R0 exits), the T0 == portfolio_B R0 proof,
      the decision (P1-P4) and the exploratory winner+A10 (R2) rows; fresh file.

Session order (TRIM_PREREG.md): exits at the open, then trims at the open, then
entries at the open, then hard max-hold exits at the close, then mark to market.

Resolved ambiguities (documented; see also AMBIGUITIES below, written to the JSON):
  A1 weight = shares x prior close mark / equity at the prior close (e_prev). The mark
     is exactly the price used in the prior day's mark-to-market (ffilled close, or
     the entry open if a ticker has no close yet), so sum of weights + cash/e_prev = 1.
     Positions sold at today's open by an exit are gone before trimming but are still
     in e_prev (the denominator is the full prior-close equity).
  A2 "sold down to X/K": the kept share count is X/K * e_prev / mark, i.e. the fraction
     of shares sold is 1 - (X/K)/w, fixed from prior-close weights; the shares are sold
     at today's open (missing open -> that day's close -> last close, as for exits).
     After the trim the position's prior-close weight is exactly X/K (rounding only).
  A3 T5 funding is dollar-denominated like entries: need = slot - cash (slot =
     e_prev/K); shares sold = need / (open x (1 - cost)), capped so the position keeps
     at least 1/K * e_prev / mark shares (its prior-close weight never goes below 1/K).
     Sources = positions held at the prior close with current prior-close weight > 1/K,
     largest weight first (ties by ticker). Candidates are processed in the same rank
     order as entries, each funding itself in turn. Literal reading of "if still short,
     the 50% rule applies": the trims are made first; if cash is then still < 50% of a
     slot the candidate is skipped and the trim proceeds stay in cash (counted as
     t5_trim_then_skip).
  A4 T6 / T7 "first session of the quarter / month" = a simulated session whose
     calendar quarter / month differs from the previous session's (the first
     simulated session qualifies too, but the book is all cash then).
  A5 T7 sells first (every position with w > 1/K to 1/K), then tops up every position
     with w < 1/K to 1/K (target shares = e_prev/K / mark, bought at the open, cost on
     top). If cash cannot fund all top-ups, every top-up is scaled pro rata by
     cash / total need. Top-ups happen before entries (they are part of the "trims"
     step), so entries that day use what cash is left.
  A6 Positions whose 252-session hard max-hold exit is at today's CLOSE are still held
     at the open and are trimmed / topped up like any other (literal "every position").
  A7 no_gme = GME's fresh-entry candidates (fires 2020-10-08 and 2021-01-13) are removed
     from the candidate pool; nothing else changes.
  A8 "trims" counts share-reducing trades (one per position per event); top-ups (T7
     only) are counted separately as "topups".
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import portfolio_B as pb  # noqa: E402

INIT = pb.INIT
EXIT_RULES = {"R0": "R0_current", "R2": "R2_A10"}
VARIANTS = {
    "T0": "no trim (= R0)",
    "T1": "cap 2x -> 1x: daily, w > 2/K sold down to 1/K",
    "T2": "cap 3x -> 1x: daily, w > 3/K sold down to 1/K",
    "T3": "hard cap 2x: daily, w > 2/K sold down to 2/K",
    "T4": "hard cap 3x: daily, w > 3/K sold down to 3/K",
    "T5": "trim to fund: cash < slot for a candidate with a free slot -> sell from w > 1/K (largest first) down to >= 1/K, only as needed",
    "T6": "quarterly trim-only: first session of each quarter, w > 1/K sold down to 1/K",
    "T7": "monthly full equal-weight rebalance: first session of each month, all positions to 1/K (trim + top-up, subject to cash)",
}
CAPS = {"T1": (2, 1), "T2": (3, 1), "T3": (2, 2), "T4": (3, 3)}   # (threshold x, target x)
ELIGIBLE = ("T1", "T2", "T3", "T4", "T5", "T6", "T7")
TOL = 1e-9

AMBIGUITIES = [
    "A1 weight = shares x prior-close mark (the exact price used in the prior day's mark-to-market) / equity at the prior close; positions exited at today's open are removed before trimming but remain in the prior-close equity denominator.",
    "A2 'sold down to X/K': kept shares = (X/K) x prior-close equity / prior-close mark (fraction sold fixed from prior-close weights), executed at today's open (missing open -> that day's close -> last close, as for exits). Post-trim prior-close weight = X/K exactly.",
    "A3 T5: need = slot - cash (slot = prior-close equity / K); shares sold at the open = need / (open x (1 - cost)), capped so each source keeps prior-close weight >= 1/K; sources = positions held at the prior close with weight > 1/K, largest first (ties by ticker); candidates handled one by one in entry-rank order. Literal: trims first, then the 50% rule; if the candidate is still skipped the proceeds stay in cash (counted as t5_trim_then_skip).",
    "A4 first session of a month/quarter = simulated session whose calendar month/quarter differs from the previous session's.",
    "A5 T7: sells (w > 1/K to 1/K) first, then top-ups (w < 1/K to 1/K, target shares from prior-close weights, bought at the open with cost on top); if cash is short all top-ups are scaled pro rata; rebalance happens before that session's entries.",
    "A6 positions whose 252-session max-hold exit falls at today's close are still held at the open and are trimmed / topped up like any other (literal 'every position').",
    "A7 no_gme removes GME's fresh-entry candidates (fires 2020-10-08 and 2021-01-13) from the candidate pool of every variant including T0.",
    "A8 'trims' = share-reducing trades (one per position per event); T7 top-ups counted separately as 'topups'.",
]


# --------------------------------------------------------------------------- setup
def setup(period: str):
    """Everything portfolio_B.run builds before simulating, via portfolio_B's own functions."""
    cfg = pb.PERIODS[period]
    px = pb.load_prices(cfg["cutoff"])          # train: panel cut at 2021-12-31 (seal)
    O, C = px["Open"], px["Close"]
    dates = C.index
    tickers = list(C.columns)
    Ov, Cv = O.to_numpy(), C.to_numpy()
    Cff = C.ffill().to_numpy()
    sig, r5, liquid = pb.build_signal(px)
    ents = pb.fresh_entries(sig, Ov)
    start = pd.Timestamp(cfg["start"])
    end = pd.Timestamp(cfg["end"]) if cfg["end"] else dates[-1]
    ps = int(np.searchsorted(dates, start, side="left"))
    pe = int(np.searchsorted(dates, end, side="right")) - 1
    assert 0 < ps <= pe < len(dates)
    anchor = dates[ps - 1]
    years = (dates[pe] - anchor).days / 365.25
    ff = pd.Timestamp(cfg["fire_from"])
    ft = pd.Timestamp(cfg["fire_to"]) if cfg["fire_to"] else dates[-1]
    cand_list = [(t, j) for t, j in ents if ff <= dates[t] <= ft and ps <= t + 1 <= pe]
    plans = {}
    for r in EXIT_RULES.values():
        plans[r] = {(t, j): pb.exit_plan(t, j, r, sig, Ov, Cv, Cff) for t, j in cand_list}
    month_start, quarter_start = set(), set()
    for d in range(ps, pe + 1):
        a, b = dates[d - 1], dates[d]
        if (a.year, a.month) != (b.year, b.month):
            month_start.add(d)
        if (a.year, (a.month - 1) // 3) != (b.year, (b.month - 1) // 3):
            quarter_start.add(d)
    return dict(period=period, dates=dates, tickers=tickers, Ov=Ov, Cv=Cv, Cff=Cff, r5=r5,
                ps=ps, pe=pe, years=years, cand_list=cand_list, plans=plans,
                month_start=month_start, quarter_start=quarter_start)


def build_cands(ctx, universe: str):
    """Same construction as portfolio_B.run; no_gme drops GME from the candidate pool."""
    tickers, r5 = ctx["tickers"], ctx["r5"]
    cands = {}
    for t, j in ctx["cand_list"]:
        if universe == "no_gme" and tickers[j] == "GME":
            continue
        cands.setdefault(t + 1, []).append((-r5[t, j], tickers[j], t, j))
    for d in cands:
        cands[d].sort()
    return cands


# --------------------------------------------------------------------------- simulate
def simulate(variant, K, cost_bps, cands, plans, ctx, log=None, basis="prior"):
    """portfolio_B.simulate + trimming. With variant 'T0' every added branch is a no-op,
    so the arithmetic is identical to portfolio_B.simulate (proven in t0_check)."""
    Ov, Cv, Cff, tickers, dates = ctx["Ov"], ctx["Cv"], ctx["Cff"], ctx["tickers"], ctx["dates"]
    ps, pe = ctx["ps"], ctx["pe"]
    cst = cost_bps / 1e4
    cash = INIT
    e_prev = INIT
    pos = {}                     # j -> dict(sh, ei, t, xi, xk, xp, px, mark)
    eq, expo = [], []
    trades = sk_slot = sk_cash = sk_held = 0
    closed = []
    flows = {}                   # ticker -> net cash flow (sells +, buys -)
    checks = dict(min_cash=np.inf, min_equity=np.inf, max_positions=0,
                  bad_entry_timing=0, bad_exit_timing=0, double_holdings=0,
                  # trim-specific audits
                  trim_not_reducing=0, topup_outside_T7=0, topup_not_increasing=0,
                  max_trim_weight_err=0.0,         # |post-trim prior-close weight - target|
                  t5_min_post_weight_minus_1K=np.inf,
                  t5_max_funding_gap=0.0,          # |cash - slot| after a fully funded T5 trim
                  cost_missing=0,                  # trims / top-ups with no cost charged (cost>0)
                  prior_close_equity_err=0.0,      # |cash + sum(sh*mark) - e_prev| at the open
                  mark_not_prior_close=0,          # mark != price used in d-1 mark-to-market
                  rebalance_off_schedule=0,        # T6/T7 trades not on a quarter/month start
                  trade_px_fallback=0)             # trims/top-ups with no open (close used)
    trims = topups = partial_topups = t5_trim_then_skip = 0
    trim_gross = topup_gross = cost_trim = 0.0
    gme = dict(max_weight=0.0, trims=0)
    reb_days = ctx["quarter_start"] if variant == "T6" else ctx["month_start"] if variant == "T7" else set()

    def tpx(d, j):
        x = Ov[d, j]
        if np.isfinite(x):
            return float(x), False
        return float(Cv[d, j] if np.isfinite(Cv[d, j]) else Cff[d, j]), True

    def ref(d, j):
        # price that converts a target weight into shares: the prior-close mark (spec reading,
        # A2) or, for the sensitivity run only, today's open (basis='open')
        return pos[j]["mark"] if basis == "prior" else tpx(d, j)[0]

    def do_trim(d, j, new_sh, target_w=None):
        nonlocal cash, trims, trim_gross, cost_trim
        p = pos[j]
        if not new_sh < p["sh"]:
            checks["trim_not_reducing"] += 1
            return 0.0
        price, fb = tpx(d, j)
        checks["trade_px_fallback"] += fb
        sold = p["sh"] - new_sh
        gross = sold * price
        cash += gross * (1 - cst)
        c = gross * cst
        if cst > 0 and not c > 0:
            checks["cost_missing"] += 1
        cost_trim += c
        trim_gross += gross
        flows[tickers[j]] = flows.get(tickers[j], 0.0) + gross * (1 - cst)
        if log is not None:
            log.append((d, "trim", j, -sold, price, gross * (1 - cst)))
        p["sh"] = new_sh
        trims += 1
        if tickers[j] == "GME":
            gme["trims"] += 1
        if target_w is not None and basis == "prior":
            err = abs(p["sh"] * p["mark"] / e_prev - target_w)
            checks["max_trim_weight_err"] = max(checks["max_trim_weight_err"], err)
        if variant in ("T6", "T7") and d not in reb_days:
            checks["rebalance_off_schedule"] += 1
        return gross * (1 - cst)

    for d in range(ps, pe + 1):
        # 0) audit: holdings valued at their marks reproduce the prior-close equity
        if pos:
            e_chk = cash + sum(p["sh"] * p["mark"] for p in pos.values())
            checks["prior_close_equity_err"] = max(checks["prior_close_equity_err"], abs(e_chk - e_prev))
            for j, p in pos.items():
                m = Cff[d - 1, j] if np.isfinite(Cff[d - 1, j]) else p["px"]
                if p["mark"] != m:
                    checks["mark_not_prior_close"] += 1
        # 1) exits at the open (triggered by the previous close)
        for j in [j for j, p in pos.items() if p["xk"] == "open" and p["xi"] == d]:
            p = pos.pop(j)
            if not d > p["ei"]:
                checks["bad_exit_timing"] += 1
            cash += p["sh"] * p["xp"] * (1 - cst)
            flows[tickers[j]] = flows.get(tickers[j], 0.0) + p["sh"] * p["xp"] * (1 - cst)
            closed.append((tickers[j], p["t"], p["ei"], d))
            if log is not None:
                log.append((d, "exit_open", j, -p["sh"], p["xp"], p["sh"] * p["xp"] * (1 - cst)))
        checks["min_cash"] = min(checks["min_cash"], cash)
        # 1b) trims at the open, weights at the prior close
        if variant in CAPS:
            thr, tgt = CAPS[variant]
            for j in list(pos):
                p = pos[j]
                if p["sh"] * p["mark"] / e_prev > thr / K:
                    do_trim(d, j, tgt / K * e_prev / ref(d, j), tgt / K)
        elif variant in ("T6", "T7") and d in reb_days:
            for j in list(pos):
                p = pos[j]
                if p["sh"] * p["mark"] / e_prev > 1 / K:
                    do_trim(d, j, 1 / K * e_prev / ref(d, j), 1 / K)
            if variant == "T7":
                ups = []
                for j, p in pos.items():
                    tgt_sh = 1 / K * e_prev / ref(d, j)
                    if tgt_sh > p["sh"]:
                        price, fb = tpx(d, j)
                        checks["trade_px_fallback"] += fb
                        add = tgt_sh - p["sh"]
                        ups.append((j, add, price, add * price * (1 + cst)))
                need = sum(u[3] for u in ups)
                scale = 1.0 if need <= cash else max(cash, 0.0) / need
                if scale < 1.0:
                    partial_topups += len(ups)
                for j, add, price, _ in ups:
                    add *= scale
                    if not add > 0:
                        checks["topup_not_increasing"] += 1
                        continue
                    gross = add * price
                    cash -= gross * (1 + cst)
                    c = gross * cst
                    if cst > 0 and not c > 0:
                        checks["cost_missing"] += 1
                    cost_trim += c
                    topup_gross += gross
                    flows[tickers[j]] = flows.get(tickers[j], 0.0) - gross * (1 + cst)
                    if log is not None:
                        log.append((d, "topup", j, add, price, -gross * (1 + cst)))
                    pos[j]["sh"] += add
                    topups += 1
                    if scale == 1.0 and basis == "prior":
                        err = abs(pos[j]["sh"] * pos[j]["mark"] / e_prev - 1 / K)
                        checks["max_trim_weight_err"] = max(checks["max_trim_weight_err"], err)
                    if d not in reb_days:
                        checks["rebalance_off_schedule"] += 1
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
            trimmed_here = False
            if variant == "T5" and cash < slot:
                need = slot - cash
                srcs = [(p["sh"] * p["mark"] / e_prev, tickers[jj], jj) for jj, p in pos.items()
                        if p["mark"] is not None and p["sh"] * p["mark"] / e_prev > 1 / K]
                srcs.sort(key=lambda x: (-x[0], x[1]))
                for _, _, jj in srcs:
                    if need <= 1e-12:
                        break
                    p = pos[jj]
                    floor_sh = 1 / K * e_prev / ref(d, jj)
                    avail = p["sh"] - floor_sh
                    if not avail > 0:
                        continue
                    price, _ = tpx(d, jj)
                    sell = min(avail, need / (price * (1 - cst)))
                    new_sh = floor_sh if sell == avail else p["sh"] - sell
                    got = do_trim(d, jj, new_sh)
                    need -= got
                    trimmed_here = True
                    w_after = pos[jj]["sh"] * pos[jj]["mark"] / e_prev
                    if basis == "prior":
                        checks["t5_min_post_weight_minus_1K"] = min(checks["t5_min_post_weight_minus_1K"], w_after - 1 / K)
                if trimmed_here and need <= 1e-12:
                    checks["t5_max_funding_gap"] = max(checks["t5_max_funding_gap"], abs(cash - slot))
            size = min(slot, cash)
            if size < 0.5 * slot:
                sk_cash += 1
                t5_trim_then_skip += trimmed_here
                continue
            if not d >= t + 1:
                checks["bad_entry_timing"] += 1
            xi, xk, xp = plans[(t, j)] if plans[(t, j)] is not None else (10**9, "none", np.nan)
            sh = size / (Ov[d, j] * (1 + cst))     # cost paid out of the slot
            cash -= size
            flows[tickers[j]] = flows.get(tickers[j], 0.0) - size
            pos[j] = dict(sh=sh, ei=d, t=t, xi=xi, xk=xk, xp=xp, px=float(Ov[d, j]), mark=None)
            if log is not None:
                log.append((d, "entry", j, sh, float(Ov[d, j]), -size))
            trades += 1
            checks["min_cash"] = min(checks["min_cash"], cash)
        checks["max_positions"] = max(checks["max_positions"], len(pos))
        # 3) hard max-hold exits at the close
        for j in [j for j, p in pos.items() if p["xk"] == "close" and p["xi"] == d]:
            p = pos.pop(j)
            if not d > p["ei"]:
                checks["bad_exit_timing"] += 1
            cash += p["sh"] * p["xp"] * (1 - cst)
            flows[tickers[j]] = flows.get(tickers[j], 0.0) + p["sh"] * p["xp"] * (1 - cst)
            closed.append((tickers[j], p["t"], p["ei"], d))
            if log is not None:
                log.append((d, "exit_close", j, -p["sh"], p["xp"], p["sh"] * p["xp"] * (1 - cst)))
        # 4) mark to market at the close (ffilled) -- same expression as portfolio_B
        inv = float(sum(p["sh"] * (Cff[d, j] if np.isfinite(Cff[d, j]) else p["px"])
                        for j, p in pos.items()))
        eq_d = cash + inv
        eq.append(eq_d)
        expo.append(inv / eq_d if eq_d > 0 else 0.0)
        for j, p in pos.items():
            p["mark"] = float(Cff[d, j]) if np.isfinite(Cff[d, j]) else p["px"]
            if tickers[j] == "GME":
                gme["max_weight"] = max(gme["max_weight"], p["sh"] * p["mark"] / eq_d)
        checks["min_cash"] = min(checks["min_cash"], cash)
        checks["min_equity"] = min(checks["min_equity"], eq_d)
        e_prev = eq_d
    spans = {}
    for tk, t, ei, xi in closed + [(tickers[j], p["t"], p["ei"], pe + 1) for j, p in pos.items()]:
        spans.setdefault(tk, []).append((ei, xi))
    for tk, sp in spans.items():
        sp.sort()
        for (a0, a1), (b0, b1) in zip(sp, sp[1:]):
            if b0 < a1:
                checks["double_holdings"] += 1
    for j, p in pos.items():                      # open positions at the final mark
        flows[tickers[j]] = flows.get(tickers[j], 0.0) + p["sh"] * p["mark"]
    if checks["t5_min_post_weight_minus_1K"] == np.inf:
        checks["t5_min_post_weight_minus_1K"] = None
    gme["pnl"] = flows.get("GME", 0.0)
    gme["pnl_share_of_final_equity"] = gme["pnl"] / eq[-1]
    return dict(eq=np.array(eq), expo=np.array(expo), trades=trades, skipped=sk_slot,
                skipped_slot=sk_slot, skipped_cash=sk_cash, skipped_held=sk_held,
                open_at_end=len(pos), closed=len(closed), checks=checks,
                trims=trims, topups=topups, partial_topups=partial_topups,
                t5_trim_then_skip=t5_trim_then_skip, trim_gross=trim_gross,
                topup_gross=topup_gross, cost_trim=cost_trim, gme=gme)


def row_of(variant, K, cost, universe, exits, s, years):
    p = pb.perf(s["eq"], years)
    name = variant if exits == "R0" else f"{variant}+A10"
    return dict(variant=name, base_variant=variant, exits=exits, K=K, cost_bps=cost,
                universe=universe, **p, exposure=float(s["expo"].mean()), trades=s["trades"],
                skipped_slot=s["skipped_slot"], skipped_cash=s["skipped_cash"],
                skipped_held=s["skipped_held"], open_at_end=s["open_at_end"],
                trims=s["trims"], topups=s["topups"], partial_topups=s["partial_topups"],
                t5_trim_then_skip=s["t5_trim_then_skip"],
                trim_gross=s["trim_gross"], topup_gross=s["topup_gross"], cost_trim=s["cost_trim"],
                gme_pnl=s["gme"]["pnl"], gme_pnl_share=s["gme"]["pnl_share_of_final_equity"],
                gme_max_weight=s["gme"]["max_weight"], gme_trims=s["gme"]["trims"],
                checks=s["checks"])


def row_ok(r):
    c = r["checks"]
    K = r["K"]
    ok = (c["min_cash"] >= -1e-12 and c["min_equity"] > 0 and c["max_positions"] <= K
          and c["bad_entry_timing"] == 0 and c["bad_exit_timing"] == 0 and c["double_holdings"] == 0
          and c["trim_not_reducing"] == 0 and c["topup_not_increasing"] == 0
          and c["max_trim_weight_err"] <= TOL and c["cost_missing"] == 0
          and c["prior_close_equity_err"] <= 1e-9 and c["mark_not_prior_close"] == 0
          and c["rebalance_off_schedule"] == 0)
    if r["base_variant"] != "T7":
        ok = ok and r["topups"] == 0
    if r["base_variant"] == "T0":
        ok = ok and r["trims"] == 0
    if c["t5_min_post_weight_minus_1K"] is not None:
        ok = ok and c["t5_min_post_weight_minus_1K"] >= -TOL and c["t5_max_funding_gap"] <= TOL
    return bool(ok)


# --------------------------------------------------------------------------- replay audit
def replay_audit(variant, K, cost_bps, cands, plans, ctx):
    """Re-derive every trim / top-up from the transaction log, the raw price arrays and the
    simulated equity curve only (no simulator state): the weight that triggered it (prior
    close shares x ffilled prior close / prior-close equity), the post-trade weight, the
    fill price (= today's open), the cost, the schedule, missed trims, and the equity
    curve itself (cash + shares x close)."""
    log = []
    s = simulate(variant, K, cost_bps, cands, plans, ctx, log=log)
    Ov, Cff, dates, ps, pe = ctx["Ov"], ctx["Cff"], ctx["dates"], ctx["ps"], ctx["pe"]
    eq, cst = s["eq"], cost_bps / 1e4
    by_day = {}
    for ev in log:
        by_day.setdefault(ev[0], []).append(ev)
    sh, epx = {}, {}
    cash, e_prev = INIT, INIT
    r = dict(events=len(log), trims=0, topups=0, price_not_open=0, cost_mismatch=0,
             trigger_bad=0, target_bad=0, missed=0, off_schedule=0, t5_cash_not_short=0,
             phase_bad=0, equity_replay_max_err=0.0)
    mk = lambda d, j: Cff[d, j] if np.isfinite(Cff[d, j]) else epx[j]
    for d in range(ps, pe + 1):
        evs = by_day.get(d, [])
        w0 = {j: sh[j] * mk(d - 1, j) / e_prev for j in sh}     # prior-close weights
        month = (dates[d].year, dates[d].month) != (dates[d - 1].year, dates[d - 1].month)
        quarter = (dates[d].year, (dates[d].month - 1) // 3) != (dates[d - 1].year, (dates[d - 1].month - 1) // 3)
        touched, phase = set(), 0      # 0 open exits, 1 trims/top-ups, 2 entries (+T5 trims), 3 close exits
        order = {"exit_open": 0, "trim": 1, "topup": 1, "entry": 2, "exit_close": 3}
        for _, kind, j, dsh, price, dcash in evs:
            ph = order[kind]
            if variant == "T5" and kind == "trim":
                ph = 2
            if ph < phase:
                r["phase_bad"] += 1
            phase = ph
            if kind in ("trim", "topup"):
                r["trims" if kind == "trim" else "topups"] += 1
                if np.isfinite(Ov[d, j]) and price != Ov[d, j]:
                    r["price_not_open"] += 1
                if kind == "trim":
                    exp_cash = -dsh * price * (1 - cst)
                else:
                    exp_cash = -dsh * price * (1 + cst)
                if not math.isclose(dcash, exp_cash, rel_tol=1e-12, abs_tol=1e-15):
                    r["cost_mismatch"] += 1
                if cst > 0 and not abs(dcash) != abs(dsh * price):      # a cost was charged
                    r["cost_mismatch"] += 1
                if cst > 0 and kind == "trim" and not dcash < abs(dsh * price):   # proceeds net of cost
                    r["cost_mismatch"] += 1
                if cst > 0 and kind == "topup" and not -dcash > abs(dsh * price):  # cost paid on top
                    r["cost_mismatch"] += 1
                w_before = sh[j] * mk(d - 1, j) / e_prev
                w_after = (sh[j] + dsh) * mk(d - 1, j) / e_prev
                if variant in CAPS:
                    thr, tgt = CAPS[variant]
                    r["trigger_bad"] += not (kind == "trim" and w_before > thr / K)
                    r["target_bad"] += not abs(w_after - tgt / K) <= TOL
                elif variant in ("T6", "T7"):
                    r["off_schedule"] += not (quarter if variant == "T6" else month)
                    if kind == "trim":
                        r["trigger_bad"] += not w_before > 1 / K
                        r["target_bad"] += not abs(w_after - 1 / K) <= TOL
                    else:
                        r["trigger_bad"] += not w_before < 1 / K
                        r["target_bad"] += not w_after <= 1 / K + TOL
                elif variant == "T5":
                    r["trigger_bad"] += not (kind == "trim" and w_before > 1 / K)
                    r["target_bad"] += not w_after >= 1 / K - TOL
                    r["t5_cash_not_short"] += not cash < e_prev / K
                else:
                    r["trigger_bad"] += 1
                touched.add(j)
            if kind == "entry":
                epx[j] = price
                sh[j] = dsh
            elif kind in ("exit_open", "exit_close"):
                del sh[j]
            else:
                sh[j] += dsh
            cash += dcash
        # missed trims: rule-mandated trims that did not happen
        held_open = [j for j in w0 if not any(e[1] == "exit_open" and e[2] == j for e in evs)]
        for j in held_open:
            if variant in CAPS:
                thr, _ = CAPS[variant]
                r["missed"] += (w0[j] > thr / K) and j not in touched
            elif variant == "T6" and quarter:
                r["missed"] += (w0[j] > 1 / K) and j not in touched
            elif variant == "T7" and month:
                r["missed"] += (abs(w0[j] - 1 / K) > TOL) and j not in touched
        e_rep = cash + sum(sh[j] * mk(d, j) for j in sh)
        r["equity_replay_max_err"] = max(r["equity_replay_max_err"], abs(e_rep - eq[d - ps]) / eq[d - ps])
        e_prev = eq[d - ps]
    r["ok"] = bool(all(r[k] == 0 for k in ("price_not_open", "cost_mismatch", "trigger_bad", "target_bad",
                                           "missed", "off_schedule", "t5_cash_not_short", "phase_bad"))
                   and r["equity_replay_max_err"] < 1e-12 and r["trims"] == s["trims"] and r["topups"] == s["topups"])
    return r


# --------------------------------------------------------------------------- T0 proof
def t0_check(ctx, Ks=(20, 10), cost=10, exits="R0"):
    """T0 vs portfolio_B.simulate (bit-for-bit equity curve) and, on train, vs the rows
    stored in portfolio_B_train.json (exact float equality)."""
    rule = EXIT_RULES[exits]
    cands = build_cands(ctx, "all")
    stored = {}
    if ctx["period"] == "train":
        with open(os.path.join(HERE, "portfolio_B_train.json")) as f:
            for r in json.load(f)["rows"]:
                stored[(r["rule"], r["K"], r["cost_bps"])] = r
    out = {}
    for K in Ks:
        mine = simulate("T0", K, cost, cands, ctx["plans"][rule], ctx)
        ref = pb.simulate(rule, K, cost, cands, ctx["plans"][rule], ctx["ps"], ctx["pe"],
                          ctx["Ov"], ctx["Cff"], None, ctx["tickers"])
        pm, pr = pb.perf(mine["eq"], ctx["years"]), pb.perf(ref["eq"], ctx["years"])
        res = dict(eq_bit_identical=bool(np.array_equal(mine["eq"], ref["eq"])),
                   counts_equal=all(mine[k] == ref[k] for k in
                                    ("trades", "skipped_slot", "skipped_cash", "skipped_held", "open_at_end")),
                   cagr=pm["cagr"], maxdd=pm["maxdd"], sharpe=pm["sharpe"])
        if (rule, K, cost) in stored:
            st = stored[(rule, K, cost)]
            res["matches_portfolio_B_train_json"] = bool(
                pm["cagr"] == st["cagr"] and pm["maxdd"] == st["maxdd"] and pm["sharpe"] == st["sharpe"]
                and mine["trades"] == st["trades"] and mine["skipped_cash"] == st["skipped_cash"]
                and float(mine["expo"].mean()) == st["exposure"])
            res["json_cagr"], res["json_maxdd"] = st["cagr"], st["maxdd"]
        res["reproduces"] = bool(res["eq_bit_identical"] and res["counts_equal"]
                                 and res.get("matches_portfolio_B_train_json", True))
        out[f"{rule}|K={K}|{cost}bps"] = res
    return out


# --------------------------------------------------------------------------- decision
def decide(rows, cost=10):
    g = {(r["variant"], r["K"], r["cost_bps"], r["universe"]): r for r in rows if r["exits"] == "R0"}
    need = [("T0", 20, cost, "all"), ("T0", 10, cost, "all"), ("T0", 20, cost, "no_gme")]
    if any(k not in g for k in need):
        return None
    b20, b10, bng = g[need[0]], g[need[1]], g[need[2]]
    res = {}
    for v in ELIGIBLE:
        ks = [(v, 20, cost, "all"), (v, 10, cost, "all"), (v, 20, cost, "no_gme")]
        if any(k not in g for k in ks):
            continue
        a20, a10, ang = g[ks[0]], g[ks[1]], g[ks[2]]
        p1 = a20["cagr"] > b20["cagr"]
        p2 = a20["maxdd"] >= b20["maxdd"] - 0.05
        p3 = a10["cagr"] > b10["cagr"]
        p4 = ang["cagr"] > bng["cagr"]
        res[v] = dict(P1=bool(p1), P2=bool(p2), P3=bool(p3), P4=bool(p4),
                      passes=bool(p1 and p2 and p3 and p4), cagr_K20=a20["cagr"],
                      d_cagr_K20=a20["cagr"] - b20["cagr"], d_maxdd_K20=a20["maxdd"] - b20["maxdd"],
                      d_cagr_K10=a10["cagr"] - b10["cagr"], d_cagr_K20_nogme=ang["cagr"] - bng["cagr"])
    passers = [v for v in res if res[v]["passes"]]
    res["passers"] = passers
    res["locked_winner"] = max(passers, key=lambda v: res[v]["cagr_K20"]) if passers else None
    res["best_train_variant_K20"] = max((v for v in ELIGIBLE if v in res), key=lambda v: res[v]["cagr_K20"])
    return res


def train_decision_from_file():
    p = os.path.join(HERE, "trim_B_train.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f).get("decision_train")


def confirm_holdout(rows, winner, cost=10):
    """TRIM_PREREG: CONFIRMED iff holdout K=20 10 bps CAGR > T0's and maxDD no more than 5 pp deeper."""
    g = {(r["variant"], r["K"], r["cost_bps"], r["universe"]): r for r in rows if r["exits"] == "R0"}
    b, w = g.get(("T0", 20, cost, "all")), g.get((winner, 20, cost, "all"))
    if winner is None or b is None or w is None:
        return None
    c1, c2 = w["cagr"] > b["cagr"], w["maxdd"] >= b["maxdd"] - 0.05
    return dict(winner=winner, cagr_gt_T0=bool(c1), maxdd_within_5pp=bool(c2), confirmed=bool(c1 and c2),
                d_cagr_K20=w["cagr"] - b["cagr"], d_maxdd_K20=w["maxdd"] - b["maxdd"])


# --------------------------------------------------------------------------- IO
def key_of(r):
    return (r["variant"], r["K"], r["cost_bps"], r["universe"], r["exits"])


def load_out(path, fresh):
    if fresh or not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def write_out(ctx, new_rows, t0, path, fresh, extra=None):
    old = load_out(path, fresh)
    rows = {key_of(r): r for r in (old["rows"] if old else [])}
    for r in new_rows:
        rows[key_of(r)] = r
    rows = sorted(rows.values(), key=lambda r: (r["exits"], r["universe"], -r["K"], r["cost_bps"], r["variant"]))
    t0_all = dict(old.get("t0_check", {}) if old else {})
    t0_all.update(t0)
    dates, ps, pe = ctx["dates"], ctx["ps"], ctx["pe"]
    out = dict(builder="B", spec="TRIM_PREREG.md", period=ctx["period"],
               sessions=f"{dates[ps].date()} .. {dates[pe].date()}", n_sessions=pe - ps + 1,
               years=ctx["years"], data_cutoff=str(dates[-1].date()),
               candidates_in_window=len(ctx["cand_list"]),
               ambiguities=AMBIGUITIES, variants=VARIANTS, t0_check=t0_all,
               t0_reproduces_R0=bool(t0_all) and all(v["reproduces"] for v in t0_all.values()),
               sanity_all_rows_pass=all(row_ok(r) for r in rows),
               rows=rows)
    out["gme_traded_in_any_R0_row"] = any(r["gme_pnl"] != 0.0 or r["gme_max_weight"] > 0
                                          for r in rows if r["exits"] == "R0" and r["universe"] == "all")
    if ctx["period"] == "train":
        out["decision_train"] = decide(rows)
    else:
        dec = train_decision_from_file()
        if dec and dec.get("locked_winner"):
            out["holdout_confirmation"] = confirm_holdout(rows, dec["locked_winner"])
    if extra:
        out.update(extra)
    with open(path, "w") as f:
        json.dump(out, f, indent=1, default=float)
    return out


def print_rows(rows):
    hdr = (f"{'variant':9} {'univ':6} {'K':>3} {'bps':>4} {'CAGR':>8} {'maxDD':>8} {'Sharpe':>6} "
           f"{'expo':>6} {'trades':>6} {'skCash':>6} {'skSlot':>6} {'trims':>6} {'tops':>5} "
           f"{'GMEpnl':>7} {'GMEmaxW':>7} ok")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{r['variant']:9} {r['universe']:6} {r['K']:>3} {r['cost_bps']:>4g} {r['cagr']:>8.2%} "
              f"{r['maxdd']:>8.2%} {r['sharpe']:>6.2f} {r['exposure']:>6.1%} {r['trades']:>6} "
              f"{r['skipped_cash']:>6} {r['skipped_slot']:>6} {r['trims']:>6} {r['topups']:>5} "
              f"{r['gme_pnl']:>7.3f} {r['gme_max_weight']:>7.1%} {row_ok(r)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=("train", "holdout"), required=True)
    ap.add_argument("--no-gme", action="store_true", help="remove GME from the candidate pool")
    ap.add_argument("--universe", choices=("all", "no_gme", "both"), default=None)
    ap.add_argument("--k", type=int, default=None, help="default: both 20 and 10")
    ap.add_argument("--cost", type=float, default=10.0)
    ap.add_argument("--exits", choices=("R0", "R2"), default="R0")
    ap.add_argument("--variants", default=",".join(VARIANTS))
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--grid", action="store_true", help="full pre-registered grid (+ exploratory A10)")
    a = ap.parse_args()
    cost = int(a.cost) if float(a.cost).is_integer() else a.cost
    path = os.path.join(HERE, f"trim_B_{a.period}.json")
    ctx = setup(a.period)
    print(f"TRIM B  period={a.period}  sessions {ctx['dates'][ctx['ps']].date()} .. "
          f"{ctx['dates'][ctx['pe']].date()}  data cut {ctx['dates'][-1].date()}  "
          f"candidates {len(ctx['cand_list'])}")

    if a.grid:
        t0 = t0_check(ctx, (20, 10), 10, "R0")
        t0.update(t0_check(ctx, (20, 10), 10, "R2"))
        print("T0 proof:", json.dumps(t0, indent=1))
        rows = []
        for uni in ("all", "no_gme"):
            cands = build_cands(ctx, uni)
            for v in VARIANTS:
                for K in (20, 10):
                    s = simulate(v, K, 10, cands, ctx["plans"]["R0_current"], ctx)
                    rows.append(row_of(v, K, 10, uni, "R0", s, ctx["years"]))
        dec = decide(rows) if a.period == "train" else train_decision_from_file()
        combo = None
        if dec is not None:
            combo = dec["locked_winner"] or dec["best_train_variant_K20"]
            for uni in ("all", "no_gme"):
                cands = build_cands(ctx, uni)
                for v in ("T0", combo):
                    for K in (20, 10):
                        s = simulate(v, K, 10, cands, ctx["plans"]["R2_A10"], ctx)
                        rows.append(row_of(v, K, 10, uni, "R2", s, ctx["years"]))
        audit = {}
        for uni in ("all", "no_gme"):
            cands = build_cands(ctx, uni)
            for v in VARIANTS:
                for K in (20, 10):
                    audit[f"{v}|{uni}|K={K}"] = replay_audit(v, K, 10, cands, ctx["plans"]["R0_current"], ctx)
        print("replay audit all ok:", all(x["ok"] for x in audit.values()))
        # sensitivity to ambiguity A2/A3: shares from the OPEN price instead of the prior-close mark
        sens = []
        cands = build_cands(ctx, "all")
        for v in ELIGIBLE:
            for K in (20, 10):
                s = simulate(v, K, 10, cands, ctx["plans"]["R0_current"], ctx, basis="open")
                p = pb.perf(s["eq"], ctx["years"])
                sens.append(dict(variant=v, K=K, cost_bps=10, universe="all", basis="open",
                                 cagr=p["cagr"], maxdd=p["maxdd"], sharpe=p["sharpe"], trims=s["trims"]))
                print(f"  sensitivity basis=open {v} K={K}: CAGR {p['cagr']:.2%} maxDD {p['maxdd']:.2%}")
        out = write_out(ctx, rows, t0, path, fresh=True,
                        extra=dict(exploratory_combo=(f"{combo}+A10" if combo else None),
                                   replay_audit=audit, sensitivity_open_basis=sens,
                                   replay_audit_all_ok=all(x["ok"] for x in audit.values())))
        print_rows(out["rows"])
        print("\nt0_reproduces_R0:", out["t0_reproduces_R0"], " sanity_all_rows_pass:", out["sanity_all_rows_pass"])
        if out.get("decision_train"):
            print("decision (train):", json.dumps(out["decision_train"], indent=1))
        print("wrote", path)
        return

    uni = a.universe or ("no_gme" if a.no_gme else "all")
    unis = ("all", "no_gme") if uni == "both" else (uni,)
    Ks = (a.k,) if a.k else (20, 10)
    variants = [v.strip() for v in a.variants.split(",") if v.strip()]
    t0 = t0_check(ctx, Ks, cost, a.exits) if "T0" in variants else {}
    if t0:
        print("T0 proof:", json.dumps(t0, indent=1))
    rows = []
    for u in unis:
        cands = build_cands(ctx, u)
        for v in variants:
            for K in Ks:
                s = simulate(v, K, cost, cands, ctx["plans"][EXIT_RULES[a.exits]], ctx)
                rows.append(row_of(v, K, cost, u, a.exits, s, ctx["years"]))
    print_rows(rows)
    out = write_out(ctx, rows, t0, path, fresh=a.fresh)
    print("\nt0_reproduces_R0:", out["t0_reproduces_R0"], " sanity_all_rows_pass:", out["sanity_all_rows_pass"])
    if out.get("decision_train"):
        print("decision (train):", json.dumps(out["decision_train"], indent=1))
    print("wrote", path)


if __name__ == "__main__":
    main()
