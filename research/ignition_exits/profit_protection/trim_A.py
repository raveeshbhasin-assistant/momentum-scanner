"""
Trimming winners in the Ignition portfolio -- INDEPENDENT BUILDER A.

Spec: TRIM_PREREG.md (binding), which extends PORTFOLIO_PREREG.md. Built on
portfolio_A.py (itself built on engine.py): data loading, candidates, ranking,
exit plans (R0 / R2) and metrics are portfolio_A's; only the simulate loop is
copied here and extended with the trimming variants T0-T7. portfolio_A.py is
imported, never edited.

    python trim_A.py --period train                       # T0-T7, K 10+20, 10 bps, universe all, R0 exits
    python trim_A.py --period train --no-gme              # same with GME removed from the candidate pool
    python trim_A.py --period train --k 20 --cost 10 --exits R2 --variants T3
    python trim_A.py --period train --grid                # the full pre-registered TRAIN grid + T0 proof + decision
    (--period holdout is SEALED until the train decision is locked)

Every invocation merges its rows into trim_A_<period>.json, keyed by
(variant, K, cost_bps, universe, exits).

Session order (TRIM_PREREG.md): exits at the open -> trims at the open -> entries at
the open (T5 trims inline, only to fund an entry) -> 252-session max-hold exits at
the close -> mark to market at the close.
"""
from __future__ import annotations

import argparse
import json
import os
import time

import numpy as np
import pandas as pd

import engine as E
import portfolio_A as PA

HERE = os.path.dirname(os.path.abspath(__file__))
MIN_FILL = PA.MIN_FILL
TOL = 1e-12
DUST = 1e-12     # relative: weights within 1e-12 of a threshold/target count as AT it (float dust)

# name -> definition (TRIM_PREREG.md, verbatim semantics)
VARIANTS = {
    "T0": dict(kind="none", desc="no trim (= R0 of the portfolio study)"),
    "T1": dict(kind="cap", trig=2, target=1, desc="cap 2x -> 1x: weight > 2/K sold down to 1/K"),
    "T2": dict(kind="cap", trig=3, target=1, desc="cap 3x -> 1x: weight > 3/K sold down to 1/K"),
    "T3": dict(kind="cap", trig=2, target=2, desc="hard cap 2x: weight > 2/K sold down to 2/K"),
    "T4": dict(kind="cap", trig=3, target=3, desc="hard cap 3x: weight > 3/K sold down to 3/K"),
    "T5": dict(kind="fund", desc="trim to fund: free slot but cash < slot -> sell largest weights (>1/K) "
                                 "down to no less than 1/K, only as much as needed"),
    "T6": dict(kind="quarterly", desc="first session of each calendar quarter: weight > 1/K sold down to 1/K, no top-ups"),
    "T7": dict(kind="monthly_ew", desc="first session of each month: every position set to 1/K (trims and top-ups, "
                                       "subject to cash)"),
}
ELIGIBLE = ["T1", "T2", "T3", "T4", "T5", "T6", "T7"]
EXITS = {"R0": "R0_current", "R2": "R2_A10"}
GME = "GME"

INTERP = [
    "Weights: w = shares x (the position's mark at the prior close) / (equity at the prior close). The mark and "
    "the equity are exactly the ones portfolio_A's mark-to-market produced at the prior close (ffilled close; "
    "entry price if no close yet), so the weights of all positions plus cash/equity sum to 1.",
    "Trim size: the ORDER is fixed before the open from prior-close weights as a fraction of shares, "
    "f = 1 - target/w, i.e. the kept shares = target x eq_prev / mark_prev. It is filled at the OPEN. So the "
    "post-trim weight on the prior-close basis equals the target exactly (within float rounding); on the open "
    "basis it differs by the overnight move (no look-ahead).",
    "Trade price for a trim/top-up at session s = the engine trade path's open p['o'][s-e] (open; missing open -> "
    "that day's close, ffilled within the path) - the same convention as close-triggered exits.",
    "Costs: 10 bps per side on every trim (proceeds = shares x price x (1-cost)) and every top-up "
    "(cash spent = shares x price x (1+cost)); entries/exits exactly as portfolio_A.",
    "T1-T4 are checked EVERY session (daily at the prior close). A position entered at an open has no prior-close "
    "weight and is never trimmed that session. Remaining shares keep the original exit plan (floor / 252-session "
    "max hold); trimming never closes a position.",
    "T5: funding happens inside the entry loop, candidate by candidate in rank order, only when the candidate "
    "passes the held and slot checks and cash < slot (slot = eq_prev/K). Donors = positions held at the prior "
    "close with current weight > 1/K, re-sorted by current weight (desc; exact ties, never observed, by panel column index) for each candidate; each "
    "donor is sold down to no less than 1/K; selling stops as soon as proceeds (net of cost) lift cash to the "
    "slot. If donors are exhausted and cash is still < 50% of the slot, the candidate is skipped (the 50% rule) "
    "and the trims already made STAND (literal reading; counted as fund_trim_then_skip).",
    "T6: first session of each calendar quarter in the sim window (first session whose (year, quarter) differs "
    "from the previous session's). T7: first session of each month, same definition.",
    "T7: sells (weight > 1/K down to 1/K) are executed first, then top-ups (weight < 1/K up to 1/K). If the "
    "top-ups' total cash need exceeds cash, every top-up is scaled pro rata by cash/need (subject to cash; "
    "a 1e-12 relative haircut keeps float rounding from leaving cash at -1e-17). "
    "Positions exactly at 1/K are not traded. Top-ups add to the position's cost basis; they never add a position.",
    "no_gme universe: every GME candidate (train fires 2020-10-08 and 2021-01-13) is removed from the candidate "
    "pool before the simulation; all other candidates, ranks and exit plans are unchanged.",
    "'trims' = number of sell legs (position-level trim trades); 'trim_sessions' = sessions with >= 1 trim; "
    "'topups' (T7 only) = number of top-up buy legs. 'trades' = new entries taken (as portfolio_A). "
    "'skipped_cash' = candidates with a free slot skipped by the 50% rule (after any T5 funding).",
    "Float dust: a weight within a relative 1e-12 of a trigger/target counts as AT it (no trade). Without this, a "
    "position trimmed to exactly 1/K in T7 read back as 0.0999...9 and drew a zero-share 'top-up'; the guard changes "
    "no economically meaningful trade.",
    "Everything else (candidates, same-day ranking, sizing, 50% rule, exits, metrics, calendar-year CAGR, "
    "end-of-period mark without exit cost) is portfolio_A.py unchanged; T0 is verified bit-identical to "
    "portfolio_A.simulate and to the R0 rows in portfolio_A_train.json.",
]


# ---------------------------------------------------------------- context (portfolio_A's setup, unchanged)
def build_context(period):
    P = PA.PERIODS[period]
    D = E._load()
    trades, Ov, Cv = D["trades"], D["Ov"], D["Cv"]
    C, _V = PA._load_panel()
    assert C.shape == Cv.shape and np.array_equal(np.isnan(C.values), np.isnan(Cv))
    dates, tick = C.index, C.columns
    Cff = C.ffill().values

    start = pd.Timestamp(P["start"])
    end = pd.Timestamp(P["end"]) if P["end"] else dates[-1]
    sess = np.flatnonzero((dates >= start) & (dates <= end))
    s0, s1 = int(sess[0]), int(sess[-1])
    years = (dates[s1] - dates[s0 - 1]).days / 365.25
    fire_lo = pd.Timestamp(P["fire_lo"])
    fire_hi = pd.Timestamp(P["fire_hi"]) if P["fire_hi"] else dates[-1]

    # candidates: verbatim portfolio_A.main
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
    gme_j = int(np.flatnonzero(tick == GME)[0]) if GME in set(tick) else None
    cands_ng = {s: [x for x in v if x[1] != gme_j] for s, v in cands.items()}
    cands_ng = {s: v for s, v in cands_ng.items() if v}
    gme_cands = [(dates[fire_idx[tid]].strftime("%Y-%m-%d"), tid) for v in cands.values() for tid, j in v if j == gme_j]

    rules = {n: (floor, fn) for n, floor, fn in PA.RULES}
    plans = {}
    for key, rname in EXITS.items():
        floor, fn = rules[rname]
        plans[key] = {tid: PA._exit_plan(trades[tid], fn(trades[tid]["p"]), floor) for tid in entry_px}

    return dict(period=period, trades=trades, Ov=Ov, Cff=Cff, dates=dates, tick=tick, sess=sess, s0=s0, s1=s1,
                years=years, fire_lo=fire_lo, fire_hi=fire_hi, fire_idx=fire_idx, entry_px=entry_px,
                cands={"all": cands, "no_gme": cands_ng}, n_cand={"all": n_cand, "no_gme": sum(map(len, cands_ng.values()))},
                gme_j=gme_j, gme_cands=gme_cands, plans=plans)


# ---------------------------------------------------------------- simulation (portfolio_A.simulate + trims)
def simulate(cands, plan, K, cost, sess, Cff, entry_px, trades, dates, variant):
    spec = VARIANTS[variant]
    kind = spec["kind"]
    cash, eq_prev = 1.0, 1.0
    pos = {}                      # j -> dict  (insertion order = portfolio_A's)
    eq, inv_s, cash_s, npos_s, maxw_s = [], [], [], [], []
    log, ev, fund_ev, post_viol = [], [], [], []
    n_taken = sk_slot = sk_cash = sk_held = 0
    fund_trim_then_skip = 0
    reasons = {"floor": 0, "rule": 0, "maxhold": 0, "open_at_end": 0}
    ym = [(d.year, d.month) for d in dates[sess]]

    def trade(q, sh_new, s, s_prev, why, target_w):
        """Resize position q to sh_new shares at the open of s. Returns the cash delta."""
        sh0 = q["sh"]
        if sh_new == sh0:
            return 0.0
        px = float(trades[q["tid"]]["p"]["o"][s - q["e"]])
        d = sh_new - sh0
        rec = q["rec"]
        if d < 0:
            notional = -d * px
            delta = notional * (1 - cost)
            rec["proceeds_trim"] += delta
            rec["n_trims"] += 1
            side = "sell"
        else:
            notional = d * px
            delta = -notional * (1 + cost)
            rec["cost_in"] += -delta
            rec["topup_cost"] += -delta
            rec["n_topups"] += 1
            side = "buy"
        q["sh"] = sh_new
        ev.append(dict(s=int(s), s_prev=int(s_prev), tid=q["tid"], j=q["j"], why=why, side=side,
                       sh_before=sh0, sh_after=sh_new, px=px, mark_prev=q["mark"], eq_prev=eq_prev,
                       w_before=sh0 * q["mark"] / eq_prev, w_after=sh_new * q["mark"] / eq_prev,
                       target_w=target_w, notional=notional, cost_paid=notional * cost, cash_delta=delta))
        return delta

    s_prev = None
    for i, s in enumerate(sess):
        new_month = i == 0 or ym[i] != ym[i - 1]
        new_q = i == 0 or (ym[i][0], (ym[i][1] - 1) // 3) != (ym[i - 1][0], (ym[i - 1][1] - 1) // 3)
        # (1) exits at the open  (verbatim portfolio_A)
        for j in [j for j, q in pos.items() if q["xs"] == s and q["kind"] == "open"]:
            q = pos.pop(j)
            proceeds = q["sh"] * q["xp"] * (1 - cost)
            cash += proceeds
            q["rec"].update(exit_s=int(s), exit_px=q["xp"], proceeds=proceeds, shares_exit=q["sh"])
            reasons[q["reason"]] += 1
        # (2) trims at the open, weights at the PRIOR close
        if pos:
            assert all(q["mark"] is not None for q in pos.values())
        if kind == "cap":
            trig, tgt = spec["trig"] / K, spec["target"] / K
            for j, q in list(pos.items()):
                w = q["sh"] * q["mark"] / eq_prev
                if w > trig * (1 + DUST):
                    cash += trade(q, tgt * eq_prev / q["mark"], s, s_prev, variant, tgt)
            post_viol += [(int(s), j) for j, q in pos.items() if q["sh"] * q["mark"] / eq_prev > trig + TOL]
        elif kind == "quarterly" and new_q and i > 0:
            tgt = 1.0 / K
            for j, q in list(pos.items()):
                w = q["sh"] * q["mark"] / eq_prev
                if w > tgt * (1 + DUST):
                    cash += trade(q, tgt * eq_prev / q["mark"], s, s_prev, variant, tgt)
            post_viol += [(int(s), j) for j, q in pos.items() if q["sh"] * q["mark"] / eq_prev > tgt + TOL]
        elif kind == "monthly_ew" and new_month and i > 0:
            tgt = 1.0 / K
            for j, q in list(pos.items()):           # sells first
                w = q["sh"] * q["mark"] / eq_prev
                if w > tgt * (1 + DUST):
                    cash += trade(q, tgt * eq_prev / q["mark"], s, s_prev, variant, tgt)
            buys = []
            for j, q in pos.items():                 # then top-ups, subject to cash (pro rata)
                w = q["sh"] * q["mark"] / eq_prev
                if w < tgt * (1 - DUST):
                    dsh = tgt * eq_prev / q["mark"] - q["sh"]
                    px = float(trades[q["tid"]]["p"]["o"][s - q["e"]])
                    buys.append((q, dsh, dsh * px * (1 + cost)))
            need = sum(b[2] for b in buys)
            # subject to cash: pro rata; a 1e-12 haircut keeps float rounding from pushing cash below 0
            scale = 1.0 if need <= cash * (1 - 1e-12) else max(cash, 0.0) * (1 - 1e-12) / need
            for q, dsh, _ in buys:
                if scale > 0:
                    cash += trade(q, q["sh"] + scale * dsh, s, s_prev, variant, tgt)
            post_viol += [(int(s), j) for j, q in pos.items() if q["sh"] * q["mark"] / eq_prev > tgt + TOL
                          or (scale == 1.0 and abs(q["sh"] * q["mark"] / eq_prev - tgt) > TOL)]
        # (3) entries at the open  (portfolio_A + T5 trim-to-fund)
        for tid, j in cands.get(s, ()):
            if j in pos:
                sk_held += 1; continue
            if len(pos) >= K:
                sk_slot += 1; continue
            slot = eq_prev / K
            trimmed_for_this = False
            if kind == "fund" and cash < slot:
                cash_before = cash
                n_ev0 = len(ev)
                while cash < slot:
                    donors = [(q["sh"] * q["mark"] / eq_prev, j2) for j2, q in pos.items()
                              if q["mark"] is not None and q["sh"] * q["mark"] / eq_prev > (1.0 / K) * (1 + DUST)]
                    if not donors:
                        break
                    donors.sort(key=lambda x: (-x[0], x[1]))
                    # largest weight first; each down to no less than 1/K; only as much as needed
                    _, jd = donors[0]
                    q = pos[jd]
                    px = float(trades[q["tid"]]["p"]["o"][s - q["e"]])
                    sh_floor = (1.0 / K) * eq_prev / q["mark"]
                    sellable = q["sh"] - sh_floor
                    need = slot - cash
                    sh_sell = min(sellable, need / (px * (1 - cost)))
                    if not sh_sell > 0:
                        break
                    sh_new = sh_floor if sh_sell >= sellable else q["sh"] - sh_sell
                    if sh_new == q["sh"]:
                        break
                    cash += trade(q, sh_new, s, s_prev, "T5", 1.0 / K)
                    # guard against an endless loop on float dust
                    if sh_new != sh_floor and slot - cash <= slot * 1e-12:
                        break
                trimmed_for_this = len(ev) > n_ev0
                if trimmed_for_this:
                    left = [(q["sh"] * q["mark"] / eq_prev) for q in pos.values()
                            if q["mark"] is not None and q["sh"] * q["mark"] / eq_prev > 1.0 / K + TOL]
                    fund_ev.append(dict(s=int(s), tid=tid, slot=slot, cash_before=cash_before, cash_after=cash,
                                        n_legs=len(ev) - n_ev0, donors_left=len(left)))
            size = min(slot, cash)
            if size < MIN_FILL * slot:
                sk_cash += 1
                fund_trim_then_skip += int(trimmed_for_this)
                continue
            px = entry_px[tid]
            sh = size / (px * (1 + cost))
            cash -= size
            xs, xkind, xp, reason = plan[tid]
            rec = dict(tid=tid, j=int(j), entry_s=int(s), entry_px=px, cost_in=size, shares=sh,
                       exit_s=None, exit_px=None, proceeds=None, planned_xs=xs, kind=xkind,
                       proceeds_trim=0.0, n_trims=0, n_topups=0, topup_cost=0.0, shares_exit=None)
            pos[j] = dict(sh=sh, xs=xs, kind=xkind, xp=xp, reason=reason, rec=rec, tid=tid, j=int(j),
                          e=int(s), mark=None)
            log.append(rec)
            n_taken += 1
            assert len(pos) <= K and cash >= -1e-12
        # (4) 252-session max-hold exits at the close  (verbatim portfolio_A)
        for j in [j for j, q in pos.items() if q["xs"] == s and q["kind"] == "close"]:
            q = pos.pop(j)
            proceeds = q["sh"] * q["xp"] * (1 - cost)
            cash += proceeds
            q["rec"].update(exit_s=int(s), exit_px=q["xp"], proceeds=proceeds, shares_exit=q["sh"])
            reasons[q["reason"]] += 1
        # (5) mark to market  (verbatim portfolio_A; the mark is kept for tomorrow's weights)
        inv = 0.0
        for j, q in pos.items():
            m = Cff[s, j]
            if not np.isfinite(m):
                m = q["rec"]["entry_px"]
            q["mark"] = float(m)
            inv += q["sh"] * m
        e_ = cash + inv
        eq.append(e_); inv_s.append(inv); cash_s.append(cash); npos_s.append(len(pos))
        maxw_s.append(max((q["sh"] * q["mark"] / e_ for q in pos.values()), default=0.0))
        eq_prev = e_
        s_prev = s
    reasons["open_at_end"] = len(pos)
    last_s = sess[-1]
    for j, q in pos.items():
        m = Cff[last_s, j]
        q["rec"]["mark_px"] = float(m if np.isfinite(m) else q["rec"]["entry_px"])
        q["rec"]["shares_end"] = q["sh"]
    return dict(eq=np.array(eq), inv=np.array(inv_s), cash=np.array(cash_s), npos=np.array(npos_s),
                maxw=np.array(maxw_s), log=log, events=ev, fund_events=fund_ev, post_viol=post_viol,
                taken=n_taken, sk_slot=sk_slot, sk_cash=sk_cash, sk_held=sk_held,
                fund_trim_then_skip=fund_trim_then_skip, reasons=reasons, open_end=pos)


# ---------------------------------------------------------------- sanity
def sanity(res, K, cost, variant, ctx):
    errs = []
    sess, trades, Cff, Ov, fire_idx = ctx["sess"], ctx["trades"], ctx["Cff"], ctx["Ov"], ctx["fire_idx"]
    dates = ctx["dates"]
    eq, cash, npos = res["eq"], res["cash"], res["npos"]
    kind = VARIANTS[variant]["kind"]
    if not (eq > 0).all():
        errs.append("equity <= 0")
    if not (cash >= -1e-12).all():
        errs.append(f"negative cash (min {cash.min()})")
    if not (npos <= K).all():
        errs.append("positions > K")
    s0, s1 = sess[0], sess[-1]
    idx = {int(s): i for i, s in enumerate(sess)}
    ym = [(d.year, d.month) for d in dates[sess]]
    # --- trade ledger (portfolio_A checks, extended for trims / top-ups)
    realized = 0.0
    by_tk = {}
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
            realized += rec["proceeds"] + rec["proceeds_trim"] - rec["cost_in"]
        else:
            realized += rec["shares_end"] * rec["mark_px"] + rec["proceeds_trim"] - rec["cost_in"]
        if rec["n_topups"] and kind != "monthly_ew":
            errs.append(f"top-up outside T7: tid {rec['tid']}")
        end = rec["exit_s"] if rec["exit_s"] is not None else s1 + 1
        by_tk.setdefault(rec["j"], []).append((rec["entry_s"], end, rec["kind"] if rec["exit_s"] is not None else "held"))
    for j, iv in by_tk.items():
        iv.sort()
        for (a0, a1, k0), (b0, b1, _) in zip(iv, iv[1:]):
            if b0 < a1 or (b0 == a1 and k0 != "open"):
                errs.append(f"double holding ticker idx {j}: {(a0, a1)} vs {(b0, b1)}")
    if abs((1.0 + realized) - eq[-1]) > 1e-8:
        errs.append(f"P&L does not reconcile: {1 + realized} vs {eq[-1]}")
    # --- trim / top-up events
    if kind == "none" and res["events"]:
        errs.append("T0 traded a trim")
    for e in res["events"]:
        s, j, tid = e["s"], e["j"], e["tid"]
        i = idx[s]
        if i == 0:
            errs.append(f"trim on the first session: {e}"); continue
        sp = int(sess[i - 1])
        tr = trades[tid]
        # weights measured at the PRIOR close
        m_exp = Cff[sp, j] if np.isfinite(Cff[sp, j]) else tr["p"]["entry"]
        if e["s_prev"] != sp or e["mark_prev"] != float(m_exp):
            errs.append(f"weight not at prior close: s {s} j {j}")
        if e["eq_prev"] != eq[i - 1]:
            errs.append(f"eq_prev is not the prior close equity: s {s}")
        if tr["e"] >= s:
            errs.append(f"trimmed a position entered this session: s {s} j {j}")
        # trades at the OPEN
        px_exp = Ov[s, j] if np.isfinite(Ov[s, j]) else tr["p"]["c"][s - tr["e"]]
        if e["px"] != float(px_exp):
            errs.append(f"trim not at the open: s {s} j {j} {e['px']} vs {px_exp}")
        # costs on every trim / top-up
        if abs(e["cost_paid"] - e["notional"] * cost) > 1e-15 or (cost > 0 and not e["cost_paid"] > 0):
            errs.append(f"cost not charged: s {s} j {j}")
        want = e["notional"] * (1 - cost) if e["side"] == "sell" else -e["notional"] * (1 + cost)
        if abs(e["cash_delta"] - want) > 1e-15:
            errs.append(f"cash delta wrong: s {s} j {j}")
        # direction: trims only reduce shares (never to zero); top-ups only in T7
        if e["side"] == "sell":
            if not (0 < e["sh_after"] < e["sh_before"]):
                errs.append(f"trim did not reduce shares: s {s} j {j}")
        else:
            if kind != "monthly_ew" or not e["sh_after"] > e["sh_before"]:
                errs.append(f"illegal top-up: s {s} j {j}")
        # rule-specific: trigger, target, schedule
        tgt = e["target_w"]
        if kind == "cap":
            if not e["w_before"] > VARIANTS[variant]["trig"] / K:
                errs.append(f"trim below trigger: s {s} j {j}")
            if abs(e["w_after"] - tgt) > 1e-12 or tgt != VARIANTS[variant]["target"] / K:
                errs.append(f"trim not at target: s {s} j {j} {e['w_after']} vs {tgt}")
        elif kind == "quarterly":
            if (ym[i][0], (ym[i][1] - 1) // 3) == (ym[i - 1][0], (ym[i - 1][1] - 1) // 3):
                errs.append(f"T6 trim off-schedule: s {s}")
            if not e["w_before"] > 1.0 / K or abs(e["w_after"] - 1.0 / K) > 1e-12:
                errs.append(f"T6 trim trigger/target wrong: s {s} j {j}")
        elif kind == "monthly_ew":
            if ym[i] == ym[i - 1]:
                errs.append(f"T7 trade off-schedule: s {s}")
            if e["side"] == "sell" and (not e["w_before"] > 1.0 / K or abs(e["w_after"] - 1.0 / K) > 1e-12):
                errs.append(f"T7 trim trigger/target wrong: s {s} j {j}")
            if e["side"] == "buy" and (not e["w_before"] < 1.0 / K or e["w_after"] > 1.0 / K + 1e-12):
                errs.append(f"T7 top-up trigger/target wrong: s {s} j {j}")
        elif kind == "fund":
            if not e["w_before"] > 1.0 / K or e["w_after"] < 1.0 / K - 1e-12:
                errs.append(f"T5 trim below 1/K: s {s} j {j}")
    if res["post_viol"]:
        errs.append(f"weights above cap after the trim step: {res['post_viol'][:5]}")
    for f in res["fund_events"]:
        # only as much as needed: cash lifted to the slot, or every donor is at 1/K
        if not (abs(f["cash_after"] - f["slot"]) <= 1e-12 * max(1.0, f["slot"]) or f["donors_left"] == 0):
            errs.append(f"T5 over/under-funded: {f}")
        if f["cash_after"] > f["slot"] + 1e-12:
            errs.append(f"T5 sold more than needed: {f}")
    return errs


# ---------------------------------------------------------------- one row
def run_one(ctx, variant, K, cost_bps, universe, exits):
    cost = cost_bps / 1e4
    cands = ctx["cands"][universe]
    res = simulate(cands, ctx["plans"][exits], K, cost, ctx["sess"], ctx["Cff"], ctx["entry_px"],
                   ctx["trades"], ctx["dates"], variant)
    m = PA.perf(res["eq"], ctx["years"], res["inv"])
    errs = sanity(res, K, cost, variant, ctx)
    ev = res["events"]
    sells = [e for e in ev if e["side"] == "sell"]
    buys = [e for e in ev if e["side"] == "buy"]
    closed = [r for r in res["log"] if r["exit_s"] is not None]
    gme = [r for r in res["log"] if r["j"] == ctx["gme_j"]]
    gme_info = []
    for r in gme:
        val_end = r["proceeds"] if r["exit_s"] is not None else r["shares_end"] * r["mark_px"]
        gme_info.append(dict(entry=str(ctx["dates"][r["entry_s"]].date()), cost_in=r["cost_in"],
                             pnl=val_end + r["proceeds_trim"] - r["cost_in"], trim_proceeds=r["proceeds_trim"],
                             n_trims=r["n_trims"]))
    label = variant if exits == "R0" else f"{variant}+A10"
    row = dict(variant=label, base_variant=variant, exits=exits, K=K, cost_bps=cost_bps, universe=universe,
               **m, trades=res["taken"], skipped=res["sk_slot"], skipped_cash=res["sk_cash"],
               skipped_held=res["sk_held"], candidates=ctx["n_cand"][universe],
               trims=len(sells), trim_sessions=len({e["s"] for e in sells}), topups=len(buys),
               fund_trim_then_skip=res["fund_trim_then_skip"],
               trim_notional=float(sum(e["notional"] for e in sells)),
               topup_notional=float(sum(e["notional"] for e in buys)),
               trim_topup_costs=float(sum(e["cost_paid"] for e in ev)),
               max_positions=int(res["npos"].max()), avg_positions=float(res["npos"].mean()),
               min_cash=float(res["cash"].min()), max_weight_close=float(res["maxw"].max()),
               avg_hold_closed=float(np.mean([r["exit_s"] - r["entry_s"] for r in closed])) if closed else float("nan"),
               exits_by=res["reasons"], gme=gme_info, sanity_ok=not errs, sanity_errors=errs[:10])
    return row, res


def key(r):
    return (r["variant"], r["K"], r["cost_bps"], r["universe"], r["exits"])


# ---------------------------------------------------------------- T0 == portfolio_A R0 proof
def prove_t0(ctx):
    """T0 vs portfolio_A: (a) in-process, bit-identical equity/cash/positions vs PA.simulate for K 10/20 x
    0/10 bps, R0 and R2 exits; (b) the R0 rows stored in portfolio_A_<period>.json (K 20 & 10, 10 bps)."""
    out = dict(inprocess=[], stored=[])
    ok = True
    for exits in ("R0", "R2"):
        for K in (10, 20):
            for cb in (0, 10):
                ra = PA.simulate(ctx["cands"]["all"], ctx["plans"][exits], K, cb / 1e4, ctx["sess"], ctx["Cff"],
                                 ctx["Ov"], ctx["entry_px"])
                rt = simulate(ctx["cands"]["all"], ctx["plans"][exits], K, cb / 1e4, ctx["sess"], ctx["Cff"],
                              ctx["entry_px"], ctx["trades"], ctx["dates"], "T0")
                same = (np.array_equal(ra["eq"], rt["eq"]) and np.array_equal(ra["cash"], rt["cash"])
                        and np.array_equal(ra["inv"], rt["inv"]) and np.array_equal(ra["npos"], rt["npos"])
                        and (ra["taken"], ra["sk_slot"], ra["sk_cash"], ra["sk_held"], ra["reasons"])
                        == (rt["taken"], rt["sk_slot"], rt["sk_cash"], rt["sk_held"], rt["reasons"]))
                out["inprocess"].append(dict(exits=exits, K=K, cost_bps=cb, bit_identical=bool(same)))
                ok &= same
    fn = os.path.join(HERE, f"portfolio_A_{ctx['period']}.json")
    stored_ok = None
    if ctx["period"] == "train" and os.path.exists(fn):
        with open(fn) as f:
            pa = json.load(f)
        stored_ok = True
        fields = ["cagr", "maxdd", "sharpe", "mar", "final_equity", "exposure", "n_sessions", "trades", "skipped",
                  "skipped_cash", "skipped_held", "max_positions", "avg_positions", "min_cash", "exits"]
        for exits, rname in EXITS.items():
            for K in (20, 10):
                ref = next(r for r in pa["rows"] if r["rule"] == rname and r["K"] == K and r["cost_bps"] == 10)
                row, res = run_one(ctx, "T0", K, 10, "all", exits)
                mine = dict(row, exits=row["exits_by"])
                diffs = {f: (mine[f], ref[f]) for f in fields if mine[f] != ref[f]}
                if K == 20:
                    curve = pa["equity_K20_10bps"][rname]["equity"]
                    if [round(float(x), 8) for x in res["eq"]] != curve:
                        diffs["equity_curve_K20"] = "differs"
                out["stored"].append(dict(ref=rname, K=K, cost_bps=10, exact=not diffs, diffs=diffs,
                                          cagr=row["cagr"], maxdd=row["maxdd"]))
                stored_ok &= not diffs
    out["all_bit_identical"] = bool(ok)
    out["stored_exact"] = stored_ok
    return out


# ---------------------------------------------------------------- decision (TRIM_PREREG.md)
def decide(rows):
    g = {(r["variant"], r["K"], r["universe"]): r for r in rows if r["cost_bps"] == 10 and r["exits"] == "R0"}
    t20, t10, t20n = g[("T0", 20, "all")], g[("T0", 10, "all")], g[("T0", 20, "no_gme")]
    dec = {}
    for v in ELIGIBLE:
        a20, a10, a20n = g[(v, 20, "all")], g[(v, 10, "all")], g[(v, 20, "no_gme")]
        p1 = a20["cagr"] > t20["cagr"]
        p2 = a20["maxdd"] >= t20["maxdd"] - 0.05
        p3 = a10["cagr"] > t10["cagr"]
        p4 = a20n["cagr"] > t20n["cagr"]
        dec[v] = dict(P1=bool(p1), P2=bool(p2), P3=bool(p3), P4=bool(p4), passes=bool(p1 and p2 and p3 and p4),
                      cagr_k20=a20["cagr"], d_cagr_k20=a20["cagr"] - t20["cagr"], d_maxdd_k20=a20["maxdd"] - t20["maxdd"],
                      d_cagr_k10=a10["cagr"] - t10["cagr"], d_cagr_k20_nogme=a20n["cagr"] - t20n["cagr"])
    passers = [v for v in ELIGIBLE if dec[v]["passes"]]
    winner = max(passers, key=lambda v: dec[v]["cagr_k20"]) if passers else None
    best = max(ELIGIBLE, key=lambda v: dec[v]["cagr_k20"])
    return dec, passers, winner, best


# ---------------------------------------------------------------- output
def fmt(r):
    return (f"{r['variant']:<8}{r['universe']:<7}{r['K']:>3}{r['cost_bps']:>4}{r['cagr']*100:>8.2f}%{r['maxdd']*100:>8.2f}%"
            f"{r['sharpe']:>7.2f}{r['mar']:>6.2f}{r['exposure']:>6.2f}{r['trades']:>7}{r['skipped']:>5}"
            f"{r['skipped_cash']:>7}{r['trims']:>6}{r['topups']:>6}{r['max_weight_close']*100:>7.1f}%"
            f"{r['final_equity']:>7.2f}  {'ok' if r['sanity_ok'] else 'SANITY FAIL'}")


HDR = (f"{'variant':<8}{'univ':<7}{'K':>3}{'bps':>4}{'CAGR':>9}{'maxDD':>9}{'Sharpe':>7}{'MAR':>6}{'expo':>6}"
       f"{'trades':>7}{'skip':>5}{'skCash':>7}{'trims':>6}{'topup':>6}{'maxW':>8}{'final':>7}")


def merge_write(period, rows, extra):
    fn = os.path.join(HERE, f"trim_A_{period}.json")
    old = {}
    if os.path.exists(fn):
        try:
            with open(fn) as f:
                old = json.load(f)
        except Exception:
            old = {}
    merged = {key(r): r for r in old.get("rows", [])}
    for r in rows:
        merged[key(r)] = r
    out = dict(old)
    out.update(extra)
    out["rows"] = sorted(merged.values(), key=lambda r: (r["exits"], r["universe"], r["K"], r["cost_bps"], r["variant"]))
    with open(fn, "w") as f:
        json.dump(out, f, indent=1, default=float)
    return fn


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=("train", "holdout"), required=True)
    ap.add_argument("--no-gme", action="store_true", help="remove GME from the candidate pool")
    ap.add_argument("--k", type=int, default=None, help="slots (default: both 10 and 20)")
    ap.add_argument("--cost", type=float, default=10, help="cost in bps per side (default 10)")
    ap.add_argument("--exits", choices=("R0", "R2"), default="R0")
    ap.add_argument("--variants", default=",".join(VARIANTS), help="comma list, default T0..T7")
    ap.add_argument("--grid", action="store_true",
                    help="full pre-registered grid: T0-T7 x K{10,20} x {all,no_gme} at 10 bps (R0), T0 proof, decision, "
                         "exploratory winner+A10 (train)")
    a = ap.parse_args(argv)
    t0 = time.time()
    ctx = build_context(a.period)
    dates, s0, s1 = ctx["dates"], ctx["s0"], ctx["s1"]
    print(f"PERIOD {a.period}: {dates[s0].date()} .. {dates[s1].date()} ({len(ctx['sess'])} sessions, "
          f"{ctx['years']:.3f} y), candidates all {ctx['n_cand']['all']} / no_gme {ctx['n_cand']['no_gme']}; "
          f"GME candidates {ctx['gme_cands']}")
    meta = dict(builder="A", period=a.period, window=[str(dates[s0].date()), str(dates[s1].date())],
                n_sessions=len(ctx["sess"]), years=ctx["years"],
                fire_window=[str(ctx["fire_lo"].date()), str(ctx["fire_hi"].date())],
                candidates=ctx["n_cand"], gme_candidates=ctx["gme_cands"],
                variants={k: v["desc"] for k, v in VARIANTS.items()}, interpretations=INTERP)
    rows = []
    extra = dict(meta)
    if a.grid:
        proof = prove_t0(ctx)
        print("\nT0 PROOF vs portfolio_A:")
        for p in proof["inprocess"]:
            print(f"  in-process {p['exits']} K{p['K']:>2} {p['cost_bps']:>2}bps bit-identical: {p['bit_identical']}")
        for p in proof["stored"]:
            print(f"  stored {p['ref']:<10} K{p['K']:>2} 10bps exact: {p['exact']}  CAGR {p['cagr']*100:.4f}%  "
                  f"maxDD {p['maxdd']*100:.4f}%  {p['diffs'] if p['diffs'] else ''}")
        extra["t0_proof"] = proof
        for uni in ("all", "no_gme"):
            for K in (10, 20):
                for v in VARIANTS:
                    rows.append(run_one(ctx, v, K, 10, uni, "R0")[0])
        combo = None
        if a.period == "train":
            dec, passers, winner, best = decide(rows)
            combo = winner or best
            extra.update(decision=dec, passers=passers, winner=winner, best_if_none=best,
                         decision_rule="P1 K20 CAGR>T0, P2 K20 maxDD within 5pp of T0, P3 K10 CAGR>T0, "
                                       "P4 no-GME K20 CAGR>T0(no-GME); TRAIN 10 bps; winner = max K20 CAGR among passers")
        else:
            fn_tr = os.path.join(HERE, "trim_A_train.json")
            with open(fn_tr) as f:
                tr = json.load(f)
            combo = tr.get("winner") or tr.get("best_if_none")
        if combo:
            extra["exploratory_combo"] = f"{combo}+A10"
            for uni in ("all", "no_gme"):
                for K in (10, 20):
                    rows.append(run_one(ctx, combo, K, 10, uni, "R2")[0])
            for uni in ("all", "no_gme"):
                for K in (10, 20):
                    rows.append(run_one(ctx, "T0", K, 10, uni, "R2")[0])     # R2 reference for the combo
    else:
        uni = "no_gme" if a.no_gme else "all"
        Ks = (a.k,) if a.k else (10, 20)
        cb = int(a.cost) if float(a.cost).is_integer() else a.cost
        for K in Ks:
            for v in [x.strip() for x in a.variants.split(",") if x.strip()]:
                rows.append(run_one(ctx, v, K, cb, uni, a.exits)[0])

    print("\n" + HDR); print("-" * len(HDR))
    for r in sorted(rows, key=lambda r: (r["exits"], r["universe"], r["K"], r["cost_bps"], r["variant"])):
        print(fmt(r))
    if a.grid and a.period == "train":
        print("\nTRAIN decision (P1 CAGR>T0 @K20, P2 maxDD within 5pp @K20, P3 CAGR>T0 @K10, P4 no-GME CAGR>T0 @K20; 10 bps)")
        for v in ELIGIBLE:
            d = extra["decision"][v]
            print(f"  {v}  dCAGR20 {d['d_cagr_k20']*100:+7.2f}pp  dMaxDD20 {d['d_maxdd_k20']*100:+6.2f}pp  "
                  f"dCAGR10 {d['d_cagr_k10']*100:+7.2f}pp  dCAGR20noGME {d['d_cagr_k20_nogme']*100:+7.2f}pp  "
                  f"{''.join(p if d[p] else '--' for p in ('P1', 'P2', 'P3', 'P4'))}{'  PASS' if d['passes'] else ''}")
        print(f"  passers: {extra['passers']}  winner: {extra['winner'] or 'NONE'}  "
              f"(exploratory combo: {extra.get('exploratory_combo')})")
    bad = {"|".join(map(str, key(r))): r["sanity_errors"] for r in rows if not r["sanity_ok"]}
    print("\nSANITY:", "all rows passed (equity>0, cash>=0, positions<=K, entries at fire+1, no ticker overlap, "
          "P&L reconciles incl. trims/top-ups, trims only reduce shares, top-ups only in T7, post-trim weight at "
          "target, cost on every trim/top-up, weights at the prior close, trades at the open, schedule/trigger "
          "respected, T5 funds only what is needed)" if not bad else bad)
    extra["sanity_ok_all_rows_this_run"] = not bad
    extra["runtime_s"] = round(time.time() - t0, 1)
    fn = merge_write(a.period, rows, extra)
    print(f"\nwrote {fn}  ({time.time() - t0:.1f}s)")
    return rows, extra


if __name__ == "__main__":
    main()
