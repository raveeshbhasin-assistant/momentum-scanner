"""Skip reasons by date (copy of portfolio_A.simulate's entry logic with logging; counts verified vs PA)."""
import numpy as np, pandas as pd
import portfolio_A as PA
import portfolio_skeptic as S
from portfolio_skeptic import W, ranked, PLANS, CFF, OV, DATES, RULE_NAMES

def sim_log(cands, plan, K, cost, sess, entry_px):
    cash, eq_prev = 1.0, 1.0
    pos = {}; ev = []
    for s in sess:
        for j in [j for j, q in pos.items() if q["xs"] == s and q["kind"] == "open"]:
            q = pos.pop(j); cash += q["sh"] * q["xp"] * (1 - cost)
        for tid, j in cands.get(s, ()):
            if j in pos: ev.append((s, tid, "held")); continue
            if len(pos) >= K: ev.append((s, tid, "slot")); continue
            slot = eq_prev / K; size = min(slot, cash)
            if size < PA.MIN_FILL * slot: ev.append((s, tid, "cash")); continue
            px = entry_px[tid]; sh = size / (px * (1 + cost)); cash -= size
            xs, kind, xp, reason = plan[tid]
            pos[j] = dict(sh=sh, xs=xs, kind=kind, xp=xp); ev.append((s, tid, "taken"))
        for j in [j for j, q in pos.items() if q["xs"] == s and q["kind"] == "close"]:
            q = pos.pop(j); cash += q["sh"] * q["xp"] * (1 - cost)
        inv = sum(q["sh"] * (CFF[s, j] if np.isfinite(CFF[s, j]) else 0) for j, q in pos.items())
        eq_prev = cash + inv
    return ev

burst_lo, burst_hi = pd.Timestamp("2020-03-01"), pd.Timestamp("2020-04-30")
for per in ("train", "holdout"):
    Wd = W(per)
    print(f"\n{per}: skip reasons (10 bps), split into the Mar-Apr 2020 burst vs everything else")
    for K in (20, 10):
        for nm in RULE_NAMES:
            ev = sim_log(ranked(Wd), PLANS[nm], K, 10 / 1e4, Wd["sess"], Wd["entry_px"])
            m = S.run(per, nm, K)
            c = pd.Series([e[2] for e in ev]).value_counts()
            assert c.get("taken", 0) == m["trades"] and c.get("slot", 0) == m["sk_slot"] and c.get("cash", 0) == m["sk_cash"], nm
            df = pd.DataFrame(ev, columns=["s", "tid", "why"]); df["d"] = DATES[df.s.values]
            inb = (df.d >= burst_lo) & (df.d <= burst_hi)
            a = df[inb].why.value_counts(); b = df[~inb].why.value_counts()
            nb = int((~inb).sum())
            print(f"  K{K} {nm:<11} burst: n {int(inb.sum()):>3} taken {a.get('taken',0):>3} slot {a.get('slot',0):>3} cash {a.get('cash',0):>3} | "
                  f"rest: n {nb:>3} taken {b.get('taken',0):>3} slot {b.get('slot',0):>3} cash {b.get('cash',0):>3} held {b.get('held',0):>2} "
                  f"capSkip% {(b.get('slot',0)+b.get('cash',0))/nb*100:5.1f}")
