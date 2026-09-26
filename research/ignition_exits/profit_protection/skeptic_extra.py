import numpy as np, pandas as pd, json
import portfolio_skeptic as S
from portfolio_skeptic import TRADES, TICK, DATES, run, scenario, fmt_scn, W, RULE_NAMES

def tid_of(tk, fire):
    for i, tr in enumerate(TRADES):
        if TICK[tr["j"]] == tk and str(tr["fire"].date()) == fire:
            return i
GME = tid_of("GME", "2020-10-08"); CELH = tid_of("CELH", "2020-05-26"); GME2 = tid_of("GME", "2021-01-13")
print("tids", GME, CELH, GME2)

# 1) status of GME / CELH in each rule's train run (K20, K10)
for K in (20, 10):
    for nm in RULE_NAMES:
        m = run("train", nm, K, keep=True); res = m["_res"]
        taken = {r["tid"]: r for r in res["log"]}
        st = []
        for name, t in (("GME@20-10-08", GME), ("CELH@20-05-26", CELH), ("GME@21-01-13", GME2)):
            if t in taken:
                r = taken[t]; pl = (r["proceeds"] if r["exit_s"] is not None else r["shares"]*r["mark_px"]) - r["cost_in"]
                xd = DATES[r["exit_s"]].date() if r["exit_s"] is not None else "open"
                st.append(f"{name} TAKEN exit {xd} pnl {pl:+.3f}")
            else:
                st.append(f"{name} not taken")
        print(f"K{K} {nm:<11} " + " | ".join(st))

# why did R0 skip GME@2020-10-08?  replay the R0 K20 run up to that session
s_g = TRADES[GME]["e"]
m = run("train", "R0_current", 20, keep=True); res = m["_res"]
i = list(W("train")["sess"]).index(s_g)
print("R0 K20 on GME entry day", DATES[s_g].date(), "npos(prev close)", res["npos"][i-1], "cash/eq prev", res["cash"][i-1]/res["eq"][i-1],
      "half-slot", 0.5/20)
cands_day = [c for c in W("train")["cands"] if c[4] == s_g]
print("candidates that day:", [(c[3], round(c[2],3)) for c in cands_day])

# 2) symmetric drops (same trade removed from every rule)
for label, ex in (("GME@2020-10-08 only", {GME}), ("CELH@2020-05-26 only", {CELH}), ("GME+CELH", {GME, CELH})):
    for per in ("train",):
        s = scenario(per, exclude_by={(nm, K): ex for nm in RULE_NAMES for K in (20, 10)})
        fmt_scn(s, f"symmetric drop {label} {per}")

# 3) drawdown with GME excluded -- what's R6/R4 maxDD episode
for nm in ("R0_current", "R2_A10", "R4_A08", "R6_A06"):
    a = run("train", nm, 20, exclude=(GME,))
    print(f"no-GME {nm:<11} K20 CAGR {a['cagr']*100:.2f} DD {a['maxdd']*100:.1f} {a['dd_peak']}->{a['dd_trough']}")

# 4) burstiness of candidates
for per in ("train", "holdout"):
    cnt = pd.Series([c[4] for c in W(per)["cands"]]).value_counts()
    n = cnt.sum()
    top = cnt.sort_values(ascending=False)
    print(f"{per}: {n} candidates on {len(cnt)} sessions; sessions with >1 cand {int((cnt>1).sum())} holding {int(cnt[cnt>1].sum())} cands; "
          f"top-10 sessions hold {int(top.iloc[:10].sum())} ({top.iloc[:10].sum()/n:.0%}); max/day {int(top.iloc[0])} on {DATES[top.index[0]].date()}")
    # monthly
    mo = pd.Series(1, index=[DATES[c[4]] for c in W(per)["cands"]]).resample("QE").sum()
    print("  per quarter:", " ".join(f"{d.strftime('%y')}Q{(d.month-1)//3+1}:{v}" for d, v in mo.items()))
