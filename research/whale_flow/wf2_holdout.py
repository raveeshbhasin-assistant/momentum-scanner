"""WF2 holdout test: apply the frozen strategies to entries 2020-01-01 .. 2026-04-01, once.

  python research/whale_flow/wf2_holdout.py --freeze   hash the design, code and frozen strategies
  python research/whale_flow/wf2_holdout.py            run the holdout (refuses without a matching freeze, or twice)
"""
import hashlib
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import wf1_run as w
from wf2_common import FROZEN, OUT, DayBase, one_position, summarise
from wf_common import WF

HERE = Path(__file__).parent
HOLD = ("2020-01-01", "2026-04-01")
YEARS = 6.25
LOCK = HERE / "WF2_frozen_sha256.txt"
CODE = ["WF2_DESIGN.md", "wf2_common.py", "wf2_features.py", "wf2_mine.py", "wf2_holdout.py"]
FROZEN_FILES = ["s1_rule.json", "s2_spec.json", "s2_model.pkl", "s3_spec.json", "s4_spec.json", "s4_model.pkl"]
DATA = ["cand_holdout.pkl", "week_holdout.pkl"]
RESULT = OUT / "holdout_report.csv"


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def hashes():
    out = {n: sha(HERE / n) for n in CODE}
    out.update({f"frozen/{n}": sha(FROZEN / n) for n in FROZEN_FILES})
    out.update({f"wf2/{n}": sha(OUT / n) for n in DATA})
    return out


def rule_mask(df, rule):
    m = np.ones(len(df), bool)
    for f, op, t in rule:
        x = df[f].to_numpy(float)
        m &= (x >= t) if op == ">=" else (x <= t)
    return m


def shuffle_within_month(values, entry, rng):
    """Placebo: the same number of fires each month, on randomly chosen candidates."""
    out = np.array(values, copy=True)
    month = pd.to_datetime(entry).dt.to_period("M").astype(str).values
    for m in np.unique(month):
        i = np.flatnonzero(month == m)
        out[i] = out[rng.permutation(i)]
    return out


def main():
    if "--freeze" in sys.argv:
        if LOCK.exists():
            sys.exit("already frozen")
        LOCK.write_text(json.dumps(hashes(), indent=1))
        print(LOCK.read_text())
        return
    if not LOCK.exists():
        sys.exit("freeze first")
    if json.loads(LOCK.read_text()) != hashes():
        sys.exit("files changed since the freeze")
    if RESULT.exists():
        sys.exit("holdout already run: it is run once")

    rng = np.random.default_rng(20261004)
    panel = w.Panel()
    db = DayBase(panel)
    cand = pd.read_pickle(OUT / "cand_holdout.pkl").sort_values("entry").reset_index(drop=True)
    week = pd.read_pickle(OUT / "week_holdout.pkl").sort_values("entry").reset_index(drop=True)
    fires, placebo, hs = {}, {}, {}

    rule = json.load(open(FROZEN / "s1_rule.json"))
    m1 = rule_mask(cand, rule["rule"])
    hs["S1 rule"] = rule["h"]
    fires["S1 rule"] = one_position(cand, rule["h"], m1)
    placebo["S1 rule"] = one_position(cand, rule["h"], shuffle_within_month(m1, cand.entry, rng))

    s2 = json.load(open(FROZEN / "s2_spec.json"))
    sc2 = pickle.load(open(FROZEN / "s2_model.pkl", "rb")).predict_proba(cand[s2["features"]].to_numpy("float32"))[:, 1]
    hs["S2 model"] = s2["h"]
    fires["S2 model"] = one_position(cand, s2["h"], sc2 >= s2["cut"])
    placebo["S2 model"] = one_position(cand, s2["h"], shuffle_within_month(sc2, cand.entry, rng) >= s2["cut"])

    f = pd.read_pickle(WF / "insider_purchases.pkl")
    u = w.make_events(f, panel.cal)["U"]
    u = u[(u.entry_date >= HOLD[0]) & (u.entry_date <= HOLD[1])]
    blocked = np.zeros(panel.elig.shape, bool)
    r3 = w.evaluate(panel, u, blocked, np.random.default_rng(w.SEED))
    r3 = r3[r3.status == "tradeable"].reset_index(drop=True)
    hs["S3 simple"] = 21
    fires["S3 simple"] = one_position(r3, 21, np.ones(len(r3), bool))
    p3 = w.evaluate(panel, u, blocked, np.random.default_rng(w.SEED + 1), selftest=True)
    placebo["S3 simple"] = one_position(p3.reset_index(drop=True), 21, np.ones(len(p3), bool))

    s4 = json.load(open(FROZEN / "s4_spec.json"))
    sc4 = pickle.load(open(FROZEN / "s4_model.pkl", "rb")).predict_proba(week[s4["features"]].to_numpy("float32"))[:, 1]
    hs["S4 broad model"] = s4["h"]
    fires["S4 broad model"] = one_position(week, s4["h"], sc4 >= s4["cut"])
    placebo["S4 broad model"] = one_position(week, s4["h"], shuffle_within_month(sc4, week.entry, rng) >= s4["cut"])

    rows, yearly = [], []
    for name, fr in fires.items():
        h = hs[name]
        fr = db.attach(fr, h)
        fr.to_csv(OUT / f"holdout_fires_{name.split()[0]}.csv", index=False)
        st = summarise(fr, h, rng, YEARS)
        pl = db.attach(placebo[name], h)
        st["placebo_precision"] = float((pl[f"exc{h}"] > 0).mean()) if len(pl) else np.nan
        st["placebo_n"] = len(pl)
        rows.append({"strategy": name, "h": h, **st})
        if len(fr):
            y = fr.assign(year=pd.to_datetime(fr.entry).dt.year, win=(fr[f"exc{h}"] > 0).astype(float),
                          exc=fr[f"exc{h}"].astype(float))
            yearly.append(y.groupby("year").agg(n=("win", "size"), precision=("win", "mean"),
                                                base_rate=("base_win", "mean"), mean_exc=("exc", "mean")).assign(strategy=name))
    # reference: every insider candidate, and every weekly row, in the holdout
    for nm, df, h in (("(all insider-purchase days)", cand, hs["S2 model"]), ("(all insider-purchase days, 21)", cand, 21)):
        ref = db.attach(one_position(df, h, np.ones(len(df), bool)), h)
        rows.append({"strategy": nm, "h": h, **summarise(ref, h, rng, YEARS)})
    rep = pd.DataFrame(rows)
    main4 = rep.strategy.isin(list(fires))
    rep.loc[main4, "holm_p"] = w.holm(rep.loc[main4, "p"].fillna(1.0).values)
    verdicts = {}
    for _, r in rep[main4].iterrows():
        g = {"n>=100": r.n >= 100, "precision>=55%": r.precision >= 0.55, "lift_holm_p<0.05": r.holm_p < 0.05,
             "mean_excess_lo>0": r.exc_lo > 0, "years>=60%": r.years_lift_pos >= 0.6,
             "ex_top5>0": r.mean_exc_ex_top5 > 0}
        g = {k: bool(v) for k, v in g.items()}
        v = "CONFIRMED" if all(g.values()) else "PARTIAL" if g["n>=100"] and g["lift_holm_p<0.05"] else "NOT CONFIRMED"
        verdicts[r.strategy] = {"verdict": v, "gates": g}
    rep["verdict"] = rep.strategy.map(lambda s: verdicts.get(s, {}).get("verdict", ""))
    rep.to_csv(RESULT, index=False)
    pd.concat(yearly).to_csv(OUT / "holdout_yearly.csv")
    (OUT / "holdout_verdict.json").write_text(json.dumps(verdicts, indent=1))
    with pd.option_context("display.width", 260, "display.max_columns", 40, "display.float_format", "{:.4f}".format):
        print(rep[["strategy", "h", "n", "per_year", "precision", "base_rate", "lift", "lift_lo", "lift_hi", "p",
                   "holm_p", "mean_exc", "exc_lo", "exc_hi", "verdict"]].to_string(index=False))
        print(rep[["strategy", "median_exc", "mean_exc_vs_random", "mean_exc_ex_top5", "share_worse_m10",
                   "share_better_p10", "years_lift_pos", "abs_win_rate", "precision_vs_peers",
                   "placebo_precision"]].to_string(index=False))
        print(pd.concat(yearly).round(4).to_string())
    print(json.dumps(verdicts, indent=1))


if __name__ == "__main__":
    main()
