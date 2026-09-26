"""
FAMILY B -- Gain-activated ratchet stops (lock-in levels).  TRAIN SPLIT ONLY.

Once the running peak gain (p["peak_ret"], close-based, inclusive of today)
reaches an activation threshold, a stop is set at entry*(1+lock). The lock is a
non-decreasing function of the (non-decreasing) peak gain and is additionally
forced through a running max, so it can only go up. The exit fires when the
CLOSE is below entry*(1+lock) and executes at the next open (engine). The
current production rule (close < pre-ignition base, "floor") stays on under every
variant (evaluate(..., floor=True)).

VARIANTS ARE DECLARED HERE, BEFORE ANY RESULT WAS SEEN. All 12 are reported.
No parameter changes after seeing results.

  kind "ladder": list of (peak_threshold, lock_level); highest reached step wins
  kind "frac":   once peak >= act, lock = f * peak_ret        (keep f of max gain)
  kind "minus":  once peak >= act, lock = peak_ret - g        (give back <= g pp)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import engine

VARIANTS = [
    # name,            kind,     params,                                         description
    ("B01_be@20",      "ladder", [(0.20, 0.00)],
     "peak>=+20% -> stop at breakeven (close < entry)"),
    ("B02_be@30",      "ladder", [(0.30, 0.00)],
     "peak>=+30% -> stop at breakeven"),
    ("B03_+5@20",      "ladder", [(0.20, 0.05)],
     "peak>=+20% -> lock +5%"),
    ("B04_+10@30",     "ladder", [(0.30, 0.10)],
     "peak>=+30% -> lock +10%"),
    ("B05_+20@50",     "ladder", [(0.50, 0.20)],
     "peak>=+50% -> lock +20%"),
    ("B06_ladderA",    "ladder", [(0.25, 0.00), (0.50, 0.20), (1.00, 0.50)],
     "ladder: +25%->0, +50%->+20%, +100%->+50%"),
    ("B07_ladderTight", "ladder", [(0.20, 0.00), (0.40, 0.15), (0.60, 0.30), (1.00, 0.60)],
     "ladder: +20%->0, +40%->+15%, +60%->+30%, +100%->+60%"),
    ("B08_ladderLoose", "ladder", [(0.30, 0.00), (0.60, 0.25), (1.20, 0.70)],
     "ladder: +30%->0, +60%->+25%, +120%->+70%"),
    ("B09_half@20",    "frac",   (0.20, 0.50),
     "peak>=+20% -> lock 50% of peak gain"),
    ("B10_half@30",    "frac",   (0.30, 0.50),
     "peak>=+30% -> lock 50% of peak gain"),
    ("B11_third@25",   "frac",   (0.25, 1.0 / 3.0),
     "peak>=+25% -> lock 1/3 of peak gain"),
    ("B12_minus25@25", "minus",  (0.25, 0.25),
     "peak>=+25% -> lock peak gain minus 25pp (breakeven at +25%, trails 25pp)"),
]
assert len(VARIANTS) <= 12

SPLITS = ("train",)   # the ONLY split this file ever evaluates


def lock_path(pr: np.ndarray, kind: str, params) -> np.ndarray:
    """Lock level (as a return vs entry) at each session; -inf = not active.
    Uses only peak_ret[0..k] at index k (peak_ret is itself a running max)."""
    lock = np.full(len(pr), -np.inf)
    if kind == "ladder":
        for thr, lk in params:
            lock = np.where(pr >= thr, np.maximum(lock, lk), lock)
    elif kind == "frac":
        act, f = params
        lock = np.where(pr >= act, f * pr, -np.inf)
    elif kind == "minus":
        act, g = params
        lock = np.where(pr >= act, pr - g, -np.inf)
    else:
        raise ValueError(kind)
    return np.maximum.accumulate(lock)          # ratchet: can only go up


def make_rule(kind, params):
    def rule(p):
        lock = lock_path(p["peak_ret"], kind, params)
        return p["ret"] < lock                   # close below entry*(1+lock)
    return rule


def ratchet_share(kind, params) -> tuple[float, float]:
    """Diagnostic (train trades only): share of trades whose FIRST exit trigger is
    the ratchet (not the floor), and their mean close-return on the trigger day."""
    D = engine._load()
    n = hits = 0
    rets = []
    for tr in D["trades"]:
        if tr["split"] != "train":
            continue
        p = tr["p"]; last = len(p["c"]) - 1
        n += 1
        rt = make_rule(kind, params)(p)[:last]
        fl = (p["c"] < p["base"])[:last]
        ir = np.flatnonzero(rt); iff = np.flatnonzero(fl)
        dr = ir[0] if len(ir) else None
        df_ = iff[0] if len(iff) else None
        if dr is not None and (df_ is None or dr <= df_):
            hits += 1
            rets.append(p["ret"][dr])
    return hits / n, (float(np.mean(rets)) if rets else float("nan"))


COLS = ["mean", "median", "win", "worst10", "roundtrip20", "giveback", "held",
        "ret_per_100d", "big_capture", "early_exits", "post_exit_fwd63", "pf", "n"]


def gates(m):
    g1 = m["mean"] >= 0.337
    g2 = m["roundtrip20"] <= 17
    g3 = m["worst10"] >= -0.208
    return g1, g2, g3


def main():
    rows = []
    base = engine.evaluate(engine.never, floor=True, splits=SPLITS)["train"]
    rows.append({"name": "BASE_floor_only", "desc": "current production rule", **base,
                 "ratchet_share": 0.0, "ratchet_trig_ret": float("nan")})
    for name, kind, params, desc in VARIANTS:
        m = engine.evaluate(make_rule(kind, params), floor=True, splits=SPLITS)["train"]
        sh, tr_ret = ratchet_share(kind, params)
        rows.append({"name": name, "desc": desc, **m, "ratchet_share": sh, "ratchet_trig_ret": tr_ret})
    df = pd.DataFrame(rows).set_index("name")
    g = df.apply(lambda r: "".join("Y" if x else "n" for x in gates(r)), axis=1)
    df["G1G2G3"] = g
    df["PASS"] = g == "YYY"
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 40)
    show = df[COLS + ["ratchet_share", "ratchet_trig_ret", "G1G2G3", "PASS"]].copy()
    fmt = {c: "{:.4f}".format for c in show.columns if show[c].dtype == float}
    print("FAMILY B -- gain-activated ratchet stops (TRAIN only, floor ON)\n")
    print(show.to_string(formatters=fmt))
    print("\nGates vs base (train): G1 mean>=0.337, G2 roundtrip20<=17, G3 worst10>=-0.208")
    passers = df[df.PASS].sort_values(["median", "mean"], ascending=False)
    print("\nPassers ranked by median (tie-break mean):")
    print(passers[["median", "mean", "roundtrip20", "worst10"]].to_string() if len(passers) else "  none")
    df.to_csv("family_B_results.csv")


if __name__ == "__main__":
    main()
