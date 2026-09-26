"""
Family A -- Time-gated profit lock (the operator's literal idea):
"after X sessions have passed, if the position is still up, take it / protect it".

Variants are DECLARED HERE, BEFORE ANY RESULT WAS SEEN. All are reported.
The engine floor (close < pre-ignition base) stays ON under every rule.
Train split only. No parameter changes after the first run.

Conventions (engine): k = sessions since entry (k=0 entry day); ret = c/entry - 1;
triggers fire on the close and sell at the next open. Every field used is
causal (value at k uses closes up to k only).
"""
import numpy as np

import engine as E

# ---------------------------------------------------------------- declared variants
VARIANTS = [
    # --- plain "after X days, if positive, sell" (first close at k>=X with ret>0)
    ("A01_after21_pos",       "k>=21 and ret>0 -> sell (short gate, lower bound of the family)",
     lambda p: (p["k"] >= 21) & (p["ret"] > 0)),
    ("A02_after63_pos",       "k>=63 and ret>0 -> sell (plain: after 3 months, if green, bank it)",
     lambda p: (p["k"] >= 63) & (p["ret"] > 0)),
    ("A03_after126_pos",      "k>=126 and ret>0 -> sell (plain: after 6 months, if green, bank it)",
     lambda p: (p["k"] >= 126) & (p["ret"] > 0)),
    ("A04_after189_pos",      "k>=189 and ret>0 -> sell (plain: after 9 months, if green, bank it)",
     lambda p: (p["k"] >= 189) & (p["ret"] > 0)),
    # --- single checkpoint: only looked at once, on session 126
    ("A05_at126_checkpoint",  "only on k==126: if ret>0 sell; else keep holding under the floor",
     lambda p: (p["k"] == 126) & (p["ret"] > 0)),
    # --- 'edge window over and it has NOT re-fired' (re-firing names keep running)
    ("A06_after63_pos_norefire",  "k>=63 and ret>0 and no re-fire since entry -> sell",
     lambda p: (p["k"] >= 63) & (p["ret"] > 0) & ~p["refired"].astype(bool)),
    ("A07_after126_pos_norefire", "k>=126 and ret>0 and no re-fire since entry -> sell",
     lambda p: (p["k"] >= 126) & (p["ret"] > 0) & ~p["refired"].astype(bool)),
    # --- 'edge expired' measured from the LATEST fire (re-fires reset the clock)
    ("A08_edge_expired63_pos", "since_fire>=63 and ret>0 -> sell (clock resets on each re-fire)",
     lambda p: (p["since_fire"] >= 63) & (p["ret"] > 0)),
    # --- banded: bank only modest gains, let big winners (>= +30%) run
    ("A09_after63_band0_30",  "k>=63 and 0<ret<0.30 -> sell (winners above +30% keep running)",
     lambda p: (p["k"] >= 63) & (p["ret"] > 0) & (p["ret"] < 0.30)),
    ("A10_after126_band0_30", "k>=126 and 0<ret<0.30 -> sell (winners above +30% keep running)",
     lambda p: (p["k"] >= 126) & (p["ret"] > 0) & (p["ret"] < 0.30)),
    # --- stop moves up after X (protect, not bank)
    ("A11_after63_breakeven_stop", "k>=63 and close<entry -> sell (stop raised from base to entry)",
     lambda p: (p["k"] >= 63) & (p["ret"] < 0)),
    ("A12_after63_lock5_if_up20",  "k>=63 and peak_ret>=0.20 and ret<0.05 -> sell (lock ~+5% on names that were up 20%)",
     lambda p: (p["k"] >= 63) & (p["peak_ret"] >= 0.20) & (p["ret"] < 0.05)),
]
assert len(VARIANTS) <= 12

SPLIT = ("train",)
COLS = ["n", "mean", "median", "win", "worst10", "roundtrip20", "giveback", "held",
        "ret_per_100d", "big_capture", "early_exits", "post_exit_fwd63"]


def gates(m):
    g1 = m["mean"] >= 0.337
    g2 = m["roundtrip20"] <= 17
    g3 = m["worst10"] >= -0.208
    return ("G1" if g1 else "--") + ("G2" if g2 else "--") + ("G3" if g3 else "--"), g1 and g2 and g3


def fmt(m):
    out = []
    for c in COLS:
        v = m[c]
        out.append(f"{v:>6d}" if isinstance(v, int) else f"{v:>8.4f}")
    return " ".join(out)


def main():
    rows = [("BASE_floor_only", "current live rule (close < pre-ignition base)", E.never)] + VARIANTS
    print(f"{'variant':<28} " + " ".join(f"{c[:8]:>8}" if c not in ("n", "roundtrip20") else f"{c[:6]:>6}"
                                         for c in COLS) + "  gates")
    results = {}
    for name, desc, rule in rows:
        m = E.evaluate(rule, floor=True, splits=SPLIT)["train"]
        results[name] = m
        g, ok = gates(m)
        print(f"{name:<28} {fmt(m)}  {g}{'  PASS' if ok else ''}")
    print("\nDescriptions:")
    for name, desc, _ in rows:
        print(f"  {name:<28} {desc}")
    return results


if __name__ == "__main__":
    main()
