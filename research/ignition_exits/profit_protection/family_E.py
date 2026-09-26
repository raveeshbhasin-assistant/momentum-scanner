"""
Family E: partial exits / scale-outs for Ignition Watch (TRAIN split only).

Idea: bank part of the position on a pre-set condition and let the remainder
run with the floor (close < pre-ignition base). The engine applies the floor to
every leg, so any leg that has not yet banked still exits on the floor.

All 12 variants are declared below BEFORE any evaluation was run; every one is
reported. No parameter was changed after results were seen.

No look-ahead: every trigger uses only p fields at index k (ret, peak_ret, dd, k),
which the engine builds from closes up to and including close k. Exits fire on
the close and execute at the next open (engine).
"""
from __future__ import annotations

import numpy as np

import engine as E

SPLITS = ("train",)   # the only split this file ever evaluates


# ---------------------------------------------------------------- trigger helpers
def _none(p):
    """Leg that only exits on the floor (or ages out at 252)."""
    return np.zeros(len(p["c"]), bool)


def _gain(x):
    """Close is up >= x vs entry."""
    return lambda p: p["ret"] >= x


def _time_up(day, x=0.0):
    """First close at session >= day that is up more than x vs entry."""
    return lambda p: (p["k"] >= day) & (p["ret"] > x)


def _trail(arm, dd):
    """Gain-activated trail: once the running-peak close is >= +arm vs entry,
    exit when the close is dd (or more) below that running peak."""
    return lambda p: (p["peak_ret"] >= arm) & (p["dd"] <= -dd)


def _lock(arm, keep):
    """Give-back lock: once the running-peak close is >= +arm vs entry,
    exit when the close falls back to <= +keep vs entry (still positive)."""
    return lambda p: (p["peak_ret"] >= arm) & (p["ret"] <= keep)


def legs(*spec):
    """spec = (trigger_fn, weight), ...  ->  rule returning the engine's legs list."""
    tot = sum(w for _, w in spec)
    assert abs(tot - 1.0) < 1e-9, tot

    def rule(p):
        return [(np.asarray(f(p), dtype=bool), w) for f, w in spec]
    return rule


T3 = 1.0 / 3.0

# ---------------------------------------------------------------- DECLARED VARIANTS (max 12)
VARIANTS = [
    ("E01_half@25",
     "1/2 sold at first close >= +25%; 1/2 floor-only",
     legs((_gain(0.25), 0.5), (_none, 0.5))),
    ("E02_half@40",
     "1/2 sold at first close >= +40%; 1/2 floor-only",
     legs((_gain(0.40), 0.5), (_none, 0.5))),
    ("E03_half@60",
     "1/2 sold at first close >= +60%; 1/2 floor-only",
     legs((_gain(0.60), 0.5), (_none, 0.5))),
    ("E04_half@100",
     "1/2 sold at first close >= +100%; 1/2 floor-only",
     legs((_gain(1.00), 0.5), (_none, 0.5))),
    ("E05_thirds@30/60",
     "1/3 at +30%, 1/3 at +60%, 1/3 floor-only",
     legs((_gain(0.30), T3), (_gain(0.60), T3), (_none, 1.0 - 2 * T3))),
    ("E06_quarters@25/50/100",
     "1/4 at +25%, 1/4 at +50%, 1/4 at +100%, 1/4 floor-only",
     legs((_gain(0.25), 0.25), (_gain(0.50), 0.25), (_gain(1.00), 0.25), (_none, 0.25))),
    ("E07_half@d63_if_up",
     "1/2 sold at first close on/after session 63 with ret > 0; 1/2 floor-only",
     legs((_time_up(63), 0.5), (_none, 0.5))),
    ("E08_half@d126_if_up",
     "1/2 sold at first close on/after session 126 with ret > 0; 1/2 floor-only",
     legs((_time_up(126), 0.5), (_none, 0.5))),
    ("E09_half@40+trail(arm40,dd25)",
     "1/2 at +40%; 1/2 on trail armed at peak >= +40%, exit close <= 25% below peak",
     legs((_gain(0.40), 0.5), (_trail(0.40, 0.25), 0.5))),
    ("E10_half@25+trail(arm50,dd30)",
     "1/2 at +25%; 1/2 on trail armed at peak >= +50%, exit close <= 30% below peak",
     legs((_gain(0.25), 0.5), (_trail(0.50, 0.30), 0.5))),
    ("E11_half_lock(20->5)",
     "1/2 sold if peak >= +20% then close falls to <= +5%; 1/2 floor-only",
     legs((_lock(0.20, 0.05), 0.5), (_none, 0.5))),
    ("E12_2/3_lock(20->10)",
     "2/3 sold if peak >= +20% then close falls to <= +10%; 1/3 floor-only",
     legs((_lock(0.20, 0.10), 2 * T3), (_none, 1.0 - 2 * T3))),
]
assert len(VARIANTS) <= 12

# pre-registered gates (train, vs floor-only reference)
G1_MEAN, G2_RT, G3_W10 = 0.337, 17, -0.208

COLS = ["mean", "median", "win", "worst10", "roundtrip20", "giveback", "held",
        "ret_per_100d", "big_capture", "early_exits", "post_exit_fwd63"]


def _fmt(v):
    if isinstance(v, (int, np.integer)):
        return f"{v:>8d}"
    return f"{v:>8.4f}"


def main():
    rows = [("REF_floor_only", "current rule (reference, not a variant)", E.evaluate(E.never, True, SPLITS)["train"])]
    for name, desc, rule in VARIANTS:
        rows.append((name, desc, E.evaluate(rule, floor=True, splits=SPLITS)["train"]))

    hdr = f"{'variant':32s} {'n':>4s} " + " ".join(f"{c[:8]:>8s}" for c in COLS) + "  G1 G2 G3 PASS"
    print(hdr)
    print("-" * len(hdr))
    out = []
    for name, desc, m in rows:
        g1, g2, g3 = m["mean"] >= G1_MEAN, m["roundtrip20"] <= G2_RT, m["worst10"] >= G3_W10
        ok = g1 and g2 and g3
        print(f"{name:32s} {m['n']:>4d} " + " ".join(_fmt(m[c]) for c in COLS)
              + f"  {'Y' if g1 else '.':>2s} {'Y' if g2 else '.':>2s} {'Y' if g3 else '.':>2s} {'PASS' if ok else '-':>4s}")
        out.append({"name": name, "description": desc, **{c: m[c] for c in COLS}, "n": m["n"], "pass": ok})
    print()
    for name, desc, _ in rows:
        print(f"  {name:32s} {desc}")
    return out


if __name__ == "__main__":
    import json
    res = main()
    with open("family_E_results.json", "w") as f:
        json.dump(res, f, indent=1, default=float)
