"""
Family C: gain-activated wide trailing / giveback stops (TRAIN split only).

Every variant ADDS an exit on top of the current floor (close < pre-ignition base).
Each trigger only switches on once the trade has worked: peak_ret (running max
close / entry - 1, inclusive of today) has reached the activation gain G. Since
peak_ret is a running max, activation is sticky. Exits fire on the close and the
engine sells at the next open.

VARIANTS ARE DECLARED HERE, BEFORE ANY EVALUATION, AND ALL ARE REPORTED.
No parameter was changed after seeing results.

  kind "trail": active once peak_ret >= G; exit when dd = c/peak - 1 <= -W
  kind "give" : active once peak_ret >= G; exit when ret <= (1 - F) * peak_ret
                (i.e. gave back fraction F of the peak gain; always exits > 0 at the close)
  kind "chand": active once peak_ret >= G; exit when c < peak - m * ATR20
"""
from __future__ import annotations

import numpy as np

import engine as E

SPLITS = ("train",)   # the ONLY split this file ever evaluates

VARIANTS = [
    # name,                 kind,    G,    param
    ("C01_trail_g20_w20",   "trail", 0.20, 0.20),
    ("C02_trail_g20_w30",   "trail", 0.20, 0.30),
    ("C03_trail_g30_w25",   "trail", 0.30, 0.25),
    ("C04_trail_g30_w35",   "trail", 0.30, 0.35),
    ("C05_trail_g50_w25",   "trail", 0.50, 0.25),
    ("C06_trail_g50_w35",   "trail", 0.50, 0.35),
    ("C07_give_g20_f40",    "give",  0.20, 0.40),
    ("C08_give_g20_f60",    "give",  0.20, 0.60),
    ("C09_give_g30_f50",    "give",  0.30, 0.50),
    ("C10_chand_g20_m4",    "chand", 0.20, 4.0),
    ("C11_chand_g30_m3",    "chand", 0.30, 3.0),
    ("C12_chand_g30_m5",    "chand", 0.30, 5.0),
]

DESCRIPTIONS = {
    "trail": "once peak >= +{G:.0%}, sell on a {X:.0%} drawdown from peak close",
    "give": "once peak >= +{G:.0%}, sell after giving back {X:.0%} of the peak gain",
    "chand": "once peak >= +{G:.0%}, sell on close < peak - {X:g} x ATR20",
}

# Pre-registered gates vs base_fail on train (from PREREGISTRATION.md)
G1_MEAN_MIN = 0.337
G2_RT20_MAX = 17
G3_WORST10_MIN = -0.208


def make_rule(kind, G, x):
    def rule(p):
        on = p["peak_ret"] >= G                   # uses closes up to k only
        if kind == "trail":
            trig = p["dd"] <= -x
        elif kind == "give":
            trig = p["ret"] <= (1.0 - x) * p["peak_ret"]
        elif kind == "chand":
            atr = p["atr"]
            trig = np.isfinite(atr) & (p["c"] < p["peak"] - x * atr)
        else:
            raise ValueError(kind)
        return np.asarray(on & trig, dtype=bool)
    return rule


COLS = ["mean", "median", "win", "worst10", "roundtrip20", "giveback", "held",
        "ret_per_100d", "big_capture", "early_exits", "post_exit_fwd63"]


def fmt_row(name, m, flag=""):
    vals = []
    for c in COLS:
        v = m[c]
        vals.append(f"{v:>8d}" if isinstance(v, int) else f"{v:>8.4f}")
    return f"{name:<20} {m['n']:>4} " + " ".join(vals) + f"  {flag}"


def main():
    results = {}
    base = E.evaluate(E.never, floor=True, splits=SPLITS)["train"]
    results["BASE_floor_only"] = base
    for name, kind, G, x in VARIANTS:
        results[name] = E.evaluate(make_rule(kind, G, x), floor=True, splits=SPLITS)["train"]

    hdr = f"{'variant':<20} {'n':>4} " + " ".join(f"{c[:8]:>8}" for c in COLS) + "  gates(G1 G2 G3)"
    print("TRAIN split only. Fractions, not percent.")
    print(hdr)
    print("-" * len(hdr))
    print(fmt_row("BASE_floor_only", base, "reference"))
    for name, kind, G, x in VARIANTS:
        m = results[name]
        g1 = m["mean"] >= G1_MEAN_MIN
        g2 = m["roundtrip20"] <= G2_RT20_MAX
        g3 = m["worst10"] >= G3_WORST10_MIN
        flag = ("PASS " if (g1 and g2 and g3) else "fail ") + "".join("Y" if g else "n" for g in (g1, g2, g3))
        print(fmt_row(name, m, flag))
    print()
    for name, kind, G, x in VARIANTS:
        print(f"{name:<20} {DESCRIPTIONS[kind].format(G=G, X=x)}")
    return results


if __name__ == "__main__":
    main()
