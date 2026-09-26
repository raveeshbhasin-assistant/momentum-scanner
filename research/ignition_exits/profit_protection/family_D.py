"""
Family D -- structure-following exits: re-based sell line and gated trend breaks.

TRAIN split only. All 12 variants are declared in VARIANTS below, before any
result was seen, and every one is reported. The engine's floor (close < original
pre-ignition base) stays ON under every variant (evaluate(..., floor=True)).

No look-ahead: every trigger at index k uses p[...] values up to and including
close k only. Rolling lows are built from PRIOR closes within the trade
(c[k-N..k-1], shrinking window for k < N; nothing before entry is used).
Exits fire on the close and are executed by the engine at the next open.

Variant definitions (G = peak_ret gate, k = sessions since entry):
  D01 base_dyn           c < base_dyn  (sell line ratchets up to the new pre-fire base on each re-fire)
  D02 ma50_g20           c < m50, armed once peak_ret >= 0.20
  D03 ma50_k63           c < m50, armed once k >= 63
  D04 ma50_g20|k63       c < m50, armed once peak_ret >= 0.20 OR k >= 63
  D05 ma50x2_g20|k63     2 consecutive closes < m50, armed (today) once peak_ret >= 0.20 OR k >= 63
  D06 ma50_g40           c < m50, armed once peak_ret >= 0.40
  D07 ma200              c < m200 (ungated)
  D08 low20_g20          c < min(prior 20 closes in trade), armed once peak_ret >= 0.20
  D09 low50_g20          c < min(prior 50 closes in trade), armed once peak_ret >= 0.20
  D10 bdyn+ma50_g20|k63  D01 OR D04
  D11 bdyn+ma50x2_g20|k63 D01 OR D05
  D12 bdyn+ma200         D01 OR D07
"""
from __future__ import annotations

import numpy as np

import engine as E

SPLIT = ("train",)


# ---------- building blocks (all causal) ----------
def _below_bdyn(p):
    return p["c"] < p["base_dyn"]


def _below(p, ma):
    m = p[ma]
    return np.isfinite(m) & (p["c"] < m)


def _below2(p, ma):
    b = _below(p, ma)
    prev = np.concatenate([[False], b[:-1]])
    return b & prev


def _armed(p, g=None, kmin=None):
    a = np.zeros(len(p["c"]), bool)
    if g is not None:
        a |= p["peak_ret"] >= g
    if kmin is not None:
        a |= p["k"] >= kmin
    return a


def _new_low(p, n):
    """close < min of the prior n closes since entry (index k uses c[max(0,k-n)..k-1])."""
    c = p["c"]
    out = np.zeros(len(c), bool)
    for k in range(1, len(c)):
        out[k] = c[k] < c[max(0, k - n):k].min()
    return out


# ---------- the 12 declared variants ----------
VARIANTS = [
    ("D01_base_dyn",           lambda p: _below_bdyn(p)),
    ("D02_ma50_g20",           lambda p: _below(p, "m50") & _armed(p, g=0.20)),
    ("D03_ma50_k63",           lambda p: _below(p, "m50") & _armed(p, kmin=63)),
    ("D04_ma50_g20|k63",       lambda p: _below(p, "m50") & _armed(p, g=0.20, kmin=63)),
    ("D05_ma50x2_g20|k63",     lambda p: _below2(p, "m50") & _armed(p, g=0.20, kmin=63)),
    ("D06_ma50_g40",           lambda p: _below(p, "m50") & _armed(p, g=0.40)),
    ("D07_ma200",              lambda p: _below(p, "m200")),
    ("D08_low20_g20",          lambda p: _new_low(p, 20) & _armed(p, g=0.20)),
    ("D09_low50_g20",          lambda p: _new_low(p, 50) & _armed(p, g=0.20)),
    ("D10_bdyn+ma50_g20|k63",  lambda p: _below_bdyn(p) | (_below(p, "m50") & _armed(p, g=0.20, kmin=63))),
    ("D11_bdyn+ma50x2_g20|k63", lambda p: _below_bdyn(p) | (_below2(p, "m50") & _armed(p, g=0.20, kmin=63))),
    ("D12_bdyn+ma200",         lambda p: _below_bdyn(p) | _below(p, "m200")),
]
assert len(VARIANTS) <= 12

# pre-registered selection gates (TRAIN, vs floor-only reference)
G1_MEAN, G2_RT20, G3_W10 = 0.337, 17, -0.208

COLS = ["mean", "median", "win", "worst10", "roundtrip20", "giveback", "held",
        "ret_per_100d", "big_capture", "early_exits", "post_exit_fwd63", "pf"]


def main():
    rows = [("REF_floor_only", E.evaluate(E.never, floor=True, splits=SPLIT)["train"])]
    for name, fn in VARIANTS:
        rows.append((name, E.evaluate(fn, floor=True, splits=SPLIT)["train"]))
    hdr = f"{'variant':26s} " + " ".join(f"{c[:9]:>9s}" for c in COLS) + "  gates"
    print(hdr)
    print("-" * len(hdr))
    for name, m in rows:
        g = (m["mean"] >= G1_MEAN, m["roundtrip20"] <= G2_RT20, m["worst10"] >= G3_W10)
        tag = "PASS" if all(g) else "".join("x" if not ok else "." for ok in g)
        vals = " ".join(f"{m[c]:9.4f}" if c != "roundtrip20" else f"{m[c]:9d}" for c in COLS)
        print(f"{name:26s} {vals}  {tag}")
    print("\ngates: G1 mean>=0.337, G2 roundtrip20<=17, G3 worst10>=-0.208 (x = fails that gate, order G1 G2 G3)")
    print("n =", rows[0][1]["n"])
    return rows


if __name__ == "__main__":
    main()
