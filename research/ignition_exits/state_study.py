"""
Ignition state study — once you hold an ignition stock, which observable
states say its edge is gone?

For every day k (0..252) of every ignition trade (same entries as
exit_study.py), record the position's state and the stock's forward 63-session
return in EXCESS of the same-day universe mean (removes market regime). A
useful SELL state is one where that forward excess is reliably negative.

Run: python research/ignition_exits/state_study.py <price_dir>
"""
from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from exit_study import H, REENTRY_GAP, load  # noqa: E402


def main(price_dir: str) -> None:
    px = load(price_dir)
    O, C, V = px["Open"], px["Close"], px["Volume"]
    r5 = C.pct_change(5, fill_method=None)
    r252 = C.pct_change(252, fill_method=None)
    volr = V.rolling(21).mean() / V.rolling(126).mean()
    ma50 = C.rolling(50).mean()
    golden = ma50 > C.rolling(200).mean()
    dv = (C * V).rolling(21).mean()
    valid = (C > 3) & (dv > 5e6) & r252.notna()
    sig = ((r5 > 0.12) & (volr > 1.5) & golden & valid).values
    fwd = (C.shift(-63) / C - 1)
    uni = fwd.where(valid).mean(axis=1)
    xs = fwd.sub(uni, axis=0).values                     # forward excess vs universe
    fwd_v = fwd.values
    uni_dd = ((C[::-1].rolling(63, min_periods=5).min()[::-1].shift(-1) / C - 1) <= -0.2).values

    Cv, Ov, M50, G, VR = C.values, O.values, ma50.values, golden.values, volr.values
    dates = C.index
    n_days, n_tk = Cv.shape
    rows = []
    for j in range(n_tk):
        last = -10_000
        for t in np.flatnonzero(sig[:, j]):
            new = t - last > REENTRY_GAP
            last = t
            if not new or t + 1 >= n_days or not np.isfinite(Ov[t + 1, j]) or t < 5:
                continue
            e, entry, base = t + 1, Ov[t + 1, j], Cv[t - 5, j]
            peak = -np.inf
            refire_k = -1
            for k in range(0, H + 1):
                d = e + k
                if d >= n_days - 63:
                    break
                c = Cv[d, j]
                if not np.isfinite(c):
                    continue
                peak = max(peak, c)
                # A re-fire is a fresh ignition after >= 5 sessions off — not the
                # original fire simply staying on for a few more days.
                if k > 0 and sig[d, j] and not sig[max(0, d - 5):d, j].any():
                    refire_k = k
                rows.append((dates[t], j, k, c / entry - 1, c / peak - 1, c < base,
                             c < M50[d, j], bool(G[d, j]), VR[d, j],
                             refire_k >= 0 and k - refire_k <= 20,
                             xs[d, j], fwd_v[d, j], uni_dd[d, j]))
    S = pd.DataFrame(rows, columns=["fire", "j", "k", "ret", "dd", "below_base", "below_ma50",
                                    "golden", "volr", "refired_20d", "xs63", "fwd63", "dd20"])
    S["split"] = np.where(S.fire < "2022-01-01", "train", "holdout")
    S["k_bucket"] = pd.cut(S.k, [-1, 20, 63, 126, 252], labels=["0-20", "21-63", "64-126", "127-252"])
    S["dd_bucket"] = pd.cut(S.dd, [-9, -0.30, -0.20, -0.10, 0.001], labels=["<-30%", "-20..-30%", "-10..-20%", "0..-10%"])
    S["ret_bucket"] = pd.cut(S.ret, [-9, -0.15, -0.05, 0.05, 0.25, 99], labels=["<-15%", "-15..-5%", "-5..+5%", "+5..+25%", ">+25%"])

    def table(by):
        g = S.groupby(["split"] + by, observed=True)
        out = g.agg(n=("xs63", "size"), trades=("fire", "nunique"), xs63=("xs63", "mean"),
                    fwd63=("fwd63", "mean"), p_dd20=("dd20", "mean"))
        return out.round(3)

    res = {}
    for by in (["k_bucket"], ["dd_bucket"], ["ret_bucket"], ["below_base"], ["below_ma50"],
               ["golden"], ["refired_20d"], ["below_base", "k_bucket"], ["dd_bucket", "below_ma50"],
               ["golden", "below_ma50"]):
        name = "+".join(by)
        tb = table(by)
        res[name] = tb.reset_index().astype({c: str for c in ["split"] + by}).to_dict(orient="records")
        print(f"\n===== by {name}")
        print(tb.to_string())
    base = S.groupby("split").agg(n=("xs63", "size"), xs63=("xs63", "mean"), p_dd20=("dd20", "mean"))
    print("\n===== all held days\n", base.round(3).to_string())
    json.dump(res, open(f"{price_dir}/state_study.json", "w"), indent=1, default=str)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "px")
