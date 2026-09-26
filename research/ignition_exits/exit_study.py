"""
Ignition exit study — when has an ignition breakout failed?

Every ignition fire 2016-2026 (S&P 500+400) becomes a trade: buy at the next
session's open, then apply each candidate exit rule to daily closes, selling
at the following open. Rules are compared on the same entries.

Design: rules are ranked on the TRAIN window (entries 2016-2021) and the
chosen rule is then checked, untouched, on the HOLDOUT (entries 2022 through
the last entry with a complete 252-session window). Entries after that are
reported separately as "live/censored".

Run: python research/ignition_exits/exit_study.py <price_dir>
where <price_dir> holds c*.pkl chunks of yfinance OHLCV (auto_adjust=True).
"""
from __future__ import annotations

import glob
import json
import sys

import numpy as np
import pandas as pd

H = 252            # max hold (sessions)
REENTRY_GAP = 20   # a new entry needs no fire in the prior 20 sessions


def load(price_dir: str):
    parts = [pd.read_pickle(f) for f in sorted(glob.glob(f"{price_dir}/c*.pkl"))]
    out = {}
    for fld in ("Open", "High", "Low", "Close", "Volume"):
        df = pd.concat([p[fld] for p in parts], axis=1)
        out[fld] = df.loc[:, ~df.columns.duplicated()].sort_index()
    return out


def main(price_dir: str) -> None:
    px = load(price_dir)
    O, Hh, L, C, V = (px[k] for k in ("Open", "High", "Low", "Close", "Volume"))
    r5 = C.pct_change(5, fill_method=None)
    r252 = C.pct_change(252, fill_method=None)
    volr = V.rolling(21).mean() / V.rolling(126).mean()
    ma20 = C.rolling(20).mean()
    ma50 = C.rolling(50).mean()
    ma200 = C.rolling(200).mean()
    golden = ma50 > ma200
    dv = (C * V).rolling(21).mean()
    valid = (C > 3) & (dv > 5e6) & r252.notna()
    sig = ((r5 > 0.12) & (volr > 1.5) & golden & valid).values
    prev_c = C.shift(1)
    tr = pd.concat([(Hh - L), (Hh - prev_c).abs(), (L - prev_c).abs()]).groupby(level=0).max()
    tr = tr.reindex(C.index)
    atr20 = tr.rolling(20).mean()
    fwd63 = (C.shift(-63) / C - 1).values
    base_fwd63 = float(np.nanmean(np.where(valid.values, fwd63, np.nan)))

    Cv, Ov, M20, M50, A20 = C.values, O.values, ma20.values, ma50.values, atr20.values
    G = golden.values
    dates = C.index
    n_days, n_tk = Cv.shape

    # ---- entries ----
    entries = []
    for j in range(n_tk):
        s = sig[:, j]
        idx = np.flatnonzero(s)
        last = -10_000
        for t in idx:
            if t - last > REENTRY_GAP and t + 1 < n_days and np.isfinite(Ov[t + 1, j]):
                entries.append((t, j))
            last = t
    print(f"entries: {len(entries)}  baseline fwd63 mean: {base_fwd63:.4f}", flush=True)

    # Signal persistence: is the fire still "on" the next day / 3 days later?
    persist = []
    for t, j in entries:
        on1 = bool(sig[t + 1, j]) if t + 1 < n_days else None
        on3 = bool(sig[t + 1:t + 4, j].any()) if t + 3 < n_days else None
        f = fwd63[t + 1, j] if t + 1 < n_days else np.nan
        persist.append((on1, on3, f))

    RULES = {
        "hold63": lambda p: p["k"] >= 63,
        "hold126": lambda p: p["k"] >= 126,
        "hold252": lambda p: p["k"] >= H,
        "dropoff_1d": lambda p: ~p["sig"],                      # sell when it falls off the list
        "stop8": lambda p: p["c"] < p["entry"] * 0.92,
        "stop10": lambda p: p["c"] < p["entry"] * 0.90,
        "stop15": lambda p: p["c"] < p["entry"] * 0.85,
        "trail15": lambda p: p["c"] < p["peak"] * 0.85,
        "trail20": lambda p: p["c"] < p["peak"] * 0.80,
        "trail25": lambda p: p["c"] < p["peak"] * 0.75,
        "trail30": lambda p: p["c"] < p["peak"] * 0.70,
        "below_ma20": lambda p: (p["c"] < p["m20"]) & (p["k"] >= 5),
        "below_ma50": lambda p: p["c"] < p["m50"],
        "below_ma50_2d": lambda p: p["below50_2d"],
        "death_cross": lambda p: ~p["g"],
        "chand3atr": lambda p: p["c"] < p["peak"] - 3 * p["atr"],
        "chand4atr": lambda p: p["c"] < p["peak"] - 4 * p["atr"],
        "base_fail": lambda p: p["c"] < p["base"],              # gave back the whole ignition week
        "base_or_trail25": lambda p: (p["c"] < p["base"]) | (p["c"] < p["peak"] * 0.75),
        "base_or_ma50_2d": lambda p: (p["c"] < p["base"]) | p["below50_2d"],
        "base_or_chand4": lambda p: (p["c"] < p["base"]) | (p["c"] < p["peak"] - 4 * p["atr"]),
        "stop10_or_trail25": lambda p: (p["c"] < p["entry"] * 0.90) | (p["c"] < p["peak"] * 0.75),
        "stop10_or_ma50_2d": lambda p: (p["c"] < p["entry"] * 0.90) | p["below50_2d"],
        "base_or_ma50_2d_or_trail25": lambda p: (p["c"] < p["base"]) | p["below50_2d"] | (p["c"] < p["peak"] * 0.75),
        # Edge window: the ignition edge lives ~63 sessions; a re-fire restarts the clock.
        "expire63": lambda p: p["since_fire"] >= 63,
        "base_or_expire63": lambda p: (p["c"] < p["base"]) | (p["since_fire"] >= 63),
        "base_or_expire126": lambda p: (p["c"] < p["base"]) | (p["since_fire"] >= 126),
        "stop15_or_expire63": lambda p: (p["c"] < p["entry"] * 0.85) | (p["since_fire"] >= 63),
    }

    rows = []
    for t, j in entries:
        e = t + 1                                  # entry session (buy at open)
        end = min(e + H, n_days - 1)
        if end <= e:
            continue
        entry = Ov[e, j]
        c = Cv[e:end + 1, j]
        if not np.isfinite(c).all():
            c = pd.Series(c).ffill().values
        k = np.arange(len(c))
        peak = np.maximum.accumulate(c)
        m50 = M50[e:end + 1, j]
        b50 = c < m50
        below50_2d = b50 & np.concatenate([[False], b50[:-1]])
        s_path = sig[e:end + 1, j]
        last_fire = np.maximum.accumulate(np.where(s_path, k, -1))
        since_fire = np.where(last_fire >= 0, k - last_fire, k + 1)   # entry fire was the session before k=0
        p = {
            "c": c, "k": k, "entry": entry, "peak": peak, "m20": M20[e:end + 1, j], "m50": m50,
            "below50_2d": below50_2d, "g": G[e:end + 1, j], "atr": A20[e:end + 1, j],
            "base": Cv[t - 5, j] if t >= 5 else -np.inf, "sig": s_path, "since_fire": since_fire,
        }
        complete = (e + H) <= n_days - 1
        max_run = np.nanmax(c) / entry - 1
        rec = {"ticker": C.columns[j], "fire": dates[t], "complete": complete, "max_run": max_run}
        last = len(c) - 1
        for name, fn in RULES.items():
            trig = np.asarray(fn(p), dtype=bool)[:last]   # a close-based trigger sells at the NEXT open
            hit = np.flatnonzero(trig)
            if len(hit):
                d = int(hit[0])
                xi = e + d + 1
                xp = Ov[xi, j] if np.isfinite(Ov[xi, j]) else Cv[xi, j]
                rec[name] = (xp / entry - 1, d + 1, True, fwd63[xi, j])
            else:
                # No trigger: complete windows exit at the max-hold close;
                # recent entries are still open and marked to market.
                rec[name] = (c[last] / entry - 1, last, False, np.nan)
        rows.append(rec)

    df = pd.DataFrame(rows)
    last_complete_fire = df.loc[df.complete, "fire"].max()
    df["split"] = np.where(df.fire < "2022-01-01", "train",
                   np.where(df.complete, "holdout", "live"))

    def summarize(sub: pd.DataFrame) -> pd.DataFrame:
        out = []
        big = sub.max_run >= 0.8
        for name in RULES:
            ret = sub[name].map(lambda x: x[0]).astype(float)
            held = sub[name].map(lambda x: x[1]).astype(float)
            early = sub[name].map(lambda x: x[2]).astype(bool)
            post = sub[name].map(lambda x: x[3]).astype(float)
            wins, losses = ret[ret > 0].sum(), -ret[ret < 0].sum()
            out.append({
                "rule": name, "n": len(sub), "mean": ret.mean(), "median": ret.median(),
                "win": (ret > 0).mean(), "pf": wins / losses if losses else np.nan,
                "worst10": ret.quantile(0.10), "held": held.mean(),
                "ret_per_100d": ret.mean() / held.mean() * 100 if held.mean() else np.nan,
                "big_capture": ret[big].mean() if big.any() else np.nan,
                "early_exits": early.mean(),
                "post_exit_fwd63": post[early].mean(),
                "post_exit_dd20": np.nan,
            })
        return pd.DataFrame(out).set_index("rule")

    res = {}
    for split in ("train", "holdout", "live"):
        sub = df[df.split == split]
        s = summarize(sub)
        res[split] = s
        print(f"\n===== {split.upper()}  entries={len(sub)}  big(>=+80% max run)={int((sub.max_run>=0.8).sum())}")
        print(s.drop(columns=["post_exit_dd20"]).round(3).sort_values("mean", ascending=False).to_string())

    # persistence analysis
    pr = pd.DataFrame(persist, columns=["on1", "on3", "fwd63"]).dropna()
    pers = {
        "p_still_on_next_day": float(pr.on1.mean()),
        "fwd63_if_on_next_day": float(pr[pr.on1].fwd63.mean()),
        "fwd63_if_off_next_day": float(pr[~pr.on1].fwd63.mean()),
        "fwd63_if_refires_within_3d": float(pr[pr.on3].fwd63.mean()),
        "fwd63_if_no_refire_3d": float(pr[~pr.on3].fwd63.mean()),
        "baseline_fwd63": base_fwd63,
    }
    print("\n===== SIGNAL PERSISTENCE (does dropping off the list matter?)")
    print(json.dumps({k: round(v, 4) for k, v in pers.items()}, indent=1))

    df.to_pickle(f"{price_dir}/exit_trades.pkl")
    json.dump({
        "entries": len(df), "last_complete_fire": str(last_complete_fire.date()),
        "splits": {k: int((df.split == k).sum()) for k in ("train", "holdout", "live")},
        "persistence": pers,
        "results": {k: v.round(4).reset_index().to_dict(orient="records") for k, v in res.items()},
    }, open(f"{price_dir}/exit_study.json", "w"), indent=1, default=str)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "px")
