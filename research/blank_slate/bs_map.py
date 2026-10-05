"""Blank-slate information map, TRAINING YEARS ONLY (signal days 2006-01-03 .. 2019-09-30).

For every stock-day, ~45 conditions known at the close are ranked into deciles across the
eligible stocks of that day, and the next 1..63 sessions are measured for the top and bottom
decile: how much the decile beat the average eligible stock, its net return, and its hit rates
(absolute, and against SPY) next to the same-day base rates. Nothing here reads a holdout row.

  python research/blank_slate/bs_map.py            -> blank_slate/map_train.csv
"""
import numpy as np
import pandas as pd

from bs_common import BS, TRAIN, Panel, cost_side

H_OC = (1, 2, 3, 5, 10, 21, 63)   # enter next open, leave at the close h sessions later
H_CC = (1, 5)                     # enter at the signal day's close (market-on-close), leave h closes later
MIN_PRICE, MIN_ADV, BIG_ADV = 5.0, 10e6, 100e6


def rsi(c, n):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.where(dn > 0))


def streak(c):
    """Signed count of consecutive up (+) or down (-) closes."""
    s = np.sign(c.diff()).fillna(0).to_numpy()
    out = np.zeros_like(s)
    for t in range(1, len(s)):
        same = s[t] == s[t - 1]
        out[t] = np.where(s[t] == 0, 0, np.where(same, out[t - 1] + s[t], s[t]))
    return pd.DataFrame(out, index=c.index, columns=c.columns)


def feature_frames(op, hi, lo, c, dv, rc):
    """Yield (name, DataFrame). Every window ends on the row's own day."""
    for n in (1, 2, 3, 5, 10, 21, 63, 126, 252):
        yield f"r{n}", c / c.shift(n) - 1
    yield "mom_12_1", c.shift(21) / c.shift(252) - 1
    yield "mom_6_1", c.shift(21) / c.shift(126) - 1
    yield "rsi2", rsi(c, 2)
    yield "rsi14", rsi(c, 14)
    yield "gap", op / c.shift(1) - 1
    yield "intraday", c / op - 1
    rng = (hi - lo)
    yield "clv", (c - lo) / rng.where(rng > 0)
    for n in (5, 10, 20, 63, 252):
        yield f"d_hi{n}", c / hi.rolling(n, min_periods=max(3, n // 2)).max() - 1
    for n in (5, 20, 252):
        yield f"d_lo{n}", c / lo.rolling(n, min_periods=max(3, n // 2)).min() - 1
    ret = c / c.shift(1) - 1
    sma20, sd20 = c.rolling(20, min_periods=15).mean(), c.rolling(20, min_periods=15).std()
    yield "z20", (c - sma20) / sd20.where(sd20 > 0)
    yield "sma20r", c / sma20 - 1
    sma50, sma200 = c.rolling(50, min_periods=50).mean(), c.rolling(200, min_periods=200).mean()
    yield "sma50r", c / sma50 - 1
    yield "sma200r", c / sma200 - 1
    yield "sma50_200", sma50 / sma200 - 1
    vol20, vol63 = ret.rolling(20, min_periods=15).std(), ret.rolling(63, min_periods=40).std()
    yield "vol20", vol20
    yield "vol63", vol63
    yield "volratio", ret.rolling(5, min_periods=4).std() / vol63.where(vol63 > 0)
    yield "range5", (rng / c).rolling(5, min_periods=4).mean()
    yield "r1_z", ret / vol20.where(vol20 > 0).shift(1)
    yield "r5_z", (c / c.shift(5) - 1) / (vol20.where(vol20 > 0).shift(5) * np.sqrt(5))
    adv = dv.rolling(20, min_periods=15).mean()
    yield "surge1", dv / adv.where(adv > 0)
    yield "surge5", dv.rolling(5, min_periods=4).mean() / dv.rolling(63, min_periods=40).mean()
    yield "log_adv", np.log(adv.where(adv > 0))
    yield "log_px", np.log(rc.where(rc > 0))


def main():
    p = Panel()
    cal = p.cal
    s0, s1 = cal.searchsorted(pd.Timestamp(TRAIN[0])), cal.searchsorted(pd.Timestamp(TRAIN[1]), side="right") - 1
    rows = slice(s0, s1 + 1)
    stock = p.kind != "etf"
    dv_df = p.df(p.dv)
    adv = dv_df.rolling(20, min_periods=15).mean().to_numpy()
    hist = (p.df(p.cl).notna().rolling(252, min_periods=1).sum() >= 200).to_numpy()
    elig = (p.rc >= MIN_PRICE) & (adv >= MIN_ADV) & hist & stock[None, :]
    cols = np.flatnonzero(elig[rows].any(0))
    print(f"train sessions {s1 - s0 + 1}, stocks ever eligible {len(cols)}, "
          f"avg eligible per day {elig[rows].sum(1).mean():.0f} "
          f"(of which delisted {elig[rows][:, p.kind == 'delisted'].sum(1).mean():.0f})", flush=True)
    E = elig[rows][:, cols]
    A = adv[rows][:, cols]
    BIG = E & (A >= BIG_ADV)
    cost2 = (2 * cost_side(A)).astype("float32")
    ao = p.op * p.adj / p.cl
    acf = p.df(p.adj).ffill().to_numpy()
    spy = p.col["SPY"]
    years = cal[rows].year.values

    # forward returns per template, on train rows
    tmpl = {}
    for kind, hs in (("oc", H_OC), ("cc", H_CC)):
        for h in hs:
            num = acf[s0 + h:s1 + h + 1]
            den = ao[s0 + 1:s1 + 2] if kind == "oc" else p.adj[rows]
            R = np.clip(num[:, cols] / den[:, cols] - 1, -1, 5).astype("float32")
            spy_r = (num[:, spy] / den[:, spy] - 1).astype("float32")
            V = E & np.isfinite(R)
            Rz = np.where(V, R, 0).astype("float32")
            net = R - cost2
            tmpl[(kind, h)] = {"V": V, "Rz": Rz, "Cz": np.where(V, cost2, 0).astype("float32"),
                               "WA": V & (net > 0), "WS": V & (net > spy_r[:, None]), "spy": spy_r}
    print("templates ready", flush=True)

    out = []

    def score(name, side, tier, M):
        for (kind, h), t in tmpl.items():
            V = t["V"] & (BIG if tier == "big" else E)
            m = M & V
            cnt, ucnt = m.sum(1), V.sum(1)
            ok = (cnt >= 5) & (ucnt >= 50)
            if ok.sum() < 250:
                continue
            c_, u_ = cnt[ok], ucnt[ok]
            dec_r = (t["Rz"] * m).sum(1)[ok] / c_
            uni_r = (t["Rz"] * V).sum(1)[ok] / u_
            sel = dec_r - uni_r
            net = dec_r - (t["Cz"] * m).sum(1)[ok] / c_
            y = years[ok]
            ymean = pd.Series(sel).groupby(y).mean()
            out.append({
                "feature": name, "side": side, "tier": tier, "entry": kind, "h": h, "days": int(ok.sum()),
                "names_per_day": float(c_.mean()),
                "sel": float(sel.mean()), "sel_t": float(sel.mean() / (sel.std() / np.sqrt(len(sel) / h))),
                "years_same_sign": float((np.sign(ymean) == np.sign(sel.mean())).mean()),
                "net_abs": float(net.mean()), "net_vs_spy": float((net - t["spy"][ok]).mean()),
                "win_abs": float(((t["WA"] & m).sum(1)[ok] / c_).mean()),
                "base_win_abs": float(((t["WA"] & V).sum(1)[ok] / u_).mean()),
                "win_spy": float(((t["WS"] & m).sum(1)[ok] / c_).mean()),
                "base_win_spy": float(((t["WS"] & V).sum(1)[ok] / u_).mean())})

    del acf, ao, adv, hist, elig
    sub = lambda a: pd.DataFrame(a[:, cols], index=cal)  # only stocks that are ever eligible in train
    op, hi, lo, c, rc, dvs = (sub(getattr(p, f)) for f in ("op", "hi", "lo", "cl", "rc", "dv"))
    del dv_df
    for name, fr in feature_frames(op, hi, lo, c, dvs, rc):
        x = fr.to_numpy()[rows]
        del fr
        for tier, U in (("all", E), ("big", BIG)):
            pct = pd.DataFrame(np.where(U, x, np.nan)).rank(axis=1, pct=True).to_numpy()
            score(name, "top", tier, pct > 0.9)
            score(name, "bottom", tier, pct <= 0.1)
        print(name, flush=True)
    st = streak(c).to_numpy()[rows]
    for nm, M in (("down_streak>=3", st <= -3), ("down_streak>=4", st <= -4), ("down_streak>=5", st <= -5),
                  ("up_streak>=3", st >= 3), ("up_streak>=5", st >= 5)):
        for tier in ("all", "big"):
            score(nm, "is", tier, M)
    res = pd.DataFrame(out)
    res["lift_abs"] = res.win_abs - res.base_win_abs
    res["lift_spy"] = res.win_spy - res.base_win_spy
    res.to_csv(BS / "map_train.csv", index=False)
    print(f"map_train.csv rows {len(res)}", flush=True)


if __name__ == "__main__":
    main()
