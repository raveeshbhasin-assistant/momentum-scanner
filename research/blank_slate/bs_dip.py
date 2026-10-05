"""ETF "dip in an uptrend" strategies: trade list and portfolio simulation.

  python research/blank_slate/bs_dip.py train        small declared grid on the training years
  python research/blank_slate/bs_dip.py freeze       hash the design, this file and the frozen specs
  python research/blank_slate/bs_dip.py holdout      frozen strategies on 2020-01-02 .. 2026-06-30, once
"""
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from bs_common import BS, HOLD, TRAIN, load_etf
from bs_map import rsi

HERE = Path(__file__).parent
INDEX = "SPY QQQ IWM DIA MDY IJR VTI RSP".split()
SECTOR = "XLB XLE XLF XLI XLK XLP XLU XLV XLY SMH XBI IBB KRE XHB XRT XME ITB IYT VNQ".split()
UNIVERSES = {"index": INDEX, "sector": SECTOR, "both": INDEX + SECTOR, "core3": ["SPY", "QQQ", "IWM"]}
COST = 0.0002          # per side
SPECS = HERE / "dip_frozen.json"
LOCK = HERE / "dip_frozen_sha256.txt"
RESULT = BS / "dip_holdout_report.csv"


def load(universe):
    out = {}
    for s in UNIVERSES[universe]:
        b = load_etf(s)
        c = b.close
        ret = c / c.shift(1) - 1
        sma20, sd20 = c.rolling(20).mean(), c.rolling(20).std()
        out[s] = pd.DataFrame({
            "adj": b.adj, "ao": b.open * b.adj / b.close, "up": c > c.rolling(200).mean(),
            "z20": (c - sma20) / sd20, "r5": c / c.shift(5) - 1, "rsi2": rsi(c.to_frame("x"), 2).x,
            "low10": c <= c.rolling(10).min(), "vol20": ret.rolling(20).std()})
    return out


def trigger(d, name):
    if name == "z20<=-2":
        return d.z20 <= -2
    if name == "r5<=-3%":
        return d.r5 <= -0.03
    if name == "low10":
        return d.low10
    if name == "rsi2<10":
        return d.rsi2 < 10
    raise ValueError(name)


def simulate(spec, window):
    """Trades and a daily equity curve. One position per ETF; at most `slots` positions, each
    1/slots of equity at entry; when signals exceed free slots the most oversold (lowest z20) go first."""
    data = load(spec["universe"])
    cal = data["SPY"].index if "SPY" in data else next(iter(data.values())).index
    lo, hi = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    h, K, entry = spec["hold"], spec["slots"], spec["entry"]
    sig = {s: (trigger(d, spec["trigger"]) & (d.up if spec["uptrend"] else True)).reindex(cal).fillna(False).to_numpy()
           for s, d in data.items()}
    adj = {s: d.adj.reindex(cal).to_numpy() for s, d in data.items()}
    ao = {s: d.ao.reindex(cal).to_numpy() for s, d in data.items()}
    z = {s: d.z20.reindex(cal).to_numpy() for s, d in data.items()}
    i0, i1 = cal.searchsorted(lo), cal.searchsorted(hi, side="right") - 1
    open_pos, trades = {}, []          # sym -> (entry_i, exit_i, entry_px, weight)
    eq, cash_w = 1.0, 1.0
    curve = []
    prev_val = {}
    for i in range(i0, min(i1 + h + 2, len(cal))):
        # mark to market
        day_ret = 0.0
        for s, (ei, xi, px, w) in list(open_pos.items()):
            ref = prev_val[s]
            now = adj[s][i]
            if np.isfinite(now):
                day_ret += w * (now / ref - 1)
                prev_val[s] = now
        eq *= 1 + day_ret
        # exits at today's close
        for s, (ei, xi, px, w) in list(open_pos.items()):
            if i >= xi:
                r = adj[s][i] / px - 1 - 2 * COST
                trades.append({"etf": s, "signal": cal[ei if entry == "cc" else ei - 1], "entry": cal[ei],
                               "exit": cal[i], "ret": r})
                eq *= 1 - w * 2 * COST
                del open_pos[s]
        curve.append((cal[i], eq, len(open_pos) / K))
        if i > i1:
            continue
        # entries: signal at today's close
        cands = [s for s in data if sig[s][i] and s not in open_pos and np.isfinite(adj[s][i])]
        cands.sort(key=lambda s: z[s][i] if np.isfinite(z[s][i]) else 0.0)
        for s in cands[:max(K - len(open_pos), 0)]:
            if entry == "cc":
                open_pos[s] = (i, i + h, adj[s][i], 1.0 / K)
                prev_val[s] = adj[s][i]
            elif i + 1 < len(cal) and np.isfinite(ao[s][i + 1]):
                # next open: the position starts earning from tomorrow's open
                open_pos[s] = (i + 1, i + h, ao[s][i + 1], 1.0 / K)
                prev_val[s] = ao[s][i + 1]
    tr = pd.DataFrame(trades)
    cv = pd.DataFrame(curve, columns=["date", "equity", "exposure"]).set_index("date")
    return tr, cv, data


def base_rates(data, h, entry, window):
    """Unconditional record of the same ETFs for the same holding period, any day in the window."""
    lo, hi = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    out = {}
    for s, d in data.items():
        r = (d.adj.shift(-h) / d.adj - 1) if entry == "cc" else (d.adj.shift(-h) / d.ao.shift(-1) - 1)
        r = (r - 2 * COST)[(d.index >= lo) & (d.index <= hi)].dropna()
        out[s] = (float((r > 0).mean()), float(r.mean()))
    return out


def boot(x, clusters, rng, B=10000):
    g = pd.DataFrame({"x": np.asarray(x, float), "c": np.asarray(clusters)}).groupby("c").x.agg(["sum", "count"])
    if len(g) < 3:
        return np.nan, np.nan, np.nan
    idx = rng.integers(0, len(g), (B, len(g)))
    m = g["sum"].values[idx].sum(1) / g["count"].values[idx].sum(1)
    return float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5)), float((m <= 0).mean())


def report(spec, window, rng):
    tr, cv, data = simulate(spec, window)
    if len(tr) == 0:
        return {"name": spec["name"], "trades": 0}, tr, cv
    base = base_rates(data, spec["hold"], spec["entry"], window)
    tr["base_win"] = tr.etf.map(lambda s: base[s][0])
    tr["base_mean"] = tr.etf.map(lambda s: base[s][1])
    tr["win"] = (tr.ret > 0).astype(float)
    sig = pd.to_datetime(tr.signal)
    years = (pd.Timestamp(window[1]) - pd.Timestamp(window[0])).days / 365.25
    # signals cluster in market-wide pullbacks: resample whole months and whole weeks, keep the wider
    res = {}
    for nm, x in (("lift", tr.win - tr.base_win), ("edge", tr.ret - tr.base_mean), ("ret", tr.ret)):
        a = boot(x, sig.dt.to_period("M").astype(str), rng)
        b = boot(x, sig.dt.to_period("W").astype(str), rng)
        res[nm] = (min(a[0], b[0]), max(a[1], b[1]), max(a[2], b[2]))
    yr = tr.assign(y=sig.dt.year).groupby("y").agg(n=("win", "size"), win=("win", "mean"), base=("base_win", "mean"),
                                                   mean=("ret", "mean"))
    yy = yr[yr.n >= 5]
    c = cv[(cv.index >= window[0])]
    dr = c.equity.pct_change().dropna()
    cagr = c.equity.iloc[-1] ** (1 / years) - 1
    dd = (c.equity / c.equity.cummax() - 1).min()
    spy = load_etf("SPY").adj
    spy = spy[(spy.index >= window[0]) & (spy.index <= c.index[-1])]
    out = {"name": spec["name"], "trades": len(tr), "per_year": len(tr) / years,
           "episodes": int(sig.dt.to_period("W").nunique()),
           "win_rate": tr.win.mean(), "base_win": tr.base_win.mean(), "lift": (tr.win - tr.base_win).mean(),
           "lift_lo": res["lift"][0], "lift_hi": res["lift"][1], "p_lift": res["lift"][2],
           "mean_trade": tr.ret.mean(), "mean_lo": res["ret"][0], "mean_hi": res["ret"][1],
           "edge_vs_any_day": (tr.ret - tr.base_mean).mean(), "edge_lo": res["edge"][0], "p_edge": res["edge"][2],
           "median_trade": tr.ret.median(), "worst_trade": tr.ret.min(),
           "share_worse_m5": float((tr.ret <= -0.05).mean()),
           "years_win_above_base": float((yy.win > yy.base).mean()) if len(yy) else np.nan, "n_years": len(yy),
           "cagr": cagr, "max_drawdown": dd, "avg_exposure": c.exposure.mean(),
           "sharpe": dr.mean() / dr.std() * np.sqrt(252) if dr.std() > 0 else np.nan,
           "spy_cagr": (spy.iloc[-1] / spy.iloc[0]) ** (1 / years) - 1,
           "spy_max_drawdown": (spy / spy.cummax() - 1).min()}
    return out, tr, yr


GRID = [dict(universe=u, trigger=t, uptrend=up, hold=h, entry=e, slots=5)
        for u in ("both", "index", "sector", "core3") for t in ("z20<=-2", "r5<=-3%", "low10", "rsi2<10")
        for up in (True, False) for h in (5, 21) for e in ("cc", "oc")]
SHOW = ["name", "trades", "per_year", "episodes", "win_rate", "base_win", "lift", "lift_lo", "p_lift", "mean_trade",
        "edge_vs_any_day", "p_edge", "worst_trade", "years_win_above_base", "cagr", "max_drawdown", "avg_exposure",
        "sharpe"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    rng = np.random.default_rng(20261005)
    pd.set_option("display.width", 260)
    pd.set_option("display.max_columns", 40)
    pd.set_option("display.float_format", "{:.4f}".format)
    if mode == "train":
        rows = []
        for g in GRID:
            g = dict(g, name=f"{g['universe']}|{g['trigger']}|{'up' if g['uptrend'] else 'any'}|h{g['hold']}|{g['entry']}")
            rows.append(report(g, TRAIN, rng)[0])
        d = pd.DataFrame(rows)
        d.to_csv(BS / "dip_train_grid.csv", index=False)
        print(d[SHOW].to_string(index=False))
    elif mode == "freeze":
        if LOCK.exists():
            sys.exit("already frozen")
        LOCK.write_text(json.dumps({f: sha(HERE / f) for f in ("BS_DESIGN.md", "bs_dip.py", "bs_common.py",
                                                               "bs_map.py", "dip_frozen.json")}, indent=1))
        print(LOCK.read_text())
    elif mode == "holdout":
        if not LOCK.exists():
            sys.exit("freeze first")
        now = {f: sha(HERE / f) for f in json.loads(LOCK.read_text())}
        if now != json.loads(LOCK.read_text()):
            sys.exit("files changed since the freeze")
        if RESULT.exists():
            sys.exit("holdout already run: it is run once")
        specs = json.loads(SPECS.read_text())
        rows, yearly = [], []
        for sp in specs:
            r, tr, yr = report(sp, HOLD, rng)
            tr.to_csv(BS / f"dip_holdout_trades_{sp['name']}.csv", index=False)
            rows.append(r)
            yearly.append(yr.assign(strategy=sp["name"]))
        d = pd.DataFrame(rows)
        prim = d.name.isin([s["name"] for s in specs if s.get("primary")])
        p = d.loc[prim, "p_lift"].fillna(1.0).to_numpy()
        order = np.argsort(p)
        adj, run = np.empty(len(p)), 0.0
        for rank, i in enumerate(order):
            run = max(run, (len(p) - rank) * p[i])
            adj[i] = min(run, 1.0)
        d.loc[prim, "holm_p"] = adj
        verdict = {}
        for _, r in d[prim].iterrows():
            g = {"trades>=60": r.trades >= 60, "win_rate>=60%": r.win_rate >= 0.60, "lift_holm_p<0.05": r.holm_p < 0.05,
                 "mean_trade_lo>0": r.mean_lo > 0, "edge_vs_any_day_lo>0": r.edge_lo > 0,
                 "years>=60%": r.years_win_above_base >= 0.6}
            g = {k: bool(v) for k, v in g.items()}
            verdict[r["name"]] = {"verdict": "CONFIRMED" if all(g.values()) else
                                  "PARTIAL" if g["trades>=60"] and g["lift_holm_p<0.05"] else "NOT CONFIRMED", "gates": g}
        d["verdict"] = d.name.map(lambda n: verdict.get(n, {}).get("verdict", "(secondary)"))
        d.to_csv(RESULT, index=False)
        pd.concat(yearly).to_csv(BS / "dip_holdout_yearly.csv")
        (BS / "dip_holdout_verdict.json").write_text(json.dumps(verdict, indent=1))
        print(d[SHOW + ["mean_lo", "edge_lo", "spy_cagr", "spy_max_drawdown", "verdict"]].to_string(index=False))
        print(pd.concat(yearly).round(4).to_string())
        print(json.dumps(verdict, indent=1))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
