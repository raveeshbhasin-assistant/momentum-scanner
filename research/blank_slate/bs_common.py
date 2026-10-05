"""Blank-slate strategy search: one daily panel for everything (2026-listed stocks, delisted
stocks from FMP, ETFs) and the train / holdout split.

  python research/blank_slate/bs_common.py build     build the panel cache (prints coverage)
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whale_flow"))
from wf_common import WF, _finish, load_survivor, norm_symbol  # noqa: E402

BS = Path("C:/dev/Trader-v3-data/blank_slate")
CACHE = BS / "panel.npz"
TRAIN = ("2006-01-03", "2019-09-30")     # signal days; mine freely
HOLD = ("2020-01-02", "2026-06-30")      # signal days; touched only by the frozen strategies
PX_TOL, MATCH_RATE = 0.05, 0.70
FIELDS = ("op", "hi", "lo", "cl", "adj", "rc", "dv")


def cost_side(adv):
    return np.where(adv >= 100e6, 0.0005, np.where(adv >= 50e6, 0.0010, 0.0025))


def load_etf(sym):
    p = BS / "etf" / f"{sym}.csv"
    df = pd.read_csv(p, parse_dates=["date"]).set_index("date").sort_index()
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close",
                            "Adj Close": "adj", "Volume": "volume", "Stock Splits": "split"})
    df = df[df.close > 0]
    return _finish(df, df.split.where(df.split > 0, 1.0))


def _delisted_meta():
    out = {}
    p = BS / "delisted_full_meta.jsonl"
    if p.exists():
        with open(p) as f:
            for l in f:
                if l.strip():
                    d = json.loads(l)
                    out[d["key"]] = d
    return out


def load_delisted_full(key, meta):
    """FMP full history for `{cik}_{SYMBOL}`, as-traded columns rebuilt from its split list.
    A dividend-adjusted close of 0 is replaced by carrying the last valid adjustment factor."""
    p = BS / "bars_delisted_full" / f"{key}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=["date"]).set_index("date").sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")  # a few files carry text in a price column
    df = df[(df.close > 0) & (df.open > 0) & (df.high > 0) & (df.low > 0)]
    df = df[~df.index.duplicated()]
    if len(df) < 60:
        return None
    adj = df["adjClose"] if "adjClose" in df.columns else df.close
    factor = (adj / df.close).where(adj > 0).ffill().bfill().fillna(1.0)
    df["adj"] = df.close * factor
    ratio = pd.Series(1.0, index=df.index)
    for d, num, den in meta.get(key, {}).get("splits", []):
        d = pd.Timestamp(d)
        if den and df.index[0] < d <= df.index[-1]:
            ratio.iloc[df.index.searchsorted(d)] *= num / den
    return _finish(df, ratio)


def validated_delisted(survivor_close):
    """Delisted series worth trusting: Form 4 trade prices agree with the bars, cut to the
    stretches of history that contain an agreeing trade (a symbol can be reused later), and not
    a duplicate of a 2026-listed series. Returns {key: DataFrame} and a QA table."""
    meta = _delisted_meta()
    t = pd.read_pickle(WF / "insider_trans.pkl")
    t = t[t.ticker.isna() & t.cik.notna() & t.code.isin(["P", "S"]) & (t.price > 0) & t.trans_date.notna()]
    t = t.assign(key=t.cik.astype(int).astype(str) + "_" + norm_symbol(t.symbol_on_form))
    trades = {k: g[["trans_date", "price"]] for k, g in t.groupby("key")}
    out, qa, by_symbol = {}, [], {}
    for key, m in meta.items():
        if m["n"] == 0:
            continue
        b = load_delisted_full(key, meta)
        why = ""
        if b is None:
            why = "too short"
        else:
            g = trades.get(key)
            if g is None:
                why = "no trades to check"
            else:
                g = g.join(b[["raw_low", "raw_high"]], on="trans_date").dropna()
                ok = (g.price >= g.raw_low * (1 - PX_TOL)) & (g.price <= g.raw_high * (1 + PX_TOL))
                if len(g) < 2 or ok.mean() < MATCH_RATE:
                    why = f"price check {int(ok.sum())}/{len(g)}"
                else:
                    # split the history at gaps of more than 60 days; keep stretches holding an agreeing trade
                    seg = (b.index.to_series().diff().dt.days > 60).cumsum()
                    good = set(seg.reindex(g.trans_date[ok.values]).dropna().unique())
                    b = b[seg.isin(good).values]
                    sym = key.split("_", 1)[1]
                    sc = survivor_close.get(sym)
                    if sc is not None:
                        both = pd.concat([b.close, sc], axis=1, join="inner").dropna()
                        if len(both) >= 60 and (both.iloc[:, 0] / both.iloc[:, 1] - 1).abs().median() < 0.02:
                            why = "duplicate of 2026-listed"
                    if not why and len(b) < 60:
                        why = "too short after cut"
                    if not why:
                        prev = by_symbol.get(sym)
                        if prev is not None and len(out[prev]) >= len(b):
                            why = "duplicate symbol"
                        else:
                            if prev is not None:
                                del out[prev]
                            by_symbol[sym] = key
                            out[key] = b
        qa.append((key, m["n"], why or "ok"))
    return out, pd.DataFrame(qa, columns=["key", "rows", "status"])


def build():
    BS.mkdir(exist_ok=True)
    cal = load_survivor("SPY").index
    cal = cal[cal >= "2004-01-01"]
    series, kind = {}, {}
    files = sorted((WF / "bars").glob("*.csv"))
    surv_close = {}
    for p in files:
        b = load_survivor(p.stem)
        if b is not None and len(b) >= 60:
            series[p.stem], kind[p.stem] = b, "stock"
            surv_close[p.stem] = b.close
    for p in sorted((BS / "etf").glob("*.csv")):
        if p.stem != "SPY":
            series["ETF:" + p.stem], kind["ETF:" + p.stem] = load_etf(p.stem), "etf"
    kind["SPY"] = "etf"
    dl, qa = validated_delisted(surv_close)
    qa.to_csv(BS / "delisted_qa.csv", index=False)
    print(qa.status.str.replace(r"\d+/\d+", "", regex=True).value_counts().to_string(), flush=True)
    for k, b in dl.items():
        series[k], kind[k] = b, "delisted"
    keys = list(series)
    T, N = len(cal), len(keys)
    arr = {f: np.full((T, N), np.nan, "float32") for f in FIELDS}
    for j, k in enumerate(keys):
        b = series[k].reindex(cal)
        arr["op"][:, j], arr["hi"][:, j], arr["lo"][:, j], arr["cl"][:, j] = b.open, b.high, b.low, b.close
        arr["adj"][:, j], arr["rc"][:, j], arr["dv"][:, j] = b.adj, b.raw_close, b.close * b.volume
    np.savez(CACHE, cal=cal.values, keys=np.array(keys), kind=np.array([kind[k] for k in keys]), **arr)
    kinds = pd.Series([kind[k] for k in keys]).value_counts().to_dict()
    print(f"panel {T} sessions x {N} series {kinds}", flush=True)
    # how many names are tradeable each year, by kind
    adv = pd.DataFrame(arr["dv"]).rolling(20, min_periods=15).mean().to_numpy()
    el = (arr["rc"] >= 5) & (adv >= 10e6)
    kd = np.array([kind[k] for k in keys])
    yr = pd.DatetimeIndex(cal).year
    rows = [(y, int(el[yr == y][:, kd == "stock"].sum(1).mean()), int(el[yr == y][:, kd == "delisted"].sum(1).mean()))
            for y in range(2005, 2027)]
    print(pd.DataFrame(rows, columns=["year", "listed_2026", "delisted"]).assign(
        delisted_share=lambda d: (d.delisted / (d.listed_2026 + d.delisted)).round(3)).to_string(index=False), flush=True)


class Panel:
    def __init__(self):
        z = np.load(CACHE, allow_pickle=True)
        self.cal = pd.DatetimeIndex(z["cal"])
        self.keys, self.kind = list(z["keys"]), z["kind"]
        for f in FIELDS:
            setattr(self, f, z[f])
        self.col = {k: j for j, k in enumerate(self.keys)}
        self.T, self.N = self.cl.shape

    def df(self, a):
        return pd.DataFrame(a, index=self.cal, columns=self.keys)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build()
    else:
        sys.exit(__doc__)
