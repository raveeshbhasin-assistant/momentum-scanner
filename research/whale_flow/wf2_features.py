"""WF2 feature tables (WF2_DESIGN.md). Every feature is known by the close of the signal day s;
labels start at the open of s+1.

Output (C:/dev/Trader-v3-data/whale_flow/wf2/):
  cand_train.pkl / cand_holdout.pkl   one row per company-day with a qualifying insider purchase
  week_train.pkl / week_holdout.pkl   every eligible 2026-listed stock, every 5th session
The mining code reads only the *_train files.

  python research/whale_flow/wf2_features.py
"""
import json

import numpy as np
import pandas as pd

import wf1_run as w
from wf2_common import DayBase, _load_delisted_fixed as load_delisted
from wf_common import WF

OUT = WF / "wf2"
TRAIN = ("2006-02-01", "2019-09-30")
HOLD = ("2020-01-01", "2026-04-01")
H = (21, 63)


# ------------------------------------------------------------------ price features
def price_frames(c, rc, dv):
    """Yield (name, DataFrame) one at a time; all windows end at the row's own day."""
    for n in (1, 5, 20, 63, 126, 252):
        yield f"p_r{n}", c / c.shift(n) - 1
    yield "p_dd_high", c / c.rolling(252, min_periods=200).max() - 1
    yield "p_up_low", c / c.rolling(252, min_periods=200).min() - 1
    yield "p_sma50", c / c.rolling(50, min_periods=50).mean() - 1
    yield "p_sma200", c / c.rolling(200, min_periods=200).mean() - 1
    ret = c / c.shift(1) - 1
    yield "p_vol20", ret.rolling(20, min_periods=15).std()
    yield "p_vol63", ret.rolling(63, min_periods=40).std()
    adv = dv.rolling(20, min_periods=15).mean()
    yield "p_log_adv", np.log(adv.where(adv > 0))
    yield "p_surge1", dv / adv
    yield "p_surge5", dv.rolling(5, min_periods=4).mean() / dv.rolling(63, min_periods=40).mean()
    yield "p_log_px", np.log(rc.where(rc > 0))


def market_frame(spy_close):
    c = spy_close
    ret = c / c.shift(1) - 1
    return pd.DataFrame({"m_r20": c / c.shift(20) - 1, "m_r63": c / c.shift(63) - 1,
                         "m_sma200": c / c.rolling(200).mean() - 1, "m_vol20": ret.rolling(20).std()})


# ------------------------------------------------------------------ insider and 8-K history
class Trail:
    """Per-company filing history, queried for trailing windows that end at session s0 (inclusive)."""

    def __init__(self, buys, sells, k8, cal):
        self.b = {c: g for c, g in buys.groupby("cik")}
        self.s = {c: (g.s.values, g.owner_cik.values, g.value.values) for c, g in sells.groupby("cik")}
        k8 = k8.assign(ks=cal.searchsorted(k8.date.values, side="left")).sort_values("ks")
        it = k8["items"].fillna("")
        for nm, pat in (("earn", "2.02"), ("i101", "1.01"), ("i302", "3.02"), ("i502", "5.02"),
                        ("i205", r"2\.05|2\.06"), ("i801", r"8\.01|7\.01")):
            k8[nm] = it.str.contains(pat, regex=True)
        self.k = {c: g for c, g in k8.groupby("cik")}
        self._bc, self._kc = {}, {}

    def _buys(self, cik):
        if cik not in self._bc:
            g = self.b.get(cik)
            self._bc[cik] = None if g is None else {
                "s": g.s.values, "who": g.who.values, "val": g.value.values,
                "cs": np.concatenate([[0.0], np.cumsum(g.value.values)]),
                "top": (g.is_ceo | g.is_cfo).values, "chair": g.is_chair.values, "off": g.is_officer.values,
                "ten": g.is_tenpct.values, "pct": g.pct_increase.values, "lag": g.lag_days.values}
        return self._bc[cik]

    def _k8(self, cik):
        if cik not in self._kc:
            g = self.k.get(cik)
            self._kc[cik] = None if g is None else {
                "s": g.ks.values, "earn": g.ks.values[g.earn.values],
                **{n: g.ks.values[g[n].values] for n in ("i101", "i302", "i502", "i205", "i801")}}
        return self._kc[cik]

    def query(self, cik, s0):
        """Features for one company at each session in the sorted array s0."""
        n = len(s0)
        out = {k: np.zeros(n, "float32") for k in (
            "ib_n30", "ib_val30", "ib_max30", "ib_n90", "ib_val90", "ib_n252", "ib_top30", "ib_chair30",
            "ib_off30", "ib_ten30", "ib_pct30", "ib_new30", "is_n90", "is_val90", "is_n252",
            "k_n20", "k_earn5", "k_i101", "k_i302", "k_i502", "k_i205", "k_i801")}
        out["ib_gap_prev"] = np.full(n, 1000, "float32")
        out["ib_since"] = np.full(n, 1000, "float32")
        out["k_since_earn"] = np.full(n, 250, "float32")
        b = self._buys(cik)
        if b is not None:
            fs = b["s"]
            hi = np.searchsorted(fs, s0, side="right")
            lo30, lo90, lo252 = (np.searchsorted(fs, s0 - d, side="right") for d in (21, 63, 252))
            out["ib_val30"], out["ib_val90"] = b["cs"][hi] - b["cs"][lo30], b["cs"][hi] - b["cs"][lo90]
            out["ib_since"] = np.where(hi > 0, s0 - fs[np.maximum(hi - 1, 0)], 1000).clip(0, 1000)
            out["ib_gap_prev"] = np.where(lo30 > 0, s0 - fs[np.maximum(lo30 - 1, 0)], 1000).clip(0, 1000)
            for i in np.flatnonzero(hi > lo252):
                out["ib_n252"][i] = len(set(b["who"][lo252[i]:hi[i]]))
                if hi[i] > lo90[i]:
                    out["ib_n90"][i] = len(set(b["who"][lo90[i]:hi[i]]))
                if hi[i] > lo30[i]:
                    sl = slice(lo30[i], hi[i])
                    out["ib_n30"][i] = len(set(b["who"][sl]))
                    out["ib_max30"][i] = b["val"][sl].max()
                    out["ib_top30"][i], out["ib_chair30"][i] = b["top"][sl].any(), b["chair"][sl].any()
                    out["ib_off30"][i], out["ib_ten30"][i] = b["off"][sl].any(), b["ten"][sl].any()
                    pct = b["pct"][sl]
                    out["ib_new30"][i] = np.isinf(pct).any()
                    fin = pct[np.isfinite(pct)]
                    out["ib_pct30"][i] = min(fin.max(), 5.0) if len(fin) else 0.0
        if cik in self.s:
            fs, who, val = self.s[cik]
            hi = np.searchsorted(fs, s0, side="right")
            lo90, lo252 = np.searchsorted(fs, s0 - 63, side="right"), np.searchsorted(fs, s0 - 252, side="right")
            for i in np.flatnonzero(hi > lo252):
                out["is_n252"][i] = len(set(who[lo252[i]:hi[i]]))
                if hi[i] > lo90[i]:
                    out["is_n90"][i] = len(set(who[lo90[i]:hi[i]]))
                    out["is_val90"][i] = val[lo90[i]:hi[i]].sum()
        k = self._k8(cik)
        if k is not None:
            cnt = lambda a, d: np.searchsorted(a, s0, side="right") - np.searchsorted(a, s0 - d, side="right")
            out["k_n20"] = cnt(k["s"], 20)
            out["k_earn5"] = cnt(k["earn"], 5) > 0
            for nm in ("i101", "i302", "i502", "i205", "i801"):
                out["k_" + nm] = cnt(k[nm], 20) > 0
            e = k["earn"]
            if len(e):
                hi = np.searchsorted(e, s0, side="right")
                out["k_since_earn"] = np.where(hi > 0, s0 - e[np.maximum(hi - 1, 0)], 250).clip(0, 250)
        return {a: np.asarray(v, "float32") for a, v in out.items()}


def sell_filings(cal):
    """Officer/director open-market sales, one row per form."""
    t = pd.read_pickle(WF / "insider_trans.pkl")
    t = t[(t.code == "S") & (t.form == "4") & (t.is_officer | t.is_director) & t.cik.notna()
          & (t.price > 0) & (t.shares > 0)]
    v = (t.shares * t.price).clip(upper=5e8)
    g = (t.assign(value=v).groupby("accession")
         .agg(cik=("cik", "first"), owner_cik=("owner_cik", "first"), filing_date=("filing_date", "first"),
              value=("value", "sum")).reset_index())
    g["cik"] = g.cik.astype(int)
    g["s"] = cal.searchsorted(g.filing_date.values, side="right") - 1
    return g.sort_values(["cik", "s"])


# ------------------------------------------------------------------ build
def main():
    OUT.mkdir(exist_ok=True)
    p = w.Panel()
    cal = p.cal
    z = np.load(WF / "panel_cache.npz", allow_pickle=True)
    df = lambda a: pd.DataFrame(a, index=cal, columns=p.tick)
    c, rc, dv = df(z["cl"]), df(p.rc), df(p.dv)
    spy = p.col["SPY"]
    mkt = market_frame(c.iloc[:, spy].astype("float64"))
    dz = w.dark_z(p)

    f = pd.read_pickle(WF / "insider_purchases.pkl")
    f = f.assign(s=cal.searchsorted(f.filing_date.values, side="right") - 1)
    q = w.qualifying(f)
    q = q.assign(who=np.where(q.n_owners == 1, q.owner_cik.astype(str), "JOINT")).sort_values(["cik", "s"])
    sells = sell_filings(cal)
    k8 = pd.read_pickle(WF / "news8k_all.pkl")
    trail = Trail(q, sells, k8, cal)
    print(f"qualifying purchases {len(q):,}; sale filings {len(sells):,}; 8-K {len(k8):,}", flush=True)

    # ---------------- insider candidates: one row per company-day with a qualifying purchase
    g = q.groupby(["key", "s"])
    cand = g.agg(cik=("cik", "first"), delisted=("delisted", "first"), filing_date=("filing_date", "max"),
                 has_series=("has_series", "first"), series_ok=("series_ok", "all"),
                 day_value=("value", "sum"), day_shares=("shares", "sum"),
                 ib_lag=("lag_days", "min")).reset_index()
    cand["form_px"] = cand.day_value / cand.day_shares
    lo, hi = pd.Timestamp(TRAIN[0]), pd.Timestamp(HOLD[1])
    ent = pd.Series(cal[np.minimum(cand.s.values + 1, len(cal) - 1)], index=cand.index)
    cand = cand[(ent >= lo) & (ent <= hi) & (cand.s >= 0) & (cand.s + 1 < len(cal))]
    rng = np.random.default_rng(w.SEED)
    blocked = np.zeros(p.elig.shape, bool)  # a company is not a control for 63 sessions after its own buy day
    for key, s in zip(cand.key, cand.s):
        j = p.col.get(key)
        if j is not None:
            blocked[s:s + 64, j] = True
    ev = w.evaluate(p, cand, blocked, rng)
    print("candidates", len(ev), ev.status.value_counts().to_dict(), flush=True)
    ev = ev[ev.status == "tradeable"].merge(cand[["key", "s", "day_value", "ib_lag"]], on=["key", "s"])
    ev = ev.reset_index(drop=True)

    feats = {}
    surv = ~ev.delisted.values.astype(bool)
    jj = np.array([p.col.get(k, -1) for k in ev.key])
    ss = ev.s.values
    # ---------------- weekly panel index (2026-listed stocks, every 5th session)
    s_lo = cal.searchsorted(lo) - 1
    s_hi = min(cal.searchsorted(hi, side="right") - 2, len(cal) - max(H) - 2)
    S = np.arange(s_lo, s_hi + 1, 5)
    okm = p.elig[S] & np.isfinite(p.ao[S + 1])
    wi, wj = np.nonzero(okm)
    ws = S[wi]
    wk = {"s": ws, "j": wj}
    for name, fr in price_frames(c, rc, dv):
        a = fr.to_numpy()
        feats[name] = np.where(surv, a[ss, np.maximum(jj, 0)], np.nan).astype("float32")
        wk[name] = a[ws, wj].astype("float32")
        del fr, a
    feats["d_z"] = np.where(surv, dz[ss, np.maximum(jj, 0)], np.nan).astype("float32")
    wk["d_z"] = dz[ws, wj]
    # delisted candidates: the same features from the company's own bars
    dl_pos = np.flatnonzero(~surv)
    for key, idx in ev[~surv].groupby("key").indices.items():
        b = load_delisted(key).reindex(cal)
        one = lambda x: x.to_frame("x")
        rows = dl_pos[idx]
        for name, fr in price_frames(one(b.close), one(b.raw_close), one(b.close * b.volume)):
            feats[name][rows] = fr.to_numpy()[ss[rows], 0]
    for name in mkt.columns:
        feats[name] = mkt[name].to_numpy()[ss].astype("float32")
        wk[name] = mkt[name].to_numpy()[ws].astype("float32")

    # insider / 8-K trailing features
    def add_trail(ciks, sarr, store, n):
        cols = None
        order = np.argsort(ciks, kind="stable")
        bounds = np.flatnonzero(np.diff(ciks[order])) + 1
        for grp in np.split(order, bounds):
            cik = ciks[grp[0]]
            o = grp[np.argsort(sarr[grp], kind="stable")]
            r = trail.query(int(cik), sarr[o]) if cik >= 0 else None
            if cols is None and r is not None:
                cols = list(r)
                for k in cols:
                    store[k] = np.full(n, np.nan, "float32")
            if r is not None:
                for k in cols:
                    store[k][o] = r[k]
    add_trail(ev.cik.values.astype(np.int64), ss, feats, len(ev))
    ct = json.loads((WF / "company_tickers.json").read_text())
    t2c = {}
    for row in ct.values():
        t2c.setdefault(row["ticker"].upper(), int(row["cik_str"]))
    col_cik = np.array([t2c.get(t, -1) for t in p.tick], dtype=np.int64)
    add_trail(col_cik[wj], ws, wk, len(ws))
    print("trailing features done", flush=True)

    X = pd.DataFrame(feats)
    out = pd.concat([ev, X], axis=1)
    out["ib_val30_adv"] = out.ib_val30 / out.adv
    out["ib_day_adv"] = out.day_value / out.adv
    raw_close = np.exp(out.p_log_px)
    out["ib_px_vs_insider"] = raw_close / out.form_px - 1
    out["log_val30"] = np.log1p(out.ib_val30)
    out["net_buy90"] = np.log1p(out.ib_val90) - np.log1p(out.is_val90)
    out["entry"] = pd.to_datetime(out.entry)
    n0 = len(out)
    out = out[np.isfinite(out.exc21) & np.isfinite(out.exc63)].reset_index(drop=True)
    print("candidate rows dropped for a non-finite return:", n0 - len(out), flush=True)
    db = DayBase(p)  # same-day success rate and mean excess of every eligible 2026-listed stock
    for h in H:
        out[f"y{h}"] = (out[f"exc{h}"] > 0).astype("int8")
        b = [db.get(int(s), h) for s in out.s.values]
        out[f"base_win{h}"], out[f"base_exc{h}"] = [x[0] for x in b], [x[1] for x in b]
    tr = out[(out.entry >= TRAIN[0]) & (out.entry <= TRAIN[1])]
    ho = out[(out.entry >= HOLD[0]) & (out.entry <= HOLD[1])]
    tr.to_pickle(OUT / "cand_train.pkl")
    ho.to_pickle(OUT / "cand_holdout.pkl")
    print(f"insider candidates: train {len(tr):,}  holdout {len(ho):,}  features {X.shape[1] + 5}", flush=True)

    # weekly panel labels
    W = pd.DataFrame(wk)
    W["key"] = np.array(p.tick)[wj]
    W["entry"] = cal[ws + 1]
    W["adv"] = p.adv[ws, wj]
    cost = 2 * w.cost_side(W.adv.values)
    for h in H:
        ret, spy_r = p.fwd(ws + 1, wj, h)
        W[f"exc{h}"] = (ret - spy_r - cost).astype("float32")
    W = W[np.isfinite(W.exc21) & np.isfinite(W.exc63)].reset_index(drop=True)
    for h in H:
        W[f"y{h}"] = (W[f"exc{h}"] > 0).astype("int8")
        W[f"base_win{h}"] = W.groupby("s")[f"y{h}"].transform("mean").astype("float32")
        W[f"base_exc{h}"] = W.groupby("s")[f"exc{h}"].transform("mean").astype("float32")
    W["ib_val30_adv"] = W.ib_val30 / W.adv
    W["net_buy90"] = np.log1p(W.ib_val90) - np.log1p(W.is_val90)
    W["log_val30"] = np.log1p(W.ib_val30)
    trw = W[(W.entry >= TRAIN[0]) & (W.entry <= TRAIN[1])]
    how = W[(W.entry >= HOLD[0]) & (W.entry <= HOLD[1])]
    trw.to_pickle(OUT / "week_train.pkl")
    how.to_pickle(OUT / "week_holdout.pkl")
    print(f"weekly panel: train {len(trw):,}  holdout {len(how):,}", flush=True)


if __name__ == "__main__":
    main()
