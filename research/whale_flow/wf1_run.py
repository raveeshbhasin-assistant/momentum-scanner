"""WF1 engine: the six registered Whale Flow cells (PREREG_WF1.md). Every rule lives here and
is hash-locked with the registration before any real window is run.

  python research/whale_flow/wf1_run.py --selftest          random names on the event dates (no real outcome)
  python research/whale_flow/wf1_run.py --counts WINDOW     event funnel and arm sizes only; no return is written
  python research/whale_flow/wf1_run.py --window DISC       2014-01-01 .. 2019-09-30 (may be looked at)
  python research/whale_flow/wf1_run.py --window CONF_A     2020-01-01 .. 2026-04-01 (run once)
  python research/whale_flow/wf1_run.py --window CONF_B     2006-02-01 .. 2013-09-30 (run once)
  python research/whale_flow/wf1_run.py --verdict           gates + GREEN/AMBER/RED from the CONF outputs
"""
import argparse
import json
import sys

import numpy as np
import pandas as pd

from wf_common import WF, load_delisted, load_survivor, trading_calendar

OUT = WF / "out"
SEED = 20261003
B = 10_000
# DISC and CONF_B stop a holding period before the next window opens, so no return day is shared
WINDOWS = {"DISC": ("2014-01-01", "2019-09-30"), "CONF_A": ("2020-01-01", "2026-04-01"),
           "CONF_B": ("2006-02-01", "2013-09-30")}
H_MAIN, H_SHORT, H_DARK = 63, 21, 21
MIN_PRICE, MIN_ADV = 5.0, 10e6
MATCH_RATE, MATCH_N = 0.70, 5   # point-in-time agreement of Form 4 trade prices with the bars
N_CONTROLS = 5
DROP = -0.15                    # "fell >= 15% over the prior 20 sessions"


def cost_side(adv):
    return np.where(adv >= 100e6, 0.0005, np.where(adv >= 50e6, 0.0010, 0.0025))


# ------------------------------------------------------------------ panel
def features(c, rc, dv):
    """Per-day state from data through that day's close. Inputs are DataFrames (T x N)."""
    adv = dv.rolling(20, min_periods=15).mean()
    sma = c.rolling(200, min_periods=200).mean()
    mx = c.rolling(252, min_periods=200).max()
    elig = (rc >= MIN_PRICE) & (adv >= MIN_ADV) & sma.notna()
    up, down = (c > sma) & (c >= 0.85 * mx), (c < sma) & (c <= 0.75 * mx)
    trend = np.where(up.values, 1, np.where(down.values, -1, 0)).astype("int8")
    return adv.to_numpy(copy=True), elig.to_numpy(copy=True), trend, (c / c.shift(20) - 1).to_numpy(copy=True)


class Panel:
    def __init__(self):
        cache = WF / "panel_cache.npz"
        self.cal = trading_calendar()
        files = sorted((WF / "bars").glob("*.csv"))
        sig = f"{len(files)}|{max(p.stat().st_mtime_ns for p in files)}|{len(self.cal)}"
        z = np.load(cache, allow_pickle=True) if cache.exists() else None
        if z is not None and str(z["sig"]) == sig:
            self.tick = list(z["tick"])
            ao, ac, cl, rc, dv = (z[k] for k in ("ao", "ac", "cl", "rc", "dv"))
        else:
            self.tick = [p.stem for p in files]
            T, N = len(self.cal), len(self.tick)
            ao, ac, cl, rc, dv = (np.full((T, N), np.nan, "float32") for _ in range(5))
            for j, t in enumerate(self.tick):
                b = load_survivor(t).reindex(self.cal)
                ao[:, j], ac[:, j] = b.open * b.adj / b.close, b.adj
                cl[:, j], rc[:, j], dv[:, j] = b.close, b.raw_close, b.close * b.volume
            np.savez(cache, sig=np.array(sig), tick=np.array(self.tick), ao=ao, ac=ac, cl=cl, rc=rc, dv=dv)
        self.col = {t: j for j, t in enumerate(self.tick)}
        df = lambda a: pd.DataFrame(a, index=self.cal, columns=self.tick)
        self.adv, self.elig, self.trend, self.r20 = features(df(cl), df(rc), df(dv))
        self.ao, self.rc, self.dv = ao, rc, dv
        self.acf = df(ac).ffill().to_numpy(copy=True)
        fin = np.isfinite(ac)
        self.last = np.where(fin.any(0), ac.shape[0] - 1 - np.argmax(fin[::-1], 0), -1)
        spy = self.col["SPY"]
        self.spy_ao, self.spy_ac = ao[:, spy].copy(), ac[:, spy].copy()
        self.elig[:, spy] = False
        a = np.where(self.elig, self.adv, np.nan)  # ADV decile among eligible names, per day
        self.dec = np.floor(pd.DataFrame(a).rank(axis=1, pct=True).to_numpy() * 10).clip(0, 9)

    def sidx(self, dates):
        """Signal day: last session on or before each date."""
        return self.cal.searchsorted(pd.to_datetime(dates).values, side="right") - 1

    def fwd(self, e, j, h):
        """Return entering at the open of session e and leaving at the close of session e+h-1
        (h sessions held), or at the series' last bar if it ends first; SPY over the same span."""
        x = np.minimum(np.minimum(e + h - 1, self.last[j]), len(self.cal) - 1)
        ok = (e < len(self.cal)) & (x >= e)
        e2 = np.where(ok, e, 0)
        ret = np.where(ok, self.acf[x, j] / self.ao[e2, j] - 1, np.nan)
        return ret, self.spy_ac[x] / self.spy_ao[e2] - 1


def series_event(panel, b, s, h_list):
    """The survivor-path quantities for one event computed from a single series' own bars.
    Returns (row, None) or (None, reason)."""
    b = b.reindex(panel.cal)
    one = lambda x: x.to_frame("x")
    adv, elig, trend, r20 = features(one(b.close), one(b.raw_close), one(b.close * b.volume))
    if s < 0 or not bool(elig[s, 0]):
        return None, "ineligible"
    e = s + 1
    ao = (b.open * b.adj / b.close).values
    acf = b.adj.ffill().values
    valid = np.flatnonzero(np.isfinite(b.adj.values))
    if e >= len(panel.cal) or not np.isfinite(ao[e]):
        return None, "no_entry_bar"
    out = {"adv": adv[s, 0], "trend": int(trend[s, 0]), "r20": r20[s, 0]}
    for h in h_list:
        x = min(e + h - 1, valid[-1], len(panel.cal) - 1)
        out[f"ret{h}"] = acf[x] / ao[e] - 1
        out[f"spy{h}"] = panel.spy_ac[x] / panel.spy_ao[e] - 1
        out[f"trunc{h}"] = bool(x < e + h - 1)
    return out, None


# ------------------------------------------------------------------ events
def qualifying(f, routine=False):
    """Qualifying purchases. A filing whose issuer has no trusted price series is kept (its
    open-market checks cannot be run) so that events lost to missing prices can be counted."""
    f = f.copy()
    # a 2026 ticker is matched by issuer CIK and needs no further proof; a delisted series is
    # matched by the symbol typed on the form, which may have been reused, so it must also agree
    # with the issuer's earlier Form 4 trade prices
    f["series_ok"] = f.has_series & (~f.delisted | (f.pit_n < MATCH_N) | (f.pit_rate >= MATCH_RATE))
    market = np.where(f.series_ok, (f.px_ok_share >= 0.9) & ~f.any_too_big, True)
    q = f[(f.is_officer | f.is_director) & (f.value >= 25_000) & market & f.lag_days.between(0, 10)
          & (f.routine == routine)]
    # one trade reported on two forms (a person and their trust or fund) counts once: identical
    # date, shares and value, with at least one of the forms reporting indirect ownership
    q = q.sort_values(["filing_date", "accession"])
    grp = q.groupby(["cik", "last_trans", "shares", "value"])
    indirect = (q.direct_shares < q.shares).groupby([q.cik, q.last_trans, q.shares, q.value]).transform("any")
    return q[~((grp.cumcount() > 0) & indirect)]


def refractory(df):
    """Keep an issuer's event only if it is at least H_MAIN sessions after its last kept one."""
    keep, last = [], {}
    for i, (cik, s) in enumerate(zip(df.cik.values, df.s.values)):
        if cik not in last or s - last[cik] >= H_MAIN:
            keep.append(i)
            last[cik] = s
    return df.iloc[keep]


def make_events(f, cal):
    f = f.assign(s=cal.searchsorted(f.filing_date.values, side="right") - 1)
    q = qualifying(f)
    # WF-1 cluster: >= 2 distinct insiders, combined >= $250k, filings within 30 calendar days.
    # A joint filing (several reporting owners) adds dollars but all joint filings together
    # count as one insider.
    q = q.assign(who=np.where(q.n_owners == 1, q.owner_cik.astype(str), "JOINT"))
    rows = []
    for cik, g in q.groupby("cik"):
        if g.who.nunique() < 2:
            continue
        d, o, v = g.filing_date.values, g.who.values, g.value.values
        for i in range(len(g)):
            w = (d >= d[i] - np.timedelta64(30, "D")) & (d <= d[i])
            if len(set(o[w])) >= 2 and v[w].sum() >= 250_000:
                rows.append(g.iloc[i])
    order = ["filing_date", "accession"]
    wf1 = refractory(pd.DataFrame(rows).sort_values(order))
    conv = q[(q.is_ceo | q.is_cfo | q.is_chair)
             & ((q.value >= 500_000) | ((q.pct_increase >= 0.10) & (q.value >= 100_000)))]
    wf2 = refractory(conv.sort_values(order))
    union = refractory(pd.concat([wf1, wf2]).drop_duplicates("accession").sort_values(order))
    rout = refractory(qualifying(f, routine=True).sort_values(order))
    return {"WF1": wf1, "WF2": wf2, "U": union, "ROUTINE": rout}


def evaluate(panel, ev, blocked, rng, selftest=False):
    """One row per registered event with its fate (`status`); tradeable rows carry net excess
    over SPY and over matched controls."""
    ev = ev.sort_values(["filing_date", "key"]).reset_index(drop=True)
    out = []
    for key, cik, delisted, fd, s, has_series, series_ok, form_px in zip(
            ev.key, ev.cik, ev.delisted, ev.filing_date, ev.s, ev.has_series, ev.series_ok, ev.form_px):
        row = {"key": key, "cik": cik, "delisted": delisted, "filing_date": fd, "s": s, "form_px": form_px}
        if s < 0 or s + 1 >= len(panel.cal):
            continue
        row["entry"] = panel.cal[s + 1]
        if selftest:  # replace the issuer with a random eligible name on the same day
            cand = np.flatnonzero(panel.elig[s] & np.isfinite(panel.ao[s + 1]))
            if len(cand) == 0:
                continue
            key, delisted, series_ok = panel.tick[rng.choice(cand)], False, True
            row.update(key=key, delisted=False)
        if not series_ok:
            out.append({**row, "status": "match_fail" if has_series else "no_series"})
            continue
        own = {panel.col.get(key, -1)}
        if delisted:
            r, why = series_event(panel, load_delisted(key), s, (H_MAIN, H_SHORT))
            if r is None:
                out.append({**row, "status": why})
                continue
            dec = np.floor((np.where(panel.elig[s], panel.adv[s], np.nan) < r["adv"]).sum()
                           / max(panel.elig[s].sum(), 1) * 10).clip(0, 9)
            own.add(panel.col.get(key.split("_", 1)[1], -1))  # the same symbol on the 2026 list
        else:
            j = panel.col.get(key)
            if j is None or not panel.elig[s, j]:
                out.append({**row, "status": "ineligible"})
                continue
            if not np.isfinite(panel.ao[s + 1, j]):
                out.append({**row, "status": "no_entry_bar"})
                continue
            r = {"adv": panel.adv[s, j], "trend": int(panel.trend[s, j]), "r20": panel.r20[s, j]}
            for h in (H_MAIN, H_SHORT):
                ret, spy = panel.fwd(np.array([s + 1]), np.array([j]), h)
                r[f"ret{h}"], r[f"spy{h}"] = ret[0], spy[0]
                r[f"trunc{h}"] = bool(panel.last[j] < s + h)
            dec = panel.dec[s, j]
        c = 2 * float(cost_side(r["adv"]))
        row.update(status="tradeable", adv=r["adv"], trend=r["trend"], r20=r["r20"])
        # matched controls: same day, same ADV decile, same trend state, same side of the 20-session
        # -15% drop line, not inside the holding period of an insider event of their own
        drop = bool(r["r20"] <= DROP)
        cand = np.flatnonzero(panel.elig[s] & (panel.dec[s] == dec) & (panel.trend[s] == r["trend"])
                              & ((panel.r20[s] <= DROP) == drop) & ~blocked[s] & np.isfinite(panel.ao[s + 1]))
        cand = cand[~np.isin(cand, list(own))]
        pick = rng.choice(cand, size=min(N_CONTROLS, len(cand)), replace=False) if len(cand) else np.array([], int)
        row["n_ctrl"] = len(pick)
        for h in (H_MAIN, H_SHORT):
            row[f"ret{h}"] = r[f"ret{h}"]
            row[f"exc{h}"] = r[f"ret{h}"] - r[f"spy{h}"] - c
            row[f"trunc{h}"] = r[f"trunc{h}"]
            if len(pick):
                cr, cs = panel.fwd(np.full(len(pick), s + 1), pick, h)
                row[f"ctl{h}"] = row[f"exc{h}"] - np.nanmean(cr - cs - 2 * cost_side(panel.adv[s, pick]))
            else:
                row[f"ctl{h}"] = np.nan
        out.append(row)
    return pd.DataFrame(out)


# ------------------------------------------------------------------ statistics
def clusters(entry):
    """Three resampling schemes. A 63-session hold spills into the next quarter, so half-year
    blocks are included and the widest interval / largest p is always the one reported."""
    return (entry.dt.strftime("%Y-%m-%d"), entry.dt.to_period("Q").astype(str),
            entry.dt.year.astype(str) + "H" + ((entry.dt.month > 6) + 1).astype(str))


def boot_mean(x, cl, rng):
    """Cluster bootstrap of the mean. Returns (lo, hi, share of draws <= 0)."""
    g = pd.DataFrame({"x": np.asarray(x, float), "c": np.asarray(cl)}).dropna().groupby("c").x.agg(["sum", "count"])
    if len(g) < 2:
        return np.nan, np.nan, np.nan
    idx = rng.integers(0, len(g), (B, len(g)))
    m = g["sum"].values[idx].sum(1) / g["count"].values[idx].sum(1)
    return np.percentile(m, 2.5), np.percentile(m, 97.5), float((m <= 0).mean())


def widest(x, entry, rng):
    r = [boot_mean(x, c, rng) for c in clusters(entry)]
    return min(a[0] for a in r), max(a[1] for a in r), max(a[2] for a in r)


def boot_diff(xa, ca, xb, cb, rng):
    a = pd.DataFrame({"x": np.asarray(xa, float), "c": np.asarray(ca)}).groupby("c").x.agg(["sum", "count"])
    b = pd.DataFrame({"x": np.asarray(xb, float), "c": np.asarray(cb)}).groupby("c").x.agg(["sum", "count"])
    cl = sorted(set(a.index) | set(b.index))
    if len(cl) < 2 or not len(a) or not len(b):
        return np.nan, np.nan, np.nan
    a, b = a.reindex(cl).fillna(0), b.reindex(cl).fillna(0)
    idx = rng.integers(0, len(cl), (B, len(cl)))
    na, nb = a["count"].values[idx].sum(1), b["count"].values[idx].sum(1)
    ok = (na > 0) & (nb > 0)
    d = a["sum"].values[idx].sum(1)[ok] / na[ok] - b["sum"].values[idx].sum(1)[ok] / nb[ok]
    return np.percentile(d, 2.5), np.percentile(d, 97.5), float((d <= 0).mean())


def level_stats(d, name, rng, h=H_MAIN, lost=0):
    """Tested statistic: control-adjusted net excess. Excess over SPY is reported and gated."""
    d = d[d.status == "tradeable"] if "status" in d.columns else d
    if len(d) < 5:
        return {"cell": name, "kind": "level", "h": h, "n": len(d), "p": np.nan}
    exc, ctl = d[f"exc{h}"], d[f"ctl{h}"]
    t = d[ctl.notna()]
    lo, hi, p = widest(t[f"ctl{h}"], t.entry, rng)
    elo, ehi, _ = widest(exc, d.entry, rng)
    yr = t.groupby(t.entry.dt.year)[f"ctl{h}"].agg(["mean", "size"])
    yr = yr[yr["size"] >= 10]
    top = t.groupby("key")[f"ctl{h}"].sum().nlargest(5).index
    ex = t[~t.key.isin(top)]
    xlo, _, _ = boot_mean(ex[f"ctl{h}"], clusters(ex.entry)[1], rng) if len(ex) else (np.nan,) * 3
    tr = d[f"trunc{h}"].astype(bool)
    grow = 1 + d[f"ret{h}"]
    return {"cell": name, "kind": "level", "h": h, "n": len(t), "n_no_control": int(ctl.isna().sum()),
            "n_delisted": int(d.delisted.sum()), "n_truncated": int(tr.sum()),
            "mean": t[f"ctl{h}"].mean(), "median": t[f"ctl{h}"].median(), "ci_lo": lo, "ci_hi": hi, "p": p,
            "exc_mean": exc.mean(), "exc_lo": elo, "exc_hi": ehi,
            "years_pos": float((yr["mean"] > 0).mean()) if len(yr) else np.nan, "n_years": len(yr),
            "mean_ex_top5": ex[f"ctl{h}"].mean() if len(ex) else np.nan, "ex_top5_lo": xlo,
            # holds cut short by the series ending: the remaining value written down by 30% / 100%
            "exc_mean_trunc_m30": (exc - np.where(tr, 0.30 * grow, 0)).mean(),
            "exc_mean_trunc_m100": (exc - np.where(tr, grow, 0)).mean(),
            # events with no usable price series (form price >= $5): the mean excess they would
            # need for the cell's SPY excess to be zero
            "n_lost": lost, "lost_breakeven_exc": -exc.sum() / lost if lost else np.nan}


def diff_stats(da, db, name, rng, two_sided, h=H_MAIN):
    """Arm A minus arm B on control-adjusted excess, so that a generic price effect shared by
    non-event stocks in the same state (trend, recent drop) does not count as an insider effect."""
    col = f"ctl{h}"
    da, db = da[da[col].notna()], db[db[col].notna()]
    if len(da) < 5 or len(db) < 5:
        return {"cell": name, "kind": "diff", "h": h, "n": len(da), "n_b": len(db), "p": np.nan}
    r = [boot_diff(da[col], ca, db[col], cb, rng) for ca, cb in list(zip(clusters(da.entry), clusters(db.entry)))[1:]]
    lo, hi, p_le = min(a[0] for a in r), max(a[1] for a in r), max(a[2] for a in r)
    p_ge = max(1 - a[2] for a in r)
    p = min(2 * min(p_le, p_ge), 1.0) if two_sided else p_le
    return {"cell": name, "kind": "diff", "h": h, "n": len(da), "n_b": len(db),
            "mean": da[col].mean() - db[col].mean(), "mean_a": da[col].mean(),
            "mean_b": db[col].mean(), "ci_lo": lo, "ci_hi": hi, "p": p}


def holm(p):
    p = np.asarray(p, float)
    order = np.argsort(p)
    adj = np.empty(len(p))
    run = 0.0
    for rank, i in enumerate(order):
        run = max(run, (len(p) - rank) * p[i])
        adj[i] = min(run, 1.0)
    return adj


# ------------------------------------------------------------------ dark flow (WF-5, WF-6)
def dark_z(panel):
    d = pd.read_pickle(WF / "darkflow.pkl")
    d = d[d.symbol.isin(panel.col)]
    sh = d.pivot(index="date", columns="symbol", values="short").reindex(index=panel.cal, columns=panel.tick)
    tot = d.pivot(index="date", columns="symbol", values="total").reindex(index=panel.cal, columns=panel.tick)
    s5 = sh.rolling(5, min_periods=4).sum() / tot.rolling(5, min_periods=4).sum()
    z = (s5 - s5.rolling(252, min_periods=200).mean()) / s5.rolling(252, min_periods=200).std()
    # symbol sanity: off-exchange volume must be a plausible share of the bar's as-traded volume
    share = tot.to_numpy() / (panel.dv / panel.rc)
    sane = pd.DataFrame(((share >= 0.05) & (share <= 1.2)).astype("float32"),
                        index=panel.cal).rolling(252, min_periods=200).mean() >= 0.9
    return z.where(sane.to_numpy()).to_numpy(dtype="float32", copy=True)


def wf5(panel, z, lo, hi, rng):
    s0, s1 = panel.cal.searchsorted(pd.Timestamp(lo)), panel.cal.searchsorted(pd.Timestamp(hi), side="right") - 1
    rows = []
    for s in range(s0, min(s1, len(panel.cal) - H_DARK - 2) + 1, 5):
        j = np.flatnonzero(panel.elig[s] & np.isfinite(z[s]) & np.isfinite(panel.ao[s + 1]))
        if len(j) < 200:
            continue
        ret, spy = panel.fwd(np.full(len(j), s + 1), j, H_DARK)
        exc = ret - spy
        net = exc - 2 * cost_side(panel.adv[s, j])
        zz = z[s, j]
        top, bot = zz >= np.percentile(zz, 90), zz <= np.percentile(zz, 10)
        rows.append({"date": panel.cal[s], "spread": np.nanmean(exc[top]) - np.nanmean(exc[bot]),
                     "top_net": np.nanmean(net[top]), "bot_net": np.nanmean(net[bot]),
                     "all_gross": np.nanmean(exc), "n": len(j)})
    d = pd.DataFrame(rows)
    if len(d) < 20:
        return d, {"cell": "WF5", "kind": "dark", "h": H_DARK, "n": len(d), "mean": np.nan, "p": np.nan}
    x = d.spread.values - d.spread.mean()
    L = 4
    var = (x @ x) / len(x) + 2 * sum((1 - k / (L + 1)) * (x[k:] @ x[:-k]) / len(x) for k in range(1, L + 1))
    cq, ch = clusters(d.date)[1:]
    r = [boot_mean(d.spread, c, rng) for c in (cq, ch)]
    lo_, hi_, p_le, p_ge = min(a[0] for a in r), max(a[1] for a in r), max(a[2] for a in r), max(1 - a[2] for a in r)
    # tradeable only as a long tilt: the favoured decile net of costs against the universe held
    # without trading (gross), both in excess of SPY
    tl = min(boot_mean(d.top_net - d.all_gross, c, rng)[0] for c in (cq, ch))
    bl = min(boot_mean(d.bot_net - d.all_gross, c, rng)[0] for c in (cq, ch))
    yr = d.groupby(d.date.dt.year).spread.mean()
    return d, {"cell": "WF5", "kind": "dark", "h": H_DARK, "n": len(d), "mean": d.spread.mean(),
               "nw_t": d.spread.mean() / np.sqrt(var / len(x)), "ci_lo": lo_, "ci_hi": hi_,
               "p": min(2 * min(p_le, p_ge), 1.0), "top_net": d.top_net.mean(), "bot_net": d.bot_net.mean(),
               "all_gross": d.all_gross.mean(), "top_vs_all_lo": tl, "bot_vs_all_lo": bl,
               "years_same_sign": float((np.sign(yr) == np.sign(d.spread.mean())).mean())}


# ------------------------------------------------------------------ run
def filed_8k(d, k8, cal):
    """True where the issuer has an 8-K in `k8` dated from session s-20 to the Form 4 filing date."""
    by = {c: g.date.values for c, g in k8.groupby("cik")}
    none = np.array([], "datetime64[ns]")
    out = []
    for cik, s, fd in zip(d.cik, d.s, d.filing_date):
        a = by.get(cik, none)
        out.append(bool(((a >= cal[max(s - 20, 0)].to_datetime64()) & (a <= np.datetime64(fd))).any()))
    return np.array(out, bool)


def path_check(panel, rng, n=200):
    """The single-series path (used for delisted issuers) must reproduce the panel path."""
    done = off = 0
    while done < n:
        s = int(rng.integers(300, len(panel.cal) - H_MAIN - 2))
        cand = np.flatnonzero(panel.elig[s] & np.isfinite(panel.ao[s + 1]))
        if len(cand) == 0:
            continue
        j = int(rng.choice(cand))
        r, why = series_event(panel, load_survivor(panel.tick[j]), s, (H_MAIN,))
        ret, spy = panel.fwd(np.array([s + 1]), np.array([j]), H_MAIN)
        assert r is not None, (panel.tick[j], s, why)
        assert abs(r["ret63"] - ret[0]) < 1e-4 and abs(r["spy63"] - spy[0]) < 1e-4, (panel.tick[j], s)
        assert abs(r["adv"] / panel.adv[s, j] - 1) < 1e-3, (panel.tick[j], s)
        off += r["trend"] != panel.trend[s, j]  # float32 panel vs float64 series at a threshold
        done += 1
    assert off <= 2, f"{off} trend-state mismatches"
    print(f"path check: {n} random name-days agree between the panel and single-series paths", flush=True)


def run(window, selftest=False, counts_only=False):
    OUT.mkdir(exist_ok=True)
    tag = "SELFTEST" if selftest else window
    cells_path = OUT / f"WF1_cells_{tag}.csv"
    sign_path = OUT / "WF1_wf5_sign.json"
    if window.startswith("CONF") and not counts_only:
        if cells_path.exists():
            sys.exit(f"{cells_path.name} exists: a confirmatory window is run once")
        if not sign_path.exists():
            sys.exit("WF1_wf5_sign.json missing: run DISC first")
    lo, hi = WINDOWS[window]
    rng = np.random.default_rng(SEED)
    panel = Panel()
    if selftest:
        path_check(panel, rng)
    f = pd.read_pickle(WF / "insider_purchases.pkl")
    evs = make_events(f, panel.cal)
    # a name is not a control while it is inside the holding period of its own insider event
    blocked = np.zeros(panel.elig.shape, bool)
    u = evs["U"]
    for key, s in zip(u.key, u.s):
        j = panel.col.get(key)
        if j is not None and s >= 0:
            blocked[s:s + H_MAIN + 1, j] = True
    res, funnel = {}, []
    for name in ("WF1", "WF2", "U", "ROUTINE"):
        e = evs[name]
        e = e[(e.entry_date >= lo) & (e.entry_date <= hi)]
        r = evaluate(panel, e, blocked, rng, selftest)
        fn = r.groupby([r.entry.dt.year, "status"]).size().unstack(fill_value=0)
        fn.insert(0, "registered", fn.sum(axis=1))
        funnel.append(fn.assign(cell=name))
        res[name] = r
        print(name, "registered", len(r), r.status.value_counts().to_dict(), flush=True)
    funnel = pd.concat(funnel).fillna(0)
    funnel.to_csv(OUT / f"WF1_funnel_{tag}.csv")
    lost = {n: int((res[n].status.isin(["no_series", "match_fail"]) & (res[n].form_px >= MIN_PRICE)).sum())
            for n in res}
    d = res["U"][res["U"].status == "tradeable"].reset_index(drop=True)
    k8 = pd.read_pickle(WF / "news8k_all.pkl")
    drop = (d.r20 <= DROP).values
    any8k = filed_8k(d, k8, panel.cal)
    earn8k = filed_8k(d, k8[k8["items"].fillna("").str.contains("2.02", regex=False)], panel.cal)
    z = dark_z(panel)
    if selftest:  # give every stock another stock's flow series
        z = z[:, rng.permutation(z.shape[1])]
    zj = np.array([z[s, panel.col[k]] if k in panel.col else np.nan for k, s in zip(d.key, d.s)])
    if counts_only:
        arms = {"U tradeable": len(d), "U with controls": int(d.ctl63.notna().sum()),
                "trend UP": int((d.trend == 1).sum()), "trend DOWN": int((d.trend == -1).sum()),
                "drop >= 15%": int(drop.sum()), "drop, earnings 8-K": int((drop & earn8k).sum()),
                "drop, no earnings 8-K": int((drop & ~earn8k).sum()), "drop, no 8-K": int((drop & ~any8k).sum()),
                "dark z available": int(np.isfinite(zj).sum()), "dark z > 0": int((zj > 0).sum()),
                "dark z < 0": int((zj < 0).sum()), "delisted": int(d.delisted.sum()),
                "WF1 tradeable": int((res["WF1"].status == "tradeable").sum()),
                "WF2 tradeable": int((res["WF2"].status == "tradeable").sum()),
                "ROUTINE tradeable": int((res["ROUTINE"].status == "tradeable").sum())}
        pd.Series(arms).to_csv(OUT / f"WF1_counts_{window}.csv", header=["n"])
        print(pd.Series(arms).to_string())
        print(funnel.groupby("cell").sum().to_string())
        return
    for name in res:
        res[name].to_csv(OUT / f"WF1_events_{name}_{tag}.csv", index=False)
    cells = [level_stats(res["WF1"], "WF1", rng, lost=lost["WF1"]),
             level_stats(res["WF2"], "WF2", rng, lost=lost["WF2"]),
             diff_stats(d[d.trend == 1], d[d.trend == -1], "WF3", rng, two_sided=True),
             diff_stats(d[drop], d[~drop], "WF4", rng, two_sided=False)]
    dser, c5 = wf5(panel, z, lo, hi, rng)
    dser.to_csv(OUT / f"WF1_wf5_series_{tag}.csv", index=False)
    cells.append(c5)
    if window == "DISC" and not selftest:
        m = c5.get("mean", np.nan)
        sign_path.write_text(json.dumps({"sign": int(np.sign(m)) if np.isfinite(m) and m != 0 else 1}))
    sign = json.loads(sign_path.read_text())["sign"] if sign_path.exists() else 1
    fin = np.isfinite(zj)
    sup = fin & (sign * zj > 0)
    cells.append(diff_stats(d[sup], d[fin & ~sup], "WF6", rng, two_sided=False))
    primary = pd.DataFrame(cells)
    primary["holm_p"] = holm(primary.p.fillna(1.0).values)
    # secondary, descriptive only (the *_survivors rows feed a gate: same sign without delisted events)
    sv = ~d.delisted.values.astype(bool)
    sec = [diff_stats(d[(d.trend == 1) & sv], d[(d.trend == -1) & sv], "WF3_survivors", rng, two_sided=True),
           diff_stats(d[drop & sv], d[~drop & sv], "WF4_survivors", rng, two_sided=False),
           diff_stats(d[sup & sv], d[fin & ~sup & sv], "WF6_survivors", rng, two_sided=False),
           level_stats(d, "U_all", rng, lost=lost["U"]), level_stats(d, "U_all_21d", rng, H_SHORT),
           level_stats(res["WF1"], "WF1_21d", rng, H_SHORT), level_stats(res["WF2"], "WF2_21d", rng, H_SHORT),
           level_stats(res["ROUTINE"], "PLACEBO_routine", rng)]
    for nm, m in (("U_trend_up", d.trend == 1), ("U_trend_down", d.trend == -1), ("U_trend_neutral", d.trend == 0),
                  ("U_drop", drop), ("U_drop_earnings_8k", drop & earn8k), ("U_drop_no_earnings_8k", drop & ~earn8k),
                  ("U_drop_no_8k", drop & ~any8k), ("U_dark_supportive", sup), ("U_dark_unsupportive", fin & ~sup),
                  ("U_delisted_only", ~sv), ("U_survivors_only", sv),
                  ("U_adv_10_50M", d.adv < 50e6), ("U_adv_50M_plus", d.adv >= 50e6)):
        if np.sum(m) >= 30:
            sec.append(level_stats(d[np.asarray(m)], nm, rng))
    out = pd.concat([primary.assign(family="primary"), pd.DataFrame(sec).assign(family="secondary")])
    out.insert(0, "window", tag)
    out.to_csv(cells_path, index=False)
    yearly = pd.concat([res[n][res[n].status == "tradeable"].assign(cell=n, year=lambda x: x.entry.dt.year)
                        .groupby(["cell", "year"])[["exc63", "ctl63"]].agg(["mean", "count"]) for n in ("WF1", "WF2", "U")])
    yearly.to_csv(OUT / f"WF1_yearly_{tag}.csv")
    show = [c for c in ["cell", "family", "n", "n_b", "mean", "ci_lo", "ci_hi", "p", "holm_p", "exc_mean", "exc_lo",
                        "years_pos", "ex_top5_lo", "n_delisted", "n_lost"] if c in out.columns]
    with pd.option_context("display.width", 250, "display.max_columns", 30, "display.float_format", "{:.4f}".format):
        print(out[show].to_string(index=False))


def verdict():
    full = pd.read_csv(OUT / "WF1_cells_CONF_A.csv")
    a = full[full.family == "primary"].set_index("cell")
    sec = full[full.family == "secondary"].set_index("cell")
    b = pd.read_csv(OUT / "WF1_cells_CONF_B.csv").set_index("cell")
    rows, green, amber = [], False, False
    for c, r in a.iterrows():
        same_b = c in b.index and np.sign(b.loc[c, "mean"]) == np.sign(r["mean"])
        if r.kind == "level":
            gates = {"holm": r.holm_p < 0.05, "control_ci": r.ci_lo > 0, "spy_excess_ci": r.exc_lo > 0,
                     "years": r.years_pos >= 0.6, "ex_top5": r.ex_top5_lo > 0, "n": r.n >= 300,
                     "conf_b": bool(same_b and r["mean"] > 0)}
            soft = r["mean"] > 0 and same_b and r.holm_p < 0.10
        elif r.kind == "diff":
            arm = {"WF3": "U_trend_up" if r["mean"] > 0 else "U_trend_down", "WF4": "U_drop",
                   "WF6": "U_dark_supportive"}[c]
            has_arm = arm in sec.index
            sv = f"{c}_survivors"
            same_sv = sv in sec.index and np.sign(sec.loc[sv, "mean"]) == np.sign(r["mean"])
            gates = {"holm": r.holm_p < 0.05, "ci": (r.ci_lo > 0) or (c == "WF3" and r.ci_hi < 0),
                     "n": min(r.n, r.n_b) >= 150, "conf_b": bool(same_b), "survivors_same_sign": bool(same_sv),
                     "favoured_arm": bool(has_arm and sec.loc[arm, "ci_lo"] > 0 and sec.loc[arm, "exc_lo"] > 0)}
            soft = bool(same_b and r.holm_p < 0.10 and has_arm and sec.loc[arm, "mean"] > 0)
        else:
            fav_lo = r.top_vs_all_lo if r["mean"] > 0 else r.bot_vs_all_lo
            gates = {"holm": r.holm_p < 0.05, "ci": (r.ci_lo > 0) or (r.ci_hi < 0),
                     "years": r.years_same_sign >= 0.6, "conf_b": bool(same_b), "favoured_decile": fav_lo > 0}
            soft = bool(same_b and r.holm_p < 0.10)
        ok = all(bool(v) for v in gates.values())
        green, amber = green or ok, amber or soft
        rows.append({"cell": c, "pass": ok, **{f"gate_{k}": bool(v) for k, v in gates.items()}})
    v = "GREEN" if green else "AMBER" if amber else "RED"
    pd.DataFrame(rows).to_csv(OUT / "WF1_gates.csv", index=False)
    (OUT / "WF1_verdict.json").write_text(json.dumps({"verdict": v, "cells": rows}, indent=1, default=str))
    print(pd.DataFrame(rows).to_string(index=False))
    print("VERDICT:", v)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", choices=list(WINDOWS))
    ap.add_argument("--counts", choices=list(WINDOWS))
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--verdict", action="store_true")
    a = ap.parse_args()
    if a.verdict:
        verdict()
    elif a.selftest:
        run("DISC", selftest=True)
    elif a.counts:
        run(a.counts, counts_only=True)
    elif a.window:
        run(a.window)
    else:
        sys.exit(__doc__)
