"""Statistics, walk-forward and comparison utilities for trade tables produced by sim.simulate_many.

A "trades" DataFrame needs at least: date (session date), R (float). Optional columns used when present:
result, pnl_pct, entry_raw/exit_raw/cost_bps_per_side/risk (for cost sensitivity), ticker.
All inference is clustered by DATE (trades on the same day are not independent).
"""
from __future__ import annotations

from collections import Counter
from typing import Callable

import numpy as np
import pandas as pd

from .common import logger

TRADE_RESULTS = ("WIN", "LOSS", "TRAIL", "TIME", "EOD")     # rows that were actually traded


# ═══════════════════════════════════════════════════════════════════
#  Bootstrap
# ═══════════════════════════════════════════════════════════════════
def cluster_bootstrap_ci(values, groups, n_boot: int = 2000, stat="mean", alpha: float = 0.05, seed: int = 0):
    """Cluster (block) bootstrap CI of `stat` over `values`, resampling whole `groups` (e.g. dates).

    stat='mean' uses a fast weighted-bincount path (exact mean of the pooled resampled observations);
    any callable f(np.ndarray) -> float is supported (slower; loops over resamples).
    Returns (point_estimate, lo, hi). NaN values are dropped. With < 2 groups the CI is (point, point).
    """
    v = np.asarray(values, dtype=float)
    g = np.asarray(groups)
    ok = ~np.isnan(v)
    v, g = v[ok], g[ok]
    if len(v) == 0:
        return (np.nan, np.nan, np.nan)
    ug, inv = np.unique(g, return_inverse=True)
    G = len(ug)
    rng = np.random.default_rng(seed)
    if stat == "mean" or stat is np.mean:
        point = float(v.mean())
        if G < 2:
            return (point, point, point)
        s = np.bincount(inv, weights=v, minlength=G)
        c = np.bincount(inv, minlength=G).astype(float)
        idx = rng.integers(0, G, size=(n_boot, G))
        boots = s[idx].sum(1) / c[idx].sum(1)
    else:
        point = float(stat(v))
        if G < 2:
            return (point, point, point)
        order = np.argsort(inv, kind="stable")
        bounds = np.searchsorted(inv[order], np.arange(G + 1))
        per = [v[order[bounds[i]:bounds[i + 1]]] for i in range(G)]
        boots = np.empty(n_boot)
        for b in range(n_boot):
            pick = rng.integers(0, G, size=G)
            boots[b] = stat(np.concatenate([per[i] for i in pick]))
    lo, hi = np.percentile(boots, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return (point, float(lo), float(hi))


# ═══════════════════════════════════════════════════════════════════
#  Summaries
# ═══════════════════════════════════════════════════════════════════
def traded(trades: pd.DataFrame) -> pd.DataFrame:
    """Rows that were actually traded (drops NO_ENTRY / NO_DATA and NaN R)."""
    t = trades
    if "result" in t.columns:
        t = t[t["result"].isin(TRADE_RESULTS)]
    return t[t["R"].notna()]


def recost_R(trades: pd.DataFrame, cost_bps_per_side: float) -> pd.Series:
    """R of each trade under a different per-side cost, from the raw fills (exact: barriers are on raw
    prices, so only the fills change). Requires entry_raw, exit_raw, risk columns; NaN otherwise."""
    need = {"entry_raw", "exit_raw", "risk"}
    if not need.issubset(trades.columns):
        return pd.Series(np.nan, index=trades.index)
    c = cost_bps_per_side / 1e4
    ef = trades["entry_raw"] * (1 + c)
    xf = trades["exit_raw"] * (1 - c)
    return (xf - ef) / trades["risk"]


def daily_series(trades: pd.DataFrame, col: str = "R") -> pd.Series:
    """Per-date total of `col` over traded rows (dates with no trades are absent)."""
    t = traded(trades)
    return t.groupby("date")[col].sum().sort_index()


def max_drawdown(series: pd.Series) -> float:
    """Max peak-to-trough drawdown of the cumulative sum of a daily series (in the series' units)."""
    if len(series) == 0:
        return np.nan
    cum = series.cumsum()
    peak = cum.cummax()
    return float((cum - peak).min())


def summarize(trades: pd.DataFrame, *, n_boot: int = 2000, cost_grid=(0.0, 5.0, 10.0), seed: int = 0) -> dict:
    """Headline statistics for a trade table (date-clustered bootstrap CIs).

    Keys: n, n_no_entry, days, picks_per_day, avgR, avgR_ci, avg_pnl_pct, avg_pnl_ci, hit_rate (WIN),
    stop_rate (LOSS), eod_rate, trail_rate, time_rate, WR (pnl>0), total_R, daily_R_mean, daily_R_std,
    sharpe_like (daily mean/std * sqrt(252)), max_drawdown_R, worst_day, best_day, worst_day_date,
    best_day_date, cost_sensitivity {bps: avgR} (when raw fills are available), start, end.
    """
    t = traded(trades)
    all_n = len(trades)
    out = {"n": int(len(t)), "n_no_entry": int(all_n - len(t)), "days": int(t["date"].nunique()) if len(t) else 0}
    if len(t) == 0:
        return out
    out["picks_per_day"] = out["n"] / out["days"]
    R = t["R"].to_numpy(dtype=float)
    d = t["date"].to_numpy()
    m, lo, hi = cluster_bootstrap_ci(R, d, n_boot=n_boot, seed=seed)
    out["avgR"], out["avgR_ci"] = m, (lo, hi)
    if "pnl_pct" in t.columns:
        m2, lo2, hi2 = cluster_bootstrap_ci(t["pnl_pct"].to_numpy(dtype=float), d, n_boot=n_boot, seed=seed)
        out["avg_pnl_pct"], out["avg_pnl_ci"] = m2, (lo2, hi2)
        out["WR"] = float((t["pnl_pct"] > 0).mean())
    else:
        out["WR"] = float((t["R"] > 0).mean())
    if "result" in t.columns:
        vc = t["result"].value_counts(normalize=True)
        out["hit_rate"] = float(vc.get("WIN", 0.0))
        out["stop_rate"] = float(vc.get("LOSS", 0.0))
        out["eod_rate"] = float(vc.get("EOD", 0.0))
        out["trail_rate"] = float(vc.get("TRAIL", 0.0))
        out["time_rate"] = float(vc.get("TIME", 0.0))
    out["total_R"] = float(R.sum())
    ds = daily_series(t)
    out["daily_R_mean"] = float(ds.mean())
    out["daily_R_std"] = float(ds.std(ddof=1)) if len(ds) > 1 else np.nan
    out["sharpe_like"] = float(ds.mean() / ds.std(ddof=1) * np.sqrt(252)) if len(ds) > 1 and ds.std(ddof=1) > 0 else np.nan
    out["max_drawdown_R"] = max_drawdown(ds)
    out["worst_day"], out["best_day"] = float(ds.min()), float(ds.max())
    out["worst_day_date"], out["best_day_date"] = ds.idxmin(), ds.idxmax()
    out["start"], out["end"] = str(min(d)), str(max(d))
    if {"entry_raw", "exit_raw", "risk"}.issubset(t.columns):
        out["cost_sensitivity"] = {float(c): float(recost_R(t, c).mean()) for c in cost_grid}
    return out


def summary_row(s: dict, label: str = "") -> dict:
    """Flatten a summarize() dict into one printable row."""
    r = {"label": label, "n": s.get("n"), "days": s.get("days"), "per_day": round(s.get("picks_per_day", np.nan), 2),
         "avgR": round(s.get("avgR", np.nan), 3)}
    ci = s.get("avgR_ci")
    r["avgR_ci"] = f"[{ci[0]:+.3f},{ci[1]:+.3f}]" if ci else ""
    for k in ("hit_rate", "stop_rate", "eod_rate", "WR"):
        r[k] = round(s.get(k, np.nan), 3)
    r["total_R"] = round(s.get("total_R", np.nan), 1)
    r["sharpe"] = round(s.get("sharpe_like", np.nan), 2)
    r["maxDD_R"] = round(s.get("max_drawdown_R", np.nan), 1)
    return r


def monthly_table(trades: pd.DataFrame) -> pd.DataFrame:
    """Per-month n, days, avgR, total_R, hit/stop/eod rates, WR."""
    t = traded(trades).copy()
    if len(t) == 0:
        return pd.DataFrame()
    t["month"] = pd.to_datetime(t["date"].astype(str)).dt.strftime("%Y-%m")
    g = t.groupby("month")
    out = pd.DataFrame({"n": g.size(), "days": g["date"].nunique(), "avgR": g["R"].mean(), "total_R": g["R"].sum()})
    if "result" in t.columns:
        out["hit_rate"] = g["result"].apply(lambda s: (s == "WIN").mean())
        out["stop_rate"] = g["result"].apply(lambda s: (s == "LOSS").mean())
        out["eod_rate"] = g["result"].apply(lambda s: (s == "EOD").mean())
    pcol = "pnl_pct" if "pnl_pct" in t.columns else "R"
    out["WR"] = g[pcol].apply(lambda s: (s > 0).mean())
    return out


# ═══════════════════════════════════════════════════════════════════
#  Splits, walk-forward, comparison
# ═══════════════════════════════════════════════════════════════════
def temporal_split(trades: pd.DataFrame, split_date) -> dict:
    """Split by date: train = date < split_date, test = date >= split_date. Returns dict with
    train_df, test_df, train (summary), test (summary)."""
    sd = pd.Timestamp(split_date).date()
    d = pd.to_datetime(trades["date"].astype(str)).dt.date
    tr, te = trades[d < sd], trades[d >= sd]
    return {"train_df": tr, "test_df": te, "train": summarize(tr), "test": summarize(te)}


class ThingsTried:
    """Multiple-testing disclosure counter. Increment once per distinct hypothesis/configuration you
    evaluated (including the ones you discarded); report() gives the count and a Bonferroni-style note."""

    def __init__(self):
        self.counter = Counter()

    def increment(self, label: str = "unnamed", k: int = 1):
        self.counter[label] += k
        return self

    @property
    def total(self) -> int:
        return int(sum(self.counter.values()))

    def report(self, alpha: float = 0.05) -> str:
        n = max(self.total, 1)
        return (f"things_tried={self.total} ({dict(self.counter)}); Bonferroni-adjusted alpha for "
                f"{alpha:.2f} family-wise = {alpha / n:.4f}; a single 95% CI excluding 0 is weak evidence "
                f"after {self.total} tries.")


def walk_forward(candidates: pd.DataFrame, fit_fn: Callable, select_fn: Callable, *, train_days: int = 20,
                 test_days: int = 5, step: int = 5, date_col: str = "date", things_tried: ThingsTried | None = None,
                 label: str = "walk_forward", verbose: bool = True) -> dict:
    """Rolling walk-forward for score-based selectors over a candidate table (rows = candidate trades with
    their simulated outcome columns; must include `date` and `R`).

    For each fold: train = the `train_days` sessions before the test block, test = the next `test_days`
    sessions; folds advance by `step` sessions. model = fit_fn(train_df); chosen = select_fn(model, test_df)
    must return a subset (rows) of test_df. Nothing from the test block is visible to fit_fn.
    Returns dict(folds=[{fold, train_start, train_end, test_start, test_end, n_train, n_selected, summary}],
                 selected=<concat of chosen rows with a 'fold' column>, pooled=summarize(selected),
                 monthly=monthly_table(selected), things_tried=<ThingsTried>).
    """
    tt = things_tried if things_tried is not None else ThingsTried()
    tt.increment(label)
    dates = np.array(sorted(pd.unique(candidates[date_col])))
    dcol = candidates[date_col].to_numpy()
    folds, chosen = [], []
    k = 0
    for start in range(0, len(dates) - train_days, step):
        tr_dates = dates[start:start + train_days]
        te_dates = dates[start + train_days:start + train_days + test_days]
        if len(te_dates) == 0:
            break
        tr = candidates[np.isin(dcol, tr_dates)]
        te = candidates[np.isin(dcol, te_dates)]
        model = fit_fn(tr)
        sel = select_fn(model, te)
        if sel is None:
            sel = te.iloc[0:0]
        sel = sel.copy()
        sel["fold"] = k
        s = summarize(sel, n_boot=200) if len(sel) else {"n": 0}
        folds.append({"fold": k, "train_start": str(tr_dates[0]), "train_end": str(tr_dates[-1]),
                      "test_start": str(te_dates[0]), "test_end": str(te_dates[-1]), "n_train": len(tr),
                      "n_selected": len(sel), "avgR": s.get("avgR", np.nan), "total_R": s.get("total_R", np.nan),
                      "hit_rate": s.get("hit_rate", np.nan)})
        chosen.append(sel)
        if verbose:
            logger.info(f"{label} fold {k}: test {te_dates[0]}..{te_dates[-1]} selected {len(sel)} avgR {s.get('avgR', np.nan):+.3f}")
        k += 1
        if start + train_days + test_days >= len(dates):
            break
    selected = pd.concat(chosen, ignore_index=True) if chosen else candidates.iloc[0:0]
    return {"folds": pd.DataFrame(folds), "selected": selected, "pooled": summarize(selected),
            "monthly": monthly_table(selected), "things_tried": tt}


def compare(baseline: pd.DataFrame, candidate: pd.DataFrame, *, col: str = "R", agg: str = "sum",
            n_boot: int = 2000, seed: int = 0) -> dict:
    """Paired day-level comparison candidate - baseline of daily `agg` (sum|mean) of `col`.

    agg='sum': days missing on one side count as 0 (no trades = 0 R). agg='mean': only days present on
    both sides. Returns dict(n_days, baseline_daily, candidate_daily, diff, diff_ci, p_boot (two-sided
    bootstrap p for diff != 0), share_days_better).
    """
    b = traded(baseline).groupby("date")[col].agg(agg)
    c = traded(candidate).groupby("date")[col].agg(agg)
    if agg == "sum":
        days = sorted(set(b.index) | set(c.index))
        b = b.reindex(days).fillna(0.0)
        c = c.reindex(days).fillna(0.0)
    else:
        days = sorted(set(b.index) & set(c.index))
        b, c = b.reindex(days), c.reindex(days)
    diff = (c - b).to_numpy(dtype=float)
    n = len(diff)
    if n == 0:
        return {"n_days": 0}
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n, size=(n_boot, n))
    boots = diff[idx].mean(1)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    p = float(2 * min((boots <= 0).mean(), (boots >= 0).mean()))
    return {"n_days": n, "baseline_daily": float(b.mean()), "candidate_daily": float(c.mean()),
            "diff": float(diff.mean()), "diff_ci": (float(lo), float(hi)), "p_boot": p,
            "share_days_better": float((diff > 0).mean())}
