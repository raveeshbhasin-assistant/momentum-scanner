"""Shared pieces for WF2 mining and its holdout test (WF2_DESIGN.md)."""
import numpy as np
import pandas as pd

import wf1_run as w
import wf_common
from wf_common import WF

OUT = WF / "wf2"
FROZEN = OUT / "frozen"
PREFIXES = ("p_", "d_", "m_", "ib_", "is_", "k_")
EXTRA = ("log_val30", "net_buy90")


def _load_delisted_fixed(key, _orig=wf_common.load_delisted):
    """FMP returns a dividend-adjusted close of 0 for 17 of the 4,252 delisted series (all or part
    of the history), which turns returns into NaN or infinity. Where that happens the last valid
    adjustment factor is carried, or 1 (no dividend adjustment) if there is none. The WF1 files
    are hash-locked, so the repair is applied here rather than in wf_common."""
    b = _orig(key)
    if b is None:
        return None
    bad = ~(b.adj > 0)
    if bad.any():
        factor = (b.adj / b.close).where(~bad).ffill().bfill().fillna(1.0)
        b["adj"] = b.close * factor
    return b


w.load_delisted = _load_delisted_fixed  # used by wf1_run.series_event via evaluate()


def feature_cols(df):
    return [c for c in df.columns if c.startswith(PREFIXES) or c in EXTRA]


def one_position(df, h, mask):
    """Apply a strategy's fire mask in time order, skipping a company that is still being held."""
    idx = np.flatnonzero(mask)
    idx = idx[np.argsort(df.s.values[idx], kind="stable")]
    keep, last = [], {}
    keys, ss = df.key.values, df.s.values
    for i in idx:
        k = keys[i]
        if k not in last or ss[i] - last[k] >= h:
            keep.append(i)
            last[k] = ss[i]
    return df.iloc[keep]


def wilson_lo(k, n, z=1.645):
    if n == 0:
        return 0.0
    ph = k / n
    return (ph + z * z / (2 * n) - z * np.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n))) / (1 + z * z / n)


class DayBase:
    """Success rate and mean net excess of every eligible 2026-listed stock bought on a given day."""

    def __init__(self, panel):
        self.p, self.cache = panel, {}

    def get(self, s, h):
        if (s, h) not in self.cache:
            p = self.p
            j = np.flatnonzero(p.elig[s] & np.isfinite(p.ao[s + 1]))
            ret, spy = p.fwd(np.full(len(j), s + 1), j, h)
            exc = ret - spy - 2 * w.cost_side(p.adv[s, j])
            exc = exc[np.isfinite(exc)]
            self.cache[(s, h)] = (float((exc > 0).mean()), float(exc.mean()))
        return self.cache[(s, h)]

    def attach(self, fires, h):
        if "base_win" in fires.columns:
            return fires
        if f"base_win{h}" in fires.columns:
            return fires.assign(base_win=fires[f"base_win{h}"], base_exc=fires[f"base_exc{h}"])
        b = [self.get(int(s), h) for s in fires.s.values]
        return fires.assign(base_win=[x[0] for x in b], base_exc=[x[1] for x in b])


def summarise(fires, h, rng, years=None):
    """Precision statistics for a set of fired trades carrying base_win / base_exc."""
    n = len(fires)
    if n == 0:
        return {"n": 0}
    exc = fires[f"exc{h}"].astype(float)
    win = (exc > 0).astype(float)
    lift = win - fires.base_win
    entry = pd.to_datetime(fires.entry)
    llo, lhi, lp = w.widest(lift, entry, rng)
    elo, ehi, _ = w.widest(exc, entry, rng)
    yr = pd.DataFrame({"y": entry.dt.year.values, "lift": lift.values}).groupby("y").lift.agg(["mean", "size"])
    yr = yr[yr["size"] >= 10]
    top = fires.groupby("key")[f"exc{h}"].sum().nlargest(5).index
    span = years or max((entry.max() - entry.min()).days / 365.25, 0.25)
    out = {"n": n, "per_year": n / span, "precision": win.mean(), "base_rate": fires.base_win.mean(),
           "lift": lift.mean(), "lift_lo": llo, "lift_hi": lhi, "p": lp,
           "mean_exc": exc.mean(), "exc_lo": elo, "exc_hi": ehi, "median_exc": exc.median(),
           "mean_exc_vs_random": (exc - fires.base_exc).mean(),
           "mean_exc_ex_top5": exc[~fires.key.isin(top).values].mean(),
           "share_worse_m10": float((exc <= -0.10).mean()), "share_better_p10": float((exc >= 0.10).mean()),
           "years_lift_pos": float((yr["mean"] > 0).mean()) if len(yr) else np.nan, "n_years": len(yr)}
    if f"ret{h}" in fires.columns:
        out["abs_win_rate"] = float((fires[f"ret{h}"] > 0).mean())
    if f"ctl{h}" in fires.columns:
        c = fires[f"ctl{h}"].dropna()
        out["precision_vs_peers"] = float((c > 0).mean()) if len(c) else np.nan
    return out
