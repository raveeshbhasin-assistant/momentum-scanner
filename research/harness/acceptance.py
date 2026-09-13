"""Acceptance run for the harness (see ACCEPTANCE.md for the resulting numbers).

    python acceptance.py [--start 2026-06-17]

1. Re-resolves every picks_master row dated >= start with the LIVE resolver's rules
   (entry at the recorded price, 0 bps, both-in-bar = LOSS, fills exactly at the levels, EOD at the 15:55 close)
   and compares result / resolve_price with the live records.
2. Recomputes the four STRONG components + extension features on the cached Yahoo bars and compares them with
   the sc_* / ext_* fields the live scanner wrote (live feed was FMP real-time, so divergence is expected).
Writes cache/acceptance_resolve.csv, cache/acceptance_strong.csv and cache/acceptance_summary.json.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import numpy as np
import pandas as pd

from harness import bars as B
from harness import sim
from harness.common import BAR_TD, CACHE_DIR, PICKS_MASTER, logger, rth_mask


def resolve_section(pm: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    t0 = time.time()
    res = sim.resolve_like_live(pm)
    secs = time.time() - t0
    out = pm[["date", "batch_time", "ticker", "entry", "stop", "target", "result", "resolve_time", "resolve_price",
              "r_realized"]].copy()
    for c in ["result", "exit_reason", "exit_ts", "exit_fill", "R", "entry_idx", "exit_idx"]:
        out["h_" + c] = res[c].to_numpy()
    traded = out["h_result"].isin(["WIN", "LOSS", "EOD"])
    agree = traded & (out["h_result"] == out["result"])
    diff = (out["h_exit_fill"] - out["resolve_price"]).abs()
    out["price_diff"] = diff
    out["agree"] = agree
    n = len(out)
    s = {
        "n_picks": int(n), "n_traded_by_harness": int(traded.sum()), "n_no_data": int((out["h_result"] == "NO_DATA").sum()),
        "n_no_entry": int((out["h_result"] == "NO_ENTRY").sum()),
        "result_agreement_all": float(agree.sum() / n), "result_agreement_traded": float(agree.sum() / max(traded.sum(), 1)),
        "resolve_secs": round(secs, 1),
        "price_diff_quantiles_traded": {str(q): float(v) for q, v in diff[traded].quantile([.5, .9, .95, .99, 1.0]).items()},
        "price_diff_lt_1c_share": float((diff[traded] < 0.01).mean()),
        "price_diff_lt_5c_share": float((diff[traded] < 0.05).mean()),
        "crosstab": pd.crosstab(out.loc[traded, "result"], out.loc[traded, "h_result"]).to_dict(),
        "R_abs_diff_quantiles": {str(q): float(v) for q, v in (out.loc[traded, "h_R"] - out.loc[traded, "r_realized"]).abs().quantile([.5, .9, .99, 1.0]).items()},
        "no_data_tickers": sorted(out.loc[out["h_result"] == "NO_DATA", "ticker"].unique().tolist()),
        "no_entry_reasons": out.loc[out["h_result"] == "NO_ENTRY", "h_exit_reason"].value_counts().to_dict(),
    }
    # exit-time agreement where the live record has one
    has_rt = traded & out["resolve_time"].notna()
    if has_rt.any():
        h_hhmm = pd.to_datetime(out.loc[has_rt, "h_exit_ts"]).dt.strftime("%H:%M")
        s["n_with_resolve_time"] = int(has_rt.sum())
        s["exit_time_agreement"] = float((h_hhmm == out.loc[has_rt, "resolve_time"].astype(str).str[:5]).mean())
    # disagreement diagnostics
    dis = out[traded & ~agree].copy()
    s["n_disagree"] = int(len(dis))
    s["disagree_by_pair"] = (dis["result"] + "->" + dis["h_result"]).value_counts().to_dict()
    s["disagree_by_ticker_top"] = dis["ticker"].value_counts().head(12).to_dict()
    s["disagree_by_date_top"] = dis["date"].value_counts().head(8).to_dict()
    s["disagree_by_batch_minute_mod5"] = (dis["batch_time"].str[3:5].astype(int) % 5).value_counts().to_dict()
    s["all_by_batch_minute_mod5"] = (out["batch_time"].str[3:5].astype(int) % 5).value_counts().to_dict()
    # how far is the recorded level from being touched on our bars? (margin in cents for LOSS/WIN disagreements)
    return out, s


def strong_section(pm: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    sig = sim.signal_timestamps(pm)
    cut_ns_all = sim._ts_to_ns(sig - BAR_TD)
    parts = []
    t0 = time.time()
    for t, g in pm.groupby("ticker", sort=False):
        ind = B.get_indicators(t)
        if ind is None:
            continue
        st = B.strong_table(ind)
        m = rth_mask(ind.index)
        rth_idx = ind.index[m]
        rth_ns = rth_idx.as_unit("ns").asi8
        cut = cut_ns_all[g.index.to_numpy()]
        pos = np.searchsorted(rth_ns, cut, side="right") - 1        # last RTH bar with start <= signal - 5min
        ok = pos >= 0
        ref = rth_idx[np.clip(pos, 0, len(rth_idx) - 1)]
        same_day = ok & (ref.date == sig.loc[g.index].dt.date.to_numpy())
        rows = st.loc[ref[same_day]]
        part = g.loc[same_day, ["date", "batch_time", "ticker", "sc_bar_green", "sc_above_vwap", "sc_new_hod",
                                "sc_pm_high_hold", "strong_signal", "ext_consec_green", "ext_range_pos",
                                "ext_above_orb_high"]].copy()
        part["ref_bar_ts"] = rows.index.to_numpy()
        for c in ["bar_green", "above_vwap", "new_hod", "pm_high_hold", "strong", "consec_green", "range_pos", "above_orb_high"]:
            part["h_" + c] = rows[c].to_numpy()
        parts.append(part)
    cmp_ = pd.concat(parts)
    secs = time.time() - t0
    s = {"n_compared": int(len(cmp_)), "n_no_reference_bar": int(len(pm) - len(cmp_)), "secs": round(secs, 1)}
    for live, mine in [("sc_bar_green", "h_bar_green"), ("sc_above_vwap", "h_above_vwap"), ("sc_new_hod", "h_new_hod"),
                       ("sc_pm_high_hold", "h_pm_high_hold"), ("strong_signal", "h_strong")]:
        a = cmp_[live].astype(str).str.lower().eq("true").to_numpy()
        b = cmp_[mine].astype(bool).to_numpy()
        s[f"agree_{live}"] = float((a == b).mean())
        s[f"live_true_rate_{live}"] = float(a.mean())
        s[f"harness_true_rate_{live}"] = float(b.mean())
    e = cmp_[cmp_["ext_consec_green"].notna()]
    s["n_ext_compared"] = int(len(e))
    if len(e):
        s["agree_ext_consec_green"] = float((e["ext_consec_green"].astype(float) == e["h_consec_green"].astype(float)).mean())
        s["agree_ext_range_pos_within_0.02"] = float(((e["ext_range_pos"].astype(float) - e["h_range_pos"].astype(float)).abs() <= 0.02).mean())
        eo = e[e["ext_above_orb_high"].notna() & e["h_above_orb_high"].notna()]
        s["agree_ext_above_orb_high"] = float((eo["ext_above_orb_high"].astype(float) == eo["h_above_orb_high"].astype(float)).mean())
    return cmp_, s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-17")
    a = ap.parse_args()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pm = pd.read_csv(PICKS_MASTER, low_memory=False)
    pm = pm[pm["date"] >= a.start].reset_index(drop=True)
    logger.info(f"acceptance: {len(pm)} picks from {a.start} ({pm['date'].nunique()} dates, {pm['ticker'].nunique()} tickers)")

    res, s1 = resolve_section(pm)
    res.to_csv(CACHE_DIR / "acceptance_resolve.csv", index=False)
    logger.info("RESOLVE " + json.dumps({k: v for k, v in s1.items() if k not in ("crosstab",)}, default=str, indent=None))
    print(pd.crosstab(res.loc[res.agree.notna() & res.h_result.isin(["WIN", "LOSS", "EOD"]), "result"],
                      res.loc[res.h_result.isin(["WIN", "LOSS", "EOD"]), "h_result"]))

    cmp_, s2 = strong_section(pm)
    cmp_.to_csv(CACHE_DIR / "acceptance_strong.csv", index=False)
    logger.info("STRONG " + json.dumps(s2, default=str))

    with open(CACHE_DIR / "acceptance_summary.json", "w") as f:
        json.dump({"start": a.start, "resolve": s1, "strong": s2}, f, indent=2, default=str)
    logger.info("wrote cache/acceptance_summary.json")


if __name__ == "__main__":
    main()
