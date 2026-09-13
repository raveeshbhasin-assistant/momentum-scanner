"""Ad-hoc smoke test for sim.py (not part of the test suite)."""
import sys, time, os
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))
from harness import bars, sim, common
ET = common.ET


def mkday(over=None):
    idx = pd.date_range("2026-07-15 09:30", periods=78, freq="5min", tz=ET)
    df = pd.DataFrame({"Open": 100.0, "High": 100.5, "Low": 99.5, "Close": 100.0, "Volume": 1000.0}, index=idx)
    for k, (o, h, l, c) in (over or {}).items():
        df.loc[idx[k], ["Open", "High", "Low", "Close"]] = [o, h, l, c]
    return df


sig = common.make_ts("2026-07-15", "09:35")
d = mkday({2: (100, 105.5, 99.8, 105)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0)
print("T1 target first:", r["result"], r["R"], r["exit_ts"].time(), r["bars_held"])
d = mkday({3: (100, 100.2, 97, 97.5)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0)
print("T2 stop first:", r["result"], r["R"], r["exit_ts"].time())
d = mkday({2: (100, 105.5, 97, 101)})
for sb in ["loss", "win", "open_close_tiebreak"]:
    r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, same_bar=sb); print("T3 same-bar", sb, r["result"], r["R"])
d = mkday({2: (100, 105.5, 97, 99)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, same_bar="open_close_tiebreak"); print("T3b red tiebreak:", r["result"])
d = mkday({2: (96, 100, 95, 99)})
for g in [True, False]:
    r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, gap_aware_stops=g); print("T4 gap", g, r["result"], r["exit_reason"], r["R"], r["exit_fill"])
d = mkday(); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0); print("T5 EOD:", r["result"], r["exit_ts"].time(), r["bars_held"])
r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, exit_time="16:00"); print("T5b 16:00:", r["exit_ts"].time())
r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, exit_time="12:00"); print("T5c 12:00:", r["exit_ts"].time())
d = mkday({2: (101, 101.5, 100.5, 101), 4: (101, 106, 100.9, 105)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="next_open", cost_bps_per_side=5)
print("T6 next_open:", r["entry_ts"].time(), round(r["entry_fill"], 4), r["result"], round(r["R"], 4), round(r["R_own"], 4), "expected R", round((105 * (1 - 5e-4) - 101 * (1 + 5e-4)) / 2, 4))
r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="first_open", cost_bps_per_side=0); print("T6b first_open:", r["entry_ts"].time(), r["entry_fill"])
r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="next_close", cost_bps_per_side=0); print("T6c next_close:", r["entry_ts"].time(), r["entry_fill"], r["entry_idx"])
d = mkday(); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, time_stop_min=30, time_stop_min_r=0.5); print("T7 time stop:", r["result"], r["exit_ts"].time(), r["R"])
d = mkday({3: (100, 102.5, 100, 102), 6: (101, 101.2, 99.9, 100.2)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, breakeven_after_r=1.0); print("T8 breakeven:", r["result"], r["exit_reason"], r["exit_ts"].time(), r["R"])
d = mkday({3: (100, 103, 100, 102.5), 4: (102.5, 104, 102, 103.5), 6: (103, 103.2, 101.5, 101.8)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, trail_after_r=1.0, trail_dist_r=1.0); print("T9 trail:", r["result"], r["exit_reason"], r["exit_fill"], "(expect 104-2=102)", r["R"])
d = mkday({3: (100, 102.5, 100, 102), 6: (101, 101.2, 97.5, 98)}); r = sim.simulate_trade(d, sig, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=0, scale_out=[(1.0, 0.5)]); print("T10 scale:", r["result"], r["R"], r["notes"])
r = sim.simulate_trade(mkday(), common.make_ts("2026-07-15", "15:57"), 100, 98, 105, entry_mode="next_open"); print("T11 no entry:", r["result"], r["exit_reason"])

# --- real picks: batch vs single equivalence + live agreement
pm = pd.read_csv(common.PICKS_MASTER, low_memory=False); pm = pm[pm.date >= "2026-06-17"].copy()
t0 = time.time(); res = sim.resolve_like_live(pm); print("resolve_like_live", round(time.time() - t0, 1), "s")
ok = res.result.isin(["WIN", "LOSS", "EOD"])
agree = (res.result[ok] == pm.result[ok]); print("result agreement", agree.mean().round(4), "n", ok.sum(), "no-data/no-entry", (~ok).sum())
diff = (res.exit_fill[ok] - pm.resolve_price[ok]).abs(); print("resolve_price |diff| quantiles", diff.quantile([.5, .9, .99, 1]).round(4).to_dict(), "exact(<0.005)", (diff < 0.005).mean().round(4))
print(pd.crosstab(pm.result[ok], res.result[ok]))
smp = pm.sample(300, random_state=1)
rb = sim.resolve_like_live(smp, verbose=False)
mism = 0
for i, row in smp.iterrows():
    D = sim.day_arrays(row.ticker, row.date)
    if D is None:
        continue
    r1 = sim.simulate_trade(D, common.make_ts(row.date, row.batch_time), row.entry, row.stop, row.target, **sim.LIVE_RESOLVER_KW)
    b = rb.loc[i]
    same = (r1["result"] == b.result and np.isclose(r1["R"], b.R, equal_nan=True) and np.isclose(r1["mfe_R"], b.mfe_R, equal_nan=True)
            and np.isclose(r1["mae_R"], b.mae_R, equal_nan=True) and r1["bars_held"] == b.bars_held and r1["exit_reason"] == b.exit_reason
            and r1["exit_ts"] == b.exit_ts and r1["entry_ts"] == b.entry_ts)
    if not same:
        mism += 1; print("MISMATCH", row.ticker, row.date, row.batch_time, r1, b.to_dict())
print("single vs batch mismatches (live kw):", mism, "/", len(smp))
rb2 = sim.simulate_many(smp, verbose=False)
mism = 0
for i, row in smp.iterrows():
    D = sim.day_arrays(row.ticker, row.date)
    if D is None:
        continue
    r1 = sim.simulate_trade(D, common.make_ts(row.date, row.batch_time), row.entry, row.stop, row.target)
    b = rb2.loc[i]
    if not (r1["result"] == b.result and np.isclose(r1["R"], b.R, equal_nan=True) and r1["exit_reason"] == b.exit_reason and np.isclose(r1["mfe_R"], b.mfe_R, equal_nan=True)):
        mism += 1; print("MISMATCH2", row.ticker, row.date, row.batch_time, r1["result"], r1["R"], r1["exit_reason"], b.result, b.R, b.exit_reason)
print("single vs batch (next_open) mismatches:", mism)
print(rb2.result.value_counts().to_dict(), "avgR", rb2.R.mean().round(3))
# management path through simulate_many
rb3 = sim.simulate_many(smp.head(50), verbose=False, breakeven_after_r=1.0)
print("mgmt path:", rb3.result.value_counts().to_dict(), rb3.exit_ts.notna().sum())
# all_bars_labels one day
t0 = time.time(); ab = sim.all_bars_labels("AAPL", "2026-07-15"); print("all_bars_labels AAPL 07-15", ab.shape, round(time.time() - t0, 2), "s")
print(ab[["hhmm", "entry", "stop", "target", "sp_result", "sp_R", "no_result", "no_R", "strong", "rvol", "rsi"]].head(5).to_string())
print(ab.sp_result.value_counts().to_dict(), ab.no_result.value_counts().to_dict())
