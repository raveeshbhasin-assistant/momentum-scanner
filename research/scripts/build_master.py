"""Join performance_log entries with the richer per-pick fields in the daily files.
Output: research/picks_master.csv (one row per perf-log entry) + picks_master.parquet if pyarrow available."""
import json, glob, os, re, sys
import pandas as pd
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LD = os.path.join(SP, "live_data")
perf = json.load(open(os.path.join(LD, "performance_log.json")))["entries"]
def hhmm(ft):
    m = re.match(r"(\d+):(\d+)\s*(AM|PM)", ft or "")
    if not m: return None
    h, mi, ap = int(m.group(1)), m.group(2), m.group(3)
    if ap == "PM" and h != 12: h += 12
    if ap == "AM" and h == 12: h = 0
    return f"{h:02d}:{mi}"
daily = {}
for p in sorted(glob.glob(os.path.join(LD, "2026-*.json"))):
    d = os.path.basename(p)[:-5]
    try: rows = json.load(open(p))
    except Exception as ex: print("bad file", p, ex); continue
    for r in rows:
        key = (d, r.get("ticker"), hhmm(r.get("found_time")))
        daily.setdefault(key, r)   # first occurrence wins
out = []
miss = 0
for e in perf:
    key = (e["date"], e["ticker"], e["batch_time"])
    r = daily.get(key)
    row = dict(e)
    sc = row.pop("strong_components", None) or {}
    for k in ["bar_green","above_vwap","new_hod","pm_high_hold","complete_bar_used"]:
        row["sc_"+k] = sc.get(k)
    ext = row.pop("extension", None) or {}
    for k in ["consec_green","range_pos","above_orb_high"]:
        row["ext_"+k] = ext.get(k)
    row["stop_pct"] = (e["entry"]-e["stop"])/e["entry"]*100 if e.get("entry") and e.get("stop") else None
    row["target_pct"] = (e["target"]-e["entry"])/e["entry"]*100 if e.get("entry") and e.get("target") else None
    row["hit_25R"] = 1 if e["result"] == "WIN" else 0
    row["stopped"] = 1 if e["result"] == "LOSS" else 0
    row["month"] = e["date"][:7]
    row["dow"] = pd.Timestamp(e["date"]).day_name()
    row["minutes_from_open"] = (int(e["batch_time"][:2])*60+int(e["batch_time"][3:5])) - 570 if e.get("batch_time") else None
    if r is None:
        miss += 1
        row["joined"] = 0
    else:
        row["joined"] = 1
        for k in ["found_timestamp","price","composite_score","technical_score","sentiment_score","atr_target",
                  "resistance_target","resistance_level","risk_reward_ratio","regime_label","leader_adjustment",
                  "earnings_adjustment","signal_strength","leader_tier","volume_score","sector_boost",
                  "premarket_boost","is_premarket_flagged","is_reentry","is_strong_upgrade"]:
            if k in r: row["df_"+k] = r[k]
        ld = r.get("leadership") or {}
        for k in ["label","sector","ticker_pct","sector_pct","spy_pct","score_adjustment"]:
            row["lead_"+k] = ld.get(k)
        er = r.get("earnings") or {}
        for k in ["has_earnings","days_until","time_of_day","badge_level"]:
            row["earn_"+k] = er.get(k)
        if row.get("df_resistance_target") and e.get("entry"):
            try: row["resist_dist_pct"] = (float(r["resistance_target"]) - e["entry"])/e["entry"]*100
            except Exception: pass
        # anything else in the daily file we haven't captured
        extra = set(r.keys()) - {"ticker","found_time","found_timestamp","price","composite_score","technical_score","sentiment_score","rvol","entry","atr_target","resistance_target","resistance_level","stop_loss","risk_reward_ratio","rsi","leadership","earnings","regime_label","leader_adjustment","earnings_adjustment","strong_signal","strong_components","elite","tradeable","anti_ext","extension","signal_strength","leader_tier","volume_score","sector_boost","premarket_boost","is_premarket_flagged","is_reentry","is_strong_upgrade"}
        if extra and len(out) < 3: print("extra daily-file keys:", sorted(extra))
    out.append(row)
df = pd.DataFrame(out)
print("rows", len(df), "unjoined", miss)
print("columns", list(df.columns))
df.to_csv(os.path.join(SP, "research", "picks_master.csv"), index=False)
try:
    df.to_parquet(os.path.join(SP, "research", "picks_master.parquet"), index=False); print("parquet ok")
except Exception as ex: print("no parquet:", ex)
print(df[["composite_score" if "composite_score" in df else "score","df_technical_score","df_sentiment_score","rvol","rsi","stop_pct","target_pct"]].describe().T if "df_technical_score" in df else df.describe().T)
print("\nsector counts:\n", df["lead_sector"].value_counts(dropna=False).head(25))
print("\nregime:\n", df["df_regime_label"].value_counts(dropna=False))
print("\nsignal_strength:\n", df["df_signal_strength"].value_counts(dropna=False))
