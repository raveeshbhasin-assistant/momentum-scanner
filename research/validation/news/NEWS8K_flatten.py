"""NEWS8K: flatten sec_8k_all_raw_*.jsonl -> news8k_events.csv + news8k_README.md (counts by item x year, item map)."""
import json, glob, os, datetime as dt
import pandas as pd, numpy as np
RES = "C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad/research"
D = RES + "/news8k"
rows = []; nc = 0; multi = 0; names = {}
for p in sorted(glob.glob(f"{D}/sec_8k_all_raw_*.jsonl")):
    for line in open(p, encoding="utf-8"):
        try: r = json.loads(line)
        except Exception: continue
        nc += 1
        tk = r["tickers"]; sect = [str(t).upper().replace(".", "-") for t in (r.get("sec_tickers") or [])]
        prim = next((t for t in sect if t in tk), tk[0])
        if len(tk) > 1: multi += 1
        names[r["cik"]] = (r.get("name"), r.get("sic"), ",".join(str(x) for x in (r.get("exch") or []) if x))
        for f in r["f"]:
            rows.append((r["cik"], prim, "|".join(tk), f["a"], f["form"], f["fd"], f["rd"], f["acc"], f["it"] or ""))
df = pd.DataFrame(rows, columns=["cik", "ticker", "tickers_all", "accession", "form", "filing_date", "report_date", "acceptance_utc", "items"])
n0 = len(df); df = df.drop_duplicates(["cik", "accession"]); ndup = n0 - len(df)
# ---- ET conversion (manual US Eastern DST rules; no tzdata dependency) ----
t = pd.to_datetime(df["acceptance_utc"], utc=True, errors="coerce")
def nth_sunday(y, m, n):  # n-th Sunday of month (n=-1 -> last)
    if n > 0:
        d = dt.date(y, m, 1); d += dt.timedelta(days=(6 - d.weekday()) % 7); return d + dt.timedelta(days=7*(n-1))
    d = dt.date(y + (m == 12), (m % 12) + 1, 1) - dt.timedelta(days=1); return d - dt.timedelta(days=(d.weekday() + 1) % 7)
def dst_bounds(y):
    if y >= 2007: a, b = nth_sunday(y, 3, 2), nth_sunday(y, 11, 1)
    else: a, b = nth_sunday(y, 4, 1), nth_sunday(y, 10, -1)
    return (pd.Timestamp(a, tz="UTC") + pd.Timedelta(hours=7), pd.Timestamp(b, tz="UTC") + pd.Timedelta(hours=6))
yrs = t.dt.year
off = pd.Series(-5.0, index=df.index)
for y in sorted(yrs.dropna().unique()):
    a, b = dst_bounds(int(y)); m = (yrs == y) & (t >= a) & (t < b); off[m] = -4.0
et = (t + pd.to_timedelta(off, unit="h")).dt.tz_localize(None)
# cross-check with zoneinfo if available
try:
    from zoneinfo import ZoneInfo
    et2 = t.dt.tz_convert(ZoneInfo("America/New_York")).dt.tz_localize(None)
    mism = int((et2 != et).sum()); zi_note = f"zoneinfo cross-check: {mism} mismatches of {len(df)}"
except Exception as e: zi_note = f"zoneinfo unavailable ({type(e).__name__}); manual DST rule used"
df["acceptance_et"] = et.dt.strftime("%Y-%m-%d %H:%M:%S")
df["et_date"] = et.dt.strftime("%Y-%m-%d")
hm = et.dt.hour * 60 + et.dt.minute
df["timing"] = np.where(hm < 9*60+30, "pre", np.where(hm < 16*60, "intra", "post"))
df.loc[t.isna(), ["timing", "acceptance_et", "et_date"]] = ["na", "", ""]
# ---- items normalisation: old numbering (pre 2004-08-23) -> new codes ----
OLD2NEW = {"1": "5.01", "2": "2.01", "3": "1.03", "4": "4.01", "5": "8.01", "6": "5.02", "7": "9.01", "8": "5.03", "9": "7.01", "10": "5.05", "11": "5.04", "12": "2.02"}
def norm_items(s, fd):
    parts = [x.strip() for x in str(s).replace(";", ",").split(",") if x.strip()]
    if fd < "2004-08-23" and parts and all(p.isdigit() for p in parts): parts = [OLD2NEW.get(p, p) for p in parts]
    return ",".join(sorted(set(parts)))
df["items_new"] = [norm_items(s, fd) for s, fd in zip(df["items"], df["filing_date"])]
df["old_numbering"] = (df["filing_date"] < "2004-08-23") & df["items"].str.match(r"^\s*\d+(\s*,\s*\d+)*\s*$")
df = df.sort_values(["acceptance_utc", "cik"]).reset_index(drop=True)
cols = ["cik", "ticker", "tickers_all", "accession", "form", "filing_date", "report_date", "acceptance_utc", "acceptance_et", "et_date", "timing", "items", "items_new", "old_numbering"]
df[cols].to_csv(RES + "/news8k_events.csv", index=False)
# ---- README ----
EVT = {"1.01": "entry into material definitive agreement", "1.02": "termination of material agreement", "1.03": "bankruptcy/receivership", "1.05": "material cybersecurity incident",
       "2.01": "completion of acquisition/disposition", "2.02": "results of operations (earnings)", "2.03": "creation of direct financial obligation (debt)", "2.04": "triggering events accelerating obligations",
       "2.05": "exit/disposal costs (restructuring)", "2.06": "material impairments", "3.01": "delisting notice / listing-rule noncompliance", "3.02": "unregistered sale of equity (dilution)", "3.03": "material modification of security-holder rights",
       "4.01": "change in accountant", "4.02": "non-reliance on prior financials (restatement)", "5.01": "change in control", "5.02": "director/officer departure or appointment (executive change)",
       "5.03": "charter/bylaw amendment; fiscal-year change", "5.04": "trading suspension under benefit plans", "5.05": "code-of-ethics amendment/waiver", "5.07": "submission of matters to shareholder vote",
       "5.08": "shareholder director nominations", "7.01": "Regulation FD disclosure", "8.01": "other events", "9.01": "financial statements and exhibits"}
ex = df.assign(item=df["items_new"].str.split(",")).explode("item"); ex = ex[ex["item"].str.len() > 0]
ex["year"] = ex["filing_date"].str[:4]
top = ex["item"].value_counts()
piv = ex.pivot_table(index="year", columns="item", values="accession", aggfunc="count", fill_value=0)
keep = [c for c in ["2.02", "1.01", "1.02", "2.01", "2.03", "2.05", "2.06", "3.02", "4.02", "5.02", "5.07", "7.01", "8.01", "9.01"] if c in piv.columns]
piv = piv[keep]; piv.insert(0, "filings", df.groupby(df["filing_date"].str[:4])["accession"].count()); piv.insert(1, "ciks", df.groupby(df["filing_date"].str[:4])["cik"].nunique())
failed = sum(len(open(p).read().split()) for p in glob.glob(f"{D}/failed_*.txt"))
L = []
L.append("# NEWS8K - all-items 8-K event dataset (SEC submissions API)\n")
L.append(f"Built {dt.datetime.now():%Y-%m-%d %H:%M}. Source: https://data.sec.gov/submissions/CIK##########.json (`filings.recent` + every archive page in `filings.files` with filingTo >= 2004-01-01). Forms kept: every form starting with `8-K` (8-K, 8-K/A, 8-K12B, 8-K12G3, 8-K15D5). Items kept: ALL. Filing dates >= 2004-01-01.\n")
L.append(f"- CIKs crawled: **{nc}** of 5,213 universe CIKs (company_tickers.json intersect us_common_symbols.csv, symbols as of 2026-09 -> survivorship: delisted names absent). Failed CIKs (after 10 retries): {failed}. CIKs mapped to >1 universe ticker (share classes): {multi} - one row per filing, `ticker` = SEC primary, `tickers_all` pipe-joined.")
L.append(f"- Rows: **{len(df):,}** unique (cik, accession) filings ({ndup} duplicates dropped). Date range {df['filing_date'].min()} .. {df['filing_date'].max()}. Item 2.02 filings: {int((ex['item']=='2.02').sum()):,} (earn dataset had 196,281 from the same source - should match closely).")
L.append("- Forms: " + ", ".join(f"{k} {v:,}" for k, v in df["form"].value_counts().items()))
L.append("- Timing (acceptance in ET; pre <09:30, intra 09:30-16:00, post >=16:00): " + ", ".join(f"{k} {v:,}" for k, v in df["timing"].value_counts().items()) + f". {zi_note}.")
L.append("- **Event-date rule for the consumer:** the tradeable reaction is the first RTH session strictly after acceptance: `pre` -> same `et_date` open; `intra` -> same session (already trading); `post` -> next trading day's open. Filings accepted after 17:30 ET carry the NEXT business day as SEC `filing_date` - use `et_date` + `timing`, not `filing_date`, to align with bars.")
L.append(f"- Old item numbering: {int(df['old_numbering'].sum()):,} filings dated before 2004-08-23 use the pre-reform integer items; `items_new` maps them (1->5.01, 2->2.01, 3->1.03, 4->4.01, 5->8.01, 6->5.02, 7->9.01, 8->5.03, 9->7.01, 10->5.05, 11->5.04, 12->2.02); `items` is the raw SEC string.\n")
L.append("## Item code -> event type (standard map)\n")
L.append("| item | event type | filings |"); L.append("|---|---|---:|")
for k in sorted(set(list(EVT)) | set(top.index[:30])):
    L.append(f"| {k} | {EVT.get(k, '(other/uncommon)')} | {int(top.get(k, 0)):,} |")
L.append("\n## Counts by year (filings, distinct CIKs, and filings carrying each key item)\n")
try: L.append(piv.to_markdown())
except Exception: L.append("```\n" + piv.to_string() + "\n```")
L.append("\n## Item co-occurrence notes\n")
solo = ex.groupby("accession")["item"].apply(lambda s: sorted(set(s) - {"9.01"}))
combo = solo.apply(lambda l: ",".join(l)).value_counts().head(25)
L.append("Top item combinations (9.01 exhibits stripped):\n")
L.append("| items (ex 9.01) | filings |"); L.append("|---|---:|")
for k, v in combo.items(): L.append(f"| {k or '(9.01 only)'} | {v:,} |")
L.append("\n## Files\n")
L.append(f"- `research/news8k_events.csv` - columns: {', '.join(cols)}")
L.append("- `research/news8k/sec_8k_all_raw_N.jsonl` - one line per CIK {cik, tickers, sec_tickers, name, sic, exch, narch, n, f:[{a,form,fd,rd,acc,it}]}")
L.append("- `research/news8k/NEWS8K_sec_8k_all_fetch.py` (crawler, resumable, 4 shards x 0.5 s), `NEWS8K_flatten.py` (generator of this file), `shard_N.log`, `failed_N.txt`")
open(RES + "/news8k_README.md", "w", encoding="utf-8").write("\n".join(L))
print(f"ciks={nc} rows={len(df)} dups={ndup} failed={failed} multi={multi} {zi_note}")
print(df["form"].value_counts().to_dict()); print(df["timing"].value_counts().to_dict())
print(piv.to_string())
