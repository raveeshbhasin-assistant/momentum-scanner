"""8-K history for insider-purchase issuers that the existing news8k_events.csv does not cover
(issuers no longer on the 2026 symbol list). Same source and fields as the NEWS8K build:
data.sec.gov/submissions, every form starting with 8-K, filing dates >= 2004-01-01.

Output: news8k_extra.jsonl (one line per CIK, resumable) and news8k_all.pkl
        (cik, date, items) = news8k_events.csv + the extra issuers, pre-2004-08 item
        numbers mapped to the modern ones as in the NEWS8K build.

  python research/whale_flow/fetch_8k_extra.py
"""
import json
import time

import pandas as pd
import requests

import fetch_delisted
from wf_common import DATA, WF

H = {"User-Agent": "Raveesh Singh raveeshsingh@gmail.com", "Accept-Encoding": "gzip, deflate"}
GAP = 0.2
OLD_ITEMS = {"1": "5.01", "2": "2.01", "3": "1.03", "4": "4.01", "5": "8.01", "6": "5.02",
             "7": "9.01", "8": "5.03", "9": "7.01", "10": "5.05", "11": "5.04", "12": "2.02"}
s = requests.Session()
s.headers.update(H)


def get(url):
    for attempt in range(6):
        time.sleep(GAP)
        try:
            r = s.get(url, timeout=60)
        except requests.RequestException:
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return {}
        time.sleep(30 * (attempt + 1))
    return None


def extract(block, out):
    if not block:
        return
    for form, fd, items in zip(block.get("form", []), block.get("filingDate", []), block.get("items", [])):
        if str(form).startswith("8-K") and fd >= "2004-01-01":
            out.append([fd, items])


def modern(items, fd):
    parts = [x.strip() for x in str(items).split(",") if x.strip()]
    if fd < "2004-08-23":
        parts = [OLD_ITEMS.get(x, x) for x in parts]
    return ",".join(parts)


def main():
    # date an 8-K became public: the ET acceptance date where known (an after-hours acceptance
    # carries the next business day as its SEC filing date), else the SEC filing date
    base = pd.read_csv(DATA / "research" / "news8k_events.csv", usecols=["cik", "et_date", "items_new"])
    base = base.rename(columns={"et_date": "date", "items_new": "items"})
    have = set(base.cik)
    ev = pd.read_pickle(WF / "insider_purchases.pkl")
    # every issuer that can end up in the purchase table: those already in it plus every
    # delisted fetch target (so this can run while the delisted price fetch is still going)
    need = sorted((set(ev.cik.astype(int)) | set(fetch_delisted.targets().cik.astype(int))) - have)
    out_path = WF / "news8k_extra.jsonl"
    done = {}
    if out_path.exists():
        with open(out_path) as f:
            for l in f:
                if l.strip():
                    d = json.loads(l)
                    done[d["cik"]] = d
    todo = [c for c in need if c not in done]
    print(f"{len(need):,} issuers outside news8k_events, {len(todo):,} to fetch", flush=True)
    with open(out_path, "a") as f:
        for i, cik in enumerate(todo, 1):
            j = get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json")
            rec = {"cik": cik, "ok": j is not None, "f": []}
            if j:
                filings = j.get("filings", {})
                extract(filings.get("recent"), rec["f"])
                for a in filings.get("files", []) or []:
                    if a.get("filingTo", "9999") >= "2004-01-01":
                        j2 = get(f"https://data.sec.gov/submissions/{a['name']}")
                        if j2 is None:
                            rec["ok"] = False
                            break
                        extract(j2, rec["f"])
            if rec["ok"]:
                f.write(json.dumps(rec) + "\n")
                f.flush()
                done[cik] = rec
            if i % 250 == 0:
                print(f"{i}/{len(todo)}", flush=True)
    extra = pd.DataFrame([(c, fd, modern(it, fd)) for c, d in done.items() if c in set(need) for fd, it in d["f"]],
                         columns=["cik", "date", "items"])
    allk = pd.concat([base, extra], ignore_index=True)
    allk["date"] = pd.to_datetime(allk.date)
    allk = allk.drop_duplicates()
    allk.to_pickle(WF / "news8k_all.pkl")
    covered = len(have & set(ev.cik.astype(int))) + sum(1 for c in need if c in done)
    print(f"news8k_all.pkl rows {len(allk):,}; event issuers with 8-K coverage {covered:,} of "
          f"{ev.cik.nunique():,}", flush=True)


if __name__ == "__main__":
    main()
