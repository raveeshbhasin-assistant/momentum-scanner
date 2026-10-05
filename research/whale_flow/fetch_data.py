"""Fetch the raw inputs for the v6 Whale Flow study (research_plan_v6_whale_flow.md).

  W1  SEC Insider Transactions Data Sets (Form 3/4/5), quarterly zips, 2006Q1 onward
  W2  FINRA Reg SHO daily short-sale volume files, 2009-08 onward
      (per-facility FNSQ/FNYX/FNRA before 2018-08-01, consolidated CNMS after)

Raw files only; no returns are touched here. Resumable: existing files are skipped.
Output: C:/dev/Trader-v3-data/whale_flow/{insider_zips,finra_shvol}/

  python research/whale_flow/fetch_data.py [insider|finra|all]
"""
import datetime as dt
import gzip
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

OUT = Path("C:/dev/Trader-v3-data/whale_flow")
SEC_UA = {"User-Agent": "Raveesh Singh raveeshsingh@gmail.com"}
SEC_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{q}_form345.zip"
FINRA_URL = "https://cdn.finra.org/equity/regsho/daily/{fac}shvol{d}.txt"
CNMS_START = dt.date(2018, 8, 1)
FINRA_START = dt.date(2009, 8, 3)


def fetch_insider():
    out = OUT / "insider_zips"
    out.mkdir(parents=True, exist_ok=True)
    today = dt.date.today()
    got = missing = 0
    for y in range(2006, today.year + 1):
        for q in range(1, 5):
            name = f"{y}q{q}"
            dest = out / f"{name}_form345.zip"
            if dest.exists() and dest.stat().st_size > 100_000:
                got += 1
                continue
            r = requests.get(SEC_URL.format(q=name), headers=SEC_UA, timeout=120)
            if r.status_code == 200 and r.content[:2] == b"PK":
                dest.write_bytes(r.content)
                got += 1
                print(f"insider {name} {len(r.content)/1e6:.1f} MB", flush=True)
            else:
                missing += 1
                print(f"insider {name} unavailable ({r.status_code})", flush=True)
            time.sleep(0.5)
    print(f"insider done: {got} quarters on disk, {missing} unavailable", flush=True)


def _finra_one(job):
    fac, day, dest = job
    if dest.exists():
        return "skip"
    for attempt in range(3):
        try:
            r = requests.get(FINRA_URL.format(fac=fac, d=day.strftime("%Y%m%d")), timeout=60)
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200 and r.content.startswith(b"Date|"):
            with gzip.open(dest, "wb") as f:
                f.write(r.content)
            return "ok"
        if r.status_code in (403, 404):
            return "none"  # holiday or facility not reporting that day
        time.sleep(2 * (attempt + 1))
    return "fail"


def fetch_finra():
    out = OUT / "finra_shvol"
    out.mkdir(parents=True, exist_ok=True)
    jobs = []
    day = FINRA_START
    end = dt.date.today() - dt.timedelta(days=1)
    while day <= end:
        if day.weekday() < 5:
            facs = ["CNMS"] if day >= CNMS_START else ["FNSQ", "FNYX", "FNRA"]
            for fac in facs:
                jobs.append((fac, day, out / f"{fac}shvol{day:%Y%m%d}.txt.gz"))
        day += dt.timedelta(days=1)
    counts = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, res in enumerate(ex.map(_finra_one, jobs), 1):
            counts[res] = counts.get(res, 0) + 1
            if i % 1000 == 0:
                print(f"finra {i}/{len(jobs)} {counts}", flush=True)
    print(f"finra done: {counts}", flush=True)


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what in ("insider", "all"):
        fetch_insider()
    if what in ("finra", "all"):
        fetch_finra()
