"""FMP inputs for the top-N study: daily market cap, dividend-adjusted close and the split list
for every ticker that was in the S&P 500 (since 2004-12) or the Nasdaq-100 (since 2007-02).

-> C:/dev/Trader-v3-data/top_n/fmp/{SYMBOL}.csv (date, adj, mcap) + fmp_meta.jsonl (rows, splits)
Needs FMP_API_KEY: `railway run python research/top_n/tn_fetch.py`. Resumable, ~200 calls/min.
"""
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whale_flow"))
import fetch_delisted as fd  # noqa: E402  (throttled FMP client)
from tn_members import TN, ndx_changes, ndx_now, sp500  # noqa: E402

OUT = TN / "fmp"
CHUNKS = [("2004-01-01", "2014-12-31"), ("2015-01-01", pd.Timestamp.today().strftime("%Y-%m-%d"))]
# ticker on the day -> symbol FMP files the same company under now
ALIAS = {"FB": "META", "WAG": "WBA", "KFT": "MDLZ", "UTX": "RTX", "MOT": "MSI", "PCLN": "BKNG", "SBC": "T",
         "ANTM": "ELV", "WLP": "ELV", "FISV": "FI", "SYMC": "GEN", "NLOK": "GEN", "HANS": "MNST", "ERTS": "EA",
         "RIMM": "BB", "ECHO": "DISH", "FNM": "FNMA", "FRE": "FMCC", "WFMI": "WFM", "IACI": "IAC", "HCP": "DOC",
         "CBS": "PARA", "VIAC": "PARA", "TMK": "GL", "LUK": "JEF", "KORS": "CPRI", "COH": "TPR", "PX": "LIN",
         "DWDP": "DD", "HRS": "LHX", "BBT": "TFC", "WLTW": "WTW", "ABC": "COR", "CTL": "LUMN", "JEC": "J",
         "RE": "EG", "PKI": "RVTY", "FLT": "CPAY", "CDAY": "DAY", "PEAK": "DOC", "WRK": "SW", "BLL": "BALL",
         "TSO": "ANDV", "LLL": "LHX", "HFC": "DINO", "INFO": "SPGI", "DISCA": "WBD", "FBHS": "FBIN", "GPS": "GAP",
         "ADS": "BFH", "CCE": "CCEP", "TIF": "TIF", "UAUA": "UAL", "LINTA": "QRTEA", "QVCA": "QRTEA", "CTRP": "TCOM",
         "PCAR": "PCAR", "BRK.B": "BRK-B", "BF.B": "BF-B"}


def universe():
    sp = sp500()
    t = set().union(*[v for d, v in sp.items() if d >= pd.Timestamp("2004-12-01")])
    ch = ndx_changes()
    t |= ndx_now() | set(ch.added) | set(ch.removed)
    t.discard("")
    syms = {ALIAS.get(x, x).replace(".", "-") for x in t} | {x.replace(".", "-") for x in t}
    return sorted(syms | {"SPY", "QQQ", "RSP", "OEF", "XLG", "QQQE", "MGK", "IVV"})


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    meta_path = TN / "fmp_meta.jsonl"
    done = set()
    if meta_path.exists():
        with open(meta_path) as f:
            done = {json.loads(l)["symbol"] for l in f if l.strip()}
    todo = [s for s in universe() if s not in done]
    print(f"{len(todo):,} symbols to fetch", flush=True)
    lock = threading.Lock()

    def one(s):
        px, mc = [], []
        for a, b in CHUNKS:
            px += fd.get("historical-price-eod/dividend-adjusted", symbol=s, **{"from": a, "to": b}) or []
            mc += fd.get("historical-market-capitalization", symbol=s, **{"from": a, "to": b}) or []
        rec = {"symbol": s, "n_px": len(px), "n_mcap": len(mc)}
        if px:
            df = pd.DataFrame(px)[["date", "adjClose"]].rename(columns={"adjClose": "adj"})
            if mc:
                df = df.merge(pd.DataFrame(mc)[["date", "marketCap"]].rename(columns={"marketCap": "mcap"}), on="date", how="outer")
            df.drop_duplicates("date").sort_values("date").to_csv(OUT / f"{s}.csv", index=False)
            rec["splits"] = [[x["date"], x["numerator"], x["denominator"]] for x in (fd.get("splits", symbol=s) or [])]
        with lock:
            mf.write(json.dumps(rec) + "\n")
            mf.flush()
        return bool(px)

    hits = 0
    with open(meta_path, "a") as mf, ThreadPoolExecutor(fd.WORKERS) as ex:
        for i, hit in enumerate(ex.map(one, todo), 1):
            hits += hit
            if i % 100 == 0:
                print(f"{i}/{len(todo)} with data {hits}", flush=True)
    print(f"done: {hits} of {len(todo)} returned prices; {len(fd.FAILED)} calls failed after retries", flush=True)


if __name__ == "__main__":
    main()
