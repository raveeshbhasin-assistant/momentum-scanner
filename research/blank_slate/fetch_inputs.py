"""Inputs for the blank-slate strategy search that are not on disk yet.

  delisted  full price history (2004 -> today) from FMP for every issuer that ever filed a Form 4
            and has no 2026 ticker: the closest thing to a list of delisted US companies that is
            available here (FMP's own delisted list is paywalled). 15,047 issuer-symbol pairs.
            -> C:/dev/Trader-v3-data/blank_slate/bars_delisted_full/{cik}_{SYMBOL}.csv + delisted_full_meta.jsonl
            Needs FMP_API_KEY: run through `railway run`. Resumable. ~200 calls/min.
  etf       daily bars for the major index, sector, bond and commodity ETFs (yfinance)
            -> C:/dev/Trader-v3-data/blank_slate/etf/{SYMBOL}.csv

  railway run python research/blank_slate/fetch_inputs.py delisted
  python research/blank_slate/fetch_inputs.py etf
"""
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whale_flow"))
import fetch_delisted as fd  # noqa: E402  (throttled FMP client)
from wf_common import SYM, WF, norm_symbol  # noqa: E402

BS = Path("C:/dev/Trader-v3-data/blank_slate")
ETFS = ("SPY QQQ IWM DIA MDY IJR VTI RSP EFA EEM TLT IEF SHY LQD HYG GLD SLV USO DBC VNQ "
        "XLB XLE XLF XLI XLK XLP XLU XLV XLY XLC XLRE SMH XBI IBB KRE XHB XRT XME GDX ITB IYT").split()


def targets():
    t = pd.read_pickle(WF / "insider_trans.pkl")
    t = t[t.ticker.isna() & t.cik.notna()]
    t = t.assign(sym=norm_symbol(t.symbol_on_form))
    t = t[t.sym.map(lambda s: bool(SYM.match(s))) & ~t.sym.isin(["NONE", "NA", "N-A"])]
    g = t.groupby(["cik", "sym"]).agg(first=("filing_date", "min"), last=("filing_date", "max"),
                                      n=("code", "size")).reset_index()
    return g.sort_values("n", ascending=False)


def delisted():
    out = BS / "bars_delisted_full"
    out.mkdir(parents=True, exist_ok=True)
    meta_path = BS / "delisted_full_meta.jsonl"
    done = set()
    if meta_path.exists():
        with open(meta_path) as f:
            done = {json.loads(l)["key"] for l in f if l.strip()}
    tg = targets()
    todo = [r for r in tg.itertuples() if f"{int(r.cik)}_{r.sym}" not in done]
    print(f"{len(tg):,} issuer-symbol pairs, {len(todo):,} to fetch", flush=True)
    lock = threading.Lock()
    rng = {"from": "2004-01-01", "to": pd.Timestamp.today().strftime("%Y-%m-%d")}

    def one(r):
        key = f"{int(r.cik)}_{r.sym}"
        px = fd.get("historical-price-eod/full", symbol=r.sym, **rng)
        rec = {"key": key, "cik": int(r.cik), "symbol": r.sym, "n": 0,
               "first_filing": str(r.first.date()), "last_filing": str(r.last.date()), "n_trans": int(r.n)}
        if px:
            df = pd.DataFrame(px)[["date", "open", "high", "low", "close", "volume"]]
            adj = fd.get("historical-price-eod/dividend-adjusted", symbol=r.sym, **rng)
            if adj:
                df = df.merge(pd.DataFrame(adj)[["date", "adjClose"]], on="date", how="left")
            df.sort_values("date").to_csv(out / f"{key}.csv", index=False)
            rec["n"], rec["has_adj"] = len(df), bool(adj)
            rec["splits"] = [[x["date"], x["numerator"], x["denominator"]]
                             for x in (fd.get("splits", symbol=r.sym) or [])]
        with lock:
            mf.write(json.dumps(rec) + "\n")
            mf.flush()
        return rec["n"] > 0

    hits = 0
    with open(meta_path, "a") as mf, ThreadPoolExecutor(fd.WORKERS) as ex:
        for i, hit in enumerate(ex.map(one, todo), 1):
            hits += hit
            if i % 500 == 0:
                print(f"{i}/{len(todo)} with data {hits}", flush=True)
    print(f"delisted full done: {hits} of {len(todo)} returned prices; {len(fd.FAILED)} calls failed after retries",
          flush=True)


def etf():
    import yfinance as yf
    out = BS / "etf"
    out.mkdir(parents=True, exist_ok=True)
    df = yf.download(ETFS, start="2004-01-01", auto_adjust=False, actions=True, group_by="ticker",
                     threads=True, progress=False)
    for t in ETFS:
        x = df[t].dropna(subset=["Close"])
        x.index = pd.to_datetime(x.index).tz_localize(None)
        x.index.name = "date"
        x.to_csv(out / f"{t}.csv")
        print(t, len(x), x.index[0].date(), flush=True)


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else ""
    if what == "delisted":
        if not fd.KEY:
            sys.exit("FMP_API_KEY missing - run through `railway run`")
        delisted()
    elif what == "etf":
        etf()
    else:
        sys.exit(__doc__)
