"""Shared loaders for the v6 Whale Flow study (research_plan_v6_whale_flow.md)."""
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

DATA = Path("C:/dev/Trader-v3-data")
WF = DATA / "whale_flow"
SYM = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


def norm_symbol(s: pd.Series) -> pd.Series:
    """Symbol as typed on a Form 4 -> FMP-style symbol (same rule as fetch_delisted.targets)."""
    return (s.fillna("").str.upper().str.strip()
            .str.replace(r"^(NYSE|NASDAQ|AMEX|NYSEMKT)\s*[:\-]\s*", "", regex=True)
            .str.replace(".", "-", regex=False))


def _finish(df, ratio):
    """Add `f` = product of split ratios dated after each bar, and raw (as-traded) columns."""
    r = ratio.reindex(df.index).fillna(1.0).replace(0, 1.0)
    rc = r[::-1].cumprod()[::-1]
    df["f"] = (rc / r).values
    for c in ("open", "high", "low", "close"):
        df["raw_" + c] = df[c] * df.f
    df["raw_volume"] = df.volume / df.f
    return df


def load_survivor(ticker):
    """Yahoo panel: prices and volume split-adjusted, `adj` also dividend-adjusted."""
    p = WF / "bars" / f"{ticker}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=["date"]).set_index("date").sort_index()
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close",
                            "Adj Close": "adj", "Volume": "volume", "Stock Splits": "split"})
    df = df[df.close > 0]
    return _finish(df, df.split.where(df.split > 0, 1.0))


_META = None


def delisted_meta():
    global _META
    if _META is None:
        _META = {}
        p = WF / "delisted_meta.jsonl"
        if p.exists():
            with open(p) as f:
                for l in f:
                    if l.strip():
                        d = json.loads(l)
                        _META[d["key"]] = d
    return _META


def load_delisted(key):
    """FMP history for a `{cik}_{SYMBOL}` pair: split-adjusted OHLCV, `adj` dividend-adjusted."""
    p = WF / "bars_delisted" / f"{key}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, parse_dates=["date"]).set_index("date").sort_index()
    df = df[df.close > 0]
    if len(df) == 0:
        return None
    df["adj"] = df["adjClose"] if "adjClose" in df.columns else df.close
    df["adj"] = df.adj.fillna(df.close)
    ratio = pd.Series(1.0, index=df.index)
    for d, num, den in delisted_meta().get(key, {}).get("splits", []):
        d = pd.Timestamp(d)
        if den and df.index[0] < d <= df.index[-1]:
            i = df.index.searchsorted(d)  # first bar on/after the split date
            ratio.iloc[i] *= num / den
    return _finish(df, ratio)


def load_spy():
    return load_survivor("SPY")


def trading_calendar():
    return load_spy().index


def next_session(cal: pd.DatetimeIndex, dates: pd.Series) -> pd.Series:
    """First trading day strictly after each date (NaT past the end of the calendar)."""
    i = cal.searchsorted(dates.values, side="right")
    out = np.where(i < len(cal), cal.values[np.minimum(i, len(cal) - 1)], np.datetime64("NaT"))
    return pd.Series(out, index=dates.index)
