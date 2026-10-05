"""Point-in-time index membership for the top-N study.

  S&P 500     fja05680/sp500 "Historical Components & Changes (Updated)" (daily snapshots since 1996)
  Nasdaq-100  rebuilt backwards from today's list with Wikipedia's change table (reaches 2007-02)

Tickers are as they were on the day; ALIAS maps an old ticker to the symbol the data vendor files
the same company under today.
"""
import re
from pathlib import Path

import pandas as pd

TN = Path("C:/dev/Trader-v3-data/top_n")


def sp500():
    df = pd.read_csv(TN / "sp500_hist.csv", parse_dates=["date"])
    return {d: set(t.split(",")) for d, t in zip(df.date, df.tickers)}


def _cells(row):
    out = []
    for line in row.strip().split("\n"):
        if line.startswith("|") and not line.startswith("|}"):
            out.append(re.sub(r"<ref.*", "", line[1:]).strip())
    return out


def ndx_changes():
    txt = (TN / "ndx_hist.txt").read_text(encoding="utf-8")
    body = txt[txt.index('id="changes"'):txt.index("==References==")]
    rows = []
    for row in body.split("\n|-")[2:]:
        c = _cells(row)
        if len(c) < 5:
            continue
        d = pd.to_datetime(c[0], errors="coerce")
        if pd.isna(d):
            continue
        rows.append((d, c[1].strip(), c[3].strip(), c[5] if len(c) > 5 else ""))
    return pd.DataFrame(rows, columns=["date", "added", "removed", "reason"])


def ndx_now():
    txt = (TN / "ndx_now.txt").read_text(encoding="utf-8")
    body = txt[txt.index('id="constituents"'):]
    body = body[:body.index("\n|}")]
    return {m.group(1) for m in re.finditer(r"^\|\s*([A-Z.]+)\s*\|\|", body, re.M)}


if __name__ == "__main__":
    ch = ndx_changes()
    now = ndx_now()
    print(len(now), "current NDX;", len(ch), "changes", ch.date.min().date(), "..", ch.date.max().date())
    cur = set(now)
    for r in ch.itertuples():           # newest first: undo each change
        if r.added:
            if r.added in cur:
                cur.discard(r.added)
            else:
                print("  undo-add miss ", r.date.date(), r.added, "|", r.reason[:70])
        if r.removed:
            if r.removed in cur:
                print("  undo-rem dup  ", r.date.date(), r.removed)
            cur.add(r.removed)
    print("members at start:", len(cur), sorted(cur))
    sp = sp500()
    ds = sorted(sp)
    print("S&P snapshots", len(ds), ds[0].date(), ds[-1].date())
    allt = set().union(*[sp[d] for d in ds if d >= pd.Timestamp("2004-12-01")])
    print("S&P tickers since 2004-12:", len(allt))
