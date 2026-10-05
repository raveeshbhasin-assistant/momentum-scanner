"""
Nasdaq Leaders — daily scan (standalone; imports nothing from the scanner, themes, ignition or pullback).

Two lists of ten Nasdaq-100 stocks, re-picked once a month (rules fixed 2026-10-05 from
research_findings_top_n.md, studies TN1-TN3):

    RANK   at the last close of the month
    TRADE  at the next session's close: ten names in equal parts, held until the next rebalance
    SIZE   the ten largest by market value (shares outstanding x close)
    MOM    the ten with the highest total return from 252 to 21 sessions before the ranking day

The scan runs every day. Most days there is nothing to do; it shows the lists as held, what would
change if the month ended today, and on the last session of the month the exact buys and sells
for the next close.

Output (nasdaq10/data/):
    latest.json           the lists as held, the pending rebalance, the month-by-month ledger
    runs.jsonl            one line per run
    history/<asof>.json   a copy of every run's latest.json
    shares.json           last known shares outstanding (used when Yahoo does not answer)

History is append-only: once a month's picks are recorded they are never re-picked, and a closed
month keeps its recorded returns (carry_forward). The scan refuses to publish on missing or stale
bars (check_coverage).

    python nasdaq10/scan.py
"""
from __future__ import annotations

import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from pandas.tseries.holiday import (AbstractHolidayCalendar, GoodFriday, Holiday, USLaborDay,
                                    USMartinLutherKingJr, USMemorialDay, USPresidentsDay, USThanksgivingDay,
                                    nearest_workday)
from pandas.tseries.offsets import CustomBusinessDay

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"

BENCH = "QQQ"
N = 10
MOM_FAR, MOM_NEAR = 252, 21
COST_SIDE = 0.001
STRATS = {"SIZE": "Ten largest", "MOM": "Ten strongest"}
# The back-tests end 2026-09-30. The first rebalance tracked here was ranked on that close and
# traded on 2026-10-01, so every month in the ledger is out of sample. Months ranked before
# LIVE_FROM are replayed from history (the page did not exist yet).
LEDGER_START = "2026-10-01"
LIVE_FROM = "2026-10-05"
VERSION = 1
WIKI_URL = "https://en.wikipedia.org/w/index.php?title=List_of_NASDAQ-100_companies&action=raw"
# a second share class of a company already in the list
SECOND_CLASS = {"GOOG": "GOOGL", "FOX": "FOXA", "LBTYK": "LBTYA", "LBTYB": "LBTYA"}

RULE = {"n": N, "mom_far": MOM_FAR, "mom_near": MOM_NEAR, "cost_pct_side": COST_SIDE * 100, "bench": BENCH,
        "ledger_start": LEDGER_START, "live_from": LIVE_FROM}

# Back-test record (research_findings_top_n.md). Equal weight, monthly, 10 bps a side, 2007-03..2026-09.
BACKTEST = {
    "verdict": "NOT CONFIRMED",
    "window": "2007-03 to 2026-09",
    "bench": {"cagr": 16.5, "max_dd": -53, "vol": 22.2},
    "SIZE": {"cagr": 19.5, "excess": 3.0, "max_dd": -48, "vol": 24.6, "years_ahead": 13, "years": 19,
             "first": 1.4, "second": 4.3, "lo": -0.3, "hi": 6.3, "worst_year": [2022, -42.7, -32.6]},
    "MOM": {"cagr": 18.8, "excess": 2.3, "max_dd": -57, "vol": 31.5, "years_ahead": 9, "years": 19,
            "first": 1.8, "second": 2.8, "lo": -6.5, "hi": 19.1, "worst_year": [2008, -39.1, -41.7]},
}


# ------------------------------------------------------------------ calendar
class _NYSE(AbstractHolidayCalendar):
    rules = [Holiday("New Year", month=1, day=1, observance=nearest_workday), USMartinLutherKingJr,
             USPresidentsDay, GoodFriday, USMemorialDay,
             Holiday("Juneteenth", month=6, day=19, start_date="2022-06-19", observance=nearest_workday),
             Holiday("Independence Day", month=7, day=4, observance=nearest_workday), USLaborDay,
             USThanksgivingDay, Holiday("Christmas", month=12, day=25, observance=nearest_workday)]


_SESSION = CustomBusinessDay(calendar=_NYSE())


def next_session(d: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(d) + _SESSION


def month_end_session(d: pd.Timestamp) -> pd.Timestamp:
    """The last session of d's month."""
    d = pd.Timestamp(d)
    while next_session(d).month == d.month:
        d = next_session(d)
    return d


# ------------------------------------------------------------------ data
def _parse_members(text: str) -> dict[str, str]:
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        t, _, name = line.partition("\t")
        out[t.strip().replace(".", "-")] = name.strip() or t.strip()
    return out


def pinned_universe() -> dict[str, str]:
    return _parse_members((HERE / "universe.txt").read_text(encoding="utf-8"))


def wiki_universe(raw: str) -> dict[str, str]:
    """Ticker -> company from the wikitext of Wikipedia's constituents table."""
    body = raw[raw.index('id="constituents"'):]
    body = body[:body.index("\n|}")]
    out = {}
    for m in re.finditer(r"^\|\s*([A-Z.]+)\s*\|\|\s*(.*?)\s*\|\|", body, re.M):
        name = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", m.group(2))
        out[m.group(1).replace(".", "-")] = re.sub(r"<ref.*", "", name).strip()
    return out


def load_universe() -> tuple[dict[str, str], str]:
    """Today's members. Wikipedia if it answers and looks like the index; the pinned file otherwise."""
    pinned = pinned_universe()
    try:
        req = urllib.request.Request(WIKI_URL, headers={"User-Agent": "nasdaq10-scan (personal research tracker)"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            live = wiki_universe(resp.read().decode("utf-8"))
        if 95 <= len(live) <= 110 and len(set(live) & set(pinned)) >= 0.8 * len(pinned):
            return live, "wikipedia"
    except Exception:
        pass
    return pinned, "pinned"


def one_class(members: dict[str, str]) -> dict[str, str]:
    return {t: n for t, n in members.items() if SECOND_CLASS.get(t) not in members}


def download(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Daily close and dividend-adjusted close."""
    import yfinance as yf

    start = (pd.Timestamp(LEDGER_START) - pd.Timedelta(days=420)).strftime("%Y-%m-%d")
    last_err = None
    for attempt in range(4):
        try:
            d = yf.download(tickers, start=start, auto_adjust=False, actions=False, group_by="ticker",
                            threads=True, progress=False)
            c = pd.DataFrame({t: d[t]["Close"] for t in tickers if t in d.columns.get_level_values(0)})
            a = pd.DataFrame({t: d[t]["Adj Close"] for t in tickers if t in d.columns.get_level_values(0)})
            for x in (c, a):
                x.index = pd.to_datetime(x.index).tz_localize(None)
            return c.dropna(how="all"), a.dropna(how="all")
        except Exception as e:  # network or schema hiccup: wait and retry
            last_err = e
            time.sleep(20 * (attempt + 1))
    raise RuntimeError(f"download failed: {last_err}")


def fetch_shares(tickers: list[str]) -> dict[str, float]:
    """Shares outstanding (all classes). Yahoo when it answers, the last known figure otherwise."""
    import yfinance as yf

    known = {}
    for p in (HERE / "shares_seed.json", DATA / "shares.json"):
        if p.exists():
            try:
                known.update(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                pass
    for t in tickers:
        for attempt in range(2):
            try:
                s = float(yf.Ticker(t).fast_info["shares"])
                if s > 0:
                    known[t] = s
                    break
            except Exception:
                time.sleep(2 * (attempt + 1))
    return {t: known[t] for t in tickers if t in known}


def drop_unfinished_session(c, a, now: datetime | None = None):
    """A run during market hours would see a half-finished bar; the rules use closes only."""
    now = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("America/New_York"))
    if len(c) and c.index[-1].date() == now.date() and (now.hour, now.minute) < (16, 10):
        return c.iloc[:-1], a.iloc[:-1]
    return c, a


def check_coverage(close: pd.DataFrame, members: dict, shares: dict, today: datetime | None = None) -> None:
    today = today or datetime.now(timezone.utc)
    if len(close) < MOM_FAR + 5:
        raise RuntimeError(f"only {len(close)} sessions of history")
    if BENCH not in close.columns or pd.isna(close[BENCH].iloc[-1]):
        raise RuntimeError(f"no {BENCH} bar on {close.index[-1].date()}")
    have = [t for t in members if t in close.columns and not pd.isna(close[t].iloc[-1])]
    if len(have) < 0.9 * len(members):
        raise RuntimeError(f"bars for only {len(have)} of {len(members)} members on {close.index[-1].date()}")
    if len([t for t in have if t in shares]) < 0.9 * len(members):
        raise RuntimeError(f"shares outstanding for only {len([t for t in have if t in shares])} of {len(members)} members")
    age = (today.replace(tzinfo=None) - close.index[-1].to_pydatetime()).days
    if age > 5:
        raise RuntimeError(f"latest bar is {age} days old ({close.index[-1].date()})")


# ------------------------------------------------------------------ rules
def _r(x, n=2):
    return None if x is None or pd.isna(x) else round(float(x), n)


def rank(code: str, i: int, close: pd.DataFrame, adj: pd.DataFrame, members: dict, shares: dict) -> list[dict]:
    """Every eligible member at session i, best first, with the number it was ranked on."""
    rows = []
    for t in members:
        if t not in close.columns or pd.isna(close[t].iloc[i]):
            continue
        if code == "SIZE":
            if t not in shares:
                continue
            rows.append((t, shares[t] * close[t].iloc[i] / 1e9))
        else:
            if i - MOM_FAR < 0 or pd.isna(adj[t].iloc[i - MOM_FAR]) or pd.isna(adj[t].iloc[i - MOM_NEAR]):
                continue
            rows.append((t, (adj[t].iloc[i - MOM_NEAR] / adj[t].iloc[i - MOM_FAR] - 1) * 100))
    rows.sort(key=lambda r: -r[1])
    return [{"ticker": t, "rank": k + 1, "metric": _r(v, 1)} for k, (t, v) in enumerate(rows)]


def build_months(close, adj, members, shares) -> list[dict]:
    """One record per month from LEDGER_START: ranked at the prior month's last close. If the latest
    bar is the last session of its month, next month's picks are added as NEW (to be bought at the
    next close)."""
    idx = close.index
    first = [i for i in range(1, len(idx)) if idx[i].month != idx[i - 1].month and idx[i] >= pd.Timestamp(LEDGER_START)]
    out = []
    for a in first:
        s = a - 1
        out.append({"month": idx[a].strftime("%Y-%m"), "signal": str(idx[s].date()), "trade": str(idx[a].date()),
                    "live": str(idx[s].date()) >= LIVE_FROM, "status": "OPEN",
                    "picks": {c: rank(c, s, close, adj, members, shares)[:N] for c in STRATS}})
    last = len(idx) - 1
    if month_end_session(idx[last]) == idx[last]:
        nxt = next_session(idx[last])
        out.append({"month": nxt.strftime("%Y-%m"), "signal": str(idx[last].date()), "trade": str(nxt.date()),
                    "live": str(idx[last].date()) >= LIVE_FROM, "status": "NEW",
                    "picks": {c: rank(c, last, close, adj, members, shares)[:N] for c in STRATS}})
    return out


def carry_forward(months: list[dict], prev: list[dict] | None) -> list[dict]:
    """Append-only history: a month's picks are whatever was first recorded; a closed month keeps
    its recorded returns."""
    if not prev:
        return months
    now = {m["month"]: m for m in months}
    out = []
    for p in prev:
        cur = now.pop(p["month"], None)
        if p.get("status") == "CLOSED" or cur is None:
            out.append(p)
        else:
            out.append({**cur, "picks": p["picks"], "signal": p["signal"], "trade": p["trade"], "live": p.get("live", cur["live"])})
    out += list(now.values())
    return sorted(out, key=lambda m: m["month"])


def _ret(adj: pd.DataFrame, tickers: list[str], a: int, b: int):
    """Equal-weight buy-and-hold return from session a to b; a name that stops trading keeps its last price."""
    px = adj.ffill()
    r = [px[t].iloc[b] / px[t].iloc[a] - 1 for t in tickers if t in px.columns and not pd.isna(px[t].iloc[a])]
    return sum(r) / len(r) if r else None


def settle(months: list[dict], close: pd.DataFrame, adj: pd.DataFrame) -> list[dict]:
    """Fill in entry dates, returns and status from prices. Closed months are left as recorded."""
    idx = close.index
    pos = {str(d.date()): i for i, d in enumerate(idx)}
    last = len(idx) - 1
    for k, m in enumerate(months):
        if m.get("status") == "CLOSED":
            continue
        a = pos.get(m["trade"])
        if a is None:                                   # trade session has not happened yet
            m["status"] = "NEW"
            continue
        nxt = pos.get(months[k + 1]["trade"]) if k + 1 < len(months) else None
        b = nxt if nxt is not None else last
        m["status"] = "CLOSED" if nxt is not None else "OPEN"
        m["through"] = str(idx[b].date())
        m["ret"] = {}
        prev_picks = months[k - 1]["picks"] if k else None
        for c in STRATS:
            names = [p["ticker"] for p in m["picks"][c]]
            gross = _ret(adj, names, a, b)
            changed = len(set(names) - {p["ticker"] for p in prev_picks[c]}) if prev_picks else len(names)
            cost = COST_SIDE * (2 if prev_picks else 1) * changed / max(len(names), 1)
            m["ret"][c] = _r((gross - cost) * 100) if gross is not None else None
        m["ret"][BENCH] = _r((adj[BENCH].iloc[b] / adj[BENCH].iloc[a] - 1) * 100)
    return months


def _chain(months: list[dict], key: str):
    v = 1.0
    for m in months:
        if m.get("ret") and m["ret"].get(key) is not None:
            v *= 1 + m["ret"][key] / 100
    return _r((v - 1) * 100)


def compute(close: pd.DataFrame, adj: pd.DataFrame, members: dict, shares: dict, prev: dict | None = None,
            universe_source: str = "pinned") -> dict:
    members = one_class(members)
    idx = close.index
    last = len(idx) - 1
    asof = str(idx[last].date())
    months = settle(carry_forward(build_months(close, adj, members, shares), (prev or {}).get("months")), close, adj)
    held = next((m for m in reversed(months) if m["status"] in ("OPEN", "CLOSED")), None)
    pending = next((m for m in months if m["status"] == "NEW"), None)
    pos = {str(d.date()): i for i, d in enumerate(idx)}
    px = adj.ffill()
    strategies = []
    for c, label in STRATS.items():
        now_rank = rank(c, last, close, adj, members, shares)
        by_ticker = {r["ticker"]: r for r in now_rank}
        holdings = []
        if held:
            a = pos[held["trade"]]
            for p in held["picks"][c]:
                t = p["ticker"]
                ok = t in close.columns
                holdings.append({"ticker": t, "name": members.get(t, t), "rank_at_pick": p["rank"], "metric_at_pick": p["metric"],
                                 "entry_px": _r(close[t].iloc[a]) if ok else None,
                                 "last": _r(close[t].ffill().iloc[last]) if ok else None,
                                 "ret": _r((px[t].iloc[last] / px[t].iloc[a] - 1) * 100) if ok else None,
                                 "rank_now": by_ticker.get(t, {}).get("rank"), "metric_now": by_ticker.get(t, {}).get("metric")})
        cur = {h["ticker"] for h in holdings}
        top_now = [r["ticker"] for r in now_rank[:N]]
        target = [p["ticker"] for p in pending["picks"][c]] if pending else None
        action = None
        if target is not None:
            action = {"buy": [t for t in target if t not in cur], "sell": sorted(cur - set(target)),
                      "keep": [t for t in target if t in cur]}
        done = [m for m in months if m.get("ret") and m["ret"].get(c) is not None]
        closed = [m for m in done if m["status"] == "CLOSED"]
        strategies.append({
            "code": c, "label": label, "holdings": holdings,
            "top_now": [{**r, "name": members.get(r["ticker"], r["ticker"])} for r in now_rank[:N + 3]],
            "would_enter": [t for t in top_now if t not in cur] if held else top_now,
            "would_leave": sorted(cur - set(top_now)),
            "action": action,
            "this_month": held["ret"].get(c) if held and held.get("ret") else None,
            "since_start": _chain(done, c), "bench_since_start": _chain(done, BENCH),
            "months_closed": len(closed), "months_ahead": sum(m["ret"][c] > m["ret"][BENCH] for m in closed),
        })
    me = month_end_session(idx[last])
    to_go = 0
    d = idx[last]
    while d < me:
        d, to_go = next_session(d), to_go + 1
    since = (prev or {}).get("asof")
    return {
        "version": VERSION, "asof": asof, "run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "rule": RULE, "backtest": BACKTEST,
        "universe": {"n": len(members), "source": universe_source},
        "next": {"rank_on": str(me.date()), "trade_on": str(next_session(me).date()), "sessions_to_rank": to_go,
                 "pending": pending is not None},
        "held_month": held["month"] if held else None, "held_since": held["trade"] if held else None,
        "bench_this_month": held["ret"].get(BENCH) if held and held.get("ret") else None,
        "strategies": strategies, "months": months,
        "summary": {"since": since, "rebalance_pending": pending is not None,
                    "buys": {s["code"]: s["action"]["buy"] for s in strategies if s["action"]},
                    "sells": {s["code"]: s["action"]["sell"] for s in strategies if s["action"]}},
    }


def read_prev() -> dict | None:
    p = DATA / "latest.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        return d if d.get("version") == VERSION else None
    except Exception:
        return None


def main() -> int:
    DATA.mkdir(exist_ok=True)
    (DATA / "history").mkdir(exist_ok=True)
    members, source = load_universe()
    tickers = sorted(one_class(members)) + [BENCH]
    c, a = drop_unfinished_session(*download(tickers))
    shares = fetch_shares(sorted(one_class(members)))
    check_coverage(c, one_class(members), shares)
    out = compute(c, a, members, shares, read_prev(), source)
    body = json.dumps(out, indent=1)
    (DATA / "latest.json").write_text(body, encoding="utf-8")
    (DATA / "history" / f"{out['asof']}.json").write_text(body, encoding="utf-8")
    (DATA / "shares.json").write_text(json.dumps(shares), encoding="utf-8")
    s = out["summary"]
    with open(DATA / "runs.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"run_at": out["run_at"], "asof": out["asof"], "since": s["since"],
                             "pending": s["rebalance_pending"], "buys": s["buys"], "sells": s["sells"],
                             "members": out["universe"]["n"], "source": source}) + "\n")
    print(f"nasdaq10 scan asof {out['asof']}: members {out['universe']['n']} ({source}), "
          f"rebalance pending {s['rebalance_pending']}, buys {s['buys']}, sells {s['sells']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
