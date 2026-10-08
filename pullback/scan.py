"""
Pullback Watch — daily scan (standalone; imports nothing from the scanner, themes or ignition).

The rule (frozen 2026-10-05 as "D1" in research/blank_slate/BS_DESIGN.md):

    SIGNAL at the close = 5-session return <= -3%  AND  close > 200-session average
    BUY  at the next session's open
    SELL at the close five sessions after the signal
    one position per fund; a fund that is being held cannot signal again

Tracked funds: SPY QQQ IWM DIA MDY IJR VTI RSP. Each fund is tracked on its own (the
back-tested portfolio held at most five at a time).

Output (pullback/data/):
    latest.json           today's state of every fund + the full trade ledger
    runs.jsonl            one line per run: what was new since the previous run
    history/<asof>.json   a copy of every run's latest.json

History is append-only: a signal or a closed trade that an earlier run recorded is never
dropped or re-priced by a later run, even if Yahoo revises its bars (carry_forward).
A trailing bar that not every fund has yet (Yahoo serves the day's row with empty prices
for an hour or more after the close) is dropped, not scored (settle); the scan refuses to
publish if any fund's bars are missing or stale (check_coverage).

    python pullback/scan.py
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"

FUNDS = {
    "SPY": "S&P 500", "QQQ": "Nasdaq-100", "IWM": "Russell 2000", "DIA": "Dow Jones Industrial Average",
    "MDY": "S&P MidCap 400", "IJR": "S&P SmallCap 600", "VTI": "Total US stock market",
    "RSP": "S&P 500, equal weight",
}
LOOKBACK, DROP, SMA, HOLD = 5, -0.03, 200, 5
COST_SIDE = 0.0002
# The rule was tested on signals through 2026-06-30. Everything after that is out of sample.
# Signals from LEDGER_START to the day before LIVE_FROM are replayed from history (the page
# did not exist yet); from LIVE_FROM on they are recorded as they happen.
LEDGER_START = "2026-07-01"
LIVE_FROM = "2026-10-05"
VERSION = 1

RULE = {"lookback": LOOKBACK, "drop_pct": DROP * 100, "sma": SMA, "hold": HOLD,
        "cost_pct_round_trip": COST_SIDE * 2 * 100, "ledger_start": LEDGER_START, "live_from": LIVE_FROM}

# Back-test record of the frozen rule (research_findings_blank_slate.md). Five 20% slots.
BACKTEST = {
    "verdict": "NOT CONFIRMED",
    "train": {"label": "Training", "years": "2006-2019", "trades": 348, "weeks": 131, "hit": 65.5, "base": 55.6,
              "lift": 9.9, "lift_lo": 1.1, "lift_hi": 18.3, "avg_trade": 0.61, "avg_lo": 0.08, "avg_hi": 1.14,
              "cagr": 3.0, "exposure": 8, "max_dd": -13, "spy_cagr": 8.4, "spy_dd": -55},
    "holdout": {"label": "Holdout (run once)", "years": "2020-2026", "trades": 190, "weeks": 75, "hit": 66.8,
                "base": 55.9, "lift": 11.0, "lift_lo": -1.2, "lift_hi": 21.9, "avg_trade": 0.74, "avg_lo": -0.05,
                "avg_hi": 1.47, "cagr": 4.2, "exposure": 9, "max_dd": -11, "spy_cagr": 15.4, "spy_dd": -34},
    "holdout_by_year": [[2020, 30, 56.7], [2021, 48, 72.9], [2022, 9, 44.4], [2023, 23, 39.1], [2024, 37, 94.6],
                        [2025, 27, 63.0], [2026, 16, 62.5]],
    "p_raw": 0.037, "p_adjusted": 0.11,
}


# ------------------------------------------------------------------ data
def download() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Daily open, close and dividend-adjusted close for every fund."""
    import yfinance as yf

    start = (pd.Timestamp(LEDGER_START) - pd.Timedelta(days=420)).strftime("%Y-%m-%d")
    tickers = list(FUNDS)
    last_err = None
    for attempt in range(4):
        try:
            # threads=False: eight tickers take a second, and yfinance's first-run timezone cache
            # (sqlite) threw "database is locked" under concurrent writes on a fresh runner
            # (2026-10-06, QQQ came back empty).
            d = yf.download(tickers, start=start, auto_adjust=False, actions=False, group_by="ticker",
                            threads=False, progress=False)
            got = set(d.columns.get_level_values(0)) if isinstance(d.columns, pd.MultiIndex) else set()
            # yf.download does not raise for a ticker that failed: it prints a notice and leaves the
            # column out or empty. Treat that as a failed download so the retry loop sees it.
            empty = [t for t in tickers if t not in got or d[t]["Close"].dropna().empty]
            if empty:
                raise RuntimeError(f"no bars for {empty}")
            o = pd.DataFrame({t: d[t]["Open"] for t in tickers})
            c = pd.DataFrame({t: d[t]["Close"] for t in tickers})
            a = pd.DataFrame({t: d[t]["Adj Close"] for t in tickers})
            for x in (o, c, a):
                x.index = pd.to_datetime(x.index).tz_localize(None)
            return settle(o, c, a)
        except Exception as e:  # network, schema or a missing ticker: wait and retry
            last_err = e
            print(f"download attempt {attempt + 1} failed: {e}", file=sys.stderr)
            time.sleep(20 * (attempt + 1))
    raise RuntimeError(f"download failed: {last_err}")


def settle(o, c, a):
    """Keep only sessions every fund has a close for.

    For an hour or more after the close Yahoo serves the day's row with empty prices (all
    eight funds on 2026-10-07 at 21:19 ET; GitHub starts the 21:40 UTC cron 3-4 hours late),
    and sometimes only part of the list has settled. Scoring such a row would either fail
    check_coverage or, worse, read a half-filled day as a close. Dropping it means the run
    scores the previous session and the pre-open run (07:00 UTC) picks up the settled bar."""
    keep = c.notna().all(axis=1)
    if len(c) > 1 and not keep.iloc[-1]:
        print(f"dropping unsettled bar {c.index[-1].date()}: "
              f"missing {[t for t in c.columns if pd.isna(c[t].iloc[-1])]}", file=sys.stderr)
    return o[keep], c[keep], a[keep]


def drop_unfinished_session(o, c, a, now: datetime | None = None):
    """A run during market hours would see a half-finished bar; the rule uses closes only."""
    now = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("America/New_York"))
    if len(c) and c.index[-1].date() == now.date() and (now.hour, now.minute) < (16, 10):
        return o.iloc[:-1], c.iloc[:-1], a.iloc[:-1]
    return o, c, a


def check_coverage(close: pd.DataFrame, today: datetime | None = None) -> None:
    today = today or datetime.now(timezone.utc)
    if len(close) < SMA + LOOKBACK + 5:
        raise RuntimeError(f"only {len(close)} sessions of history")
    missing = [t for t in FUNDS if t not in close.columns or pd.isna(close[t].iloc[-1])]
    if missing:
        raise RuntimeError(f"no bar on {close.index[-1].date()} for {missing}")
    age = (today.replace(tzinfo=None) - close.index[-1].to_pydatetime()).days
    if age > 5:
        raise RuntimeError(f"latest bar is {age} days old ({close.index[-1].date()})")


# ------------------------------------------------------------------ rule
def _r(x, n=4):
    return None if x is None or pd.isna(x) else round(float(x), n)


def signals(close: pd.DataFrame) -> dict:
    r5 = close / close.shift(LOOKBACK) - 1
    sma = close.rolling(SMA, min_periods=SMA).mean()
    return {"r5": r5, "sma": sma, "sig": (r5 <= DROP) & (close > sma)}


def build_trades(open_: pd.DataFrame, close: pd.DataFrame, adj: pd.DataFrame, f: dict) -> list[dict]:
    """Replay the rule from LEDGER_START. One trade per signal; one position per fund."""
    idx = close.index
    start = idx.searchsorted(pd.Timestamp(LEDGER_START))
    last = len(idx) - 1
    adj_open = open_ * adj / close
    trades = []
    for t in FUNDS:
        free_from = start
        for i in range(start, len(idx)):
            if i < free_from or not bool(f["sig"][t].iloc[i]):
                continue
            exit_i = i + HOLD
            tr = {"ticker": t, "signal": str(idx[i].date()), "live": str(idx[i].date()) >= LIVE_FROM,
                  "r5_at_signal": _r(f["r5"][t].iloc[i] * 100, 2), "close_at_signal": _r(close[t].iloc[i], 2),
                  "sma_at_signal": _r(f["sma"][t].iloc[i], 2),
                  "entry": None, "entry_px": None, "exit": None, "exit_px": None, "ret": None,
                  "sessions_held": 0, "last_close": _r(close[t].iloc[last], 2), "open_ret": None}
            if i == last:
                tr["status"] = "NEW"                      # buy at the next open
            else:
                tr["entry"], tr["entry_px"] = str(idx[i + 1].date()), _r(open_[t].iloc[i + 1], 2)
                basis = adj_open[t].iloc[i + 1]
                if exit_i <= last:
                    tr["status"] = "CLOSED"
                    tr["exit"], tr["exit_px"] = str(idx[exit_i].date()), _r(close[t].iloc[exit_i], 2)
                    tr["ret"] = _r((adj[t].iloc[exit_i] / basis - 1 - 2 * COST_SIDE) * 100, 2)
                    tr["sessions_held"] = HOLD
                else:
                    tr["status"] = "OPEN"
                    tr["sessions_held"] = last - i
                    tr["open_ret"] = _r((adj[t].iloc[last] / basis - 1 - 2 * COST_SIDE) * 100, 2)
            trades.append(tr)
            # exits are processed before entries, so a signal on the exit day can start a new trade
            free_from = exit_i
    return trades


def carry_forward(trades: list[dict], prev: list[dict] | None) -> list[dict]:
    """Append-only history: whatever an earlier run recorded stays. A trade already closed keeps
    its recorded numbers; a recorded trade the rebuild no longer produces is kept and marked."""
    if not prev:
        return trades
    now = {(t["ticker"], t["signal"]): t for t in trades}
    out = []
    for p in prev:
        k = (p["ticker"], p["signal"])
        cur = now.pop(k, None)
        if p.get("status") == "CLOSED":
            out.append(p)                                  # recorded result is final
        elif cur is not None:
            out.append(cur)
        else:
            out.append({**p, "kept": True})                # revised bars no longer show it
    out += list(now.values())
    return sorted(out, key=lambda t: (t["signal"], t["ticker"]))


def fund_states(close: pd.DataFrame, f: dict, trades: list[dict]) -> list[dict]:
    last = len(close) - 1
    open_by = {t["ticker"]: t for t in trades if t.get("status") in ("NEW", "OPEN")}
    out = []
    for t, name in FUNDS.items():
        c, sma, r5 = close[t].iloc[last], f["sma"][t].iloc[last], f["r5"][t].iloc[last]
        above = bool(c > sma) if not pd.isna(sma) else False
        tr = open_by.get(t)
        if tr and tr["status"] == "NEW":
            status = "SIGNAL"
        elif tr:
            status = "HOLDING"
        elif not above:
            status = "OFF"
        else:
            status = "WATCH"
        # the close five sessions ago is the reference: the fund signals at or below 97% of it
        ref = close[t].iloc[last - LOOKBACK + 1] if last - LOOKBACK + 1 >= 0 else None
        out.append({"ticker": t, "name": name, "close": _r(c, 2), "r5": _r(r5 * 100, 2), "sma": _r(sma, 2),
                    "vs_sma": _r((c / sma - 1) * 100, 2) if not pd.isna(sma) else None, "above_sma": above,
                    "status": status,
                    "to_trigger": _r((DROP - r5) * 100, 2) if above and status == "WATCH" else None,
                    "next_trigger_close": _r(ref * (1 + DROP), 2) if ref is not None else None,
                    "sessions_held": tr["sessions_held"] if tr else None,
                    "sessions_left": (HOLD - tr["sessions_held"]) if tr else None,
                    "open_ret": tr.get("open_ret") if tr else None})
    return out


def base_rate(open_: pd.DataFrame, close: pd.DataFrame, adj: pd.DataFrame) -> dict:
    """The same funds bought on ANY day since LEDGER_START and held the same five sessions."""
    idx = close.index
    start = idx.searchsorted(pd.Timestamp(LEDGER_START))
    adj_open = open_ * adj / close
    rets = []
    for t in FUNDS:
        r = adj[t].shift(-HOLD) / adj_open[t].shift(-1) - 1 - 2 * COST_SIDE
        rets += r.iloc[start:].dropna().tolist()
    if not rets:
        return {"n": 0, "hit": None, "avg": None}
    s = pd.Series(rets)
    return {"n": len(s), "hit": _r((s > 0).mean() * 100, 1), "avg": _r(s.mean() * 100, 2)}


def _stats(rows: list[dict]) -> dict:
    r = [t["ret"] for t in rows if t.get("ret") is not None]
    if not r:
        return {"n": 0, "wins": 0, "hit": None, "avg": None, "best": None, "worst": None}
    return {"n": len(r), "wins": sum(x > 0 for x in r), "hit": round(100 * sum(x > 0 for x in r) / len(r), 1),
            "avg": round(sum(r) / len(r), 2), "best": max(r), "worst": min(r)}


def compute(open_: pd.DataFrame, close: pd.DataFrame, adj: pd.DataFrame, prev: dict | None = None) -> dict:
    f = signals(close)
    trades = carry_forward(build_trades(open_, close, adj, f), (prev or {}).get("trades"))
    asof = str(close.index[-1].date())
    since = (prev or {}).get("asof")
    closed = [t for t in trades if t.get("status") == "CLOSED"]
    new_signals = [t["ticker"] for t in trades if t["signal"] == asof]
    new_exits = [t["ticker"] for t in closed if t.get("exit") and (since is None or t["exit"] > since)
                 and t["exit"] <= asof] if since else []
    return {
        "version": VERSION, "asof": asof, "run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "rule": RULE, "backtest": BACKTEST,
        "funds": fund_states(close, f, trades),
        "trades": trades,
        "summary": {"since": since, "new_signals": new_signals, "new_exits": new_exits,
                    "open": sum(t.get("status") in ("NEW", "OPEN") for t in trades),
                    "all": _stats(closed), "live": _stats([t for t in closed if t.get("live")]),
                    "backfilled": _stats([t for t in closed if not t.get("live")]),
                    "base": base_rate(open_, close, adj)},
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
    o, c, a = drop_unfinished_session(*download())
    check_coverage(c)
    out = compute(o, c, a, read_prev())
    body = json.dumps(out, indent=1)
    (DATA / "latest.json").write_text(body, encoding="utf-8")
    (DATA / "history" / f"{out['asof']}.json").write_text(body, encoding="utf-8")
    s = out["summary"]
    with open(DATA / "runs.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"run_at": out["run_at"], "asof": out["asof"], "since": s["since"],
                             "signals": s["new_signals"], "exits": s["new_exits"], "open": s["open"],
                             "closed": s["all"]["n"]}) + "\n")
    print(f"pullback scan asof {out['asof']}: signals {s['new_signals']}, exits {s['new_exits']}, "
          f"open {s['open']}, closed {s['all']['n']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
