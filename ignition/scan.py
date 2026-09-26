"""
ignition/scan.py — Ignition Watch scan + position ledger (standalone module).

Scans the S&P 500 + 400 universe (universe.txt) for the IGNITION signal,
replays every fire since LEDGER_START as a position, applies the exit rule,
and writes ignition/data/{latest.json, runs.jsonl, history/<asof>.json}.

    python ignition/scan.py            # full scan (needs Yahoo access)

Run of record: .github/workflows/ignition-daily.yml (weekdays after the
close), which commits the data to the `ignition-data` branch so every run is
kept in git. themes_web only reads that branch (scheduler.refresh_ignition).

SIGNAL (backtest 2016-2026, 903 tickers, 2.18M ticker-days — README.md):
    IGNITION = 5-day return > +12% AND 21-day avg volume > 1.5x its 126-day
    avg AND 50-DMA > 200-DMA (price > $3, 21-day avg dollar volume > $5M).

EXIT (research/ignition_exits/README.md — 748 trades, train 2016-21, holdout
2022-25, live 2025-26):
    SELL  = close below the pre-ignition base (the close 5 sessions before
            the fire: the whole ignition week given back). Sell next open.
            Best price-based exit in train, holdout AND live.
    Dropping off the signal list is NOT a sell — it was the worst of 28
    rules tested; fires that are gone the next day still returned +11.6%
    over 63 sessions vs +4.6% baseline.
    EDGE EXPIRED = 63 sessions since the last fire without a re-fire. The
            signal's excess return is concentrated in that window; a review
            flag, not a sell.
    A position with no re-fire for 252 sessions is closed as TIME.
    6-MONTH REVIEW (flag only; research/ignition_exits/profit_protection/):
            126+ sessions after entry and up 0-30% on the close = "the
            6-month take-profit rule would sell here". It failed the
            pre-registered bar (it costs ~6 pp of mean per trade), so it never
            closes a position; the ledger reports what following it would have
            returned (summary.checkpoint_whatif) next to the live rule.
    2x CAP REVIEW (flag only; profit_protection/README.md "Trimming winners"):
            assuming equal dollars at each entry, a held position's weight vs
            an equal share = (1 + ret) / mean(1 + ret) over held positions.
            At >= 2x it is flagged with the fraction to sell to get back to 2x.
            In a 2016-26 portfolio test, a hard 2x cap cut the drawdown at
            nearly every portfolio size for ~0.5 pp CAGR (risk control only).

The ledger is rebuilt from prices every run, so a missed run never loses an
event: anything that happened since the previous run is reported as new.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
LATEST = DATA_DIR / "latest.json"
RUNS = DATA_DIR / "runs.jsonl"
HISTORY_DIR = DATA_DIR / "history"
ET = ZoneInfo("America/New_York")

LEDGER_START = "2025-01-02"
BATCH = 150

R5_MIN = 0.12
VOLR_MIN = 1.5
MIN_PRICE = 3.0
MIN_DOLLAR_VOL = 5e6
BASE_LAG = 5          # pre-ignition base = close BASE_LAG sessions before the fire
EDGE_WINDOW = 63      # sessions after the last fire where the edge lives
REFIRE_GAP = 5        # a re-fire needs the signal off for this many sessions first
REFIRE_HOT = 20       # re-fired within this many sessions = strongest state
MAX_QUIET = 252       # close a position after this many sessions without a re-fire
CHECKPOINT_AFTER = 126            # 6-month review: sessions since entry ...
CHECKPOINT_BAND = (0.0, 0.30)     # ... while the gain is inside this band (exclusive)
WEIGHT_CAP = 2.0                  # 2x cap review: weight vs an equal share of the held book

BACKTEST = {
    "window": "2016-2026, S&P 500+400 (903 tickers), 2.18M ticker-days, 2,271 fires",
    "p_plus40_63d": 26.9, "p_plus40_baseline": 4.6,
    "p_plus80_63d": 8.2, "p_plus80_baseline": 0.6,
    "mean_63d": 17.0, "mean_63d_baseline": 4.5,
    "p_dd20_63d": 22.7, "p_dd20_baseline": 11.7,
    "years_positive": "10 of 11",
}
EXIT_RESEARCH = {
    "trades": 748,
    "dropoff_fwd63": 11.6, "stayon_fwd63": 15.4, "baseline_fwd63": 4.6, "p_off_next_day": 39.4,
    "rules_tested": 28,
    "base_fail_mean": {"train": 36.7, "holdout": 33.7, "live": 12.7},
    "dropoff_mean": {"train": -1.8, "holdout": -0.5, "live": -1.1},
    "refire_xs63": {"train": 4.3, "holdout": 16.7},
    "no_refire_xs63": {"train": 0.3, "holdout": 5.3},
}


def load_universe() -> list[str]:
    tickers: set[str] = set()
    for line in (HERE / "universe.txt").read_text(encoding="utf-8").splitlines():
        if not line.startswith("#"):
            tickers.update(t for t in line.split() if t)
    return sorted(tickers)


def download(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Adjusted daily open/close/volume from ~430 days before LEDGER_START."""
    import yfinance as yf

    start = (pd.Timestamp(LEDGER_START) - timedelta(days=430)).strftime("%Y-%m-%d")
    opens, closes, vols = [], [], []
    for i in range(0, len(tickers), BATCH):
        d = yf.download(tickers[i:i + BATCH], start=start, auto_adjust=True,
                        progress=False, threads=True)
        if d.empty:
            continue
        opens.append(d["Open"].astype("float32"))
        closes.append(d["Close"].astype("float32"))
        vols.append(d["Volume"].astype("float32"))
    if not closes:
        raise RuntimeError("yfinance returned no data for any batch")

    def join(parts):
        df = pd.concat(parts, axis=1)
        return df.loc[:, ~df.columns.duplicated()].sort_index()

    o, c, v = join(opens), join(closes), join(vols)
    # Drop a trailing row only a handful of tickers have (a partial intraday bar).
    if len(c) > 1 and c.iloc[-1].notna().mean() < 0.5:
        o, c, v = o.iloc[:-1], c.iloc[:-1], v.iloc[:-1]
    return o, c, v


def _pct(x) -> float | None:
    return None if x is None or not np.isfinite(x) else round(float(x) * 100, 1)


def _px(x) -> float | None:
    return None if x is None or not np.isfinite(x) else round(float(x), 2)


def signals(close: pd.DataFrame, vol: pd.DataFrame) -> dict:
    r5 = close.pct_change(5, fill_method=None)
    r63 = close.pct_change(63, fill_method=None)
    r252 = close.pct_change(252, fill_method=None)
    volr = vol.rolling(21).mean() / vol.rolling(126).mean()
    golden = close.rolling(50).mean() > close.rolling(200).mean()
    dollar_vol = (close * vol).rolling(21).mean()
    valid = (close > MIN_PRICE) & (dollar_vol > MIN_DOLLAR_VOL) & r252.notna()
    sig = (r5 > R5_MIN) & (volr > VOLR_MIN) & golden & valid
    return {"r5": r5, "r63": r63, "volr": volr, "golden": golden, "valid": valid, "sig": sig,
            "hi252": close.rolling(252).max()}


def build_ledger(open_: pd.DataFrame, close: pd.DataFrame, f: dict) -> tuple[list, list]:
    """Replay every fire since LEDGER_START as a position. Returns (positions, events)."""
    dates = close.index
    n = len(dates)
    start_i = int(dates.searchsorted(pd.Timestamp(LEDGER_START)))
    S, O, C = f["sig"].values, open_.values, close.values
    R5, VR = f["r5"].values, f["volr"].values
    ds = [str(d.date()) for d in dates]
    positions, events = [], []

    for j, tk in enumerate(close.columns):
        i = max(start_i, BASE_LAG)
        while i < n:
            if not S[i, j]:
                i += 1
                continue
            fire = i
            base = C[fire - BASE_LAG, j]
            pos = {
                "ticker": tk, "fired": ds[fire], "fire_close": _px(C[fire, j]),
                "fire_r5": _pct(R5[fire, j]), "fire_volr": _px(VR[fire, j]),
                "base": _px(base), "entry_date": None, "entry": None,
                "refires": 0, "last_fire": ds[fire], "last_refire": None, "exit_date": None, "exit": None,
                "exit_reason": None, "sell_signal": None,
                "checkpoint": None, "checkpoint_ret": None, "checkpoint_exit": None,
            }
            events.append({"date": ds[fire], "ticker": tk, "type": "FIRE",
                           "detail": f"+{_pct(R5[fire, j])}% week on {_px(VR[fire, j])}x volume"})
            for e in range(fire + 1, min(fire + 6, n)):   # first tradable open (skips a halt)
                if np.isfinite(O[e, j]):
                    pos["entry_date"], pos["entry"] = ds[e], _px(O[e, j])
                    break
            entry_i = ds.index(pos["entry_date"]) if pos["entry_date"] else None
            last_fire, peak, expired_logged = fire, C[fire, j], False
            d = fire + 1
            closed = False
            while d < n:
                c = C[d, j]
                if np.isfinite(c):
                    peak = max(peak, c)
                if S[d, j]:
                    # Consecutive "on" days are the same ignition; only a fresh
                    # fire after REFIRE_GAP sessions off counts as a re-fire.
                    if not S[max(0, d - REFIRE_GAP):d, j].any():
                        pos["refires"] += 1
                        pos["last_refire"] = ds[d]
                        events.append({"date": ds[d], "ticker": tk, "type": "REFIRE",
                                       "detail": f"+{_pct(R5[d, j])}% week on {_px(VR[d, j])}x volume"})
                    last_fire = d
                    pos["last_fire"] = ds[d]
                    expired_logged = False
                quiet = d - last_fire
                if not expired_logged and quiet >= EDGE_WINDOW:
                    expired_logged = True
                    events.append({"date": ds[d], "ticker": tk, "type": "EDGE_EXPIRED",
                                   "detail": f"{EDGE_WINDOW} sessions since last fire"})
                if (pos["checkpoint"] is None and entry_i is not None and d - entry_i >= CHECKPOINT_AFTER
                        and np.isfinite(c)):
                    r = c / pos["entry"] - 1
                    if CHECKPOINT_BAND[0] < r < CHECKPOINT_BAND[1]:
                        pos["checkpoint"], pos["checkpoint_ret"] = ds[d], _pct(r)
                        if d + 1 < n and np.isfinite(O[d + 1, j]):
                            pos["checkpoint_exit"] = _px(O[d + 1, j])
                        events.append({"date": ds[d], "ticker": tk, "type": "CHECKPOINT",
                                       "detail": f"{d - entry_i} sessions in, up {_pct(r):+.1f}% (under "
                                                 f"+{CHECKPOINT_BAND[1]:.0%}): 6-month take-profit review"})
                reason = None
                if np.isfinite(c) and np.isfinite(base) and c < base:
                    reason = "IGNITION_FAILED"
                elif quiet >= MAX_QUIET:
                    reason = "TIME"
                if reason:
                    pos["sell_signal"] = ds[d]
                    pos["exit_reason"] = reason
                    if d + 1 < n and np.isfinite(O[d + 1, j]):
                        pos["exit_date"], pos["exit"] = ds[d + 1], _px(O[d + 1, j])
                    detail = (f"closed {_px(c)} below pre-ignition base {_px(base)}"
                              if reason == "IGNITION_FAILED" else f"{MAX_QUIET} sessions without a re-fire")
                    events.append({"date": ds[d], "ticker": tk, "type": "SELL", "detail": detail})
                    closed = True
                    break
                d += 1
            last_c = pd.Series(C[fire:n, j]).ffill().iloc[-1] if not closed else None
            pos["peak"] = _px(peak)
            pos["sessions_since_fire"] = int(min(d, n - 1) - last_fire)
            since_refire = (n - 1 - ds.index(pos["last_refire"])) if pos["last_refire"] else None
            pos["status"] = _status(pos, closed, n - 1 - last_fire, since_refire)
            if pos["entry"]:
                mark = pos["exit"] if pos["exit"] else (last_c if not closed else C[d, j])
                pos["ret"] = _pct(mark / pos["entry"] - 1)
                pos["peak_ret"] = _pct(peak / pos["entry"] - 1)
            else:
                pos["ret"] = pos["peak_ret"] = None
            # What following the 6-month review would have returned: sold at the
            # next open after the flag (or marked now if that open hasn't happened).
            pos["whatif_ret"] = (_pct(pos["checkpoint_exit"] / pos["entry"] - 1)
                                 if pos["checkpoint_exit"] and pos["entry"] else pos["ret"])
            if pos["entry_date"]:
                end_i = ds.index(pos["exit_date"]) if pos["exit_date"] else (d if closed else n - 1)
                pos["held"] = int(end_i - ds.index(pos["entry_date"]))
            if not closed:
                pos["price"] = _px(last_c)
                pos["to_stop"] = _pct(base / last_c - 1) if np.isfinite(base) and np.isfinite(last_c) else None
            positions.append(pos)
            i = d + 1 if closed else n
    events.sort(key=lambda e: (e["date"], e["ticker"]))
    return positions, events


def _status(pos: dict, closed: bool, since_last_fire: int, since_refire: int | None) -> str:
    if closed:
        return "SELL_PENDING" if pos["exit"] is None else "CLOSED"
    if pos["entry"] is None:
        return "NEW"
    if since_refire is not None and since_refire <= REFIRE_HOT:
        return "REFIRED"
    if since_last_fire >= EDGE_WINDOW:
        return "EDGE_EXPIRED"
    return "HOLD"


def cap_review(positions: list) -> None:
    """Estimate each held position's weight vs an equal share, assuming equal
    dollars went into every entry: (1 + ret) / mean(1 + ret) over held positions
    (bought, not closed or pending sale). Sets weight_x, and cap_trim = the
    fraction of the position to sell to get back to WEIGHT_CAP when above it."""
    held = [p for p in positions
            if p["status"] in ("HOLD", "REFIRED", "EDGE_EXPIRED") and p.get("ret") is not None]
    for p in positions:
        p["weight_x"] = p["cap_trim"] = None
    if not held:
        return
    mean_gross = float(np.mean([1 + p["ret"] / 100 for p in held]))
    for p in held:
        w = (1 + p["ret"] / 100) / mean_gross
        p["weight_x"] = round(w, 2)
        if w >= WEIGHT_CAP:
            p["cap_trim"] = round(100 * (1 - WEIGHT_CAP / w), 1)


def extras(close: pd.DataFrame, vol: pd.DataFrame, f: dict) -> dict:
    last = pd.DataFrame({
        "price": close.iloc[-1], "r63": f["r63"].iloc[-1],
        "rs63": f["r63"].iloc[-1] - f["r63"].iloc[-1].mean(),
        "volr": f["volr"].iloc[-1], "pct_hi": close.iloc[-1] / f["hi252"].iloc[-1] - 1,
        "golden": f["golden"].iloc[-1], "valid": f["valid"].iloc[-1],
    })
    lead = last[last.valid & last.golden & (last.pct_hi > -0.15) & (last.rs63 > 0.15)]
    leaders = [{"ticker": t, "price": _px(r.price), "r63": _pct(r.r63), "rs63": _pct(r.rs63),
                "volr": _px(r.volr), "pct_from_hi": _pct(r.pct_hi)}
               for t, r in lead.sort_values("rs63", ascending=False).head(15).iterrows()]
    r5 = f["r5"].iloc[-1]
    near = r5.between(0.08, R5_MIN) & (f["volr"].iloc[-1] > 1.3) & f["golden"].iloc[-1] & f["valid"].iloc[-1]
    near_misses = sorted(({"ticker": t, "r5": _pct(r5[t]), "volr": _px(f["volr"].iloc[-1][t]),
                           "price": _px(close.iloc[-1][t])} for t in near[near].index),
                         key=lambda x: -(x["r5"] or 0))[:12]
    return {"leaders": leaders, "near_misses": near_misses}


def _stats(rets: list) -> dict:
    rets = [r for r in rets if r is not None]
    if not rets:
        return {"n": 0, "avg": None, "median": None, "pct_up": None}
    return {"n": len(rets), "avg": round(float(np.mean(rets)), 1),
            "median": round(float(np.median(rets)), 1),
            "pct_up": round(100 * float(np.mean([r > 0 for r in rets])), 1)}


def summarize(positions: list, events: list, since: str | None, asof: str, recent_from: str) -> dict:
    open_ = [p for p in positions if p["status"] not in ("CLOSED",)]
    closed = [p for p in positions if p["status"] == "CLOSED"]
    new_events = [e for e in events if (since is None and e["date"] == asof) or (since and e["date"] > since)]
    return {
        "open": len(open_),
        "by_status": {s: sum(p["status"] == s for p in positions)
                      for s in ("NEW", "HOLD", "REFIRED", "EDGE_EXPIRED", "SELL_PENDING")},
        "closed": len(closed),
        # Closed trades are mostly stop-outs by construction (winners stay open),
        # so judge the rule on everything: realized + open marked to market.
        "all_positions": _stats([p["ret"] for p in positions]),
        "open_positions": _stats([p["ret"] for p in open_]),
        "closed_positions": _stats([p["ret"] for p in closed]),
        "failed_positions": _stats([p["ret"] for p in closed if p["exit_reason"] == "IGNITION_FAILED"]),
        "aged_out_positions": _stats([p["ret"] for p in closed if p["exit_reason"] == "TIME"]),
        "recent_sells": sorted({e["ticker"] for e in events
                                if e["type"] == "SELL" and e["date"] >= recent_from}),
        "since": since,
        "new_events": new_events,
        "new_fires": sorted({e["ticker"] for e in new_events if e["type"] in ("FIRE", "REFIRE")}),
        "new_sells": sorted({e["ticker"] for e in new_events if e["type"] == "SELL"}),
        "new_expired": sorted({e["ticker"] for e in new_events if e["type"] == "EDGE_EXPIRED"}),
        "new_checkpoints": sorted({e["ticker"] for e in new_events if e["type"] == "CHECKPOINT"}),
        # 6-month review: open positions flagged, and the whole ledger re-scored as
        # if every flag had been sold at the next open (same positions, same marks).
        "checkpoint_open": sorted(p["ticker"] for p in open_ if p.get("checkpoint")),
        "checkpoint_whatif": _stats([p.get("whatif_ret") for p in positions]),
        "cap_review": [p["ticker"] for p in sorted(positions, key=lambda p: -(p.get("weight_x") or 0))
                       if p.get("cap_trim") is not None],
    }


def read_runs() -> list[dict]:
    if not RUNS.exists():
        return []
    out = []
    for line in RUNS.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def compute(open_: pd.DataFrame, close: pd.DataFrame, vol: pd.DataFrame, prev_asof: str | None) -> dict:
    f = signals(close, vol)
    positions, events = build_ledger(open_, close, f)
    cap_review(positions)
    asof = str(close.index[-1].date())
    since = prev_asof or None   # a re-run on the same data date reports nothing new
    recent_from = str(close.index[max(0, len(close.index) - 20)].date())
    return {
        "version": 2,
        "asof": asof,
        "scanned_at": datetime.now(ET).strftime("%Y-%m-%d %H:%M ET"),
        "universe": int(close.shape[1]),
        "ledger_start": LEDGER_START,
        "rule": {"r5_min": R5_MIN, "volr_min": VOLR_MIN, "min_price": MIN_PRICE,
                 "min_dollar_vol": MIN_DOLLAR_VOL, "base_lag": BASE_LAG,
                 "edge_window": EDGE_WINDOW, "refire_hot": REFIRE_HOT, "max_quiet": MAX_QUIET,
                 "checkpoint_after": CHECKPOINT_AFTER, "weight_cap": WEIGHT_CAP},
        "backtest": BACKTEST,
        "exit_research": EXIT_RESEARCH,
        "summary": summarize(positions, events, since, asof, recent_from),
        "positions": positions,
        "events": events,
        **extras(close, vol, f),
    }


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    runs = read_runs()
    prev_asof = runs[-1]["asof"] if runs else None

    o, c, v = download(load_universe())
    result = compute(o, c, v, prev_asof)
    s = result["summary"]

    tmp = LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(result, indent=1), encoding="utf-8")
    tmp.replace(LATEST)
    (HISTORY_DIR / f"{result['asof']}.json").write_text(json.dumps({
        "asof": result["asof"], "scanned_at": result["scanned_at"], "summary": s,
        "open_positions": [p for p in result["positions"] if p["status"] != "CLOSED"],
    }, indent=1), encoding="utf-8")
    run = {"run_at": result["scanned_at"], "asof": result["asof"], "since": s["since"],
           "fires": s["new_fires"], "sells": s["new_sells"], "expired": s["new_expired"],
           "checkpoints": s["new_checkpoints"],
           "open": s["open"], "closed": s["closed"]}
    with RUNS.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(run) + "\n")

    print(f"ignition scan asof={result['asof']} universe={result['universe']} open={s['open']} "
          f"closed={s['closed']} fires={','.join(s['new_fires']) or '-'} "
          f"sells={','.join(s['new_sells']) or '-'} since={s['since']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
