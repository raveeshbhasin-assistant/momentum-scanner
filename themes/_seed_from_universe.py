"""
themes/_seed_from_universe.py — Seed a theme's candidates.json from a local
_universe.json, reusing the exact data-fetch path in refresh_data.py.

Why this exists
───────────────
refresh_data.py drives its universe off the in-file UNIVERSES dict. When several
themes are being built in parallel we do NOT want each build editing that shared
dict (merge conflicts). Instead each theme drops a `_universe.json` in its own
directory (ticker -> metadata, identical shape to a UNIVERSES entry) and runs:

    python themes/_seed_from_universe.py <slug>

This fetches real fundamentals + 1Y price action via the SAME helpers
refresh_data.py uses (FMP when a key is present, yfinance otherwise), computes
cohort RS ranks, and writes:
    themes/<slug>/candidates.json            (source of truth)
    themes/<slug>/history/candidates_<date>.json
    themes/<slug>/candidates.md              (generic bucket-grouped view)

Once every theme is built, the operator folds each _universe.json into
refresh_data.py UNIVERSES + ACTIVE_THEMES so the nightly Railway job keeps them
fresh. Until then, this seed is the source of truth (same contract as glp1's
initial yfinance seed).

Hard rule: does NOT import from app.py. Reuses refresh_data.py helpers only.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd

_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import refresh_data as rd  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _fmt_mcap(v):
    if v is None:
        return "—"
    try:
        v = float(v)
        if v >= 1e12: return f"${v/1e12:.2f}T"
        if v >= 1e9:  return f"${v/1e9:.1f}B"
        if v >= 1e6:  return f"${v/1e6:.0f}M"
        return f"${v:,.0f}"
    except Exception:
        return str(v)


def _fmt_pct(v, plus=True):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        sign = "+" if plus and float(v) >= 0 else ""
        return f"{sign}{float(v):.1f}%"
    except Exception:
        return str(v)


def _fmt_num(v, decimals=2):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{float(v):,.{decimals}f}"
    except Exception:
        return str(v)


def _render_markdown(theme: str, result: dict) -> str:
    """Generic bucket-grouped candidates view. Works for any theme's buckets
    (unlike refresh_data._render_markdown which hard-codes the AI-DC buckets)."""
    display = theme.replace("_", " ").title()
    cands = result["candidates"]
    # Preserve first-seen bucket order from the universe
    bucket_order: list[str] = []
    for c in cands:
        b = c.get("bucket", "—")
        if b not in bucket_order:
            bucket_order.append(b)

    L = [f"# {display} — Candidates", ""]
    L.append(f"_Generated **{result['trade_date']}** from `_seed_from_universe.py`. "
             f"Source of truth: `candidates.json`. Refreshed nightly once folded into "
             f"`refresh_data.py`. Universe of {result['universe_size']} tickers._")
    L.append("")
    L.append("**Note:** RS ranks are computed *within this candidate cohort* (0-100, higher = "
             "better relative strength), not against the full market.")
    if any(c.get("side") for c in cands):
        L.append("")
        L.append("**Long/Short theme:** `side` marks SHORT (demand-destruction / stranded) vs "
                 "LONG (beneficiary). For shorts, a deteriorating fundamental is a *positive* signal.")
    L += ["", "---", "", "## Quick reference table", ""]
    hdr = "| Ticker | Company | Bucket | " + ("Side | " if any(c.get("side") for c in cands) else "") + \
          "Mkt Cap | Price | 1Y% | 3M% | Δ52wH | RS3M | P/E | Rev YoY |"
    sep = "|--------|---------|--------|" + ("------|" if any(c.get("side") for c in cands) else "") + \
          "---------|-------|-----|-----|-------|------|-----|---------|"
    L += [hdr, sep]
    ordered = sorted(cands, key=lambda c: (
        bucket_order.index(c.get("bucket", "—")),
        -(c.get("3m_pct") if c.get("3m_pct") is not None else -999),
    ))
    has_side = any(c.get("side") for c in cands)
    for c in ordered:
        side_cell = f" {c.get('side','—')} |" if has_side else ""
        rev = c.get("revenue_growth_yoy")
        L.append(
            f"| **{c['ticker']}** | {c.get('company','—')} | {str(c.get('bucket','—'))[:22]} |"
            f"{side_cell} {_fmt_mcap(c.get('market_cap'))} | ${_fmt_num(c.get('price'))} | "
            f"{_fmt_pct(c.get('1y_pct'))} | {_fmt_pct(c.get('3m_pct'))} | "
            f"{_fmt_pct(c.get('dist_from_52w_high_pct'))} | "
            f"{_fmt_num(c.get('rs_3m'), 0) if c.get('rs_3m') is not None else '—'} | "
            f"{_fmt_num(c.get('pe'), 1) if c.get('pe') else '—'} | "
            f"{_fmt_pct(rev*100) if rev is not None else '—'} |"
        )
    L += ["", "---", ""]

    for bucket in bucket_order:
        rows = [c for c in cands if c.get("bucket") == bucket]
        L.append(f"## {bucket}")
        L.append("")
        for c in sorted(rows, key=lambda x: -(x.get("3m_pct") if x.get("3m_pct") is not None else -999)):
            side = f" · **Side:** {c.get('side')}" if c.get("side") else ""
            L.append(f"### {c['ticker']} — {c.get('company','—')}")
            L.append("")
            L.append(f"**Bucket:** {c.get('bucket','—')} · **Sub:** {c.get('sub','—')} · "
                     f"**Specificity:** {c.get('specificity','—')}/5{side}  ")
            L.append(f"**Sector:** {c.get('sector','—')} · **Industry:** {c.get('industry','—')}")
            L.append("")
            if c.get("note"):
                L.append(f"_{c.get('note')}_")
                L.append("")
            L.append("| Price | Mkt Cap | P/E | P/S | Rev YoY | 1Y% | 3M% | Δ52wH | RS3M |")
            L.append("|-------|---------|-----|-----|---------|-----|-----|-------|------|")
            rev = c.get("revenue_growth_yoy")
            L.append(
                f"| ${_fmt_num(c.get('price'))} | {_fmt_mcap(c.get('market_cap'))} | "
                f"{_fmt_num(c.get('pe'), 1) if c.get('pe') else '—'} | "
                f"{_fmt_num(c.get('ps'), 1) if c.get('ps') else '—'} | "
                f"{_fmt_pct(rev*100) if rev is not None else '—'} | "
                f"{_fmt_pct(c.get('1y_pct'))} | {_fmt_pct(c.get('3m_pct'))} | "
                f"{_fmt_pct(c.get('dist_from_52w_high_pct'))} | "
                f"{_fmt_num(c.get('rs_3m'), 0) if c.get('rs_3m') is not None else '—'} |"
            )
            L.append("")
            if c.get("error"):
                L.append(f"> ⚠ Data error: `{c.get('error')}`")
                L.append("")
        L += ["---", ""]

    L.append("_Next step: `scoring.md` applies the rubric and shortlists the tracker._")
    return "\n".join(L) + "\n"


def seed(theme: str) -> dict:
    theme_dir = _HERE / theme
    uni_path = theme_dir / "_universe.json"
    if not uni_path.exists():
        raise FileNotFoundError(f"{uni_path} missing — write the universe first")
    universe = json.loads(uni_path.read_text(encoding="utf-8"))
    tickers = list(universe.keys())
    logger.info(f"[{theme}] seeding {len(tickers)} tickers")

    bars_map = rd._fetch_daily_bars(tickers)
    logger.info(f"[{theme}] daily bars for {len(bars_map)}/{len(tickers)}")

    candidates: list[dict] = []
    with httpx.Client(timeout=20) as client:
        for i, tk in enumerate(tickers):
            try:
                row = rd.refresh_ticker(tk, universe[tk], client, bars_map.get(tk))
            except Exception as e:
                logger.warning(f"[{theme}] {tk} failed: {e}")
                row = {"ticker": tk, "company": universe[tk].get("company"),
                       "bucket": universe[tk].get("bucket"), "error": str(e)}
            # carry side overlay through if present
            if "side" in universe[tk]:
                row["side"] = universe[tk]["side"]
            candidates.append(row)
            if (i + 1) % 4 == 0:
                time.sleep(0.6)

    returns = {c["ticker"]: c for c in candidates if "1m_pct" in c}
    rs_ranks = rd._compute_rs_ranks(returns)
    for c in candidates:
        if c["ticker"] in rs_ranks:
            c.update(rs_ranks[c["ticker"]])

    result = {
        "theme": theme,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": datetime.now().strftime("%Y-%m-%d"),
        "universe_size": len(tickers),
        "candidates": candidates,
    }

    (theme_dir / "candidates.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    hist = theme_dir / "history"
    hist.mkdir(exist_ok=True)
    (hist / f"candidates_{result['trade_date']}.json").write_text(
        json.dumps(result, indent=2, default=str), encoding="utf-8")
    (theme_dir / "candidates.md").write_text(_render_markdown(theme, result), encoding="utf-8")

    n_px = sum(1 for c in candidates if c.get("price"))
    logger.info(f"[{theme}] wrote candidates.json + .md — {n_px}/{len(tickers)} priced")
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python themes/_seed_from_universe.py <slug> [<slug> ...]")
        sys.exit(1)
    for th in sys.argv[1:]:
        seed(th)
