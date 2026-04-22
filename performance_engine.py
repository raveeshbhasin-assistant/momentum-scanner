"""
Performance Engine (v3.5)
─────────────────────────
Read-side logic for the /performance page. Loads data/performance_log.json
(the v3.5 categorized trade store) and computes rollups the template can
render directly.

Categories
──────────
    A  — High-Score Leader        : score ≥ 60 AND leadership_label == "LEADER"
    B  — High-Score Non-Leader    : score ≥ 60 AND leadership_label in
                                    {FOLLOWER, LAGGARD, SOLO_MOVER}
    C  — Low-Score                : score < 60 AND leadership_label is classified
    D  — Unclassified             : leadership_label missing / UNKNOWN
                                    (typically early-session picks before
                                     sector rotation has settled)

POSTCLOSE picks (scanner signals emitted after 16:00 ET — currently a
data-quality bug) are excluded from the performance store entirely.

The file format is intentionally simple: a single JSON object with a
`version` marker and an `entries` list, each entry a flat dict. Pure
Python — no dependencies beyond stdlib.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

PERF_LOG_VERSION = "v3.5"
DATA_DIR = Path(__file__).parent / "data"
PERF_LOG_PATH = DATA_DIR / "performance_log.json"

HIGH_SCORE_THRESHOLD = 60
LEADER_LABEL = "LEADER"
CLASSIFIED_LABELS = {"LEADER", "FOLLOWER", "LAGGARD", "SOLO_MOVER"}

CATEGORY_NAMES = {
    "A": "High-Score Leader",
    "B": "High-Score Non-Leader",
    "C": "Low-Score",
    "D": "Unclassified",
}


# ═══════════════════════════════════════════════════════════════
#  CATEGORIZATION
# ═══════════════════════════════════════════════════════════════

def assign_category(score: Optional[float], leadership_label: Optional[str]) -> str:
    """Return 'A' | 'B' | 'C' | 'D' for a pick given its score and leadership label."""
    lbl = (leadership_label or "").upper() if leadership_label else ""
    if lbl not in CLASSIFIED_LABELS:
        return "D"
    try:
        s = float(score) if score is not None else 0
    except (TypeError, ValueError):
        s = 0
    if s >= HIGH_SCORE_THRESHOLD and lbl == LEADER_LABEL:
        return "A"
    if s >= HIGH_SCORE_THRESHOLD:
        return "B"
    return "C"


def time_bucket(batch_time: str) -> str:
    """Map HH:MM (24h) to a broad time-of-day bucket."""
    if not batch_time:
        return "unknown"
    try:
        hh = int(batch_time.split(":")[0])
    except (ValueError, IndexError):
        return "unknown"
    if hh < 10:
        return "09:30-10:00"
    if hh < 11:
        return "10:00-11:00"
    if hh < 12:
        return "11:00-12:00"
    if hh < 13:
        return "12:00-13:00"
    if hh < 14:
        return "13:00-14:00"
    if hh < 15:
        return "14:00-15:00"
    return "15:00-16:00"


# ═══════════════════════════════════════════════════════════════
#  I/O
# ═══════════════════════════════════════════════════════════════

def _ensure_dir():
    DATA_DIR.mkdir(exist_ok=True)


def load_entries() -> list[dict]:
    """Load the flat list of entries from the performance log. Empty list if none."""
    _ensure_dir()
    if not PERF_LOG_PATH.exists():
        return []
    try:
        with open(PERF_LOG_PATH) as f:
            doc = json.load(f)
        if isinstance(doc, dict):
            return list(doc.get("entries", []))
        # Backward compat: if someone dropped a bare list
        if isinstance(doc, list):
            return doc
    except (json.JSONDecodeError, IOError) as e:
        logger.warning("Failed to read %s: %s", PERF_LOG_PATH, e)
    return []


def save_entries(entries: list[dict]):
    """Write the entries back to disk in the versioned wrapper."""
    _ensure_dir()
    doc = {
        "version": PERF_LOG_VERSION,
        "updated": datetime.utcnow().isoformat() + "Z",
        "entries": entries,
    }
    tmp = PERF_LOG_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(doc, f, indent=2, default=str)
    tmp.replace(PERF_LOG_PATH)


def upsert_day(date_str: str, day_entries: list[dict]):
    """
    Replace any existing entries for `date_str` with `day_entries`.
    Used by the end-of-day resolve job so re-runs are idempotent.
    """
    existing = load_entries()
    kept = [e for e in existing if e.get("date") != date_str]
    kept.extend(day_entries)
    # Sort by date, then batch_time
    kept.sort(key=lambda e: (e.get("date", ""), e.get("batch_time", "")))
    save_entries(kept)


# ═══════════════════════════════════════════════════════════════
#  ENTRY NORMALIZATION
# ═══════════════════════════════════════════════════════════════

def normalize_entry(raw: dict) -> dict:
    """
    Normalize a raw dict into the canonical shape the template expects.
    Safe to call on already-normalized entries.
    """
    score = raw.get("score", raw.get("composite_score", 0)) or 0
    label = raw.get("leadership_label")
    if label is None and isinstance(raw.get("leadership"), dict):
        label = raw["leadership"].get("label")
    cat = raw.get("category") or assign_category(score, label)
    bt = raw.get("batch_time") or raw.get("time_et") or ""
    result = raw.get("result", raw.get("status", ""))
    pnl = _f(raw.get("pnl_dollar", raw.get("pnl_per_share")))
    # P&L-adjusted ("effective") reclassification:
    # If a pick is sold at EOD (close-only exit), upgrade to WIN if it ended
    # above entry, downgrade to LOSS if it ended below entry. Pure ties stay EOD.
    effective_result = result
    if result == "EOD" and pnl is not None:
        if pnl > 0:
            effective_result = "WIN"
        elif pnl < 0:
            effective_result = "LOSS"
    return {
        "date": raw.get("date", ""),
        "batch_time": bt,
        "ticker": raw.get("ticker", ""),
        "entry": _f(raw.get("entry")),
        "stop": _f(raw.get("stop")),
        "target": _f(raw.get("target")),
        "score": int(score) if _is_num(score) else 0,
        "rsi": int(raw["rsi"]) if _is_num(raw.get("rsi")) else None,
        "rvol": _f(raw.get("rvol")),
        "leadership_label": label,
        "category": cat,
        "time_bucket": time_bucket(bt),
        "result": result,
        "effective_result": effective_result,
        "resolve_time": raw.get("resolve_time", ""),
        "resolve_price": _f(raw.get("resolve_price")),
        "pnl_dollar": pnl,
        "r_realized": _f(raw.get("r_realized")),
        "appearance": raw.get("appearance", 1),
        "post_close": bool(raw.get("post_close", False)),
        "note": raw.get("note", ""),
    }


def _is_num(x) -> bool:
    try:
        float(x)
        return True
    except (TypeError, ValueError):
        return False


def _f(x):
    if x is None or x == "":
        return None
    try:
        return round(float(x), 4)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════
#  AGGREGATION
# ═══════════════════════════════════════════════════════════════

def _rollup(group: list[dict], result_key: str = "result") -> dict:
    """
    Compute headline stats for a group of entries.

    `result_key` selects which field drives WIN/LOSS/EOD counts:
      - "result"           : original close-out reason (raw)
      - "effective_result" : EOD upgraded/downgraded by sign of pnl (P&L-adjusted)
    P&L and R totals are independent of the key at the group level — they are
    always real numbers — but the `by_outcome` block is keyed by the active
    result_key, so WIN/LOSS/EOD splits differ between raw and effective views.

    The `by_outcome` sub-dict (added in v3.5.7) lets the UI respect the
    W/L/EOD outcome filter in cumulative/range mode, where per-pick rows
    aren't available. Each outcome carries its own n/total_r/total_pnl so
    the headline can be recomputed for any subset of the three outcomes.
    """
    if not group:
        return {
            "n": 0, "wins": 0, "losses": 0, "eods": 0,
            "win_rate": None, "total_r": 0.0, "avg_r": 0.0,
            "total_pnl": 0.0, "avg_pnl": 0.0,
            "by_outcome": _empty_by_outcome(),
        }
    w = sum(1 for e in group if e.get(result_key) == "WIN")
    l = sum(1 for e in group if e.get(result_key) == "LOSS")
    eod = sum(1 for e in group if e.get(result_key) == "EOD")
    rs = [e["r_realized"] for e in group if _is_num(e.get("r_realized"))]
    pnls = [e["pnl_dollar"] for e in group if _is_num(e.get("pnl_dollar"))]
    total_r = sum(rs) if rs else 0.0
    total_pnl = sum(pnls) if pnls else 0.0
    wr = (w / (w + l) * 100) if (w + l) > 0 else None

    # Per-outcome split for filter-respecting headlines in cum/range mode.
    # Any outcome not in {WIN, LOSS, EOD} is dropped (engine contract).
    by_outcome = _empty_by_outcome()
    for e in group:
        oc = e.get(result_key)
        if oc not in ("WIN", "LOSS", "EOD"):
            continue
        b = by_outcome[oc]
        b["n"] += 1
        if _is_num(e.get("r_realized")):
            b["total_r"] += float(e["r_realized"])
        if _is_num(e.get("pnl_dollar")):
            b["total_pnl"] += float(e["pnl_dollar"])
    for oc in by_outcome:
        by_outcome[oc]["total_r"] = round(by_outcome[oc]["total_r"], 3)
        by_outcome[oc]["total_pnl"] = round(by_outcome[oc]["total_pnl"], 2)

    return {
        "n": len(group),
        "wins": w,
        "losses": l,
        "eods": eod,
        "win_rate": wr,
        "total_r": round(total_r, 3),
        "avg_r": round(total_r / len(group), 3) if group else 0.0,
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(group), 3) if group else 0.0,
        "by_outcome": by_outcome,
    }


def _empty_by_outcome() -> dict:
    """Shape-stable empty split used by _rollup. Keeps keys consistent for UI."""
    return {
        "WIN":  {"n": 0, "total_r": 0.0, "total_pnl": 0.0},
        "LOSS": {"n": 0, "total_r": 0.0, "total_pnl": 0.0},
        "EOD":  {"n": 0, "total_r": 0.0, "total_pnl": 0.0},
    }


def build_view(entries: list[dict], date_str: Optional[str] = None) -> dict:
    """
    Produce the dict the template reads. If `date_str` is provided, the
    'single_day' slice is scoped to that date; otherwise it's scoped to
    the most recent date present in the log.

    Returns:
        {
          "dates": ["2026-04-20", ...],
          "selected_date": "2026-04-20",
          "cumulative": {
            "raw":       {"overall": {...}, "by_category": {...}, "by_category_time": {...}},
            "effective": {"overall": {...}, "by_category": {...}, "by_category_time": {...}},
          },
          "single_day": {
            "date": "2026-04-20",
            "raw":       {...},
            "effective": {...},
            "picks": [<entry dicts>...]
          },
          "diagnostics": {...}
        }

    `raw` uses each pick's original exit reason (WIN/LOSS/EOD). `effective`
    reclassifies EOD picks by the sign of their P&L (EOD + gain → WIN,
    EOD + loss → LOSS, exact-zero stays EOD). The UI toggles between the two.
    """
    normalized = [normalize_entry(e) for e in entries]
    dates = sorted({e["date"] for e in normalized if e.get("date")})
    if not dates:
        empty = _bucketed_rollup([])
        return {
            "dates": [],
            "selected_date": None,
            "cumulative": empty,
            "single_day": None,
            "diagnostics": {
                "post_close_count": 0, "stale_tickers": [],
                "unclassified_count": 0, "last_updated": None,
            },
        }

    if date_str is None or date_str not in dates:
        date_str = dates[-1]

    # Cumulative = everything in the log
    cumulative = _bucketed_rollup(normalized)

    day_entries = [e for e in normalized if e["date"] == date_str]
    single_day = _bucketed_rollup(day_entries)
    single_day["date"] = date_str
    single_day["picks"] = sorted(
        day_entries,
        key=lambda e: (e["batch_time"], e["ticker"]),
    )

    return {
        "dates": dates,
        "selected_date": date_str,
        "categories": CATEGORY_NAMES,
        "cumulative": cumulative,
        "single_day": single_day,
        "diagnostics": _diagnostics(entries, normalized),
    }


def _bucketed_rollup(entries: list[dict]) -> dict:
    """
    Overall + by_category + by_category_time rollups for a slice of entries.

    Returns both `raw` and `effective` sub-dicts so the UI can toggle between
    them without a round-trip. Same P&L / R totals either way — only the
    WIN/LOSS/EOD counts (and the derived win rate) differ.
    """
    return {
        "raw":       _rollup_variants(entries, result_key="result"),
        "effective": _rollup_variants(entries, result_key="effective_result"),
    }


def _rollup_variants(entries: list[dict], result_key: str) -> dict:
    """Compute overall + by_category + by_category_time for one result_key."""
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        by_cat[e["category"]].append(e)

    by_category = {c: _rollup(g, result_key) for c, g in by_cat.items()}
    # Ensure all four categories present even if empty
    for c in ("A", "B", "C", "D"):
        by_category.setdefault(c, _rollup([], result_key))

    by_category_time: dict[str, dict[str, dict]] = {}
    for c, g in by_cat.items():
        buckets: dict[str, list[dict]] = defaultdict(list)
        for e in g:
            buckets[e["time_bucket"]].append(e)
        by_category_time[c] = {
            t: _rollup(grp, result_key) for t, grp in sorted(buckets.items())
        }

    return {
        "overall": _rollup(entries, result_key),
        "by_category": by_category,
        "by_category_time": by_category_time,
    }


def _diagnostics(raw_entries: list[dict], normalized: list[dict]) -> dict:
    """Surface data-quality info for the diagnostics panel."""
    # POSTCLOSE picks are excluded from the store; we only see them if a
    # caller passes them through normalize_entry with post_close=True.
    post_close = [e for e in normalized if e.get("post_close")]
    unclassified = [e for e in normalized if e["category"] == "D"]
    # Known stale mappings we want to surface (hardcoded for now — cheap)
    known_stale = {"SQ": "XYZ"}
    stale_found = sorted({
        f"{e['ticker']} → {known_stale[e['ticker']]}"
        for e in normalized
        if e.get("ticker") in known_stale
    })
    last_updated = None
    try:
        if PERF_LOG_PATH.exists():
            with open(PERF_LOG_PATH) as f:
                doc = json.load(f)
            if isinstance(doc, dict):
                last_updated = doc.get("updated")
    except Exception:
        pass
    return {
        "post_close_count": len(post_close),
        "post_close_picks": [
            {"date": e["date"], "batch_time": e["batch_time"], "ticker": e["ticker"],
             "score": e["score"], "leadership_label": e["leadership_label"]}
            for e in post_close
        ],
        "stale_tickers": stale_found,
        "unclassified_count": len(unclassified),
        "last_updated": last_updated,
    }


# ═══════════════════════════════════════════════════════════════
#  PUBLIC ENTRYPOINT FOR THE ROUTE
# ═══════════════════════════════════════════════════════════════

def get_view(selected_date: Optional[str] = None) -> dict:
    """Top-level function consumed by app.py's /performance route."""
    return build_view(load_entries(), selected_date)


# ═══════════════════════════════════════════════════════════════
#  RANGE VIEW (v3.5.7)
# ═══════════════════════════════════════════════════════════════

def build_range_view(entries: list[dict], start: str, end: str) -> dict:
    """
    View dict for an inclusive date range [start, end].

    Uses the same `_bucketed_rollup` primitive as cumulative and single-day,
    so filter semantics (categories, outcome split, time buckets) are
    byte-identical to the other two modes. Concatenates per-pick rows for
    the range so the picks table can be reused in the UI.

    `start` and `end` are expected to be "YYYY-MM-DD". Callers are
    responsible for validating format and start <= end; this function
    trusts its inputs and returns an empty range view if no entries match.

    Returns:
        {
          "dates": ["2026-04-20", ...],          # all dates in the log
          "start": "2026-04-20",
          "end":   "2026-04-22",
          "days":  ["2026-04-20", "2026-04-22"], # dates inside the range that have data
          "range": {
              "raw":       {"overall": {...}, "by_category": {...}, "by_category_time": {...}},
              "effective": {...},
              "picks":     [<entry dicts>...]    # sorted by (date, batch_time, ticker)
          },
          "categories": {...},
          "diagnostics": {...}                   # computed over full log, not just the range
        }
    """
    normalized = [normalize_entry(e) for e in entries]
    dates = sorted({e["date"] for e in normalized if e.get("date")})

    in_range = [e for e in normalized if start <= e["date"] <= end]
    days = sorted({e["date"] for e in in_range})
    rollups = _bucketed_rollup(in_range)
    rollups["picks"] = sorted(
        in_range,
        key=lambda e: (e["date"], e["batch_time"], e["ticker"]),
    )

    return {
        "dates": dates,
        "start": start,
        "end": end,
        "days": days,
        "categories": CATEGORY_NAMES,
        "range": rollups,
        "diagnostics": _diagnostics(entries, normalized),
    }


def get_range_view(start: str, end: str) -> dict:
    """Top-level function consumed by app.py's /api/performance/range route."""
    return build_range_view(load_entries(), start, end)
