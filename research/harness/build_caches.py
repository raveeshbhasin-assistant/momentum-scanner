"""Build the harness caches: breadth_5m.csv.gz and the default all-bars label table (2.0x ATR stop / 2.5R).

    python build_caches.py [--stop-mult 2.0] [--target-r 2.5] [--atr-kind scanner] [--no-resume]

Resumable (per-ticker part files under cache/allbars_parts/). Progress is logged to stdout and cache/build.log.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from harness import bars as B
from harness import sim
from harness.common import CACHE_DIR, logger


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stop-mult", type=float, default=2.0)
    ap.add_argument("--target-r", type=float, default=2.5)
    ap.add_argument("--atr-kind", default="scanner")
    ap.add_argument("--first", default="09:35")
    ap.add_argument("--last", default="14:30")
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--skip-breadth", action="store_true")
    a = ap.parse_args()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(CACHE_DIR / "build.log")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    logger.addHandler(fh)
    t0 = time.time()
    if not a.skip_breadth:
        b = B.breadth_table(force=False)
        logger.info(f"breadth table: {len(b)} rows, {b.index.min()} .. {b.index.max()} ({time.time() - t0:.0f}s)")
    ms = B.market_state_table()
    logger.info(f"market state table: {len(ms)} rows")
    t1 = time.time()
    full = sim.build_allbars(a.stop_mult, a.target_r, a.atr_kind, a.first, a.last, resume=not a.no_resume)
    logger.info(f"allbars built: {len(full)} rows in {time.time() - t1:.0f}s; total {time.time() - t0:.0f}s")
    for pfx in ("sp_", "no_"):
        vc = full[pfx + "result"].value_counts(normalize=True).round(4).to_dict()
        logger.info(f"{pfx} result shares: {vc}; avgR {full[pfx + 'R'].mean():+.4f}")
    early = full[(full["hhmm"] >= "09:35") & (full["hhmm"] <= "10:00")]
    for pfx in ("sp_", "no_"):
        vc = early[pfx + "result"].value_counts(normalize=True).round(4).to_dict()
        logger.info(f"09:35-10:00 {pfx} result shares: {vc}; avgR {early[pfx + 'R'].mean():+.4f} n={len(early)}")


if __name__ == "__main__":
    main()
