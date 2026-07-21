"""
themes_web/scheduler.py — Daily refresh job.

Runs candidates + tracker_live refreshes at 18:00 ET weekdays.
Started by the FastAPI app on startup; stopped on shutdown.
Also exposes a manual trigger via POST /api/refresh.
"""
from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent
_PROJECT_ROOT = _HERE.parent
_PYTHON = sys.executable

_scheduler: BackgroundScheduler | None = None


def _run_refresh_for_theme(theme: str) -> dict:
    """
    Run both refresh scripts for one theme as subprocesses. Subprocess
    keeps the work isolated from the FastAPI event loop and respects the
    'no app.py import from themes/' rule (no Python-level import here).
    """
    result = {"theme": theme, "candidates": None, "tracker_live": None}

    cmd1 = [_PYTHON, "themes/refresh_data.py", theme]
    cmd2 = [_PYTHON, "themes/tracker_refresh.py", theme]

    logger.info(f"[scheduler] Refreshing {theme} — candidates first")
    try:
        r1 = subprocess.run(cmd1, cwd=_PROJECT_ROOT, capture_output=True, text=True, timeout=900)
        result["candidates"] = {"returncode": r1.returncode, "tail": (r1.stdout + r1.stderr)[-300:]}
        if r1.returncode != 0:
            logger.warning(f"[scheduler] candidates refresh non-zero for {theme}: {r1.returncode}")
    except Exception as e:
        logger.exception(f"[scheduler] candidates refresh failed for {theme}: {e}")
        result["candidates"] = {"error": str(e)}

    logger.info(f"[scheduler] Refreshing {theme} — tracker_live")
    try:
        r2 = subprocess.run(cmd2, cwd=_PROJECT_ROOT, capture_output=True, text=True, timeout=300)
        result["tracker_live"] = {"returncode": r2.returncode, "tail": (r2.stdout + r2.stderr)[-300:]}
        if r2.returncode != 0:
            logger.warning(f"[scheduler] tracker_live refresh non-zero for {theme}: {r2.returncode}")
    except Exception as e:
        logger.exception(f"[scheduler] tracker_live refresh failed for {theme}: {e}")
        result["tracker_live"] = {"error": str(e)}

    # Re-score using the freshly refreshed candidates + tracker_live data
    score_script = _PROJECT_ROOT / "themes" / theme / "_score_run.py"
    if score_script.exists():
        logger.info(f"[scheduler] Re-scoring {theme}")
        try:
            r3 = subprocess.run([_PYTHON, str(score_script)],
                                  cwd=_PROJECT_ROOT, capture_output=True, text=True, timeout=120)
            result["rescore"] = {"returncode": r3.returncode, "tail": (r3.stdout + r3.stderr)[-300:]}
            if r3.returncode != 0:
                logger.warning(f"[scheduler] rescore non-zero for {theme}: {r3.returncode}")
        except Exception as e:
            logger.exception(f"[scheduler] rescore failed for {theme}: {e}")
            result["rescore"] = {"error": str(e)}

    return result


def refresh_referrals() -> dict:
    """
    Rebuild the referral-moat scorecards + static site (referral_moat/ is a
    standalone module — subprocess keeps the no-cross-import rule, same as
    the themes refresh). Runs monthly; also callable via
    POST /api/refresh_referrals.
    """
    result: dict = {}
    for script in ("referral_moat/build.py", "referral_moat/make_site.py"):
        logger.info(f"[scheduler] referral refresh — {script}")
        try:
            r = subprocess.run([_PYTHON, script], cwd=_PROJECT_ROOT,
                               capture_output=True, text=True, timeout=1800)
            result[script] = {"returncode": r.returncode,
                              "tail": (r.stdout + r.stderr)[-300:]}
            if r.returncode != 0:
                logger.warning(f"[scheduler] {script} non-zero: {r.returncode}")
                break  # don't regenerate the site from a failed build
        except Exception as e:
            logger.exception(f"[scheduler] {script} failed: {e}")
            result[script] = {"error": str(e)}
            break
    return result


def _scheduled_job():
    """Cron entry point — refresh every active theme that has a tracker."""
    # Discover active themes by scanning themes/ for tracker.json
    themes_dir = _PROJECT_ROOT / "themes"
    if not themes_dir.exists():
        return
    active = []
    for child in themes_dir.iterdir():
        if not child.is_dir() or child.name.startswith(("_", ".")):
            continue
        if (child / "tracker.json").exists():
            active.append(child.name)
    logger.info(f"[scheduler] Daily refresh starting for: {active}")
    for theme in active:
        _run_refresh_for_theme(theme)
    logger.info(f"[scheduler] Daily refresh complete at {datetime.now().isoformat()}")


def start_scheduler() -> BackgroundScheduler:
    """Start the background scheduler (idempotent)."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler
    _scheduler = BackgroundScheduler(timezone="America/New_York")
    # 18:00 ET weekdays (Mon-Fri)
    trigger = CronTrigger(hour=18, minute=0, day_of_week="mon-fri")
    _scheduler.add_job(
        _scheduled_job,
        trigger=trigger,
        id="themes_daily_refresh",
        name="Daily themes refresh (candidates + tracker_live)",
        replace_existing=True,
        misfire_grace_time=3600,  # if missed, run within 1 hour of when it should have fired
    )
    # Monthly referral-moat refresh: 1st of the month, 19:00 ET (after the
    # daily themes job window). Statements move quarterly; monthly is plenty.
    _scheduler.add_job(
        refresh_referrals,
        trigger=CronTrigger(day=1, hour=19, minute=0),
        id="referral_monthly_refresh",
        name="Monthly referral-moat scorecards refresh",
        replace_existing=True,
        misfire_grace_time=6 * 3600,
    )
    _scheduler.start()
    logger.info("[scheduler] Started. Daily refresh 18:00 ET weekdays; "
                "referral refresh monthly (1st, 19:00 ET).")
    return _scheduler


def stop_scheduler():
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("[scheduler] Stopped.")


def trigger_manual_refresh(theme: str | None = None) -> dict:
    """Run refresh synchronously — used by POST /api/refresh."""
    if theme:
        return _run_refresh_for_theme(theme)
    # All active themes
    themes_dir = _PROJECT_ROOT / "themes"
    results = {}
    for child in themes_dir.iterdir():
        if child.is_dir() and not child.name.startswith(("_", ".")) and (child / "tracker.json").exists():
            results[child.name] = _run_refresh_for_theme(child.name)
    return results
