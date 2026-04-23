"""
Momentum Scanner — Web Application
───────────────────────────────────
FastAPI server that runs scheduled scans and serves
a professional trading dashboard.
"""

import json
import logging
from datetime import datetime, time as dtime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler

import config
from scanner import run_scan
from notifier import send_scan_email, send_test_email
from history import add_signals_to_daily, load_daily_finds, get_history_days, cleanup_old_files
from sector_rotation import (
    detect_sector_rotation,
    get_sector_priority_tickers,
    _last_rotation_snapshot,
)
from premarket import run_premarket_scan, reset_daily as reset_premarket
from daily_analysis import analyze_day
from data_backup import backup_data_files
from performance_engine import (
    get_view as _get_performance_view,
    get_range_view as _get_performance_range_view,
)
from market_regime import get_regime
from earnings import refresh_earnings_cache
from backtest import (
    run_backtest as _run_backtest,
    Filters as _BacktestFilters,
    save_result as _save_backtest,
    load_result as _load_backtest,
    list_results as _list_backtests,
)
from theme_scanner import (
    run_theme_scan as _run_theme_scan,
    save_scan as _save_theme_scan,
    load_scan as _load_theme_scan,
)
from theme_backtest import (
    run_backtest as _run_theme_backtest,
    save_backtest as _save_theme_backtest,
    load_backtest as _load_theme_backtest,
)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── App Setup ─────────────────────────────────────────────────
app = FastAPI(title="Momentum Scanner", version="3.5.10")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# ── State ─────────────────────────────────────────────────────
scan_results: list[dict] = []
last_scan_time: str = "Never"
scan_history: list[dict] = []  # History of past scans
is_scanning: bool = False
_today_tickers_seen: set = set()  # For re-entry suppression
_last_reset_date: str = ""        # Track daily reset
_sector_priority: list[str] = []  # Current hot-sector tickers


# ═══════════════════════════════════════════════════════════════
#  SCANNER JOBS
# ═══════════════════════════════════════════════════════════════

def _reset_daily_state():
    """Reset daily tracking state at start of each trading day."""
    global _today_tickers_seen, _last_reset_date
    today = datetime.now(config.ET).strftime("%Y-%m-%d")
    if today != _last_reset_date:
        _today_tickers_seen = set()
        _last_reset_date = today
        reset_premarket()
        logger.info(f"Daily state reset for {today}")


# US equity regular trading hours, used to gate re-entry dedup (v3.5.9).
# The main scan cron fires at 9:00 and 16:30 ET too; without this gate a
# ticker that flashed strong pre-market would burn its once-per-day
# dedup slot and be suppressed during the actual tradeable session.
_RTH_OPEN = dtime(9, 30)
_RTH_CLOSE = dtime(16, 0)


def _in_regular_session(now_et: datetime) -> bool:
    """
    True iff `now_et` is Mon–Fri between 09:30 and 16:00 ET inclusive.

    Callers must pass a datetime already localized to config.ET — this
    function does not itself convert. scheduled_scan always does so.
    """
    if now_et.weekday() >= 5:
        return False
    return _RTH_OPEN <= now_et.time() <= _RTH_CLOSE


def _ts_is_rth(ts) -> bool:
    """
    True iff an ISO timestamp string (or datetime) represents a moment
    inside the US regular trading session (Mon–Fri 09:30–16:00 ET).

    Exposed to Jinja templates as `is_rth(find.found_timestamp)` so that
    today.html and history.html can bake a `data-rth` attribute onto
    each find row. The "Market hours only" toggle then hides outside-
    RTH rows client-side without a reload.

    Pre-v3.5.9 daily files may contain outside-RTH rows from the 9:00
    and 16:30 scheduler fires; v3.5.9+ never writes outside-RTH rows,
    so on fresh data every row is RTH and this filter is a no-op.

    Missing / malformed timestamps return False so the toggle hides
    unlabeled rows rather than leaving orphan entries visible.
    """
    if not ts:
        return False
    try:
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts)
        elif isinstance(ts, datetime):
            dt = ts
        else:
            # Unknown type (int, list, etc.) — treat as malformed.
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=config.ET)
        else:
            dt = dt.astimezone(config.ET)
    except (ValueError, TypeError):
        return False
    return _in_regular_session(dt)


# Expose is_rth() to every Jinja template (v3.5.10: market-hours filter toggle)
templates.env.globals["is_rth"] = _ts_is_rth


def premarket_scan_job():
    """Run pre-market scan at 8:00 and 9:00 AM ET, plus refresh earnings cache."""
    _reset_daily_state()
    logger.info("Running pre-market catalyst scan...")
    try:
        tickers = config.get_full_universe()
        flagged = run_premarket_scan(tickers)
        logger.info(f"Pre-market scan: {len(flagged)} tickers flagged")
    except Exception as e:
        logger.error(f"Pre-market scan failed: {e}")

    # ── Refresh earnings cache (v3.3) ──
    try:
        cache = refresh_earnings_cache(force=False)
        logger.info(f"Earnings cache: {len(cache)} tickers with upcoming prints")
    except Exception as e:
        logger.error(f"Earnings refresh failed: {e}")


SECTOR_SNAPSHOT_MAX_AGE_SEC = 20 * 60  # Refresh if older than 20 min


# ═══════════════════════════════════════════════════════════════
#  DATA VOLUME SEED
# ═══════════════════════════════════════════════════════════════
#
#  Railway's filesystem is ephemeral. To persist performance data across
#  deploys we mount a volume at /app/data. A freshly mounted volume is
#  empty, which would wipe the bootstrap performance_log.json that ships
#  in the repo. To keep the initial dataset we commit a pristine copy
#  into data_seed/ (not gitignored) and copy it into the live data/
#  directory on first boot after the volume is empty.
#
#  Idempotent: once the volume has files, subsequent boots no-op.
#  Safe locally: on dev machines without a volume, data/ already exists
#  and the helper just skips.
#
_APP_ROOT = Path(__file__).parent
_DATA_DIR = _APP_ROOT / "data"
_DATA_SEED_DIR = _APP_ROOT / "data_seed"


def _seed_data_volume():
    """Copy bootstrap files from data_seed/ to data/ when the volume is empty."""
    try:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning(f"Data volume seed: could not mkdir {_DATA_DIR}: {e}")
        return

    if not _DATA_SEED_DIR.exists():
        # No seed files bundled — nothing to do.
        return

    seeded = 0
    skipped = 0
    for src in _DATA_SEED_DIR.iterdir():
        if not src.is_file():
            continue
        dst = _DATA_DIR / src.name
        if dst.exists():
            skipped += 1
            continue
        try:
            dst.write_bytes(src.read_bytes())
            seeded += 1
        except OSError as e:
            logger.warning(f"Data volume seed: failed to copy {src.name}: {e}")

    if seeded or skipped:
        logger.info(
            f"Data volume seed: copied {seeded}, skipped {skipped} "
            f"(from {_DATA_SEED_DIR} → {_DATA_DIR})"
        )


def sector_rotation_job():
    """Detect sector rotation at 9:30 and periodically during the day."""
    global _sector_priority
    logger.info("Running sector rotation detection...")
    try:
        top_sectors = detect_sector_rotation(top_n=config.SECTOR_TOP_N)
        _sector_priority = get_sector_priority_tickers(top_sectors)
        sector_names = [s["sector"] for s in top_sectors]
        logger.info(
            f"Sector rotation: {sector_names} → "
            f"{len(_sector_priority)} priority tickers"
        )
    except Exception as e:
        logger.error(f"Sector rotation detection failed: {e}")


def _ensure_fresh_sector_snapshot():
    """
    Self-heal guard so leadership classification always has a snapshot.

    Fixes a v3.4 bug where 45% of 2026-04-20 picks landed in Category D
    (Unclassified) because the scanner fired before sector_rotation_job
    had populated _last_rotation_snapshot, or after a snapshot went stale.
    Runs sector_rotation_job synchronously when the cached snapshot is
    missing or older than SECTOR_SNAPSHOT_MAX_AGE_SEC.
    """
    ts = _last_rotation_snapshot.get("ts")
    now = datetime.now()
    stale = ts is None or (now - ts).total_seconds() > SECTOR_SNAPSHOT_MAX_AGE_SEC
    if stale:
        age = "missing" if ts is None else f"{(now - ts).total_seconds()/60:.1f} min old"
        logger.info(f"Sector snapshot {age} — refreshing inline before scan")
        sector_rotation_job()


def _backup_volume_files(today: str, tag_prefix: str = "EOD") -> dict:
    """
    Back up the persistent volume files to the data-backups branch.

    Always attempts backup regardless of how analyze_day() terminated —
    on a no-picks day analyze_day() early-returns, but we still want the
    current performance_log.json preserved on GitHub. Safe no-op if
    GITHUB_BACKUP_* env vars are unset.
    """
    # performance_log.json drives /performance; <date>.json is the
    # per-day scanner-finds file written by history.save_daily_finds().
    files = [
        _DATA_DIR / "performance_log.json",
        _DATA_DIR / f"{today}.json",
    ]

    try:
        return backup_data_files(files=files, tag=f"{tag_prefix} {today}")
    except Exception as e:
        logger.error(f"Data backup raised unexpectedly: {e}")
        return {}


def post_market_analysis_job():
    """
    Run daily analysis after market close (4:15 PM ET as of v3.5.6.2).
    Fetches Yahoo Finance 5m bars, computes WIN/LOSS/EOD for all
    picks from today, and appends results to scanner_trade_log.xlsx.
    The performance page reads from this file on each page load.

    v3.5.6: backup of the data volume now lives here (not in analyze_day)
    so it runs every weekday — even on no-picks days when analyze_day
    returns early before reaching its own backup step.
    """
    today = datetime.now(config.ET).strftime("%Y-%m-%d")
    logger.info(f"Running post-market analysis for {today}...")
    try:
        analyze_day(today)
        logger.info(f"Post-market analysis complete for {today}")
    except Exception as e:
        logger.error(f"Post-market analysis failed: {e}")

    # Belt-and-suspenders: back up the volume regardless of analyze_day's
    # outcome. A failed / empty analysis should never block backup of
    # whatever's already on disk.
    try:
        summary = _backup_volume_files(today, tag_prefix="EOD")
        if summary:
            logger.info(f"Post-market backup: {summary}")
    except Exception as e:
        logger.error(f"Post-market backup failed: {e}")


def scheduled_scan():
    """Run by APScheduler every 30 min during market hours (Mon-Fri 9 AM – 4:30 PM ET)."""
    global scan_results, last_scan_time, is_scanning

    now = datetime.now(config.ET)
    _reset_daily_state()

    # ── DEAD ZONE FILTER: Skip lunch batches (0W/21L over 3 days) ──
    batch_key = now.strftime("%H:%M")
    # Map scheduler times to batch keys
    dead_zone_times = {"11:30", "12:00"}  # scheduler fires at :00/:30
    if batch_key in dead_zone_times:
        logger.info(f"SKIP: {batch_key} is in the lunch dead zone (0W/21L). Skipping scan.")
        return

    logger.info("Running scheduled scan...")
    is_scanning = True

    # Ensure a sector-rotation snapshot exists before the scan runs —
    # otherwise leadership_label comes back UNKNOWN and picks land in
    # Category D on the /performance page. Cheap no-op when the
    # snapshot is fresh.
    _ensure_fresh_sector_snapshot()

    try:
        try:
            results = run_scan(sector_priority=_sector_priority)
        except Exception as scan_err:
            logger.warning(f"Live scan error: {scan_err}")
            results = []

        # ── RE-ENTRY SUPPRESSION: Mark and filter re-entries ──
        # v3.5.9: the dedup set is only mutated during regular trading
        # hours (Mon–Fri 09:30–16:00 ET). Pre-market (9:00 fire) and
        # after-hours (16:30 fire) still produce signals and still get
        # logged + persisted, but they don't consume a ticker's
        # once-per-day slot. This guarantees every strong ticker gets
        # at least one emission during regular session.
        in_rth = _in_regular_session(now)
        if config.SUPPRESS_REENTRIES and results and in_rth:
            filtered = []
            for signal in results:
                ticker = signal["ticker"]
                if ticker in _today_tickers_seen:
                    signal["is_reentry"] = True
                    logger.info(f"RE-ENTRY suppressed: {ticker} (already signaled today)")
                    # Still record it but don't show in top signals
                    continue
                else:
                    signal["is_reentry"] = False
                    _today_tickers_seen.add(ticker)
                    filtered.append(signal)
            # Log what was filtered
            suppressed = len(results) - len(filtered)
            if suppressed > 0:
                logger.info(f"Re-entry filter: {suppressed} signals suppressed, {len(filtered)} kept")
            results = filtered
        elif config.SUPPRESS_REENTRIES and results and not in_rth:
            # Outside regular session — don't mutate the dedup set, but
            # still annotate every signal so downstream code has a
            # consistent shape. Tickers that fire here may re-appear
            # during RTH and will be emitted fresh.
            for signal in results:
                signal["is_reentry"] = False
            logger.info(
                f"Outside RTH ({now.strftime('%a %H:%M ET')}): "
                f"{len(results)} signal(s) passed through without dedup-set update"
            )

        if not results:
            logger.warning("Live scan returned no results after filters")

        scan_results = results
        last_scan_time = now.strftime("%Y-%m-%d %I:%M:%S %p ET")

        # Persist to daily cumulative finds (for /today and /history pages).
        # v3.5.9: gate on RTH so the 9:00 and 16:30 scheduler fires don't
        # pollute data/{date}.json with un-actionable pre-market / post-
        # close emissions. The in-memory `scan_results` is still updated
        # above so the live dashboard reflects the latest scan, but the
        # persisted file stays clean.
        if results and in_rth:
            add_signals_to_daily(results)
        elif results and not in_rth:
            logger.info(
                f"Outside RTH ({now.strftime('%a %H:%M ET')}): "
                f"{len(results)} signal(s) NOT persisted to daily finds "
                "(live dashboard still shows them)"
            )

        # ── Email notifier (v3.4.3) ─────────────────────────────────
        # Sends a strong-signals-only email after each scan. Fully
        # protected — never blocks or crashes the scheduler.
        try:
            send_scan_email(
                signals=results,
                regime=get_regime() if config.MARKET_REGIME_ENABLED else None,
                scan_time=last_scan_time,
            )
        except Exception as notif_err:
            logger.error(f"Notifier dispatch error: {notif_err}")

        # Store in history
        scan_history.append({
            "time": last_scan_time,
            "signal_count": len(results),
            "top_ticker": results[0]["ticker"] if results else "—",
            "top_score": results[0]["composite_score"] if results else 0,
        })
        # Keep last 20 scans
        if len(scan_history) > 20:
            scan_history.pop(0)

        logger.info(f"Scan complete: {len(results)} signals")
    except Exception as e:
        logger.error(f"Scan failed: {e}")
    finally:
        is_scanning = False


# ── Scheduler Setup ───────────────────────────────────────────
scheduler = BackgroundScheduler(timezone=config.ET)

# Main momentum scan: every 30 min during market hours
scheduler.add_job(
    scheduled_scan,
    "cron",
    day_of_week="mon-fri",
    hour="9-16",
    minute="0,30",
    timezone=config.ET,
    id="momentum_scan",
    max_instances=1,
    misfire_grace_time=300,
)

# Pre-market catalyst scan: 8:00 AM and 9:00 AM ET
scheduler.add_job(
    premarket_scan_job,
    "cron",
    day_of_week="mon-fri",
    hour="8,9",
    minute="0",
    timezone=config.ET,
    id="premarket_scan",
    max_instances=1,
)

# Sector rotation: 9:30 AM then every hour
scheduler.add_job(
    sector_rotation_job,
    "cron",
    day_of_week="mon-fri",
    hour="9-15",
    minute="30",
    timezone=config.ET,
    id="sector_rotation",
    max_instances=1,
)

# Post-market daily analysis: 4:15 PM ET.
# Moved from 17:00 → 16:15 in v3.5.6.2 so results are ready an hour earlier.
# Yahoo's 5m bar for 15:55→16:00 is usually finalized within 10–15 min of the
# close, so 16:15 gives it just enough slack without waiting another 45 min.
scheduler.add_job(
    post_market_analysis_job,
    "cron",
    day_of_week="mon-fri",
    hour="16",
    minute="15",
    timezone=config.ET,
    id="post_market_analysis",
    max_instances=1,
    misfire_grace_time=600,
)


@app.on_event("startup")
async def startup():
    # Seed data/ from data_seed/ if the mounted volume is empty.
    # Must run before anything else touches data/ (cleanup, reads, writes).
    _seed_data_volume()

    scheduler.start()
    cleanup_old_files()

    # Warm the sector-rotation snapshot so the first scan after a restart
    # already has leadership context — avoids a Category D spike on deploys
    # that land mid-session. Failure here is non-fatal: the inline guard in
    # scheduled_scan will retry on the next batch.
    now_et = datetime.now(config.ET)
    in_session = now_et.weekday() < 5 and 9 <= now_et.hour < 16
    if in_session:
        try:
            sector_rotation_job()
        except Exception as e:
            logger.warning(f"Startup sector-rotation warm-up failed: {e}")

    logger.info(
        f"Scheduler started: v3.5 — performance categories, sector-snapshot self-heal, "
        f"VIX regime gating, leader-as-tier display, weak-signal floor 40, "
        f"partial-bar RVOL fix, 60-min MAE manual rule. Mon-Fri 8:00 AM – 5:00 PM ET"
    )
    logger.info(f"Dashboard running at http://localhost:{config.PORT}")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the main dashboard."""
    regime = get_regime() if config.MARKET_REGIME_ENABLED else None
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "signals": scan_results,
            "last_scan": last_scan_time,
            "scan_interval": config.SCAN_INTERVAL_MINUTES,
            "is_scanning": is_scanning,
            "has_finnhub_key": bool(config.FINNHUB_API_KEY),
            "data_source": "FMP Real-Time" if config.FMP_API_KEY else "yfinance (15m delay)",
            "regime": regime,
        },
    )


@app.get("/api/signals", response_class=JSONResponse)
async def api_signals():
    """JSON API for current signals."""
    return {
        "signals": scan_results,
        "last_scan": last_scan_time,
        "is_scanning": is_scanning,
        "signal_count": len(scan_results),
    }


@app.get("/api/history", response_class=JSONResponse)
async def api_history():
    """JSON API for scan history."""
    return {"history": scan_history}


@app.post("/api/scan", response_class=JSONResponse)
async def trigger_scan():
    """Manually trigger a scan."""
    global is_scanning
    if is_scanning:
        return {"status": "already_running"}

    # Run in background
    import threading
    t = threading.Thread(target=scheduled_scan)
    t.start()
    return {"status": "started"}


@app.get("/api/config", response_class=JSONResponse)
async def get_config():
    """Return current scanner configuration."""
    return {
        "scan_interval_minutes": config.SCAN_INTERVAL_MINUTES,
        "min_rvol": config.MIN_RVOL,
        "min_composite_score": config.MIN_COMPOSITE_SCORE,
        "risk_reward_ratio": config.RISK_REWARD_RATIO,
        "atr_stop_multiplier": config.ATR_STOP_MULTIPLIER,
        "risk_per_trade_pct": config.RISK_PER_TRADE_PCT,
        "technical_weight": config.TECHNICAL_WEIGHT,
        "sentiment_weight": config.SENTIMENT_WEIGHT,
        "volume_weight": config.VOLUME_WEIGHT,
        "ticker_count": len(config.SP500_LIQUID),
    }


@app.get("/today", response_class=HTMLResponse)
async def today_page(request: Request):
    """Serve today's cumulative finds table."""
    finds = load_daily_finds()
    regime = get_regime() if config.MARKET_REGIME_ENABLED else None
    return templates.TemplateResponse(
        request=request,
        name="today.html",
        context={
            "finds": finds,
            "find_count": len(finds),
            "unique_tickers": len(set(f["ticker"] for f in finds)),
            "regime": regime,
        },
    )


@app.get("/api/regime", response_class=JSONResponse)
async def api_regime():
    """Current market regime (VIX-based)."""
    return get_regime() if config.MARKET_REGIME_ENABLED else {"label": "DISABLED"}


@app.get("/api/diagnose/egress", response_class=JSONResponse)
async def api_diagnose_egress():
    """
    Probe outbound network egress from the Railway container.

    Tests raw TCP connects (IPv4 only — matches the notifier's A-record
    path) to a fixed set of (host, port) targets and reports success,
    timeout, refusal, or other errors per target. No data is sent beyond
    the TCP handshake; on success the socket is closed immediately.

    Purpose: definitively confirm whether Railway's egress is dropping
    SMTP ports (25 / 465 / 587) while allowing HTTPS (443). If so, the
    notifier must move off smtplib onto an HTTPS-based email API.
    """
    import socket as _socket
    import time as _time

    targets = [
        ("smtp.gmail.com", 465, "SMTPS (implicit TLS — v3.5.6 email path)"),
        ("smtp.gmail.com", 587, "SMTP+STARTTLS (v3.5.5 email path)"),
        ("smtp.gmail.com", 25,  "SMTP plain (baseline — usually blocked)"),
        ("api.resend.com", 443, "HTTPS (Resend API — proposed v3.5.7 path)"),
        ("api.github.com", 443, "HTTPS (GitHub API — proves 443 works, since backup succeeded)"),
    ]

    results = []
    for host, port, note in targets:
        entry = {"host": host, "port": port, "note": note}
        # Resolve IPv4 only so we match the _IPv4SMTPS / backup code path.
        try:
            infos = _socket.getaddrinfo(
                host, port, _socket.AF_INET, _socket.SOCK_STREAM
            )
            entry["resolved_ip"] = infos[0][4][0] if infos else None
        except _socket.gaierror as e:
            entry["result"] = "dns_error"
            entry["error"] = str(e)
            results.append(entry)
            continue

        sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        sock.settimeout(5.0)
        t0 = _time.monotonic()
        try:
            sock.connect((entry["resolved_ip"], port))
            entry["result"] = "ok"
            entry["connect_ms"] = round((_time.monotonic() - t0) * 1000, 1)
        except _socket.timeout:
            entry["result"] = "timeout"
            entry["connect_ms"] = round((_time.monotonic() - t0) * 1000, 1)
        except ConnectionRefusedError as e:
            entry["result"] = "refused"
            entry["error"] = str(e)
        except OSError as e:
            entry["result"] = "error"
            entry["error"] = f"{type(e).__name__}: {e}"
        finally:
            try:
                sock.close()
            except OSError:
                pass
        results.append(entry)

    summary = {
        "smtp_blocked": all(
            r["result"] != "ok" for r in results if r["port"] in (25, 465, 587)
        ),
        "https_works": any(
            r["result"] == "ok" for r in results if r["port"] == 443
        ),
    }
    return {"summary": summary, "targets": results}


@app.post("/api/backup/trigger", response_class=JSONResponse)
async def api_trigger_backup():
    """
    Manually trigger a backup of the persistent data volume to GitHub.

    Same files as the 4:15 PM EOD job, useful when:
      • The EOD job was skipped because there were no picks
      • You want to snapshot the current state before a deploy
      • You need to recover after the volume was accidentally cleared

    Idempotent. No-op if GITHUB_BACKUP_TOKEN / GITHUB_BACKUP_REPO are unset.
    """
    today = datetime.now(config.ET).strftime("%Y-%m-%d")
    summary = _backup_volume_files(today, tag_prefix="manual")
    return {"today": today, "summary": summary}


@app.post("/api/analyze/trigger", response_class=JSONResponse)
async def api_trigger_analyze(date: str | None = None):
    """
    Manually run the post-market analysis for a given date (default: today ET).

    Wraps post_market_analysis_job so it can be kicked off outside the 4:15 PM
    cron window — useful for:
      • Running today's analysis immediately after a deploy
      • Re-analyzing a past day after a data fix
      • Verifying the pipeline end-to-end without waiting for 16:15 ET

    The underlying analyze_day() is idempotent at the per-date level:
    performance_log.upsert_day(date) replaces that date's rows only, so
    prior days (e.g. Apr 20) are never touched by a rerun of a later date.

    Query params:
        date: YYYY-MM-DD (defaults to today in ET)

    Returns:
        {
            "date": "2026-04-22",
            "analysis": "ok" | "failed: <err>",
            "backup": { "<path>": "created|updated|unchanged|missing|error", ... }
        }
    """
    target = date or datetime.now(config.ET).strftime("%Y-%m-%d")

    # Validate date format early — fail fast with a useful error.
    try:
        datetime.strptime(target, "%Y-%m-%d")
    except ValueError:
        return {
            "error": f"invalid date '{target}' — expected YYYY-MM-DD",
            "date": target,
        }

    analysis_status = "ok"
    try:
        analyze_day(target)
    except Exception as e:
        analysis_status = f"failed: {e}"
        logger.error(f"Manual analyze_day({target}) failed: {e}")

    try:
        summary = _backup_volume_files(target, tag_prefix="manual-analyze")
    except Exception as e:
        logger.error(f"Manual analyze trigger: backup raised: {e}")
        summary = {}

    return {"date": target, "analysis": analysis_status, "backup": summary}


@app.post("/api/notify/test", response_class=JSONResponse)
async def api_notify_test():
    """
    Fire a single round-trip test email via Resend.

    Bypasses both the market-hours gate and the category filter, so you
    can verify RESEND_API_KEY / NOTIFY_FROM / NOTIFY_EMAIL at any time —
    not just during a Cat A/B intraday scan. Never raises: surface the
    error dict in the response body if send fails.

    Returns the dict from notifier.send_test_email(), e.g.:
        {
            "ok": true,
            "from": "Momentum Scanner <scanner@yourdomain.com>",
            "to": ["you@example.com"],
            "subject": "[MScan] Test email — round-trip OK @ 2026-04-22 ...",
            "sent_at": "2026-04-22 03:47 PM ET",
            "market_hours_only": true,
            "allowed_categories": ["A", "B"],
            "note": "..."
        }

    On failure:
        {"ok": false, "error": "...", "from": "...", "to": [...]}
    """
    try:
        return send_test_email()
    except Exception as exc:
        logger.error(f"api_notify_test: unexpected failure: {exc}")
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"unexpected: {exc}"},
        )


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    """Serve multi-day history page."""
    days = get_history_days()
    return templates.TemplateResponse(
        request=request,
        name="history.html",
        context={"days": days},
    )


@app.get("/api/history/day", response_class=JSONResponse)
async def api_history_day(date: str):
    """
    Return a single day's finds by date string (YYYY-MM-DD).

    Used by the date picker on the History page to surface any arbitrary
    archived day. Files older than HISTORY_DAYS+1 are cleaned up by the
    daily cron, so lookups for dates past that window may return an empty
    list — the UI shows a 'no data' state in that case.
    """
    try:
        # Validate the date string parses; 400 on bad input.
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "date must be YYYY-MM-DD"},
        )
    finds = load_daily_finds(date)
    return {
        "ok": True,
        "date": date,
        "finds": finds,
        "signal_count": len(finds),
        "unique_tickers": len({f.get("ticker") for f in finds}),
    }


@app.get("/api/today", response_class=JSONResponse)
async def api_today():
    """JSON API for today's cumulative finds."""
    finds = load_daily_finds()
    return {"finds": finds, "count": len(finds)}


@app.get("/performance", response_class=HTMLResponse)
async def performance_page(request: Request, date: str | None = None):
    """
    Serve the performance tracker dashboard (v3.5).

    Reads the categorized log at data/performance_log.json via
    performance_engine. Single-day slice defaults to the latest date
    present in the log, but ?date=YYYY-MM-DD overrides.
    """
    view = _get_performance_view(date)
    return templates.TemplateResponse(
        request=request,
        name="performance.html",
        context={
            "view": view,
        },
    )


@app.get("/api/performance", response_class=JSONResponse)
async def api_performance(date: str | None = None):
    """JSON API for the performance dashboard (same view dict the template receives)."""
    return _get_performance_view(date)


@app.get("/api/performance/range", response_class=JSONResponse)
async def api_performance_range(start: str, end: str):
    """
    Range view for the performance dashboard (v3.5.7).

    Returns the same rollup shape as single_day/cumulative but filtered to
    entries whose date falls within [start, end] inclusive. Both parameters
    are required and must be YYYY-MM-DD. This is a read-only projection over
    the existing performance_log.json — it never mutates state.
    """
    for label, value in (("start", start), ("end", end)):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": f"invalid {label}='{value}' — expected YYYY-MM-DD"},
            )
    if start > end:
        return JSONResponse(
            status_code=400,
            content={"error": f"start '{start}' must be <= end '{end}'"},
        )
    return _get_performance_range_view(start, end)


def _load_trade_log() -> tuple[list[dict], list[dict]]:
    """Read scanner_trade_log.xlsx and return (trades, daily_summary) for the dashboard."""
    xlsx_path = BASE_DIR / "scanner_trade_log.xlsx"
    if not xlsx_path.exists():
        return [], []

    try:
        import openpyxl
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
        ws = wb["Trade Log"]

        trades = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row[0]:  # skip empty rows
                continue
            trades.append({
                "date": str(row[0]),
                "batch_time": str(row[1]) if row[1] else "",
                "ticker": str(row[2]) if row[2] else "",
                "entry_price": float(row[3]) if row[3] else 0,
                "target": float(row[4]) if row[4] else 0,
                "stop": float(row[5]) if row[5] else 0,
                "rvol": float(row[6]) if row[6] else 0,
                "rsi": float(row[7]) if row[7] else 0,
                "score": float(row[8]) if row[8] else 0,
                "result": str(row[9]) if row[9] else "",
                "exit_price": float(row[10]) if row[10] else 0,
                "pnl_dollar": float(row[11]) if row[11] else 0,
                "pnl_pct": float(row[12]) if row[12] else 0,
                "appearance": int(row[13]) if row[13] else 1,
            })

        # Build daily summary
        from collections import defaultdict
        by_day = defaultdict(list)
        for t in trades:
            by_day[t["date"]].append(t)

        daily_summary = []
        for day in sorted(by_day.keys()):
            day_trades = by_day[day]
            # v3.4 — win is any trade with pnl > 0 (not just target hits)
            w = sum(1 for t in day_trades if t["result"] == "WIN")       # legacy target-hit
            l = sum(1 for t in day_trades if t["result"] == "LOSS")      # legacy stop-hit
            e = sum(1 for t in day_trades if t["result"] == "EOD")
            pw = sum(1 for t in day_trades if t["pnl_dollar"] > 0)       # P&L positive
            pl = sum(1 for t in day_trades if t["pnl_dollar"] < 0)
            completed = pw + pl
            daily_summary.append({
                "date": day,
                "total": len(day_trades),
                "wins": w,          # legacy target hits
                "losses": l,        # legacy stop hits
                "eods": e,
                "pnl_wins": pw,
                "pnl_losses": pl,
                "win_rate": round(pw / completed * 100, 1) if completed > 0 else 0,  # v3.4 def
                "target_hit_rate": round(w / (w + l) * 100, 1) if (w + l) > 0 else 0,  # legacy def
                "net_pnl": round(sum(t["pnl_dollar"] for t in day_trades), 2),
            })

        wb.close()
        return trades, daily_summary

    except Exception as e:
        logger.error(f"Failed to load trade log: {e}")
        return [], []


@app.get("/logic", response_class=HTMLResponse)
async def logic_page(request: Request):
    """Serve the algorithm logic explanation page."""
    return templates.TemplateResponse(
        request=request,
        name="logic.html",
    )


# ═══════════════════════════════════════════════════════════════
#  THEMEHUNTER (parallel prototype scanner, v0.1)
# ═══════════════════════════════════════════════════════════════

def _theme_buckets_from_result(result: dict) -> list[tuple[str, str, str, list[dict]]]:
    """Split the flat signals list into A/B/C buckets for the template."""
    all_sigs = (result.get("signals") or []) + (result.get("watchlist") or [])
    by_bucket = {"A": [], "B": [], "C": []}
    for s in all_sigs:
        b = s.get("bucket", "A")
        if b in by_bucket:
            by_bucket[b].append(s)
    return [
        ("A", "Theme Leaders",
         "Top stocks in the top 3 theme baskets today. Ride the group's dominant rotation.",
         by_bucket["A"]),
        ("B", "Gap + News",
         "Tickers gapping ≥3% on above-average volume with an identifiable catalyst.",
         by_bucket["B"]),
        ("C", "Low-Float Runners",
         "Sub-$5B cap, RVOL ≥1.5x, theme-tagged. The USAR/MVRL/AVEX bucket.",
         by_bucket["C"]),
    ]


def _theme_rs_sorted(theme_rs: dict) -> list[tuple[str, dict]]:
    """Theme list sorted by percentile desc (exclude SPY/QQQ bookkeeping)."""
    items = [(k, v) for k, v in theme_rs.items() if not k.startswith("_")]
    return sorted(items, key=lambda x: -x[1].get("percentile", 0))


@app.get("/theme-scanner", response_class=HTMLResponse)
async def theme_scanner_page(request: Request, mode: str = "live"):
    """
    ThemeHunter parallel scanner — serves last-cached scan for the
    requested mode. If no cache exists, renders empty shell. Rescans
    happen via POST /api/theme-scan.
    """
    cache_name = "rewind_0945" if mode == "rewind" else "latest"
    result = _load_theme_scan(cache_name) or {
        "generated_at": "",
        "universe_sizes": {"A": 0, "B": 0, "C": 0},
        "theme_rs": {},
        "signals": [],
        "watchlist": [],
    }
    return templates.TemplateResponse(
        request=request,
        name="theme_scanner.html",
        context={
            "result": result,
            "mode": mode,
            "buckets": _theme_buckets_from_result(result),
            "themes_sorted": _theme_rs_sorted(result.get("theme_rs") or {}),
        },
    )


@app.get("/api/theme-scan", response_class=JSONResponse)
async def api_theme_scan(mode: str = "live"):
    """JSON API for current cached theme scan."""
    cache_name = "rewind_0945" if mode == "rewind" else "latest"
    result = _load_theme_scan(cache_name)
    if result is None:
        return JSONResponse({"error": "No cached theme scan for this mode. POST to rerun."},
                            status_code=404)
    return JSONResponse(result)


@app.post("/api/theme-scan", response_class=JSONResponse)
async def api_theme_scan_run(mode: str = "live", force: int = 0):
    """Trigger a fresh ThemeHunter scan. Blocking — returns the result JSON."""
    try:
        if mode == "rewind":
            result = _run_theme_scan(min_score=60.0, max_intraday=120, as_of_hhmm="09:45")
            _save_theme_scan(result, name="rewind_0945")
        else:
            result = _run_theme_scan(min_score=60.0, max_intraday=120)
            _save_theme_scan(result, name="live")
            _save_theme_scan(result, name="latest")
        return JSONResponse({"ok": True, "signal_count": len(result.get("signals", []))})
    except Exception as e:
        logger.exception("Theme scan failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ═══════════════════════════════════════════════════════════════
#  THEMEHUNTER BACKTEST (parallel, separate from v3.4.2)
# ═══════════════════════════════════════════════════════════════

@app.get("/theme-backtest", response_class=HTMLResponse)
async def theme_backtest_page(request: Request):
    """ThemeHunter intraday replay backtest — shows last-cached run."""
    result = _load_theme_backtest("latest") or {
        "generated_at": "",
        "trade_date": "",
        "summary": {},
        "timeline": [],
        "trades": [],
        "by_bucket": {},
        "by_theme": {},
        "by_tier": {},
        "per_snap_counts": {},
        "qualified_count": 0,
        "min_score": 60.0,
        "snapshots_used": [],
    }
    return templates.TemplateResponse(
        request=request,
        name="theme_backtest.html",
        context={"result": result},
    )


@app.get("/api/theme-backtest", response_class=JSONResponse)
async def api_theme_backtest_get():
    """JSON of last-cached ThemeHunter backtest."""
    result = _load_theme_backtest("latest")
    if result is None:
        return JSONResponse({"error": "No backtest cached. POST to /api/theme-backtest/run."},
                            status_code=404)
    return JSONResponse(result)


@app.post("/api/theme-backtest/run", response_class=JSONResponse)
async def api_theme_backtest_run(min_score: float = 60.0):
    """
    Trigger a fresh ThemeHunter backtest for today. Blocking — takes ~5 min
    because it runs 10 scanner snapshots sequentially.
    """
    try:
        result = _run_theme_backtest(min_score=min_score)
        _save_theme_backtest(result, name="latest")
        # Also snapshot by trade_date for history
        td = result.get("trade_date")
        if td:
            _save_theme_backtest(result, name=td)
        return JSONResponse({
            "ok": True,
            "trades": result.get("summary", {}).get("trades", 0),
            "total_R": result.get("summary", {}).get("total_R", 0),
            "win_rate_pct": result.get("summary", {}).get("win_rate_pct", 0),
        })
    except Exception as e:
        logger.exception("Theme backtest failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ═══════════════════════════════════════════════════════════════
#  BACKTEST (v3.4.2)
# ═══════════════════════════════════════════════════════════════

@app.get("/backtest", response_class=HTMLResponse)
async def backtest_page(request: Request):
    """Serve the backtest UI — loads the latest saved run, if any."""
    latest = _load_backtest("latest")
    runs = _list_backtests()
    return templates.TemplateResponse(
        request=request,
        name="backtest.html",
        context={
            "latest": latest,
            "runs": runs,
        },
    )


@app.post("/api/backtest/run", response_class=JSONResponse)
async def api_backtest_run(payload: dict):
    """Kick off a backtest. Expects JSON body:
      {
        "start_date": "YYYY-MM-DD",   (optional — defaults to 10 days back)
        "end_date":   "YYYY-MM-DD",   (optional — defaults to today)
        "filters":    { ... filters ... },  (optional)
        "max_tickers": int (optional, for quick runs)
      }
    Blocking call — returns the full result JSON when done.
    """
    from datetime import date as _d, datetime as _dt, timedelta as _td

    try:
        end_s = payload.get("end_date")
        start_s = payload.get("start_date")
        end_date = _dt.fromisoformat(end_s).date() if end_s else _d.today()
        start_date = _dt.fromisoformat(start_s).date() if start_s else end_date - _td(days=10)

        filters = _BacktestFilters.from_dict(payload.get("filters"))
        max_tickers = payload.get("max_tickers")

        result = _run_backtest(
            start_date=start_date,
            end_date=end_date,
            filters=filters,
            max_tickers=max_tickers,
        )
        _save_backtest(result, name="latest")
        return JSONResponse(result)
    except Exception as e:
        logger.exception("Backtest run failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/backtest/latest", response_class=JSONResponse)
async def api_backtest_latest():
    """Return the most recently saved backtest result, if any."""
    latest = _load_backtest("latest")
    if latest is None:
        return JSONResponse({"error": "No backtest has been run yet."}, status_code=404)
    return JSONResponse(latest)


@app.get("/api/backtest/runs", response_class=JSONResponse)
async def api_backtest_runs():
    """List all saved backtest runs."""
    return JSONResponse({"runs": _list_backtests()})


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.HOST, port=config.PORT)
