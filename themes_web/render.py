"""
themes_web/render.py — Markdown + theme-discovery helpers.

Shared by the 5 page handlers in app.py. Keeps rendering logic in one place
so we get consistent typography, Mermaid handling, and TOC extraction.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

import markdown
from markdown.extensions.toc import TocExtension

_HERE = Path(__file__).parent
_PROJECT_ROOT = _HERE.parent
_THEMES_DIR = _PROJECT_ROOT / "themes"
_BACKLOG_PATH = _THEMES_DIR / "BACKLOG.md"


# ─────────────────────────────────────────────────────────────
# Slug helpers — turn a display name into a stable URL slug
# ─────────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    """Convert a display name to a snake_case slug."""
    s = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip().lower()
    s = re.sub(r"\s+", "_", s)
    return s


# ─────────────────────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────────────────────

def _convert_mermaid_blocks(md_text: str) -> str:
    """
    Replace ```mermaid ... ``` fences with HTML blocks that the Mermaid JS
    library can pick up client-side. The markdown library otherwise wraps
    the content in <pre><code class="language-mermaid"> which Mermaid won't
    auto-detect.
    """
    pattern = re.compile(r"```mermaid\s*\n(.*?)\n```", re.DOTALL)
    return pattern.sub(lambda m: f'<div class="mermaid">{m.group(1)}</div>', md_text)


def render_markdown(md_text: str) -> tuple[str, str]:
    """
    Render markdown to HTML. Returns (html, toc_html). The toc_html is the
    sidebar table of contents derived from H2/H3 headings.
    """
    md_text = _convert_mermaid_blocks(md_text)
    md = markdown.Markdown(extensions=[
        "tables",
        TocExtension(toc_depth="2-3", anchorlink=False, permalink=False),
        "fenced_code",
        "attr_list",
        "sane_lists",
    ])
    html = md.convert(md_text)
    toc = getattr(md, "toc", "")
    return html, toc


def read_markdown_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────
# Backlog parsing — for the theme dropdown
# ─────────────────────────────────────────────────────────────

_SECTION_RE = re.compile(r"^##\s+(Active|On Deck|Backlog|Completed|Failed|Retired)\b.*$",
                         re.IGNORECASE | re.MULTILINE)
_THEME_LINE_RE = re.compile(r"^\d+\.\s+\*\*(.+?)\*\*\s*(?:—|-|–)?\s*(.*)$", re.MULTILINE)


def parse_backlog() -> list[dict]:
    """
    Parse `themes/BACKLOG.md` and return a list of themes, each with
    {slug, display_name, status, description, blurb}.
    Status is one of: Active, On Deck, Backlog, Completed, Failed/Retired.
    """
    if not _BACKLOG_PATH.exists():
        return []
    text = _BACKLOG_PATH.read_text(encoding="utf-8")

    # Find all section headers and their start positions
    sections = []
    for m in _SECTION_RE.finditer(text):
        sections.append((m.group(1).strip(), m.start()))
    sections.append(("", len(text)))  # sentinel

    themes: list[dict] = []
    for i in range(len(sections) - 1):
        section_name, start = sections[i]
        end = sections[i + 1][1]
        section_text = text[start:end]
        # Normalize status label
        status = section_name
        if status.lower().startswith("on deck"):
            status = "On Deck"
        for tm in _THEME_LINE_RE.finditer(section_text):
            display = tm.group(1).strip()
            blurb = tm.group(2).strip()
            # Compact first-sentence blurb for tooltips
            blurb_short = blurb.split(".")[0].strip()
            if blurb_short and not blurb_short.endswith("."):
                blurb_short += "."
            themes.append({
                "slug": slugify(display),
                "display_name": display,
                "status": status,
                "blurb": blurb_short[:200],
            })
    return themes


# ─────────────────────────────────────────────────────────────
# Theme discovery — combines BACKLOG.md + tracker.json presence
# ─────────────────────────────────────────────────────────────

def discover_themes_full() -> list[dict]:
    """
    Returns a list of all themes (active and planned), each with
    {slug, display_name, status, has_tracker, blurb}.

    - If a theme has a tracker.json present, it's clickable and `has_tracker=True`.
    - Otherwise the entry comes from BACKLOG.md and is `has_tracker=False`.
    """
    out: list[dict] = []
    seen_slugs: set[str] = set()

    # First pass: themes with a tracker.json on disk (the real ones)
    if _THEMES_DIR.exists():
        for child in sorted(_THEMES_DIR.iterdir()):
            if not child.is_dir() or child.name.startswith(("_", ".")):
                continue
            tracker_path = child / "tracker.json"
            if not tracker_path.exists():
                continue
            try:
                t = json.loads(tracker_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            slug = child.name
            seen_slugs.add(slug)
            out.append({
                "slug": slug,
                "display_name": t.get("theme_display_name") or slug.replace("_", " ").title(),
                "status": t.get("theme_status", "Active"),
                "has_tracker": True,
                "blurb": (t.get("plain_summary") or "")[:200],
            })

    # Second pass: themes in BACKLOG.md that don't have a tracker yet
    for b in parse_backlog():
        # The slug parse_backlog produces might not match the directory naming
        # convention (e.g., "AI Data Center Build-Out" → "ai_data_center_buildout"
        # vs the actual directory "ai_data_center"). For the planned themes
        # without a tracker, slug doesn't need to navigate anywhere — but we
        # avoid duplicating any name that's already in the on-disk list.
        if b["slug"] in seen_slugs:
            continue
        # Also check fuzzy match — if a display name aligns with an existing
        # tracker's display name, skip
        if any(t["display_name"].lower() == b["display_name"].lower() for t in out):
            continue
        out.append({
            "slug": b["slug"],
            "display_name": b["display_name"],
            "status": b["status"],
            "has_tracker": False,
            "blurb": b["blurb"],
        })

    # Sort: Active first, then On Deck, then Backlog, then alphabetical within group
    status_order = {"Active": 0, "On Deck": 1, "Backlog": 2, "Completed": 8, "Failed": 9, "Retired": 9}
    out.sort(key=lambda t: (status_order.get(t["status"], 5), t["display_name"]))
    return out


# ─────────────────────────────────────────────────────────────
# Theme-context loader (shared by all 5 page handlers)
# ─────────────────────────────────────────────────────────────

def load_tracker_json(slug: str) -> Optional[dict]:
    path = _THEMES_DIR / slug / "tracker.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_candidates_json(slug: str) -> Optional[dict]:
    path = _THEMES_DIR / slug / "candidates.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_scoring_json(slug: str) -> Optional[dict]:
    path = _THEMES_DIR / slug / "scoring_log.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_tracker_live_json(slug: str) -> Optional[dict]:
    path = _THEMES_DIR / slug / "tracker_live.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def theme_dir(slug: str) -> Path:
    return _THEMES_DIR / slug


# ─────────────────────────────────────────────────────────────
# Portfolio aggregation — merge holdings across all active themes
# ─────────────────────────────────────────────────────────────

def aggregate_portfolio() -> dict:
    """
    Walk every Active theme with a tracker.json + read its candidates.json for
    current prices, return a portfolio-level view:

        {
            "themes":    [ {slug, display_name, locked_at, holdings_count,
                            tracker_chg_pct, spy_chg_pct, vs_spy_pp,
                            spy_at_lock, plain_summary} ... ],
            "holdings":  [ {ticker, company, bucket, sub, theme_slug,
                            theme_display_name, theme_count, entry_price,
                            current_price, chg_pct, chg_vs_spy_pp, conviction,
                            scoring_total, thesis_status, scoring_rationale,
                            spy_chg_pct_for_theme} ... ],
            "overlaps":  [ {ticker, company, themes: [{slug, display_name,
                            conviction, scoring_total, entry_price}],
                            count} ... ],
            "summary":   {unique_positions, tracker_lines, overlap_count,
                          themes_count, avg_chg_pct, avg_vs_spy_pp,
                          best_performer, worst_performer},
        }

    Notes:
    - Per-holding chg_pct uses *that theme's* SPY-at-lock as the benchmark.
      That keeps each holding's vs-SPY comparison faithful to its own theme
      timeline, even when themes were locked on different dates.
    - Aggregate "avg_vs_spy_pp" is the mean of per-holding chg_vs_spy_pp
      values — a simple equal-weight average across tracker lines. Overlap
      tickers count once per theme (intentional, since they're 2x positions).
    """
    themes_meta: list[dict] = []
    all_holdings: list[dict] = []
    by_ticker: dict[str, list[dict]] = {}

    for t in discover_themes_full():
        if not t.get("has_tracker"):
            continue
        if t.get("status") != "Active":
            continue
        slug = t["slug"]
        tracker = load_tracker_json(slug) or {}
        cands_doc = load_candidates_json(slug) or {}
        candidates_map = {c["ticker"]: c for c in cands_doc.get("candidates", [])}

        bench = tracker.get("benchmarks_at_init") or {}
        spy_lock = float(bench.get("spy_at_theme_lock") or 0)
        # No live SPY fetch in aggregation; treat current SPY = lock SPY for
        # now (same simplification the tracker page makes). Live overlay can
        # update on the client side later.
        spy_now = spy_lock
        spy_chg_pct = ((spy_now / spy_lock - 1) * 100) if spy_lock > 0 else 0.0

        per_theme_chg_sum = 0.0
        per_theme_n = 0

        for h in tracker.get("holdings") or []:
            tk = h["ticker"]
            entry = float(h.get("entry_price") or 0)
            cand = candidates_map.get(tk) or {}
            now = float(cand.get("price") or entry)
            chg_pct = ((now / entry - 1) * 100) if entry > 0 else 0.0
            chg_vs_spy = chg_pct - spy_chg_pct

            row = {
                **h,
                "theme_slug": slug,
                "theme_display_name": tracker.get("theme_display_name") or slug,
                "current_price": round(now, 2),
                "chg_pct": round(chg_pct, 2),
                "chg_vs_spy_pp": round(chg_vs_spy, 2),
                "spy_chg_pct_for_theme": round(spy_chg_pct, 2),
            }
            all_holdings.append(row)
            by_ticker.setdefault(tk, []).append(row)
            per_theme_chg_sum += chg_pct
            per_theme_n += 1

        avg_per_theme = (per_theme_chg_sum / per_theme_n) if per_theme_n else 0.0
        themes_meta.append({
            "slug": slug,
            "display_name": tracker.get("theme_display_name") or slug,
            "locked_at": tracker.get("theme_locked_at"),
            "tracker_initialized_at": tracker.get("tracker_initialized_at"),
            "holdings_count": per_theme_n,
            "tracker_chg_pct": round(avg_per_theme, 2),
            "spy_chg_pct": round(spy_chg_pct, 2),
            "vs_spy_pp": round(avg_per_theme - spy_chg_pct, 2),
            "spy_at_lock": spy_lock,
            "plain_summary": tracker.get("plain_summary") or "",
        })

    # Cross-theme overlap detection — any ticker appearing in >1 theme
    overlaps: list[dict] = []
    for tk, rows in by_ticker.items():
        if len(rows) > 1:
            overlaps.append({
                "ticker": tk,
                "company": rows[0].get("company"),
                "count": len(rows),
                "themes": [
                    {
                        "slug": r["theme_slug"],
                        "display_name": r["theme_display_name"],
                        "conviction": r.get("conviction"),
                        "scoring_total": r.get("scoring_total"),
                        "entry_price": r.get("entry_price"),
                        "bucket": r.get("bucket"),
                    }
                    for r in rows
                ],
            })
    overlaps.sort(key=lambda o: (-o["count"], o["ticker"]))

    # Annotate each holding with its theme_count (1 or 2+) so the table can
    # surface the 2x positions inline.
    for row in all_holdings:
        row["theme_count"] = len(by_ticker.get(row["ticker"], []))

    # Summary
    avg_chg = sum(h["chg_pct"] for h in all_holdings) / len(all_holdings) if all_holdings else 0.0
    avg_vs_spy = sum(h["chg_vs_spy_pp"] for h in all_holdings) / len(all_holdings) if all_holdings else 0.0
    best = max(all_holdings, key=lambda h: h["chg_pct"]) if all_holdings else None
    worst = min(all_holdings, key=lambda h: h["chg_pct"]) if all_holdings else None
    summary = {
        "themes_count": len(themes_meta),
        "unique_positions": len(by_ticker),
        "tracker_lines": len(all_holdings),
        "overlap_count": len(overlaps),
        "avg_chg_pct": round(avg_chg, 2),
        "avg_vs_spy_pp": round(avg_vs_spy, 2),
        "best_performer": {
            "ticker": best["ticker"], "chg_pct": best["chg_pct"],
            "theme_display_name": best["theme_display_name"],
        } if best else None,
        "worst_performer": {
            "ticker": worst["ticker"], "chg_pct": worst["chg_pct"],
            "theme_display_name": worst["theme_display_name"],
        } if worst else None,
    }

    return {
        "themes": themes_meta,
        "holdings": all_holdings,
        "overlaps": overlaps,
        "summary": summary,
    }


def base_context(slug: str, active_page: str) -> dict:
    """Common context every page template needs."""
    tracker = load_tracker_json(slug)
    return {
        "active_slug": slug,
        "active_page": active_page,
        "tracker": tracker,
        "all_themes": discover_themes_full(),
    }
