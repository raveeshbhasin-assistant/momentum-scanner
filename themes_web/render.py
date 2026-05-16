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


def base_context(slug: str, active_page: str) -> dict:
    """Common context every page template needs."""
    tracker = load_tracker_json(slug)
    return {
        "active_slug": slug,
        "active_page": active_page,
        "tracker": tracker,
        "all_themes": discover_themes_full(),
    }
