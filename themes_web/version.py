"""
themes_web/version.py — single source of truth for the themes_web app version.

Convention (mirrors the scanner's config.APP_VERSION + templates/logic.html):
- Every user-visible change to themes_web bumps THEMES_WEB_VERSION (semver-ish
  x.y.z: minor for new pages/features, patch for fixes/copy).
- Each bump gets an entry at the TOP of themes_web/RELEASES.md (what/why/how
  verified/rollback), rendered live at /releases.
- app.py wires this into FastAPI(version=...) and a Jinja global, and the
  footer in base.html displays it on every page — so the deployed version is
  always visible and always matches this constant. No hand-copied strings.
"""

THEMES_WEB_VERSION = "1.4.0"
