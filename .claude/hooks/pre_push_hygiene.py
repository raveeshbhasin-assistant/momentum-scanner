"""PreToolUse hook: inject the release-hygiene checklist before any `git push`.

Reads the Bash tool call JSON on stdin. If the command contains `git push`,
emits additionalContext so Claude re-checks the release hygiene BEFORE the
push runs. For every other command it prints nothing and exits 0 (no-op).

Registered in .claude/settings.json (project scope, committed). Rationale:
Railway auto-deploys origin/main, so a push IS a release — versioned
surfaces must move together every time (operator request, 2026-07-08).
"""
import json
import sys

CHECKLIST = """RELEASE-HYGIENE GATE (this Bash call contains `git push` — Railway auto-deploys origin/main, so a push IS a release). Before pushing, verify each item against the actual diff being pushed; fix and re-stage anything missing:
1. config.APP_VERSION bumped (config.py, top) — every user-visible change gets a new x.y.z. The FastAPI version, page footers, and static cache-bust all derive from this one constant (v3.8.3); do NOT hand-edit version strings elsewhere.
2. templates/logic.html has a release entry for this version: new `<div class="release current" id="vX-Y-Z">` at the top with the Current pill, previous release's `current` class + pill removed, what/why/verification/rollback documented.
3. No stray hardcoded version strings introduced: grep the previous version number across templates/ and *.py — only logic.html release history may mention old versions.
4. config.py threshold changes carry a citation (backtest window / research file) per CLAUDE.md.
5. New per-pick fields plumbed through ALL THREE sites (history.add_signals_to_daily, daily_analysis.analyze_day, performance_engine.normalize_entry) and passed through conditionally — never default-stamped (v3.8.1 lesson).
6. Tests green: `python -m pytest tests/ -q` run after the LAST code edit; Jinja-parse any touched templates.
7. Commit message references the version and cites research where thresholds changed.
If every item already holds, proceed with the push. Otherwise fix first — do not push partial hygiene."""


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return
    cmd = (payload.get("tool_input") or {}).get("command") or ""
    if "git push" not in cmd:
        return
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": CHECKLIST,
        }
    }))


if __name__ == "__main__":
    main()
