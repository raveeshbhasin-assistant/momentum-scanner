"""
Data Backup (v3.5.4)
────────────────────
Belt-and-suspenders backup of the persistent data files to GitHub.

The Railway volume at /app/data is primary persistence. This module ships
a daily snapshot of the critical JSON files to a dedicated `data-backups`
branch of the same GitHub repo via the GitHub Contents API — no git
clone/push, no credentials mounted in the container beyond a single
personal-access token.

Why a separate branch:
    Writing to `main` would retrigger a Railway redeploy, causing a loop.
    `data-backups` lives outside the production deploy path but is still
    browsable and restorable through the same GitHub UI.

Configuration (env vars):
    GITHUB_BACKUP_TOKEN  — fine-grained PAT with "Contents: read/write"
                           scope on the target repo.
    GITHUB_BACKUP_REPO   — "owner/repo", e.g. "raveeshsingh/Trader-v3".
    GITHUB_BACKUP_BRANCH — optional. Default "data-backups".

If either of the first two is missing, every call no-ops with a log line
so the EOD job never fails on backup errors.
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
DEFAULT_BRANCH = "data-backups"
USER_AGENT = "MomentumScanner-DataBackup/3.5.4"


def _creds() -> Optional[tuple[str, str, str]]:
    token = os.environ.get("GITHUB_BACKUP_TOKEN")
    repo = os.environ.get("GITHUB_BACKUP_REPO")
    branch = os.environ.get("GITHUB_BACKUP_BRANCH", DEFAULT_BRANCH)
    if not token or not repo:
        return None
    return token, repo, branch


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }


def _ensure_branch(client: httpx.Client, token: str, repo: str, branch: str):
    """Create the backup branch from main if it doesn't exist. Idempotent."""
    r = client.get(f"{GITHUB_API}/repos/{repo}/branches/{branch}", headers=_headers(token))
    if r.status_code == 200:
        return
    if r.status_code != 404:
        r.raise_for_status()

    # Branch missing — look up main's HEAD SHA, then create branch off it.
    r = client.get(f"{GITHUB_API}/repos/{repo}/git/ref/heads/main", headers=_headers(token))
    r.raise_for_status()
    main_sha = r.json()["object"]["sha"]
    r = client.post(
        f"{GITHUB_API}/repos/{repo}/git/refs",
        headers=_headers(token),
        json={"ref": f"refs/heads/{branch}", "sha": main_sha},
    )
    r.raise_for_status()
    logger.info(f"Data backup: created branch '{branch}' from main @ {main_sha[:7]}")


def _put_file(
    client: httpx.Client, token: str, repo: str, branch: str,
    local_path: Path, repo_path: str, message: str,
) -> str:
    """PUT a single file to the backup branch. Returns 'created', 'updated', or 'unchanged'."""
    content_b64 = base64.b64encode(local_path.read_bytes()).decode("ascii")
    url = f"{GITHUB_API}/repos/{repo}/contents/{repo_path}"

    # Fetch existing file (if any) to get its sha — required to overwrite.
    r = client.get(url, params={"ref": branch}, headers=_headers(token))
    existing_sha: Optional[str] = None
    existing_content: Optional[str] = None
    if r.status_code == 200:
        j = r.json()
        existing_sha = j.get("sha")
        existing_content = j.get("content", "").replace("\n", "")
    elif r.status_code != 404:
        r.raise_for_status()

    if existing_content is not None and existing_content == content_b64:
        return "unchanged"

    payload = {"message": message, "content": content_b64, "branch": branch}
    if existing_sha:
        payload["sha"] = existing_sha
    r = client.put(url, headers=_headers(token), json=payload)
    r.raise_for_status()
    return "updated" if existing_sha else "created"


def backup_data_files(files: Iterable[Path], tag: str = "") -> dict:
    """
    Back up one or more files to the data-backups branch.

    Each file is written to `data/<basename>` on the branch (matching its
    path in the runtime container). Safe to call unconditionally — no-ops
    cleanly when env vars are missing.

    Returns a summary dict: {path: "created"|"updated"|"unchanged"|"error"}.
    """
    summary: dict[str, str] = {}
    creds = _creds()
    if not creds:
        logger.info("Data backup: GITHUB_BACKUP_TOKEN/REPO not set, skipping")
        return summary
    token, repo, branch = creds

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    try:
        with httpx.Client(timeout=15.0) as client:
            _ensure_branch(client, token, repo, branch)
            for p in files:
                p = Path(p)
                if not p.exists():
                    summary[str(p)] = "missing"
                    continue
                try:
                    msg = f"backup: {p.name} {tag} @ {ts}".strip()
                    result = _put_file(
                        client, token, repo, branch,
                        local_path=p, repo_path=f"data/{p.name}", message=msg,
                    )
                    summary[str(p)] = result
                except httpx.HTTPError as e:
                    summary[str(p)] = "error"
                    logger.warning(f"Data backup: {p.name} failed: {e}")
    except httpx.HTTPError as e:
        logger.warning(f"Data backup: top-level failure: {e}")
        return summary

    ok = [k for k, v in summary.items() if v in ("created", "updated", "unchanged")]
    bad = [k for k, v in summary.items() if v not in ("created", "updated", "unchanged")]
    if ok:
        logger.info(f"Data backup: {len(ok)} file(s) synced to {repo}@{branch}")
    if bad:
        logger.warning(f"Data backup: {len(bad)} file(s) skipped or failed: {bad}")
    return summary
