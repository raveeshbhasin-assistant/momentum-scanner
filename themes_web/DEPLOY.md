# Deploying themes_web to Railway

The themes_web app is a **separate Railway service** from the deployed momentum scanner. Both live in the same git repo. Both share the same env vars (FMP_API_KEY, FINNHUB_API_KEY). They serve on different domains.

This deployment does NOT touch the existing momentum scanner Railway service.

## One-time setup (in Railway dashboard)

1. **Open the same Railway project** that hosts the momentum scanner.

2. **Create a new service** in that project: **+ New → GitHub Repo → select Trader v3 repo**. (Same repo as the existing scanner service.)

3. **Set the start command** for the new service:

   ```
   uvicorn themes_web.app:app --host 0.0.0.0 --port $PORT
   ```

   Railway settings → Service settings → Start Command.

4. **Set environment variables** (copy from the existing scanner service):
   - `FMP_API_KEY` — same value as the momentum scanner
   - `FINNHUB_API_KEY` — same value as the momentum scanner
   - `TZ=America/New_York` — required so APScheduler cron evaluates 18:00 ET correctly

5. **Set a domain** (Railway settings → Networking → Generate Domain). Note the URL — that's your themes_web URL. The live one is `alert-youthfulness-production-354a.up.railway.app` (service `alert-youthfulness` in project `reasonable-forgiveness`).

6. **Deploy.** Railway auto-deploys on push to main. First deploy installs Python deps including `markdown`, `apscheduler`, and the FMP/Finnhub deps already in `requirements.txt`.

## Verifying the deploy

After Railway shows the service as **Active**:

- Open `https://<your-domain>/` — should redirect to `/themes/ai_data_center/tracker`
- All 5 tabs (Tracker / Thesis / Supply chain / Candidates / Scoring) should render
- Tracker detail panel should show real Finnhub news headlines (those work in any env with FINNHUB_API_KEY)
- 13F + earnings sections will populate after the first scheduled refresh runs at 18:00 ET on a weekday — or manually via `POST /api/refresh/ai_data_center`

## How the daily refresh works

- APScheduler starts on app startup.
- Cron: 18:00 ET, Mon-Fri.
- Each run executes `python themes/refresh_data.py <theme>` and `python themes/tracker_refresh.py <theme>` as subprocesses for every theme that has a `tracker.json`.
- Output files (`candidates.json`, `tracker_live.json`, dated history snapshots) are written to the **container's filesystem**.

⚠️ **Railway containers are ephemeral.** Files written at runtime are lost on redeploy. The dated history snapshots in `themes/<slug>/history/` will not persist across redeploys. Two ways to handle this:

1. **Accept ephemerality for now.** The current-state files (`candidates.json`, `tracker_live.json`) are rebuilt on the next scheduled refresh anyway. Only the historical snapshots are lost — these are useful for retrospective analysis later but not critical immediately.
2. **Add a Railway volume mount** (Settings → Volumes → Mount at `/app/themes`). Persists across redeploys. Recommended once you're past the prototyping phase.

## Manually triggering a refresh

After deploy, while you're still validating things, run a refresh manually:

```bash
curl -X POST https://<your-domain>/api/refresh/ai_data_center
```

This blocks for ~5 minutes (refresh fetches across all candidates + tracker live data) and returns a JSON summary. Useful right after deploy so the page has real data without waiting until 18:00 ET.

## What is and is not shared with the existing momentum scanner

**Shared (same repo, same git history):**
- `themes/` directory — both apps read it (themes_web actively, scanner doesn't import)
- `requirements.txt` — same Python deps installed in both services
- `fmp_data.py`, `news.py`, `config.py`, `scanner.py` — themes_web/tracker_refresh.py imports `config` and `news`, which is allowed

**Not shared (strict separation):**
- The deployed momentum scanner does NOT import anything from `themes/` or `themes_web/`
- The two services have separate Railway URLs
- Either service can be redeployed or rolled back independently

If something in themes_web breaks, the momentum scanner is unaffected.

## Rolling back

Railway settings → Deployments → roll back to a prior deploy. Affects only the themes_web service.

## Local development

```bash
cd "Trader v3"
uvicorn themes_web.app:app --port 8001 --reload
```

Visit `http://localhost:8001`. APScheduler starts; daily refresh runs at 18:00 ET *if you leave it running*. Manual refresh via `POST http://localhost:8001/api/refresh/ai_data_center`.

The local `.env` file (with FMP_API_KEY etc.) is automatically loaded by config.py — same behavior as the momentum scanner.
