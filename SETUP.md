# Dealer Leaderboard, Setup Guide

This package contains everything needed to run the Shadowspec dealer leaderboard inside the Unicrest Portal, with data refreshed daily from HubSpot via a scheduled GitHub Action.

## What's in the box

- `dealer_leaderboard.html`, the frontend (single file, drop into the portal repo root)
- `data/dealer_data.json`, the seed dataset (replaced nightly by the scheduled job)
- `scripts/update_dealer_data.py`, the worker that pulls from HubSpot and writes the JSON
- `scripts/discover_properties.py`, one-time helper to find your HubSpot property names
- `scripts/requirements.txt`, Python dependencies
- `.github/workflows/update_dealers.yml`, GitHub Action that runs daily and commits the JSON

## Architecture

```
HubSpot
   │
   ▼
GitHub Action (cron, daily)
   │  runs scripts/update_dealer_data.py
   ▼
data/dealer_data.json  ◄── committed back to repo
   │
   ▼
dealer_leaderboard.html (in browser, fetches JSON on load)
```

No browser side API keys, no CORS issues, no manual sync. The page just reads a static JSON file.

## Setup steps

### 1. HubSpot Private App

- In HubSpot, go to Settings → Integrations → Private Apps → Create.
- Name it `Dealer Leaderboard Sync`.
- Scopes (read only is fine):
  - `crm.objects.companies.read`
  - `crm.objects.deals.read`
  - `crm.objects.line_items.read`
  - `crm.schemas.companies.read` (for property discovery)
- Create, copy the access token.

### 2. GitHub repo secret

- In the `unicrestgroup/unicrest-portal` repo (or wherever you deploy), go to Settings → Secrets and variables → Actions → New repository secret.
- Name: `HUBSPOT_TOKEN`
- Value: paste the token from step 1.

### 3. Drop the files into the repo

Place files at these paths in the portal repo:

```
/dealer_leaderboard.html
/data/dealer_data.json
/scripts/update_dealer_data.py
/scripts/discover_properties.py
/scripts/requirements.txt
/.github/workflows/update_dealers.yml
```

### 4. Discover HubSpot property names

The script ships with placeholder property names like `dealer_grade`, `discount_rate`, `target_2026`. These need to match the actual internal names in your HubSpot account.

Run the discovery script locally:

```bash
HUBSPOT_TOKEN=your-token-here python3 scripts/discover_properties.py
```

It prints all company properties matching keywords like `grade`, `discount`, `target`, etc., plus your deal pipeline stage IDs.

Edit the constants at the top of `scripts/update_dealer_data.py` to match. The block to update is clearly labelled `# Config: ADJUST THESE TO MATCH YOUR HUBSPOT INSTANCE`.

### 5. Test the worker locally

```bash
pip install -r scripts/requirements.txt
HUBSPOT_TOKEN=your-token-here DEBUG=1 python3 scripts/update_dealer_data.py
```

It will write `data/dealer_data.json`. Open `dealer_leaderboard.html` in a browser to confirm the dashboard renders.

### 6. Enable the GitHub Action

- Push everything to the repo.
- Go to Actions → "Update dealer leaderboard data" → Run workflow (manual trigger to test).
- Check the run log. If it succeeds, `data/dealer_data.json` is committed back to the repo.
- The cron is set to 18:00 UTC daily (06:00 NZST or 07:00 NZDT next day). Adjust `cron` in the workflow if you want a different time.

### 7. Add to the portal

- The portal serves files from the repo root, so `dealer_leaderboard.html` is now available at `https://unicrestgroup.github.io/unicrest-portal/dealer_leaderboard.html`.
- Add it to the portal's `PINNED_APPS` array, or via the portal's "Add app" form.

## Suggested portal pinned-app entry

```javascript
{
  name: "Dealer Leaderboard",
  emoji: "🏆",
  category: "Sales",
  description: "AU, NZ and USA dealer sales and product counts, refreshed daily from HubSpot",
  type: "external",
  url: "https://unicrestgroup.github.io/unicrest-portal/dealer_leaderboard.html"
}
```

## Visibility note

The portal repo is public, so `data/dealer_data.json` is publicly readable to anyone who knows the URL. Dealer revenue figures will be visible.

If that's not acceptable:

- **Easiest fix**: make the repo private and switch GitHub Pages to a private-pages plan, or move hosting to a private Azure Static Web App.
- **Better fix**: instead of committing the JSON, have the action upload it to Azure Blob Storage with a signed URL the frontend reads via Microsoft Graph (matches the portal's existing MS auth).

Happy to build the Azure variant if needed, just say the word.

## Troubleshooting

- **Workflow runs but JSON is empty**: property names in the script don't match HubSpot. Re-run `discover_properties.py` and update the constants.
- **HTTP 401 from HubSpot**: token is wrong or revoked. Regenerate in HubSpot, update the GitHub secret.
- **Workflow runs but no commit**: the JSON didn't change since last run. That's fine, no-op commits are skipped.
- **App shows "Could not load dealer data"**: the JSON path is wrong, or GitHub Pages hasn't redeployed since the commit. Wait a minute, hard refresh.
- **Dashboard loads but year columns are zero**: the deal pipeline stage filter (`WON_STAGE_IDS`) doesn't match your actual stage. Use the discovery script output to find the right stage ID.

## Operational checklist

- [ ] HubSpot Private App created, token copied
- [ ] `HUBSPOT_TOKEN` secret added to GitHub repo
- [ ] Files committed to portal repo at the paths above
- [ ] `discover_properties.py` run, constants in `update_dealer_data.py` updated
- [ ] Local test run produces a valid `data/dealer_data.json`
- [ ] First manual workflow run succeeds and commits the JSON
- [ ] App opens in the portal, shows real data and a recent "Last sync" timestamp
- [ ] Daily cron schedule confirmed in workflow file
