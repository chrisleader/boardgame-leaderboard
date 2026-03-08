# Board Game Leaderboard (Screenshot-driven)

This app helps build an online leaderboard from screenshot results of games.

It is designed for mixed screenshot sources (you + friends), so it does **not** infer winners from words like "victory" / "defeat". Instead, it uses extracted per-player scores and a game-specific winner rule.

## Features

- Scan a screenshots folder recursively
- OCR text extraction per image (if Tesseract is installed)
- Heuristic relevance scoring to find likely result screenshots
- Manual review/confirmation flow for each candidate
- SQLite-backed leaderboard with wins, placement points, and win-share metrics
- Static export of the homepage to `docs/index.html`
- GitHub Pages workflow that publishes the static page on push to `main`

## Quick start

1. Create and activate a virtualenv.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install Tesseract OCR (recommended):

```bash
brew install tesseract
```

4. Run app:

```bash
python app.py
```

5. Open [http://localhost:8080](http://localhost:8080)

## Publish a static public page (GitHub Pages)

This repo includes:

- Static builder script: `build_static_site.py`
- GitHub Pages workflow: `.github/workflows/publish-pages.yml`

### First-time setup

1. In GitHub, open repository **Settings → Pages**.
2. Under **Build and deployment**, choose **GitHub Actions**.
3. Push to `main` (or run the workflow manually from Actions).
4. GitHub will publish `docs/index.html` as the public site.

### Ongoing update flow (new finished game)

1. Add the new screenshot file under `screenshots/<game>/`.
2. Run the local app and review/save scores for the new match:
   - `python app.py`
   - open `http://localhost:8080` and save the result.
3. Rebuild static page:
   - `python build_static_site.py`
4. Commit and push:
   - `git add screenshots leaderboard.db docs/index.html`
   - `git commit -m "Add new game result"`
   - `git push`
5. GitHub Pages auto-refreshes from the workflow.

## Workflow

1. Scan your screenshots folder (e.g. `~/Desktop/Screenshots`).
2. Open `Review` on high-relevance screenshots.
3. Confirm game name + score lines (`Player,Score`).
   - For non-numeric dominance/coalition result markers, use `Player,NA`.
4. Save result to update leaderboard.

## Notes

- Images are referenced by absolute path in the database.
- If OCR is unavailable, screenshots can still be reviewed manually.
- The leaderboard ranking uses overall win share (wins / games played), and also shows per-game win share.
- For matches with non-numeric dominance/coalition markers, winner detection uses placement order (1st place) instead of VP totals.
