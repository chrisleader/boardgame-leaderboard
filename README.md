# Board Game Leaderboard (Screenshot-driven)

A configurable leaderboard system that ingests game-result screenshots, supports manual review, and publishes a static site.

The core pipeline is game-agnostic. Game-specific analytics (currently Root faction analytics) are treated as optional modules.

## What this project does

- Recursively scans screenshot folders
- Extracts OCR text (if Tesseract is available)
- Scores screenshot relevance to find likely result screens
- Supports manual review/confirmation for extracted scores
- Stores matches/scores in SQLite
- Computes overall and per-game win metrics
- Exports static HTML (`docs/index.html`) for GitHub Pages

## Architecture

### Core (generic)

- Match ingestion + OCR
- Score parsing + canonical player aliases
- Match review/save flow
- Overall + per-game leaderboards
- Static build and publish pipeline

### Optional modules

- Controlled by `ENABLED_MODULES`
- Current module: `root`
- Root module adds:
  - color-sample faction inference
  - Root-only faction analytics (share, win rates, matchup matrix, player-faction stats)
  - Root-specific UI sections

Root module data/context is loaded through a module boundary (`load_root_module_context`) and injected into templates only when enabled.

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

## Module configuration

Set enabled modules via `ENABLED_MODULES` (comma-separated).

- Default (unset): `root`
- Disable all modules:

```bash
ENABLED_MODULES="" python app.py
```

- Enable Root explicitly:

```bash
ENABLED_MODULES="root" python app.py
```

When `root` is disabled, Root-specific inference/analytics and the Root tab are hidden.

## Data and privacy model

This repo is intended to stay broadly reusable and safe to share publicly:

- keep private screenshots and local DB outside tracked git files
- publish only generated static output + reusable code/config
- avoid committing personal raw game data

Current `.gitignore` is configured to exclude local/private game assets.

## Publish static site (GitHub Pages)

### Included

- static builder: `build_static_site.py`
- pages workflow: `.github/workflows/publish-pages.yml`
- publish target: `docs/index.html`

### First-time setup

1. In GitHub: **Settings → Pages**
2. Under **Build and deployment**, choose **GitHub Actions**
3. Push to `main` (or run workflow manually)

### Ongoing update flow

1. Start app: `python app.py`
2. Open admin upload: [http://localhost:8080/admin/upload](http://localhost:8080/admin/upload)
3. Upload one screenshot
4. Review/edit detected game + scores
5. Click **Save + Publish live stats** to rebuild and push static output

Alternative CLI workflow:

```bash
./update_and_publish.sh "Add new game result"
```

The script:

- builds `docs/index.html` from local `leaderboard.db`
- stages `docs/index.html`, `root_faction_colors.csv`, and `README.md`
- commits and pushes to `main`

## Root module notes

- Baseline color file: `root_faction_colors.csv`
- Columns: `Faction`, `Color Sample 1`, `Color Sample 2`
- Use 6-char hex RGB values (without `#`), e.g. `11823E`
- Root faction assignment runs on save/rebuild when Root module is enabled

## Extending to other games/modules

To add another module, follow the Root pattern:

1. Add module-specific inference/analytics helpers
2. Gate logic with `module_enabled("<module_name>")`
3. Return module context dict (like `load_root_module_context`)
4. Render module UI conditionally in templates
5. Keep core leaderboard flow unchanged

This keeps the project usable as a general screenshot leaderboard framework while still supporting rich game-specific analytics.
