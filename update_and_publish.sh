#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required but not found in PATH." >&2
  exit 1
fi

COMMIT_MESSAGE="${*:-Update leaderboard static page ($(date -u '+%Y-%m-%d %H:%M UTC'))}"

echo "Building static homepage from local leaderboard data..."
python3 build_static_site.py

echo "Staging publishable updates..."
git add docs/index.html root_faction_colors.csv README.md

if git diff --cached --quiet; then
  echo "No staged changes found. Nothing to publish."
  exit 0
fi

echo "Committing changes..."
git commit -m "$COMMIT_MESSAGE"

echo "Pushing to origin/main..."
git push origin main

echo "Done. GitHub Pages will refresh automatically."
