from __future__ import annotations

from pathlib import Path

from flask import render_template

import app as leaderboard_app


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "docs"
OUTPUT_FILE = OUTPUT_DIR / "index.html"


def build_static_homepage() -> Path:
    leaderboard_app.init_db()
    with leaderboard_app.db_conn() as conn:
        leaderboard = leaderboard_app.leaderboard_rows(conn)
        per_game = leaderboard_app.per_game_win_rates(conn)
        per_game_map = leaderboard_app.per_game_rows_map(per_game)
        root_module_context = leaderboard_app.load_root_module_context(conn)

    with leaderboard_app.app.app_context():
        html = render_template(
            "home.html",
            leaderboard=leaderboard,
            per_game=per_game,
            per_game_map=per_game_map,
            **root_module_context,
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    return OUTPUT_FILE


if __name__ == "__main__":
    built = build_static_homepage()
    print(f"Built static homepage: {built}")
