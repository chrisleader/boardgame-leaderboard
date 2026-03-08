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
        leaderboard_app.load_root_faction_samples(conn)
        leaderboard_app.backfill_root_factions(conn)
        leaderboard = leaderboard_app.leaderboard_rows(conn)
        per_game = leaderboard_app.per_game_win_rates(conn)
        per_game_groups = leaderboard_app.group_per_game_rows(per_game)
        root_faction_share = leaderboard_app.root_faction_share_rows(conn)
        root_faction_win_rates = leaderboard_app.root_faction_win_rate_rows(conn)
        root_faction_matchups = leaderboard_app.root_faction_matchup_rows(conn)
        root_faction_matchup_factions, root_faction_matchup_matrix = (
            leaderboard_app.build_root_faction_matchup_matrix(root_faction_matchups)
        )
        root_player_faction_rows = leaderboard_app.root_player_faction_rows(conn)
        root_player_faction_groups = leaderboard_app.group_root_player_faction_rows(root_player_faction_rows)
        root_faction_palette = leaderboard_app.root_faction_palette(conn)

    with leaderboard_app.app.app_context():
        html = render_template(
            "home.html",
            leaderboard=leaderboard,
            per_game=per_game,
            per_game_groups=per_game_groups,
            root_faction_share=root_faction_share,
            root_faction_win_rates=root_faction_win_rates,
            root_faction_matchup_factions=root_faction_matchup_factions,
            root_faction_matchup_matrix=root_faction_matchup_matrix,
            root_player_faction_groups=root_player_faction_groups,
            root_faction_palette=root_faction_palette,
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    return OUTPUT_FILE


if __name__ == "__main__":
    built = build_static_homepage()
    print(f"Built static homepage: {built}")
