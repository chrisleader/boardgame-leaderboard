from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable, NamedTuple

from dateutil import parser as dt_parser
from flask import Flask, flash, redirect, render_template, request, send_file, url_for
from PIL import Image

try:
    import pytesseract
except Exception:  # pragma: no cover
    pytesseract = None


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "leaderboard.db"

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".heic"}
SCORE_LINE_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9 _\-\.]{1,30}?)\s+(\d{1,5})\s*$")
DATE_RE = re.compile(r"(\d{4}[\-\/]\d{1,2}[\-\/]\d{1,2})")
UNKNOWN_SCORE_TOKENS = {"na", "n/a", "?", "x", "icon", "dom", "dominance"}
PLAYER_ALIASES = {
    "migidoes": "hypersundays",
}


class ExtractedScore(NamedTuple):
    player_name: str
    score: int | None
    score_status: str = "numeric"


app = Flask(__name__)
app.secret_key = "leaderboard-dev-key"


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                higher_is_better INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                screenshot_path TEXT NOT NULL UNIQUE,
                ocr_text TEXT,
                extracted_scores_json TEXT,
                relevance_score REAL NOT NULL DEFAULT 0,
                relevant INTEGER,
                reviewed INTEGER NOT NULL DEFAULT 0,
                game_id INTEGER,
                played_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (game_id) REFERENCES games(id)
            );

            CREATE TABLE IF NOT EXISTS match_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                score INTEGER,
                score_status TEXT NOT NULL DEFAULT 'numeric',
                placement INTEGER,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                FOREIGN KEY (player_id) REFERENCES players(id),
                UNIQUE (match_id, player_id)
            );
            """
        )
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(match_scores)").fetchall()
        }
        if "score_status" not in columns:
            conn.execute(
                "ALTER TABLE match_scores ADD COLUMN score_status TEXT NOT NULL DEFAULT 'numeric'"
            )
        if "placement" not in columns:
            conn.execute("ALTER TABLE match_scores ADD COLUMN placement INTEGER")

        table_info = conn.execute("PRAGMA table_info(match_scores)").fetchall()
        score_info = next((row for row in table_info if row["name"] == "score"), None)
        if score_info and int(score_info["notnull"]) == 1:
            old_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(match_scores)").fetchall()
            }
            score_status_expr = "COALESCE(score_status, 'numeric')" if "score_status" in old_columns else "'numeric'"
            placement_expr = "placement" if "placement" in old_columns else "NULL"
            conn.executescript(
                f"""
                ALTER TABLE match_scores RENAME TO match_scores_old;
                CREATE TABLE match_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id INTEGER NOT NULL,
                    player_id INTEGER NOT NULL,
                    score INTEGER,
                    score_status TEXT NOT NULL DEFAULT 'numeric',
                    placement INTEGER,
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                    FOREIGN KEY (player_id) REFERENCES players(id),
                    UNIQUE (match_id, player_id)
                );
                INSERT INTO match_scores (id, match_id, player_id, score, score_status, placement)
                SELECT
                    id,
                    match_id,
                    player_id,
                    score,
                    {score_status_expr},
                    {placement_expr}
                FROM match_scores_old;
                DROP TABLE match_scores_old;
                """
            )


def safe_parse_datetime(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    try:
        return dt_parser.parse(raw_value).isoformat()
    except Exception:
        return None


def run_ocr(image_path: Path) -> str:
    if pytesseract is None:
        return ""
    try:
        with Image.open(image_path) as image:
            return pytesseract.image_to_string(image)
    except Exception:
        return ""


def extract_scores(ocr_text: str) -> list[ExtractedScore]:
    scores: list[ExtractedScore] = []
    seen = set()

    for line in ocr_text.splitlines():
        match = SCORE_LINE_RE.match(line)
        if not match:
            continue
        player_name = canonicalize_player_name(re.sub(r"\s+", " ", match.group(1).strip()))
        score = int(match.group(2))
        if len(player_name) < 2:
            continue
        key = (player_name.lower(), score)
        if key in seen:
            continue
        seen.add(key)
        scores.append(ExtractedScore(player_name=player_name, score=score))

    return scores


def calc_relevance(ocr_text: str, extracted_scores: list[ExtractedScore]) -> float:
    if not ocr_text:
        return 0.0

    text = ocr_text.lower()
    keyword_hits = sum(
        token in text
        for token in ["score", "points", "leaderboard", "ranking", "round", "final", "results"]
    )

    if len(extracted_scores) >= 2:
        numeric_signal = min(len(extracted_scores), 6)
    else:
        numeric_signal = 0

    relevance = numeric_signal * 0.9 + keyword_hits * 0.6

    if "victory" in text or "defeat" in text:
        relevance += 0.1

    return relevance


def infer_played_at(image_path: Path, ocr_text: str) -> str | None:
    filename_match = DATE_RE.search(image_path.stem)
    if filename_match:
        parsed = safe_parse_datetime(filename_match.group(1))
        if parsed:
            return parsed

    text_match = DATE_RE.search(ocr_text)
    if text_match:
        parsed = safe_parse_datetime(text_match.group(1))
        if parsed:
            return parsed

    return datetime.fromtimestamp(image_path.stat().st_mtime).isoformat()


def iter_image_files(folder: Path) -> Iterable[Path]:
    for root, _, files in os.walk(folder):
        for filename in files:
            path = Path(root) / filename
            if path.suffix.lower() in IMAGE_EXTENSIONS:
                yield path


def upsert_match_from_image(conn: sqlite3.Connection, image_path: Path) -> bool:
    existing = conn.execute(
        "SELECT id FROM matches WHERE screenshot_path = ?", (str(image_path),)
    ).fetchone()
    if existing:
        return False

    ocr_text = run_ocr(image_path)
    extracted_scores = extract_scores(ocr_text)
    relevance_score = calc_relevance(ocr_text, extracted_scores)
    played_at = infer_played_at(image_path, ocr_text)

    conn.execute(
        """
        INSERT INTO matches (
            screenshot_path,
            ocr_text,
            extracted_scores_json,
            relevance_score,
            relevant,
            reviewed,
            played_at,
            created_at
        )
        VALUES (?, ?, ?, ?, NULL, 0, ?, ?)
        """,
        (
            str(image_path),
            ocr_text,
            json.dumps([score._asdict() for score in extracted_scores]),
            relevance_score,
            played_at,
            datetime.utcnow().isoformat(),
        ),
    )
    return True


def ensure_game(conn: sqlite3.Connection, name: str, higher_is_better: bool) -> int:
    row = conn.execute("SELECT id FROM games WHERE name = ?", (name.strip(),)).fetchone()
    if row:
        conn.execute(
            "UPDATE games SET higher_is_better = ? WHERE id = ?",
            (1 if higher_is_better else 0, row["id"]),
        )
        return int(row["id"])

    cursor = conn.execute(
        "INSERT INTO games (name, higher_is_better) VALUES (?, ?)",
        (name.strip(), 1 if higher_is_better else 0),
    )
    return int(cursor.lastrowid)


def canonicalize_player_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name.strip())
    if not cleaned:
        return cleaned
    alias_target = PLAYER_ALIASES.get(cleaned.lower())
    return alias_target if alias_target else cleaned


def ensure_player(conn: sqlite3.Connection, name: str) -> int:
    canonical_name = canonicalize_player_name(name)
    row = conn.execute("SELECT id FROM players WHERE name = ?", (canonical_name,)).fetchone()
    if row:
        return int(row["id"])

    cursor = conn.execute("INSERT INTO players (name) VALUES (?)", (canonical_name,))
    return int(cursor.lastrowid)


def parse_scores_text(raw_scores: str) -> list[ExtractedScore]:
    parsed: list[ExtractedScore] = []

    for line in raw_scores.splitlines():
        line = line.strip()
        if not line:
            continue

        if "," in line:
            maybe_name, maybe_score = line.rsplit(",", maxsplit=1)
        elif ":" in line:
            maybe_name, maybe_score = line.rsplit(":", maxsplit=1)
        else:
            parts = line.rsplit(" ", maxsplit=1)
            if len(parts) != 2:
                continue
            maybe_name, maybe_score = parts

        maybe_name = maybe_name.strip()
        maybe_score = maybe_score.strip()

        if not maybe_name:
            continue

        if maybe_score.isdigit():
            parsed.append(
                ExtractedScore(
                    player_name=canonicalize_player_name(maybe_name),
                    score=int(maybe_score),
                    score_status="numeric",
                )
            )
            continue

        if maybe_score.lower() in UNKNOWN_SCORE_TOKENS:
            parsed.append(
                ExtractedScore(
                    player_name=canonicalize_player_name(maybe_name),
                    score=None,
                    score_status="unknown",
                )
            )

    return parsed


def assign_placements(scores: list[ExtractedScore], higher_is_better: bool) -> list[int]:
    indexed = list(enumerate(scores))
    known = [(idx, item) for idx, item in indexed if item.score_status == "numeric" and item.score is not None]
    unknown = [(idx, item) for idx, item in indexed if item.score_status != "numeric" or item.score is None]

    if higher_is_better:
        known.sort(key=lambda item: item[1].score, reverse=True)
    else:
        known.sort(key=lambda item: item[1].score)

    placements: list[int] = [0] * len(scores)
    position = 1
    for idx, _ in known:
        placements[idx] = position
        position += 1
    for idx, _ in unknown:
        placements[idx] = position
        position += 1
    return placements


def prepare_temp_views(conn: sqlite3.Connection) -> None:
    conn.execute("DROP VIEW IF EXISTS match_winners")
    conn.execute(
        """
        CREATE TEMP VIEW match_winners AS
        WITH flags AS (
            SELECT
                ms.match_id,
                MAX(
                    CASE
                        WHEN ms.score_status != 'numeric' OR ms.score IS NULL THEN 1
                        ELSE 0
                    END
                ) AS has_unknown
            FROM match_scores ms
            GROUP BY ms.match_id
        ),
        bounds AS (
            SELECT
                m.id AS match_id,
                g.higher_is_better AS higher_is_better,
                MAX(ms.score) AS max_score,
                MIN(ms.score) AS min_score
            FROM matches m
            JOIN games g ON g.id = m.game_id
            JOIN match_scores ms ON ms.match_id = m.id
            WHERE m.relevant = 1
              AND ms.score_status = 'numeric'
            GROUP BY m.id, g.higher_is_better
        )
        SELECT
            ms.match_id,
            ms.player_id,
            CASE
                WHEN f.has_unknown = 1 AND ms.placement = 1 THEN 1
                WHEN f.has_unknown = 0 AND b.higher_is_better = 1 AND ms.score = b.max_score THEN 1
                WHEN f.has_unknown = 0 AND b.higher_is_better = 0 AND ms.score = b.min_score THEN 1
                ELSE 0
            END AS is_winner
        FROM match_scores ms
        JOIN matches m ON m.id = ms.match_id AND m.relevant = 1
        JOIN flags f ON f.match_id = ms.match_id
        LEFT JOIN bounds b ON b.match_id = ms.match_id
        """
    )

    conn.execute("DROP VIEW IF EXISTS match_participants")
    conn.execute(
        """
        CREATE TEMP VIEW match_participants AS
        SELECT
            ms.match_id,
            ms.player_id,
            ms.placement,
            ms.score,
            ms.score_status,
            COUNT(*) OVER (PARTITION BY ms.match_id) AS field_size
        FROM match_scores ms
        JOIN matches m ON m.id = ms.match_id
        WHERE m.relevant = 1
        """
    )


def leaderboard_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    prepare_temp_views(conn)
    return conn.execute(
        """
        SELECT
            p.id,
            p.name,
            COUNT(mp.match_id) AS games_played,
            COALESCE(SUM(CASE WHEN mp.score_status = 'numeric' THEN mp.score END), 0) AS total_points,
            COALESCE(SUM(mw.is_winner), 0) AS wins,
            COALESCE(
                SUM(
                    CASE
                        WHEN mp.placement IS NOT NULL THEN mp.field_size - mp.placement
                        ELSE 0
                    END
                ),
                0
            ) AS placement_points,
            COALESCE(SUM(mw.is_winner), 0) * 1.0 / COUNT(mp.match_id) AS win_rate
        FROM players p
        LEFT JOIN match_participants mp ON mp.player_id = p.id
        LEFT JOIN match_winners mw ON mw.match_id = mp.match_id AND mw.player_id = p.id
        GROUP BY p.id, p.name
        HAVING games_played > 0
        ORDER BY win_rate DESC, wins DESC, games_played DESC, placement_points DESC, p.name ASC
        """
    ).fetchall()


def per_game_win_rates(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    prepare_temp_views(conn)
    return conn.execute(
        """
        SELECT
            g.name AS game_name,
            p.name AS player_name,
            COUNT(mp.match_id) AS games_played,
            COALESCE(SUM(mw.is_winner), 0) AS wins,
            COALESCE(SUM(mw.is_winner), 0) * 1.0 / COUNT(mp.match_id) AS win_rate
        FROM match_participants mp
        JOIN matches m ON m.id = mp.match_id
        JOIN games g ON g.id = m.game_id
        JOIN players p ON p.id = mp.player_id
        LEFT JOIN match_winners mw ON mw.match_id = mp.match_id AND mw.player_id = mp.player_id
        GROUP BY g.name, p.name
        HAVING games_played > 0
        ORDER BY g.name ASC, win_rate DESC, wins DESC, p.name ASC
        """
    ).fetchall()


@app.route("/")
def home():
    with db_conn() as conn:
        leaderboard = leaderboard_rows(conn)
        per_game = per_game_win_rates(conn)

    return render_template(
        "home.html",
        leaderboard=leaderboard,
        per_game=per_game,
    )


@app.route("/scan", methods=["POST"])
def scan():
    folder_raw = (request.form.get("folder_path") or "").strip()
    if not folder_raw:
        flash("Provide a folder path.", "error")
        return redirect(url_for("home"))

    folder = Path(folder_raw).expanduser().resolve()
    if not folder.exists() or not folder.is_dir():
        flash(f"Folder not found: {folder}", "error")
        return redirect(url_for("home"))

    scanned = 0
    imported = 0
    with db_conn() as conn:
        for image_path in iter_image_files(folder):
            scanned += 1
            if upsert_match_from_image(conn, image_path):
                imported += 1

    flash(f"Scanned {scanned} image files, imported {imported} new entries from {folder}.", "success")
    return redirect(url_for("home"))


@app.route("/review/<int:match_id>", methods=["GET", "POST"])
def review(match_id: int):
    with db_conn() as conn:
        match = conn.execute("SELECT * FROM matches WHERE id = ?", (match_id,)).fetchone()
        if not match:
            flash("Match not found.", "error")
            return redirect(url_for("home"))

        if request.method == "POST":
            action = request.form.get("action", "save")

            if action == "ignore":
                conn.execute(
                    "UPDATE matches SET relevant = 0, reviewed = 1 WHERE id = ?", (match_id,)
                )
                conn.execute("DELETE FROM match_scores WHERE match_id = ?", (match_id,))
                flash("Screenshot marked as not a game result.", "success")
                return redirect(url_for("home"))

            game_name = (request.form.get("game_name") or "").strip()
            higher_is_better = request.form.get("higher_is_better", "1") == "1"
            played_at = safe_parse_datetime(request.form.get("played_at")) or match["played_at"]
            parsed_scores = parse_scores_text(request.form.get("scores_text") or "")

            if not game_name:
                flash("Game name is required.", "error")
                return redirect(url_for("review", match_id=match_id))

            if len(parsed_scores) < 2:
                flash("Add at least two player scores.", "error")
                return redirect(url_for("review", match_id=match_id))

            game_id = ensure_game(conn, game_name, higher_is_better)
            conn.execute("DELETE FROM match_scores WHERE match_id = ?", (match_id,))
            has_unknown = any(item.score_status != "numeric" or item.score is None for item in parsed_scores)
            if has_unknown:
                placements = list(range(1, len(parsed_scores) + 1))
            else:
                placements = assign_placements(parsed_scores, higher_is_better)

            for item, placement in zip(parsed_scores, placements):
                player_id = ensure_player(conn, item.player_name)
                conn.execute(
                    """
                    INSERT INTO match_scores (match_id, player_id, score, score_status, placement)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (match_id, player_id, item.score, item.score_status, placement),
                )

            conn.execute(
                """
                UPDATE matches
                SET relevant = 1,
                    reviewed = 1,
                    game_id = ?,
                    played_at = ?,
                    extracted_scores_json = ?
                WHERE id = ?
                """,
                (
                    game_id,
                    played_at,
                    json.dumps([item._asdict() for item in parsed_scores]),
                    match_id,
                ),
            )
            flash("Result saved.", "success")
            return redirect(url_for("home"))

        extracted_scores = json.loads(match["extracted_scores_json"] or "[]")
        scores_text = "\n".join(
            f"{canonicalize_player_name(row['player_name'])},{row['score'] if row['score'] is not None else 'NA'}"
            for row in extracted_scores
        )

        return render_template(
            "review.html",
            match=match,
            scores_text=scores_text,
            ocr_available=pytesseract is not None,
        )


@app.route("/image/<int:match_id>")
def serve_image(match_id: int):
    with db_conn() as conn:
        row = conn.execute(
            "SELECT screenshot_path FROM matches WHERE id = ?", (match_id,)
        ).fetchone()

    if not row:
        return "Not found", 404

    image_path = Path(row["screenshot_path"])
    if not image_path.exists():
        return "Image not found", 404

    return send_file(image_path)


init_db()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
