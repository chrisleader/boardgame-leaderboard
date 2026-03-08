from __future__ import annotations

import colorsys
import csv
import itertools
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
ROOT_FACTION_COLORS_PATH = BASE_DIR / "root_faction_colors.csv"

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".heic"}
SCORE_LINE_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9 _\-\.]{1,30}?)\s+(\d{1,5})\s*$")
DATE_RE = re.compile(r"(\d{4}[\-\/]\d{1,2}[\-\/]\d{1,2})")
UNKNOWN_SCORE_TOKENS = {"na", "n/a", "?", "x", "icon", "dom", "dominance"}
ROOT_SLOT_X_RATIOS = {
    3: [0.27, 0.58, 0.84],
    4: [0.26, 0.50, 0.67, 0.84],
}
ROOT_SLOT_Y_BANDS = [(0.74, 0.81), (0.75, 0.82), (0.76, 0.83)]
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

            CREATE TABLE IF NOT EXISTS factions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_name TEXT NOT NULL,
                name TEXT NOT NULL,
                UNIQUE (game_name, name)
            );

            CREATE TABLE IF NOT EXISTS faction_color_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                sample_name TEXT NOT NULL,
                color_hex TEXT NOT NULL,
                r INTEGER NOT NULL,
                g INTEGER NOT NULL,
                b INTEGER NOT NULL,
                FOREIGN KEY (faction_id) REFERENCES factions(id) ON DELETE CASCADE,
                UNIQUE (faction_id, sample_name)
            );

            CREATE TABLE IF NOT EXISTS match_player_factions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                faction_id INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'auto_color',
                confidence REAL NOT NULL DEFAULT 0,
                sampled_r INTEGER,
                sampled_g INTEGER,
                sampled_b INTEGER,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                FOREIGN KEY (player_id) REFERENCES players(id),
                FOREIGN KEY (faction_id) REFERENCES factions(id),
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
        load_root_faction_samples(conn)


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


def parse_hex_color(raw_value: str) -> tuple[int, int, int] | None:
    cleaned = (raw_value or "").strip().lstrip("#")
    if len(cleaned) != 6 or not re.fullmatch(r"[0-9a-fA-F]{6}", cleaned):
        return None
    return tuple(int(cleaned[index : index + 2], 16) for index in (0, 2, 4))


def load_root_faction_samples(conn: sqlite3.Connection) -> None:
    if not ROOT_FACTION_COLORS_PATH.exists():
        return

    with ROOT_FACTION_COLORS_PATH.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for row in reader:
            faction_name = (row.get("Faction") or "").strip()
            if not faction_name:
                continue

            faction_row = conn.execute(
                "SELECT id FROM factions WHERE game_name = 'root' AND name = ?",
                (faction_name,),
            ).fetchone()
            if faction_row:
                faction_id = int(faction_row["id"])
            else:
                faction_id = int(
                    conn.execute(
                        "INSERT INTO factions (game_name, name) VALUES ('root', ?)",
                        (faction_name,),
                    ).lastrowid
                )

            for sample_number, column_name in enumerate(("Color Sample 1", "Color Sample 2"), start=1):
                parsed = parse_hex_color(row.get(column_name) or "")
                if parsed is None:
                    continue
                sample_name = f"sample_{sample_number}"
                color_hex = "".join(f"{value:02X}" for value in parsed)
                conn.execute(
                    """
                    INSERT INTO faction_color_samples (faction_id, sample_name, color_hex, r, g, b)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(faction_id, sample_name) DO UPDATE SET
                        color_hex = excluded.color_hex,
                        r = excluded.r,
                        g = excluded.g,
                        b = excluded.b
                    """,
                    (faction_id, sample_name, color_hex, parsed[0], parsed[1], parsed[2]),
                )


def root_faction_samples(conn: sqlite3.Connection) -> dict[str, list[tuple[int, int, int]]]:
    rows = conn.execute(
        """
        SELECT f.name AS faction_name, s.r, s.g, s.b
        FROM factions f
        JOIN faction_color_samples s ON s.faction_id = f.id
        WHERE f.game_name = 'root'
        ORDER BY f.name ASC, s.sample_name ASC
        """
    ).fetchall()
    grouped: dict[str, list[tuple[int, int, int]]] = {}
    for row in rows:
        grouped.setdefault(row["faction_name"], []).append((row["r"], row["g"], row["b"]))
    return grouped


def relative_luminance(rgb: tuple[int, int, int]) -> float:
    channels: list[float] = []
    for channel in rgb:
        normalized = channel / 255.0
        if normalized <= 0.03928:
            channels.append(normalized / 12.92)
        else:
            channels.append(((normalized + 0.055) / 1.055) ** 2.4)
    red, green, blue = channels
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue


def contrast_ratio(left_rgb: tuple[int, int, int], right_rgb: tuple[int, int, int]) -> float:
    left_luminance = relative_luminance(left_rgb)
    right_luminance = relative_luminance(right_rgb)
    lighter = max(left_luminance, right_luminance)
    darker = min(left_luminance, right_luminance)
    return (lighter + 0.05) / (darker + 0.05)


def best_text_color(background_rgb: tuple[int, int, int]) -> str:
    white = (255, 255, 255)
    near_black = (17, 24, 39)
    white_ratio = contrast_ratio(background_rgb, white)
    black_ratio = contrast_ratio(background_rgb, near_black)
    return "#FFFFFF" if white_ratio > black_ratio else "#111827"


def root_faction_palette(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    rows = conn.execute(
        """
        SELECT
            f.name AS faction_name,
            COALESCE(
                MAX(CASE WHEN s.sample_name = 'sample_1' THEN s.color_hex END),
                MIN(s.color_hex)
            ) AS color_hex
        FROM factions f
        LEFT JOIN faction_color_samples s ON s.faction_id = f.id
        WHERE f.game_name = 'root'
        GROUP BY f.name
        ORDER BY f.name ASC
        """
    ).fetchall()
    palette: dict[str, dict[str, str]] = {}
    for row in rows:
        color_hex = row["color_hex"]
        if not color_hex:
            continue
        rgb = parse_hex_color(color_hex)
        if rgb is None:
            continue
        palette[row["faction_name"]] = {
            "hex": color_hex,
            "text": best_text_color(rgb),
        }
    return palette


def median_int(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return int(round((ordered[midpoint - 1] + ordered[midpoint]) / 2))


def root_slot_ratios(player_count: int) -> list[float]:
    if player_count in ROOT_SLOT_X_RATIOS:
        return ROOT_SLOT_X_RATIOS[player_count]
    if player_count <= 1:
        return [0.5]
    start = 0.27
    end = 0.84
    gap = (end - start) / (player_count - 1)
    return [start + index * gap for index in range(player_count)]


def extract_root_slot_colors(
    image_path: Path,
    player_count: int,
    y_start_ratio: float,
    y_end_ratio: float,
) -> list[tuple[int, int, int]]:
    with Image.open(image_path) as image:
        rgb_image = image.convert("RGB")
        width, height = rgb_image.size
        colors: list[tuple[int, int, int]] = []
        for slot_ratio in root_slot_ratios(player_count):
            center_x = int(slot_ratio * width)
            span_x = max(12, int(0.045 * width))
            left = max(0, center_x - span_x)
            right = min(width, center_x + span_x)
            top = max(0, int(y_start_ratio * height))
            bottom = min(height, int(y_end_ratio * height))

            patch = rgb_image.crop((left, top, right, bottom))
            patch_pixels = list(patch.getdata())
            filtered_pixels: list[tuple[int, int, int]] = []
            for red, green, blue in patch_pixels:
                channel_max = max(red, green, blue)
                channel_min = min(red, green, blue)
                if channel_max <= 35 or channel_max >= 245:
                    continue
                saturation = (channel_max - channel_min) / max(channel_max, 1)
                if saturation <= 0.18:
                    continue
                filtered_pixels.append((red, green, blue))
            usable = filtered_pixels if len(filtered_pixels) >= max(20, len(patch_pixels) // 8) else patch_pixels
            reds = [pixel[0] for pixel in usable]
            greens = [pixel[1] for pixel in usable]
            blues = [pixel[2] for pixel in usable]
            colors.append((median_int(reds), median_int(greens), median_int(blues)))
        return colors


def root_hsv_distance(left_rgb: tuple[int, int, int], right_rgb: tuple[int, int, int]) -> float:
    left_hsv = colorsys.rgb_to_hsv(*(channel / 255.0 for channel in left_rgb))
    right_hsv = colorsys.rgb_to_hsv(*(channel / 255.0 for channel in right_rgb))
    hue_distance = abs(left_hsv[0] - right_hsv[0])
    hue_distance = min(hue_distance, 1.0 - hue_distance)
    return (
        2.8 * hue_distance
        + 0.9 * abs(left_hsv[1] - right_hsv[1])
        + 0.55 * abs(left_hsv[2] - right_hsv[2])
    )


def assign_root_factions(
    slot_colors: list[tuple[int, int, int]],
    faction_samples_map: dict[str, list[tuple[int, int, int]]],
) -> tuple[list[tuple[str, float]], float] | None:
    faction_names = sorted(faction_samples_map)
    player_count = len(slot_colors)
    if player_count == 0 or len(faction_names) < player_count:
        return None

    distance_rows: list[dict[str, float]] = []
    for slot_color in slot_colors:
        row = {}
        for faction_name in faction_names:
            row[faction_name] = min(
                root_hsv_distance(slot_color, sample_rgb)
                for sample_rgb in faction_samples_map[faction_name]
            )
        distance_rows.append(row)

    best_score: float | None = None
    best_permutation: tuple[str, ...] | None = None
    second_best_score: float | None = None
    for permutation in itertools.permutations(faction_names, player_count):
        score = sum(distance_rows[index][faction_name] for index, faction_name in enumerate(permutation))
        if best_score is None or score < best_score:
            if best_score is not None:
                second_best_score = best_score
            best_score = score
            best_permutation = permutation
            continue
        if second_best_score is None or score < second_best_score:
            second_best_score = score

    if best_permutation is None or best_score is None:
        return None

    comparison_score = second_best_score if second_best_score is not None else best_score + 1
    global_confidence = max(
        0.0,
        min(1.0, (comparison_score - best_score) / max(comparison_score, 1e-6)),
    )

    assignments: list[tuple[str, float]] = []
    for index, faction_name in enumerate(best_permutation):
        sorted_distances = sorted(distance_rows[index].values())
        best_distance = sorted_distances[0]
        second_distance = sorted_distances[1] if len(sorted_distances) > 1 else best_distance + 1
        slot_confidence = max(
            0.0,
            min(1.0, (second_distance - best_distance) / max(second_distance, 1e-6)),
        )
        confidence = max(0.0, min(1.0, 0.5 * slot_confidence + 0.5 * global_confidence))
        assignments.append((faction_name, confidence))

    return assignments, global_confidence


def infer_root_factions_for_match(conn: sqlite3.Connection, match_id: int) -> int:
    match_row = conn.execute(
        """
        SELECT m.id, m.screenshot_path
        FROM matches m
        JOIN games g ON g.id = m.game_id
        WHERE m.id = ? AND m.relevant = 1 AND g.name = 'root'
        """,
        (match_id,),
    ).fetchone()
    if not match_row:
        return 0

    image_path = Path(match_row["screenshot_path"])
    if not image_path.exists():
        return 0

    participants = conn.execute(
        """
        SELECT ms.player_id
        FROM match_scores ms
        WHERE ms.match_id = ?
        ORDER BY
            CASE WHEN ms.placement IS NULL THEN 999 ELSE ms.placement END ASC,
            CASE WHEN ms.score IS NULL THEN -999999 ELSE ms.score END DESC,
            ms.player_id ASC
        """,
        (match_id,),
    ).fetchall()
    player_count = len(participants)
    if player_count < 2:
        return 0

    faction_samples_map = root_faction_samples(conn)
    if len(faction_samples_map) < player_count:
        return 0

    best_assignment: list[tuple[str, float]] | None = None
    best_slot_colors: list[tuple[int, int, int]] | None = None
    best_confidence = -1.0
    for y_start_ratio, y_end_ratio in ROOT_SLOT_Y_BANDS:
        slot_colors = extract_root_slot_colors(image_path, player_count, y_start_ratio, y_end_ratio)
        assigned = assign_root_factions(slot_colors, faction_samples_map)
        if assigned is None:
            continue
        assignments, global_confidence = assigned
        if global_confidence > best_confidence:
            best_confidence = global_confidence
            best_assignment = assignments
            best_slot_colors = slot_colors

    if best_assignment is None or best_slot_colors is None:
        return 0

    faction_id_rows = conn.execute(
        "SELECT id, name FROM factions WHERE game_name = 'root'"
    ).fetchall()
    faction_id_by_name = {row["name"]: int(row["id"]) for row in faction_id_rows}

    updated = 0
    updated_at = datetime.utcnow().isoformat()
    for participant_row, (faction_name, confidence), sampled_rgb in zip(
        participants, best_assignment, best_slot_colors
    ):
        player_id = int(participant_row["player_id"])
        existing = conn.execute(
            "SELECT source FROM match_player_factions WHERE match_id = ? AND player_id = ?",
            (match_id, player_id),
        ).fetchone()
        if existing and existing["source"] == "manual":
            continue
        faction_id = faction_id_by_name.get(faction_name)
        if faction_id is None:
            continue
        conn.execute(
            """
            INSERT INTO match_player_factions (
                match_id,
                player_id,
                faction_id,
                source,
                confidence,
                sampled_r,
                sampled_g,
                sampled_b,
                updated_at
            )
            VALUES (?, ?, ?, 'auto_color', ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, player_id) DO UPDATE SET
                faction_id = excluded.faction_id,
                source = excluded.source,
                confidence = excluded.confidence,
                sampled_r = excluded.sampled_r,
                sampled_g = excluded.sampled_g,
                sampled_b = excluded.sampled_b,
                updated_at = excluded.updated_at
            """,
            (
                match_id,
                player_id,
                faction_id,
                confidence,
                sampled_rgb[0],
                sampled_rgb[1],
                sampled_rgb[2],
                updated_at,
            ),
        )
        updated += 1
    return updated


def backfill_root_factions(conn: sqlite3.Connection) -> int:
    updated = 0
    match_rows = conn.execute(
        """
        SELECT m.id
        FROM matches m
        JOIN games g ON g.id = m.game_id
        WHERE m.relevant = 1 AND g.name = 'root'
        ORDER BY m.id ASC
        """
    ).fetchall()
    for row in match_rows:
        updated += infer_root_factions_for_match(conn, int(row["id"]))
    return updated


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


def root_faction_share_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        WITH root_matches AS (
            SELECT m.id
            FROM matches m
            JOIN games g ON g.id = m.game_id
            WHERE m.relevant = 1 AND g.name = 'root'
        ),
        total AS (
            SELECT COUNT(*) AS total_games
            FROM root_matches
        ),
        usage AS (
            SELECT
                f.name AS faction_name,
                COUNT(DISTINCT mpf.match_id) AS games_with_faction
            FROM match_player_factions mpf
            JOIN factions f ON f.id = mpf.faction_id
            JOIN root_matches rm ON rm.id = mpf.match_id
            WHERE f.game_name = 'root'
            GROUP BY f.name
        )
        SELECT
            usage.faction_name,
            usage.games_with_faction,
            total.total_games,
            CASE
                WHEN total.total_games > 0 THEN usage.games_with_faction * 1.0 / total.total_games
                ELSE 0
            END AS game_share
        FROM usage
        JOIN total
        ORDER BY game_share DESC, games_with_faction DESC, faction_name ASC
        """
    ).fetchall()


def root_faction_win_rate_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    prepare_temp_views(conn)
    return conn.execute(
        """
        SELECT
            f.name AS faction_name,
            COUNT(*) AS games_played,
            COALESCE(SUM(mw.is_winner), 0) AS wins,
            COALESCE(SUM(mw.is_winner), 0) * 1.0 / COUNT(*) AS win_rate
        FROM match_player_factions mpf
        JOIN factions f ON f.id = mpf.faction_id AND f.game_name = 'root'
        JOIN matches m ON m.id = mpf.match_id AND m.relevant = 1
        JOIN games g ON g.id = m.game_id AND g.name = 'root'
        LEFT JOIN match_winners mw ON mw.match_id = mpf.match_id AND mw.player_id = mpf.player_id
        GROUP BY f.name
        ORDER BY win_rate DESC, wins DESC, games_played DESC, faction_name ASC
        """
    ).fetchall()


def root_faction_matchup_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    prepare_temp_views(conn)
    return conn.execute(
        """
        WITH root_assignments AS (
            SELECT
                mpf.match_id,
                mpf.player_id,
                f.name AS faction_name
            FROM match_player_factions mpf
            JOIN matches m ON m.id = mpf.match_id AND m.relevant = 1
            JOIN games g ON g.id = m.game_id AND g.name = 'root'
            JOIN factions f ON f.id = mpf.faction_id AND f.game_name = 'root'
        ),
        faction_vs_opponent AS (
            SELECT DISTINCT
                ra.match_id,
                ra.player_id,
                ra.faction_name,
                opp.faction_name AS opponent_faction_name
            FROM root_assignments ra
            JOIN root_assignments opp
              ON opp.match_id = ra.match_id
             AND opp.player_id != ra.player_id
             AND opp.faction_name != ra.faction_name
        )
        SELECT
            fvo.faction_name,
            fvo.opponent_faction_name,
            COUNT(DISTINCT fvo.match_id) AS games_against,
            COALESCE(SUM(mw.is_winner), 0) AS wins,
            COALESCE(SUM(mw.is_winner), 0) * 1.0 / COUNT(DISTINCT fvo.match_id) AS win_rate
        FROM faction_vs_opponent fvo
        LEFT JOIN match_winners mw
          ON mw.match_id = fvo.match_id
         AND mw.player_id = fvo.player_id
        GROUP BY fvo.faction_name, fvo.opponent_faction_name
        ORDER BY fvo.faction_name ASC, win_rate DESC, wins DESC, fvo.opponent_faction_name ASC
        """
    ).fetchall()


def semantic_win_rate_colors(win_rate: float) -> dict[str, str]:
    clamped = max(0.0, min(1.0, win_rate))
    hue = 120.0 * clamped
    saturation = 0.65
    lightness = 0.84
    red_f, green_f, blue_f = colorsys.hls_to_rgb(hue / 360.0, lightness, saturation)
    red = round(red_f * 255)
    green = round(green_f * 255)
    blue = round(blue_f * 255)
    rgb = (red, green, blue)
    return {
        "background": f"#{red:02X}{green:02X}{blue:02X}",
        "text": best_text_color(rgb),
    }


def build_root_faction_matchup_matrix(
    rows: list[sqlite3.Row],
) -> tuple[list[str], list[dict[str, object]]]:
    factions: set[str] = set()
    win_rate_map: dict[tuple[str, str], float] = {}
    wins_map: dict[tuple[str, str], int] = {}

    for row in rows:
        faction_name = str(row["faction_name"])
        opponent_faction_name = str(row["opponent_faction_name"])
        factions.add(faction_name)
        factions.add(opponent_faction_name)
        win_rate_map[(faction_name, opponent_faction_name)] = float(row["win_rate"])
        wins_map[(faction_name, opponent_faction_name)] = int(row["wins"])

    ordered_factions = sorted(factions)
    matrix_rows: list[dict[str, object]] = []
    for faction_name in ordered_factions:
        cells: list[dict[str, object]] = []
        for opponent_faction_name in ordered_factions:
            if faction_name == opponent_faction_name:
                cells.append(
                    {
                        "win_rate": None,
                        "is_diagonal": True,
                    }
                )
                continue
            win_rate = win_rate_map.get((faction_name, opponent_faction_name))
            if win_rate is None:
                cells.append(
                    {
                        "win_rate": None,
                        "wins": None,
                        "is_diagonal": False,
                    }
                )
                continue

            style = semantic_win_rate_colors(win_rate)
            cells.append(
                {
                    "win_rate": win_rate,
                    "wins": wins_map[(faction_name, opponent_faction_name)],
                    "is_diagonal": False,
                    "background": style["background"],
                    "text": style["text"],
                }
            )
        matrix_rows.append({"faction_name": faction_name, "cells": cells})

    return ordered_factions, matrix_rows


def root_player_faction_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    prepare_temp_views(conn)
    return conn.execute(
        """
        SELECT
            p.name AS player_name,
            f.name AS faction_name,
            COUNT(*) AS games_played,
            COALESCE(SUM(mw.is_winner), 0) AS wins,
            COALESCE(SUM(mw.is_winner), 0) * 1.0 / COUNT(*) AS win_rate
        FROM match_player_factions mpf
        JOIN players p ON p.id = mpf.player_id
        JOIN factions f ON f.id = mpf.faction_id AND f.game_name = 'root'
        JOIN matches m ON m.id = mpf.match_id AND m.relevant = 1
        JOIN games g ON g.id = m.game_id AND g.name = 'root'
        LEFT JOIN match_winners mw ON mw.match_id = mpf.match_id AND mw.player_id = mpf.player_id
        GROUP BY p.id, p.name, f.name
        ORDER BY p.name ASC, games_played DESC, win_rate DESC, wins DESC, faction_name ASC
        """
    ).fetchall()


def group_root_player_faction_rows(rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        player_name = str(row["player_name"])
        grouped.setdefault(player_name, []).append(row)
    return [{"player_name": player_name, "rows": player_rows} for player_name, player_rows in grouped.items()]


def group_per_game_rows(rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        game_name = str(row["game_name"])
        grouped.setdefault(game_name, []).append(row)
    return [{"game_name": game_name, "rows": game_rows} for game_name, game_rows in grouped.items()]


@app.route("/")
def home():
    with db_conn() as conn:
        load_root_faction_samples(conn)
        backfill_root_factions(conn)
        leaderboard = leaderboard_rows(conn)
        per_game = per_game_win_rates(conn)
        per_game_groups = group_per_game_rows(per_game)
        root_faction_share = root_faction_share_rows(conn)
        root_faction_win_rates = root_faction_win_rate_rows(conn)
        root_faction_matchups = root_faction_matchup_rows(conn)
        root_faction_matchup_factions, root_faction_matchup_matrix = build_root_faction_matchup_matrix(
            root_faction_matchups
        )
        root_player_faction_rows_data = root_player_faction_rows(conn)
        root_player_faction_groups = group_root_player_faction_rows(root_player_faction_rows_data)
        root_faction_palette_map = root_faction_palette(conn)

    return render_template(
        "home.html",
        leaderboard=leaderboard,
        per_game=per_game,
        per_game_groups=per_game_groups,
        root_faction_share=root_faction_share,
        root_faction_win_rates=root_faction_win_rates,
        root_faction_matchup_factions=root_faction_matchup_factions,
        root_faction_matchup_matrix=root_faction_matchup_matrix,
        root_player_faction_groups=root_player_faction_groups,
        root_faction_palette=root_faction_palette_map,
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
            if game_name.strip().lower() == "root":
                load_root_faction_samples(conn)
                infer_root_factions_for_match(conn, match_id)
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
