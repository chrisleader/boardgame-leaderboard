from __future__ import annotations

import colorsys
import csv
import difflib
import itertools
import json
import os
import re
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Iterable, NamedTuple
from uuid import uuid4

from dateutil import parser as dt_parser
from flask import Flask, flash, get_flashed_messages, redirect, render_template, request, send_file, url_for
from PIL import Image, ImageEnhance, ImageOps
from werkzeug.utils import secure_filename

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
SUPPORTED_GAMES = ("root", "everdell", "dune")


def parse_enabled_modules(raw_value: str | None) -> set[str]:
    if raw_value is None:
        return {"root"}
    if not raw_value.strip():
        return set()
    modules = {token.strip().lower() for token in raw_value.split(",") if token.strip()}
    return modules


ENABLED_MODULES = parse_enabled_modules(os.getenv("ENABLED_MODULES"))
UPLOAD_REVIEW_ENABLED = os.getenv("ENABLE_UPLOAD_REVIEW", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def module_enabled(module_name: str) -> bool:
    return module_name.strip().lower() in ENABLED_MODULES


def upload_review_enabled() -> bool:
    return UPLOAD_REVIEW_ENABLED


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
        if module_enabled("root"):
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


def known_player_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name FROM players ORDER BY name ASC").fetchall()
    return [str(row["name"]) for row in rows]


def normalize_player_token(raw_value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", raw_value.lower())


def match_known_player_name(raw_value: str, known_players: list[str]) -> str | None:
    cleaned = canonicalize_player_name(raw_value)
    token = normalize_player_token(cleaned)
    if not token:
        return None

    if not known_players:
        return cleaned

    for player_name in known_players:
        if normalize_player_token(player_name) == token:
            return player_name

    best_name = cleaned
    best_score = 0.0
    for player_name in known_players:
        score = difflib.SequenceMatcher(
            None, normalize_player_token(player_name), token
        ).ratio()
        if score > best_score:
            best_score = score
            best_name = player_name

    return best_name if best_score >= 0.72 else None


def parse_numeric_token(raw_token: str) -> tuple[int, int] | None:
    matches = re.findall(r"\d{1,4}", raw_token)
    if not matches:
        return None

    digits = max(matches, key=len)
    value = int(digits)
    digit_len = len(digits)
    if digit_len == 4:
        value = int(digits[:3])
        digit_len = 3
    if digit_len == 3 and value > 150:
        value = int(digits[:2])
        digit_len = 2
    if value <= 0 or value > 300:
        return None
    return value, digit_len


def extract_scores_from_bottom_band(
    image_path: Path,
    known_players: list[str],
) -> list[ExtractedScore]:
    if pytesseract is None:
        return []

    try:
        with Image.open(image_path) as image:
            width, height = image.size
            x1 = int(width * 0.05)
            x2 = int(width * 0.95)
            token_rows: list[dict[str, object]] = []
            for y_start_ratio in (0.66, 0.68, 0.70):
                y1 = int(height * y_start_ratio)
                y2 = int(height * 0.98)
                if x2 <= x1 or y2 <= y1:
                    continue
                bottom_band = image.crop((x1, y1, x2, y2))
                grayscale = ImageOps.grayscale(bottom_band)
                thresholded = ImageEnhance.Contrast(grayscale).enhance(3.0).point(
                    lambda value: 255 if value > 120 else 0
                )

                for variant in (bottom_band, thresholded):
                    data = pytesseract.image_to_data(
                        variant,
                        config="--psm 6",
                        output_type=pytesseract.Output.DICT,
                    )
                    token_count = len(data.get("text", []))
                    for index in range(token_count):
                        raw_text = (data["text"][index] or "").strip()
                        if not raw_text:
                            continue
                        conf_raw = data["conf"][index]
                        try:
                            confidence = float(conf_raw)
                        except Exception:
                            confidence = -1.0
                        token_rows.append(
                            {
                                "raw_text": raw_text,
                                "left": int(data["left"][index]),
                                "top": int(data["top"][index]),
                                "width": int(data["width"][index]),
                                "height": int(data["height"][index]),
                                "confidence": confidence,
                            }
                        )
    except Exception:
        return []

    if not token_rows:
        return []

    name_positions: dict[str, dict[str, object]] = {}
    numeric_candidates: list[dict[str, object]] = []
    for token in token_rows:
        raw_text = str(token["raw_text"])
        confidence = float(token["confidence"])
        left = int(token["left"])

        if re.search(r"[A-Za-z]", raw_text):
            alpha_only = re.sub(r"[^A-Za-z]", "", raw_text)
            if len(alpha_only) >= 3 and confidence >= 30:
                matched_name = match_known_player_name(raw_text, known_players)
                if not matched_name:
                    continue
                existing = name_positions.get(matched_name)
                if not existing or confidence > float(existing["confidence"]):
                    name_positions[matched_name] = {"left": left, "confidence": confidence}

        numeric = parse_numeric_token(raw_text)
        if numeric is None:
            continue
        score_value, digit_len = numeric
        if digit_len == 1 and confidence < 70:
            continue
        numeric_candidates.append(
            {
                "score": score_value,
                "digits": digit_len,
                "left": left,
                "confidence": confidence,
            }
        )

    if not name_positions:
        return []

    sorted_names = sorted(
        name_positions.items(),
        key=lambda row: int(row[1]["left"]),
    )
    used_candidate_indexes: set[int] = set()
    max_horizontal_gap = max(120, int((x2 - x1) * 0.25))
    extracted: list[ExtractedScore] = []

    for player_name, position in sorted_names:
        name_x = int(position["left"])
        best_candidate_idx: int | None = None
        best_rank: tuple[int, int, float] | None = None
        for index, candidate in enumerate(numeric_candidates):
            if index in used_candidate_indexes:
                continue
            score_x = int(candidate["left"])
            distance = name_x - score_x
            if distance < 0 or distance > max_horizontal_gap:
                continue
            rank = (
                0 if int(candidate["digits"]) >= 2 else 1,
                distance,
                -float(candidate["confidence"]),
            )
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best_candidate_idx = index

        if best_candidate_idx is None:
            extracted.append(
                ExtractedScore(
                    player_name=player_name,
                    score=None,
                    score_status="unknown",
                )
            )
            continue

        used_candidate_indexes.add(best_candidate_idx)
        extracted.append(
            ExtractedScore(
                player_name=player_name,
                score=int(numeric_candidates[best_candidate_idx]["score"]),
                score_status="numeric",
            )
        )

    return extracted


def extract_scores_from_ranked_rows(
    image_path: Path,
    known_players: list[str],
) -> list[ExtractedScore]:
    if pytesseract is None:
        return []

    def extract_from_token_rows(
        token_rows: list[dict[str, object]],
        image_width: int,
    ) -> list[ExtractedScore]:
        if not token_rows:
            return []

        name_positions: dict[str, dict[str, object]] = {}
        numeric_candidates: list[dict[str, object]] = []
        for token in token_rows:
            raw_text = str(token["raw_text"])
            confidence = float(token["confidence"])
            left = int(token["left"])
            top = int(token["top"])
            width = int(token["width"])
            height = int(token["height"])

            if re.search(r"[A-Za-z]", raw_text):
                alpha_only = re.sub(r"[^A-Za-z]", "", raw_text)
                if len(alpha_only) >= 3 and confidence >= 18:
                    matched_name = match_known_player_name(raw_text, known_players)
                    if matched_name:
                        existing = name_positions.get(matched_name)
                        if not existing or confidence > float(existing["confidence"]):
                            name_positions[matched_name] = {
                                "left": left,
                                "top": top,
                                "width": width,
                                "height": height,
                                "confidence": confidence,
                            }

            numeric = parse_numeric_token(raw_text)
            if numeric is None:
                continue
            score_value, digit_len = numeric
            if score_value > 99:
                continue
            if digit_len == 1 and confidence < 55:
                continue
            if digit_len >= 2 and confidence < 40:
                continue
            numeric_candidates.append(
                {
                    "score": score_value,
                    "digits": digit_len,
                    "left": left,
                    "top": top,
                    "confidence": confidence,
                }
            )

        if not name_positions:
            return []

        sorted_names = sorted(
            name_positions.items(),
            key=lambda row: (int(row[1]["top"]), int(row[1]["left"])),
        )
        used_candidate_indexes: set[int] = set()
        extracted: list[ExtractedScore] = []
        max_horizontal_gap = max(120, int(image_width * 0.62))

        for player_name, position in sorted_names:
            name_left = int(position["left"])
            name_top = int(position["top"])
            name_width = int(position["width"])
            name_height = int(position["height"])
            max_vertical_gap = max(24, int(name_height * 2.2))
            best_candidate_idx: int | None = None
            best_rank: tuple[int, int, int, float] | None = None
            for index, candidate in enumerate(numeric_candidates):
                if index in used_candidate_indexes:
                    continue
                score_x = int(candidate["left"])
                score_y = int(candidate["top"])
                horizontal_gap = score_x - name_left
                if horizontal_gap < max(10, int(name_width * 0.25)):
                    continue
                if horizontal_gap > max_horizontal_gap:
                    continue
                vertical_gap = abs(score_y - name_top)
                if vertical_gap > max_vertical_gap:
                    continue
                rank = (
                    0 if int(candidate["digits"]) >= 2 else 1,
                    vertical_gap,
                    abs(horizontal_gap - int(image_width * 0.18)),
                    -float(candidate["confidence"]),
                )
                if best_rank is None or rank < best_rank:
                    best_rank = rank
                    best_candidate_idx = index

            if best_candidate_idx is None:
                extracted.append(
                    ExtractedScore(
                        player_name=player_name,
                        score=None,
                        score_status="unknown",
                    )
                )
                continue

            used_candidate_indexes.add(best_candidate_idx)
            extracted.append(
                ExtractedScore(
                    player_name=player_name,
                    score=int(numeric_candidates[best_candidate_idx]["score"]),
                    score_status="numeric",
                )
            )

        return extracted

    try:
        with Image.open(image_path) as image:
            rgb_image = image.convert("RGB")
            width, height = rgb_image.size
            full_gray = ImageOps.grayscale(rgb_image)
            full_cont2 = ImageEnhance.Contrast(full_gray).enhance(2.0)
            full_cont3 = ImageEnhance.Contrast(full_gray).enhance(3.0)
            full_thr = full_cont3.point(lambda value: 255 if value > 145 else 0)

            right_panel = rgb_image.crop(
                (
                    int(width * 0.32),
                    int(height * 0.06),
                    int(width * 0.98),
                    int(height * 0.90),
                )
            )
            panel_gray = ImageOps.grayscale(right_panel)
            panel_cont2 = ImageEnhance.Contrast(panel_gray).enhance(2.0)
            panel_cont3 = ImageEnhance.Contrast(panel_gray).enhance(3.0)
            panel_thr = panel_cont3.point(lambda value: 255 if value > 145 else 0)

            upscaled_full = rgb_image.resize(
                (int(width * 2), int(height * 2)),
                Image.Resampling.LANCZOS,
            )
            up_full_gray = ImageOps.grayscale(upscaled_full)
            up_full_cont2 = ImageEnhance.Contrast(up_full_gray).enhance(2.0)

            upscaled_panel = right_panel.resize(
                (int(right_panel.width * 3), int(right_panel.height * 3)),
                Image.Resampling.LANCZOS,
            )
            up_panel_gray = ImageOps.grayscale(upscaled_panel)
            up_panel_cont2 = ImageEnhance.Contrast(up_panel_gray).enhance(2.0)

            variants: list[tuple[Image.Image, str]] = [
                (full_gray, "--psm 6"),
                (full_cont2, "--psm 6"),
                (full_cont3, "--psm 6"),
                (full_thr, "--psm 6"),
                (panel_gray, "--psm 6"),
                (panel_cont2, "--psm 6"),
                (panel_cont3, "--psm 6"),
                (panel_thr, "--psm 6"),
                (up_full_gray, "--psm 6"),
                (up_full_cont2, "--psm 6"),
                (up_panel_gray, "--psm 6"),
                (up_panel_cont2, "--psm 6"),
                (up_panel_cont2, "--psm 11"),
            ]
    except Exception:
        return []

    best_scores: list[ExtractedScore] = []
    for variant_image, psm_config in variants:
        data = pytesseract.image_to_data(
            variant_image,
            config=psm_config,
            output_type=pytesseract.Output.DICT,
        )
        token_rows: list[dict[str, object]] = []
        token_count = len(data.get("text", []))
        for index in range(token_count):
            raw_text = (data["text"][index] or "").strip()
            if not raw_text:
                continue
            conf_raw = data["conf"][index]
            try:
                confidence = float(conf_raw)
            except Exception:
                confidence = -1.0
            token_rows.append(
                {
                    "raw_text": raw_text,
                    "left": int(data["left"][index]),
                    "top": int(data["top"][index]),
                    "width": int(data["width"][index]),
                    "height": int(data["height"][index]),
                    "confidence": confidence,
                }
            )

        candidate = extract_from_token_rows(token_rows, variant_image.width)
        if extracted_scores_quality(candidate) > extracted_scores_quality(best_scores):
            best_scores = candidate

    return best_scores


def extracted_scores_numeric_count(scores: list[ExtractedScore]) -> int:
    return sum(
        1
        for row in scores
        if row.score_status == "numeric" and row.score is not None
    )


def extracted_scores_quality(scores: list[ExtractedScore]) -> tuple[int, int]:
    return (len(scores), extracted_scores_numeric_count(scores))


def best_extracted_scores_for_image(
    conn: sqlite3.Connection,
    image_path: Path,
    initial_scores: list[ExtractedScore],
) -> list[ExtractedScore]:
    best_scores = initial_scores
    known_players = known_player_names(conn)
    for candidate in (
        extract_scores_from_bottom_band(image_path, known_players),
        extract_scores_from_ranked_rows(image_path, known_players),
    ):
        if extracted_scores_quality(candidate) > extracted_scores_quality(best_scores):
            best_scores = candidate
    return best_scores


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

    numeric_scores = [
        row
        for row in extracted_scores
        if row.score_status == "numeric" and row.score is not None
    ]

    if len(numeric_scores) >= 2:
        numeric_signal = min(len(numeric_scores), 6)
    else:
        numeric_signal = 0

    relevance = numeric_signal * 0.9 + keyword_hits * 0.6

    if "victory" in text or "defeat" in text:
        relevance += 0.1

    return relevance


def count_numeric_scores(raw_scores: list[dict[str, object]]) -> int:
    total = 0
    for row in raw_scores:
        if row.get("score") is None:
            continue
        status = str(row.get("score_status") or "numeric").lower()
        if status == "numeric":
            total += 1
    return total


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
    extracted_scores = best_extracted_scores_for_image(conn, image_path, extracted_scores)
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


def best_root_faction_assignment(
    image_path: Path,
    player_count: int,
    faction_samples_map: dict[str, list[tuple[int, int, int]]],
) -> tuple[list[tuple[str, float]], list[tuple[int, int, int]], float] | None:
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
        return None
    return best_assignment, best_slot_colors, best_confidence


def preview_root_factions_from_scores(
    conn: sqlite3.Connection,
    image_path: Path,
    scores: list[ExtractedScore],
    higher_is_better: bool = True,
) -> tuple[list[dict[str, object]], float] | None:
    if not image_path.exists():
        return None
    if len(scores) < 2:
        return None

    cleaned_scores = [
        ExtractedScore(
            player_name=canonicalize_player_name(item.player_name),
            score=item.score,
            score_status=item.score_status or ("numeric" if item.score is not None else "unknown"),
        )
        for item in scores
        if canonicalize_player_name(item.player_name)
    ]
    if len(cleaned_scores) < 2:
        return None

    placements = assign_placements(cleaned_scores, higher_is_better)
    indexed_scores = list(enumerate(cleaned_scores))
    indexed_scores.sort(
        key=lambda row: (
            placements[row[0]],
            -row[1].score if row[1].score is not None else 999999,
            row[0],
        )
    )

    player_order = [row[1].player_name for row in indexed_scores]
    player_count = len(player_order)
    faction_samples_map = root_faction_samples(conn)
    if len(faction_samples_map) < player_count:
        return None

    assigned = best_root_faction_assignment(image_path, player_count, faction_samples_map)
    if assigned is None:
        return None

    assignment_rows, _, global_confidence = assigned
    preview_rows = [
        {
            "player_name": player_name,
            "faction_name": faction_name,
            "confidence": confidence,
        }
        for player_name, (faction_name, confidence) in zip(player_order, assignment_rows)
    ]
    return preview_rows, global_confidence


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

    best = best_root_faction_assignment(image_path, player_count, faction_samples_map)
    if best is None:
        return 0
    best_assignment, best_slot_colors, _ = best

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


def infer_supported_game_name(
    conn: sqlite3.Connection,
    image_path: Path,
    ocr_text: str,
    scores: list[ExtractedScore],
) -> str:
    parent_name = image_path.parent.name.strip().lower()
    if parent_name in SUPPORTED_GAMES:
        return parent_name

    text = (ocr_text or "").lower()
    text_has_dune_markers = any(
        token in text
        for token in (
            "atreides",
            "harkonnen",
            "fremen",
            "bene gesserit",
            "spice",
            "view board",
            "continue",
        )
    )
    text_has_everdell_markers = any(
        token in text
        for token in (
            "everdell",
            "main menu",
            "berry",
            "berries",
            "twig",
            "twigs",
            "resin",
            "pebble",
            "pebbles",
        )
    )
    text_has_ordinal_rows = bool(
        re.search(r"\b(1st|ist|2nd|2n0d|3rd|4th)\b", text)
    )

    numeric_scores = [
        row.score
        for row in scores
        if row.score_status == "numeric" and row.score is not None
    ]

    image_width = 0
    if image_path.exists():
        try:
            with Image.open(image_path) as image:
                image_width = image.size[0]
        except Exception:
            image_width = 0

    if module_enabled("root") and len(scores) >= 3 and image_path.exists():
        try:
            faction_samples_map = root_faction_samples(conn)
            assignment = best_root_faction_assignment(
                image_path,
                len(scores),
                faction_samples_map,
            )
        except Exception:
            assignment = None
        if assignment is not None and assignment[2] >= 0.20:
            return "root"

    if text_has_dune_markers:
        return "dune"
    if text_has_everdell_markers:
        return "everdell"

    if text_has_ordinal_rows:
        if numeric_scores:
            if max(numeric_scores) <= 22:
                return "dune"
            if max(numeric_scores) >= 30:
                return "everdell"
        if 0 < image_width <= 900:
            return "dune"
        return "everdell"

    if 0 < image_width <= 900 and len(scores) >= 3:
        return "dune"

    if numeric_scores and len(scores) >= 3 and max(numeric_scores) <= 40 and image_width >= 1000:
        return "root"

    return ""


def parse_scores_text(raw_scores: str) -> list[ExtractedScore]:
    parsed: list[ExtractedScore] = []

    for line in raw_scores.splitlines():
        line = line.strip()
        if not line:
            continue

        maybe_name = ""
        maybe_score = ""

        pipe_parts = [part.strip() for part in line.split("|")]
        if len(pipe_parts) >= 3 and pipe_parts[0].isdigit():
            maybe_name = pipe_parts[1]
            maybe_score = pipe_parts[2]
        else:
            comma_parts = [part.strip() for part in line.split(",")]
            if len(comma_parts) >= 3 and comma_parts[0].isdigit():
                maybe_name = comma_parts[1]
                maybe_score = comma_parts[2]
            elif "," in line:
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

        placement_prefix = re.match(r"^\s*\d+\s*[\.\)\-:]\s*(.+)$", maybe_name)
        if placement_prefix:
            maybe_name = placement_prefix.group(1).strip()

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
        ORDER BY p.name ASC, win_rate DESC, wins DESC, games_played DESC, faction_name ASC
        """
    ).fetchall()


def group_root_player_faction_rows(rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        player_name = str(row["player_name"])
        grouped.setdefault(player_name, []).append(row)
    ordered_player_names = sorted(grouped.keys(), key=lambda name: name.casefold())
    return [
        {"player_name": player_name, "rows": grouped[player_name]}
        for player_name in ordered_player_names
    ]


def group_per_game_rows(rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        game_name = str(row["game_name"])
        grouped.setdefault(game_name, []).append(row)
    return [{"game_name": game_name, "rows": game_rows} for game_name, game_rows in grouped.items()]


def per_game_rows_map(rows: list[sqlite3.Row]) -> dict[str, list[sqlite3.Row]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        game_name = str(row["game_name"]).strip().lower()
        grouped.setdefault(game_name, []).append(row)
    return grouped


def save_uploaded_screenshot(file_storage) -> Path:
    original_name = secure_filename(file_storage.filename or "")
    suffix = Path(original_name).suffix.lower()
    if suffix not in IMAGE_EXTENSIONS:
        raise ValueError("Unsupported file type.")

    uploads_dir = BASE_DIR / "screenshots" / "_uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{timestamp}_{uuid4().hex[:10]}{suffix}"
    destination = uploads_dir / filename
    file_storage.save(destination)
    return destination


def publish_live_stats(commit_message: str | None = None) -> tuple[bool, str]:
    try:
        import build_static_site

        built_path = build_static_site.build_static_homepage()
    except Exception as error:
        return False, f"Result saved, but static build failed: {error}"

    default_message = f"Refresh leaderboard static page ({datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')})"
    message = (commit_message or default_message).strip()
    if not message:
        message = default_message

    try:
        subprocess.run(
            ["git", "add", "docs/index.html"],
            cwd=BASE_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        staged_check = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )
        if staged_check.returncode == 0:
            return True, f"Result saved. Live page already up to date ({built_path})."
        if staged_check.returncode not in (0, 1):
            return False, f"Result saved, but publish staging check failed: {staged_check.stderr.strip()}"

        subprocess.run(
            ["git", "commit", "-m", message],
            cwd=BASE_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ["git", "push", "origin", "main"],
            cwd=BASE_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        error_text = (error.stderr or error.stdout or str(error)).strip()
        return False, f"Result saved, but publish failed: {error_text}"

    return True, f"Result saved and live stats published ({built_path})."


def load_root_module_context(conn: sqlite3.Connection) -> dict[str, object]:
    if not module_enabled("root"):
        return {
            "root_module_enabled": False,
            "root_faction_share": [],
            "root_faction_win_rates": [],
            "root_faction_matchup_factions": [],
            "root_faction_matchup_matrix": [],
            "root_player_faction_groups": [],
            "root_faction_palette": {},
        }

    load_root_faction_samples(conn)
    backfill_root_factions(conn)
    root_faction_share = root_faction_share_rows(conn)
    root_faction_win_rates = root_faction_win_rate_rows(conn)
    root_faction_matchups = root_faction_matchup_rows(conn)
    root_faction_matchup_factions, root_faction_matchup_matrix = build_root_faction_matchup_matrix(
        root_faction_matchups
    )
    root_player_faction_rows_data = root_player_faction_rows(conn)
    root_player_faction_groups = group_root_player_faction_rows(root_player_faction_rows_data)
    root_faction_palette_map = root_faction_palette(conn)

    return {
        "root_module_enabled": True,
        "root_faction_share": root_faction_share,
        "root_faction_win_rates": root_faction_win_rates,
        "root_faction_matchup_factions": root_faction_matchup_factions,
        "root_faction_matchup_matrix": root_faction_matchup_matrix,
        "root_player_faction_groups": root_player_faction_groups,
        "root_faction_palette": root_faction_palette_map,
    }


@app.route("/")
def home():
    flash_messages = get_flashed_messages(with_categories=True)
    with db_conn() as conn:
        leaderboard = leaderboard_rows(conn)
        per_game = per_game_win_rates(conn)
        per_game_map = per_game_rows_map(per_game)
        root_module_context = load_root_module_context(conn)

    return render_template(
        "home.html",
        leaderboard=leaderboard,
        per_game=per_game,
        per_game_map=per_game_map,
        flash_messages=flash_messages,
        show_admin_controls=upload_review_enabled(),
        **root_module_context,
    )


@app.route("/admin/upload", methods=["GET", "POST"])
def admin_upload():
    if not upload_review_enabled():
        flash("Upload/review flow is currently disabled.", "error")
        return redirect(url_for("home"))

    if request.method == "POST":
        uploaded_file = request.files.get("screenshot")
        if uploaded_file is None or not uploaded_file.filename:
            flash("Choose a screenshot file to upload.", "error")
            return redirect(url_for("admin_upload"))

        try:
            saved_path = save_uploaded_screenshot(uploaded_file)
        except ValueError as error:
            flash(str(error), "error")
            return redirect(url_for("admin_upload"))
        except Exception as error:
            flash(f"Upload failed: {error}", "error")
            return redirect(url_for("admin_upload"))

        with db_conn() as conn:
            upsert_match_from_image(conn, saved_path)
            match_row = conn.execute(
                "SELECT id FROM matches WHERE screenshot_path = ?",
                (str(saved_path),),
            ).fetchone()

        if not match_row:
            flash("Screenshot uploaded but could not create a review entry.", "error")
            return redirect(url_for("admin_upload"))

        flash("Screenshot uploaded. Review and confirm details.", "success")
        return redirect(url_for("review", match_id=int(match_row["id"])))

    with db_conn() as conn:
        recent_matches = conn.execute(
            """
            SELECT
                m.id,
                m.created_at,
                m.reviewed,
                m.relevance_score,
                m.screenshot_path,
                g.name AS game_name
            FROM matches m
            LEFT JOIN games g ON g.id = m.game_id
            ORDER BY m.id DESC
            LIMIT 20
            """
        ).fetchall()

    return render_template("upload.html", recent_matches=recent_matches)


@app.route("/scan", methods=["POST"])
def scan():
    if not upload_review_enabled():
        flash("Upload/review flow is currently disabled.", "error")
        return redirect(url_for("home"))

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
    if not upload_review_enabled():
        flash("Upload/review flow is currently disabled.", "error")
        return redirect(url_for("home"))

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
            if game_name.lower() in SUPPORTED_GAMES:
                game_name = game_name.lower()
            higher_is_better = request.form.get("higher_is_better", "1") == "1"
            publish_after_save = action == "save_publish"
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
            if module_enabled("root") and game_name.strip().lower() == "root":
                load_root_faction_samples(conn)
                infer_root_factions_for_match(conn, match_id)

            if publish_after_save:
                conn.commit()
                publish_ok, publish_message = publish_live_stats()
                flash(publish_message, "success" if publish_ok else "error")
            else:
                flash("Result saved.", "success")
            return redirect(url_for("home"))

        extracted_scores = json.loads(match["extracted_scores_json"] or "[]")
        existing_numeric = count_numeric_scores(extracted_scores)
        extracted_rows_for_compare = [
            ExtractedScore(
                player_name=canonicalize_player_name(str(row.get("player_name", "")).strip()),
                score=row.get("score"),
                score_status=str(row.get("score_status") or ("numeric" if row.get("score") is not None else "unknown")),
            )
            for row in extracted_scores
            if str(row.get("player_name", "")).strip()
        ]
        if len(extracted_scores) < 2 or existing_numeric < len(extracted_scores):
            fallback_scores = best_extracted_scores_for_image(
                conn,
                Path(match["screenshot_path"]),
                extracted_rows_for_compare,
            )
            fallback_numeric = extracted_scores_numeric_count(fallback_scores)
            if (
                len(fallback_scores) > len(extracted_scores)
                or fallback_numeric > existing_numeric
            ):
                extracted_scores = [row._asdict() for row in fallback_scores]
                conn.execute(
                    "UPDATE matches SET extracted_scores_json = ?, relevance_score = ? WHERE id = ?",
                    (
                        json.dumps(extracted_scores),
                        calc_relevance(match["ocr_text"] or "", fallback_scores),
                        match_id,
                    ),
                )
        existing_game = conn.execute(
            """
            SELECT g.name, g.higher_is_better
            FROM games g
            JOIN matches m ON m.game_id = g.id
            WHERE m.id = ?
            """,
            (match_id,),
        ).fetchone()
        screenshot_path = Path(match["screenshot_path"])
        extracted_score_rows = [
            ExtractedScore(
                player_name=canonicalize_player_name(str(row.get("player_name", "")).strip()),
                score=row.get("score"),
                score_status=str(row.get("score_status") or ("numeric" if row.get("score") is not None else "unknown")),
            )
            for row in extracted_scores
            if str(row.get("player_name", "")).strip()
        ]
        if existing_game:
            default_game_name = str(existing_game["name"])
            default_higher_is_better = bool(existing_game["higher_is_better"])
        else:
            default_game_name = infer_supported_game_name(
                conn,
                screenshot_path,
                str(match["ocr_text"] or ""),
                extracted_score_rows,
            )
            default_higher_is_better = True

        root_faction_preview: list[dict[str, object]] = []
        root_faction_preview_confidence: float | None = None
        if module_enabled("root") and extracted_score_rows:
            load_root_faction_samples(conn)
            preview = preview_root_factions_from_scores(
                conn,
                Path(match["screenshot_path"]),
                extracted_score_rows,
                default_higher_is_better,
            )
            if preview is not None:
                preview_rows, preview_confidence = preview
                if default_game_name.strip().lower() == "root" or preview_confidence >= 0.30:
                    root_faction_preview = preview_rows
                    root_faction_preview_confidence = preview_confidence
                    if not default_game_name.strip():
                        default_game_name = "root"
        scores_text = "\n".join(
            f"{index}|{canonicalize_player_name(row['player_name'])}|{row['score'] if row['score'] is not None else 'NA'}"
            for index, row in enumerate(extracted_scores, start=1)
        )

        return render_template(
            "review.html",
            match=match,
            scores_text=scores_text,
            ocr_available=pytesseract is not None,
            default_game_name=default_game_name,
            default_higher_is_better=default_higher_is_better,
            root_faction_preview=root_faction_preview,
            root_faction_preview_confidence=root_faction_preview_confidence,
            supported_games=SUPPORTED_GAMES,
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
