"""Database schema and utilities for the photo recovery pipeline."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .config import DB_PATH


SCHEMA = """
-- Core photo data (Stage 1)
CREATE TABLE IF NOT EXISTS photos (
    id TEXT PRIMARY KEY,           -- SHA256 hash
    mime_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    width INTEGER,
    height INTEGER,
    date_taken DATETIME,
    date_source TEXT,              -- 'exif', 'filename', 'mtime'
    has_exif BOOLEAN DEFAULT 0,    -- Has any EXIF data
    perceptual_hash TEXT,          -- pHash, computed in Stage 3
    dhash TEXT,                    -- dHash, computed in Stage 3
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- All source paths for each photo (Stage 1)
-- Preserves path info from exact duplicates
CREATE TABLE IF NOT EXISTS photo_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_id TEXT NOT NULL REFERENCES photos(id),
    source_path TEXT NOT NULL,
    filename TEXT NOT NULL
);

-- Individual decisions (Stage 2)
CREATE TABLE IF NOT EXISTS individual_decisions (
    photo_id TEXT PRIMARY KEY REFERENCES photos(id),
    decision TEXT NOT NULL,        -- 'reject' or 'separate'
    rule_name TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Duplicate groups (Stage 4)
CREATE TABLE IF NOT EXISTS duplicate_groups (
    photo_id TEXT PRIMARY KEY REFERENCES photos(id),
    group_id INTEGER NOT NULL
);

-- Group rejections (Stage 5)
CREATE TABLE IF NOT EXISTS group_rejections (
    photo_id TEXT PRIMARY KEY REFERENCES photos(id),
    group_id INTEGER NOT NULL,
    rule_name TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated paths from rejected duplicates
CREATE TABLE IF NOT EXISTS aggregated_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kept_photo_id TEXT NOT NULL REFERENCES photos(id),
    source_path TEXT NOT NULL,     -- Path from a rejected duplicate
    from_photo_id TEXT NOT NULL    -- Which rejected photo this came from
);

-- Pipeline state tracking
CREATE TABLE IF NOT EXISTS pipeline_state (
    stage TEXT PRIMARY KEY,
    completed_at DATETIME,
    photo_count INTEGER,
    notes TEXT
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_photos_perceptual_hash ON photos(perceptual_hash);
CREATE INDEX IF NOT EXISTS idx_photo_paths_photo_id ON photo_paths(photo_id);
CREATE INDEX IF NOT EXISTS idx_individual_decisions_decision ON individual_decisions(decision);
CREATE INDEX IF NOT EXISTS idx_duplicate_groups_group_id ON duplicate_groups(group_id);
CREATE INDEX IF NOT EXISTS idx_group_rejections_group_id ON group_rejections(group_id);
CREATE INDEX IF NOT EXISTS idx_aggregated_paths_kept_photo_id ON aggregated_paths(kept_photo_id);
"""


def init_db(db_path: Path = DB_PATH) -> None:
    """Initialize the database with the schema."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    # Migration: add dhash column if missing
    try:
        conn.execute("ALTER TABLE photos ADD COLUMN dhash TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists

    conn.commit()
    conn.close()


@contextmanager
def get_connection(db_path: Path = DB_PATH) -> Iterator[sqlite3.Connection]:
    """Get a database connection with row factory enabled."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def record_stage_completion(
    conn: sqlite3.Connection,
    stage: str,
    photo_count: int,
    notes: str = None
) -> None:
    """Record that a pipeline stage has completed."""
    conn.execute("""
        INSERT OR REPLACE INTO pipeline_state (stage, completed_at, photo_count, notes)
        VALUES (?, ?, ?, ?)
    """, (stage, datetime.now().isoformat(), photo_count, notes))
    conn.commit()


def get_stage_status(conn: sqlite3.Connection, stage: str) -> dict | None:
    """Get the status of a pipeline stage."""
    cursor = conn.execute(
        "SELECT * FROM pipeline_state WHERE stage = ?",
        (stage,)
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def clear_stage_data(conn: sqlite3.Connection, stage: str) -> None:
    """Clear data from a specific stage for re-running."""
    if stage == "1":
        conn.execute("DELETE FROM photo_paths")
        conn.execute("DELETE FROM photos")
    elif stage == "2":
        conn.execute("DELETE FROM individual_decisions")
    elif stage == "3":
        conn.execute("UPDATE photos SET perceptual_hash = NULL")
    elif stage == "4":
        conn.execute("DELETE FROM duplicate_groups")
    elif stage == "5":
        conn.execute("DELETE FROM group_rejections")
        conn.execute("DELETE FROM aggregated_paths")

    conn.execute("DELETE FROM pipeline_state WHERE stage = ?", (stage,))
    conn.commit()


def get_photo_count(conn: sqlite3.Connection) -> int:
    """Get total number of photos in the database."""
    cursor = conn.execute("SELECT COUNT(*) FROM photos")
    return cursor.fetchone()[0]


def get_photos_without_decision(conn: sqlite3.Connection) -> list[dict]:
    """Get photos that haven't been classified in Stage 2."""
    cursor = conn.execute("""
        SELECT p.*, GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM photos p
        LEFT JOIN individual_decisions d ON p.id = d.photo_id
        LEFT JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE d.photo_id IS NULL
        GROUP BY p.id
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_photos_for_phash(conn: sqlite3.Connection) -> list[dict]:
    """Get photos that need hashing (not rejected/separated, missing pHash or dHash)."""
    cursor = conn.execute("""
        SELECT p.id, pp.source_path
        FROM photos p
        JOIN photo_paths pp ON p.id = pp.photo_id
        LEFT JOIN individual_decisions d ON p.id = d.photo_id
        WHERE (p.perceptual_hash IS NULL OR p.dhash IS NULL)
        AND d.photo_id IS NULL
        GROUP BY p.id
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_photos_for_grouping(conn: sqlite3.Connection) -> list[dict]:
    """Get photos with perceptual hashes for duplicate grouping."""
    cursor = conn.execute("""
        SELECT p.id, p.perceptual_hash, p.width, p.height, p.file_size, p.has_exif
        FROM photos p
        LEFT JOIN individual_decisions d ON p.id = d.photo_id
        WHERE p.perceptual_hash IS NOT NULL
        AND d.photo_id IS NULL
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_group_members(conn: sqlite3.Connection, group_id: int) -> list[dict]:
    """Get all members of a duplicate group with their photo data."""
    cursor = conn.execute("""
        SELECT
            p.*,
            dg.group_id,
            GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE dg.group_id = ?
        GROUP BY p.id
    """, (group_id,))
    return [dict(row) for row in cursor.fetchall()]


def get_all_group_ids(conn: sqlite3.Connection) -> list[int]:
    """Get all unique group IDs."""
    cursor = conn.execute("SELECT DISTINCT group_id FROM duplicate_groups ORDER BY group_id")
    return [row[0] for row in cursor.fetchall()]


def get_accepted_photos(conn: sqlite3.Connection) -> list[dict]:
    """Get photos that are accepted (not individually rejected and not group rejected)."""
    cursor = conn.execute("""
        SELECT
            p.*,
            GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM photos p
        JOIN photo_paths pp ON p.id = pp.photo_id
        LEFT JOIN individual_decisions d ON p.id = d.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        WHERE d.photo_id IS NULL
        AND gr.photo_id IS NULL
        GROUP BY p.id
    """)
    return [dict(row) for row in cursor.fetchall()]
