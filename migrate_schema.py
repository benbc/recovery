#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = []
# ///
"""
One-off migration: Clean up duplicate_groups schema.

Removes old columns (auto_resolved, is_suggested_keeper) and adds single 'rejected' column.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("organized/photos.db")


def main():
    conn = sqlite3.connect(DB_PATH)

    # Check current schema
    cursor = conn.execute("PRAGMA table_info(duplicate_groups)")
    columns = {row[1] for row in cursor.fetchall()}

    print(f"Current columns: {columns}")

    if "rejected" in columns and "auto_resolved" not in columns:
        print("Already migrated.")
        conn.close()
        return

    print("Migrating duplicate_groups schema...")

    conn.executescript("""
        -- Create new table with clean schema
        CREATE TABLE duplicate_groups_new (
            photo_id TEXT,
            group_id INTEGER,
            group_size INTEGER,
            rank_in_group INTEGER,
            quality_score INTEGER,
            width INTEGER,
            height INTEGER,
            file_size INTEGER,
            confidence_score INTEGER,
            rejected INTEGER DEFAULT 0,
            FOREIGN KEY (photo_id) REFERENCES photos(id)
        );

        -- Copy data (rejected starts as 0 for all)
        INSERT INTO duplicate_groups_new
            (photo_id, group_id, group_size, rank_in_group, quality_score,
             width, height, file_size, confidence_score, rejected)
        SELECT
            photo_id, group_id, group_size, rank_in_group, quality_score,
            width, height, file_size, confidence_score, 0
        FROM duplicate_groups;

        -- Swap tables
        DROP TABLE duplicate_groups;
        ALTER TABLE duplicate_groups_new RENAME TO duplicate_groups;

        -- Recreate index
        CREATE INDEX IF NOT EXISTS idx_duplicate_groups_group_id
            ON duplicate_groups(group_id);
    """)
    conn.commit()
    conn.close()

    print("Migration complete.")


if __name__ == "__main__":
    main()
