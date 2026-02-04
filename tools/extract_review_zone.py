#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = []
# ///
"""
Extract review zone pairs into a separate table for fast querying.

Run this once (or when thresholds change) before using group_merger.
"""

import json
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"
THRESHOLDS_FILE = Path(__file__).parent.parent / "output" / "threshold_boundaries.json"


def load_thresholds():
    """Load threshold boundaries from file."""
    if not THRESHOLDS_FILE.exists():
        return None, None

    with open(THRESHOLDS_FILE) as f:
        thresholds = json.load(f)

    return thresholds.get("complete", []), thresholds.get("single", [])


def build_threshold_limits(boundary_cells: list[str]) -> dict[int, int]:
    """Build max phash16 for each colorhash from boundary cells."""
    if not boundary_cells:
        return {}

    max_p_by_c = {}
    for cell in boundary_cells:
        p, c = map(int, cell.split(","))
        if c not in max_p_by_c or p > max_p_by_c[c]:
            max_p_by_c[c] = p

    return max_p_by_c


def build_review_zone_ranges(relaxed: dict[int, int], strict: dict[int, int]) -> list[tuple[int, int, int]]:
    """Build list of (colorhash, min_phash16, max_phash16) ranges for the review zone."""
    ranges = []
    for colorhash, relaxed_max in relaxed.items():
        strict_max = strict.get(colorhash, -1)

        if strict_max < 0:
            ranges.append((colorhash, 0, relaxed_max))
        elif relaxed_max > strict_max:
            ranges.append((colorhash, strict_max + 1, relaxed_max))

    return ranges


def main():
    print(f"Database: {DB_PATH}")
    print(f"Thresholds: {THRESHOLDS_FILE}")
    print()

    # Load thresholds
    relaxed_boundary, strict_boundary = load_thresholds()
    if not relaxed_boundary or not strict_boundary:
        print("ERROR: No threshold boundaries found")
        return 1

    relaxed_limits = build_threshold_limits(relaxed_boundary)
    strict_limits = build_threshold_limits(strict_boundary)
    ranges = build_review_zone_ranges(relaxed_limits, strict_limits)

    print(f"Review zone: {len(ranges)} colorhash ranges")
    for ch, min_p, max_p in ranges:
        print(f"  colorhash={ch}: phash16 {min_p}-{max_p}")
    print()

    conn = sqlite3.connect(DB_PATH)

    # Create table
    print("Creating review_zone_pairs table...")
    conn.execute("DROP TABLE IF EXISTS review_zone_pairs")
    conn.execute("""
        CREATE TABLE review_zone_pairs (
            photo_id_1 TEXT NOT NULL,
            photo_id_2 TEXT NOT NULL,
            phash16_dist INTEGER NOT NULL,
            colorhash_dist INTEGER NOT NULL,
            PRIMARY KEY (photo_id_1, photo_id_2)
        )
    """)

    # Extract pairs from each range
    total_rows = 0
    t_start = time.time()

    for colorhash, min_phash, max_phash in ranges:
        query = """
            INSERT INTO review_zone_pairs
            SELECT photo_id_1, photo_id_2, phash16_dist, colorhash_dist
            FROM photo_pairs
            WHERE colorhash_dist = ? AND phash16_dist >= ? AND phash16_dist <= ?
        """
        print(f"Extracting colorhash={colorhash} (phash16 {min_phash}-{max_phash})...", end=" ", flush=True)
        t0 = time.time()
        cursor = conn.execute(query, (colorhash, min_phash, max_phash))
        rows = cursor.rowcount
        conn.commit()
        elapsed = time.time() - t0
        print(f"{rows} rows in {elapsed:.1f}s")
        total_rows += rows

    # Create index for stable ordering
    print()
    print("Creating index for stable ordering...")
    conn.execute("CREATE INDEX idx_review_zone_order ON review_zone_pairs (photo_id_1, photo_id_2)")
    conn.commit()

    elapsed_total = time.time() - t_start
    print()
    print(f"Done: {total_rows} pairs extracted in {elapsed_total:.1f}s")

    # Show table size
    cursor = conn.execute("SELECT COUNT(*) FROM review_zone_pairs")
    count = cursor.fetchone()[0]
    print(f"Table review_zone_pairs has {count} rows")

    conn.close()
    return 0


if __name__ == "__main__":
    exit(main())
