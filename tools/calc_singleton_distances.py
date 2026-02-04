#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "tqdm",
# ]
# ///
"""
Calculate phash and dhash distances for ungrouped (singleton) kept photos.

Creates a singleton_pairs table with distances for photos not in composite_groups.
This enables finding potential duplicates among the remaining ungrouped photos.
"""

import sqlite3
from itertools import combinations
from pathlib import Path

from tqdm import tqdm

DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"


def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate hamming distance between two hex hash strings."""
    if not hash1 or not hash2:
        return 256  # Max distance if missing

    # Convert hex to int and XOR
    try:
        h1 = int(hash1, 16)
        h2 = int(hash2, 16)
        return bin(h1 ^ h2).count('1')
    except (ValueError, TypeError):
        return 256


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    print(f"Database: {DB_PATH}")
    print()

    conn = get_connection()

    # Find singleton kept photos (not in composite_groups)
    print("Finding singleton kept photos...")
    cursor = conn.execute("""
        SELECT kp.id, p.perceptual_hash, p.dhash
        FROM kept_photos kp
        JOIN photos p ON kp.id = p.id
        LEFT JOIN composite_groups cg ON kp.id = cg.photo_id
        WHERE cg.photo_id IS NULL
    """)

    singletons = [dict(row) for row in cursor.fetchall()]
    print(f"Found {len(singletons):,} singleton photos")

    if len(singletons) < 2:
        print("Need at least 2 singletons to compute pairs.")
        return

    n_pairs = len(singletons) * (len(singletons) - 1) // 2
    print(f"Will compute {n_pairs:,} pairs")
    print()

    # Create output table
    print("Creating singleton_pairs table...")
    conn.execute("DROP TABLE IF EXISTS singleton_pairs")
    conn.execute("""
        CREATE TABLE singleton_pairs (
            photo_id_1 TEXT NOT NULL,
            photo_id_2 TEXT NOT NULL,
            phash_dist INTEGER NOT NULL,
            dhash_dist INTEGER NOT NULL,
            PRIMARY KEY (photo_id_1, photo_id_2)
        )
    """)
    conn.commit()

    # Compute distances
    print("Computing distances...")
    batch = []
    batch_size = 10000

    for p1, p2 in tqdm(combinations(singletons, 2), total=n_pairs, desc="Pairs"):
        id1, id2 = p1['id'], p2['id']
        if id1 > id2:
            id1, id2 = id2, id1

        phash_dist = hamming_distance(p1['perceptual_hash'], p2['perceptual_hash'])
        dhash_dist = hamming_distance(p1['dhash'], p2['dhash'])

        batch.append({
            'photo_id_1': id1,
            'photo_id_2': id2,
            'phash_dist': phash_dist,
            'dhash_dist': dhash_dist,
        })

        if len(batch) >= batch_size:
            conn.executemany(
                """INSERT INTO singleton_pairs
                   (photo_id_1, photo_id_2, phash_dist, dhash_dist)
                   VALUES (:photo_id_1, :photo_id_2, :phash_dist, :dhash_dist)""",
                batch
            )
            conn.commit()
            batch = []

    # Insert remaining
    if batch:
        conn.executemany(
            """INSERT INTO singleton_pairs
               (photo_id_1, photo_id_2, phash_dist, dhash_dist)
               VALUES (:photo_id_1, :photo_id_2, :phash_dist, :dhash_dist)""",
            batch
        )
        conn.commit()

    # Create indexes
    print("Creating indexes...")
    conn.execute("CREATE INDEX idx_singleton_phash ON singleton_pairs(phash_dist)")
    conn.execute("CREATE INDEX idx_singleton_dhash ON singleton_pairs(dhash_dist)")
    conn.commit()

    # Stats
    cursor = conn.execute("SELECT COUNT(*) FROM singleton_pairs")
    total = cursor.fetchone()[0]

    cursor = conn.execute("SELECT COUNT(*) FROM singleton_pairs WHERE phash_dist <= 10")
    close_phash = cursor.fetchone()[0]

    cursor = conn.execute("SELECT COUNT(*) FROM singleton_pairs WHERE dhash_dist <= 10")
    close_dhash = cursor.fetchone()[0]

    conn.close()

    print()
    print("Done!")
    print(f"Total pairs:           {total:,}")
    print(f"Close pairs (phash≤10): {close_phash:,}")
    print(f"Close pairs (dhash≤10): {close_dhash:,}")


if __name__ == '__main__':
    main()
