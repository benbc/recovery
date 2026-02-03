"""
Pipeline2 Stage 1b: Compute All Pairwise Distances

Computes distances between all pairs of kept photos and caches them.
This enables instant threshold exploration without sampling.

With 12,836 photos = 82M pairs. At ~1M pairs/sec, takes ~80 seconds.
Storage: ~1-2 GB depending on schema.
"""

from collections import defaultdict

from tqdm import tqdm

from pipeline.database import get_connection, record_stage_completion
from pipeline.utils.hashing import hamming_distance


def get_kept_photos_with_all_hashes(conn) -> list[dict]:
    """
    Get kept photos that have all hashes (including extended).
    """
    cursor = conn.execute("""
        SELECT
            p.id,
            p.perceptual_hash as phash,
            p.dhash,
            eh.phash_16,
            eh.colorhash,
            dg.group_id as primary_group
        FROM photos p
        JOIN extended_hashes eh ON p.id = eh.photo_id
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        LEFT JOIN duplicate_groups dg ON p.id = dg.photo_id
        WHERE jd.photo_id IS NULL
        AND gr.photo_id IS NULL
        AND id.photo_id IS NULL
        AND p.perceptual_hash IS NOT NULL
        AND p.dhash IS NOT NULL
        AND eh.phash_16 IS NOT NULL
        AND eh.colorhash IS NOT NULL
    """)
    return [dict(row) for row in cursor.fetchall()]


def run_stage1b(clear_existing: bool = False) -> None:
    """
    Run Pipeline2 Stage 1b: Compute All Pairwise Distances.
    """
    print("=" * 70)
    print("PIPELINE2 STAGE 1B: COMPUTE ALL PAIRWISE DISTANCES")
    print("=" * 70)
    print()

    with get_connection() as conn:
        # Create table
        if clear_existing:
            conn.execute("DROP TABLE IF EXISTS photo_pairs")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS photo_pairs (
                photo_id_1 TEXT NOT NULL,
                photo_id_2 TEXT NOT NULL,
                same_primary_group INTEGER NOT NULL,
                phash_dist INTEGER NOT NULL,
                dhash_dist INTEGER NOT NULL,
                phash16_dist INTEGER NOT NULL,
                colorhash_dist INTEGER NOT NULL,
                PRIMARY KEY (photo_id_1, photo_id_2)
            )
        """)

        # Create indexes for fast threshold queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_phash ON photo_pairs(phash_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_dhash ON photo_pairs(dhash_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_phash16 ON photo_pairs(phash16_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_colorhash ON photo_pairs(colorhash_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_same_group ON photo_pairs(same_primary_group)")
        conn.commit()

        # Check existing count
        existing = conn.execute("SELECT COUNT(*) FROM photo_pairs").fetchone()[0]
        if existing > 0 and not clear_existing:
            print(f"Table already has {existing:,} pairs. Use --clear to recompute.")
            return

        # Get photos
        print("Loading kept photos with all hashes...")
        photos = get_kept_photos_with_all_hashes(conn)
        print(f"Found {len(photos):,} photos")

        total_pairs = len(photos) * (len(photos) - 1) // 2
        print(f"Computing {total_pairs:,} pairs...")
        print()

        # Compute all pairs
        batch = []
        batch_size = 10000
        pair_count = 0
        same_group_count = 0

        for i in tqdm(range(len(photos)), desc="Computing pairs"):
            for j in range(i + 1, len(photos)):
                p1 = photos[i]
                p2 = photos[j]

                # Ensure consistent ordering (smaller id first)
                if p1["id"] > p2["id"]:
                    p1, p2 = p2, p1

                # Check if same primary group
                same_group = (
                    p1["primary_group"] is not None
                    and p1["primary_group"] == p2["primary_group"]
                )
                if same_group:
                    same_group_count += 1

                # Compute all distances
                batch.append({
                    "photo_id_1": p1["id"],
                    "photo_id_2": p2["id"],
                    "same_primary_group": 1 if same_group else 0,
                    "phash_dist": hamming_distance(p1["phash"], p2["phash"]),
                    "dhash_dist": hamming_distance(p1["dhash"], p2["dhash"]),
                    "phash16_dist": hamming_distance(p1["phash_16"], p2["phash_16"]),
                    "colorhash_dist": hamming_distance(p1["colorhash"], p2["colorhash"]),
                })
                pair_count += 1

                # Insert in batches
                if len(batch) >= batch_size:
                    conn.executemany("""
                        INSERT OR REPLACE INTO photo_pairs
                        (photo_id_1, photo_id_2, same_primary_group,
                         phash_dist, dhash_dist, phash16_dist, colorhash_dist)
                        VALUES (:photo_id_1, :photo_id_2, :same_primary_group,
                                :phash_dist, :dhash_dist, :phash16_dist, :colorhash_dist)
                    """, batch)
                    conn.commit()
                    batch = []

        # Insert remaining
        if batch:
            conn.executemany("""
                INSERT OR REPLACE INTO photo_pairs
                (photo_id_1, photo_id_2, same_primary_group,
                 phash_dist, dhash_dist, phash16_dist, colorhash_dist)
                VALUES (:photo_id_1, :photo_id_2, :same_primary_group,
                        :phash_dist, :dhash_dist, :phash16_dist, :colorhash_dist)
            """, batch)
            conn.commit()

        # Record completion
        record_stage_completion(
            conn, "p2_1b",
            pair_count,
            f"same_group={same_group_count}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("PIPELINE2 STAGE 1B COMPLETE")
    print("=" * 70)
    print()
    print(f"Total pairs computed: {pair_count:,}")
    print(f"Same-group pairs:     {same_group_count:,}")
    print(f"Cross-group pairs:    {pair_count - same_group_count:,}")
    print()

    # Show distance distribution sample
    with get_connection() as conn:
        print("Distance distributions (cross-group pairs only):")
        for hash_type in ["phash", "dhash", "phash16", "colorhash"]:
            cursor = conn.execute(f"""
                SELECT {hash_type}_dist as dist, COUNT(*) as cnt
                FROM photo_pairs
                WHERE same_primary_group = 0
                GROUP BY {hash_type}_dist
                ORDER BY {hash_type}_dist
                LIMIT 10
            """)
            rows = cursor.fetchall()
            print(f"\n  {hash_type}:")
            for row in rows:
                print(f"    {row['dist']:3d}: {row['cnt']:,}")


if __name__ == "__main__":
    run_stage1b()
