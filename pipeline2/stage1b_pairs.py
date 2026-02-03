"""
Pipeline2 Stage 1b: Compute All Pairwise Distances

Computes distances between all pairs of kept photos and caches them.
This enables instant threshold exploration without sampling.

With 12,836 photos = 82M pairs. Uses multiprocessing for speed.
Storage: ~1-2 GB depending on schema.
"""

import multiprocessing as mp
import os

from tqdm import tqdm

from pipeline.database import get_connection, record_stage_completion
from pipeline.utils.hashing import hamming_distance


def get_kept_photos_with_all_hashes(conn) -> list[dict]:
    """
    Get kept photos that have all hashes (including extended).
    """
    cursor = conn.execute("""
        SELECT
            kp.id,
            kp.perceptual_hash as phash,
            kp.dhash,
            kp.phash_16,
            kp.colorhash,
            dg.group_id as primary_group
        FROM kept_photos_with_hashes kp
        LEFT JOIN duplicate_groups dg ON kp.id = dg.photo_id
    """)
    return [dict(row) for row in cursor.fetchall()]


# Global for worker processes (set via initializer)
_photos = None
_n = None


def _init_worker(photos):
    """Initialize worker process with shared photo data."""
    global _photos, _n
    _photos = photos
    _n = len(photos)


def _pair_index_to_ij(k: int, n: int) -> tuple[int, int]:
    """
    Convert linear pair index k to (i, j) where i < j.

    For n items, pairs are ordered as:
    k=0: (0,1), k=1: (0,2), ..., k=n-2: (0,n-1),
    k=n-1: (1,2), k=n: (1,3), ...
    """
    # i is the largest value such that i*(2n-i-1)/2 <= k
    # Using quadratic formula
    i = int((2 * n - 1 - ((2 * n - 1) ** 2 - 8 * k) ** 0.5) / 2)
    # j is the remainder
    j = k - i * (2 * n - i - 1) // 2 + i + 1
    return i, j


def _compute_pairs_chunk(args: tuple) -> list[tuple]:
    """
    Compute pairs for a chunk of the linear pair index space.

    args: (start_k, end_k) - range of linear indices to compute

    Returns list of tuples:
        (id1, id2, same_group, phash_dist, dhash_dist, phash16_dist, colorhash_dist)
    """
    start_k, end_k = args
    results = []
    n = _n

    for k in range(start_k, end_k):
        i, j = _pair_index_to_ij(k, n)
        p1 = _photos[i]
        p2 = _photos[j]

        # Ensure consistent ordering (lexicographically smaller id first)
        id1, id2 = (p1[0], p2[0]) if p1[0] < p2[0] else (p2[0], p1[0])

        # Check if same primary group (index 5 is primary_group)
        same_group = 1 if (p1[5] is not None and p1[5] == p2[5]) else 0

        # Compute distances (indices: 1=phash, 2=dhash, 3=phash_16, 4=colorhash)
        results.append((
            id1,
            id2,
            same_group,
            hamming_distance(p1[1], p2[1]),  # phash
            hamming_distance(p1[2], p2[2]),  # dhash
            hamming_distance(p1[3], p2[3]),  # phash_16
            hamming_distance(p1[4], p2[4]),  # colorhash
        ))

    return results


def run_stage1b(clear_existing: bool = False) -> None:
    """
    Run Pipeline2 Stage 1b: Compute All Pairwise Distances.
    """
    print("=" * 70)
    print("PIPELINE2 STAGE 1B: COMPUTE ALL PAIRWISE DISTANCES")
    print("=" * 70)
    print()

    with get_connection() as conn:
        # Optimize for bulk inserts
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=-512000")  # 512MB cache

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
            ) WITHOUT ROWID
        """)
        conn.commit()

        # Check existing count
        existing = conn.execute("SELECT COUNT(*) FROM photo_pairs").fetchone()[0]
        if existing > 0 and not clear_existing:
            print(f"Table already has {existing:,} pairs. Use --clear to recompute.")
            return

        # Get photos
        print("Loading kept photos with all hashes...")
        photos_dicts = get_kept_photos_with_all_hashes(conn)
        print(f"Found {len(photos_dicts):,} photos")

        # Convert to tuples for efficient multiprocessing
        # (id, phash, dhash, phash_16, colorhash, primary_group)
        photos = [
            (p["id"], p["phash"], p["dhash"], p["phash_16"], p["colorhash"], p["primary_group"])
            for p in photos_dicts
        ]
        del photos_dicts

        n = len(photos)
        total_pairs = n * (n - 1) // 2
        print(f"Computing {total_pairs:,} pairs...")

        num_workers = max(1, os.cpu_count() - 2)
        print(f"Using {num_workers} workers")

        # Create chunks of roughly equal size
        # Each chunk is 10k pairs - small enough for good load balancing,
        # large enough to amortize overhead
        chunk_size = 10000
        num_chunks = (total_pairs + chunk_size - 1) // chunk_size
        chunks = []
        for i in range(num_chunks):
            start_k = i * chunk_size
            end_k = min((i + 1) * chunk_size, total_pairs)
            chunks.append((start_k, end_k))

        print(f"Split into {num_chunks:,} chunks of ~{chunk_size:,} pairs each")
        print()

        # Compute pairs in parallel
        pair_count = 0
        same_group_count = 0
        batch = []
        batch_size = 500000  # Larger batches for fewer commits
        commit_interval = 2000000  # Commit every 2M rows
        rows_since_commit = 0

        with mp.Pool(num_workers, initializer=_init_worker, initargs=(photos,)) as pool:
            for results in tqdm(
                pool.imap_unordered(_compute_pairs_chunk, chunks),
                total=num_chunks,
                desc="Computing pairs"
            ):
                # Extend batch directly (faster than per-row append)
                batch.extend(results)
                pair_count += len(results)
                same_group_count += sum(1 for r in results if r[2])
                rows_since_commit += len(results)

                if len(batch) >= batch_size:
                    conn.executemany("""
                        INSERT INTO photo_pairs
                        (photo_id_1, photo_id_2, same_primary_group,
                         phash_dist, dhash_dist, phash16_dist, colorhash_dist)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    batch = []

                    # Commit less frequently
                    if rows_since_commit >= commit_interval:
                        conn.commit()
                        rows_since_commit = 0

        # Insert remaining
        if batch:
            conn.executemany("""
                INSERT INTO photo_pairs
                (photo_id_1, photo_id_2, same_primary_group,
                 phash_dist, dhash_dist, phash16_dist, colorhash_dist)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, batch)
        conn.commit()

        # Create indexes after bulk insert (faster)
        print("\nCreating indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_phash ON photo_pairs(phash_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_dhash ON photo_pairs(dhash_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_phash16 ON photo_pairs(phash16_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_colorhash ON photo_pairs(colorhash_dist)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pairs_same_group ON photo_pairs(same_primary_group)")
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
