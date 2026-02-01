"""
Stage 3: Perceptual Hash

Compute perceptual hashes (pHash and dHash) for photos not rejected/separated.
Skip classified photos entirely (save time on expensive operation).
Resumable: skip photos that already have hashes.
Can import valid hashes from old database.

Output: `perceptual_hash` and `dhash` columns populated
"""

import sqlite3
from pathlib import Path

from tqdm import tqdm

from .config import BATCH_SIZE, OLD_DB_PATH
from .database import (
    get_connection,
    get_photos_for_phash,
    record_stage_completion,
)
from .utils.hashing import compute_hashes


def import_hashes_from_old_db(old_db_path: Path) -> None:
    """
    Import perceptual hashes from an old database.

    The old database should have a `photos` table with `id` and `perceptual_hash`
    columns. Hashes are only imported for photos that exist in the new database
    and don't already have hashes.
    """
    print(f"Importing hashes from {old_db_path}...")

    if not old_db_path.exists():
        print(f"Error: Old database not found at {old_db_path}")
        return

    old_conn = sqlite3.connect(old_db_path)
    old_conn.row_factory = sqlite3.Row

    with get_connection() as new_conn:
        # Get photos that need hashes
        cursor = new_conn.execute("""
            SELECT id FROM photos
            WHERE perceptual_hash IS NULL
        """)
        need_hashes = {row["id"] for row in cursor.fetchall()}
        print(f"Found {len(need_hashes):,} photos needing hashes")

        # Get hashes from old database
        cursor = old_conn.execute("""
            SELECT id, perceptual_hash FROM photos
            WHERE perceptual_hash IS NOT NULL
        """)

        imported = 0
        batch = []

        for row in cursor:
            if row["id"] in need_hashes:
                batch.append({
                    "id": row["id"],
                    "perceptual_hash": row["perceptual_hash"],
                })

                if len(batch) >= BATCH_SIZE:
                    new_conn.executemany(
                        "UPDATE photos SET perceptual_hash = :perceptual_hash WHERE id = :id",
                        batch,
                    )
                    new_conn.commit()
                    imported += len(batch)
                    batch = []

        # Final batch
        if batch:
            new_conn.executemany(
                "UPDATE photos SET perceptual_hash = :perceptual_hash WHERE id = :id",
                batch,
            )
            new_conn.commit()
            imported += len(batch)

        print(f"Imported {imported:,} hashes from old database")

    old_conn.close()


def run_stage3(import_from: Path = None, clear_existing: bool = False) -> None:
    """
    Run Stage 3: Perceptual Hash.

    Args:
        import_from: Optional path to old database to import hashes from
        clear_existing: If True, clear existing hashes before running
    """
    print("=" * 70)
    print("STAGE 3: PERCEPTUAL HASH")
    print("=" * 70)
    print()

    with get_connection() as conn:
        if clear_existing:
            print("Clearing existing hashes...")
            conn.execute("UPDATE photos SET perceptual_hash = NULL, dhash = NULL")
            conn.commit()

        # Import from old database if specified
        if import_from:
            import_hashes_from_old_db(import_from)

        # Get photos that need hashing
        print("Finding photos that need hashing...")
        photos = get_photos_for_phash(conn)
        print(f"Found {len(photos):,} photos to hash")
        print()

        if not photos:
            print("No photos to hash.")
            # Record completion even if nothing to do
            total = conn.execute(
                "SELECT COUNT(*) FROM photos WHERE perceptual_hash IS NOT NULL"
            ).fetchone()[0]
            record_stage_completion(conn, "3", total, "no new hashes computed")
            return

        # Track stats
        computed = 0
        errors = 0
        batch = []

        # Compute hashes
        for photo in tqdm(photos, desc="Computing hashes"):
            source_path = Path(photo["source_path"])

            if not source_path.exists():
                errors += 1
                continue

            phash, dhash = compute_hashes(source_path)
            if phash and dhash:
                batch.append({
                    "id": photo["id"],
                    "perceptual_hash": phash,
                    "dhash": dhash,
                })
                computed += 1
            else:
                errors += 1

            # Flush batch periodically
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "UPDATE photos SET perceptual_hash = :perceptual_hash, dhash = :dhash WHERE id = :id",
                    batch,
                )
                conn.commit()
                batch = []

        # Final batch
        if batch:
            conn.executemany(
                "UPDATE photos SET perceptual_hash = :perceptual_hash, dhash = :dhash WHERE id = :id",
                batch,
            )
            conn.commit()

        # Record completion
        total = conn.execute(
            "SELECT COUNT(*) FROM photos WHERE perceptual_hash IS NOT NULL"
        ).fetchone()[0]
        record_stage_completion(
            conn, "3",
            total,
            f"computed={computed}, errors={errors}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 3 COMPLETE")
    print("=" * 70)
    print()
    print(f"Hashes computed:     {computed:,}")
    print(f"Errors:              {errors:,}")
    print(f"Total with hashes:   {total:,}")
    print()


if __name__ == "__main__":
    run_stage3()
