"""
Pipeline2 Stage 1: Compute Extended Hashes

Computes additional hashes for kept photos to enable more discriminative matching:
- phash_16: 16x16 pHash (256 bits vs default 64 bits)
- colorhash: color-aware hash (captures what grayscale hashes miss)

These are stored in a new table and used by secondary grouping to reduce false positives.
"""

from pathlib import Path

from tqdm import tqdm

from pipeline.config import FILES_DIR
from pipeline.database import get_connection, record_stage_completion
from pipeline.utils.hashing import compute_extended_hashes


# Map MIME types to extensions (same as pipeline1)
MIME_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
    "image/webp": ".webp",
    "image/heic": ".heic",
    "image/heif": ".heif",
}


def get_file_path(photo_id: str, mime_type: str) -> Path:
    """Get the path to the linked file for a photo."""
    ext = MIME_TO_EXT.get(mime_type, ".bin")
    return FILES_DIR / photo_id[:2] / f"{photo_id}{ext}"


def get_kept_photos_needing_hashes(conn) -> list[dict]:
    """
    Get kept photos that need extended hashes computed.

    Returns photos not in junk_deletions, group_rejections, or individual_decisions,
    that don't yet have extended hashes.
    """
    cursor = conn.execute("""
        SELECT p.id, p.mime_type
        FROM photos p
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        LEFT JOIN extended_hashes eh ON p.id = eh.photo_id
        WHERE jd.photo_id IS NULL
        AND gr.photo_id IS NULL
        AND id.photo_id IS NULL
        AND p.perceptual_hash IS NOT NULL
        AND eh.photo_id IS NULL
    """)
    return [dict(row) for row in cursor.fetchall()]


def run_stage1(clear_existing: bool = False) -> None:
    """
    Run Pipeline2 Stage 1: Compute Extended Hashes.

    Computes phash_16 and colorhash for all kept photos.
    """
    print("=" * 70)
    print("PIPELINE2 STAGE 1: COMPUTE EXTENDED HASHES")
    print("=" * 70)
    print()

    with get_connection() as conn:
        # Create table if needed
        conn.execute("""
            CREATE TABLE IF NOT EXISTS extended_hashes (
                photo_id TEXT PRIMARY KEY,
                phash_16 TEXT,
                colorhash TEXT
            )
        """)

        if clear_existing:
            print("Clearing existing extended hashes...")
            conn.execute("DELETE FROM extended_hashes")
            conn.commit()

        # Get photos needing hashes
        print("Finding kept photos needing extended hashes...")
        photos = get_kept_photos_needing_hashes(conn)
        print(f"Found {len(photos):,} photos to process")
        print()

        if not photos:
            print("All kept photos already have extended hashes.")
            record_stage_completion(conn, "p2_1", 0, "no new photos")
            return

        # Compute hashes
        print("Computing extended hashes...")
        success = 0
        failed = 0
        batch = []
        batch_size = 100

        for photo in tqdm(photos, desc="Hashing"):
            file_path = get_file_path(photo["id"], photo["mime_type"])

            if not file_path.exists():
                failed += 1
                continue

            hashes = compute_extended_hashes(file_path)

            if hashes["phash_16"] is None and hashes["colorhash"] is None:
                failed += 1
                continue

            batch.append({
                "photo_id": photo["id"],
                "phash_16": hashes["phash_16"],
                "colorhash": hashes["colorhash"],
            })
            success += 1

            # Insert in batches
            if len(batch) >= batch_size:
                conn.executemany(
                    """
                    INSERT INTO extended_hashes (photo_id, phash_16, colorhash)
                    VALUES (:photo_id, :phash_16, :colorhash)
                    """,
                    batch,
                )
                conn.commit()
                batch = []

        # Insert remaining
        if batch:
            conn.executemany(
                """
                INSERT INTO extended_hashes (photo_id, phash_16, colorhash)
                VALUES (:photo_id, :phash_16, :colorhash)
                """,
                batch,
            )
            conn.commit()

        # Record completion
        record_stage_completion(
            conn, "p2_1",
            success,
            f"failed={failed}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("PIPELINE2 STAGE 1 COMPLETE")
    print("=" * 70)
    print()
    print(f"Photos hashed: {success:,}")
    print(f"Failed:        {failed:,}")
    print()


if __name__ == "__main__":
    run_stage1()
