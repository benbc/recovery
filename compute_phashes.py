#!/usr/bin/env python3
# /// script
# dependencies = ["pillow", "imagehash", "tqdm"]
# ///
"""
Compute perceptual hashes for all photos in the database.

This script adds a perceptual hash (phash) to each photo record, which will
be used later to identify duplicate/resized versions of the same image.

Perceptual hashing creates a fingerprint that's similar for visually similar
images, even if they're different sizes or slightly modified.

Features:
- Resumable: skips photos that already have a perceptual hash
- Progress bar with ETA
- Batch commits every 1000 records for safety
- Error handling: skips corrupt images, logs errors
- Only processes photos marked as potential photos (is_non_photo = 0)
"""

import sqlite3
import sys
from pathlib import Path
from PIL import Image
import imagehash
from tqdm import tqdm

DB_PATH = Path("organized/photos.db")
OUTPUT_ROOT = Path("organized")

def compute_phash(image_path: Path) -> str | None:
    """
    Compute perceptual hash for an image.

    Returns hex string of hash, or None if image can't be opened.
    """
    try:
        with Image.open(image_path) as img:
            # Convert to RGB if needed (handles RGBA, grayscale, etc.)
            if img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')

            # Compute perceptual hash
            phash = imagehash.phash(img)
            return str(phash)
    except Exception as e:
        return None

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Add perceptual_hash column if it doesn't exist
    try:
        conn.execute("ALTER TABLE photos ADD COLUMN perceptual_hash TEXT")
        conn.commit()
        print("Added perceptual_hash column to database")
    except sqlite3.OperationalError:
        # Column already exists
        pass

    # Get photos that need perceptual hashes (only non-filtered photos)
    cursor = conn.execute("""
        SELECT id, path
        FROM photos
        WHERE perceptual_hash IS NULL
        AND is_non_photo = 0
        ORDER BY id
    """)

    photos = cursor.fetchall()

    if not photos:
        print("No photos need perceptual hashes computed.")
        print("All photos already have hashes or all are filtered as non-photos.")
        conn.close()
        return

    print(f"\n{'='*70}")
    print(f"COMPUTING PERCEPTUAL HASHES")
    print(f"{'='*70}\n")
    print(f"Photos to process: {len(photos):,}")
    print(f"Estimated time: ~{len(photos) / 9 / 60:.1f} minutes")
    print(f"(at ~9 images/second)\n")

    processed = 0
    errors = 0
    error_log = []

    # Process in batches for periodic commits
    batch_size = 1000
    batch = []

    for photo_id, path in tqdm(photos, desc="Computing hashes", unit="img"):
        # Path is relative to organized/
        image_path = OUTPUT_ROOT / path

        if not image_path.exists():
            errors += 1
            error_log.append((photo_id, path, "File not found"))
            continue

        # Compute hash
        phash = compute_phash(image_path)

        if phash is None:
            errors += 1
            error_log.append((photo_id, path, "Could not compute hash"))
            continue

        # Add to batch
        batch.append((phash, photo_id))
        processed += 1

        # Commit batch
        if len(batch) >= batch_size:
            conn.executemany("""
                UPDATE photos
                SET perceptual_hash = ?
                WHERE id = ?
            """, batch)
            conn.commit()
            batch = []

    # Commit remaining batch
    if batch:
        conn.executemany("""
            UPDATE photos
            SET perceptual_hash = ?
            WHERE id = ?
        """, batch)
        conn.commit()

    print(f"\n{'='*70}")
    print("RESULTS")
    print(f"{'='*70}\n")
    print(f"Successfully processed: {processed:6,} photos")
    print(f"Errors:                 {errors:6,} photos")

    if error_log:
        print(f"\nError summary (first 20):")
        for photo_id, path, reason in error_log[:20]:
            print(f"  {photo_id[:16]}... : {reason}")

        if len(error_log) > 20:
            print(f"  ... and {len(error_log) - 20} more")

    # Verify completion
    cursor = conn.execute("""
        SELECT COUNT(*)
        FROM photos
        WHERE perceptual_hash IS NOT NULL
        AND is_non_photo = 0
    """)
    total_with_hash = cursor.fetchone()[0]

    cursor = conn.execute("""
        SELECT COUNT(*)
        FROM photos
        WHERE is_non_photo = 0
    """)
    total_photos = cursor.fetchone()[0]

    print(f"\n{'='*70}")
    print(f"Database status:")
    print(f"  Total potential photos:     {total_photos:6,}")
    print(f"  Photos with perceptual hash: {total_with_hash:6,} ({total_with_hash*100/total_photos:.1f}%)")

    if total_with_hash < total_photos:
        missing = total_photos - total_with_hash
        print(f"  Missing hashes:              {missing:6,} (rerun to complete)")
    else:
        print(f"\n✓ All photos have perceptual hashes!")

    print(f"{'='*70}\n")

    conn.close()

if __name__ == "__main__":
    main()
