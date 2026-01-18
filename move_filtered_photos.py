#!/usr/bin/env python3
# /// script
# dependencies = ["tqdm"]
# ///
"""
Move filtered non-photos to organized/filtered/ directory.

Takes photos marked with is_non_photo=1 and moves them from organized/images/
to organized/filtered/{reason}/ subdirectories. Files are moved (not copied)
to save space, and database records are kept for potential restoration.
"""

import sqlite3
import sys
from pathlib import Path
from tqdm import tqdm

DB_PATH = Path("organized/photos.db")
OUTPUT_ROOT = Path("organized")
IMAGES_DIR = OUTPUT_ROOT / "images"
FILTERED_DIR = OUTPUT_ROOT / "filtered"

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Get all photos marked as non-photos
    cursor = conn.execute("""
        SELECT id, path, non_photo_reason, confidence_score
        FROM photos
        WHERE is_non_photo = 1
        ORDER BY non_photo_reason
    """)

    photos = cursor.fetchall()

    if not photos:
        print("No photos marked for filtering. Run filter_non_photos.py first.")
        conn.close()
        sys.exit(0)

    print(f"\n{'='*70}")
    print(f"MOVING FILTERED PHOTOS")
    print(f"{'='*70}")
    print(f"\nFound {len(photos):,} photos marked as non-photos")
    print(f"Moving from organized/images/ to organized/filtered/\n")

    # Create filtered directory structure
    FILTERED_DIR.mkdir(exist_ok=True)

    moved_count = 0
    error_count = 0
    already_moved = 0
    moved_by_reason = {}

    for photo_id, path, reason, confidence_score in tqdm(photos, desc="Moving files"):
        # Path in database is relative to organized/ directory
        source_path = OUTPUT_ROOT / path

        if not source_path.exists():
            # Already moved or deleted
            already_moved += 1
            continue

        # Build destination path: organized/filtered/{reason}/{confidence}/
        # Keep the confidence subdirectory structure
        confidence_bucket = (
            "high_confidence" if confidence_score >= 70
            else "medium_confidence" if confidence_score >= 40
            else "low_confidence"
        )

        dest_dir = FILTERED_DIR / reason / confidence_bucket
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_path = dest_dir / source_path.name

        try:
            # Move the file
            source_path.rename(dest_path)

            # Update database with new path (relative to organized/)
            relative_path = dest_path.relative_to(OUTPUT_ROOT)
            conn.execute("""
                UPDATE photos
                SET path = ?
                WHERE id = ?
            """, (str(relative_path), photo_id))

            moved_count += 1
            moved_by_reason[reason] = moved_by_reason.get(reason, 0) + 1

        except Exception as e:
            print(f"\nError moving {source_path}: {e}")
            error_count += 1
            continue

    # Commit all database updates
    conn.commit()

    print(f"\n{'='*70}")
    print("RESULTS")
    print(f"{'='*70}\n")

    if moved_count > 0:
        print("Moved by category:")
        for reason, count in sorted(moved_by_reason.items(),
                                     key=lambda x: x[1],
                                     reverse=True):
            print(f"  {reason:30} {count:6,} photos")

        print(f"\n{'='*70}")
        print(f"Successfully moved:  {moved_count:6,} photos")

    if already_moved > 0:
        print(f"Already moved:       {already_moved:6,} photos")

    if error_count > 0:
        print(f"Errors:              {error_count:6,} photos")

    print(f"{'='*70}\n")

    # Show directory structure
    print("Filtered photos organized in:")
    print(f"  {FILTERED_DIR}/")
    if FILTERED_DIR.exists():
        for reason_dir in sorted(FILTERED_DIR.iterdir()):
            if reason_dir.is_dir():
                total = sum(1 for _ in reason_dir.rglob("*") if _.is_file())
                print(f"    ├── {reason_dir.name}/ ({total:,} files)")

    print()

    # Clean up empty directories in organized/images/
    print("Cleaning up empty directories...")
    removed_dirs = 0
    for conf_dir in IMAGES_DIR.iterdir():
        if conf_dir.is_dir():
            # Remove if empty
            try:
                if not any(conf_dir.iterdir()):
                    conf_dir.rmdir()
                    removed_dirs += 1
            except:
                pass

    if removed_dirs > 0:
        print(f"Removed {removed_dirs} empty directories\n")

    conn.close()

if __name__ == "__main__":
    main()
