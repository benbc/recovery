"""
Stage 1: Scan & Extract

Walk source directory, identify images by MIME type, compute SHA256,
extract EXIF metadata, parse dates, and store ALL source paths for each
unique hash (preserving path info from duplicates).

Output: `photos` table + `photo_paths` table
"""

import sqlite3
from pathlib import Path

import magic
from tqdm import tqdm

from .config import (
    BATCH_SIZE,
    EXCLUDE_FILENAMES,
    IMAGE_MIME_TYPES,
    SOURCE_ROOT,
)
from .database import get_connection, init_db, record_stage_completion
from .utils.hashing import compute_sha256
from .utils.metadata import determine_date, extract_dimensions, extract_exif


def scan_source_directory(source_root: Path = SOURCE_ROOT) -> list[Path]:
    """Scan source directory for all files."""
    print(f"Scanning {source_root}...")
    all_files = []

    for file_path in source_root.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.name in EXCLUDE_FILENAMES:
            continue
        if file_path.name.startswith("._"):  # Mac resource forks
            continue
        all_files.append(file_path)

    print(f"Found {len(all_files):,} files to process")
    return all_files


def process_file(file_path: Path) -> dict | None:
    """
    Process a single file and extract metadata.

    Returns a dict with photo data, or None if not an image.
    """
    # Check MIME type
    try:
        mime_type = magic.from_file(str(file_path), mime=True)
    except Exception:
        return None

    if mime_type not in IMAGE_MIME_TYPES:
        return None

    # Compute SHA256 hash
    file_hash = compute_sha256(file_path)

    # Extract EXIF data
    exif_data = extract_exif(file_path)

    # Get dimensions
    width, height = extract_dimensions(file_path)

    # Determine date
    date_taken, date_source = determine_date(file_path, exif_data)

    return {
        "id": file_hash,
        "mime_type": mime_type,
        "file_size": file_path.stat().st_size,
        "width": width,
        "height": height,
        "date_taken": date_taken.isoformat() if date_taken else None,
        "date_source": date_source,
        "has_exif": exif_data["has_exif"],
        "source_path": str(file_path),
        "filename": file_path.name,
    }


def run_stage1(source_root: Path = SOURCE_ROOT, clear_existing: bool = False) -> None:
    """
    Run Stage 1: Scan & Extract.

    Args:
        source_root: Directory to scan for photos
        clear_existing: If True, clear existing data before running
    """
    print("=" * 70)
    print("STAGE 1: SCAN & EXTRACT")
    print("=" * 70)
    print()

    # Initialize database
    init_db()

    with get_connection() as conn:
        if clear_existing:
            print("Clearing existing Stage 1 data...")
            conn.execute("DELETE FROM photo_paths")
            conn.execute("DELETE FROM photos")
            conn.commit()

        # Get existing photo IDs to avoid reprocessing
        cursor = conn.execute("SELECT id FROM photos")
        existing_ids = {row["id"] for row in cursor.fetchall()}
        print(f"Found {len(existing_ids):,} existing photos in database")

        # Scan source directory
        all_files = scan_source_directory(source_root)

        # Track stats
        stats = {
            "images_found": 0,
            "new_photos": 0,
            "new_paths": 0,
            "errors": 0,
        }

        # Batch for database inserts
        photo_batch = []
        path_batch = []

        def flush_batches():
            nonlocal photo_batch, path_batch
            if photo_batch:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO photos
                    (id, mime_type, file_size, width, height, date_taken, date_source, has_exif)
                    VALUES (:id, :mime_type, :file_size, :width, :height, :date_taken, :date_source, :has_exif)
                    """,
                    photo_batch,
                )
                photo_batch = []
            if path_batch:
                conn.executemany(
                    """
                    INSERT INTO photo_paths (photo_id, source_path, filename)
                    VALUES (:photo_id, :source_path, :filename)
                    """,
                    path_batch,
                )
                path_batch = []
            conn.commit()

        # Process files
        for file_path in tqdm(all_files, desc="Processing files"):
            try:
                result = process_file(file_path)
                if result is None:
                    continue

                stats["images_found"] += 1
                photo_id = result["id"]

                # Check if this exact path already exists
                cursor = conn.execute(
                    "SELECT 1 FROM photo_paths WHERE source_path = ?",
                    (result["source_path"],)
                )
                if cursor.fetchone():
                    continue  # Path already recorded

                # Add path
                path_batch.append({
                    "photo_id": photo_id,
                    "source_path": result["source_path"],
                    "filename": result["filename"],
                })
                stats["new_paths"] += 1

                # Add photo if new
                if photo_id not in existing_ids:
                    photo_batch.append(result)
                    existing_ids.add(photo_id)
                    stats["new_photos"] += 1

                # Flush batches periodically
                if len(photo_batch) >= BATCH_SIZE or len(path_batch) >= BATCH_SIZE:
                    flush_batches()

            except Exception as e:
                stats["errors"] += 1
                tqdm.write(f"Error processing {file_path}: {e}")

        # Final flush
        flush_batches()

        # Record completion
        total_photos = conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
        total_paths = conn.execute("SELECT COUNT(*) FROM photo_paths").fetchone()[0]
        record_stage_completion(
            conn, "1",
            total_photos,
            f"paths={total_paths}, new_photos={stats['new_photos']}, errors={stats['errors']}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 1 COMPLETE")
    print("=" * 70)
    print()
    print(f"Images found:    {stats['images_found']:,}")
    print(f"New photos:      {stats['new_photos']:,}")
    print(f"New paths:       {stats['new_paths']:,}")
    print(f"Errors:          {stats['errors']:,}")
    print()
    print(f"Total photos:    {total_photos:,}")
    print(f"Total paths:     {total_paths:,}")
    print()


if __name__ == "__main__":
    run_stage1()
