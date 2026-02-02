"""
Stage 1: Scan & Extract

Walk source directory, identify images by MIME type, compute SHA256,
extract EXIF metadata, and store ALL source paths for each unique hash
(preserving path info from duplicates).

Output: `photos` table + `photo_paths` table + `photo_date_sources` table
"""

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
from .utils.metadata import (
    extract_dimensions,
    extract_exif,
    get_exif_date_confidence,
    get_file_mtime,
    parse_date_from_filename,
    parse_date_from_path,
)


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

    Returns a dict with photo data and date sources, or None if not an image.
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

    # Get file mtime
    mtime = get_file_mtime(file_path)

    # Collect date sources for this path
    date_sources = []

    # EXIF dates
    if exif_data["exif_datetime_original"]:
        confidence = get_exif_date_confidence(exif_data, exif_data["exif_datetime_original"])
        date_sources.append({
            "source_type": "exif_original",
            "date_value": exif_data["exif_datetime_original"],
            "confidence": confidence,
            "raw_value": None,
        })

    if exif_data["exif_datetime_digitized"]:
        # Only add if different from original
        if exif_data["exif_datetime_digitized"] != exif_data["exif_datetime_original"]:
            confidence = get_exif_date_confidence(exif_data, exif_data["exif_datetime_digitized"])
            date_sources.append({
                "source_type": "exif_digitized",
                "date_value": exif_data["exif_datetime_digitized"],
                "confidence": confidence,
                "raw_value": None,
            })

    if exif_data["exif_datetime"]:
        # Only add if different from original and digitized
        if (exif_data["exif_datetime"] != exif_data["exif_datetime_original"] and
            exif_data["exif_datetime"] != exif_data["exif_datetime_digitized"]):
            confidence = get_exif_date_confidence(exif_data, exif_data["exif_datetime"])
            # DateTime alone (no Original) is lower confidence
            if confidence == "high":
                confidence = "medium"
            date_sources.append({
                "source_type": "exif_datetime",
                "date_value": exif_data["exif_datetime"],
                "confidence": confidence,
                "raw_value": None,
            })

    # Filename date
    filename_result = parse_date_from_filename(file_path.name)
    if filename_result:
        date_value, raw_value = filename_result
        date_sources.append({
            "source_type": "filename",
            "date_value": date_value,
            "confidence": "medium",
            "raw_value": raw_value,
        })

    # Path date
    path_result = parse_date_from_path(str(file_path))
    if path_result:
        date_value, confidence, raw_value = path_result
        date_sources.append({
            "source_type": "path_semantic",
            "date_value": date_value,
            "confidence": confidence,
            "raw_value": raw_value,
        })

    # mtime (always add as last resort)
    if mtime:
        date_sources.append({
            "source_type": "mtime",
            "date_value": mtime,
            "confidence": "low",
            "raw_value": None,
        })

    return {
        "photo": {
            "id": file_hash,
            "mime_type": mime_type,
            "file_size": file_path.stat().st_size,
            "width": width,
            "height": height,
            "exif_make": exif_data["exif_make"],
            "exif_model": exif_data["exif_model"],
            "exif_software": exif_data["exif_software"],
            "exif_datetime": exif_data["exif_datetime"],
            "exif_datetime_original": exif_data["exif_datetime_original"],
            "exif_datetime_digitized": exif_data["exif_datetime_digitized"],
        },
        "path": {
            "photo_id": file_hash,
            "source_path": str(file_path),
            "filename": file_path.name,
            "mtime": mtime,
        },
        "date_sources": date_sources,
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
            conn.execute("DELETE FROM photo_date_sources")
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
            "date_sources": 0,
            "errors": 0,
        }

        # Batch for database inserts
        photo_batch = []
        path_batch = []
        date_source_batch = []

        def flush_batches():
            nonlocal photo_batch, path_batch, date_source_batch
            if photo_batch:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO photos
                    (id, mime_type, file_size, width, height,
                     exif_make, exif_model, exif_software,
                     exif_datetime, exif_datetime_original, exif_datetime_digitized)
                    VALUES (:id, :mime_type, :file_size, :width, :height,
                            :exif_make, :exif_model, :exif_software,
                            :exif_datetime, :exif_datetime_original, :exif_datetime_digitized)
                    """,
                    photo_batch,
                )
                photo_batch = []
            if path_batch:
                # Insert paths and get their IDs for date sources
                for path_data in path_batch:
                    cursor = conn.execute(
                        """
                        INSERT INTO photo_paths (photo_id, source_path, filename, mtime)
                        VALUES (:photo_id, :source_path, :filename, :mtime)
                        """,
                        path_data,
                    )
                    path_id = cursor.lastrowid
                    # Update date sources with path_id
                    for ds in path_data.get("_date_sources", []):
                        ds["path_id"] = path_id
                        date_source_batch.append(ds)
                path_batch = []
            if date_source_batch:
                conn.executemany(
                    """
                    INSERT INTO photo_date_sources
                    (photo_id, source_type, date_value, confidence, raw_value, path_id)
                    VALUES (:photo_id, :source_type, :date_value, :confidence, :raw_value, :path_id)
                    """,
                    date_source_batch,
                )
                date_source_batch = []
            conn.commit()

        # Process files
        for file_path in tqdm(all_files, desc="Processing files"):
            try:
                result = process_file(file_path)
                if result is None:
                    continue

                stats["images_found"] += 1
                photo_id = result["photo"]["id"]

                # Check if this exact path already exists
                cursor = conn.execute(
                    "SELECT 1 FROM photo_paths WHERE source_path = ?",
                    (result["path"]["source_path"],)
                )
                if cursor.fetchone():
                    continue  # Path already recorded

                # Prepare date sources with photo_id
                date_sources_for_path = []
                for ds in result["date_sources"]:
                    ds["photo_id"] = photo_id
                    ds["path_id"] = None  # Will be set when path is inserted
                    date_sources_for_path.append(ds)
                    stats["date_sources"] += 1

                # Add path with attached date sources
                path_data = result["path"]
                path_data["_date_sources"] = date_sources_for_path
                path_batch.append(path_data)
                stats["new_paths"] += 1

                # Add photo if new
                if photo_id not in existing_ids:
                    photo_batch.append(result["photo"])
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
        total_date_sources = conn.execute("SELECT COUNT(*) FROM photo_date_sources").fetchone()[0]
        record_stage_completion(
            conn, "1",
            total_photos,
            f"paths={total_paths}, date_sources={total_date_sources}, new_photos={stats['new_photos']}, errors={stats['errors']}"
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
    print(f"Date sources:    {stats['date_sources']:,}")
    print(f"Errors:          {stats['errors']:,}")
    print()
    print(f"Total photos:    {total_photos:,}")
    print(f"Total paths:     {total_paths:,}")
    print(f"Total date srcs: {total_date_sources:,}")
    print()


if __name__ == "__main__":
    run_stage1()
