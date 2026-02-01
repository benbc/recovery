"""
Stage 1b: Link Files

Create hardlinks to all photos in a simple directory structure.
This provides:
1. Simple paths for serving files during analysis
2. Safety: original files are never touched after this stage

Directory structure: files/{hash[0:2]}/{hash}.{ext}

Output: `files/` directory with hardlinks to all photos
"""

import os
from pathlib import Path

from tqdm import tqdm

from .config import FILES_DIR, MIME_TO_EXT
from .database import get_connection, record_stage_completion


def get_extension(mime_type: str, original_filename: str) -> str:
    """
    Determine file extension from MIME type, falling back to original extension.
    """
    # Try MIME type first
    if mime_type in MIME_TO_EXT:
        return MIME_TO_EXT[mime_type]

    # Fall back to original extension
    ext = Path(original_filename).suffix.lower()
    if ext:
        return ext

    # Last resort
    return ".bin"


def get_link_path(photo_id: str, ext: str, files_dir: Path = FILES_DIR) -> Path:
    """
    Get the path where this photo should be linked.

    Structure: files/{hash[0:2]}/{hash}.{ext}
    """
    subdir = photo_id[:2]
    return files_dir / subdir / f"{photo_id}{ext}"


def run_stage1b(files_dir: Path = FILES_DIR, clear_existing: bool = False):
    """
    Create hardlinks to all photos.

    Args:
        files_dir: Directory for linked files
        clear_existing: If True, remove existing links first
    """
    print("=" * 70)
    print("STAGE 1b: Link Files")
    print("=" * 70)
    print()

    if clear_existing and files_dir.exists():
        print(f"Clearing existing files in {files_dir}...")
        import shutil
        shutil.rmtree(files_dir)

    files_dir.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        # Get all photos with their first source path
        cursor = conn.execute("""
            SELECT p.id, p.mime_type, pp.source_path, pp.filename
            FROM photos p
            JOIN photo_paths pp ON p.id = pp.photo_id
            WHERE pp.id IN (
                SELECT MIN(id) FROM photo_paths GROUP BY photo_id
            )
        """)
        photos = list(cursor.fetchall())

    print(f"Creating hardlinks for {len(photos):,} photos...")
    print(f"Target directory: {files_dir}")
    print()

    created = 0
    skipped = 0
    errors = 0
    error_details = []

    for row in tqdm(photos, desc="Linking"):
        photo_id = row["id"]
        mime_type = row["mime_type"]
        source_path = row["source_path"]
        filename = row["filename"]

        ext = get_extension(mime_type, filename)
        link_path = get_link_path(photo_id, ext, files_dir)

        # Skip if already exists
        if link_path.exists():
            skipped += 1
            continue

        # Create subdirectory if needed
        link_path.parent.mkdir(parents=True, exist_ok=True)

        # Create hardlink
        try:
            os.link(source_path, link_path)
            created += 1
        except OSError as e:
            errors += 1
            if len(error_details) < 10:
                error_details.append(f"{source_path}: {e}")

    print()
    print(f"Created: {created:,}")
    print(f"Skipped (existing): {skipped:,}")
    print(f"Errors: {errors:,}")

    if error_details:
        print()
        print("First errors:")
        for err in error_details:
            print(f"  {err}")

    # Record completion
    with get_connection() as conn:
        record_stage_completion(conn, "1b", created + skipped, f"created={created}, skipped={skipped}, errors={errors}")

    print()
    print("Stage 1b complete!")
