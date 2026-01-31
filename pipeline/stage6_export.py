"""
Stage 6: Export

Copy/hardlink accepted photos to organized directory.
Structure: flat with hash-based names (date-based organization TBD).
Include all aggregated paths as metadata.

Output: Organized photos ready for use
"""

import os
import shutil
from pathlib import Path

from tqdm import tqdm

from .config import EXPORT_DIR
from .database import (
    get_connection,
    get_accepted_photos,
    record_stage_completion,
)


def get_all_paths_for_photo(conn, photo_id: str) -> list[str]:
    """Get all source paths for a photo, including aggregated paths."""
    paths = []

    # Original paths
    cursor = conn.execute(
        "SELECT source_path FROM photo_paths WHERE photo_id = ?",
        (photo_id,)
    )
    for row in cursor:
        paths.append(row["source_path"])

    # Aggregated paths from rejected duplicates
    cursor = conn.execute(
        "SELECT source_path FROM aggregated_paths WHERE kept_photo_id = ?",
        (photo_id,)
    )
    for row in cursor:
        paths.append(row["source_path"])

    return paths


def run_stage6(
    export_dir: Path = EXPORT_DIR,
    use_hardlinks: bool = True,
    clear_existing: bool = False
) -> None:
    """
    Run Stage 6: Export.

    Args:
        export_dir: Directory to export photos to
        use_hardlinks: If True, use hardlinks instead of copying (saves space)
        clear_existing: If True, clear existing exports before running
    """
    print("=" * 70)
    print("STAGE 6: EXPORT")
    print("=" * 70)
    print()
    print(f"Export directory: {export_dir}")
    print(f"Using {'hardlinks' if use_hardlinks else 'copies'}")
    print()

    # Create export directory
    export_dir.mkdir(parents=True, exist_ok=True)

    if clear_existing:
        print("Clearing existing exports...")
        for f in export_dir.glob("*"):
            if f.is_file():
                f.unlink()
        print()

    with get_connection() as conn:
        # Get accepted photos
        print("Finding accepted photos...")
        photos = get_accepted_photos(conn)
        print(f"Found {len(photos):,} accepted photos")
        print()

        if not photos:
            print("No photos to export.")
            record_stage_completion(conn, "6", 0, "no photos")
            return

        # Track stats
        exported = 0
        skipped = 0
        errors = 0

        # Export photos
        for photo in tqdm(photos, desc="Exporting photos"):
            photo_id = photo["id"]

            # Find a source file that exists
            all_paths = get_all_paths_for_photo(conn, photo_id)
            source_path = None
            for path in all_paths:
                if os.path.exists(path):
                    source_path = Path(path)
                    break

            if not source_path:
                errors += 1
                continue

            # Determine destination path (flat with hash-based name)
            ext = source_path.suffix or ".jpg"
            dest_path = export_dir / f"{photo_id}{ext}"

            if dest_path.exists():
                skipped += 1
                continue

            try:
                if use_hardlinks:
                    try:
                        os.link(source_path, dest_path)
                    except OSError:
                        # Fall back to copy if hardlink fails (cross-device)
                        shutil.copy2(source_path, dest_path)
                else:
                    shutil.copy2(source_path, dest_path)

                exported += 1

            except Exception as e:
                tqdm.write(f"Error exporting {photo_id}: {e}")
                errors += 1

        # Record completion
        record_stage_completion(
            conn, "6",
            exported,
            f"skipped={skipped}, errors={errors}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 6 COMPLETE")
    print("=" * 70)
    print()
    print(f"Photos exported:   {exported:,}")
    print(f"Already existed:   {skipped:,}")
    print(f"Errors:            {errors:,}")
    print()
    print(f"Export location: {export_dir}")
    print()


if __name__ == "__main__":
    run_stage6()
