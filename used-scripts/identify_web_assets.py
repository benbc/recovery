#!/usr/bin/env python3
# /// script
# dependencies = ["tqdm"]
# ///
"""
Identify web page assets (images from browser-saved HTML pages).

Web browsers save pages as "Page Name.htm" + "Page Name_files/" directory.
This script identifies photos in *_files/ directories that have a matching
.htm/.html file, marking them as web_page_assets in the database.

Safe filtering: Only marks images where we can verify the browser save pattern.
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

DB_PATH = Path("organized/photos.db")

def extract_files_dir_info(path: str) -> tuple[Path, str] | None:
    """
    Extract info about a *_files/ directory from a photo path.

    Returns: (parent_dir, base_name) or None
    Example:
        /path/to/Foresight_files/image.jpg -> (Path('/path/to'), 'Foresight')
        /path/to/Old Desktop files/pic.jpg -> (Path('/path/to'), 'Old Desktop')
    """
    path_obj = Path(path)

    # Find the *_files directory in the path
    for i, part in enumerate(path_obj.parts):
        if part.endswith('_files'):
            # Get parent directory (everything up to this point)
            parent = Path(*path_obj.parts[:i])
            # Get base name (remove _files suffix)
            base_name = part[:-6]  # Remove '_files'
            return (parent, base_name)

    return None

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Add column if it doesn't exist
    try:
        conn.execute("ALTER TABLE photos ADD COLUMN is_web_page_asset BOOLEAN DEFAULT 0")
        conn.commit()
        print("Added is_web_page_asset column to database")
    except sqlite3.OperationalError:
        # Column already exists
        pass

    # Get all photos in *_files/ directories
    cursor = conn.execute("""
        SELECT id, original_path
        FROM photos
        WHERE original_path LIKE '%_files/%'
    """)

    photos = cursor.fetchall()
    print(f"\nFound {len(photos)} photos in *_files/ directories")

    if not photos:
        print("Nothing to do.")
        conn.close()
        return

    # Group photos by their _files directory
    files_dirs = defaultdict(list)
    for photo_id, original_path in photos:
        info = extract_files_dir_info(original_path)
        if info:
            parent_dir, base_name = info
            files_dirs[(parent_dir, base_name)].append(photo_id)

    print(f"Found {len(files_dirs)} unique *_files/ directories")
    print("\nChecking for matching .htm/.html files...\n")

    # Check each directory for matching HTML file
    web_asset_ids = []
    verified_dirs = []
    non_web_dirs = []

    for (parent_dir, base_name), photo_ids in tqdm(files_dirs.items(), desc="Verifying"):
        # Check for matching HTML file
        htm_path = parent_dir / f"{base_name}.htm"
        html_path = parent_dir / f"{base_name}.html"

        if htm_path.exists() or html_path.exists():
            # This is a browser-saved web page
            web_asset_ids.extend(photo_ids)
            verified_dirs.append((parent_dir, base_name, len(photo_ids)))
        else:
            # Not a web page - could be real photos
            non_web_dirs.append((parent_dir, base_name, len(photo_ids)))

    # Update database
    if web_asset_ids:
        print(f"\n\nMarking {len(web_asset_ids)} photos as web page assets...")

        # Use batch updates for efficiency
        placeholders = ','.join('?' * len(web_asset_ids))
        conn.execute(f"""
            UPDATE photos
            SET is_web_page_asset = 1
            WHERE id IN ({placeholders})
        """, web_asset_ids)

        conn.commit()
        print("✓ Database updated")

    # Print summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    if verified_dirs:
        print(f"\n✓ Verified web page assets ({len(verified_dirs)} directories, {len(web_asset_ids)} photos):")
        for parent, base, count in sorted(verified_dirs):
            print(f"  {parent}/{base}_files/ ({count} images)")

    if non_web_dirs:
        print(f"\n⚠ NOT web assets ({len(non_web_dirs)} directories, will be kept):")
        for parent, base, count in sorted(non_web_dirs):
            print(f"  {parent}/{base}_files/ ({count} images)")

    # Statistics
    cursor = conn.execute("""
        SELECT COUNT(*)
        FROM photos
        WHERE is_web_page_asset = 1
    """)
    total_marked = cursor.fetchone()[0]

    print(f"\n{'='*70}")
    print(f"Total photos marked as web_page_asset: {total_marked}")
    print(f"{'='*70}\n")

    conn.close()

if __name__ == "__main__":
    main()
