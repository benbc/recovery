#!/usr/bin/env python3
"""
Create directories with symlinks to inspect large duplicate groups.

This script creates a browse directory with symlinks to all photos in
large duplicate groups, making it easy to visually inspect them in a
file browser.
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("organized/photos.db")
OUTPUT_ROOT = Path("organized")
INSPECT_DIR = Path("inspect_groups")

def create_group_links(group_id: int):
    """Create symlinks for all photos in a group."""
    conn = sqlite3.connect(DB_PATH)

    # Get all photos in this group
    cursor = conn.execute("""
        SELECT dg.photo_id, dg.rank_in_group, dg.width, dg.height,
               dg.quality_score, dg.is_suggested_keeper, p.path, p.original_path
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ?
        ORDER BY dg.rank_in_group
    """, (group_id,))

    photos = cursor.fetchall()
    conn.close()

    if not photos:
        print(f"No photos found for group {group_id}")
        return

    group_size = len(photos)
    print(f"\nGroup {group_id}: {group_size} photos")

    # Create directory for this group
    group_dir = INSPECT_DIR / f"group_{group_id:05d}_size_{group_size}"
    group_dir.mkdir(parents=True, exist_ok=True)

    # Create symlinks
    for rank, (photo_id, rank_num, w, h, quality, keeper, path, orig_path) in enumerate(photos, 1):
        # Source file
        source = OUTPUT_ROOT / path

        if not source.exists():
            print(f"  Warning: {source} not found")
            continue

        # Create a descriptive name for the symlink
        keeper_mark = "KEEPER_" if keeper else ""
        filename = source.name
        link_name = f"{rank:04d}_{keeper_mark}{w}x{h}_q{quality}_{filename}"
        link_path = group_dir / link_name

        # Create symlink
        try:
            if link_path.exists():
                link_path.unlink()
            link_path.symlink_to(source.resolve())
        except Exception as e:
            print(f"  Error creating link: {e}")

    print(f"Created {len(photos)} symlinks in {group_dir}")

    # Create a text file with details
    info_file = group_dir / "GROUP_INFO.txt"
    with open(info_file, 'w') as f:
        f.write(f"Group {group_id}\n")
        f.write(f"Total photos: {group_size}\n")
        f.write(f"=" * 70 + "\n\n")

        for photo_id, rank_num, w, h, quality, keeper, path, orig_path in photos:
            f.write(f"Rank {rank_num}:\n")
            f.write(f"  Resolution: {w}×{h} ({w*h:,} pixels)\n")
            f.write(f"  Quality score: {quality:,}\n")
            f.write(f"  Keeper: {'YES' if keeper else 'no'}\n")
            f.write(f"  Current path: {path}\n")
            f.write(f"  Original path: {orig_path}\n")
            f.write("\n")

    print(f"Created info file: {info_file}")
    return group_dir

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    # Get command line argument or default to largest groups
    if len(sys.argv) > 1:
        group_ids = [int(arg) for arg in sys.argv[1:]]
    else:
        # Find largest groups
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute("""
            SELECT DISTINCT group_id, group_size
            FROM duplicate_groups
            ORDER BY group_size DESC
            LIMIT 10
        """)

        large_groups = cursor.fetchall()
        conn.close()

        print("Largest duplicate groups:")
        for gid, size in large_groups:
            print(f"  Group {gid}: {size} photos")

        group_ids = [gid for gid, size in large_groups]

    # Clean up old inspect directory
    if INSPECT_DIR.exists():
        import shutil
        shutil.rmtree(INSPECT_DIR)

    INSPECT_DIR.mkdir(parents=True, exist_ok=True)

    # Create links for each group
    created_dirs = []
    for group_id in group_ids:
        group_dir = create_group_links(group_id)
        if group_dir:
            created_dirs.append(group_dir)

    print(f"\n{'='*70}")
    print(f"Created inspection directories for {len(created_dirs)} groups")
    print(f"Location: {INSPECT_DIR.resolve()}")
    print(f"{'='*70}\n")
    print("Open this directory in your file browser to inspect photos visually.")

if __name__ == "__main__":
    main()
