#!/usr/bin/env python3
# /// script
# dependencies = ["tqdm"]
# ///
"""
Filter out clearly identifiable non-photos from the collection.

This script identifies photos that are definitely not personal photos based on
safe, specific path and filename patterns. It marks them in the database but
does NOT delete them - they can be reviewed and restored if needed.

Categories filtered:
- Minecraft game textures
- HUE Animation stop-motion frames
- iChat/Skype icons and emoticons
- Web page assets (verified with matching .htm files)
- Flip Video camcorder thumbnails
- iPhoto face detection crops
- Trash/deleted files

All filters use conservative patterns to avoid false positives.
"""

import sqlite3
import sys
from pathlib import Path
from tqdm import tqdm

DB_PATH = Path("organized/photos.db")

# Filter patterns with descriptions
FILTER_CATEGORIES = [
    {
        'name': 'minecraft',
        'description': 'Minecraft game textures',
        'pattern': '%minecraft%',
        'size_check': None,
    },
    {
        'name': 'hue_animation',
        'description': 'HUE Animation stop-motion frames',
        'pattern': '%HUE Animation%',
        'size_check': None,
    },
    {
        'name': 'ichat_icons',
        'description': 'iChat/Skype icons and emoticons',
        'pattern': '%/iChat Icons/%',
        'size_check': None,
    },
    {
        'name': 'web_page_asset',
        'description': 'Web page assets (browser saved files)',
        'pattern': None,  # Already marked by identify_web_assets.py
        'column_check': 'is_web_page_asset = 1',
    },
    {
        'name': 'flip_video_thumbs',
        'description': 'Flip Video camcorder thumbnails',
        'pattern': '%/My Flip Video Prefs/%',
        'size_check': None,
    },
    {
        'name': 'face_detection',
        'description': 'iPhoto face detection crops',
        'pattern': None,  # Multiple patterns, handled separately
        'face_pattern': True,
        'size_check': 'width * height <= 250000',
    },
    {
        'name': 'face_detection_10plus',
        'description': 'iPhoto face detection crops (10+)',
        'pattern': None,
        'face_pattern_extended': True,
        'size_check': 'width * height <= 250000',
    },
    {
        'name': 'flipshare_previews',
        'description': 'FlipShare video preview thumbnails',
        'pattern': '%/FlipShare Data/Previews/%',
        'size_check': None,
    },
    {
        'name': 'tiny_icons',
        'description': 'Tiny icons and emoji (≤5000 pixels)',
        'pattern': None,
        'size_check': 'width * height <= 5000',
    },
    {
        'name': 'face_detection_modelresources',
        'description': 'Face detection thumbnails (modelresources)',
        'pattern': '%/modelresources/%',
        'size_check': 'ABS(width - height) <= 10 AND width <= 200',
    },
    {
        'name': 'trash',
        'description': 'Deleted files (trash directories)',
        'pattern': '%/.Trash%',
        'size_check': None,
    },
]

def build_face_pattern_sql():
    """Build SQL condition for face detection pattern (_face0.jpg through _face9.jpg)"""
    conditions = [f"original_path LIKE '%_face{i}.jpg'" for i in range(10)]
    return '(' + ' OR '.join(conditions) + ')'

def build_face_pattern_extended_sql():
    """Build SQL condition for face detection pattern (_face10.jpg through _face99.jpg)"""
    conditions = []
    for i in range(10, 100):
        conditions.append(f"original_path LIKE '%_face{i}.jpg'")
    return '(' + ' OR '.join(conditions) + ')'

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Add columns if they don't exist
    try:
        conn.execute("ALTER TABLE photos ADD COLUMN is_non_photo BOOLEAN DEFAULT 0")
        conn.execute("ALTER TABLE photos ADD COLUMN non_photo_reason TEXT")
        conn.commit()
        print("Added filtering columns to database")
    except sqlite3.OperationalError:
        # Columns already exist
        pass

    print("\n" + "="*70)
    print("FILTERING NON-PHOTOS")
    print("="*70)

    total_filtered = 0
    category_counts = {}

    for category in tqdm(FILTER_CATEGORIES, desc="Processing categories"):
        name = category['name']
        description = category['description']

        # Build WHERE clause
        conditions = []

        if category.get('pattern'):
            conditions.append(f"original_path LIKE '{category['pattern']}'")

        if category.get('column_check'):
            conditions.append(category['column_check'])

        if category.get('face_pattern'):
            conditions.append(build_face_pattern_sql())

        if category.get('face_pattern_extended'):
            conditions.append(build_face_pattern_extended_sql())

        if category.get('size_check'):
            conditions.append(category['size_check'])

        if not conditions:
            continue

        where_clause = ' AND '.join(conditions)

        # Count matching photos
        cursor = conn.execute(f"""
            SELECT COUNT(*)
            FROM photos
            WHERE ({where_clause})
            AND is_non_photo = 0
        """)
        count = cursor.fetchone()[0]

        if count > 0:
            # Mark photos as non-photos
            conn.execute(f"""
                UPDATE photos
                SET is_non_photo = 1,
                    non_photo_reason = ?
                WHERE ({where_clause})
                AND is_non_photo = 0
            """, (name,))

            conn.commit()
            category_counts[name] = (description, count)
            total_filtered += count

    print("\n" + "="*70)
    print("RESULTS")
    print("="*70)

    if category_counts:
        print("\nFiltered categories:\n")
        for name, (description, count) in sorted(category_counts.items(),
                                                  key=lambda x: x[1][1],
                                                  reverse=True):
            print(f"  {description:45} {count:6,} photos")

        print(f"\n{'='*70}")
        print(f"Total photos marked as non-photos: {total_filtered:,}")
        print(f"{'='*70}")
    else:
        print("\nNo new photos filtered (all already marked)")

    # Summary statistics
    cursor = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_non_photo = 1 THEN 1 ELSE 0 END) as filtered,
            SUM(CASE WHEN is_non_photo = 0 THEN 1 ELSE 0 END) as remaining
        FROM photos
    """)

    total, filtered, remaining = cursor.fetchone()

    print(f"\nCollection summary:")
    print(f"  Total photos in database:     {total:6,}")
    print(f"  Marked as non-photos:          {filtered:6,} ({filtered*100/total:5.1f}%)")
    print(f"  Remaining (potential photos):  {remaining:6,} ({remaining*100/total:5.1f}%)")

    # Show breakdown by reason
    print(f"\nBreakdown by filter reason:")
    cursor = conn.execute("""
        SELECT non_photo_reason, COUNT(*) as count
        FROM photos
        WHERE is_non_photo = 1
        GROUP BY non_photo_reason
        ORDER BY count DESC
    """)

    for reason, count in cursor:
        desc = next((cat['description'] for cat in FILTER_CATEGORIES
                    if cat['name'] == reason), reason)
        print(f"  {desc:45} {count:6,} photos")

    print(f"\n{'='*70}\n")
    print("Note: Photos are marked but NOT deleted.")
    print("Use move_filtered_photos.py to move them to organized/filtered/")
    print("or query the database to review them.")
    print()

    conn.close()

if __name__ == "__main__":
    main()
