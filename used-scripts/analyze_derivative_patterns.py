#!/usr/bin/env python3
"""
Analyze actual patterns in duplicate groups to understand what derivative patterns exist.
This will help us build a data-driven auto-resolution strategy.
"""

import sqlite3
import re
from pathlib import Path
from collections import defaultdict, Counter

DB_PATH = Path("organized/photos.db")

def extract_path_components(original_path: str) -> dict:
    """Extract useful components from a path for pattern analysis."""
    parts = Path(original_path).parts
    filename = Path(original_path).name
    stem = Path(original_path).stem

    # Find directory names that might indicate thumbnails/derivatives
    path_keywords = []
    for part in parts:
        part_lower = part.lower()
        if any(keyword in part_lower for keyword in ['thumb', 'cache', 'preview', 'resized']):
            path_keywords.append(part)

    # Check for filename patterns
    filename_prefixes = []
    filename_suffixes = []

    # Prefixes
    if re.match(r'^thumb_', stem, re.IGNORECASE):
        filename_prefixes.append('thumb_')
    if re.match(r'^img_', stem, re.IGNORECASE):
        filename_prefixes.append('img_')

    # Suffixes
    if re.search(r'_\d{3,4}$', stem):
        match = re.search(r'_(\d{3,4})$', stem)
        filename_suffixes.append(f'_{match.group(1)}')
    if re.search(r'_thumb$', stem, re.IGNORECASE):
        filename_suffixes.append('_thumb')
    if re.search(r'_small$', stem, re.IGNORECASE):
        filename_suffixes.append('_small')

    return {
        'path_keywords': path_keywords,
        'filename_prefixes': filename_prefixes,
        'filename_suffixes': filename_suffixes,
        'stem': stem,
        'filename': filename
    }

def analyze_groups_with_size_variance():
    """Find groups where there's significant size variance (likely original + derivatives)."""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("Analyzing groups with size variance...")
    print()

    # Find groups with significant resolution variance
    cursor = conn.execute("""
        SELECT
            group_id,
            COUNT(*) as photo_count,
            MAX(width * height) as max_pixels,
            MIN(width * height) as min_pixels,
            MAX(width * height) * 1.0 / MIN(width * height) as size_ratio
        FROM duplicate_groups
        GROUP BY group_id
        HAVING size_ratio > 2.0 AND photo_count >= 3
        ORDER BY size_ratio DESC
        LIMIT 50
    """)

    groups_with_variance = cursor.fetchall()

    print(f"Found {len(groups_with_variance)} groups with significant size variance")
    print()

    # Analyze patterns in these groups
    path_keyword_counter = Counter()
    prefix_counter = Counter()
    suffix_counter = Counter()

    resolution_patterns = []

    for group in groups_with_variance[:20]:  # Analyze first 20
        group_id = group['group_id']

        cursor = conn.execute("""
            SELECT
                dg.photo_id,
                dg.width,
                dg.height,
                dg.file_size,
                dg.quality_score,
                p.original_path
            FROM duplicate_groups dg
            JOIN photos p ON dg.photo_id = p.id
            WHERE dg.group_id = ?
            ORDER BY dg.quality_score DESC
        """, (group_id,))

        photos = [dict(row) for row in cursor.fetchall()]

        # Track resolutions
        resolutions = [(p['width'], p['height']) for p in photos]
        resolution_patterns.append({
            'group_id': group_id,
            'resolutions': resolutions
        })

        # Analyze each photo
        for photo in photos:
            components = extract_path_components(photo['original_path'])

            for keyword in components['path_keywords']:
                path_keyword_counter[keyword] += 1
            for prefix in components['filename_prefixes']:
                prefix_counter[prefix] += 1
            for suffix in components['filename_suffixes']:
                suffix_counter[suffix] += 1

    print("=" * 80)
    print("PATTERN ANALYSIS")
    print("=" * 80)
    print()

    print("Most common path keywords indicating derivatives:")
    for keyword, count in path_keyword_counter.most_common(10):
        print(f"  {keyword}: {count}")
    print()

    print("Most common filename prefixes:")
    for prefix, count in prefix_counter.most_common(10):
        print(f"  {prefix}: {count}")
    print()

    print("Most common filename suffixes:")
    for suffix, count in suffix_counter.most_common(10):
        print(f"  {suffix}: {count}")
    print()

    print("=" * 80)
    print("RESOLUTION PATTERNS (first 10 groups)")
    print("=" * 80)
    print()

    for pattern in resolution_patterns[:10]:
        res_counts = Counter(pattern['resolutions'])
        print(f"Group {pattern['group_id']}:")
        for res, count in sorted(res_counts.items(), key=lambda x: x[0][0] * x[0][1], reverse=True):
            print(f"  {res[0]}x{res[1]}: {count} photo(s)")
        print()

    conn.close()

def find_groups_with_identical_filenames():
    """Find groups where multiple photos have the exact same base filename."""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("=" * 80)
    print("GROUPS WITH FILENAME COLLISIONS")
    print("=" * 80)
    print()

    # For each group, check if there are filename collisions
    cursor = conn.execute("""
        SELECT DISTINCT group_id
        FROM duplicate_groups
        ORDER BY group_id
        LIMIT 100
    """)

    groups_with_collisions = []

    for row in cursor:
        group_id = row['group_id']

        photos = conn.execute("""
            SELECT p.original_path, dg.width, dg.height
            FROM duplicate_groups dg
            JOIN photos p ON dg.photo_id = p.id
            WHERE dg.group_id = ?
        """, (group_id,)).fetchall()

        # Extract base filenames (removing extensions and resolution suffixes)
        base_names = []
        for photo in photos:
            stem = Path(photo['original_path']).stem
            # Remove common patterns
            stem = re.sub(r'^thumb_', '', stem, flags=re.IGNORECASE)
            stem = re.sub(r'_\d{3,4}$', '', stem)
            stem = re.sub(r'_thumb$', '', stem, flags=re.IGNORECASE)
            base_names.append(stem.upper())

        # Check for duplicates
        name_counts = Counter(base_names)
        if any(count > 1 for count in name_counts.values()):
            groups_with_collisions.append({
                'group_id': group_id,
                'name_counts': name_counts,
                'total_photos': len(photos)
            })

    print(f"Found {len(groups_with_collisions)} groups (out of first 100) with filename collisions")
    print()
    print("Examples:")
    for group in groups_with_collisions[:10]:
        print(f"  Group {group['group_id']}: {group['total_photos']} photos")
        for name, count in group['name_counts'].most_common(3):
            if count > 1:
                print(f"    '{name}' appears {count} times")
        print()

    conn.close()

def main():
    print("Analyzing derivative patterns in duplicate groups...")
    print()

    analyze_groups_with_size_variance()
    find_groups_with_identical_filenames()

    print("=" * 80)
    print("Analysis complete. Use these patterns to build auto-resolution logic.")
    print("=" * 80)

if __name__ == '__main__':
    main()
