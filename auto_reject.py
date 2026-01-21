#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = []
# ///
"""
Auto-resolve duplicate groups by rejecting obvious derivatives.

For each photo in a duplicate group, we check rejection rules.
Whatever survives all rules gets kept.

Rejection rules:
1. THUMBNAIL: Photo is a thumbnail (path or filename indicates this) AND
   a non-thumbnail higher-resolution version exists in the same filename cluster
2. IPHOTO_DUPLICATE: Photo is from iPhoto Library AND a same-resolution
   version exists in Photos Library in the same filename cluster
3. PHOTOBOOTH_FILTERED: Photo is from Photo Booth Pictures/ folder
   (these have filters applied)
"""

import re
import sqlite3
from collections import defaultdict
from pathlib import Path

DB_PATH = Path("organized/photos.db")


# -----------------------------------------------------------------------------
# Path classification helpers
# -----------------------------------------------------------------------------

def is_thumbnail_path(path: str) -> bool:
    """Check if path indicates this is a thumbnail."""
    path_lower = path.lower()
    return "/thumbnails/" in path_lower


def is_thumbnail_filename(path: str) -> bool:
    """Check if filename indicates this is a thumbnail."""
    filename = Path(path).name.lower()
    return filename.startswith("thumb_")


def is_thumbnail(path: str) -> bool:
    """Check if photo is a thumbnail (by path or filename)."""
    return is_thumbnail_path(path) or is_thumbnail_filename(path)


def is_photos_library(path: str) -> bool:
    """Check if path is from modern macOS Photos Library (.photoslibrary)."""
    return ".photoslibrary/" in path.lower()


def is_iphoto_library(path: str) -> bool:
    """Check if path is from iPhoto Library (.photolibrary)."""
    return ".photolibrary/" in path.lower()


def is_photobooth_pictures(path: str) -> bool:
    """Check if path is a filtered Photo Booth photo (Pictures subfolder)."""
    return "photo booth library/pictures/" in path.lower()


# -----------------------------------------------------------------------------
# Filename clustering
# -----------------------------------------------------------------------------

def extract_base_filename(path: str) -> str:
    """
    Extract base filename for clustering.

    Removes known derivative patterns to group originals with their derivatives.
    """
    filename = Path(path).stem  # Remove extension

    # Remove thumbnail prefix
    filename = re.sub(r"^thumb_", "", filename, flags=re.IGNORECASE)

    # Remove !cid_ prefix (email attachment extracts)
    filename = re.sub(r"^!cid_", "", filename, flags=re.IGNORECASE)

    # Remove known resolution suffix (_1024 is the only common one)
    filename = re.sub(r"_1024$", "", filename)

    return filename.upper()  # Normalize case


# -----------------------------------------------------------------------------
# Rejection rules
# -----------------------------------------------------------------------------

def check_thumbnail_rule(photo: dict, cluster: list[dict]) -> str | None:
    """
    Rule 1: Reject thumbnails when a non-thumbnail higher-resolution version exists.

    Only rejects if there's a "master" (non-thumbnail) to keep.
    If we only have thumbnails, keep them all for manual review.

    Returns rejection reason string, or None if rule doesn't apply.
    """
    if not is_thumbnail(photo["original_path"]):
        return None

    photo_resolution = photo["width"] * photo["height"]

    for other in cluster:
        if other["photo_id"] == photo["photo_id"]:
            continue
        if is_thumbnail(other["original_path"]):
            continue  # Don't reject thumbnail just because another thumbnail is bigger
        other_resolution = other["width"] * other["height"]
        if other_resolution > photo_resolution:
            return "thumbnail_with_master"

    return None


def check_iphoto_duplicate_rule(photo: dict, cluster: list[dict]) -> str | None:
    """
    Rule 2: Reject iPhoto version when same-resolution Photos Library version exists.

    Returns rejection reason string, or None if rule doesn't apply.
    """
    if not is_iphoto_library(photo["original_path"]):
        return None

    photo_resolution = photo["width"] * photo["height"]

    for other in cluster:
        if other["photo_id"] == photo["photo_id"]:
            continue
        if not is_photos_library(other["original_path"]):
            continue
        other_resolution = other["width"] * other["height"]
        if other_resolution == photo_resolution:
            return "iphoto_duplicate_of_photos"

    return None


def check_photobooth_filtered_rule(photo: dict, cluster: list[dict]) -> str | None:
    """
    Rule 3: Reject all Photo Booth filtered photos.

    Photo Booth stores originals in Originals/ and filtered versions in Pictures/.
    We always reject filtered versions - they have effects applied that we don't want.

    Returns rejection reason string, or None if rule doesn't apply.
    """
    if is_photobooth_pictures(photo["original_path"]):
        return "photobooth_filtered"

    return None


def check_rejection_rules(photo: dict, cluster: list[dict]) -> str | None:
    """
    Check all rejection rules for a photo.

    Returns rejection reason string, or None if photo should be kept.
    """
    # Try each rule in order
    reason = check_thumbnail_rule(photo, cluster)
    if reason:
        return reason

    reason = check_iphoto_duplicate_rule(photo, cluster)
    if reason:
        return reason

    reason = check_photobooth_filtered_rule(photo, cluster)
    if reason:
        return reason

    return None


# -----------------------------------------------------------------------------
# Main processing
# -----------------------------------------------------------------------------

def process_group(conn: sqlite3.Connection, group_id: int) -> list[tuple[str, str]]:
    """
    Process a single duplicate group.

    Returns list of (photo_id, reason) tuples for rejected photos.
    """
    cursor = conn.execute("""
        SELECT
            dg.photo_id,
            dg.width,
            dg.height,
            dg.file_size,
            p.original_path
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ?
    """, (group_id,))

    photos = [dict(row) for row in cursor.fetchall()]

    if not photos:
        return []

    # Group by base filename
    clusters = defaultdict(list)
    for photo in photos:
        base_name = extract_base_filename(photo["original_path"])
        clusters[base_name].append(photo)

    # Check each photo against rejection rules
    rejections = []

    for cluster in clusters.values():
        for photo in cluster:
            reason = check_rejection_rules(photo, cluster)
            if reason:
                rejections.append((photo["photo_id"], reason))

    return rejections


def main():
    print("Auto-resolving duplicate groups...")
    print()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Reset all rejections (start fresh)
    conn.execute("UPDATE duplicate_groups SET rejected = 0")
    conn.commit()

    # Get all duplicate groups
    cursor = conn.execute("""
        SELECT DISTINCT group_id
        FROM duplicate_groups
        ORDER BY group_id
    """)
    group_ids = [row["group_id"] for row in cursor.fetchall()]

    print(f"Processing {len(group_ids)} duplicate groups...")
    print()

    # Process each group
    total_rejections = 0
    reason_counts = defaultdict(int)

    for group_id in group_ids:
        rejections = process_group(conn, group_id)

        for photo_id, reason in rejections:
            conn.execute("""
                UPDATE duplicate_groups
                SET rejected = 1
                WHERE photo_id = ?
            """, (photo_id,))
            reason_counts[reason] += 1
            total_rejections += 1

    conn.commit()

    # Print summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print(f"Groups processed:   {len(group_ids):,}")
    print(f"Photos rejected:    {total_rejections:,}")
    print()
    print("Rejections by reason:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count:,}")
    print()

    conn.close()


if __name__ == "__main__":
    main()
