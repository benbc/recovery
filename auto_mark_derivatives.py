#!/usr/bin/env python3
# /// script
# dependencies = []
# ///
"""
Auto-resolve duplicate groups that contain only derivatives (thumbnails) of the same photo(s).

Data-driven approach based on actual patterns found in the database:
- "Thumbnails" in path (most reliable: 43/43 occurrences)
- "thumb_" filename prefix (26 occurrences)
- "_1024" resolution suffix (21 occurrences)

Strategy:
1. Extract base filename from each photo using ONLY observed patterns
2. Group photos by base filename within each duplicate group
3. For each base filename cluster, keep ONLY photos at maximum resolution
4. Auto-resolve groups where all rejected photos are clearly derivatives
"""

import re
import sqlite3
from collections import defaultdict
from pathlib import Path

DB_PATH = Path("organized/photos.db")


def extract_base_filename(path: str) -> str:
    """
    Extract the base filename using ONLY patterns observed in real data.

    Observed patterns from analyze_derivative_patterns.py:
    - thumb_ prefix (26 occurrences)
    - _1024 suffix (21 occurrences)
    """
    filename = Path(path).stem  # Remove extension

    # Remove ONLY observed prefixes
    filename = re.sub(r"^thumb_", "", filename, flags=re.IGNORECASE)

    # Remove ONLY observed suffixes
    filename = re.sub(r"_1024$", "", filename)

    return filename.upper()  # Normalize case


def is_derivative_path(path: str) -> bool:
    """
    Check if path indicates this is a derivative/thumbnail.

    Based on observed data: "Thumbnails" appeared 43 times, all others rare.
    """
    path_lower = path.lower()

    # ONLY the most reliable indicator from our data
    return "/thumbnails/" in path_lower or "/thumb_" in path_lower


def is_modern_photos_library(path: str) -> bool:
    """
    Check if path is from the modern macOS Photos Library.

    Photos libraries use .photoslibrary extension (plural).
    This includes "Photos Library.photoslibrary", "Rose's photo library.photoslibrary",
    and any other custom-named Photos libraries.
    """
    path_lower = path.lower()
    return ".photoslibrary/" in path_lower


def is_iphoto_library(path: str) -> bool:
    """
    Check if path is from iPhoto Library.

    iPhoto libraries use .photolibrary extension (singular).
    """
    path_lower = path.lower()
    return ".photolibrary/" in path_lower


def analyze_group(conn, group_id: int) -> dict:
    """
    Analyze a duplicate group to determine if it can be auto-resolved.

    Returns:
        {
            'can_auto_resolve': bool,
            'filename_clusters': {base_filename: [photo_ids]},
            'suggested_keepers': [photo_ids],
            'reason': str
        }
    """
    cursor = conn.execute(
        """
        SELECT
            dg.photo_id,
            dg.quality_score,
            dg.width,
            dg.height,
            dg.file_size,
            p.path,
            p.original_path
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ?
        ORDER BY dg.quality_score DESC
    """,
        (group_id,),
    )

    photos = [dict(row) for row in cursor.fetchall()]

    if not photos:
        return {"can_auto_resolve": False, "reason": "Empty group"}

    # Group by base filename
    filename_clusters = defaultdict(list)
    for photo in photos:
        base_name = extract_base_filename(photo["original_path"])
        filename_clusters[base_name].append(photo)

    # Check if we can auto-resolve
    suggested_keepers = []
    has_derivatives = False

    for base_name, cluster in filename_clusters.items():
        if len(cluster) == 1:
            # Only one photo with this base name - keep it
            suggested_keepers.append(cluster[0]["photo_id"])
            continue

        # Multiple versions of the same base filename
        # Sort by quality score, then by file size (larger is better)
        cluster.sort(key=lambda x: (x["quality_score"], x["file_size"]), reverse=True)

        # Find the max resolution in this cluster
        max_resolution = cluster[0]["width"] * cluster[0]["height"]

        # Get all photos at max resolution
        max_res_photos = [
            p for p in cluster if p["width"] * p["height"] == max_resolution
        ]

        # If there are multiple photos at max resolution, prefer Photos Library over iPhoto, then largest file
        if len(max_res_photos) > 1:
            # Separate modern Photos Library from iPhoto Library and others
            modern_photos = [
                p
                for p in max_res_photos
                if is_modern_photos_library(p["original_path"])
            ]
            iphoto_lib = [
                p for p in max_res_photos if is_iphoto_library(p["original_path"])
            ]
            other = [
                p
                for p in max_res_photos
                if not is_modern_photos_library(p["original_path"])
                and not is_iphoto_library(p["original_path"])
            ]

            # Prefer modern Photos Library, then other libraries, then iPhoto Library (as last resort)
            # Within each category, prefer larger file size
            if modern_photos:
                modern_photos.sort(key=lambda x: x["file_size"], reverse=True)
                best = modern_photos[0]
            elif other:
                other.sort(key=lambda x: x["file_size"], reverse=True)
                best = other[0]
            else:
                iphoto_lib.sort(key=lambda x: x["file_size"], reverse=True)
                best = iphoto_lib[0]
        else:
            best = max_res_photos[0]

        # Check if lower quality versions are clearly derivatives
        derivatives_found = False

        for photo in cluster:
            if photo["photo_id"] == best["photo_id"]:
                continue

            # Check if it's a derivative
            if is_derivative_path(photo["original_path"]):
                derivatives_found = True
            # Check if it's significantly smaller
            elif (
                photo["width"] < best["width"] * 0.9
                or photo["height"] < best["height"] * 0.9
            ):
                derivatives_found = True
            # Check if it's at max resolution but smaller file (likely re-encoded)
            elif (
                photo["width"] * photo["height"] == max_resolution
                and photo["file_size"] < best["file_size"]
            ):
                derivatives_found = True

        if derivatives_found:
            has_derivatives = True
            # Keep the best quality version (largest file at max resolution)
            suggested_keepers.append(best["photo_id"])
        else:
            # Can't clearly identify derivatives - needs manual review
            # Keep all versions for manual review
            for photo in cluster:
                suggested_keepers.append(photo["photo_id"])

    # Determine if we can auto-resolve or partially resolve
    # Two modes:
    # 1. FULL auto-resolution: All keepers at exact max resolution, all rejects are derivatives
    # 2. PARTIAL resolution: Mark obvious derivatives even if keepers vary in resolution

    can_auto_resolve = False
    can_partial_resolve = False
    partial_rejects = []
    reason = ""

    if not has_derivatives:
        reason = "No clear derivative patterns found"
    elif len(suggested_keepers) == len(photos):
        reason = "No photos would be rejected"
    else:
        # Check if all suggested keepers are at EXACT maximum resolution
        keeper_photos = [p for p in photos if p["photo_id"] in suggested_keepers]
        rejected_photos = [p for p in photos if p["photo_id"] not in suggested_keepers]

        max_resolution = max(p["width"] * p["height"] for p in keeper_photos)
        min_keeper_resolution = min(p["width"] * p["height"] for p in keeper_photos)

        # Check for FULL auto-resolution
        if min_keeper_resolution == max_resolution:
            # Additionally verify rejected photos are clearly derivatives
            # Get max file size among keepers for comparison
            max_keeper_file_size = max(p["file_size"] for p in keeper_photos)

            all_rejected_are_derivatives = True
            for rejected in rejected_photos:
                is_derivative = (
                    is_derivative_path(rejected["original_path"])
                    or rejected["width"] * rejected["height"] < max_resolution * 0.5
                    or
                    # Also accept max-res photos with smaller file sizes (re-encoded versions)
                    (
                        rejected["width"] * rejected["height"] == max_resolution
                        and rejected["file_size"] < max_keeper_file_size
                    )
                )
                if not is_derivative:
                    all_rejected_are_derivatives = False
                    break

            if all_rejected_are_derivatives:
                can_auto_resolve = True
                num_rejected = len(photos) - len(suggested_keepers)
                num_clusters = len(filename_clusters)
                reason = f"Auto-resolved: {num_rejected} derivatives rejected, {len(suggested_keepers)} originals kept across {num_clusters} base filename(s)"
            else:
                reason = "Some rejected photos don't match derivative patterns - needs manual review"
        else:
            # Can't fully auto-resolve, but check for PARTIAL resolution
            # Mark only OBVIOUS thumbnails/derivatives for removal
            for rejected in rejected_photos:
                rejected_resolution = rejected["width"] * rejected["height"]
                # Only mark as derivative if it's clearly a thumbnail:
                # 1. In a thumbnail path, OR
                # 2. Less than 50% of the MINIMUM keeper resolution (very conservative)
                if (
                    is_derivative_path(rejected["original_path"])
                    or rejected_resolution < min_keeper_resolution * 0.5
                ):
                    partial_rejects.append(rejected["photo_id"])

            if partial_rejects:
                can_partial_resolve = True
                reason = f"Partial resolution: {len(partial_rejects)} obvious thumbnails can be removed, {len(suggested_keepers)} photos need manual review"
            else:
                reason = f"Keepers not all at max resolution (max={max_resolution}, min={min_keeper_resolution}) - needs manual review"

    return {
        "can_auto_resolve": can_auto_resolve,
        "can_partial_resolve": can_partial_resolve,
        "partial_rejects": partial_rejects,
        "filename_clusters": {
            k: [p["photo_id"] for p in v] for k, v in filename_clusters.items()
        },
        "suggested_keepers": suggested_keepers,
        "reason": reason,
    }


def main():
    print("Auto-marking derivatives in duplicate groups...")
    print()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Add auto_resolved column if it doesn't exist
    try:
        conn.execute(
            "ALTER TABLE duplicate_groups ADD COLUMN auto_resolved INTEGER DEFAULT 0"
        )
        conn.commit()
        print("Added auto_resolved column to duplicate_groups table")
    except sqlite3.OperationalError:
        # Column already exists
        pass

    # Get only groups that haven't been auto-resolved yet
    cursor = conn.execute("""
        SELECT DISTINCT group_id, group_size
        FROM duplicate_groups
        WHERE auto_resolved = 0 OR auto_resolved IS NULL
        ORDER BY group_id
    """)
    groups = cursor.fetchall()

    print(f"Analyzing {len(groups)} duplicate groups...")
    print()

    auto_resolved_count = 0
    partial_resolved_count = 0
    manual_review_count = 0
    total_derivatives_rejected = 0
    total_partial_derivatives_rejected = 0

    stats_by_reason = defaultdict(int)

    for group in groups:
        group_id = group["group_id"]
        analysis = analyze_group(conn, group_id)

        stats_by_reason[analysis["reason"]] += 1

        if analysis["can_auto_resolve"]:
            # FULL auto-resolution: mark entire group as resolved
            # First, unmark all photos in this group
            conn.execute(
                """
                UPDATE duplicate_groups
                SET is_suggested_keeper = 0, auto_resolved = 0
                WHERE group_id = ?
            """,
                (group_id,),
            )

            # Mark suggested keepers
            if analysis["suggested_keepers"]:
                placeholders = ",".join("?" * len(analysis["suggested_keepers"]))
                conn.execute(
                    f"""
                    UPDATE duplicate_groups
                    SET is_suggested_keeper = 1, auto_resolved = 1
                    WHERE group_id = ? AND photo_id IN ({placeholders})
                """,
                    [group_id] + analysis["suggested_keepers"],
                )

            # Mark non-keepers as auto-resolved too
            conn.execute(
                """
                UPDATE duplicate_groups
                SET auto_resolved = 1
                WHERE group_id = ? AND is_suggested_keeper = 0
            """,
                (group_id,),
            )

            auto_resolved_count += 1
            num_rejected = group["group_size"] - len(analysis["suggested_keepers"])
            total_derivatives_rejected += num_rejected

        elif analysis["can_partial_resolve"]:
            # PARTIAL resolution: mark only obvious derivatives as resolved
            # Keepers remain unmarked (auto_resolved = 0) for manual review
            if analysis["partial_rejects"]:
                placeholders = ",".join("?" * len(analysis["partial_rejects"]))
                conn.execute(
                    f"""
                    UPDATE duplicate_groups
                    SET auto_resolved = 1, is_suggested_keeper = 0
                    WHERE group_id = ? AND photo_id IN ({placeholders})
                """,
                    [group_id] + analysis["partial_rejects"],
                )

            partial_resolved_count += 1
            total_partial_derivatives_rejected += len(analysis["partial_rejects"])
            manual_review_count += 1  # Still counts as needing manual review

        else:
            manual_review_count += 1

    conn.commit()

    # Print summary
    print("=" * 80)
    print("AUTO-RESOLUTION SUMMARY")
    print("=" * 80)
    print()
    print(f"Total groups analyzed:            {len(groups):,}")
    print(f"Fully auto-resolved groups:       {auto_resolved_count:,}")
    print(f"Partially resolved groups:        {partial_resolved_count:,}")
    print(f"Require manual review:            {manual_review_count:,}")
    print(f"Derivatives auto-rejected:        {total_derivatives_rejected:,}")
    print(f"Thumbnails partially rejected:    {total_partial_derivatives_rejected:,}")
    print()
    print("Breakdown by reason:")
    print("-" * 80)
    for reason, count in sorted(
        stats_by_reason.items(), key=lambda x: x[1], reverse=True
    ):
        if "Auto-resolved" in reason:
            print(f"  ✓ {reason}: {count:,} groups")
        elif "Partial resolution" in reason:
            print(f"  ◐ {reason}: {count:,} groups")
        else:
            print(f"  ⚠ {reason}: {count:,} groups")
    print()

    # Show some examples of auto-resolved groups
    print("=" * 80)
    print("EXAMPLES OF AUTO-RESOLVED GROUPS")
    print("=" * 80)
    print()

    cursor = conn.execute("""
        SELECT
            dg.group_id,
            COUNT(*) as total_photos,
            SUM(CASE WHEN dg.is_suggested_keeper = 1 THEN 1 ELSE 0 END) as keepers,
            COUNT(*) - SUM(CASE WHEN dg.is_suggested_keeper = 1 THEN 1 ELSE 0 END) as rejected
        FROM duplicate_groups dg
        WHERE dg.auto_resolved = 1
        GROUP BY dg.group_id
        HAVING keepers > 0
        ORDER BY rejected DESC
        LIMIT 10
    """)

    examples = cursor.fetchall()
    if examples:
        print(f"{'Group':<10} {'Total':<8} {'Kept':<8} {'Rejected':<10} Example")
        print("-" * 80)
        for ex in examples:
            # Get an example keeper filename
            keeper = conn.execute(
                """
                SELECT p.original_path
                FROM duplicate_groups dg
                JOIN photos p ON dg.photo_id = p.id
                WHERE dg.group_id = ? AND dg.is_suggested_keeper = 1
                LIMIT 1
            """,
                (ex["group_id"],),
            ).fetchone()

            filename = Path(keeper["original_path"]).name if keeper else "N/A"
            print(
                f"{ex['group_id']:<10} {ex['total_photos']:<8} {ex['keepers']:<8} {ex['rejected']:<10} {filename}"
            )

    print()
    print("=" * 80)
    print(f"✓ Auto-resolution complete!")
    print(f"✓ {manual_review_count:,} groups remaining for manual review")
    print("=" * 80)

    conn.close()


if __name__ == "__main__":
    main()
