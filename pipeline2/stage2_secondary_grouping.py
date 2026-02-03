"""
Pipeline2 Stage 2: Secondary Grouping

Creates a secondary grouping layer that merges primary groups and incorporates
ungrouped singles, while preserving the original duplicate_groups table.

This stage:
1. Gets all "kept" photos (not in junk_deletions, group_rejections, individual_decisions)
2. Compares all pairs using is_same_scene()
3. Clusters with complete-linkage
4. Creates secondary_groups for merged/new groups

The secondary_groups table is an overlay - photos not in it either:
- Remain in their primary group only (no merge needed)
- Or are truly ungrouped singletons
"""

from collections import defaultdict

from tqdm import tqdm

from pipeline.database import get_connection, record_stage_completion
from pipeline.stage4_group import (
    find_connected_components,
    complete_linkage_cluster,
    should_group,
)
from pipeline.utils.hashing import hamming_distance


def get_kept_photos_with_hashes(conn) -> list[dict]:
    """
    Get photos that are kept (not deleted/rejected) and have hashes.

    Excludes:
    - junk_deletions
    - group_rejections
    - individual_decisions (both reject and separate)
    """
    cursor = conn.execute("""
        SELECT p.id, p.perceptual_hash, p.dhash
        FROM photos p
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        WHERE jd.photo_id IS NULL
        AND gr.photo_id IS NULL
        AND id.photo_id IS NULL
        AND p.perceptual_hash IS NOT NULL
        AND p.dhash IS NOT NULL
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_primary_group_for_photo(conn, photo_id: str) -> int | None:
    """Get the primary group ID for a photo, or None if ungrouped."""
    cursor = conn.execute(
        "SELECT group_id FROM duplicate_groups WHERE photo_id = ?",
        (photo_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def run_stage2(clear_existing: bool = False) -> None:
    """
    Run Pipeline2 Stage 2: Secondary Grouping.

    Creates secondary_groups table as an overlay on primary groups.
    """
    print("=" * 70)
    print("PIPELINE2 STAGE 2: SECONDARY GROUPING")
    print("=" * 70)
    print()

    with get_connection() as conn:
        # Create/clear tables
        print("Setting up tables...")
        conn.execute("DROP TABLE IF EXISTS secondary_groups")
        conn.execute("DROP TABLE IF EXISTS secondary_unlinked_pairs")

        conn.execute("""
            CREATE TABLE secondary_groups (
                photo_id TEXT PRIMARY KEY,
                secondary_group_id INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX idx_secondary_groups_group
            ON secondary_groups(secondary_group_id)
        """)

        conn.execute("""
            CREATE TABLE secondary_unlinked_pairs (
                photo_id_1 TEXT NOT NULL,
                photo_id_2 TEXT NOT NULL,
                phash_dist INTEGER NOT NULL,
                dhash_dist INTEGER NOT NULL,
                reason TEXT NOT NULL,
                PRIMARY KEY (photo_id_1, photo_id_2)
            )
        """)
        conn.commit()

        # Get kept photos with hashes
        print("Loading kept photos with hashes...")
        photos = get_kept_photos_with_hashes(conn)
        print(f"Found {len(photos):,} kept photos with hashes")
        print()

        if len(photos) < 2:
            print("Need at least 2 photos to compare.")
            record_stage_completion(conn, "p2_2", 0, "insufficient photos")
            return

        # Build index for photo lookup
        id_to_idx = {p["id"]: i for i, p in enumerate(photos)}

        # Find all pairs that satisfy should_group()
        print("Finding candidate pairs...")
        edges = []  # List of (i, j) indices
        distances = {}  # (i, j) -> (phash_dist, dhash_dist) for i < j

        total_pairs = len(photos) * (len(photos) - 1) // 2
        print(f"Comparing {total_pairs:,} pairs...")

        for i in tqdm(range(len(photos)), desc="Comparing"):
            phash1 = photos[i]["perceptual_hash"]
            dhash1 = photos[i]["dhash"]

            for j in range(i + 1, len(photos)):
                phash2 = photos[j]["perceptual_hash"]
                dhash2 = photos[j]["dhash"]

                phash_dist = hamming_distance(phash1, phash2)
                dhash_dist = hamming_distance(dhash1, dhash2)

                if should_group(phash_dist, dhash_dist):
                    edges.append((i, j))
                    distances[(i, j)] = (phash_dist, dhash_dist)

        print(f"\nFound {len(edges):,} candidate pairs")

        if not edges:
            print("No pairs found that satisfy grouping criteria.")
            record_stage_completion(conn, "p2_2", 0, "no pairs")
            return

        # Find connected components
        print("Finding connected components...")
        components = find_connected_components(edges, len(photos))
        multi_photo_components = [c for c in components if len(c) > 1]
        print(f"Found {len(multi_photo_components):,} components with 2+ photos")

        # Perform complete-linkage clustering within each component
        print("Performing complete-linkage clustering...")
        all_clusters = []

        for component in tqdm(multi_photo_components, desc="Clustering"):
            component_indices = list(component)
            clusters = complete_linkage_cluster(component_indices, distances)
            all_clusters.extend(clusters)

        # Filter to clusters with 2+ members
        multi_photo_clusters = [c for c in all_clusters if len(c) >= 2]
        print(f"Found {len(multi_photo_clusters):,} clusters with 2+ photos")

        # Determine which clusters need secondary groups
        print("\nAnalyzing clusters for secondary grouping...")

        secondary_groups = []  # List of (secondary_group_id, set of photo_ids)
        stats = {
            "merged_primary_groups": 0,
            "singles_incorporated": 0,
            "unchanged_primary_groups": 0,
        }

        for cluster in multi_photo_clusters:
            photo_ids = {photos[idx]["id"] for idx in cluster}

            # Get primary groups for each photo in cluster
            primary_groups = {}
            ungrouped = set()

            for photo_id in photo_ids:
                pg = get_primary_group_for_photo(conn, photo_id)
                if pg is not None:
                    if pg not in primary_groups:
                        primary_groups[pg] = set()
                    primary_groups[pg].add(photo_id)
                else:
                    ungrouped.add(photo_id)

            # Decide if this cluster needs a secondary group
            needs_secondary = False

            if len(primary_groups) > 1:
                # Merging multiple primary groups
                needs_secondary = True
                stats["merged_primary_groups"] += len(primary_groups)

            if ungrouped:
                # Incorporating ungrouped singles
                needs_secondary = True
                stats["singles_incorporated"] += len(ungrouped)

            if len(primary_groups) == 1 and not ungrouped:
                # Cluster exactly matches a single primary group subset
                # Check if it's the entire primary group
                pg_id = list(primary_groups.keys())[0]
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM duplicate_groups WHERE group_id = ?",
                    (pg_id,)
                )
                primary_group_size = cursor.fetchone()[0]

                if len(photo_ids) < primary_group_size:
                    # Subset of primary group - might need secondary if other
                    # photos from same primary group are in different clusters
                    # For now, skip - we'll handle this case if needed
                    pass
                else:
                    # Exact match - no secondary group needed
                    stats["unchanged_primary_groups"] += 1
                    continue

            if needs_secondary:
                secondary_groups.append(photo_ids)

        print(f"\nSecondary groups to create: {len(secondary_groups):,}")
        print(f"  Primary groups merged: {stats['merged_primary_groups']:,}")
        print(f"  Singles incorporated: {stats['singles_incorporated']:,}")
        print(f"  Unchanged primary groups: {stats['unchanged_primary_groups']:,}")

        # Build index: photo_idx -> secondary_group_id (or None)
        idx_to_secondary = {}
        for sg_id, photo_ids in enumerate(secondary_groups):
            for photo_id in photo_ids:
                idx = id_to_idx[photo_id]
                idx_to_secondary[idx] = sg_id

        # Find unlinked pairs
        print("\nFinding unlinked pairs...")
        unlinked_pairs = []

        for (i, j), (pd, dd) in distances.items():
            sg_i = idx_to_secondary.get(i)
            sg_j = idx_to_secondary.get(j)

            # Check if they ended up in different final groups
            # (either different secondary groups, or one/both not in secondary)
            if sg_i != sg_j or (sg_i is None and sg_j is None):
                id1, id2 = photos[i]["id"], photos[j]["id"]
                if id1 > id2:
                    id1, id2 = id2, id1

                if sg_i is None and sg_j is None:
                    reason = "both_no_secondary"
                elif sg_i is None or sg_j is None:
                    reason = "one_no_secondary"
                else:
                    reason = "different_secondary"

                unlinked_pairs.append({
                    "photo_id_1": id1,
                    "photo_id_2": id2,
                    "phash_dist": pd,
                    "dhash_dist": dd,
                    "reason": reason,
                })

        print(f"Found {len(unlinked_pairs):,} unlinked pairs")

        # Insert secondary groups
        print("\nSaving to database...")
        records = []
        for sg_id, photo_ids in enumerate(secondary_groups):
            for photo_id in photo_ids:
                records.append({
                    "photo_id": photo_id,
                    "secondary_group_id": sg_id,
                })

        if records:
            conn.executemany(
                """
                INSERT INTO secondary_groups (photo_id, secondary_group_id)
                VALUES (:photo_id, :secondary_group_id)
                """,
                records,
            )

        # Insert unlinked pairs
        if unlinked_pairs:
            conn.executemany(
                """
                INSERT INTO secondary_unlinked_pairs
                (photo_id_1, photo_id_2, phash_dist, dhash_dist, reason)
                VALUES (:photo_id_1, :photo_id_2, :phash_dist, :dhash_dist, :reason)
                """,
                unlinked_pairs,
            )

        conn.commit()

        # Calculate final stats
        total_in_secondary = len(records)

        # Unlinked pair stats
        unlinked_by_reason = defaultdict(int)
        for p in unlinked_pairs:
            unlinked_by_reason[p["reason"]] += 1

        # Record completion
        record_stage_completion(
            conn, "p2_2",
            len(secondary_groups),
            f"photos={total_in_secondary}, unlinked={len(unlinked_pairs)}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("PIPELINE2 STAGE 2 COMPLETE")
    print("=" * 70)
    print()
    print(f"Secondary groups created: {len(secondary_groups):,}")
    print(f"Photos in secondary groups: {total_in_secondary:,}")
    print()
    print(f"Unlinked pairs: {len(unlinked_pairs):,}")
    for reason, count in sorted(unlinked_by_reason.items()):
        print(f"  {reason}: {count:,}")
    print()


if __name__ == "__main__":
    run_stage2()
