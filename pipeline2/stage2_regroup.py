"""
Stage 2: Regroup using pHash16 + colorHash thresholds.

Two-stage clustering:
1. Complete linkage with relaxed threshold -> creates tight "kernel" groups
2. Single linkage extension with strict threshold -> extends kernels cautiously

Uses threshold boundaries drawn in the tuner UI, stored in threshold_boundaries.json.
Boundaries define the last INCLUDED cells - pairs at or below the boundary are grouped.

Ignores primary groups - this is a fresh regrouping of all kept photos.
"""

import json
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

from pipeline.database import get_connection, record_stage_completion
from .graph_utils import (
    find_connected_components,
    complete_linkage_cluster,
    single_linkage_extend,
)


THRESHOLDS_FILE = Path(__file__).parent.parent / "output" / "threshold_boundaries.json"


def load_thresholds() -> dict:
    """Load threshold boundaries from file."""
    if not THRESHOLDS_FILE.exists():
        raise FileNotFoundError(
            f"Threshold boundaries not found: {THRESHOLDS_FILE}\n"
            "Use the threshold tuner to draw boundaries first."
        )
    with open(THRESHOLDS_FILE) as f:
        return json.load(f)


def build_threshold_predicate(boundary_cells: list[str]):
    """
    Build a predicate function from boundary cells.

    Boundary cells are "phash16,colorhash" strings representing the LAST INCLUDED
    cells at each colorhash level. A pair is included if:
    - Its colorhash is in the boundary, AND
    - Its phash16 is <= the max phash16 for that colorhash

    Args:
        boundary_cells: List of "p,c" strings

    Returns:
        Predicate function: (phash16_dist, colorhash_dist) -> bool
    """
    if not boundary_cells:
        # Empty boundary = nothing passes
        return lambda p, c: False

    # Build max phash16 for each colorhash
    max_p_by_c = {}
    for cell in boundary_cells:
        p, c = map(int, cell.split(","))
        if c not in max_p_by_c or p > max_p_by_c[c]:
            max_p_by_c[c] = p

    def predicate(phash16_dist: int, colorhash_dist: int) -> bool:
        if colorhash_dist not in max_p_by_c:
            return False
        return phash16_dist <= max_p_by_c[colorhash_dist]

    return predicate


def run_stage2() -> None:
    """
    Run Stage 2: Regroup using pHash16 + colorHash thresholds.

    Two-stage clustering:
    1. Complete linkage with relaxed threshold -> kernel groups
    2. Single linkage extension with strict threshold -> extended groups
    """
    print("=" * 70)
    print("PIPELINE2 STAGE 2: REGROUP (pHash16 + colorHash)")
    print("=" * 70)
    print()

    # Load thresholds
    thresholds = load_thresholds()
    complete_boundary = thresholds.get("complete", [])
    single_boundary = thresholds.get("single", [])

    if not complete_boundary:
        print("ERROR: No 'complete' boundary defined in threshold_boundaries.json")
        return
    if not single_boundary:
        print("ERROR: No 'single' boundary defined in threshold_boundaries.json")
        return

    print(f"Complete linkage boundary: {len(complete_boundary)} cells")
    print(f"Single linkage boundary: {len(single_boundary)} cells")
    print()

    # Build predicates
    relaxed_pred = build_threshold_predicate(complete_boundary)
    strict_pred = build_threshold_predicate(single_boundary)

    with get_connection() as conn:
        # Clear existing stage 3 data
        print("Clearing existing Stage 2 data...")
        conn.execute("DROP TABLE IF EXISTS p2_groups")
        conn.execute("DROP TABLE IF EXISTS p2_unlinked_pairs")
        conn.commit()

        # Create output tables
        conn.execute("""
            CREATE TABLE p2_groups (
                photo_id TEXT NOT NULL PRIMARY KEY,
                group_id INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE p2_unlinked_pairs (
                photo_id_1 TEXT NOT NULL,
                photo_id_2 TEXT NOT NULL,
                phash16_dist INTEGER NOT NULL,
                colorhash_dist INTEGER NOT NULL,
                threshold TEXT NOT NULL,
                reason TEXT NOT NULL,
                PRIMARY KEY (photo_id_1, photo_id_2)
            )
        """)
        conn.commit()

        # Load kept photos with extended hashes
        print("Loading kept photos with extended hashes...")
        cursor = conn.execute("""
            SELECT kp.id, eh.phash_16, eh.colorhash
            FROM kept_photos kp
            JOIN extended_hashes eh ON kp.id = eh.photo_id
        """)
        photos = [dict(row) for row in cursor.fetchall()]
        print(f"Found {len(photos):,} photos with extended hashes")

        if len(photos) < 2:
            print("Need at least 2 photos to compare.")
            record_stage_completion(conn, "p2_2", 0, "insufficient photos")
            return

        # Build index
        id_to_idx = {p["id"]: i for i, p in enumerate(photos)}

        # Load precomputed pairs from photo_pairs table
        print("Loading precomputed pairs...")
        cursor = conn.execute("""
            SELECT photo_id_1, photo_id_2, phash16_dist, colorhash_dist
            FROM photo_pairs
        """)

        # Build distance dict and find edges for each threshold
        distances = {}  # (i, j) -> (phash16_dist, colorhash_dist)
        relaxed_edges = []  # Edges satisfying relaxed threshold
        strict_edges = []   # Edges satisfying strict threshold

        pair_count = 0
        for row in tqdm(cursor, desc="Processing pairs"):
            id1, id2 = row["photo_id_1"], row["photo_id_2"]
            if id1 not in id_to_idx or id2 not in id_to_idx:
                continue

            i, j = id_to_idx[id1], id_to_idx[id2]
            if i > j:
                i, j = j, i

            p16_dist = row["phash16_dist"]
            ch_dist = row["colorhash_dist"]
            distances[(i, j)] = (p16_dist, ch_dist)

            if relaxed_pred(p16_dist, ch_dist):
                relaxed_edges.append((i, j))
            if strict_pred(p16_dist, ch_dist):
                strict_edges.append((i, j))

            pair_count += 1

        print(f"\nProcessed {pair_count:,} pairs")
        print(f"Relaxed threshold edges: {len(relaxed_edges):,}")
        print(f"Strict threshold edges: {len(strict_edges):,}")

        if not relaxed_edges:
            print("No pairs satisfy relaxed threshold.")
            record_stage_completion(conn, "p2_2", 0, "no pairs")
            return

        # Stage 1: Find connected components using relaxed edges
        print("\nStage 1: Finding connected components (relaxed threshold)...")
        components = find_connected_components(relaxed_edges, len(photos))
        multi_photo_components = [c for c in components if len(c) > 1]
        print(f"Found {len(multi_photo_components):,} components with 2+ photos")

        # Stage 1: Complete linkage within components
        print("Stage 1: Complete linkage clustering...")

        def relaxed_should_merge(dist):
            return relaxed_pred(dist[0], dist[1])

        kernels = []
        singletons_from_clustering = set()

        for component in tqdm(multi_photo_components, desc="Clustering"):
            component_indices = list(component)
            clusters = complete_linkage_cluster(
                component_indices,
                distances,
                relaxed_should_merge,
                distance_key=lambda d: (d[0], d[1]),  # Sort by phash16, then colorhash
            )
            for cluster in clusters:
                if len(cluster) >= 2:
                    kernels.append(cluster)
                else:
                    singletons_from_clustering |= cluster

        # Collect all singletons (from clustering + never in a component)
        all_in_components = set()
        for c in components:
            all_in_components |= c
        never_in_component = set(range(len(photos))) - all_in_components
        all_singletons = singletons_from_clustering | never_in_component

        print(f"Complete linkage kernels: {len(kernels):,}")
        print(f"Singletons after complete linkage: {len(all_singletons):,}")

        # Stage 2: Single linkage extension
        print("\nStage 2: Single linkage extension (strict threshold)...")

        def strict_should_link(dist):
            return strict_pred(dist[0], dist[1])

        final_groups = single_linkage_extend(
            kernels,
            all_singletons,
            distances,
            strict_should_link,
        )

        # Filter to groups with 2+ members
        duplicate_groups = [g for g in final_groups if len(g) >= 2]
        print(f"Final groups (2+ members): {len(duplicate_groups):,}")

        # Find unlinked pairs
        print("\nFinding unlinked pairs...")

        # Build group membership
        idx_to_group = {}
        for group_id, group in enumerate(duplicate_groups):
            for idx in group:
                idx_to_group[idx] = group_id

        unlinked_pairs = []

        # Check relaxed pairs that ended up separated
        for i, j in relaxed_edges:
            group_i = idx_to_group.get(i)
            group_j = idx_to_group.get(j)

            if group_i != group_j:
                id1, id2 = photos[i]["id"], photos[j]["id"]
                if id1 > id2:
                    id1, id2 = id2, id1

                p16_dist, ch_dist = distances[(min(i, j), max(i, j))]

                if group_i is None and group_j is None:
                    reason = "both_singleton"
                elif group_i is None or group_j is None:
                    reason = "one_singleton"
                else:
                    reason = "different_groups"

                unlinked_pairs.append({
                    "photo_id_1": id1,
                    "photo_id_2": id2,
                    "phash16_dist": p16_dist,
                    "colorhash_dist": ch_dist,
                    "threshold": "relaxed",
                    "reason": reason,
                })

        print(f"Unlinked pairs (relaxed): {len(unlinked_pairs):,}")

        # Save groups
        print("\nSaving groups to database...")
        records = []
        for group_id, group in enumerate(duplicate_groups):
            for idx in group:
                records.append({
                    "photo_id": photos[idx]["id"],
                    "group_id": group_id,
                })

        if records:
            conn.executemany(
                "INSERT INTO p2_groups (photo_id, group_id) VALUES (:photo_id, :group_id)",
                records,
            )

        # Save unlinked pairs
        if unlinked_pairs:
            conn.executemany(
                """
                INSERT INTO p2_unlinked_pairs
                (photo_id_1, photo_id_2, phash16_dist, colorhash_dist, threshold, reason)
                VALUES (:photo_id_1, :photo_id_2, :phash16_dist, :colorhash_dist, :threshold, :reason)
                """,
                unlinked_pairs,
            )

        # Create index
        conn.execute("CREATE INDEX IF NOT EXISTS idx_p2_groups_group ON p2_groups(group_id)")
        conn.commit()

        # Stats
        total_in_groups = sum(len(g) for g in duplicate_groups)
        group_sizes = defaultdict(int)
        for g in duplicate_groups:
            group_sizes[len(g)] += 1

        unlinked_by_reason = defaultdict(int)
        for p in unlinked_pairs:
            unlinked_by_reason[p["reason"]] += 1

        record_stage_completion(
            conn, "p2_2",
            len(duplicate_groups),
            f"photos={total_in_groups}, kernels={len(kernels)}, unlinked={len(unlinked_pairs)}"
        )

    # Summary
    print()
    print("=" * 70)
    print("PIPELINE2 STAGE 2 COMPLETE")
    print("=" * 70)
    print()
    print(f"Complete linkage kernels:  {len(kernels):,}")
    print(f"Final groups (2+):         {len(duplicate_groups):,}")
    print(f"Photos in groups:          {total_in_groups:,}")
    print(f"Potential reductions:      {total_in_groups - len(duplicate_groups):,}")
    print()

    if group_sizes:
        print("Group size distribution:")
        for size in sorted(group_sizes.keys()):
            count = group_sizes[size]
            total = size * count
            print(f"  {size:3} photos/group: {count:6,} groups ({total:8,} photos)")

    print()
    print(f"Unlinked pairs (satisfy relaxed but separated): {len(unlinked_pairs):,}")
    for reason, count in sorted(unlinked_by_reason.items()):
        print(f"  {reason}: {count:,}")
    print()


if __name__ == "__main__":
    run_stage2()
