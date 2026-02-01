"""
Stage 4: Group Duplicates

Cluster photos by perceptual hash similarity using complete-linkage clustering.
Uses priority-based merging to ensure consistent results regardless of input order.

Complete-linkage ensures that ALL pairs within a group satisfy should_group().
This avoids the "chaining problem" where union-find creates groups containing
dissimilar photos connected via intermediate similar photos.

Combined pHash/dHash thresholds (from visual sampling):
- pHash ≤10: group (reliable same scene)
- pHash 12: group if dHash <22, exclude if ≥22
- pHash 14: group if dHash ≤17, exclude if >17
- pHash >14: don't group

Output:
- `duplicate_groups` table (photo_id, group_id)
- `unlinked_pairs` table (pairs that satisfy should_group but ended up separated)
"""

import heapq
from collections import defaultdict

from tqdm import tqdm

from .config import (
    PHASH_SAFE_GROUP,
    PHASH_BORDERLINE_12,
    PHASH_BORDERLINE_14,
    DHASH_EXCLUDE_AT_12,
    DHASH_INCLUDE_AT_14,
)
from .database import (
    get_connection,
    get_photos_for_grouping,
    record_stage_completion,
)
from .utils.hashing import hamming_distance


def should_group(phash_dist: int, dhash_dist: int) -> bool:
    """
    Determine if two photos should be grouped based on combined pHash/dHash thresholds.

    Returns True if the photos should be in the same group.
    """
    if phash_dist <= PHASH_SAFE_GROUP:
        # pHash ≤10: reliable same scene
        return True
    elif phash_dist <= PHASH_BORDERLINE_12:
        # pHash 11-12: group unless dHash strongly disagrees
        return dhash_dist < DHASH_EXCLUDE_AT_12
    elif phash_dist <= PHASH_BORDERLINE_14:
        # pHash 13-14: only group if dHash confirms
        return dhash_dist <= DHASH_INCLUDE_AT_14
    else:
        # pHash >14: don't group
        return False


def find_connected_components(edges: list[tuple[int, int]], n: int) -> list[set[int]]:
    """
    Find connected components in a graph using union-find.

    Args:
        edges: List of (i, j) index pairs
        n: Total number of nodes

    Returns:
        List of sets, each set contains indices in a connected component
    """
    parent = list(range(n))
    rank = [0] * n

    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx == ry:
            return
        if rank[rx] < rank[ry]:
            parent[rx] = ry
        elif rank[rx] > rank[ry]:
            parent[ry] = rx
        else:
            parent[ry] = rx
            rank[rx] += 1

    for i, j in edges:
        union(i, j)

    components = defaultdict(set)
    for i in range(n):
        components[find(i)].add(i)

    return list(components.values())


def complete_linkage_cluster(
    indices: list[int],
    distances: dict[tuple[int, int], tuple[int, int]],
) -> list[set[int]]:
    """
    Perform priority-based complete-linkage clustering.

    Merges clusters in order of increasing distance (pHash first, then dHash).
    Only merges if ALL pairs between clusters satisfy should_group().

    Args:
        indices: List of photo indices to cluster
        distances: Dict mapping (i, j) -> (phash_dist, dhash_dist) for i < j

    Returns:
        List of sets, each set contains indices in a cluster
    """
    if len(indices) <= 1:
        return [set(indices)]

    # Map global indices to local indices for this component
    idx_to_local = {idx: i for i, idx in enumerate(indices)}
    local_to_idx = {i: idx for idx, i in idx_to_local.items()}
    n = len(indices)

    # Each photo starts in its own cluster
    # cluster_id -> set of local indices
    clusters = {i: {i} for i in range(n)}
    # local index -> cluster_id it belongs to
    point_to_cluster = {i: i for i in range(n)}

    # Build distance matrix for pairs within this component
    # Key: (local_i, local_j) where local_i < local_j
    # Value: (phash_dist, dhash_dist)
    local_distances = {}
    for (i, j), (pd, dd) in distances.items():
        if i in idx_to_local and j in idx_to_local:
            li, lj = idx_to_local[i], idx_to_local[j]
            if li > lj:
                li, lj = lj, li
            local_distances[(li, lj)] = (pd, dd)

    # Track complete-linkage distance between clusters
    # Key: (c1, c2) where c1 < c2
    # Value: (phash_dist, dhash_dist) - max over all pairs between clusters
    cluster_distances = {}

    # Initialize cluster distances from local distances
    for (li, lj), dist in local_distances.items():
        c1, c2 = point_to_cluster[li], point_to_cluster[lj]
        if c1 > c2:
            c1, c2 = c2, c1
        cluster_distances[(c1, c2)] = dist

    # Priority queue: (phash_dist, dhash_dist, cluster1, cluster2)
    # Lower distance = higher priority for merging
    heap = []
    for (c1, c2), (pd, dd) in cluster_distances.items():
        if should_group(pd, dd):
            heapq.heappush(heap, (pd, dd, c1, c2))

    # Merge clusters in priority order
    while heap:
        pd, dd, c1, c2 = heapq.heappop(heap)

        # Check if clusters still exist (may have been merged)
        if c1 not in clusters or c2 not in clusters:
            continue

        # Check if distance is still valid (may have been updated)
        key = (c1, c2) if c1 < c2 else (c2, c1)
        if key not in cluster_distances:
            continue
        current_dist = cluster_distances[key]
        if current_dist != (pd, dd):
            # Distance was updated, this entry is stale
            continue

        # Merge c2 into c1
        merged_points = clusters[c1] | clusters[c2]
        clusters[c1] = merged_points
        del clusters[c2]
        del cluster_distances[key]

        # Update point_to_cluster for merged points
        for p in clusters[c2] if c2 in clusters else merged_points:
            point_to_cluster[p] = c1

        # Update distances to other clusters
        for other_c in list(clusters.keys()):
            if other_c == c1:
                continue

            # Compute complete-linkage distance: max over all pairs
            max_dist = (0, 0)
            all_pairs_ok = True

            for p1 in merged_points:
                for p2 in clusters[other_c]:
                    li, lj = min(p1, p2), max(p1, p2)
                    if (li, lj) not in local_distances:
                        # Missing pair means it doesn't satisfy should_group
                        all_pairs_ok = False
                        break
                    pair_dist = local_distances[(li, lj)]
                    if not should_group(*pair_dist):
                        all_pairs_ok = False
                        break
                    if pair_dist > max_dist:
                        max_dist = pair_dist
                if not all_pairs_ok:
                    break

            # Update or remove cluster distance
            new_key = (min(c1, other_c), max(c1, other_c))
            old_key1 = (min(c1, other_c), max(c1, other_c))
            old_key2 = (min(c2, other_c), max(c2, other_c))

            # Remove old entries
            cluster_distances.pop(old_key1, None)
            cluster_distances.pop(old_key2, None)

            if all_pairs_ok:
                cluster_distances[new_key] = max_dist
                heapq.heappush(heap, (max_dist[0], max_dist[1], new_key[0], new_key[1]))

    # Convert back to global indices
    result = []
    for cluster_points in clusters.values():
        result.append({local_to_idx[p] for p in cluster_points})

    return result


def run_stage4(clear_existing: bool = False) -> None:
    """
    Run Stage 4: Group Duplicates.

    Uses complete-linkage clustering with priority-based merging.
    This ensures all pairs within a group satisfy should_group() and
    produces consistent results regardless of input order.

    Args:
        clear_existing: If True, clear existing groups before running
    """
    print("=" * 70)
    print("STAGE 4: GROUP DUPLICATES (Complete-Linkage)")
    print("=" * 70)
    print()
    print("Thresholds:")
    print(f"  pHash ≤{PHASH_SAFE_GROUP}: group")
    print(f"  pHash {PHASH_SAFE_GROUP+1}-{PHASH_BORDERLINE_12}: group if dHash <{DHASH_EXCLUDE_AT_12}")
    print(f"  pHash {PHASH_BORDERLINE_12+1}-{PHASH_BORDERLINE_14}: group if dHash ≤{DHASH_INCLUDE_AT_14}")
    print(f"  pHash >{PHASH_BORDERLINE_14}: don't group")
    print()

    with get_connection() as conn:
        if clear_existing:
            print("Clearing existing Stage 4 data...")
            conn.execute("DELETE FROM duplicate_groups")
            conn.execute("DROP TABLE IF EXISTS unlinked_pairs")
            conn.commit()

        # Create unlinked_pairs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS unlinked_pairs (
                photo_id_1 TEXT NOT NULL,
                photo_id_2 TEXT NOT NULL,
                phash_dist INTEGER NOT NULL,
                dhash_dist INTEGER NOT NULL,
                reason TEXT NOT NULL,
                PRIMARY KEY (photo_id_1, photo_id_2)
            )
        """)
        conn.commit()

        # Get photos with perceptual hashes
        print("Loading photos with perceptual hashes...")
        photos = get_photos_for_grouping(conn)
        print(f"Found {len(photos):,} photos for grouping")
        print()

        if len(photos) < 2:
            print("Need at least 2 photos to compare.")
            record_stage_completion(conn, "4", 0, "insufficient photos")
            return

        # Build index for photo lookup
        id_to_idx = {p["id"]: i for i, p in enumerate(photos)}

        # Find all pairs that satisfy should_group()
        print("Finding candidate pairs...")
        edges = []  # List of (i, j) indices for should_group pairs
        distances = {}  # (i, j) -> (phash_dist, dhash_dist) for i < j

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
            print("No duplicate pairs found.")
            record_stage_completion(conn, "4", 0, "no duplicates")
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
        duplicate_groups = [c for c in all_clusters if len(c) >= 2]
        print(f"Found {len(duplicate_groups):,} duplicate groups")

        # Find unlinked pairs: pairs that satisfy should_group() but are in different groups
        print("Finding unlinked pairs...")

        # Build index: photo_idx -> group_id (or None if singleton)
        idx_to_group = {}
        for group_id, cluster in enumerate(duplicate_groups):
            for idx in cluster:
                idx_to_group[idx] = group_id

        unlinked_pairs = []
        for (i, j), (pd, dd) in distances.items():
            group_i = idx_to_group.get(i)
            group_j = idx_to_group.get(j)

            if group_i != group_j:
                # Different groups (or one/both are singletons)
                id1, id2 = photos[i]["id"], photos[j]["id"]
                if id1 > id2:
                    id1, id2 = id2, id1

                if group_i is None and group_j is None:
                    reason = "both_singleton"
                elif group_i is None or group_j is None:
                    reason = "one_singleton"
                else:
                    reason = "different_groups"

                unlinked_pairs.append({
                    "photo_id_1": id1,
                    "photo_id_2": id2,
                    "phash_dist": pd,
                    "dhash_dist": dd,
                    "reason": reason,
                })

        print(f"Found {len(unlinked_pairs):,} unlinked pairs")

        # Insert groups into database
        print("Saving groups to database...")
        records = []
        for group_id, cluster in enumerate(duplicate_groups):
            for idx in cluster:
                records.append({
                    "photo_id": photos[idx]["id"],
                    "group_id": group_id,
                })

        if records:
            conn.executemany(
                """
                INSERT INTO duplicate_groups (photo_id, group_id)
                VALUES (:photo_id, :group_id)
                """,
                records,
            )

        # Insert unlinked pairs
        if unlinked_pairs:
            conn.executemany(
                """
                INSERT OR REPLACE INTO unlinked_pairs
                (photo_id_1, photo_id_2, phash_dist, dhash_dist, reason)
                VALUES (:photo_id_1, :photo_id_2, :phash_dist, :dhash_dist, :reason)
                """,
                unlinked_pairs,
            )

        conn.commit()

        # Calculate stats
        group_sizes = defaultdict(int)
        for cluster in duplicate_groups:
            group_sizes[len(cluster)] += 1

        total_in_groups = sum(len(c) for c in duplicate_groups)

        # Unlinked pair stats
        unlinked_by_reason = defaultdict(int)
        for p in unlinked_pairs:
            unlinked_by_reason[p["reason"]] += 1

        # Record completion
        record_stage_completion(
            conn, "4",
            len(duplicate_groups),
            f"photos_in_groups={total_in_groups}, pairs={len(edges)}, unlinked={len(unlinked_pairs)}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 4 COMPLETE")
    print("=" * 70)
    print()
    print(f"Duplicate groups:       {len(duplicate_groups):,}")
    print(f"Photos in groups:       {total_in_groups:,}")
    print(f"Potential reductions:   {total_in_groups - len(duplicate_groups):,}")
    print()

    if group_sizes:
        print("Group size distribution:")
        for size in sorted(group_sizes.keys()):
            count = group_sizes[size]
            total = size * count
            print(f"  {size:3} photos/group: {count:6,} groups ({total:8,} photos)")

    print()
    print(f"Unlinked pairs (satisfy should_group but separated): {len(unlinked_pairs):,}")
    for reason, count in sorted(unlinked_by_reason.items()):
        print(f"  {reason}: {count:,}")
    print()
    print("Use 'SELECT * FROM unlinked_pairs ORDER BY phash_dist, dhash_dist' to review")
    print()


if __name__ == "__main__":
    run_stage4()
