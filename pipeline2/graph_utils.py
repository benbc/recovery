"""
Graph algorithms for clustering photos.

Provides:
- find_connected_components: Union-find for connected components
- complete_linkage_cluster: Complete linkage clustering with custom predicate
- single_linkage_extend: Extend existing clusters using single linkage
"""

import heapq
from collections import defaultdict
from typing import Callable


def find_connected_components(
    edges: list[tuple[int, int]],
    n: int,
) -> list[set[int]]:
    """
    Find connected components in a graph using union-find.

    Args:
        edges: List of (i, j) index pairs representing edges
        n: Total number of nodes (0 to n-1)

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
    distances: dict[tuple[int, int], tuple],
    should_merge: Callable[[tuple], bool],
    distance_key: Callable[[tuple], tuple] | None = None,
) -> list[set[int]]:
    """
    Perform priority-based complete-linkage clustering.

    Merges clusters in order of increasing distance.
    Only merges if ALL pairs between clusters satisfy should_merge().

    Args:
        indices: List of node indices to cluster
        distances: Dict mapping (i, j) -> distance_tuple for i < j
        should_merge: Predicate that takes a distance tuple and returns True if merge allowed
        distance_key: Optional function to extract sort key from distance tuple.
                      If None, the distance tuple itself is used for sorting.

    Returns:
        List of sets, each set contains indices in a cluster
    """
    if len(indices) <= 1:
        return [set(indices)] if indices else []

    if distance_key is None:
        distance_key = lambda d: d

    # Map global indices to local indices for this component
    idx_to_local = {idx: i for i, idx in enumerate(indices)}
    local_to_idx = {i: idx for idx, i in idx_to_local.items()}
    n = len(indices)

    # Each node starts in its own cluster
    clusters = {i: {i} for i in range(n)}
    point_to_cluster = {i: i for i in range(n)}

    # Build local distance matrix
    local_distances = {}
    for (i, j), dist in distances.items():
        if i in idx_to_local and j in idx_to_local:
            li, lj = idx_to_local[i], idx_to_local[j]
            if li > lj:
                li, lj = lj, li
            local_distances[(li, lj)] = dist

    # Track complete-linkage distance between clusters
    cluster_distances = {}

    # Initialize cluster distances
    for (li, lj), dist in local_distances.items():
        c1, c2 = point_to_cluster[li], point_to_cluster[lj]
        if c1 > c2:
            c1, c2 = c2, c1
        cluster_distances[(c1, c2)] = dist

    # Priority queue: (sort_key, cluster1, cluster2, distance)
    heap = []
    for (c1, c2), dist in cluster_distances.items():
        if should_merge(dist):
            heapq.heappush(heap, (distance_key(dist), c1, c2, dist))

    # Merge clusters in priority order
    while heap:
        _, c1, c2, dist = heapq.heappop(heap)

        # Check if clusters still exist
        if c1 not in clusters or c2 not in clusters:
            continue

        # Check if distance is still valid
        key = (c1, c2) if c1 < c2 else (c2, c1)
        if key not in cluster_distances:
            continue
        if cluster_distances[key] != dist:
            continue

        # Merge c2 into c1
        merged_points = clusters[c1] | clusters[c2]
        clusters[c1] = merged_points
        del clusters[c2]
        del cluster_distances[key]

        # Update point_to_cluster
        for p in merged_points:
            point_to_cluster[p] = c1

        # Update distances to other clusters
        for other_c in list(clusters.keys()):
            if other_c == c1:
                continue

            # Compute complete-linkage distance: max over all pairs
            max_dist = None
            all_pairs_ok = True

            for p1 in merged_points:
                for p2 in clusters[other_c]:
                    li, lj = min(p1, p2), max(p1, p2)
                    if (li, lj) not in local_distances:
                        all_pairs_ok = False
                        break
                    pair_dist = local_distances[(li, lj)]
                    if not should_merge(pair_dist):
                        all_pairs_ok = False
                        break
                    if max_dist is None or distance_key(pair_dist) > distance_key(max_dist):
                        max_dist = pair_dist
                if not all_pairs_ok:
                    break

            # Update cluster distance
            new_key = (min(c1, other_c), max(c1, other_c))
            old_key1 = (min(c1, other_c), max(c1, other_c))
            old_key2 = (min(c2, other_c), max(c2, other_c))

            cluster_distances.pop(old_key1, None)
            cluster_distances.pop(old_key2, None)

            if all_pairs_ok and max_dist is not None:
                cluster_distances[new_key] = max_dist
                heapq.heappush(heap, (distance_key(max_dist), new_key[0], new_key[1], max_dist))

    # Convert back to global indices
    return [{local_to_idx[p] for p in cluster_points} for cluster_points in clusters.values()]


def single_linkage_extend(
    clusters: list[set[int]],
    singletons: set[int],
    distances: dict[tuple[int, int], tuple],
    should_link: Callable[[tuple], bool],
) -> list[set[int]]:
    """
    Extend existing clusters using single linkage.

    For each singleton, if it has ANY edge to a cluster member that satisfies
    should_link(), add it to that cluster. If it links to multiple clusters,
    those clusters are merged.

    Clusters can also be merged if any pair between them satisfies should_link().

    Args:
        clusters: List of existing clusters (sets of indices)
        singletons: Set of singleton indices not in any cluster
        distances: Dict mapping (i, j) -> distance_tuple for i < j
        should_link: Predicate that takes distance tuple and returns True if link allowed

    Returns:
        List of extended/merged clusters (singletons that don't link remain singletons)
    """
    # Build cluster membership lookup
    point_to_cluster = {}
    cluster_list = [set(c) for c in clusters]  # Copy clusters

    for cluster_id, cluster in enumerate(cluster_list):
        for p in cluster:
            point_to_cluster[p] = cluster_id

    # Track which clusters have been merged (maps old_id -> new_id)
    def find_cluster(cluster_id):
        """Follow merge chain to find current cluster."""
        visited = set()
        while cluster_id in merged_into and cluster_id not in visited:
            visited.add(cluster_id)
            cluster_id = merged_into[cluster_id]
        return cluster_id

    merged_into = {}

    def merge_clusters(c1, c2):
        """Merge cluster c2 into c1."""
        c1 = find_cluster(c1)
        c2 = find_cluster(c2)
        if c1 == c2:
            return c1
        # Merge smaller into larger
        if len(cluster_list[c1]) < len(cluster_list[c2]):
            c1, c2 = c2, c1
        cluster_list[c1] |= cluster_list[c2]
        for p in cluster_list[c2]:
            point_to_cluster[p] = c1
        cluster_list[c2] = set()
        merged_into[c2] = c1
        return c1

    # First pass: check if any clusters should be merged directly
    cluster_ids = list(range(len(cluster_list)))
    for i, c1_id in enumerate(cluster_ids):
        for c2_id in cluster_ids[i+1:]:
            c1_id_curr = find_cluster(c1_id)
            c2_id_curr = find_cluster(c2_id)
            if c1_id_curr == c2_id_curr:
                continue

            # Check if any pair between clusters satisfies should_link
            for p1 in cluster_list[c1_id_curr]:
                for p2 in cluster_list[c2_id_curr]:
                    key = (min(p1, p2), max(p1, p2))
                    if key in distances and should_link(distances[key]):
                        merge_clusters(c1_id_curr, c2_id_curr)
                        break
                else:
                    continue
                break

    # Second pass: try to add singletons to clusters
    for singleton in singletons:
        linked_clusters = set()

        for cluster_id, cluster in enumerate(cluster_list):
            if not cluster:  # Skip merged-away clusters
                continue
            cluster_id = find_cluster(cluster_id)

            for member in cluster_list[cluster_id]:
                key = (min(singleton, member), max(singleton, member))
                if key in distances and should_link(distances[key]):
                    linked_clusters.add(cluster_id)
                    break

        if linked_clusters:
            # Add singleton to first cluster, merge others if multiple
            linked_list = list(linked_clusters)
            target_cluster = linked_list[0]
            cluster_list[target_cluster].add(singleton)
            point_to_cluster[singleton] = target_cluster

            for other_cluster in linked_list[1:]:
                merge_clusters(target_cluster, other_cluster)

    # Return non-empty clusters
    return [c for c in cluster_list if c]
