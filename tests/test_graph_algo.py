# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "tqdm",
#     "pillow",
#     "imagehash",
# ]
# ///
"""
Comprehensive unit tests for the graph algorithm code in stage4_group.py.

Tests for:
- find_connected_components: Union-find algorithm for finding connected components
- complete_linkage_cluster: Complete-linkage clustering within components
"""

import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from pipeline.stage4_group import (
    find_connected_components,
    complete_linkage_cluster,
    should_group,
)


class TestShouldGroup:
    """Tests for the should_group predicate function."""

    def test_phash_within_safe_threshold_always_groups(self):
        """pHash ≤10 should always group regardless of dHash."""
        for phash in range(11):  # 0-10
            for dhash in [0, 10, 20, 30, 50]:
                assert should_group(phash, dhash) is True, (
                    f"pHash={phash}, dHash={dhash} should group"
                )

    def test_phash_11_12_excludes_high_dhash(self):
        """pHash 11-12 should exclude when dHash ≥22."""
        for phash in [11, 12]:
            # Should group when dHash < 22
            for dhash in [0, 10, 21]:
                assert should_group(phash, dhash) is True, (
                    f"pHash={phash}, dHash={dhash} should group"
                )
            # Should NOT group when dHash ≥ 22
            for dhash in [22, 30, 50]:
                assert should_group(phash, dhash) is False, (
                    f"pHash={phash}, dHash={dhash} should NOT group"
                )

    def test_phash_13_14_requires_low_dhash(self):
        """pHash 13-14 should only group when dHash ≤17."""
        for phash in [13, 14]:
            # Should group when dHash ≤ 17
            for dhash in [0, 10, 17]:
                assert should_group(phash, dhash) is True, (
                    f"pHash={phash}, dHash={dhash} should group"
                )
            # Should NOT group when dHash > 17
            for dhash in [18, 25, 50]:
                assert should_group(phash, dhash) is False, (
                    f"pHash={phash}, dHash={dhash} should NOT group"
                )

    def test_phash_above_14_never_groups(self):
        """pHash >14 should never group regardless of dHash."""
        for phash in [15, 16, 20, 30, 50]:
            for dhash in [0, 5, 10, 17, 21]:
                assert should_group(phash, dhash) is False, (
                    f"pHash={phash}, dHash={dhash} should NOT group"
                )


class TestFindConnectedComponents:
    """Tests for the union-find connected components algorithm."""

    def test_empty_graph(self):
        """Empty graph returns singleton components."""
        result = find_connected_components([], 3)
        assert len(result) == 3
        assert all(len(c) == 1 for c in result)

    def test_single_edge(self):
        """Single edge connects two nodes."""
        result = find_connected_components([(0, 1)], 3)
        # Should have 2 components: {0,1} and {2}
        sizes = sorted([len(c) for c in result])
        assert sizes == [1, 2]

    def test_chain_connectivity(self):
        """Chain of edges forms single component."""
        # 0-1-2-3 (chain)
        edges = [(0, 1), (1, 2), (2, 3)]
        result = find_connected_components(edges, 4)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}

    def test_multiple_disconnected_components(self):
        """Disconnected subgraphs form separate components."""
        # Component 1: 0-1
        # Component 2: 2-3-4
        # Component 3: 5 (singleton)
        edges = [(0, 1), (2, 3), (3, 4)]
        result = find_connected_components(edges, 6)
        assert len(result) == 3
        component_sets = [frozenset(c) for c in result]
        assert frozenset({0, 1}) in component_sets
        assert frozenset({2, 3, 4}) in component_sets
        assert frozenset({5}) in component_sets

    def test_triangle(self):
        """Fully connected triangle is single component."""
        edges = [(0, 1), (1, 2), (0, 2)]
        result = find_connected_components(edges, 3)
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_edge_order_independence(self):
        """Result is independent of edge order."""
        edges1 = [(0, 1), (1, 2), (2, 3)]
        edges2 = [(2, 3), (0, 1), (1, 2)]
        edges3 = [(1, 2), (2, 3), (0, 1)]

        r1 = find_connected_components(edges1, 4)
        r2 = find_connected_components(edges2, 4)
        r3 = find_connected_components(edges3, 4)

        # All should produce single component with all nodes
        assert len(r1) == len(r2) == len(r3) == 1
        assert r1[0] == r2[0] == r3[0] == {0, 1, 2, 3}

    def test_star_topology(self):
        """Star topology (central node connected to all) forms single component."""
        # Node 0 is central, connected to 1, 2, 3, 4
        edges = [(0, 1), (0, 2), (0, 3), (0, 4)]
        result = find_connected_components(edges, 5)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3, 4}


class TestCompleteLinkageClustering:
    """Tests for the complete-linkage clustering algorithm."""

    def test_single_node(self):
        """Single node returns singleton cluster."""
        result = complete_linkage_cluster([0], {})
        assert len(result) == 1
        assert result[0] == {0}

    def test_empty_indices(self):
        """Empty indices returns empty result."""
        result = complete_linkage_cluster([], {})
        assert len(result) == 1
        assert result[0] == set()

    def test_two_nodes_should_group(self):
        """Two nodes that should group end up together."""
        indices = [0, 1]
        distances = {(0, 1): (5, 5)}  # Well within threshold
        result = complete_linkage_cluster(indices, distances)
        assert len(result) == 1
        assert result[0] == {0, 1}

    def test_two_nodes_should_not_group(self):
        """Two nodes that should NOT group stay separate."""
        indices = [0, 1]
        distances = {(0, 1): (20, 20)}  # Above threshold
        result = complete_linkage_cluster(indices, distances)
        # Both should be separate singletons
        assert len(result) == 2
        assert {frozenset(c) for c in result} == {frozenset({0}), frozenset({1})}

    def test_triangle_all_should_group(self):
        """Three nodes that all pairwise should_group form single cluster."""
        indices = [0, 1, 2]
        distances = {
            (0, 1): (5, 5),
            (0, 2): (5, 5),
            (1, 2): (5, 5),
        }
        result = complete_linkage_cluster(indices, distances)
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_chain_with_missing_link_stays_separate(self):
        """
        CRITICAL TEST: Chain A-B-C where A-C does NOT satisfy should_group.

        This is the "chaining problem" that complete-linkage should solve.
        A-B and B-C both satisfy should_group(), but A-C has high distance.

        Expected: A and C should NOT be in the same cluster.
        """
        indices = [0, 1, 2]
        # 0-1 and 1-2 satisfy should_group
        # 0-2 does NOT satisfy should_group (distance 20)
        distances = {
            (0, 1): (8, 8),   # should_group = True
            (1, 2): (8, 8),   # should_group = True
            # (0, 2) NOT in distances - distance would be 20 which doesn't satisfy should_group
        }
        result = complete_linkage_cluster(indices, distances)

        # Complete linkage should NOT merge A and C into same cluster
        # because there's no edge between them (they don't satisfy should_group)
        cluster_sets = [frozenset(c) for c in result]

        # Assert that 0 and 2 are NOT in the same cluster
        for cluster in result:
            assert not ({0, 2} <= cluster), (
                f"Nodes 0 and 2 should NOT be in the same cluster! Got: {result}"
            )

    def test_chain_with_distant_endpoints_explicit(self):
        """
        Explicit test: A-B-C where A-C has phash=20.

        Pass explicit distance for A-C that doesn't satisfy should_group.
        """
        indices = [0, 1, 2]
        distances = {
            (0, 1): (8, 8),   # should_group = True
            (0, 2): (20, 20), # should_group = False (phash > 14)
            (1, 2): (8, 8),   # should_group = True
        }
        result = complete_linkage_cluster(indices, distances)

        # Assert that 0 and 2 are NOT in the same cluster
        for cluster in result:
            assert not ({0, 2} <= cluster), (
                f"Nodes 0 and 2 should NOT be in the same cluster! Got: {result}"
            )

    def test_four_node_diamond_missing_diagonal(self):
        r"""
        Diamond shape: 0-1, 1-2, 2-3, 0-3 but NOT 0-2 or 1-3.

            0
           / \
          1   3
           \ /
            2

        Edges 0-1, 1-2, 2-3, 0-3 all satisfy should_group.
        Diagonals 0-2 and 1-3 do NOT satisfy should_group.
        """
        indices = [0, 1, 2, 3]
        distances = {
            (0, 1): (8, 8),   # should_group = True
            (0, 3): (8, 8),   # should_group = True
            (1, 2): (8, 8),   # should_group = True
            (2, 3): (8, 8),   # should_group = True
            # (0, 2) and (1, 3) NOT in distances - they don't satisfy should_group
        }
        result = complete_linkage_cluster(indices, distances)

        # Complete linkage should split this into clusters where
        # all pairs within each cluster satisfy should_group
        # 0 and 2 should NOT be together
        # 1 and 3 should NOT be together
        for cluster in result:
            assert not ({0, 2} <= cluster), (
                f"Nodes 0 and 2 should NOT be in the same cluster! Got: {result}"
            )
            assert not ({1, 3} <= cluster), (
                f"Nodes 1 and 3 should NOT be in the same cluster! Got: {result}"
            )

    def test_five_node_path_distant_ends(self):
        """
        Path 0-1-2-3-4 where consecutive pairs satisfy should_group
        but endpoints (0,4) do not.
        """
        indices = [0, 1, 2, 3, 4]
        distances = {
            (0, 1): (8, 8),
            (1, 2): (8, 8),
            (2, 3): (8, 8),
            (3, 4): (8, 8),
            # All non-adjacent pairs NOT in distances
        }
        result = complete_linkage_cluster(indices, distances)

        # In complete-linkage, we can only merge clusters if ALL pairs
        # between them satisfy should_group. Since (0,4), (0,3), (0,2),
        # (1,4), (1,3), (2,4) are missing, the final clusters should be limited.
        # At minimum, 0 and 4 should not be in same cluster.
        for cluster in result:
            assert not ({0, 4} <= cluster), (
                f"Nodes 0 and 4 should NOT be in the same cluster! Got: {result}"
            )

    def test_priority_merges_closest_first(self):
        """Verify that clusters with smaller distances merge first."""
        indices = [0, 1, 2]
        distances = {
            (0, 1): (2, 2),   # Closest pair
            (0, 2): (10, 10), # Farther
            (1, 2): (10, 10), # Farther
        }
        result = complete_linkage_cluster(indices, distances)
        # All three should merge since all satisfy should_group
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_borderline_threshold_12(self):
        """Test clustering at pHash=12 boundary."""
        indices = [0, 1, 2]
        distances = {
            (0, 1): (12, 21),  # should_group = True (dHash < 22)
            (0, 2): (12, 22),  # should_group = False (dHash >= 22)
            (1, 2): (12, 21),  # should_group = True
        }
        result = complete_linkage_cluster(indices, distances)

        # 0-2 should NOT be in same cluster
        for cluster in result:
            assert not ({0, 2} <= cluster), (
                f"Nodes 0 and 2 should NOT be in the same cluster! Got: {result}"
            )

    def test_borderline_threshold_14(self):
        """Test clustering at pHash=14 boundary."""
        indices = [0, 1, 2]
        distances = {
            (0, 1): (14, 17),  # should_group = True (dHash <= 17)
            (0, 2): (14, 18),  # should_group = False (dHash > 17)
            (1, 2): (14, 17),  # should_group = True
        }
        result = complete_linkage_cluster(indices, distances)

        # 0-2 should NOT be in same cluster
        for cluster in result:
            assert not ({0, 2} <= cluster), (
                f"Nodes 0 and 2 should NOT be in the same cluster! Got: {result}"
            )

    def test_global_to_local_index_mapping(self):
        """Test that non-contiguous global indices are handled correctly."""
        # Simulate a component with indices [5, 10, 15] instead of [0, 1, 2]
        indices = [5, 10, 15]
        distances = {
            (5, 10): (5, 5),   # should_group = True
            (5, 15): (5, 5),   # should_group = True
            (10, 15): (5, 5),  # should_group = True
        }
        result = complete_linkage_cluster(indices, distances)

        # All should merge
        assert len(result) == 1
        assert result[0] == {5, 10, 15}

    def test_global_indices_chain_missing_link(self):
        """Chain with non-contiguous indices and missing link."""
        indices = [100, 200, 300]
        distances = {
            (100, 200): (8, 8),  # should_group = True
            (200, 300): (8, 8),  # should_group = True
            # (100, 300) NOT in distances - doesn't satisfy should_group
        }
        result = complete_linkage_cluster(indices, distances)

        # 100 and 300 should NOT be in same cluster
        for cluster in result:
            assert not ({100, 300} <= cluster), (
                f"Nodes 100 and 300 should NOT be in the same cluster! Got: {result}"
            )

    def test_merge_order_independence(self):
        """Result should be independent of which equal-distance pair is processed first."""
        indices = [0, 1, 2, 3]
        # All pairs have same distance
        distances = {
            (0, 1): (5, 5),
            (0, 2): (5, 5),
            (0, 3): (5, 5),
            (1, 2): (5, 5),
            (1, 3): (5, 5),
            (2, 3): (5, 5),
        }
        # Should all merge since it's a complete graph with same distances
        result = complete_linkage_cluster(indices, distances)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}

    def test_large_component_with_hub(self):
        """
        Hub-and-spoke pattern where spokes don't connect to each other.

        Hub (0) connects to all spokes (1, 2, 3, 4)
        Spokes don't connect to each other.
        """
        indices = [0, 1, 2, 3, 4]
        distances = {
            (0, 1): (5, 5),
            (0, 2): (5, 5),
            (0, 3): (5, 5),
            (0, 4): (5, 5),
            # No edges between spokes - they don't satisfy should_group
        }
        result = complete_linkage_cluster(indices, distances)

        # Spokes should NOT all be in same cluster (only hub can merge with one spoke)
        # After first merge (0 with any spoke), the new cluster can't merge with
        # other spokes because the spoke-to-spoke pairs don't satisfy should_group
        cluster_sizes = sorted([len(c) for c in result])

        # At most 2 nodes should be together (hub + one spoke)
        assert max(cluster_sizes) <= 2, (
            f"Expected max cluster size 2, got {max(cluster_sizes)}. Clusters: {result}"
        )

    def test_two_triangles_connected_by_weak_link(self):
        """
        Two cliques connected by a single weak link.

        Triangle 1: 0-1-2 (all pairs satisfy should_group)
        Triangle 2: 3-4-5 (all pairs satisfy should_group)
        Weak link: 2-3 satisfies should_group
        Missing: 0-3, 0-4, 0-5, 1-3, 1-4, 1-5, 2-4, 2-5 don't satisfy should_group
        """
        indices = [0, 1, 2, 3, 4, 5]
        distances = {
            # Triangle 1
            (0, 1): (5, 5),
            (0, 2): (5, 5),
            (1, 2): (5, 5),
            # Triangle 2
            (3, 4): (5, 5),
            (3, 5): (5, 5),
            (4, 5): (5, 5),
            # Weak link
            (2, 3): (10, 10),
            # All other cross-triangle pairs NOT in distances
        }
        result = complete_linkage_cluster(indices, distances)

        # The two triangles should NOT fully merge
        # 0 should never be in same cluster as 4 or 5
        for cluster in result:
            assert not ({0, 4} <= cluster), f"0 and 4 shouldn't be together: {result}"
            assert not ({0, 5} <= cluster), f"0 and 5 shouldn't be together: {result}"
            assert not ({1, 4} <= cluster), f"1 and 4 shouldn't be together: {result}"
            assert not ({1, 5} <= cluster), f"1 and 5 shouldn't be together: {result}"


class TestCompleteLinkageEdgeCases:
    """Edge cases and regression tests for complete-linkage clustering."""

    def test_stale_heap_entry_handling(self):
        """
        Test that stale heap entries are correctly skipped.

        When a cluster is merged, old heap entries referencing it should be skipped.
        """
        indices = [0, 1, 2]
        distances = {
            (0, 1): (3, 3),   # Will be merged first
            (0, 2): (5, 5),
            (1, 2): (5, 5),
        }
        result = complete_linkage_cluster(indices, distances)
        # All should merge
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_cluster_distance_update_after_merge(self):
        """
        Test that cluster distances are correctly updated after merge.

        After merging {0,1}, the distance to {2} should be the MAX of
        dist(0,2) and dist(1,2).
        """
        indices = [0, 1, 2]
        distances = {
            (0, 1): (3, 3),   # Will be merged first
            (0, 2): (5, 5),   # Close to 2
            (1, 2): (14, 17), # At threshold boundary
        }
        result = complete_linkage_cluster(indices, distances)
        # Complete linkage uses max distance, max((5,5), (14,17)) = (14,17)
        # which still satisfies should_group, so all should merge
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_cluster_distance_update_blocks_merge(self):
        """
        Test that cluster distance update can block merging.

        After merging {0,1}, if max(dist(0,2), dist(1,2)) doesn't satisfy
        should_group, the merge with {2} should be blocked.
        """
        indices = [0, 1, 2]
        distances = {
            (0, 1): (3, 3),   # Will be merged first
            (0, 2): (5, 5),   # Close to 2
            (1, 2): (15, 20), # Does NOT satisfy should_group
        }
        result = complete_linkage_cluster(indices, distances)

        # After {0,1} merge, the max distance to {2} is (15, 20) which
        # doesn't satisfy should_group. So {2} should stay separate.
        cluster_sets = {frozenset(c) for c in result}
        assert frozenset({2}) in cluster_sets, (
            f"Node 2 should be in its own cluster. Got: {result}"
        )

    def test_missing_distance_means_no_group(self):
        """
        CRITICAL: A missing distance entry should mean the pair doesn't satisfy should_group.

        This tests the core assumption that only should_group=True pairs are in distances.
        """
        indices = [0, 1, 2]
        # Only include distances where should_group is True
        distances = {
            (0, 1): (5, 5),   # should_group = True
            # (0, 2) is missing - means it doesn't satisfy should_group
            (1, 2): (5, 5),   # should_group = True
        }
        result = complete_linkage_cluster(indices, distances)

        # 0 and 2 should NOT be in same cluster because (0,2) not in distances
        for cluster in result:
            if 0 in cluster and 2 in cluster:
                pytest.fail(
                    f"Nodes 0 and 2 should NOT be in the same cluster! "
                    f"Missing distance entry means they don't satisfy should_group. "
                    f"Got clusters: {result}"
                )

    def test_larger_chain_demonstrates_bug(self):
        """
        Longer chain to clearly demonstrate the chaining bug.

        0-1-2-3-4 where only adjacent pairs satisfy should_group.
        """
        indices = [0, 1, 2, 3, 4]
        # Only adjacent pairs have distances (satisfy should_group)
        distances = {
            (0, 1): (8, 8),
            (1, 2): (8, 8),
            (2, 3): (8, 8),
            (3, 4): (8, 8),
        }
        result = complete_linkage_cluster(indices, distances)

        # Verify that endpoints are not together
        for cluster in result:
            if 0 in cluster and 4 in cluster:
                pytest.fail(
                    f"Endpoints 0 and 4 should NOT be in the same cluster! "
                    f"Got clusters: {result}"
                )

        # Also verify other non-adjacent pairs
        for cluster in result:
            if 0 in cluster and 3 in cluster:
                pytest.fail(
                    f"Non-adjacent 0 and 3 should NOT be in the same cluster! "
                    f"Got clusters: {result}"
                )
            if 1 in cluster and 4 in cluster:
                pytest.fail(
                    f"Non-adjacent 1 and 4 should NOT be in the same cluster! "
                    f"Got clusters: {result}"
                )


class TestCompleteLinkageInvariant:
    """
    Tests that verify the fundamental invariant of complete-linkage clustering:
    ALL pairs within a cluster must satisfy should_group().
    """

    def _verify_complete_linkage_invariant(
        self,
        clusters: list[set[int]],
        distances: dict[tuple[int, int], tuple[int, int]],
    ) -> None:
        """
        Verify that ALL pairs within each cluster satisfy should_group().

        If a pair is not in distances, it means they don't satisfy should_group,
        so they should NOT be in the same cluster.
        """
        for cluster in clusters:
            if len(cluster) <= 1:
                continue

            cluster_list = list(cluster)
            for i in range(len(cluster_list)):
                for j in range(i + 1, len(cluster_list)):
                    a, b = cluster_list[i], cluster_list[j]
                    key = (min(a, b), max(a, b))

                    if key not in distances:
                        pytest.fail(
                            f"Pair {key} is in same cluster but NOT in distances dict! "
                            f"This means they don't satisfy should_group. "
                            f"Cluster: {cluster}"
                        )

                    phash_dist, dhash_dist = distances[key]
                    if not should_group(phash_dist, dhash_dist):
                        pytest.fail(
                            f"Pair {key} with distance ({phash_dist}, {dhash_dist}) "
                            f"does NOT satisfy should_group but is in same cluster! "
                            f"Cluster: {cluster}"
                        )

    def test_invariant_chain_pattern(self):
        """Verify invariant on chain where endpoints don't satisfy should_group."""
        indices = [0, 1, 2]
        distances = {
            (0, 1): (8, 8),
            (1, 2): (8, 8),
        }
        result = complete_linkage_cluster(indices, distances)
        self._verify_complete_linkage_invariant(result, distances)

    def test_invariant_diamond_pattern(self):
        """Verify invariant on diamond with missing diagonals."""
        indices = [0, 1, 2, 3]
        distances = {
            (0, 1): (8, 8),
            (0, 3): (8, 8),
            (1, 2): (8, 8),
            (2, 3): (8, 8),
        }
        result = complete_linkage_cluster(indices, distances)
        self._verify_complete_linkage_invariant(result, distances)

    def test_invariant_hub_and_spoke(self):
        """Verify invariant on hub-and-spoke topology."""
        indices = [0, 1, 2, 3, 4]
        distances = {
            (0, 1): (5, 5),
            (0, 2): (5, 5),
            (0, 3): (5, 5),
            (0, 4): (5, 5),
        }
        result = complete_linkage_cluster(indices, distances)
        self._verify_complete_linkage_invariant(result, distances)

    def test_invariant_two_cliques(self):
        """Verify invariant on two cliques with weak bridge."""
        indices = [0, 1, 2, 3, 4, 5]
        distances = {
            (0, 1): (5, 5),
            (0, 2): (5, 5),
            (1, 2): (5, 5),
            (3, 4): (5, 5),
            (3, 5): (5, 5),
            (4, 5): (5, 5),
            (2, 3): (10, 10),
        }
        result = complete_linkage_cluster(indices, distances)
        self._verify_complete_linkage_invariant(result, distances)

    def test_invariant_large_sparse_graph(self):
        """Verify invariant on larger sparse graph."""
        # 10 nodes in a ring where only adjacent pairs connect
        n = 10
        indices = list(range(n))
        distances = {}
        for i in range(n):
            j = (i + 1) % n
            distances[(min(i, j), max(i, j))] = (8, 8)

        result = complete_linkage_cluster(indices, distances)
        self._verify_complete_linkage_invariant(result, distances)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
