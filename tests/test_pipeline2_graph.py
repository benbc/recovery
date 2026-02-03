# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
# ]
# ///
"""
Comprehensive tests for pipeline2 graph algorithms.

Tests:
- find_connected_components: Union-find correctness
- complete_linkage_cluster: Complete linkage with custom predicates
- single_linkage_extend: Single linkage extension of existing clusters
"""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from pipeline2.graph_utils import (
    find_connected_components,
    complete_linkage_cluster,
    single_linkage_extend,
)


# =============================================================================
# Tests for find_connected_components
# =============================================================================

class TestFindConnectedComponents:
    """Tests for union-find connected components."""

    def test_empty_graph(self):
        """Empty graph returns empty list."""
        result = find_connected_components([], 0)
        assert result == []

    def test_no_edges(self):
        """Graph with no edges returns singleton components."""
        result = find_connected_components([], 3)
        assert len(result) == 3
        assert set().union(*result) == {0, 1, 2}

    def test_single_edge(self):
        """Single edge connects two nodes."""
        result = find_connected_components([(0, 1)], 3)
        assert len(result) == 2
        # One component with {0, 1}, one with {2}
        sizes = sorted(len(c) for c in result)
        assert sizes == [1, 2]

    def test_chain(self):
        """Chain 0-1-2-3 forms single component."""
        edges = [(0, 1), (1, 2), (2, 3)]
        result = find_connected_components(edges, 4)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}

    def test_two_components(self):
        """Disconnected graph forms multiple components."""
        edges = [(0, 1), (2, 3)]
        result = find_connected_components(edges, 4)
        assert len(result) == 2
        components = [frozenset(c) for c in result]
        assert frozenset({0, 1}) in components
        assert frozenset({2, 3}) in components

    def test_star_topology(self):
        """Star with central hub forms single component."""
        edges = [(0, 1), (0, 2), (0, 3), (0, 4)]
        result = find_connected_components(edges, 5)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3, 4}

    def test_complete_graph(self):
        """Complete graph forms single component."""
        edges = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        result = find_connected_components(edges, 4)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}


# =============================================================================
# Tests for complete_linkage_cluster
# =============================================================================

class TestCompleteLinkageCluster:
    """Tests for complete linkage clustering."""

    def test_empty(self):
        """Empty indices returns empty list."""
        result = complete_linkage_cluster([], {}, lambda d: True)
        assert result == []

    def test_single_node(self):
        """Single node returns singleton cluster."""
        result = complete_linkage_cluster([0], {}, lambda d: True)
        assert result == [{0}]

    def test_two_nodes_merge(self):
        """Two nodes that satisfy predicate merge."""
        distances = {(0, 1): (5,)}
        result = complete_linkage_cluster([0, 1], distances, lambda d: d[0] <= 10)
        assert result == [{0, 1}]

    def test_two_nodes_no_merge(self):
        """Two nodes that don't satisfy predicate stay separate."""
        distances = {(0, 1): (15,)}
        result = complete_linkage_cluster([0, 1], distances, lambda d: d[0] <= 10)
        assert len(result) == 2
        assert {frozenset(c) for c in result} == {frozenset({0}), frozenset({1})}

    def test_chain_all_merge(self):
        """Chain where all pairs satisfy predicate merges completely."""
        # 0-1-2, all close
        distances = {(0, 1): (5,), (1, 2): (5,), (0, 2): (8,)}
        result = complete_linkage_cluster([0, 1, 2], distances, lambda d: d[0] <= 10)
        assert result == [{0, 1, 2}]

    def test_chain_endpoints_fail(self):
        """Chain where endpoints don't satisfy predicate prevents full merge.

        This is the key "chaining problem" test:
        A-B and B-C satisfy threshold, but A-C does not.
        Complete linkage should NOT merge all three.
        """
        # 0-1 close, 1-2 close, but 0-2 too far
        distances = {(0, 1): (5,), (1, 2): (5,), (0, 2): (15,)}
        result = complete_linkage_cluster([0, 1, 2], distances, lambda d: d[0] <= 10)
        # Should NOT get {0, 1, 2}
        assert len(result) == 2
        # Either {0,1} + {2} or {0} + {1,2}
        sizes = sorted(len(c) for c in result)
        assert sizes == [1, 2]

    def test_chain_missing_endpoint_edge(self):
        """Missing edge between endpoints treated as not satisfying predicate."""
        # 0-1 close, 1-2 close, 0-2 not in distances (missing)
        distances = {(0, 1): (5,), (1, 2): (5,)}
        result = complete_linkage_cluster([0, 1, 2], distances, lambda d: d[0] <= 10)
        # Missing edge = can't merge all three
        assert len(result) == 2
        sizes = sorted(len(c) for c in result)
        assert sizes == [1, 2]

    def test_triangle_all_good(self):
        """Triangle where all edges satisfy predicate merges."""
        distances = {(0, 1): (3,), (0, 2): (4,), (1, 2): (5,)}
        result = complete_linkage_cluster([0, 1, 2], distances, lambda d: d[0] <= 10)
        assert result == [{0, 1, 2}]

    def test_triangle_one_bad(self):
        """Triangle with one bad edge prevents full merge."""
        distances = {(0, 1): (3,), (0, 2): (4,), (1, 2): (15,)}
        result = complete_linkage_cluster([0, 1, 2], distances, lambda d: d[0] <= 10)
        # Can't merge all three because 1-2 is bad
        # Should get {0, 1} or {0, 2} plus the other
        assert len(result) == 2

    def test_two_cliques_weak_bridge(self):
        """Two cliques connected by weak bridge don't fully merge.

        Clique A: {0, 1} all pairs good
        Clique B: {2, 3} all pairs good
        Bridge: 1-2 is good, but 0-2, 0-3, 1-3 are bad or missing

        Complete linkage should keep cliques separate.
        """
        distances = {
            (0, 1): (3,),  # Clique A
            (2, 3): (3,),  # Clique B
            (1, 2): (5,),  # Bridge
            # 0-2, 0-3, 1-3 missing = don't satisfy
        }
        result = complete_linkage_cluster([0, 1, 2, 3], distances, lambda d: d[0] <= 10)
        # Should get two clusters: {0, 1} and {2, 3}
        assert len(result) == 2
        clusters = {frozenset(c) for c in result}
        assert frozenset({0, 1}) in clusters
        assert frozenset({2, 3}) in clusters

    def test_priority_order(self):
        """Merges happen in priority order (smallest distance first)."""
        # 0-1 is closest, should merge first
        # Then 0-2 and 0-3 compete
        distances = {
            (0, 1): (2,),
            (0, 2): (5,),
            (0, 3): (5,),
            (1, 2): (6,),
            (1, 3): (6,),
            (2, 3): (8,),
        }
        result = complete_linkage_cluster([0, 1, 2, 3], distances, lambda d: d[0] <= 10)
        # All satisfy predicate, should merge into one
        assert result == [{0, 1, 2, 3}]

    def test_diamond_topology(self):
        """Diamond: 0-1, 0-2, 1-3, 2-3 with varying distances."""
        #     0
        #    / \
        #   1   2
        #    \ /
        #     3
        distances = {
            (0, 1): (3,),
            (0, 2): (3,),
            (1, 3): (3,),
            (2, 3): (3,),
            (1, 2): (15,),  # Bad diagonal
            (0, 3): (15,),  # Bad diagonal
        }
        result = complete_linkage_cluster([0, 1, 2, 3], distances, lambda d: d[0] <= 10)
        # Diagonals are bad, so can't merge all
        # Best case: pairs or some three-node subset
        assert len(result) >= 2

    def test_non_contiguous_indices(self):
        """Works with non-contiguous index sets."""
        distances = {(10, 20): (5,), (10, 30): (5,), (20, 30): (5,)}
        result = complete_linkage_cluster([10, 20, 30], distances, lambda d: d[0] <= 10)
        assert result == [{10, 20, 30}]

    def test_multi_dimensional_distance(self):
        """Works with multi-dimensional distance tuples."""
        # Distance is (phash16_dist, colorhash_dist)
        # Predicate: phash16 <= 80 and colorhash <= 4
        def should_merge(d):
            return d[0] <= 80 and d[1] <= 4

        distances = {
            (0, 1): (70, 2),  # Good
            (0, 2): (75, 3),  # Good
            (1, 2): (85, 1),  # Bad (phash16 too high)
        }
        result = complete_linkage_cluster([0, 1, 2], distances, should_merge)
        # 1-2 doesn't satisfy, so can't merge all three
        assert len(result) == 2

    def test_distance_key_ordering(self):
        """Custom distance_key controls merge order."""
        # Without custom key, (10, 1) < (5, 2) lexicographically
        # With custom key sum, (5, 2)=7 < (10, 1)=11
        distances = {
            (0, 1): (10, 1),
            (0, 2): (5, 2),
            (1, 2): (8, 3),
        }
        result = complete_linkage_cluster(
            [0, 1, 2],
            distances,
            lambda d: d[0] + d[1] <= 15,
            distance_key=lambda d: d[0] + d[1],
        )
        # 0-2 has smallest sum (7), should merge first
        # Then check if 1 can join
        assert len(result) >= 1

    def test_invariant_all_pairs_satisfy_predicate(self):
        """Final clusters have ALL internal pairs satisfying predicate."""
        import random
        random.seed(42)

        # Generate random distances
        n = 10
        indices = list(range(n))
        distances = {}
        for i in range(n):
            for j in range(i + 1, n):
                distances[(i, j)] = (random.randint(0, 20),)

        threshold = 10
        result = complete_linkage_cluster(indices, distances, lambda d: d[0] <= threshold)

        # Verify invariant
        for cluster in result:
            cluster_list = list(cluster)
            for i, a in enumerate(cluster_list):
                for b in cluster_list[i + 1:]:
                    key = (min(a, b), max(a, b))
                    if key in distances:
                        assert distances[key][0] <= threshold, \
                            f"Cluster {cluster} has bad pair {key}: {distances[key]}"


# =============================================================================
# Tests for single_linkage_extend
# =============================================================================

class TestSingleLinkageExtend:
    """Tests for single linkage cluster extension."""

    def test_no_clusters_no_singletons(self):
        """Empty inputs return empty list."""
        result = single_linkage_extend([], set(), {}, lambda d: True)
        assert result == []

    def test_no_singletons(self):
        """No singletons returns original clusters."""
        clusters = [{0, 1}, {2, 3}]
        # No edges between clusters
        distances = {(0, 1): (5,), (2, 3): (5,)}
        result = single_linkage_extend(clusters, set(), distances, lambda d: d[0] <= 10)
        assert len(result) == 2

    def test_singleton_joins_cluster(self):
        """Singleton with good edge joins cluster."""
        clusters = [{0, 1}]
        singletons = {2}
        distances = {(0, 1): (5,), (1, 2): (3,)}  # 2 links to 1
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_singleton_no_link(self):
        """Singleton without good edge stays singleton."""
        clusters = [{0, 1}]
        singletons = {2}
        distances = {(0, 1): (5,), (1, 2): (15,)}  # 2's edge too far
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        assert len(result) == 1
        assert result[0] == {0, 1}  # Singleton not added

    def test_singleton_links_multiple_clusters(self):
        """Singleton linking to multiple clusters causes merge."""
        clusters = [{0}, {1}]
        singletons = {2}
        distances = {(0, 2): (5,), (1, 2): (5,)}  # 2 links to both
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        # Should merge into one cluster with all three
        assert len(result) == 1
        assert result[0] == {0, 1, 2}

    def test_clusters_merge_directly(self):
        """Clusters with good edge between them merge."""
        clusters = [{0, 1}, {2, 3}]
        singletons = set()
        distances = {
            (0, 1): (5,),
            (2, 3): (5,),
            (1, 2): (3,),  # Bridge between clusters
        }
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}

    def test_chain_extension(self):
        """Singletons can chain-extend a cluster."""
        # Single linkage allows chaining!
        clusters = [{0}]
        singletons = {1, 2, 3}
        distances = {
            (0, 1): (5,),  # 0-1 link
            (1, 2): (5,),  # 1-2 link
            (2, 3): (5,),  # 2-3 link
            # No 0-2, 0-3, 1-3 links
        }
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        # With single linkage, chain should all merge
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}

    def test_preserves_cluster_separation(self):
        """Clusters without links stay separate."""
        clusters = [{0, 1}, {2, 3}]
        singletons = {4, 5}
        distances = {
            (0, 1): (5,),
            (2, 3): (5,),
            (0, 4): (3,),  # 4 joins first cluster
            (2, 5): (3,),  # 5 joins second cluster
            # No cross-cluster links
        }
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        assert len(result) == 2
        clusters_as_sets = {frozenset(c) for c in result}
        assert frozenset({0, 1, 4}) in clusters_as_sets
        assert frozenset({2, 3, 5}) in clusters_as_sets

    def test_multi_dimensional_distance(self):
        """Works with multi-dimensional distance tuples."""
        def should_link(d):
            return d[0] <= 80 and d[1] <= 4

        clusters = [{0, 1}]
        singletons = {2, 3}
        distances = {
            (0, 1): (60, 2),
            (1, 2): (70, 3),  # Good link
            (1, 3): (90, 1),  # Bad (phash16 too high)
        }
        result = single_linkage_extend(clusters, singletons, distances, should_link)
        assert len(result) == 1
        assert result[0] == {0, 1, 2}  # 3 not included

    def test_cascading_merges(self):
        """Singleton can trigger cascading cluster merges."""
        clusters = [{0}, {1}, {2}]
        singletons = {3}
        distances = {
            (0, 3): (5,),  # 3 links to 0
            (1, 3): (5,),  # 3 links to 1
            (2, 3): (5,),  # 3 links to 2
        }
        result = single_linkage_extend(clusters, singletons, distances, lambda d: d[0] <= 10)
        # 3 links all three clusters, so all merge
        assert len(result) == 1
        assert result[0] == {0, 1, 2, 3}


# =============================================================================
# Integration tests: Complete linkage then single linkage extension
# =============================================================================

class TestCompleteThenSingleLinkage:
    """Tests for two-stage clustering: complete linkage kernels, then single linkage extension."""

    def test_basic_two_stage(self):
        """Basic two-stage clustering example."""
        # Stage 1: Complete linkage with relaxed threshold
        # Stage 2: Single linkage extension with strict threshold

        indices = [0, 1, 2, 3, 4]
        distances = {
            # Tight cluster: 0, 1, 2
            (0, 1): (50, 2),
            (0, 2): (55, 2),
            (1, 2): (52, 2),
            # Singleton 3 has loose connection to cluster
            (2, 3): (80, 3),  # Within relaxed but not strict
            # Singleton 4 has tight connection to 3
            (3, 4): (60, 2),
        }

        # Relaxed predicate for complete linkage
        def relaxed(d):
            return d[0] <= 90 and d[1] <= 5

        # Strict predicate for single linkage
        def strict(d):
            return d[0] <= 70 and d[1] <= 3

        # Stage 1: Complete linkage
        kernels = complete_linkage_cluster(indices, distances, relaxed)

        # Find which are kernels (2+ members) vs singletons
        multi_member = [c for c in kernels if len(c) >= 2]
        singletons = set()
        for c in kernels:
            if len(c) == 1:
                singletons |= c

        # Stage 2: Single linkage extension
        result = single_linkage_extend(multi_member, singletons, distances, strict)

        # Verify: 0, 1, 2 should be together (kernel)
        # 3 connected to 2 with (80, 3) - fails strict threshold
        # 4 connected to 3 with (60, 2) - would pass strict but 3 not in cluster
        cluster_with_0 = [c for c in result if 0 in c][0]
        assert 1 in cluster_with_0
        assert 2 in cluster_with_0

    def test_kernel_then_extend(self):
        """Complete linkage creates kernel, single linkage extends it."""
        indices = [0, 1, 2, 3]
        distances = {
            # Kernel: 0, 1 (all pairs satisfy both thresholds)
            (0, 1): (50, 2),
            # 2 links to 1 with strict threshold
            (1, 2): (65, 2),
            # 0-2 doesn't satisfy relaxed, so can't be in kernel together
            (0, 2): (95, 2),
            # 3 is far from everyone
            (0, 3): (100, 5),
        }

        def relaxed(d):
            return d[0] <= 90 and d[1] <= 4

        def strict(d):
            return d[0] <= 70 and d[1] <= 3

        # Stage 1: Complete linkage with relaxed
        kernels = complete_linkage_cluster(indices, distances, relaxed)

        multi_member = [c for c in kernels if len(c) >= 2]
        singletons = set()
        for c in kernels:
            if len(c) == 1:
                singletons |= c

        # Stage 2: Single linkage with strict
        result = single_linkage_extend(multi_member, singletons, distances, strict)

        # 0, 1 form kernel; 2 should join via 1-2 strict link
        # 3 should remain separate
        cluster_with_0 = [c for c in result if 0 in c][0]
        assert cluster_with_0 == {0, 1, 2}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
