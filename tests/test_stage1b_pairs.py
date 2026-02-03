# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "tqdm",
# ]
# ///
"""
Unit tests for stage1b_pairs.py.

Tests the pair indexing function that converts between linear pair indices
and (i, j) coordinates.
"""

import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from pipeline2.stage1b_pairs import _pair_index_to_ij


class TestPairIndexToIJ:
    """Tests for the _pair_index_to_ij function."""

    def test_first_pair(self):
        """k=0 should give (0, 1) for any n >= 2."""
        for n in [2, 4, 10, 100, 1000]:
            i, j = _pair_index_to_ij(0, n)
            assert (i, j) == (0, 1), f"n={n}: expected (0,1), got ({i},{j})"

    def test_small_n_exhaustive(self):
        """For small n, verify all pairs are generated correctly."""
        n = 5
        # Expected pairs in order:
        # (0,1), (0,2), (0,3), (0,4), (1,2), (1,3), (1,4), (2,3), (2,4), (3,4)
        expected = [
            (0, 1), (0, 2), (0, 3), (0, 4),
            (1, 2), (1, 3), (1, 4),
            (2, 3), (2, 4),
            (3, 4),
        ]
        total_pairs = n * (n - 1) // 2
        assert len(expected) == total_pairs

        for k, (expected_i, expected_j) in enumerate(expected):
            i, j = _pair_index_to_ij(k, n)
            assert (i, j) == (expected_i, expected_j), (
                f"k={k}: expected ({expected_i},{expected_j}), got ({i},{j})"
            )

    def test_valid_pair_constraint(self):
        """All results should satisfy i < j."""
        for n in [10, 100, 1000]:
            total_pairs = n * (n - 1) // 2
            # Test first, last, and some middle values
            test_ks = [0, 1, n - 2, n - 1, n, total_pairs // 2, total_pairs - 1]
            for k in test_ks:
                if k < total_pairs:
                    i, j = _pair_index_to_ij(k, n)
                    assert i < j, f"n={n}, k={k}: got i={i} >= j={j}"
                    assert i >= 0, f"n={n}, k={k}: got negative i={i}"
                    assert j < n, f"n={n}, k={k}: got j={j} >= n={n}"

    def test_boundary_i_equals_0_to_1(self):
        """Test the boundary between i=0 and i=1.

        For i=0, k ranges from 0 to n-2.
        For i=1, k starts at n-1.
        """
        for n in [10, 100, 1000, 12836]:
            # Last pair with i=0
            k_last_i0 = n - 2
            i, j = _pair_index_to_ij(k_last_i0, n)
            assert i == 0, f"n={n}, k={k_last_i0}: expected i=0, got i={i}"
            assert j == n - 1, f"n={n}, k={k_last_i0}: expected j={n-1}, got j={j}"

            # First pair with i=1
            k_first_i1 = n - 1
            i, j = _pair_index_to_ij(k_first_i1, n)
            assert i == 1, f"n={n}, k={k_first_i1}: expected i=1, got i={i}"
            assert j == 2, f"n={n}, k={k_first_i1}: expected j=2, got j={j}"

    def test_boundary_general_transitions(self):
        """Test boundaries between different i values.

        For a given i, k_min(i) = i * (2n - i - 1) // 2
        """
        n = 100
        for i_val in range(min(10, n - 1)):
            # Compute k_min for this i
            k_min = i_val * (2 * n - i_val - 1) // 2

            # k_min should give (i_val, i_val + 1)
            i, j = _pair_index_to_ij(k_min, n)
            assert i == i_val, (
                f"n={n}, k={k_min} (k_min for i={i_val}): expected i={i_val}, got i={i}"
            )
            assert j == i_val + 1, (
                f"n={n}, k={k_min} (k_min for i={i_val}): expected j={i_val+1}, got j={j}"
            )

            # k_min - 1 should have i = i_val - 1 (if i_val > 0)
            if i_val > 0:
                i, j = _pair_index_to_ij(k_min - 1, n)
                assert i == i_val - 1, (
                    f"n={n}, k={k_min - 1} (before k_min for i={i_val}): "
                    f"expected i={i_val - 1}, got i={i}"
                )

    def test_last_pair(self):
        """The last pair should be (n-2, n-1)."""
        for n in [4, 10, 100, 1000, 12836]:
            total_pairs = n * (n - 1) // 2
            k_last = total_pairs - 1
            i, j = _pair_index_to_ij(k_last, n)
            assert (i, j) == (n - 2, n - 1), (
                f"n={n}, k={k_last}: expected ({n-2},{n-1}), got ({i},{j})"
            )

    def test_roundtrip_consistency(self):
        """Forward formula followed by inverse should give back original (i, j)."""
        def ij_to_pair_index(i: int, j: int, n: int) -> int:
            """Forward formula: convert (i, j) to linear index k."""
            return i * (2 * n - i - 1) // 2 + (j - i - 1)

        for n in [10, 100, 1000]:
            # Test a sampling of pairs
            test_pairs = [
                (0, 1), (0, n - 1),
                (1, 2), (1, n - 1),
                (n // 2, n // 2 + 1), (n // 2, n - 1),
                (n - 2, n - 1),
            ]
            for i_orig, j_orig in test_pairs:
                k = ij_to_pair_index(i_orig, j_orig, n)
                i, j = _pair_index_to_ij(k, n)
                assert (i, j) == (i_orig, j_orig), (
                    f"n={n}, original ({i_orig},{j_orig}) -> k={k} -> ({i},{j})"
                )

    def test_production_n_value(self):
        """Test with the actual production value of n=12836."""
        n = 12836
        total_pairs = n * (n - 1) // 2  # 82,360,930

        # Test critical boundaries
        test_cases = [
            (0, (0, 1)),
            (n - 2, (0, n - 1)),  # k=12834 -> (0, 12835)
            (n - 1, (1, 2)),       # k=12835 -> (1, 2)
            (total_pairs - 1, (n - 2, n - 1)),
        ]

        for k, expected in test_cases:
            i, j = _pair_index_to_ij(k, n)
            assert (i, j) == expected, (
                f"n={n}, k={k}: expected {expected}, got ({i},{j})"
            )

    def test_no_self_pairs(self):
        """No pair should have i == j (comparing photo with itself)."""
        for n in [10, 100, 1000, 12836]:
            total_pairs = n * (n - 1) // 2
            # Sample various k values including boundaries
            step = max(1, total_pairs // 1000)
            for k in range(0, total_pairs, step):
                i, j = _pair_index_to_ij(k, n)
                assert i != j, f"n={n}, k={k}: got self-pair ({i},{i})"


class TestPairIndexCoverage:
    """Tests that verify complete coverage of all pairs."""

    def test_all_pairs_generated_small_n(self):
        """For small n, verify every (i, j) pair is generated exactly once."""
        n = 20
        total_pairs = n * (n - 1) // 2

        seen_pairs = set()
        for k in range(total_pairs):
            i, j = _pair_index_to_ij(k, n)
            pair = (i, j)
            assert pair not in seen_pairs, f"Duplicate pair {pair} at k={k}"
            seen_pairs.add(pair)

        # Verify we got all expected pairs
        expected_pairs = {(i, j) for i in range(n) for j in range(i + 1, n)}
        assert seen_pairs == expected_pairs, (
            f"Missing pairs: {expected_pairs - seen_pairs}, "
            f"Extra pairs: {seen_pairs - expected_pairs}"
        )

    def test_monotonic_i_values(self):
        """As k increases, i should be non-decreasing."""
        n = 100
        total_pairs = n * (n - 1) // 2

        prev_i = -1
        for k in range(total_pairs):
            i, j = _pair_index_to_ij(k, n)
            assert i >= prev_i, f"k={k}: i={i} decreased from prev_i={prev_i}"
            prev_i = i


class TestPairIndexFloatingPointPrecision:
    """Tests to verify floating-point precision doesn't cause bugs."""

    def test_explicit_boundary_12834_12836(self):
        """Explicitly test the boundary case I analyzed by hand.

        For n=12836:
        - k=12834 should be (0, 12835) - the last pair with i=0
        - k=12835 should be (1, 2) - the first pair with i=1

        The formula uses floating-point sqrt which could have precision issues.
        """
        n = 12836

        # Verify k=12834 gives i=0 (not i=1)
        i, j = _pair_index_to_ij(12834, n)
        assert i == 0, f"k=12834: expected i=0, got i={i} (j={j})"
        assert j == 12835, f"k=12834: expected j=12835, got j={j}"

        # Verify k=12835 gives i=1
        i, j = _pair_index_to_ij(12835, n)
        assert i == 1, f"k=12835: expected i=1, got i={i}"
        assert j == 2, f"k=12835: expected j=2, got j={j}"

    def test_all_boundary_cases_large_n(self):
        """Test every boundary between i values for production n.

        At each boundary k_min(i), verify the transition is correct.
        """
        n = 12836

        for i_val in range(min(50, n - 1)):  # Test first 50 boundaries
            # k_min(i) is the first k value where the pair has first index = i
            k_min = i_val * (2 * n - i_val - 1) // 2

            # At k_min, we should get (i_val, i_val + 1)
            i, j = _pair_index_to_ij(k_min, n)
            assert i == i_val, (
                f"At k_min={k_min} for i_val={i_val}: expected i={i_val}, got i={i}"
            )

            # At k_min - 1 (if valid), we should get i = i_val - 1
            if k_min > 0:
                i_prev, j_prev = _pair_index_to_ij(k_min - 1, n)
                assert i_prev == i_val - 1, (
                    f"At k={k_min - 1} (just before k_min for i={i_val}): "
                    f"expected i={i_val - 1}, got i={i_prev}"
                )

    def test_no_invalid_pairs_sampling(self):
        """Sample many k values and ensure no invalid pairs are produced.

        An invalid pair would be one where i >= j or i == j.
        """
        n = 12836
        total_pairs = n * (n - 1) // 2

        # Sample 10000 k values spread across the entire range
        import random
        random.seed(42)  # Reproducible
        sample_ks = random.sample(range(total_pairs), min(10000, total_pairs))

        # Also include all boundary cases
        boundaries = []
        for i_val in range(n - 1):
            k_min = i_val * (2 * n - i_val - 1) // 2
            boundaries.extend([k_min, k_min - 1] if k_min > 0 else [k_min])

        for k in set(sample_ks + boundaries):
            if 0 <= k < total_pairs:
                i, j = _pair_index_to_ij(k, n)
                assert 0 <= i < j < n, (
                    f"k={k}: invalid pair ({i}, {j}), expected 0 <= i < j < {n}"
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
