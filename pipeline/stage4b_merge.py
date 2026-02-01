"""
Stage 4b: Merge Bridge-Connected Groups

Merges groups that have many "bridges" (pairs satisfying should_group) between them.
These are groups that got split due to a few outlier photos but likely represent
the same scene.

Threshold: 50+ bridges between groups (conservative, zero false positives in testing)

This stage should be run after stage4_group.py.
"""

from collections import defaultdict

from .database import get_connection, record_stage_completion


def find_groups_to_merge(conn, min_bridges: int = 50) -> list[tuple[int, int, int]]:
    """
    Find pairs of groups with enough bridges to merge.

    Returns list of (group1, group2, bridge_count) tuples.
    """
    cursor = conn.execute("""
        WITH group_bridges AS (
            SELECT
                CASE WHEN dg1.group_id < dg2.group_id THEN dg1.group_id ELSE dg2.group_id END as g1,
                CASE WHEN dg1.group_id < dg2.group_id THEN dg2.group_id ELSE dg1.group_id END as g2,
                COUNT(*) as bridge_count
            FROM unlinked_pairs up
            JOIN duplicate_groups dg1 ON up.photo_id_1 = dg1.photo_id
            JOIN duplicate_groups dg2 ON up.photo_id_2 = dg2.photo_id
            WHERE up.reason = 'different_groups'
            GROUP BY g1, g2
            HAVING bridge_count >= ?
        )
        SELECT * FROM group_bridges ORDER BY bridge_count DESC
    """, (min_bridges,))

    return [(row['g1'], row['g2'], row['bridge_count']) for row in cursor]


def build_merge_map(pairs: list[tuple[int, int, int]]) -> dict[int, int]:
    """
    Build a map of group_id -> target_group_id using union-find.

    Handles transitive merges: if A merges with B and B merges with C,
    all three end up with the same target.
    """
    parent = {}

    def find(x):
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            # Keep the smaller group_id as canonical
            if px < py:
                parent[py] = px
            else:
                parent[px] = py

    for g1, g2, _ in pairs:
        union(g1, g2)

    # Build final map of groups that need updating
    groups_to_update = {}
    for g1, g2, _ in pairs:
        target1 = find(g1)
        target2 = find(g2)
        if g1 != target1:
            groups_to_update[g1] = target1
        if g2 != target2:
            groups_to_update[g2] = target2

    return groups_to_update


def run_stage4b(min_bridges: int = 50) -> None:
    """
    Run Stage 4b: Merge Bridge-Connected Groups.

    Args:
        min_bridges: Minimum number of bridges required to merge two groups
    """
    print("=" * 70)
    print("STAGE 4b: MERGE BRIDGE-CONNECTED GROUPS")
    print("=" * 70)
    print()
    print(f"Threshold: {min_bridges}+ bridges between groups")
    print()

    with get_connection() as conn:
        # Find groups to merge
        print("Finding group pairs with sufficient bridges...")
        pairs_to_merge = find_groups_to_merge(conn, min_bridges)
        print(f"Found {len(pairs_to_merge)} group pairs to merge")

        if not pairs_to_merge:
            print("No groups to merge.")
            record_stage_completion(conn, "4b", 0, "no merges needed")
            return

        # Show top pairs
        print("\nTop pairs by bridge count:")
        for g1, g2, count in pairs_to_merge[:10]:
            print(f"  Groups {g1} + {g2}: {count} bridges")
        if len(pairs_to_merge) > 10:
            print(f"  ... and {len(pairs_to_merge) - 10} more")

        # Build merge map
        print("\nBuilding merge map...")
        merge_map = build_merge_map(pairs_to_merge)
        print(f"Will update {len(merge_map)} groups")

        # Group by target for reporting
        by_target = defaultdict(list)
        for old, new in merge_map.items():
            by_target[new].append(old)

        print(f"Merging into {len(by_target)} target groups")

        # Perform merges
        print("\nPerforming merges...")
        for old_group, new_group in merge_map.items():
            conn.execute(
                "UPDATE duplicate_groups SET group_id = ? WHERE group_id = ?",
                (new_group, old_group)
            )
        conn.commit()

        # Clean up unlinked_pairs that are now in the same group
        print("Cleaning up unlinked_pairs...")
        cursor = conn.execute("""
            DELETE FROM unlinked_pairs
            WHERE EXISTS (
                SELECT 1 FROM duplicate_groups dg1, duplicate_groups dg2
                WHERE dg1.photo_id = unlinked_pairs.photo_id_1
                AND dg2.photo_id = unlinked_pairs.photo_id_2
                AND dg1.group_id = dg2.group_id
            )
        """)
        removed_pairs = cursor.rowcount
        print(f"Removed {removed_pairs} unlinked pairs now in same group")
        conn.commit()

        # Get new stats
        cursor = conn.execute("""
            SELECT COUNT(DISTINCT group_id) as groups, COUNT(*) as photos
            FROM duplicate_groups
        """)
        stats = cursor.fetchone()

        # Record completion
        record_stage_completion(
            conn, "4b",
            len(pairs_to_merge),
            f"merged={len(merge_map)}, targets={len(by_target)}, removed_pairs={removed_pairs}"
        )

    # Print summary
    print()
    print("=" * 70)
    print("STAGE 4b COMPLETE")
    print("=" * 70)
    print()
    print(f"Group pairs merged:    {len(pairs_to_merge)}")
    print(f"Groups updated:        {len(merge_map)}")
    print(f"Resulting groups:      {len(by_target)}")
    print(f"Unlinked pairs removed: {removed_pairs}")
    print()
    print(f"New totals: {stats['groups']:,} groups, {stats['photos']:,} photos")
    print()


if __name__ == "__main__":
    run_stage4b()
