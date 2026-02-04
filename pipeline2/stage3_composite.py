"""
Stage 3: Create composite groups by joining primary and secondary (P2) groups.

Uses union-find to merge:
1. Primary groups (kept photos only)
2. P2 groups (which may link photos across primary groups or add new groupings)

Result: composite_groups table with the merged groupings.
"""

from collections import defaultdict

from tqdm import tqdm

from pipeline.database import get_connection, record_stage_completion


def run_stage3() -> None:
    """
    Run Stage 3: Create composite groups from primary + P2 groups.

    Uses union-find to merge groups based on shared photos.
    """
    print("=" * 70)
    print("PIPELINE2 STAGE 3: COMPOSITE GROUPS")
    print("=" * 70)
    print()

    with get_connection() as conn:
        # Clear existing composite groups
        print("Clearing existing composite groups...")
        conn.execute("DROP TABLE IF EXISTS composite_groups")
        conn.commit()

        # Create output table
        conn.execute("""
            CREATE TABLE composite_groups (
                photo_id TEXT NOT NULL PRIMARY KEY,
                group_id INTEGER NOT NULL
            )
        """)
        conn.commit()

        # Load kept photos
        print("Loading kept photos...")
        cursor = conn.execute("SELECT id FROM kept_photos")
        kept_photos = {row[0] for row in cursor.fetchall()}
        print(f"  Kept photos: {len(kept_photos):,}")

        # Create photo -> index mapping
        photo_list = list(kept_photos)
        photo_to_idx = {photo: i for i, photo in enumerate(photo_list)}
        n = len(photo_list)

        # Union-find structure
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

        # Load primary groups (kept photos only)
        print("Loading primary groups...")
        cursor = conn.execute("""
            SELECT dg.group_id, dg.photo_id
            FROM duplicate_groups dg
            JOIN kept_photos kp ON dg.photo_id = kp.id
            ORDER BY dg.group_id
        """)

        primary_edges = 0
        current_group = None
        group_photos = []

        for row in cursor.fetchall():
            group_id, photo_id = row
            if group_id != current_group:
                # Process previous group
                if len(group_photos) >= 2:
                    for i in range(1, len(group_photos)):
                        idx1 = photo_to_idx[group_photos[0]]
                        idx2 = photo_to_idx[group_photos[i]]
                        union(idx1, idx2)
                        primary_edges += 1
                current_group = group_id
                group_photos = []
            group_photos.append(photo_id)

        # Don't forget last group
        if len(group_photos) >= 2:
            for i in range(1, len(group_photos)):
                idx1 = photo_to_idx[group_photos[0]]
                idx2 = photo_to_idx[group_photos[i]]
                union(idx1, idx2)
                primary_edges += 1

        print(f"  Primary edges: {primary_edges:,}")

        # Load P2 groups
        print("Loading P2 groups...")
        cursor = conn.execute("""
            SELECT group_id, photo_id
            FROM p2_groups
            ORDER BY group_id
        """)

        p2_edges = 0
        current_group = None
        group_photos = []

        for row in cursor.fetchall():
            group_id, photo_id = row
            if photo_id not in photo_to_idx:
                continue  # Skip non-kept photos

            if group_id != current_group:
                # Process previous group
                if len(group_photos) >= 2:
                    for i in range(1, len(group_photos)):
                        idx1 = photo_to_idx[group_photos[0]]
                        idx2 = photo_to_idx[group_photos[i]]
                        union(idx1, idx2)
                        p2_edges += 1
                current_group = group_id
                group_photos = []
            group_photos.append(photo_id)

        # Don't forget last group
        if len(group_photos) >= 2:
            for i in range(1, len(group_photos)):
                idx1 = photo_to_idx[group_photos[0]]
                idx2 = photo_to_idx[group_photos[i]]
                union(idx1, idx2)
                p2_edges += 1

        print(f"  P2 edges: {p2_edges:,}")

        # Build composite groups from union-find
        print("Building composite groups...")
        components = defaultdict(list)
        for i in range(n):
            root = find(i)
            components[root].append(photo_list[i])

        # Filter to groups with 2+ photos
        composite_groups = [photos for photos in components.values() if len(photos) >= 2]
        print(f"  Composite groups (2+): {len(composite_groups):,}")

        # Assign sequential group IDs and save
        print("Saving composite groups...")
        records = []
        for group_id, photos in enumerate(composite_groups):
            for photo_id in photos:
                records.append({"photo_id": photo_id, "group_id": group_id})

        if records:
            conn.executemany(
                "INSERT INTO composite_groups (photo_id, group_id) VALUES (:photo_id, :group_id)",
                records,
            )

        conn.execute("CREATE INDEX idx_composite_group ON composite_groups(group_id)")
        conn.commit()

        # Statistics
        total_in_groups = sum(len(g) for g in composite_groups)
        singletons = len(kept_photos) - total_in_groups

        group_sizes = defaultdict(int)
        for g in composite_groups:
            group_sizes[len(g)] += 1

        # Compare with sources
        print("\nComparing with source groups...")

        # Count primary groups (kept photos only)
        cursor = conn.execute("""
            SELECT COUNT(DISTINCT dg.group_id)
            FROM duplicate_groups dg
            JOIN kept_photos kp ON dg.photo_id = kp.id
            GROUP BY dg.group_id
            HAVING COUNT(*) >= 2
        """)
        primary_group_count = len(cursor.fetchall())

        # Count P2 groups
        cursor = conn.execute("""
            SELECT COUNT(DISTINCT group_id)
            FROM p2_groups
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        """)
        p2_group_count = len(cursor.fetchall())

        record_stage_completion(
            conn, "p2_3",
            len(composite_groups),
            f"photos={total_in_groups}, from_primary={primary_group_count}, from_p2={p2_group_count}"
        )

    # Summary
    print()
    print("=" * 70)
    print("PIPELINE2 STAGE 3 COMPLETE")
    print("=" * 70)
    print()
    print(f"Input:")
    print(f"  Kept photos:              {len(kept_photos):,}")
    print(f"  Primary groups (kept):    {primary_group_count:,}")
    print(f"  P2 groups:                {p2_group_count:,}")
    print()
    print(f"Output:")
    print(f"  Composite groups:         {len(composite_groups):,}")
    print(f"  Photos in groups:         {total_in_groups:,}")
    print(f"  Singletons:               {singletons:,}")
    print(f"  Potential reductions:     {total_in_groups - len(composite_groups):,}")
    print()

    if group_sizes:
        print("Group size distribution:")
        for size in sorted(group_sizes.keys()):
            count = group_sizes[size]
            total = size * count
            print(f"  {size:3} photos/group: {count:6,} groups ({total:8,} photos)")
    print()


if __name__ == "__main__":
    run_stage3()
