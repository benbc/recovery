"""
Stage 4: Group Duplicates

Cluster photos by perceptual hash similarity using hamming distance.
Uses union-find algorithm for efficient grouping.
Store group membership only (ranking calculated on-the-fly when needed).

Output: `duplicate_groups` table (photo_id, group_id)
"""

from collections import defaultdict

from tqdm import tqdm

from .config import HAMMING_THRESHOLD
from .database import (
    get_connection,
    get_photos_for_grouping,
    record_stage_completion,
)
from .utils.hashing import hamming_distance


class UnionFind:
    """Union-Find data structure for grouping connected duplicates."""

    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        """Find root of x with path compression."""
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x

        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        """Union two sets by rank."""
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x == root_y:
            return

        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1

    def get_groups(self) -> dict[str, list[str]]:
        """Return dictionary mapping root to list of members."""
        groups = defaultdict(list)
        for x in self.parent:
            root = self.find(x)
            groups[root].append(x)
        return dict(groups)


def run_stage4(
    threshold: int = HAMMING_THRESHOLD,
    clear_existing: bool = False
) -> None:
    """
    Run Stage 4: Group Duplicates.

    Args:
        threshold: Hamming distance threshold for grouping (lower = stricter)
        clear_existing: If True, clear existing groups before running
    """
    print("=" * 70)
    print("STAGE 4: GROUP DUPLICATES")
    print("=" * 70)
    print()
    print(f"Hamming distance threshold: {threshold}")
    print()

    with get_connection() as conn:
        if clear_existing:
            print("Clearing existing Stage 4 data...")
            conn.execute("DELETE FROM duplicate_groups")
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

        # Build index for faster comparisons
        print("Finding duplicate pairs...")
        uf = UnionFind()
        duplicate_pairs = 0

        # Compare all pairs (O(n^2) but necessary for clustering)
        # This is the expensive part - could be optimized with LSH if needed
        for i in tqdm(range(len(photos)), desc="Comparing"):
            hash1 = photos[i]["perceptual_hash"]
            id1 = photos[i]["id"]

            # Ensure this photo is in the union-find
            uf.find(id1)

            for j in range(i + 1, len(photos)):
                hash2 = photos[j]["perceptual_hash"]
                id2 = photos[j]["id"]

                dist = hamming_distance(hash1, hash2)

                if dist <= threshold:
                    uf.union(id1, id2)
                    duplicate_pairs += 1

        print(f"\nFound {duplicate_pairs:,} duplicate pairs")

        # Get groups with 2+ members
        print("Building groups...")
        all_groups = uf.get_groups()
        duplicate_groups = {
            root: members
            for root, members in all_groups.items()
            if len(members) > 1
        }

        print(f"Found {len(duplicate_groups):,} duplicate groups")

        # Insert into database
        print("Saving groups to database...")
        records = []
        for group_id, (root, members) in enumerate(duplicate_groups.items()):
            for photo_id in members:
                records.append({
                    "photo_id": photo_id,
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
            conn.commit()

        # Calculate stats
        group_sizes = defaultdict(int)
        for root, members in duplicate_groups.items():
            group_sizes[len(members)] += 1

        total_in_groups = sum(len(m) for m in duplicate_groups.values())

        # Record completion
        record_stage_completion(
            conn, "4",
            len(duplicate_groups),
            f"photos_in_groups={total_in_groups}, pairs={duplicate_pairs}"
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


if __name__ == "__main__":
    run_stage4()
