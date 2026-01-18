#!/usr/bin/env python3
# /// script
# dependencies = ["tqdm"]
# ///
"""
Find duplicate photo groups using perceptual hash similarity.

This script identifies groups of photos that are likely duplicates (same photo
at different resolutions, thumbnails, etc.) based on perceptual hash hamming
distance. It uses a union-find algorithm to group connected duplicates.

For each group, it ranks photos by quality (resolution × confidence) to help
identify which version to keep.

Usage: Set HAMMING_THRESHOLD to control sensitivity (default: 12)
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

DB_PATH = Path("organized/photos.db")
HAMMING_THRESHOLD = 8  # Maximum hamming distance to consider duplicates

def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate hamming distance between two hex hash strings."""
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count('1')

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

    def get_groups(self):
        """Return dictionary mapping root to list of members."""
        groups = defaultdict(list)
        for x in self.parent:
            root = self.find(x)
            groups[root].append(x)
        return dict(groups)

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Load all photos with perceptual hashes
    print("Loading photos from database...")
    cursor = conn.execute("""
        SELECT id, perceptual_hash, width, height, file_size, confidence_score, original_path
        FROM photos
        WHERE perceptual_hash IS NOT NULL
        AND is_non_photo = 0
        ORDER BY id
    """)

    photos = cursor.fetchall()
    print(f"Loaded {len(photos):,} photos with perceptual hashes\n")

    if len(photos) < 2:
        print("Need at least 2 photos to compare")
        sys.exit(0)

    # Build index by hash for faster lookups
    print("Building hash index...")
    hash_to_photos = defaultdict(list)
    for i, (photo_id, phash, w, h, size, conf, path) in enumerate(photos):
        hash_to_photos[phash].append(i)

    print(f"Found {len(hash_to_photos):,} unique hashes")
    print(f"Average photos per hash: {len(photos) / len(hash_to_photos):.2f}\n")

    # Find all duplicate pairs using union-find
    print(f"Finding duplicate pairs (threshold ≤ {HAMMING_THRESHOLD})...")
    uf = UnionFind()
    duplicate_pairs = 0

    # Compare all pairs
    for i in tqdm(range(len(photos)), desc="Comparing", unit="photos"):
        id1, hash1, w1, h1, size1, conf1, path1 = photos[i]

        for j in range(i + 1, len(photos)):
            id2, hash2, w2, h2, size2, conf2, path2 = photos[j]

            # Calculate hamming distance
            dist = hamming_distance(hash1, hash2)

            if dist <= HAMMING_THRESHOLD:
                uf.union(i, i)  # Ensure both are in the structure
                uf.union(j, j)
                uf.union(i, j)
                duplicate_pairs += 1

    print(f"\nFound {duplicate_pairs:,} duplicate pairs")

    # Get groups
    print("\nGrouping duplicates...")
    groups = uf.get_groups()

    # Filter to only groups with 2+ members
    duplicate_groups = {root: members for root, members in groups.items() if len(members) > 1}

    print(f"Found {len(duplicate_groups):,} duplicate groups")

    if not duplicate_groups:
        print("\nNo duplicate groups found!")
        conn.close()
        return

    # Analyze groups
    print(f"\n{'='*70}")
    print("DUPLICATE GROUP ANALYSIS")
    print(f"{'='*70}\n")

    # Create duplicate_groups table
    conn.execute("DROP TABLE IF EXISTS duplicate_groups")
    conn.execute("""
        CREATE TABLE duplicate_groups (
            photo_id TEXT,
            group_id INTEGER,
            group_size INTEGER,
            rank_in_group INTEGER,
            quality_score INTEGER,
            width INTEGER,
            height INTEGER,
            file_size INTEGER,
            confidence_score INTEGER,
            is_suggested_keeper BOOLEAN,
            FOREIGN KEY (photo_id) REFERENCES photos(id)
        )
    """)

    group_sizes = defaultdict(int)
    total_photos_in_groups = 0

    for group_id, (root, members) in enumerate(duplicate_groups.items()):
        # Get full photo info for group members
        group_photos = []
        for idx in members:
            photo_id, phash, w, h, size, conf, path = photos[idx]
            quality = (w * h) * (conf / 100)  # Resolution weighted by confidence
            group_photos.append({
                'id': photo_id,
                'width': w,
                'height': h,
                'file_size': size,
                'confidence': conf,
                'quality': quality,
                'path': path,
            })

        # Sort by quality (highest first)
        group_photos.sort(key=lambda x: x['quality'], reverse=True)

        # Insert into database
        for rank, photo in enumerate(group_photos):
            conn.execute("""
                INSERT INTO duplicate_groups
                (photo_id, group_id, group_size, rank_in_group, quality_score,
                 width, height, file_size, confidence_score, is_suggested_keeper)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                photo['id'],
                group_id,
                len(group_photos),
                rank + 1,  # 1-indexed rank
                int(photo['quality']),
                photo['width'],
                photo['height'],
                photo['file_size'],
                photo['confidence'],
                1 if rank == 0 else 0,  # Top quality = keeper
            ))

        group_sizes[len(group_photos)] += 1
        total_photos_in_groups += len(group_photos)

    conn.commit()

    # Print statistics
    print("Group size distribution:")
    for size in sorted(group_sizes.keys()):
        count = group_sizes[size]
        photos_count = size * count
        print(f"  {size:3} photos per group: {count:6,} groups ({photos_count:8,} photos)")

    print(f"\n{'='*70}")
    print(f"Total photos in duplicate groups:  {total_photos_in_groups:8,}")
    print(f"Total duplicate groups:             {len(duplicate_groups):8,}")
    print(f"Photos that could be rejected:      {total_photos_in_groups - len(duplicate_groups):8,}")
    print(f"                                     ({(total_photos_in_groups - len(duplicate_groups)) * 100 / len(photos):.1f}% of collection)")
    print(f"{'='*70}\n")

    # Show some example groups
    print("Example duplicate groups (largest first):\n")

    cursor = conn.execute("""
        SELECT group_id, group_size
        FROM duplicate_groups
        GROUP BY group_id
        ORDER BY group_size DESC
        LIMIT 10
    """)

    for group_id, group_size in cursor:
        print(f"\nGroup {group_id} ({group_size} photos):")

        cursor2 = conn.execute("""
            SELECT dg.rank_in_group, dg.width, dg.height, dg.quality_score,
                   dg.is_suggested_keeper, p.original_path
            FROM duplicate_groups dg
            JOIN photos p ON dg.photo_id = p.id
            WHERE dg.group_id = ?
            ORDER BY dg.rank_in_group
            LIMIT 5
        """, (group_id,))

        for rank, w, h, quality, keeper, path in cursor2:
            keeper_mark = " [KEEP]" if keeper else ""
            path_short = Path(path).name
            print(f"  #{rank}: {w:4}×{h:4} (quality={quality:10,}) {path_short}{keeper_mark}")

    print(f"\n{'='*70}")
    print("Results saved to duplicate_groups table")
    print("Next: Use review UI to confirm rejections")
    print(f"{'='*70}\n")

    conn.close()

if __name__ == "__main__":
    main()
