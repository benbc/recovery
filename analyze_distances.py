#!/usr/bin/env python3
# /// script
# dependencies = ["tqdm"]
# ///
"""
Analyze hamming distance distribution for all pairs of perceptual hashes.

This script compares ALL pairs of photos to find the natural threshold for
duplicate detection. By looking at the actual distance distribution and
breaking it down by context (thumbnails, size differences, etc.), we can
choose a safe threshold that captures real duplicates without false positives.

Performance: ~10M comparisons/sec = ~2 minutes for 62k photos (1.9B pairs)
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

DB_PATH = Path("organized/photos.db")

def hamming_distance(hash1: str, hash2: str) -> int:
    """
    Calculate hamming distance between two hex hash strings.
    Fast bitwise XOR + bit counting.
    """
    # Convert hex to int, XOR, count bits
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count('1')

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    # Load all photos with perceptual hashes
    print("Loading photos from database...")
    cursor = conn.execute("""
        SELECT id, perceptual_hash, width, height, file_size, original_path
        FROM photos
        WHERE perceptual_hash IS NOT NULL
        AND is_non_photo = 0
        ORDER BY id
    """)

    photos = cursor.fetchall()
    print(f"Loaded {len(photos):,} photos with perceptual hashes")

    if len(photos) < 2:
        print("Need at least 2 photos to compare")
        sys.exit(0)

    # Calculate total pairs
    total_pairs = len(photos) * (len(photos) - 1) // 2
    print(f"Total pairs to compare: {total_pairs:,} ({total_pairs/1e9:.2f} billion)")
    print(f"Estimated time: ~{total_pairs / 10_000_000 / 60:.1f} minutes at 10M pairs/sec\n")

    # Track distance histogram
    distance_counts = defaultdict(int)

    # Track context for interesting pairs (distance <= 20)
    interesting_pairs = []
    MAX_INTERESTING = 20

    print("Comparing all pairs...")
    comparisons = 0

    # Compare all pairs
    for i in tqdm(range(len(photos)), desc="Progress", unit="photos"):
        id1, hash1, w1, h1, size1, path1 = photos[i]

        for j in range(i + 1, len(photos)):
            id2, hash2, w2, h2, size2, path2 = photos[j]

            # Calculate hamming distance
            dist = hamming_distance(hash1, hash2)
            distance_counts[dist] += 1
            comparisons += 1

            # Track interesting pairs for context analysis
            if dist <= MAX_INTERESTING:
                # Calculate context flags
                both_thumbnails = '/Thumbnails/' in path1 and '/Thumbnails/' in path2
                either_thumbnail = '/Thumbnails/' in path1 or '/Thumbnails/' in path2

                # Size difference
                max_pixels = max(w1 * h1, w2 * h2)
                min_pixels = min(w1 * h1, w2 * h2)
                size_ratio = max_pixels / min_pixels if min_pixels > 0 else 1.0

                # Same filename base (ignoring path and extension)
                name1 = Path(path1).stem
                name2 = Path(path2).stem
                same_filename = name1 == name2

                # Same parent directory
                parent1 = Path(path1).parent
                parent2 = Path(path2).parent
                same_dir = parent1 == parent2

                interesting_pairs.append({
                    'distance': dist,
                    'both_thumbnails': both_thumbnails,
                    'either_thumbnail': either_thumbnail,
                    'size_ratio': size_ratio,
                    'same_filename': same_filename,
                    'same_dir': same_dir,
                })

    print(f"\n\nCompleted {comparisons:,} comparisons")

    # Print distance distribution
    print(f"\n{'='*70}")
    print("HAMMING DISTANCE DISTRIBUTION")
    print(f"{'='*70}\n")

    print(f"{'Distance':<12} {'Total Pairs':<15} {'Percentage':<12}")
    print(f"{'-'*12} {'-'*15} {'-'*12}")

    for dist in sorted(distance_counts.keys()):
        count = distance_counts[dist]
        pct = count * 100 / total_pairs
        print(f"{dist:<12} {count:<15,} {pct:>10.4f}%")

        # Stop showing after distance becomes very rare
        if dist > 30 and count < 10:
            remaining = sum(distance_counts[d] for d in distance_counts if d > dist)
            if remaining > 0:
                print(f"{'...':<12} {remaining:<15,} (distances > {dist})")
            break

    # Analyze context for interesting distances
    print(f"\n{'='*70}")
    print("CONTEXT ANALYSIS (Distance <= 20)")
    print(f"{'='*70}\n")

    if interesting_pairs:
        # Group by distance
        by_distance = defaultdict(list)
        for pair in interesting_pairs:
            by_distance[pair['distance']].append(pair)

        print(f"{'Dist':<6} {'Total':<10} {'Both Thumb':<12} {'Size >2x':<12} "
              f"{'Size >4x':<12} {'Same File':<12} {'Same Dir':<12}")
        print(f"{'-'*6} {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12}")

        for dist in sorted(by_distance.keys()):
            pairs = by_distance[dist]
            total = len(pairs)
            both_thumb = sum(1 for p in pairs if p['both_thumbnails'])
            size_2x = sum(1 for p in pairs if p['size_ratio'] > 2)
            size_4x = sum(1 for p in pairs if p['size_ratio'] > 4)
            same_file = sum(1 for p in pairs if p['same_filename'])
            same_dir = sum(1 for p in pairs if p['same_dir'])

            print(f"{dist:<6} {total:<10,} {both_thumb:<12,} {size_2x:<12,} "
                  f"{size_4x:<12,} {same_file:<12,} {same_dir:<12,}")

    # Recommendation
    print(f"\n{'='*70}")
    print("RECOMMENDATION")
    print(f"{'='*70}\n")

    # Find natural gap (where count drops significantly)
    drops = []
    for dist in sorted(distance_counts.keys())[1:]:
        if dist > 5 and dist < 20:
            prev_count = distance_counts.get(dist - 1, 0)
            curr_count = distance_counts[dist]
            if prev_count > 0:
                drop_ratio = curr_count / prev_count
                drops.append((dist, drop_ratio, prev_count, curr_count))

    if drops:
        # Find biggest drop
        biggest_drop = min(drops, key=lambda x: x[1])
        threshold = biggest_drop[0] - 1

        print(f"Natural cutoff appears around distance {threshold}")
        print(f"  - Steep drop from {biggest_drop[2]:,} pairs at distance {biggest_drop[0]-1}")
        print(f"    to {biggest_drop[3]:,} pairs at distance {biggest_drop[0]}")

        # Calculate stats for this threshold
        pairs_at_threshold = sum(distance_counts[d] for d in distance_counts if d <= threshold)
        print(f"\n  Using threshold {threshold}:")
        print(f"  - Would match {pairs_at_threshold:,} pairs")

        if interesting_pairs:
            matched = [p for p in interesting_pairs if p['distance'] <= threshold]
            if matched:
                thumb_pct = sum(1 for p in matched if p['either_thumbnail']) * 100 / len(matched)
                size_pct = sum(1 for p in matched if p['size_ratio'] > 2) * 100 / len(matched)
                print(f"  - {thumb_pct:.1f}% involve thumbnails")
                print(f"  - {size_pct:.1f}% have >2x size difference")
    else:
        print("No clear natural cutoff found in range 5-20")
        print("Consider manual threshold selection based on distribution above")

    print(f"\n{'='*70}\n")

    conn.close()

if __name__ == "__main__":
    main()
