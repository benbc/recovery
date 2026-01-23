#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = []
# ///
"""Compare two files byte-by-byte to find where they differ."""

import sys

path1 = sys.argv[1]
path2 = sys.argv[2]

with open(path1, 'rb') as f1, open(path2, 'rb') as f2:
    data1 = f1.read()
    data2 = f2.read()

print(f"File 1 size: {len(data1)} bytes")
print(f"File 2 size: {len(data2)} bytes")

if len(data1) != len(data2):
    print(f"Size difference: {abs(len(data1) - len(data2))} bytes")

# Find all differing positions
min_len = min(len(data1), len(data2))
diff_positions = [i for i in range(min_len) if data1[i] != data2[i]]

print(f"\nDiffering bytes: {len(diff_positions)}")

if diff_positions:
    print(f"First diff at byte: {diff_positions[0]}")
    print(f"Last diff at byte: {diff_positions[-1]}")

    # Show the first few differences
    print("\nFirst 10 differences:")
    for pos in diff_positions[:10]:
        print(f"  Byte {pos:6d}: 0x{data1[pos]:02x} vs 0x{data2[pos]:02x}")

    # Check if differences are contiguous or scattered
    if len(diff_positions) > 1:
        gaps = [diff_positions[i+1] - diff_positions[i] for i in range(len(diff_positions)-1)]
        if all(g == 1 for g in gaps):
            print(f"\nDifferences are CONTIGUOUS from byte {diff_positions[0]} to {diff_positions[-1]}")
        else:
            print(f"\nDifferences are SCATTERED across {len(diff_positions)} locations")

    # Show context around first difference
    start = max(0, diff_positions[0] - 20)
    end = min(min_len, diff_positions[0] + 40)
    print(f"\nContext around first diff (bytes {start}-{end}):")
    print(f"  File 1: {data1[start:end]}")
    print(f"  File 2: {data2[start:end]}")
