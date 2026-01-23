#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = ['pillow']
# ///
"""Compare two images to find differences."""

import hashlib
import sys
from PIL import Image

path1 = sys.argv[1]
path2 = sys.argv[2]

img1 = Image.open(path1)
img2 = Image.open(path2)

print(f"Image 1: {img1.size}, mode={img1.mode}")
print(f"Image 2: {img2.size}, mode={img2.mode}")

# Compare pixel data
pixels1 = list(img1.getdata())
pixels2 = list(img2.getdata())

if pixels1 == pixels2:
    print("\nPixel data is IDENTICAL")
else:
    diffs = sum(1 for p1, p2 in zip(pixels1, pixels2) if p1 != p2)
    print(f"\nPixel differences: {diffs} pixels differ out of {len(pixels1)}")

# Hash just the pixel data
img1_hash = hashlib.md5(img1.tobytes()).hexdigest()
img2_hash = hashlib.md5(img2.tobytes()).hexdigest()
print(f"\nPixel data hash 1: {img1_hash}")
print(f"Pixel data hash 2: {img2_hash}")

if img1_hash == img2_hash:
    print("\n=> Images are visually identical, difference is in metadata/encoding only")
