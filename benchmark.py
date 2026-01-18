#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "pillow",
#   "imagehash",
# ]
# ///

import time
from pathlib import Path
from PIL import Image
import imagehash
import hashlib
import sqlite3

# Get sample photos
conn = sqlite3.connect('organized/photos.db')
cursor = conn.execute('SELECT original_path FROM photos WHERE confidence_score >= 70 LIMIT 20')
sample_paths = [row[0] for row in cursor.fetchall()]

print("Benchmarking operations on 20 sample photos...")
print("=" * 60)

# Benchmark: File hashing
start = time.time()
for path in sample_paths:
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    _ = sha256.hexdigest()
file_hash_time = time.time() - start

# Benchmark: Image open only
start = time.time()
for path in sample_paths:
    try:
        img = Image.open(path)
        _ = img.size
        img.close()
    except:
        pass
image_open_time = time.time() - start

# Benchmark: Perceptual hash (includes image open)
start = time.time()
for path in sample_paths:
    try:
        img = Image.open(path)
        _ = str(imagehash.phash(img))
        img.close()
    except:
        pass
phash_total_time = time.time() - start

n = len(sample_paths)
phash_overhead = phash_total_time - image_open_time

print(f"File hashing (SHA256):     {file_hash_time:.3f}s ({file_hash_time/n*1000:.1f}ms per image)")
print(f"Image open + dimensions:   {image_open_time:.3f}s ({image_open_time/n*1000:.1f}ms per image)")
print(f"Perceptual hashing total:  {phash_total_time:.3f}s ({phash_total_time/n*1000:.1f}ms per image)")
print()

phash_overhead_pct = (phash_overhead / phash_total_time) * 100 if phash_total_time > 0 else 0
print(f"Perceptual hash overhead:  {phash_overhead:.3f}s ({phash_overhead/n*1000:.1f}ms per image)")
print(f"Overhead percentage:       {phash_overhead_pct:.1f}% of image processing time")
print()

# Project for full RECOVERY run
total_images = 210096
current_rate = 9.2  # images/sec from red-drive test
current_time_hours = total_images / current_rate / 3600

if phash_overhead_pct > 5:
    speedup_factor = 1 / (1 - phash_overhead_pct/100)
    new_rate = current_rate * speedup_factor
    new_time_hours = total_images / new_rate / 3600
    time_saved_hours = current_time_hours - new_time_hours

    print("=== Impact on full RECOVERY run ===")
    print(f"Current estimated time:    {current_time_hours:.1f} hours @ {current_rate:.1f} images/sec")
    print(f"Without perceptual hash:   {new_time_hours:.1f} hours @ {new_rate:.1f} images/sec")
    print(f"Time saved:                {time_saved_hours:.1f} hours ({time_saved_hours*60:.0f} minutes)")
    print(f"Speedup:                   {speedup_factor:.2f}x")
    print()
    print("Trade-off:")
    print("  - Skip phash now: Faster initial scan, but need separate Phase 2")
    print("    pass later for deduplication (total time ~same)")
    print("  - Keep phash: Slower now, but ready for Phase 2 dedup immediately")
else:
    print("Perceptual hashing overhead is negligible (<5%)")
