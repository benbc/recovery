# Duplicate Detection Plan

## Overview

Identify and remove logical duplicates (thumbnails, resized versions) from the photo collection using perceptual hashing.

**Current status:** 85,710 photos processed
- High confidence: 51,282 (60%)
- Medium confidence: 23,751 (28%) - mostly thumbnails without EXIF
- Low confidence: 10,677 (12%)

**Goal:** Reduce dataset by identifying and removing thumbnails/resized versions while keeping best quality originals.

## Stage 1: Compute Perceptual Hashes (~6 hours)

**Script: `compute_phashes.py`**

**Purpose:** Add perceptual hash to all photos in database

**Algorithm:**
```python
# For each photo in database without perceptual_hash:
#   - Open image file from organized/images/
#   - Compute perceptual hash using imagehash.phash()
#   - UPDATE photos SET perceptual_hash = ? WHERE id = ?
#
# Features:
#   - Progress bar (tqdm)
#   - Batch commits every 1000 records
#   - Resumable (skips photos with existing perceptual_hash)
#   - Error handling (skip corrupt images, log errors)
```

**Run:** `./compute_phashes.py` (background, ~6 hours)

**Output:** Database with perceptual_hash column populated

---

## Stage 2: Analyze Hamming Distance Distribution (~6 minutes)

**Script: `analyze_distances.py`**

**Purpose:** Find natural threshold for duplicate detection by analyzing ALL pairwise distances

**Why all pairs:** Only 6 minutes for 3.7 billion comparisons (hamming distance is very fast)

**Algorithm:**
```python
# 1. Load all perceptual hashes from database
#    - ~85k hashes, 8 bytes each = 0.7 MB (trivial memory)
#
# 2. Compare ALL pairs (3.7 billion comparisons)
#    - Calculate hamming distance for each pair
#    - Only track pairs with distance ≤ 20
#    - Hamming distance: XOR + bit count (~10M comparisons/sec)
#
# 3. Build histogram of distances
#
# 4. Break down by context:
#    - Both in /Thumbnails/ vs one/neither
#    - Size difference (>2x, >4x, etc.)
#    - Same filename base
#    - Same parent directory
#
# 5. Output recommendation
```

**Example Output:**
```
Hamming Distance Distribution
==============================
Distance    Total Pairs    In Thumbnails    Size Diff >2x    Same Filename
0           234            189              201              156
1           456            312              378              201
2           189            134              145              89
3           892            645              723              312
4           1,234          891              1,089            445
5           2,341          1,678            2,001            623
6           3,456          2,123            2,890            891
7           4,567          2,456            3,234            1,023
8           5,234          2,789            3,456            1,156
9           4,892          2,234            2,987            987
10          3,234          1,456            2,123            678
--- Gap here? ---
11          456            123              234              45
12          234            67               123              23
13          123            34               67               12
...

Recommendation: Natural cutoff appears at distance 10
  - 95% of Thumbnail pairs: distance ≤ 10
  - 98% of size-different pairs: distance ≤ 10
  - Steep drop-off after distance 10
```

**Decision point:** Choose hamming distance threshold (likely 8-12)

---

## Stage 3: Find Duplicate Groups (~1 minute)

**Script: `find_duplicate_groups.py --threshold 10`**

**Purpose:** Create groups of likely duplicate photos

**Algorithm:**
```python
# 1. Load all photos with (id, perceptual_hash, width, height, file_size, path)
#
# 2. Build pairs list:
#    - For each pair with hamming distance ≤ threshold:
#      - Add to candidate pairs
#
# 3. Create connected components:
#    - Use Union-Find or graph clustering
#    - If A matches B, and B matches C → all in one group
#
# 4. For each group with 2+ photos:
#    - Sort by priority:
#      a) Largest dimensions (width * height)
#      b) Largest file size (tiebreaker)
#      c) NOT in /Thumbnails/ path (bonus)
#      d) Has EXIF data (bonus)
#    - Rank photos: 1 = best, 2 = second best, etc.
#
# 5. Create duplicate_groups table:
CREATE TABLE duplicate_groups (
    group_id INTEGER,
    photo_id TEXT,
    is_best BOOLEAN,
    rank_in_group INTEGER,
    width INTEGER,
    height INTEGER,
    file_size INTEGER,
    hamming_distance_to_best INTEGER,
    FOREIGN KEY (photo_id) REFERENCES photos(id)
)
```

**Output:**
- New `duplicate_groups` table
- Statistics:
  - Total groups: X
  - Total photos in groups: Y
  - Photos marked as potential duplicates (non-best): Z
  - Example: "Found 12,345 groups containing 28,901 photos. 16,556 marked as duplicates for review."

---

## Stage 4: Review UI

**Script: `review_duplicates.py`** (Flask webapp)

**Purpose:** Manual review of duplicate groups

**Interface:**

Display one group at a time:
- All photos scaled to same display height (400px)
- Photos sorted by rank (best first)
- For each photo show:
  - Thumbnail (scaled)
  - Filename (truncated)
  - Dimensions (WxH)
  - File size (MB)
  - Confidence score
  - Path (truncated, hover for full)
  - Hamming distance from best
  - **[Keep]** or **[Reject]** button (pre-selected: best=Keep, others=Reject)

**Actions:**
- "Apply & Next" - saves decisions, next group
- "Keep All & Next" - marks all Keep, next group
- "Skip" - mark for later review

**Progress:** "Group 157 of 1,243 (12.6%)"

**Keyboard shortcuts:**
- Space: Apply & Next
- A: Keep All
- 1-9: Toggle photo 1-9 Keep/Reject
- S: Skip

**Database changes:**
```sql
ALTER TABLE photos ADD COLUMN rejected BOOLEAN DEFAULT 0;
ALTER TABLE photos ADD COLUMN rejection_reason TEXT;
-- rejection_reason: 'duplicate', 'user_marked', NULL
```

---

## Stage 5: Apply Rejections

**Script: `apply_rejections.py`**

**Purpose:** Move rejected photos out of main collection

**Algorithm:**
```python
# For each photo where rejected = 1:
#   - Source: organized/images/{confidence}/{hash}.ext
#   - Dest: organized/rejected/{reason}/{confidence}/{hash}.ext
#   - Move hardlink (doesn't free space, but organizes)
#   - Keep database record (can undo)
#
# Directory structure:
#   organized/rejected/
#     ├── duplicate/
#     │   ├── high_confidence/
#     │   ├── medium_confidence/
#     │   └── low_confidence/
#     └── user_marked/
#         ├── high_confidence/
#         ├── medium_confidence/
#         └── low_confidence/
```

**Output:**
- Summary: "Moved X photos to rejected/, kept Y originals"
- Photos remain in database for potential restoration

**Undo capability:** Can restore by moving files back and setting `rejected = 0`

---

## Design Decisions

### Best Photo Selection Criteria
Priority order:
1. **Largest pixel dimensions** (width × height)
2. **Largest file size** (tiebreaker for same dimensions)
3. **NOT in /Thumbnails/ path** (bonus weight)
4. **Has EXIF data** (bonus weight)

Rationale: Largest dimensions = highest quality. File size catches uncompressed vs compressed.

### Hamming Distance Threshold
- To be determined from Stage 2 analysis
- Typical range: 8-12
- Look for natural gap in distribution

### Rejection Strategy
- **Don't delete** - move to rejected/ directory
- Keep database records
- Easy to undo if mistakes found

### Review All Duplicates
- No automatic deletion
- Manual review via webapp ensures no false positives
- Pre-selection makes review fast (just confirm or adjust)

---

## Implementation Checklist

- [ ] Write `compute_phashes.py`
- [ ] Run Stage 1 (~6 hours, can run during workday)
- [ ] Write `analyze_distances.py`
- [ ] Run Stage 2 analysis (~6 minutes)
- [ ] Review distribution, choose threshold
- [ ] Write `find_duplicate_groups.py`
- [ ] Run Stage 3 with chosen threshold
- [ ] Write `review_duplicates.py` webapp
- [ ] Review duplicates via webapp
- [ ] Write `apply_rejections.py`
- [ ] Run Stage 5 to move rejected photos

---

## Performance Estimates

| Stage | Script | Runtime | Output |
|-------|--------|---------|--------|
| 1 | compute_phashes.py | ~6 hours | 85k perceptual hashes |
| 2 | analyze_distances.py | ~6 minutes | Distance distribution |
| 3 | find_duplicate_groups.py | ~1 minute | Duplicate groups table |
| 4 | review_duplicates.py | Variable | User decisions |
| 5 | apply_rejections.py | ~1 minute | Organized files |

**Total automated time:** ~6 hours
**Manual review time:** Depends on number of groups (estimated 1-2 hours for 10k-20k groups)

---

## Expected Results

Based on current data (28% medium + 12% low confidence, mostly thumbnails):
- Estimated duplicate groups: 10,000-20,000
- Estimated photos to remove: 15,000-30,000
- Final collection: ~55,000-70,000 unique photos
- Mostly high-confidence photos with originals preserved
