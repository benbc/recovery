# Photo Recovery Pipeline - TODO

## Current State

Completed:
- [x] Stage 1: Scan & Extract (85,710 photos, 106,246 paths)
- [x] Stage 1b: Create hardlinks for safety/serving
- [x] Stage 2: Individual Classification (26,580 rejected, 18,710 separated)
- [x] Stage 3: Perceptual Hash (pHash + dHash with EXIF rotation normalization)
- [x] Stage 4: Group Duplicates (9,353 groups, 37,282 photos)
- [x] Stage 4b: Merge Bridge-Connected Groups (31 merges)
- [x] Stage 5: Group Rejection (26,571 rejections)

Next:
- [ ] Stage 6: Export

## Remaining Decisions

- [ ] Decide export directory structure (by date? flat? hybrid?)

## Potential Future Work

- [ ] **Directory coherence**: If X% of files in a directory are rejected, reject the rest too
- [ ] **pHash aberrations**: Some completely different photos at distance 6-10 (rare, documented in RULES.md)
- [ ] Archive old code in used-scripts/
