# Photo Recovery Pipeline - TODO

## Current State

Completed:
- [x] Stage 1: Scan & Extract (85,710 photos, 106,246 paths)
- [x] Stage 1b: Create hardlinks for safety/serving
- [x] Stage 2: Individual Classification (29,875 rejected, 9,551 separated)
- [x] Stage 3: Perceptual Hash (pHash + dHash with EXIF rotation normalization)

Next:
- [ ] **Tune hamming threshold** before running Stage 4 (grouping)
- [ ] Stage 4: Group Duplicates
- [ ] Stage 5: Group Rejection
- [ ] Stage 6: Export

## Hamming Distance Tuning

### pHash (DONE)
Visual sampling complete. Results:
- **≤2**: Definitely same photo → use for DERIVATIVE, GENERIC_NAME
- **≤10**: Same scene → use for grouping threshold
- Distance 4 is ~50/50, too risky for auto-decisions

### dHash (DONE)
dHash useful as secondary signal in pHash borderline cases:
- pHash 4-6: dHash 0 = same photo, >1 = different
- pHash 10-14: dHash ≤17 = same scene, ≥22 = different

### Cropping detection (FUTURE)
Anticipate dHash helping with crop detection, but haven't seen enough evidence yet. Plan to review groups after Stage 4 and try to include dHash in automated resolution logic for Stage 5.

**Observed crops during rule verification:**
- Group #13: contains a crop

## Group Rules (Implemented, Need Testing)

Automated rejection (high confidence):
- IPHOTO_COPY: Prefer Photos.app over iPhoto library
- THUMBNAIL: Reject thumbnails when larger exists
- PREVIEW: Reject /Previews/ versions when larger exists
- DERIVATIVE: Reject same-ratio smaller versions (hamming ≤2)

Human selection detection:
- GENERIC_NAME: Reject camera-named (IMG_xxx) when human-named identical exists
- HUMAN_SELECTED: Keep photos with selection signals, reject others
  - Signals: semantic filename, crop (different aspect ratio), moved-from-siblings

## Rules Needing Visual Verification

- [ ] **rule_thumbnail**: Verify `is_same_photo()` threshold (pHash ≤2, or pHash ≤6 with dHash=0)
- [ ] **rule_preview**: Check if filename matching is sufficient
- [ ] **rule_iphoto_copy**: Get examples - may need tighter matching
- [ ] **rule_derivative**: Verify `is_same_photo()` threshold (pHash ≤2, or pHash ≤6 with dHash=0)
- [ ] **rule_human_selected**: Test semantic name detection, crop detection, moved-from-siblings

## Exploration Tools

- [x] Hamming threshold tuner UI (`tools/threshold_tuner.py`)
- [x] Group browser (`tools/group_viewer.py`)
- [x] Unlinked pairs viewer (`tools/unlinked_viewer.py`)
- [x] Bridge viewer (`tools/bridge_viewer.py`)
- [ ] Group rule viewer (like individual rule viewer but for groups)

## Potential Heuristics to Explore

- [ ] **Directory coherence**: If X% of files in a directory are rejected, reject the rest too
  - Need exploration tool first to see examples

## Issues to Investigate

- [x] **FATHER_IN_LAW rule incomplete**: Fixed - expanded to include `/Thumbnails/2013/03/03/` and `/Tor's childhood/` paths. Re-run Stage 2 to apply.
- [ ] **pHash aberrations to investigate**:
  - Distance 6: a6a4d859592ba7e4 and a6a6dc59190ba7d4 are completely different photos
  - Distance 10: c63179ce077ae027 and e63171ce47c3c227 are different photos

## After Pipeline Works

- [ ] Decide export directory structure (by date? flat?)
- [ ] Archive old code in used-scripts/
