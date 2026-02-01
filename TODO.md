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

## Hamming Distance Tuning (BLOCKING)

Must tune before Stage 4. All these thresholds should be tuned together with visual sampling:

| Rule | Current | Notes |
|------|---------|-------|
| Grouping threshold | TBD | Stage 4 clustering - MUST TUNE FIRST |
| THUMBNAIL | ≤4 | May need adjustment |
| DERIVATIVE | ≤2 | For same-aspect-ratio resizes |
| GENERIC_NAME | =0 | Strictest; requires identical hash |

- [ ] Build threshold tuning tool (show pairs at each hamming distance)
- [ ] Sample pairs at levels 0, 2, 4, 6, 8, 10, 12
- [ ] Determine where "same photo" becomes "different photo"

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

- [ ] **rule_thumbnail**: Verify hamming ≤4 threshold
- [ ] **rule_preview**: Check if filename matching is sufficient
- [ ] **rule_iphoto_copy**: Get examples - may need tighter matching
- [ ] **rule_derivative**: Verify hamming ≤2 is safe
- [ ] **rule_human_selected**: Test semantic name detection, crop detection, moved-from-siblings

## Exploration Tools (Build When Needed)

- [ ] Hamming threshold tuner UI
- [ ] Group browser (view photos grouped by perceptual similarity)
- [ ] Group rule viewer (like individual rule viewer but for groups)

## Potential Heuristics to Explore

- [ ] **Directory coherence**: If X% of files in a directory are rejected, reject the rest too
  - Need exploration tool first to see examples

## After Pipeline Works

- [ ] Decide export directory structure (by date? flat?)
- [ ] Archive old code in used-scripts/
