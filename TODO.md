# Photo Recovery Pipeline - TODO

## Current State

**Pipeline 1 complete, manual curation done, working on pipeline 2 (secondary grouping).**

Pipeline 1 done:
- [x] Full pipeline run with enriched date extraction
- [x] Manual group review (971 groups reviewed, 61 splits)
- [x] Junk deletion review

Pipeline 2 (post-curation):
- [x] Stage 1: Extended hashes computed (phash_16, colorhash) for all 12,836 kept photos
- [ ] **Stage 1b: Rerun pairwise distance computation** - previous run had incomplete data
- [x] Threshold tuner tool built with 2D explorer, rating system, auto-scan
- [ ] **Find exact 2D boundary** - map the phash16/colorhash decision boundary using ratings
- [ ] **Implement two-stage clustering**: complete linkage first (tight cores), then single linkage (extend cores)
- [ ] **Rerun group rules after secondary grouping** - will catch derivatives missed due to original grouping failures
- [ ] Implement date derivation (pipeline2/stage3)
- [ ] Implement export with date-based organization (pipeline2/stage4)

Threshold tuning progress:
- Initial ratings recorded at phash16 88-92 range showing diagonal boundary
- Same-group pairs (known duplicates) cluster at low distances - confirms approach

## Remaining Decisions

- [ ] Export directory structure (by date? flat? hybrid?)
- [ ] How to handle groups with conflicting dates
- [ ] Manual review of ~90 conflict groups for patterns
- [ ] **Post-split date inheritance**: When a group is manually split, auto-rejected photos
      stay with their original group. Their date metadata may be used for date assignment,
      but they might conceptually belong to the split-off group. Consider whether to:
      (a) move auto-rejected photos during splits, (b) exclude auto-rejected photos from
      date calculation, or (c) handle this in manual review

## Potential Future Work

- [ ] **Directory coherence**: If X% of files in a directory are rejected, reject the rest too
- [ ] **pHash aberrations**: Some completely different photos at distance 6-10 (rare, documented in RULES.md)
- [ ] Archive old code in used-scripts/
- [ ] **Multi-threading for expensive operations**: Hashing and pairwise distance computation are single-threaded. Consider parallelizing if we need to rerun these or similar operations.
