# Photo Recovery Pipeline - TODO

## Current State

**Pipeline 1 complete, manual curation done, working on pipeline 2 (secondary grouping).**

Pipeline 1 done:
- [x] Full pipeline run with enriched date extraction
- [x] Manual group review (971 groups reviewed, 61 splits)
- [x] Junk deletion review

Pipeline 2 (post-curation):
- [x] Secondary grouping stage implemented (pipeline2/stage1)
- [ ] **Tune secondary grouping thresholds** - current run only found 110 groups, likely many more
- [ ] **Rerun group rules after secondary grouping** - will catch derivatives missed due to original grouping failures
- [ ] Implement date derivation (pipeline2/stage2)
- [ ] Implement export with date-based organization (pipeline2/stage3)

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
