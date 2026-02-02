# Photo Recovery Pipeline - TODO

## Current State

**About to rerun full pipeline** with enriched date extraction schema.

Changes made:
- [x] Enriched EXIF extraction (make, model, software, all date fields)
- [x] Multiple date sources per photo (photo_date_sources table)
- [x] Path-based date parsing (semantic patterns like "Xmas 2004", "April 2010")
- [x] Filename-based date parsing (enhanced patterns)
- [x] Confidence levels for dates (high/medium/low/suspect/unusable)
- [x] IPHOTO_COPY rule flipped to prefer iPhoto (EXIF preserved better)
- [x] Stage 6 removed (needs date assignment logic first)

Next:
- [ ] Rerun pipeline from Stage 1
- [ ] Implement dynamic date selection function (for groups)
- [ ] Design export with date-based organization
- [ ] Implement Stage 6: Export

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
