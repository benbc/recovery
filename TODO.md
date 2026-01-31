# Photo Recovery Pipeline - TODO

## Before First Full Run

- [ ] Review machinery (stages, database, runner)
- [ ] Run Stage 1 on a small subset to verify it works

## Rules Needing Verification

These need visual examples before we can tune thresholds:

- [ ] **TINY_ICON**: Verify <5000px images aren't thumbnails we want to keep
- [ ] **rule_preview**: Check if needs per-preview hamming distance (like thumbnail fix)
- [ ] **rule_iphoto_copy**: Get examples - may need tighter matching (hamming/names)
- [ ] **rule_derivative**: Verify hamming ≤2 is safe (was identical hash before)

## Hamming Distance Tuning

All these thresholds should be tuned together with visual sampling:

| Rule | Current | Notes |
|------|---------|-------|
| THUMBNAIL | ≤4 | May need to be lower |
| DERIVATIVE | ≤2 | Looser than before |
| GENERIC_NAME | =0 | Strictest; probably correct |
| Grouping threshold | ≤8 | Stage 4 clustering |

- [ ] Build threshold tuning tool (Stage 2.5) before first grouping run
- [ ] Sample pairs at each hamming distance level (0, 2, 4, 6, 8, 10, 12)
- [ ] Determine where "same photo" becomes "different photo"

## Exploration Tools (Build When Needed)

- [ ] Threshold tuner UI
- [ ] Query tool for database exploration
- [ ] Group browser
- [ ] Directory structure explorer

## After Pipeline Works

- [ ] Decide export directory structure (by date? flat?)
- [ ] Archive old code in used-scripts/
