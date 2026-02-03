# Plan: Secondary Grouping

## Goal

Create a secondary grouping layer that merges primary groups and incorporates ungrouped singles, while preserving the original `duplicate_groups` table for date derivation.

## Data Model

### Existing (unchanged)
- `duplicate_groups` (photo_id, group_id) - curated primary groups including manual splits

### New Tables
```sql
CREATE TABLE secondary_groups (
    photo_id TEXT PRIMARY KEY,
    secondary_group_id INTEGER NOT NULL
);
CREATE INDEX idx_secondary_groups_group ON secondary_groups(secondary_group_id);

CREATE TABLE secondary_unlinked_pairs (
    photo_id_1 TEXT NOT NULL,
    photo_id_2 TEXT NOT NULL,
    phash_dist INTEGER NOT NULL,
    dhash_dist INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (photo_id_1, photo_id_2)
);
```

Photos not in `secondary_groups` either:
- Remain in their primary group only (no merge needed)
- Or are truly ungrouped singletons

## Algorithm

### 1. Identify Kept Photos

Get all photos that are:
- NOT in `junk_deletions`
- NOT in `group_rejections`
- NOT in `individual_decisions` (reject or separate)

These are the photos we want to potentially group.

### 2. Load Hashes

For each kept photo, load:
- `perceptual_hash` (pHash)
- `dhash`

Skip photos missing either hash.

### 3. Compare All Pairs

Use existing `is_same_scene(phash_dist, dhash_dist)` function from `pipeline/utils/hashing.py`:
- pHash ≤10: group
- pHash 11-12: group if dHash <22
- pHash 13-14: group if dHash ≤17
- pHash >14: don't group

Build edge list of pairs that should be grouped.

### 4. Cluster Using Complete-Linkage

Use existing `complete_linkage_cluster()` from `pipeline/stage4_group.py`:
- Ensures all pairs within a cluster satisfy `should_group()`
- Deterministic results regardless of input order

### 5. Map to Secondary Groups

For each cluster with 2+ photos:
- If cluster contains photos from multiple primary groups → create secondary group (merging)
- If cluster contains ungrouped singles → create secondary group
- If cluster exactly matches a single primary group → no secondary group needed (already grouped)

### 6. Store Results

Insert into `secondary_groups` table only photos that are in merged/new groups.

## Relationship to Primary Groups

For date derivation, walk backwards:
```
secondary_group → photos → primary groups (via duplicate_groups) → all photos in those primary groups
```

This gives us the full set of related photos for date inference.

## Implementation

### New Stage: 4c (Secondary Grouping)

Location: `pipeline/stage4c_secondary.py`

```python
def run_stage4c():
    # 1. Get kept photos with hashes
    # 2. Compare all pairs using is_same_scene()
    # 3. Cluster with complete_linkage_cluster()
    # 4. Determine which clusters need secondary groups
    # 5. Insert into secondary_groups table
```

### Reuse from stage4_group.py
- `is_same_scene()` (via import from utils/hashing)
- `complete_linkage_cluster()`
- `find_connected_components()`
- `hamming_distance()`

## Expected Outcomes

- Primary groups that should merge → get same secondary_group_id
- Ungrouped singles that match → get grouped in secondary_groups
- Truly unique photos → no entry in secondary_groups

## Stats to Report

- Photos considered
- Pairs compared
- Secondary groups created
- Primary groups merged (groups that now share a secondary group)
- Singles incorporated (ungrouped photos now in secondary groups)

## Unlinked Pairs

Track unlinked pairs (pairs that satisfy `is_same_scene()` but ended up in different secondary groups) for validation, same as Stage 4. Allows reviewing near-matches that didn't cluster.

## Thresholds

Start with same logic as Stage 4 (no changes):
- pHash ≤10: group
- pHash 11-12: group if dHash <22
- pHash 13-14: group if dHash ≤17
- pHash >14: don't group

May tune later based on results.

## Performance Estimate

- 12,836 kept photos with hashes
- ~82M pairs to compare
- Original Stage 4 compared 46,283 photos (~1B pairs)
- This is ~1/13 the work, should complete quickly

## Next Steps

1. Implement stage4c_secondary.py
2. Run and review results
3. Use unlinked pairs viewer to validate grouping quality
4. Tune thresholds if needed
