# Photo Recovery Pipeline - Architecture Plan

## Goal

Build a reproducible pipeline to extract family photos from archived computer contents, handle duplicates sensibly, and produce a clean set of photos with metadata.

## Design Principles

1. **Staged pipeline**: Each stage has clear inputs/outputs and can be re-run independently
2. **Two rule types**: Individual rules (photo properties) vs group rules (relationship to duplicates)
3. **Three outcomes**: Reject (junk), Separate (handle differently), Accept (keep)
4. **Preserve information**: Store all source paths, aggregate metadata from duplicates
5. **Consolidate logic**: All rules documented and defined in one place per type
6. **Idempotent stages**: Re-running a stage from scratch produces the same results
7. **Manual review throughout**: Tools for exploration to refine rules, not just final review
8. **Build tools when needed**: Don't build speculatively; create exploration tools as needs arise

---

## Pipeline Stages

### Stage 1: Scan & Extract
- Walk source directory, identify images by MIME type (using python-magic)
- Compute SHA256, extract EXIF metadata, parse dates
- Store ALL source paths for each unique hash (don't lose path info from duplicates)
- **Output**: `photos` table + `photo_paths` table

### Stage 2: Individual Classification
- Apply rules based on photo's own properties (path, size, dimensions, filename)
- Rules are functions that examine one photo in isolation
- Two outcomes: **reject** (junk) or **separate** (handle differently)
- Separated photos skip expensive later stages (perceptual hashing, grouping)
- **Output**: `individual_decisions` table (photo_id, decision, rule_name)

### Stage 2.5: Threshold Tuning (Manual, One-time)
- Before first grouping run, visually sample pairs at each hamming distance
- Distances are even numbers only (0, 2, 4, 6, 8, 10, 12...)
- Build simple side-by-side UI to review samples at each level
- Determine where "same photo" becomes "different photo"
- **Output**: Chosen threshold value for Stage 4

### Stage 3: Perceptual Hash
- Compute imagehash.phash() for photos not rejected/separated
- Skip classified photos entirely (save time on expensive operation)
- Resumable: skip photos that already have hashes
- Can import valid hashes from old database
- **Output**: `perceptual_hash` column populated

### Stage 4: Group Duplicates
- Cluster photos by perceptual hash similarity (hamming distance <= threshold)
- Use union-find algorithm
- Store group membership only (ranking calculated on-the-fly when needed)
- **Output**: `duplicate_groups` table (photo_id, group_id)

### Stage 5: Group Rejection
- Apply rules based on relationship to other group members
- Rules can use hamming distance between specific photos as evidence
- Ranking for decisions: resolution > file_size > has_exif > path_quality
- When rejecting, aggregate path info from rejected photo to kept photo(s)
- **Output**: `group_rejections` table (photo_id, group_id, rule_name)

### Stage 6: Export
- Copy/hardlink accepted photos to organized directory
- Structure TBD (by date? flat?)
- Include all aggregated paths as metadata
- **Output**: Organized photos ready for use

---

## Database Schema

```sql
-- Core photo data (Stage 1)
CREATE TABLE photos (
    id TEXT PRIMARY KEY,           -- SHA256 hash
    mime_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    width INTEGER,
    height INTEGER,
    date_taken DATETIME,
    date_source TEXT,              -- 'exif', 'filename', 'mtime'
    has_exif BOOLEAN DEFAULT 0,    -- Has any EXIF data
    perceptual_hash TEXT,          -- Computed in Stage 3
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- All source paths for each photo (Stage 1)
-- Preserves path info from exact duplicates
CREATE TABLE photo_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_id TEXT NOT NULL REFERENCES photos(id),
    source_path TEXT NOT NULL,
    filename TEXT NOT NULL
);

-- Individual decisions (Stage 2)
CREATE TABLE individual_decisions (
    photo_id TEXT PRIMARY KEY REFERENCES photos(id),
    decision TEXT NOT NULL,        -- 'reject' or 'separate'
    rule_name TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Duplicate groups (Stage 4)
CREATE TABLE duplicate_groups (
    photo_id TEXT PRIMARY KEY REFERENCES photos(id),
    group_id INTEGER NOT NULL
);

-- Group rejections (Stage 5)
CREATE TABLE group_rejections (
    photo_id TEXT PRIMARY KEY REFERENCES photos(id),
    group_id INTEGER NOT NULL,
    rule_name TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated paths from rejected duplicates
CREATE TABLE aggregated_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kept_photo_id TEXT NOT NULL REFERENCES photos(id),
    source_path TEXT NOT NULL,     -- Path from a rejected duplicate
    from_photo_id TEXT NOT NULL    -- Which rejected photo this came from
);

-- Pipeline state tracking
CREATE TABLE pipeline_state (
    stage TEXT PRIMARY KEY,
    completed_at DATETIME,
    photo_count INTEGER,
    notes TEXT
);
```

---

## Rule Definitions

### Individual Rules (Stage 2)

These classify photos based on properties alone, without knowing about duplicates.

#### Rejection Rules (junk - discard)

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `TINY_ICON` | width * height < 5000 | Too small to be a real photo |
| `MINECRAFT_TEXTURE` | path contains minecraft patterns | Game assets |
| `HUE_ANIMATION` | path matches HUE stop-motion folders | Animation frames |
| `ICHAT_ICON` | path contains iChat/Messages icon folders | Chat app assets |
| `WEB_ASSET` | companion .htm file exists | Saved web page |
| `FACE_CROP` | in modelresources/, square, <=500px | Photos.app face detection |
| `STOCK_GREETING` | 3-digit filename in Thumbnails/ | Greeting card templates |
| `FLAG_ICON` | in known flag icons folder | System icons (needs review) |
| `SYSTEM_CACHE` | path contains cache/temp patterns | Transient files |

#### Separation Rules (keep but handle separately)

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `FATHER_IN_LAW` | path matches `%/tor/Pictures/2013/03/03/%` | Digitized collection, different handling |
| `PHOTOBOOTH_ORIGINAL` | path matches `Photo Booth Library/Originals/` | Manual curation needed |

### Group Rules (Stage 5)

These reject photos based on comparison with other group members.
Rules can use hamming distance as evidence (close = definitely same, threshold = maybe).

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `THUMBNAIL` | smaller version when larger exists, close hamming distance | Keep original |
| `PREVIEW` | in /Previews/ when larger version exists | Keep original |
| `IPHOTO_COPY` | in .photolibrary when same in .photoslibrary | Prefer newer app |
| `PHOTOBOOTH_FILTERED` | filtered version when /Originals/ exists | Keep original |
| `DERIVATIVE` | resized version of identical content | Keep largest |
| `GENERIC_NAME` | IMG_xxx when human-named pixel-identical exists | Prefer named |

---

## File Structure

```
recovery/
├── pipeline/
│   ├── __init__.py
│   ├── config.py              # Paths, thresholds, constants
│   ├── database.py            # Schema, connection, utilities
│   ├── stage1_scan.py         # Scan & extract
│   ├── stage2_individual.py   # Individual classification
│   ├── stage3_phash.py        # Perceptual hashing
│   ├── stage4_group.py        # Duplicate grouping
│   ├── stage5_group_reject.py # Group rejection rules
│   ├── stage6_export.py       # Export accepted photos
│   ├── rules/
│   │   ├── __init__.py
│   │   ├── individual.py      # All individual rules
│   │   └── group.py           # All group rules
│   └── utils/
│       ├── __init__.py
│       ├── metadata.py        # EXIF extraction, date parsing
│       └── hashing.py         # SHA256, perceptual hash
├── tools/                     # Built as needed
│   └── (exploration tools created on demand)
├── run_pipeline.py            # Main entry point
├── organized/                 # Output directory
│   ├── photos.db             # SQLite database
│   └── images/               # Exported photos
└── RULES.md                  # Human-readable rule documentation
```

---

## Running the Pipeline

```bash
# Full run from scratch
./run_pipeline.py --from-stage 1

# Re-run from Stage 4 onwards (after fixing grouping logic)
./run_pipeline.py --from-stage 4

# Run single stage
./run_pipeline.py --stage 2

# Import perceptual hashes from old database
./run_pipeline.py --stage 3 --import-hashes ../old/photos.db
```

All scripts:
- Log to stdout AND to a file (using tee or similar) for shared visibility
- Web UIs run with Flask debug mode for auto-reload on code changes

---

## Exploration Tools (Build When Needed)

Tools to create as needs arise during rule development:

- **Threshold tuner**: Side-by-side pairs at each hamming distance level
- **Directory structure explorer**: Pruned tree view with file counts, spot patterns
- **Group browser**: View photos grouped by perceptual similarity (exists in old code)
- **Directory browser**: View photos grouped by parent directory
- **Stats dashboard**: Counts by stage, by rule, by decision type

---

## Implementation Order

1. **Database schema & utilities** - Foundation
2. **Stage 1: Scan** - Get photos into database with all paths
3. **Basic exploration** - Query tool, simple browse UI
4. **Stage 2: Individual rules** - Consolidate existing rules, add separation rules
5. **Threshold tuning tool** - Visual sampling for hamming distance
6. **Stage 3: Perceptual hash** - With import from old DB
7. **Stage 4: Grouping** - Union-find clustering
8. **Stage 5: Group rules** - Consolidate existing rules, use hamming distance
9. **Stage 6: Export** - Final output
10. **Cleanup** - Archive/delete old code in used-scripts/

---

## Known Rule Issues to Address

- **FACE_CROP**: Current threshold is 200px, needs to be ~500px (480x480 crops exist)
- **FLAG_ICON**: Known incomplete, review when implementing
- **Directory coherence**: Consider using sibling file characteristics as evidence (exploration tool first)

---

## Design Decisions

- **No confidence score**: Use explicit properties for ranking. Ad-hoc scoring for exploration only.
- **Duplicate ranking**: resolution > file_size > has_exif > path_quality (calculated on-the-fly)
- **Hamming distance**: Used for grouping AND as evidence within group rules
- **Path preservation**: All source paths stored; aggregated from rejected duplicates to kept photos
- **Separate vs Reject**: Some photos (father-in-law, Photo Booth) are preserved but handled outside main pipeline
- **Code review**: Keep code clean and clear; flag significant logic changes for discussion; can always modify and re-run

---

## Open Questions

1. Directory structure for exported photos - by date? flat? (decide later)
2. Final manual deduplication workflow for "same scene, different shot" groups
