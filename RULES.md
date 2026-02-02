# Photo Recovery Pipeline - Rule Documentation

This document describes all rules used in the photo recovery pipeline to classify
and deduplicate photos.

## Overview

The pipeline uses two types of rules:

1. **Individual Rules (Stage 2)**: Classify photos based on their own properties alone
2. **Group Rules (Stage 5)**: Reject photos based on comparison with duplicate group members

## Individual Rules (Stage 2)

These rules examine a single photo in isolation. Each rule can produce one of two outcomes:
- **reject**: This is junk - discard it
- **separate**: Keep but handle outside the main pipeline

### Rejection Rules

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `TINY_ICON` | width × height < 5,000 pixels | Too small to be a real photo (icons, emoji, UI elements) |
| `MINECRAFT_TEXTURE` | path contains "minecraft" | Game textures, not personal photos |
| `ICHAT_ICON` | path contains chat app folders + max dimension < 200px | Chat app emoticons and icons |
| `WEB_ASSET` | in `*_files/` directory with companion .htm file | Browser-saved web page assets |
| `FACE_CROP` | in `/modelresources/`, square, ≤500px; or filename contains `_face` | Photos.app/iPhoto face detection thumbnails |
| `STOCK_GREETING` | 3-digit filename in `20140223-155504/` folder | Built-in greeting card templates |
| `FLAG_ICON` | in known flag icons folder (20121223-175144) | System icons imported into iPhoto |
| `FLIP_VIDEO_THUMB` | in `FlipShare Data/Previews/` or `My Flip Video Prefs/` | Auto-generated video thumbnails |
| `VIDEO_THUMBNAIL` | filename starts with `MVI_` or ends with `.THM` | Video preview images from cameras/iPhoto |
| `APP_RESOURCE` | path contains `.app/Contents/` | Application bundle resources, not personal photos |
| `TRASHES` | path contains `/.Trashes/` | Files in macOS external volume trash |

### Separation Rules

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `FATHER_IN_LAW` | path matches `/tor/Pictures/2013/03/03/` or `/Thumbnails/2013/03/03/` or `/Tor's childhood/` | Separate digitized collection needing different handling |
| `PHOTOBOOTH` | path matches `Photo Booth Library/Originals/` or `Photo Booth Library/Pictures/` | Needs manual curation; separating both paths keeps them out of duplicate groups |
| `HUE_ANIMATION` | path contains "HUE Animation" | Animation software frames - kids might want these someday |

## Group Rules (Stage 5)

These rules examine a photo in the context of its duplicate group. They use properties
like resolution, file size, and hamming distance to determine which version to keep.

**Ranking (for reference only)**: resolution > file_size > has_exif

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `THUMBNAIL` | Smaller version when larger non-thumbnail exists AND low hamming distance to that specific master | Keep the original, discard thumbnail |
| `PREVIEW` | In `/Previews/` when larger file with same filename exists | Keep the original, discard preview |
| `IPHOTO_COPY` | In `.photolibrary` when same resolution exists in `.photoslibrary` | Prefer newer Photos.app over older iPhoto |
| `DERIVATIVE` | Smaller resolution with hamming distance ≤2 to larger version | Keep the highest resolution version |
| `SAME_RES_DUPLICATE` | Same resolution and is_same_photo; one in library-generated path or smaller file size | Prefer non-library path, then larger file |

**Rule order**: THUMBNAIL → PREVIEW → IPHOTO_COPY → DERIVATIVE → SAME_RES_DUPLICATE

Each rule only sees photos not yet rejected by earlier rules, so it's never possible to reject all photos in a group.

Note: `GENERIC_NAME` was removed (only matched 2 groups). `PHOTOBOOTH_FILTERED` was removed - Photo Booth photos are now separated in Stage 2. `HUMAN_SELECTED` was removed - it incorrectly rejected originals when groups contained similar-but-different photos.

## Hamming Distance Usage

Perceptual hash hamming distance is used in two ways:

1. **Grouping (Stage 4)**: Photos with hamming distance ≤ threshold are grouped together
2. **Evidence (Stage 5)**: Within a group, hamming distance provides confidence

### pHash Thresholds (based on visual sampling)

| Distance | Meaning | Use |
|----------|---------|-----|
| 0 | Identical content | - |
| ≤2 | Definitely same photo (minor processing differences) | DERIVATIVE rule |
| 4 | ~50% same photo, ~50% same scene different shot | Too risky for auto-decisions |
| 6-8 | Mostly same scene, different shot | - |
| ≤10 | Same scene or similar composition | Grouping threshold |
| 12+ | Increasingly unrelated | - |

**Note**: Some aberrations exist (completely different photos at distance 6-10). These are rare but mean we can't trust intermediate distances for automated decisions.

### dHash as secondary signal

dHash can help in pHash borderline cases:

**pHash 4-6 (borderline same photo):**
- dHash 0 = same photo (strong signal)
- dHash >1 = different photo

### Combined thresholds for grouping

| pHash | dHash | Decision | Rationale |
|-------|-------|----------|-----------|
| ≤10 | any | Group | Reliable same scene |
| 12 | <22 | Group | Mostly same scene, not enough evidence to exclude |
| 12 | ≥22 | Exclude | dHash confirms different scene |
| 14 | ≤17 | Group | dHash confirms same scene |
| 14 | >17 | Exclude | Mostly different, not enough evidence to include |
| >14 | any | Exclude | Different scene |

### Same photo (high confidence)

| Criteria | Use |
|----------|-----|
| pHash ≤2 | DERIVATIVE rule |
| pHash ≤6 AND dHash=0 | Also same photo |

## Path Quality Scoring

When comparing paths, the pipeline prefers:
1. Photos.app library paths (`.photoslibrary/`)
2. iPhoto library paths (`.photolibrary/`)
3. Regular filesystem paths
4. Thumbnails and previews (lowest preference)

## Implementation Details

### Individual Rules

Rules are defined in `pipeline/rules/individual.py`. Each rule is a function that takes
a photo dict and returns `(decision, rule_name)` or `None`.

```python
def rule_tiny_icon(photo: dict) -> Optional[tuple[str, str]]:
    width = photo.get("width") or 0
    height = photo.get("height") or 0
    if width * height < 5000:
        return ("reject", "TINY_ICON")
    return None
```

### Group Rules

Rules are defined in `pipeline/rules/group.py`. Each rule is a function that takes
a list of group member dicts and returns a list of `(rejected_id, rule_name)` tuples.

```python
def rule_thumbnail(group: list[dict]) -> list[tuple[str, str]]:
    # Compare thumbnails to masters, return rejections
    ...
```

## Known Issues and Future Work

- **FACE_CROP**: Current threshold is 500px; some 480×480 crops exist that may need adjustment
- **FLAG_ICON**: Uses hardcoded folder name; may need expansion for other flag collections
- **Directory coherence**: Consider using sibling file characteristics as evidence (build exploration tool first)

## Implementation: Hash Comparison Helpers

All hash comparisons use the helper functions in `pipeline/utils/hashing.py`:

| Function | Use Case | Criteria |
|----------|----------|----------|
| `is_same_photo()` | DERIVATIVE, THUMBNAIL rules | pHash ≤2, OR (pHash ≤6 AND dHash=0) |
| `is_same_scene()` | Grouping (Stage 4) | See combined thresholds above |

These helpers ensure consistent threshold application throughout the codebase.

## Adding New Rules

1. Add the rule function to the appropriate module (`individual.py` or `group.py`)
2. Add the rule to the registry list (`REJECTION_RULES`, `SEPARATION_RULES`, or `GROUP_RULES`)
3. Update this documentation
4. Re-run the relevant stage to apply the new rule
