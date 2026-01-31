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
| `HUE_ANIMATION` | path contains "HUE Animation" | Animation software frames, not personal photos |
| `ICHAT_ICON` | path contains chat app folders + small size | Chat app emoticons and icons |
| `WEB_ASSET` | in `*_files/` directory with companion .htm file | Browser-saved web page assets |
| `FACE_CROP` | in `/modelresources/`, square, ≤500px | Photos.app face detection thumbnails |
| `STOCK_GREETING` | 3-digit filename in `/Thumbnails/` path | Built-in greeting card templates |
| `FLAG_ICON` | in known flag icons folder (20121223-175144) | System icons imported into iPhoto |
| `SYSTEM_CACHE` | path contains cache/temp patterns | Transient system files |
| `FLIP_VIDEO_THUMB` | in `FlipShare Data/Previews/` | Auto-generated video thumbnails |

### Separation Rules

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `FATHER_IN_LAW` | path matches `/tor/Pictures/2013/03/03/` | Separate digitized collection needing different handling |
| `PHOTOBOOTH_ORIGINAL` | path matches `Photo Booth Library/Originals/` | Needs manual curation to select best shots |

## Group Rules (Stage 5)

These rules examine a photo in the context of its duplicate group. They use properties
like resolution, file size, and hamming distance to determine which version to keep.

**Ranking for decisions**: resolution > file_size > has_exif > path_quality

| Rule Name | Condition | Rationale |
|-----------|-----------|-----------|
| `THUMBNAIL` | Smaller version when larger non-thumbnail exists, close hamming distance | Keep the original, discard thumbnail |
| `PREVIEW` | In `/Previews/` when larger file with same filename exists | Keep the original, discard preview |
| `IPHOTO_COPY` | In `.photolibrary` when same resolution exists in `.photoslibrary` | Prefer newer Photos.app over older iPhoto |
| `PHOTOBOOTH_FILTERED` | In Photo Booth `/Pictures/` when `/Originals/` version exists | Keep original, discard filtered version |
| `DERIVATIVE` | Smaller resolution with hamming distance ≤2 to larger version | Keep the highest resolution version |
| `GENERIC_NAME` | Camera-named (IMG_xxx) when human-named pixel-identical exists | Prefer the renamed version |

## Hamming Distance Usage

Perceptual hash hamming distance is used in two ways:

1. **Grouping (Stage 4)**: Photos with hamming distance ≤ threshold are grouped together
2. **Evidence (Stage 5)**: Within a group, hamming distance provides confidence:
   - Distance 0: Identical content (different resolution or format)
   - Distance ≤2: Definitely the same photo
   - Distance ≤4: Very likely the same photo
   - Distance ≤8: Probably the same photo (default threshold)
   - Distance >8: May be different photos (similar composition)

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
a list of group member dicts and returns a list of `(rejected_id, kept_id, rule_name)` tuples.

```python
def rule_thumbnail(group: list[dict]) -> list[tuple[str, str, str]]:
    # Compare thumbnails to masters, return rejections
    ...
```

## Known Issues and Future Work

- **FACE_CROP**: Current threshold is 500px; some 480×480 crops exist that may need adjustment
- **FLAG_ICON**: Uses hardcoded folder name; may need expansion for other flag collections
- **Directory coherence**: Consider using sibling file characteristics as evidence (build exploration tool first)

## Adding New Rules

1. Add the rule function to the appropriate module (`individual.py` or `group.py`)
2. Add the rule to the registry list (`REJECTION_RULES`, `SEPARATION_RULES`, or `GROUP_RULES`)
3. Update this documentation
4. Re-run the relevant stage to apply the new rule
