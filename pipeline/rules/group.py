"""
Group Rules (Stage 5)

These reject photos based on comparison with other group members.
Rules can use hamming distance between specific photos as evidence
(close = definitely same, threshold = maybe).

Rule categories:
1. Automated rejection (confident): APP_MIGRATION, THUMBNAIL, PREVIEW, RESIZE_DERIVATIVE
2. Human selection detection: HUMAN_SELECTED (semantic name, crop, moved-from-siblings)

Fallback ranking (when no rules apply): resolution > file_size > has_exif

Each rule returns a list of (rejected_photo_id, rule_name) tuples.
"""

import re
from collections import Counter
from pathlib import Path
from typing import Callable, Optional

from ..utils.hashing import hamming_distance, is_same_photo


# Type alias for group rule functions
# Returns list of (rejected_photo_id, rule_name) tuples
GroupRuleFunc = Callable[[list[dict]], list[tuple[str, str]]]


def _get_paths(photo: dict) -> list[str]:
    """Get all source paths for a photo."""
    all_paths = photo.get("all_paths", "")
    if all_paths:
        return all_paths.split("|")
    return []


def _get_first_path(photo: dict) -> str:
    """Get the first source path for a photo."""
    paths = _get_paths(photo)
    return paths[0] if paths else ""


def _resolution(photo: dict) -> int:
    """Get resolution (width * height) for a photo."""
    return (photo.get("width") or 0) * (photo.get("height") or 0)


def _is_thumbnail_path(path: str) -> bool:
    """Check if path indicates a thumbnail."""
    path_lower = path.lower()
    return "/thumbnails/" in path_lower or path_lower.startswith("thumb_")


def _is_thumbnail_filename(path: str) -> bool:
    """Check if filename indicates a thumbnail."""
    filename = Path(path).name.lower()
    return filename.startswith("thumb_")


def _is_thumbnail(photo: dict) -> bool:
    """Check if any path indicates this is a thumbnail."""
    for path in _get_paths(photo):
        if _is_thumbnail_path(path) or _is_thumbnail_filename(path):
            return True
    return False


def _is_previews_path(path: str) -> bool:
    """Check if path is in a Previews folder."""
    return "/previews/" in path.lower()


def _is_iphoto_library(path: str) -> bool:
    """Check if path is from iPhoto Library (.photolibrary)."""
    return ".photolibrary/" in path.lower()


def _is_photos_library(path: str) -> bool:
    """Check if path is from modern Photos Library (.photoslibrary)."""
    return ".photoslibrary/" in path.lower()


def _is_camera_generated_name(filename: str) -> bool:
    """Check if filename looks like a camera-generated name."""
    stem = Path(filename).stem.upper()
    patterns = [
        r"^IMG_\d+$",
        r"^IMG_E\d+$",  # iPhone edited
        r"^DSC_?\d+$",
        r"^DSCN?\d+$",
        r"^P\d{7}$",
        r"^\d{8}_\d+$",  # YYYYMMDD_XXXX
        r"^\d{8}-\d+$",  # YYYYMMDD-XXXX
        r"^PHOTO-\d{4}-\d{2}-\d{2}",  # PHOTO-YYYY-MM-DD
    ]
    return any(re.match(p, stem) for p in patterns)


def _has_semantic_name(photo: dict) -> bool:
    """
    Check if photo has a human-assigned semantic name.

    Returns True if the filename appears to be human-named rather than
    camera-generated (IMG_xxx, DSC_xxx, etc.).
    """
    path = _get_first_path(photo)
    if not path:
        return False
    filename = Path(path).name
    return not _is_camera_generated_name(filename)


def _get_aspect_ratio(photo: dict) -> float:
    """Get aspect ratio (width/height) for a photo."""
    width = photo.get("width") or 0
    height = photo.get("height") or 0
    if height == 0:
        return 0
    return width / height


def _is_crop_of_others(photo: dict, group: list[dict]) -> bool:
    """
    Check if photo appears to be a crop (different aspect ratio, smaller).

    A crop has:
    - Different aspect ratio from most others in the group (>5% difference)
    - Smaller total pixels than the largest in group

    This signals human selection - someone chose this framing.
    """
    my_ratio = _get_aspect_ratio(photo)
    my_pixels = _resolution(photo)

    if my_ratio == 0 or my_pixels == 0:
        return False

    # Get aspect ratios of others
    other_ratios = []
    max_pixels = my_pixels
    for other in group:
        if other["id"] == photo["id"]:
            continue
        ratio = _get_aspect_ratio(other)
        if ratio > 0:
            other_ratios.append(ratio)
        pixels = _resolution(other)
        if pixels > max_pixels:
            max_pixels = pixels

    if not other_ratios:
        return False

    # Find most common aspect ratio among others
    # (simple approach: use median)
    other_ratios.sort()
    median_ratio = other_ratios[len(other_ratios) // 2]

    # Check if my ratio differs significantly (>5%)
    ratio_diff = abs(my_ratio - median_ratio) / median_ratio if median_ratio else 0
    is_different_ratio = ratio_diff > 0.05

    # Check if I'm smaller (indicating crop, not source of crops)
    is_smaller = my_pixels < max_pixels

    return is_different_ratio and is_smaller


def _get_parent_folder(path: str) -> str:
    """Get the parent folder path for grouping."""
    return str(Path(path).parent)


def _was_moved_from_siblings(photo: dict, group: list[dict]) -> bool:
    """
    Check if photo was moved to a different location than its siblings.

    If most photos in a group share a parent folder but this one is elsewhere,
    it suggests someone deliberately moved/organized this photo.

    Requires at least 2 other photos sharing a common location.
    """
    my_paths = _get_paths(photo)
    if not my_paths:
        return False

    my_parents = {_get_parent_folder(p) for p in my_paths}

    # Collect parent folders from all other photos
    sibling_parents = []
    for other in group:
        if other["id"] == photo["id"]:
            continue
        for path in _get_paths(other):
            sibling_parents.append(_get_parent_folder(path))

    if len(sibling_parents) < 2:
        return False

    # Find most common parent among siblings
    parent_counts = Counter(sibling_parents)
    most_common_parent, count = parent_counts.most_common(1)[0]

    # Need at least 2 siblings in the common location
    if count < 2:
        return False

    # Check if I'm NOT in the common location
    return most_common_parent not in my_parents


def _extract_base_filename(path: str) -> str:
    """
    Extract base filename for clustering.
    Removes known derivative patterns to group originals with their derivatives.
    """
    filename = Path(path).stem

    # Remove thumbnail prefix
    filename = re.sub(r"^thumb_", "", filename, flags=re.IGNORECASE)

    # Remove !cid_ prefix (email attachment extracts)
    filename = re.sub(r"^!cid_", "", filename, flags=re.IGNORECASE)

    # Remove known resolution suffix
    filename = re.sub(r"_1024$", "", filename)

    # Normalize iPhone edited photos: IMG_E1234 -> IMG_1234
    filename = re.sub(r"^(IMG_)E(\d+)$", r"\1\2", filename, flags=re.IGNORECASE)

    # Normalize spaces and dashes
    filename = re.sub(r"[-_\s]+", " ", filename)

    return filename.upper()


def _rank_photo(photo: dict) -> tuple:
    """
    Create a ranking tuple for a photo.
    Higher values = better quality = should be kept.
    Ranking: resolution > file_size > has_exif

    Used as fallback/hint when no selection rules apply.
    """
    resolution = _resolution(photo)
    file_size = photo.get("file_size") or 0
    has_exif = 1 if photo.get("has_exif") else 0

    return (resolution, file_size, has_exif)


# =============================================================================
# GROUP REJECTION RULES
# =============================================================================

def rule_thumbnail(group: list[dict]) -> list[tuple[str, str]]:
    """
    THUMBNAIL: Reject smaller thumbnail when larger non-thumbnail exists.

    For each thumbnail, checks if ANY master exists that is BOTH:
    1. Higher resolution than the thumbnail
    2. Same photo (using is_same_photo threshold)

    This handles groups with multiple unrelated photos correctly.
    """
    rejections = []

    # Find thumbnails and non-thumbnails
    thumbnails = [p for p in group if _is_thumbnail(p)]
    masters = [p for p in group if not _is_thumbnail(p)]

    if not thumbnails or not masters:
        return []

    for thumb in thumbnails:
        thumb_res = _resolution(thumb)
        thumb_phash = thumb.get("perceptual_hash")
        thumb_dhash = thumb.get("dhash")

        if not thumb_phash or not thumb_dhash:
            continue

        # Check if ANY master is larger and same photo
        for master in masters:
            master_res = _resolution(master)
            master_phash = master.get("perceptual_hash")
            master_dhash = master.get("dhash")

            if not master_phash or not master_dhash:
                continue

            # Must be larger and same photo
            if master_res > thumb_res:
                phash_dist = hamming_distance(thumb_phash, master_phash)
                dhash_dist = hamming_distance(thumb_dhash, master_dhash)
                if is_same_photo(phash_dist, dhash_dist):
                    rejections.append((thumb["id"], "THUMBNAIL"))
                    break  # Only reject once

    return rejections


def rule_preview(group: list[dict]) -> list[tuple[str, str]]:
    """
    PREVIEW: Reject preview versions when larger original exists.

    Preview files are in /Previews/ directories in photo libraries.
    """
    rejections = []

    previews = []
    non_previews = []

    for photo in group:
        if any(_is_previews_path(p) for p in _get_paths(photo)):
            previews.append(photo)
        else:
            non_previews.append(photo)

    if not previews or not non_previews:
        return []

    for preview in previews:
        preview_filename = Path(_get_first_path(preview)).name.lower()
        preview_size = preview.get("file_size") or 0

        # Check against ALL non-previews for a match
        for non_preview in non_previews:
            non_preview_filename = Path(_get_first_path(non_preview)).name.lower()
            non_preview_size = non_preview.get("file_size") or 0

            if preview_filename == non_preview_filename and non_preview_size > preview_size:
                rejections.append((preview["id"], "PREVIEW"))
                break  # Only reject once

    return rejections


def rule_iphoto_copy(group: list[dict]) -> list[tuple[str, str]]:
    """
    IPHOTO_COPY: Reject iPhoto version when same exists in Photos.app library.

    Prefer the newer Photos.app version over older iPhoto version.
    """
    rejections = []

    iphoto_photos = []
    photos_photos = []

    for photo in group:
        paths = _get_paths(photo)
        if any(_is_iphoto_library(p) for p in paths):
            iphoto_photos.append(photo)
        if any(_is_photos_library(p) for p in paths):
            photos_photos.append(photo)

    if not iphoto_photos or not photos_photos:
        return []

    # For each iPhoto photo, check if same resolution exists in Photos
    for iphoto in iphoto_photos:
        iphoto_res = _resolution(iphoto)

        for photos in photos_photos:
            photos_res = _resolution(photos)

            if iphoto_res == photos_res:
                rejections.append((iphoto["id"], "IPHOTO_COPY"))
                break

    return rejections


# NOTE: rule_photobooth_filtered removed - Photo Booth photos are now separated
# in Stage 2 (individual rules) so they won't appear in duplicate groups.


def rule_derivative(group: list[dict]) -> list[tuple[str, str]]:
    """
    DERIVATIVE: Reject resized versions of identical content.

    When a smaller photo is the same photo (using is_same_photo threshold)
    as a larger one, reject the smaller one as a derivative.
    """
    rejections = []

    if len(group) < 2:
        return []

    for photo in group:
        photo_res = _resolution(photo)
        photo_phash = photo.get("perceptual_hash")
        photo_dhash = photo.get("dhash")

        if not photo_phash or not photo_dhash:
            continue

        # Check if ANY larger photo is the same photo
        for other in group:
            if other["id"] == photo["id"]:
                continue

            other_res = _resolution(other)
            other_phash = other.get("perceptual_hash")
            other_dhash = other.get("dhash")

            if not other_phash or not other_dhash:
                continue

            # Other must be significantly larger
            if other_res <= photo_res or photo_res >= other_res * 0.9:
                continue

            phash_dist = hamming_distance(photo_phash, other_phash)
            dhash_dist = hamming_distance(photo_dhash, other_dhash)
            if is_same_photo(phash_dist, dhash_dist):
                rejections.append((photo["id"], "DERIVATIVE"))
                break  # Only reject once

    return rejections


def rule_generic_name(group: list[dict]) -> list[tuple[str, str]]:
    """
    GENERIC_NAME: Reject camera-named version when human-named pixel-identical exists.

    If two photos are pixel-identical (same file size + identical hash) and one
    has a camera-generated name (IMG_xxx) while the other has a human name,
    reject the camera-named version.
    """
    rejections = []

    # Group by file size (pixel-identical implies same size)
    by_size = {}
    for photo in group:
        size = photo.get("file_size") or 0
        if size not in by_size:
            by_size[size] = []
        by_size[size].append(photo)

    for size, photos in by_size.items():
        if len(photos) < 2:
            continue

        # Find camera-named and human-named
        camera_named = []
        human_named = []

        for photo in photos:
            path = _get_first_path(photo)
            filename = Path(path).name
            if _is_camera_generated_name(filename):
                camera_named.append(photo)
            else:
                human_named.append(photo)

        if not camera_named or not human_named:
            continue

        # For each camera-named photo, check if ANY human-named photo is identical
        for camera in camera_named:
            camera_hash = camera.get("perceptual_hash")
            if not camera_hash:
                continue

            for human in human_named:
                human_hash = human.get("perceptual_hash")
                if not human_hash:
                    continue

                if hamming_distance(camera_hash, human_hash) == 0:
                    rejections.append((camera["id"], "GENERIC_NAME"))
                    break  # Only reject once per camera-named photo

    return rejections


def rule_human_selected(group: list[dict]) -> list[tuple[str, str]]:
    """
    HUMAN_SELECTED: Keep photos with evidence of human selection, reject others.

    Selection signals:
    - Semantic filename (not camera-generated like IMG_xxx)
    - Is a crop (different aspect ratio, smaller - someone chose that framing)
    - Was moved from siblings (most of group in one place, this one elsewhere)

    If any photos have selection signals, keep those and reject the rest.
    If multiple have signals, keep all of them (human wanted multiple versions).
    If none have signals, don't reject anything (leave for manual review).
    """
    rejections = []

    # Find photos with selection signals
    selected = []
    not_selected = []

    for photo in group:
        has_signal = (
            _has_semantic_name(photo) or
            _is_crop_of_others(photo, group) or
            _was_moved_from_siblings(photo, group)
        )
        if has_signal:
            selected.append(photo)
        else:
            not_selected.append(photo)

    # If no selection signals found, don't reject anything
    if not selected:
        return []

    # If all have signals (or only one photo), don't reject anything
    if not not_selected:
        return []

    # Reject all non-selected photos
    for photo in not_selected:
        rejections.append((photo["id"], "HUMAN_SELECTED"))

    return rejections


# =============================================================================
# RULE REGISTRY
# =============================================================================

# Automated rejection rules (high confidence, apply first)
AUTOMATED_RULES: list[GroupRuleFunc] = [
    rule_iphoto_copy,    # APP_MIGRATION: prefer Photos.app over iPhoto
    rule_thumbnail,      # THUMBNAIL_PATH: reject thumbnails
    rule_preview,        # PREVIEW: reject preview versions
    rule_derivative,     # RESIZE_DERIVATIVE: reject resized versions
]

# Human selection rules (detect curation signals)
SELECTION_RULES: list[GroupRuleFunc] = [
    rule_generic_name,   # Strict: same size + identical hash + semantic name
    rule_human_selected, # General: any selection signal (name, crop, moved)
]

GROUP_RULES: list[GroupRuleFunc] = AUTOMATED_RULES + SELECTION_RULES


def apply_group_rules(group: list[dict]) -> list[tuple[str, str]]:
    """
    Apply all group rules to a duplicate group.

    Returns list of (rejected_photo_id, rule_name) tuples.
    A photo can only be rejected once (first rule wins).
    """
    all_rejections = []
    rejected_ids = set()

    for rule in GROUP_RULES:
        rejections = rule(group)
        for rejected_id, rule_name in rejections:
            if rejected_id not in rejected_ids:
                all_rejections.append((rejected_id, rule_name))
                rejected_ids.add(rejected_id)

    return all_rejections
