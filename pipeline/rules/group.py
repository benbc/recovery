"""
Group Rules (Stage 5)

These reject photos based on comparison with other group members.
Rules can use hamming distance between specific photos as evidence
(close = definitely same, threshold = maybe).

Ranking for decisions: resolution > file_size > has_exif > path_quality

Each rule returns a tuple of (rejected_photo_id, kept_photo_id, rule_name)
or None if the rule doesn't apply.
"""

import re
from pathlib import Path
from typing import Callable, Optional

from ..utils.hashing import hamming_distance


# Type alias for group rule functions
GroupRuleFunc = Callable[[list[dict]], list[tuple[str, str, str]]]


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


def _is_photobooth_pictures(path: str) -> bool:
    """Check if path is a filtered Photo Booth photo (Pictures subfolder)."""
    return "photo booth library/pictures/" in path.lower()


def _is_photobooth_originals(path: str) -> bool:
    """Check if path is an original Photo Booth photo (Originals subfolder)."""
    return "photo booth library/originals/" in path.lower()


def _is_camera_generated_name(filename: str) -> bool:
    """Check if filename looks like a camera-generated name."""
    stem = Path(filename).stem.upper()
    patterns = [
        r"^IMG_\d+$",
        r"^DSC_?\d+$",
        r"^DSCN?\d+$",
        r"^P\d{7}$",
        r"^\d{8}_\d+$",  # YYYYMMDD_XXXX
    ]
    return any(re.match(p, stem) for p in patterns)


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
    Ranking: resolution > file_size > has_exif > path_quality
    """
    resolution = _resolution(photo)
    file_size = photo.get("file_size") or 0
    has_exif = 1 if photo.get("has_exif") else 0

    # Path quality: prefer non-thumbnail, non-preview paths
    path_quality = 0
    for path in _get_paths(photo):
        if _is_thumbnail_path(path) or _is_previews_path(path):
            continue
        if _is_photos_library(path):
            path_quality = 3  # Prefer Photos.app
        elif _is_iphoto_library(path):
            path_quality = max(path_quality, 2)
        else:
            path_quality = max(path_quality, 1)

    return (resolution, file_size, has_exif, path_quality)


# =============================================================================
# GROUP REJECTION RULES
# =============================================================================

def rule_thumbnail(group: list[dict]) -> list[tuple[str, str, str]]:
    """
    THUMBNAIL: Reject smaller thumbnail when larger non-thumbnail exists.

    Uses hamming distance as confidence: close = definitely same photo.
    Only rejects thumbnails when a "master" exists to keep.
    """
    rejections = []

    # Find thumbnails and non-thumbnails
    thumbnails = [p for p in group if _is_thumbnail(p)]
    masters = [p for p in group if not _is_thumbnail(p)]

    if not thumbnails or not masters:
        return []

    # Sort masters by resolution (highest first)
    masters.sort(key=lambda p: _resolution(p), reverse=True)
    best_master = masters[0]

    for thumb in thumbnails:
        thumb_res = _resolution(thumb)
        master_res = _resolution(best_master)

        # Only reject if master is actually larger
        if master_res > thumb_res:
            # Check hamming distance for confidence
            if thumb.get("perceptual_hash") and best_master.get("perceptual_hash"):
                dist = hamming_distance(
                    thumb["perceptual_hash"],
                    best_master["perceptual_hash"]
                )
                # Only reject if very similar (hamming <= 4)
                if dist <= 4:
                    rejections.append((thumb["id"], best_master["id"], "THUMBNAIL"))

    return rejections


def rule_preview(group: list[dict]) -> list[tuple[str, str, str]]:
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

    # Find best non-preview
    non_previews.sort(key=_rank_photo, reverse=True)
    best = non_previews[0]

    for preview in previews:
        # Check filename match
        preview_filename = Path(_get_first_path(preview)).name.lower()
        best_filename = Path(_get_first_path(best)).name.lower()

        if preview_filename == best_filename:
            if (best.get("file_size") or 0) > (preview.get("file_size") or 0):
                rejections.append((preview["id"], best["id"], "PREVIEW"))

    return rejections


def rule_iphoto_copy(group: list[dict]) -> list[tuple[str, str, str]]:
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
                rejections.append((iphoto["id"], photos["id"], "IPHOTO_COPY"))
                break

    return rejections


def rule_photobooth_filtered(group: list[dict]) -> list[tuple[str, str, str]]:
    """
    PHOTOBOOTH_FILTERED: Reject filtered Photo Booth versions when original exists.

    Photo Booth stores originals in /Originals/ and filtered in /Pictures/.
    """
    rejections = []

    filtered = []
    originals = []

    for photo in group:
        paths = _get_paths(photo)
        if any(_is_photobooth_pictures(p) for p in paths):
            filtered.append(photo)
        if any(_is_photobooth_originals(p) for p in paths):
            originals.append(photo)

    if not filtered or not originals:
        return []

    # Reject all filtered versions when any original exists
    best_original = max(originals, key=_rank_photo)
    for photo in filtered:
        rejections.append((photo["id"], best_original["id"], "PHOTOBOOTH_FILTERED"))

    return rejections


def rule_derivative(group: list[dict]) -> list[tuple[str, str, str]]:
    """
    DERIVATIVE: Reject resized versions of identical content.

    When photos have same perceptual hash but different resolutions,
    keep the largest one.
    """
    rejections = []

    # Sort by resolution (highest first)
    sorted_group = sorted(group, key=lambda p: _resolution(p), reverse=True)

    if len(sorted_group) < 2:
        return []

    best = sorted_group[0]
    best_res = _resolution(best)

    for photo in sorted_group[1:]:
        photo_res = _resolution(photo)

        # Skip if same resolution (might be different photos)
        if photo_res == best_res:
            continue

        # Check hamming distance
        if photo.get("perceptual_hash") and best.get("perceptual_hash"):
            dist = hamming_distance(photo["perceptual_hash"], best["perceptual_hash"])

            # Very similar (hamming <= 2) = definitely derivative
            if dist <= 2 and photo_res < best_res * 0.9:
                rejections.append((photo["id"], best["id"], "DERIVATIVE"))

    return rejections


def rule_generic_name(group: list[dict]) -> list[tuple[str, str, str]]:
    """
    GENERIC_NAME: Reject camera-named version when human-named pixel-identical exists.

    If two photos are pixel-identical (same file size + very close hash) and one
    has a camera-generated name (IMG_xxx) while the other has a human name,
    keep the human-named version.
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

        # Pick best human-named as keeper
        best_human = max(human_named, key=_rank_photo)

        for camera in camera_named:
            # Verify very close hash (hamming <= 0 = identical)
            if camera.get("perceptual_hash") and best_human.get("perceptual_hash"):
                dist = hamming_distance(
                    camera["perceptual_hash"],
                    best_human["perceptual_hash"]
                )
                if dist == 0:
                    rejections.append((camera["id"], best_human["id"], "GENERIC_NAME"))

    return rejections


# =============================================================================
# RULE REGISTRY
# =============================================================================

GROUP_RULES: list[GroupRuleFunc] = [
    rule_thumbnail,
    rule_preview,
    rule_iphoto_copy,
    rule_photobooth_filtered,
    rule_derivative,
    rule_generic_name,
]


def apply_group_rules(group: list[dict]) -> list[tuple[str, str, str]]:
    """
    Apply all group rules to a duplicate group.

    Returns list of (rejected_photo_id, kept_photo_id, rule_name) tuples.
    A photo can only be rejected once (first rule wins).
    """
    all_rejections = []
    rejected_ids = set()

    for rule in GROUP_RULES:
        rejections = rule(group)
        for rejected_id, kept_id, rule_name in rejections:
            if rejected_id not in rejected_ids:
                all_rejections.append((rejected_id, kept_id, rule_name))
                rejected_ids.add(rejected_id)

    return all_rejections
