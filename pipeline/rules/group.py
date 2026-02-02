"""
Group Rules (Stage 5)

These reject photos based on comparison with other group members.
Rules can use hamming distance between specific photos as evidence
(close = definitely same, threshold = maybe).

Rule categories:
- THUMBNAIL: Reject smaller thumbnail when larger non-thumbnail exists
- PREVIEW: Reject preview versions when larger original exists
- IPHOTO_COPY: Prefer Photos.app over iPhoto when same resolution
- DERIVATIVE: Reject resized versions (same content, smaller)
- SAME_RES_DUPLICATE: Reject same-resolution duplicates (prefer non-library, then larger)

Each rule returns a list of (rejected_photo_id, rule_name) tuples.
"""

import re
from pathlib import Path
from typing import Callable

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


def _is_in_thumbnails_folder(photo: dict) -> bool:
    """Check if photo is in a /Thumbnails/ folder (strong path signal)."""
    for path in _get_paths(photo):
        if "/thumbnails/" in path.lower():
            return True
    return False


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


def _is_library_generated_path(path: str) -> bool:
    """Check if path is in a library-generated folder (not user-organized)."""
    path_lower = path.lower()
    return (
        "/previews/" in path_lower
        or "/thumbnails/" in path_lower
        or "/modelresources/" in path_lower
    )


def _is_iphoto_library(path: str) -> bool:
    """Check if path is from iPhoto Library (.photolibrary)."""
    return ".photolibrary/" in path.lower()


def _is_photos_library(path: str) -> bool:
    """Check if path is from modern Photos Library (.photoslibrary)."""
    return ".photoslibrary/" in path.lower()


# =============================================================================
# GROUP REJECTION RULES
# =============================================================================

def _get_base_filename(path: str) -> str:
    """Extract base filename without resolution suffixes like _1024."""
    stem = Path(path).stem.lower()
    # Remove common resolution suffixes
    stem = re.sub(r"_\d+$", "", stem)
    # Remove thumb_ prefix
    stem = re.sub(r"^thumb_", "", stem)
    return stem


def rule_thumbnail(group: list[dict]) -> list[tuple[str, str]]:
    """
    THUMBNAIL: Reject smaller thumbnail when larger non-thumbnail exists.

    For each thumbnail, checks if ANY master exists that is BOTH:
    1. Higher resolution than the thumbnail
    2. Same photo - verified by either:
       - is_same_photo hash threshold, OR
       - Same base filename (for path-confirmed thumbnails in /Thumbnails/)

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

        # Path-confirmed thumbnails can also match by filename
        path_confirmed = _is_in_thumbnails_folder(thumb)
        thumb_base_names = {_get_base_filename(p) for p in _get_paths(thumb)}

        # Check if ANY master is larger and same photo
        for master in masters:
            master_res = _resolution(master)

            # Must be larger
            if master_res <= thumb_res:
                continue

            # Check hash similarity
            is_match = False
            if thumb_phash and thumb_dhash:
                master_phash = master.get("perceptual_hash")
                master_dhash = master.get("dhash")
                if master_phash and master_dhash:
                    phash_dist = hamming_distance(thumb_phash, master_phash)
                    dhash_dist = hamming_distance(thumb_dhash, master_dhash)
                    is_match = is_same_photo(phash_dist, dhash_dist)

            # For path-confirmed thumbnails, also check filename match
            if not is_match and path_confirmed:
                master_base_names = {_get_base_filename(p) for p in _get_paths(master)}
                if thumb_base_names & master_base_names:  # Any overlap
                    is_match = True

            if is_match:
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
    IPHOTO_COPY: Reject iPhoto version when same photo exists in Photos.app library.

    Prefer the newer Photos.app version over older iPhoto version.
    Uses is_same_photo hash check (allows slight resolution differences from
    library processing).
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

    # For each iPhoto photo, check if same photo exists in Photos
    for iphoto in iphoto_photos:
        iphoto_phash = iphoto.get("perceptual_hash")
        iphoto_dhash = iphoto.get("dhash")

        if not iphoto_phash or not iphoto_dhash:
            continue

        for photos in photos_photos:
            photos_phash = photos.get("perceptual_hash")
            photos_dhash = photos.get("dhash")

            if not photos_phash or not photos_dhash:
                continue

            # Same photo check (allows slight resolution differences)
            phash_dist = hamming_distance(iphoto_phash, photos_phash)
            dhash_dist = hamming_distance(iphoto_dhash, photos_dhash)
            if is_same_photo(phash_dist, dhash_dist):
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


def _has_library_generated_path(photo: dict) -> bool:
    """Check if photo has any library-generated path."""
    return any(_is_library_generated_path(p) for p in _get_paths(photo))


def _pick_dominated_same_res(p1: dict, p2: dict) -> str:
    """
    Pick which photo to reject when both are same resolution and same photo.

    Returns photo_id to reject.

    Priority:
    1. Prefer non-library path over library-generated path
    2. If both library or both non-library: prefer larger file size
    3. Arbitrary tiebreaker: keep first by id
    """
    lib1 = _has_library_generated_path(p1)
    lib2 = _has_library_generated_path(p2)

    # Rule 1: Prefer non-library over library
    if lib1 and not lib2:
        return p1["id"]
    if lib2 and not lib1:
        return p2["id"]

    # Rule 2: Both library or both non-library - prefer larger file
    size1 = p1.get("file_size") or 0
    size2 = p2.get("file_size") or 0

    if size1 > size2:
        return p2["id"]
    elif size2 > size1:
        return p1["id"]

    # Rule 3: Arbitrary - keep first by id (deterministic)
    if p1["id"] < p2["id"]:
        return p2["id"]
    else:
        return p1["id"]


def rule_same_res_duplicate(group: list[dict]) -> list[tuple[str, str]]:
    """
    SAME_RES_DUPLICATE: Reject duplicate when same photo exists at same resolution.

    For pairs that:
    - Pass is_same_photo threshold (pHash ≤ 2, or pHash ≤ 6 with dHash = 0)
    - Have same resolution (width * height), including rotated pairs

    Decision priority:
    1. Prefer non-library path over library-generated path
       (/Previews/, /Thumbnails/, /modelresources/)
    2. If both library or both non-library: prefer larger file size
    """
    rejections = []
    dominated = set()

    for i, photo in enumerate(group):
        if photo["id"] in dominated:
            continue

        photo_res = _resolution(photo)
        photo_phash = photo.get("perceptual_hash")
        photo_dhash = photo.get("dhash")

        if not photo_phash or not photo_dhash:
            continue

        for other in group[i + 1 :]:
            if other["id"] in dominated:
                continue

            other_res = _resolution(other)
            other_phash = other.get("perceptual_hash")
            other_dhash = other.get("dhash")

            if not other_phash or not other_dhash:
                continue

            # Must be same resolution (pixel count)
            if photo_res != other_res:
                continue

            # Must be same photo (strict threshold)
            phash_dist = hamming_distance(photo_phash, other_phash)
            dhash_dist = hamming_distance(photo_dhash, other_dhash)
            if not is_same_photo(phash_dist, dhash_dist):
                continue

            # Pick which one to reject
            dominated_id = _pick_dominated_same_res(photo, other)
            if dominated_id:
                dominated.add(dominated_id)

    for photo_id in dominated:
        rejections.append((photo_id, "SAME_RES_DUPLICATE"))

    return rejections


# =============================================================================
# RULE REGISTRY
# =============================================================================

# Rules in order of application.
# Each rule only sees photos not yet rejected by earlier rules.
# Order: THUMBNAIL -> PREVIEW -> IPHOTO_COPY -> DERIVATIVE -> SAME_RES_DUPLICATE
GROUP_RULES: list[GroupRuleFunc] = [
    rule_thumbnail,          # Reject thumbnails when larger exists
    rule_preview,            # Reject preview versions when larger exists
    rule_iphoto_copy,        # Prefer Photos.app over iPhoto
    rule_derivative,         # Reject resized versions (same content, smaller)
    rule_same_res_duplicate, # Reject same-res duplicates (prefer non-library, then larger)
]


def apply_group_rules(group: list[dict]) -> list[tuple[str, str]]:
    """
    Apply all group rules to a duplicate group.

    Rules are applied in order. Each rule only sees photos not yet rejected
    by earlier rules, so it's never possible to reject all photos.

    Returns list of (rejected_photo_id, rule_name) tuples.
    """
    all_rejections = []
    rejected_ids = set()

    for rule in GROUP_RULES:
        # Filter to only non-rejected photos for this rule
        remaining = [p for p in group if p["id"] not in rejected_ids]
        if len(remaining) < 2:
            # Need at least 2 photos to compare
            break
        rejections = rule(remaining)
        for rejected_id, rule_name in rejections:
            if rejected_id not in rejected_ids:
                all_rejections.append((rejected_id, rule_name))
                rejected_ids.add(rejected_id)

    return all_rejections
