"""
Individual Rules (Stage 2)

These classify photos based on their own properties alone, without knowing
about duplicates. Each rule returns a tuple of (decision, rule_name) or None.

Decisions:
- 'reject': Junk - discard this photo
- 'separate': Keep but handle separately from main pipeline
"""

import os
import re
from pathlib import Path
from typing import Callable, Optional


# Type alias for rule functions
RuleFunc = Callable[[dict], Optional[tuple[str, str]]]


def _get_paths(photo: dict) -> list[str]:
    """Get all source paths for a photo."""
    all_paths = photo.get("all_paths", "")
    if all_paths:
        return all_paths.split("|")
    return []


def _any_path_matches(photo: dict, pattern: str) -> bool:
    """Check if any path contains the pattern (case-insensitive)."""
    pattern_lower = pattern.lower()
    for path in _get_paths(photo):
        if pattern_lower in path.lower():
            return True
    return False


def _any_path_regex(photo: dict, pattern: str) -> bool:
    """Check if any path matches the regex pattern."""
    regex = re.compile(pattern, re.IGNORECASE)
    for path in _get_paths(photo):
        if regex.search(path):
            return True
    return False


# =============================================================================
# REJECTION RULES (junk - discard)
# =============================================================================

def rule_tiny_icon(photo: dict) -> Optional[tuple[str, str]]:
    """
    TINY_ICON: Reject images too small to be real photos.

    Condition: width * height < 5000 pixels
    Rationale: These are icons, emoji, or other UI elements
    """
    width = photo.get("width") or 0
    height = photo.get("height") or 0
    if width * height < 5000:
        return ("reject", "TINY_ICON")
    return None


def rule_minecraft_texture(photo: dict) -> Optional[tuple[str, str]]:
    """
    MINECRAFT_TEXTURE: Reject Minecraft game assets.

    Condition: path contains 'minecraft'
    Rationale: Game textures, not personal photos
    """
    if _any_path_matches(photo, "minecraft"):
        return ("reject", "MINECRAFT_TEXTURE")
    return None


def rule_hue_animation(photo: dict) -> Optional[tuple[str, str]]:
    """
    HUE_ANIMATION: Reject HUE stop-motion animation frames.

    Condition: path contains 'HUE Animation'
    Rationale: Animation software frames, not personal photos
    """
    if _any_path_matches(photo, "HUE Animation"):
        return ("reject", "HUE_ANIMATION")
    return None


def rule_ichat_icon(photo: dict) -> Optional[tuple[str, str]]:
    """
    ICHAT_ICON: Reject iChat/Messages icon folders.

    Condition: path contains '/iChat Icons/' or similar
    Rationale: Chat app emoticons and icons
    """
    patterns = ["/iChat Icons/", "/Messages/", "/Skype/"]
    for pattern in patterns:
        if _any_path_matches(photo, pattern):
            # Check it's actually an icon (small dimension)
            width = photo.get("width") or 0
            height = photo.get("height") or 0
            if max(width, height) < 200:
                return ("reject", "ICHAT_ICON")
    return None


def rule_web_asset(photo: dict) -> Optional[tuple[str, str]]:
    """
    WEB_ASSET: Reject images saved as part of web pages.

    Condition: in a *_files/ directory AND companion .htm file exists
    Rationale: Browser-saved web page assets, not personal photos
    """
    for path in _get_paths(photo):
        # Check if in *_files directory pattern (browser save pattern)
        match = re.search(r"(.+)_files/", path, re.IGNORECASE)
        if match:
            base = match.group(1)
            # Check for companion HTML file
            for ext in [".htm", ".html"]:
                if os.path.exists(base + ext):
                    return ("reject", "WEB_ASSET")
    return None


def rule_face_crop(photo: dict) -> Optional[tuple[str, str]]:
    """
    FACE_CROP: Reject Photos.app face detection crops.

    Condition: in /modelresources/, square, max dimension <= 500px
    Rationale: Auto-generated face thumbnails for recognition
    """
    if not _any_path_matches(photo, "/modelresources/"):
        return None

    width = photo.get("width") or 0
    height = photo.get("height") or 0

    # Must be small (max dimension <= 500px)
    if max(width, height) > 500:
        return None

    # Must be roughly square (within 10%)
    if width == 0 or height == 0:
        return None
    aspect = width / height
    if abs(aspect - 1.0) > 0.1:
        return None

    return ("reject", "FACE_CROP")


def rule_stock_greeting(photo: dict) -> Optional[tuple[str, str]]:
    """
    STOCK_GREETING: Reject stock greeting card template images.

    Condition: 3-digit filename in /Thumbnails/ path
    Rationale: Built-in greeting card templates, not personal photos
    """
    for path in _get_paths(photo):
        if "/thumbnails/" not in path.lower():
            continue

        filename = Path(path).stem
        # Remove _1024 suffix if present
        filename = re.sub(r"_1024$", "", filename)

        # Check if it's exactly 3 digits
        if re.match(r"^\d{3}$", filename):
            return ("reject", "STOCK_GREETING")

    return None


def rule_flag_icon(photo: dict) -> Optional[tuple[str, str]]:
    """
    FLAG_ICON: Reject country flag icons.

    Condition: in known flag icons folder (20121223-175144)
    Rationale: System icons imported into iPhoto
    Note: This rule may need expansion - review when implementing
    """
    if _any_path_matches(photo, "20121223-175144"):
        return ("reject", "FLAG_ICON")
    return None


def rule_system_cache(photo: dict) -> Optional[tuple[str, str]]:
    """
    SYSTEM_CACHE: Reject files in system cache/temp directories.

    Condition: path contains cache/temp patterns
    Rationale: Transient files, not permanent photos
    """
    cache_patterns = [
        "/.cache/",
        "/cache/",
        "/.thumbnails/",
        "/temp/",
        "/.Trash/",
        "/Trash/",
        # Note: FlipShare handled by rule_flip_video_thumb
        "/My Flip Video Prefs/",
    ]
    for pattern in cache_patterns:
        if _any_path_matches(photo, pattern):
            return ("reject", "SYSTEM_CACHE")
    return None


def rule_flip_video_thumb(photo: dict) -> Optional[tuple[str, str]]:
    """
    FLIP_VIDEO_THUMB: Reject FlipShare video preview thumbnails.

    Condition: in FlipShare Data/Previews/ path
    Rationale: Auto-generated video previews
    """
    if _any_path_matches(photo, "/FlipShare Data/Previews/"):
        return ("reject", "FLIP_VIDEO_THUMB")
    return None


# =============================================================================
# SEPARATION RULES (keep but handle separately)
# =============================================================================

def rule_father_in_law(photo: dict) -> Optional[tuple[str, str]]:
    """
    FATHER_IN_LAW: Separate father-in-law's digitized collection.

    Condition: path matches %/tor/Pictures/2013/03/03/%
    Rationale: Separate digitized collection, needs different handling
    """
    if _any_path_regex(photo, r"/tor/Pictures/2013/03/03/"):
        return ("separate", "FATHER_IN_LAW")
    return None


def rule_photobooth(photo: dict) -> Optional[tuple[str, str]]:
    """
    PHOTOBOOTH: Separate all Photo Booth photos for manual curation.

    Condition: path matches 'Photo Booth Library/Originals/' or 'Photo Booth Library/Pictures/'
    Rationale: Need manual review to select best shots; separating both
               means they won't be grouped with non-Photo Booth photos
    """
    if _any_path_matches(photo, "Photo Booth Library/Originals/"):
        return ("separate", "PHOTOBOOTH")
    if _any_path_matches(photo, "Photo Booth Library/Pictures/"):
        return ("separate", "PHOTOBOOTH")
    return None


# =============================================================================
# RULE REGISTRY
# =============================================================================

# Rejection rules - applied in order, first match wins
REJECTION_RULES: list[RuleFunc] = [
    rule_tiny_icon,
    rule_minecraft_texture,
    rule_hue_animation,
    rule_ichat_icon,
    rule_web_asset,
    rule_face_crop,
    rule_stock_greeting,
    rule_flag_icon,
    rule_system_cache,
    rule_flip_video_thumb,
]

# Separation rules - applied in order, first match wins
SEPARATION_RULES: list[RuleFunc] = [
    rule_father_in_law,
    rule_photobooth,
]

# All individual rules
INDIVIDUAL_RULES: list[RuleFunc] = REJECTION_RULES + SEPARATION_RULES


def apply_individual_rules(photo: dict) -> Optional[tuple[str, str]]:
    """
    Apply all individual rules to a photo.

    Returns (decision, rule_name) or None if no rule matches.
    Separation rules are checked first - if a photo is separated,
    we don't need to check rejection rules.
    """
    # Check separation rules first
    for rule in SEPARATION_RULES:
        result = rule(photo)
        if result:
            return result

    # Then rejection rules
    for rule in REJECTION_RULES:
        result = rule(photo)
        if result:
            return result

    return None
