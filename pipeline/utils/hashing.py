"""SHA256 and perceptual hashing utilities."""

import hashlib
from pathlib import Path
from typing import Optional

import imagehash
from PIL import Image, ImageOps


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_perceptual_hash(file_path: Path) -> Optional[str]:
    """
    Compute perceptual hash (pHash) of an image.

    Returns hex string of the hash, or None if the image can't be processed.
    Applies EXIF rotation normalization before hashing.
    """
    try:
        with Image.open(file_path) as img:
            img = ImageOps.exif_transpose(img)
            return str(imagehash.phash(img))
    except Exception:
        return None


def compute_dhash(file_path: Path) -> Optional[str]:
    """
    Compute difference hash (dHash) of an image.

    dHash is based on gradient direction and is good for detecting
    crops and edits. Returns hex string or None if can't be processed.
    Applies EXIF rotation normalization before hashing.
    """
    try:
        with Image.open(file_path) as img:
            img = ImageOps.exif_transpose(img)
            return str(imagehash.dhash(img))
    except Exception:
        return None


def compute_hashes(file_path: Path) -> tuple[Optional[str], Optional[str]]:
    """
    Compute both pHash and dHash for an image.

    Returns (phash, dhash) tuple. Either may be None on error.
    More efficient than calling separately as image is only opened once.
    """
    try:
        with Image.open(file_path) as img:
            img = ImageOps.exif_transpose(img)
            phash = str(imagehash.phash(img))
            dhash = str(imagehash.dhash(img))
            return (phash, dhash)
    except Exception:
        return (None, None)


def hamming_distance(hash1: str, hash2: str) -> int:
    """
    Calculate hamming distance between two hex hash strings.

    Lower distance = more similar images.
    """
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count("1")


# =============================================================================
# Hash comparison predicates
#
# These implement the threshold rules documented in RULES.md.
# Use these consistently throughout the codebase for hash comparisons.
# =============================================================================

# Thresholds (duplicated from config to avoid circular imports)
_PHASH_SAME_PHOTO = 2
_PHASH_SAME_PHOTO_WITH_DHASH = 6
_DHASH_SAME_PHOTO = 0

_PHASH_SAFE_GROUP = 10
_PHASH_BORDERLINE_12 = 12
_PHASH_BORDERLINE_14 = 14
_DHASH_EXCLUDE_AT_12 = 22
_DHASH_INCLUDE_AT_14 = 17


def is_same_photo(phash_dist: int, dhash_dist: int) -> bool:
    """
    Determine if two photos are the same photo (high confidence).

    Same photo means identical content with minor processing differences
    (resizing, compression, etc). Used for DERIVATIVE and THUMBNAIL rules.

    Criteria (from RULES.md):
    - pHash ≤2: definitely same photo
    - pHash ≤6 AND dHash=0: also same photo
    """
    if phash_dist <= _PHASH_SAME_PHOTO:
        return True
    if phash_dist <= _PHASH_SAME_PHOTO_WITH_DHASH and dhash_dist == _DHASH_SAME_PHOTO:
        return True
    return False


def is_same_scene(phash_dist: int, dhash_dist: int) -> bool:
    """
    Determine if two photos are from the same scene (for grouping).

    Same scene means photos that should be grouped together as duplicates
    or near-duplicates. More permissive than is_same_photo().

    Criteria (from RULES.md):
    - pHash ≤10: group (reliable same scene)
    - pHash 11-12: group if dHash <22
    - pHash 13-14: group if dHash ≤17
    - pHash >14: don't group
    """
    if phash_dist <= _PHASH_SAFE_GROUP:
        return True
    elif phash_dist <= _PHASH_BORDERLINE_12:
        return dhash_dist < _DHASH_EXCLUDE_AT_12
    elif phash_dist <= _PHASH_BORDERLINE_14:
        return dhash_dist <= _DHASH_INCLUDE_AT_14
    else:
        return False
