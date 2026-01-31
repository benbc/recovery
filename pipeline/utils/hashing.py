"""SHA256 and perceptual hashing utilities."""

import hashlib
from pathlib import Path
from typing import Optional

import imagehash
from PIL import Image


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
    """
    try:
        with Image.open(file_path) as img:
            return str(imagehash.phash(img))
    except Exception:
        return None


def hamming_distance(hash1: str, hash2: str) -> int:
    """
    Calculate hamming distance between two hex hash strings.

    Lower distance = more similar images.
    """
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count("1")
