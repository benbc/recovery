"""Pipeline configuration - paths, thresholds, and constants."""

from pathlib import Path

# Source directory containing archived computer contents
SOURCE_ROOT = Path("/home/ben/photo-recovery")

# Output directory for new pipeline (separate from old organized/)
OUTPUT_ROOT = Path("/home/ben/src/benbc/recovery/output")

# Database path
DB_PATH = OUTPUT_ROOT / "photos.db"

# Directory for linked files (hardlinks to originals, simple paths for serving)
FILES_DIR = OUTPUT_ROOT / "files"

# Directory for exported photos
EXPORT_DIR = OUTPUT_ROOT / "exported"

# Old database path (for importing hashes from previous pipeline)
OLD_DB_PATH = Path("/home/ben/src/benbc/recovery/archive/organized/photos.db")

# Perceptual hash hamming distance thresholds
# Based on visual sampling of pHash and dHash combinations

# Same photo detection (high confidence)
#   pHash ≤2: definitely same photo
#   pHash ≤6 AND dHash=0: also same photo
PHASH_SAME_PHOTO = 2
PHASH_SAME_PHOTO_WITH_DHASH = 6
DHASH_SAME_PHOTO = 0

# Grouping thresholds (same scene detection)
#   pHash ≤10: reliable same scene
#   pHash 12: group if dHash <22, exclude if ≥22
#   pHash 14: group if dHash ≤17, exclude if >17
#   pHash >14: different scene
PHASH_SAFE_GROUP = 10
PHASH_BORDERLINE_12 = 12
PHASH_BORDERLINE_14 = 14
DHASH_EXCLUDE_AT_12 = 22  # dHash ≥22 at pHash 12 → exclude
DHASH_INCLUDE_AT_14 = 17  # dHash ≤17 at pHash 14 → include

# Image MIME types to process, with their file extensions
MIME_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
    "image/webp": ".webp",
    "image/heic": ".heic",
    "image/heif": ".heif",
}

IMAGE_MIME_TYPES = set(MIME_TO_EXT.keys())

# Files to always skip
EXCLUDE_FILENAMES = {".DS_Store", "Thumbs.db", "desktop.ini", ".picasa.ini"}

# Batch size for database commits
BATCH_SIZE = 1000
