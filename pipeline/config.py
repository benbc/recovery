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

# Perceptual hash hamming distance threshold for grouping
# Distances are even numbers (0, 2, 4, 6, 8, 10, 12...)
# Lower = stricter (fewer false positives), Higher = more inclusive
HAMMING_THRESHOLD = 8

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
