# Photo Recovery Plan

## Overview

Extract and organize ~210k image files from a 248GB archive of old computer backups. The archive contains valuable family photos mixed with system files, thumbnails, stock images, and other non-personal content. Since this is a recovery scenario, some original photos may be lost and only thumbnails remain.

## Current Archive Structure

- **Location**: `~/photo-recovery/`
  - `red-drive/`: 9.6GB, ~20k files, ~2.1k images
  - `RECOVERY/`: 239GB, ~435k files, ~210k images (MUCH LARGER)
- **Total**: 248GB, ~210k image files
- **Content**: Complete computer backups including:
  - Apple Photos/iPhoto libraries (possibly incomplete)
  - Loose photo directories
  - Downloads, documents, system files
  - Trash/recycle bins

## Goals

1. **Extract all potentially valuable photos** from the messy archive
2. **Deduplicate intelligently** - remove exact duplicates and keep best quality versions
3. **Preserve thumbnails** when no original exists (data loss scenario)
4. **Score confidence** - likelihood each image is a personal photo vs stock/junk
5. **Extract metadata** for browsing/organizing later
6. **Prepare for web UI** - database structure for future webapp

## Phase 1: Metadata Extraction & Deduplication Script

### Scanning Strategy

**Scan entire ~/photo-recovery directory** - don't focus only on obvious photo locations due to messy structure.

**Identify images by MIME type** (using `python-magic`), not just extensions, since:
- Files may lack extensions
- Files may have wrong extensions
- Photos may be in any format (JPEG, PNG, GIF, BMP, TIFF, RAW, etc.)

### What to Include

**Everything except:**
- System files: `.DS_Store`, `Thumbs.db`, `desktop.ini`
- Mac resource forks: `._*` files (these are metadata, not photos)
- Very small images in system paths (< 200px, likely UI icons)

**Specifically include:**
- Photos in obvious locations (Pictures, Photos, DCIM, Camera)
- Photos in random locations (desktop, documents, downloads)
- Thumbnails (may be only remaining copy)
- Photos in trash/recycle bins (may contain deleted originals)
- Photos with no EXIF data (converted/edited photos)

### Metadata Extraction

For each image file:

1. **Calculate hash** (SHA256 or MD5) - for exact duplicate detection
2. **Calculate perceptual hash** (using `imagehash`) - for finding resized versions
3. **Extract EXIF data** (using `piexif` or `exifread`):
   - Date taken (DateTimeOriginal, CreateDate)
   - Camera make/model
   - GPS coordinates
   - Image dimensions
4. **Extract file metadata**:
   - File size, mtime, ctime
   - MIME type
   - Original file path
5. **Analyze path** for context clues:
   - Date patterns in path/filename
   - Event names in parent directories
   - Photo library vs loose file
   - Whether it's in a thumbnail directory

### Deduplication Strategy

**Exact duplicates** (same hash):
- Keep only one copy
- Don't track that there were duplicates

**Logical duplicates** (resized/thumbnail versions):
- Group by perceptual hash similarity (hamming distance < threshold)
- Within each group:
  - Prefer originals over thumbnails (by path analysis)
  - Keep highest resolution version
  - If only thumbnails exist, keep best available
- Mark in metadata: `is_thumbnail`, `best_available`

**Apple Photo Libraries** (iPhoto, Photos.app):
- May have originals in: `Masters/`, `resources/`, or loose directories
- Will have thumbnails in: `Thumbnails/`, `Previews/`
- Match thumbnails to originals via perceptual hashing
- Only keep thumbnail if no corresponding original found

### Confidence Scoring (0-100)

Score likelihood that image is a personal photo worth reviewing:

**Add points for:**
- Has camera EXIF (make/model): +40
- Camera pattern filename (IMG_, DSC_, P\d{7}, etc.): +20
- In photo directory (Pictures, Photos, DCIM): +20
- Reasonable dimensions (>800px): +10
- Has GPS data: +10

**Subtract points for:**
- In cache/temp directories: -30
- Path contains: stock, wallpaper, icon, template: -20
- Very small (< 200px): -20
- Generic web filename pattern: -10

**Conservative approach**: Prefer false positives (keeping junk) over false negatives (losing real photos)

### Database Schema (SQLite)

```sql
CREATE TABLE photos (
  id TEXT PRIMARY KEY,           -- hash (SHA256)
  path TEXT,                     -- organized output path
  original_path TEXT,            -- source path for reference

  -- Dating
  date_taken DATETIME,           -- best available date
  date_source TEXT,              -- 'exif', 'filename', 'mtime'

  -- Camera
  camera_make TEXT,
  camera_model TEXT,

  -- Image properties
  width INTEGER,
  height INTEGER,
  file_size INTEGER,
  mime_type TEXT,

  -- Location
  has_gps BOOLEAN,
  latitude REAL,
  longitude REAL,

  -- Quality/confidence
  is_thumbnail BOOLEAN,          -- is this a thumbnail/preview?
  best_available BOOLEAN,        -- is this the best version we have?
  confidence_score INTEGER,      -- 0-100

  -- Perceptual hash for duplicate detection
  perceptual_hash TEXT,

  -- Future webapp features
  tags TEXT,                     -- JSON array
  is_favorite BOOLEAN,

  -- Timestamps
  created_at DATETIME,
  updated_at DATETIME
);

CREATE INDEX idx_date_taken ON photos(date_taken);
CREATE INDEX idx_confidence ON photos(confidence_score);
CREATE INDEX idx_perceptual_hash ON photos(perceptual_hash);
CREATE INDEX idx_camera ON photos(camera_make, camera_model);
```

### Output Structure

```
organized/
├── photos.db                  -- SQLite database
└── images/
    ├── high_confidence/       -- Score >= 70 (likely personal photos)
    │   └── <hash>.jpg
    ├── medium_confidence/     -- Score 40-69 (uncertain)
    │   └── <hash>.png
    └── low_confidence/        -- Score < 40 (likely junk but kept)
        └── <hash>.gif
```

Files organized by confidence for easier manual review. Can query database to reorganize by date/camera/etc later.

### Implementation Details

**Performance considerations** (210k images):
- Progress bar with logging every 1000 files (using `tqdm`)
- Batch database commits (every 1000 records)
- Resume capability - skip already-processed files
- Multiprocessing for CPU-bound operations (hashing, perceptual hashing)

**Python dependencies:**
```python
dependencies = [
  "python-magic",      # MIME type detection
  "piexif",            # EXIF extraction (or "exifread")
  "pillow",            # Image operations, dimensions
  "imagehash",         # Perceptual hashing for duplicate detection
  "tqdm"               # Progress bars
]
```

**Error handling:**
- Corrupt/unreadable images: log and skip, don't crash
- Missing EXIF: gracefully degrade to file metadata
- Permission errors: log and continue

## Phase 2: Web Viewer (Future)

**Not implementing yet**, but database schema designed for:

**Features:**
- Timeline view (by date taken/created)
- Grid/gallery view with lazy loading
- Filter by: date range, confidence score, camera, thumbnails-only
- Search by: filename, path, metadata
- Tag and favorite photos (updates database)
- Flag for deletion
- View metadata sidebar
- Export selections to organized folder structure

**Tech stack options:**
- Simple: Flask/FastAPI + htmx + SQLite (single file server)
- Modern: Python backend + React/Vue frontend
- Ultra-simple: Static site generator with JS search

**Actions from webapp:**
- Tag/favorite photos
- Mark for deletion
- Export selections to date-based or other folder structure
- Update confidence scores manually

## Phase 3: Export/Organization (Future)

Once photos are reviewed and tagged via webapp:
- Export to date-based folder structure (YYYY/MM/DD or YYYY-MM)
- Export favorites/tagged collections
- Delete flagged duplicates/junk
- Generate photo album/archive

## Key Principles

1. **Conservative**: Don't delete anything we're uncertain about
2. **Comprehensive**: Scan everything, not just obvious photo locations
3. **Preserve quality**: Keep best available version, but note when only thumbnails exist
4. **Metadata-rich**: Extract everything possible for future querying
5. **Recoverable**: Keep original paths so we can always trace back to source
6. **Flexible**: Database allows reorganizing in multiple ways without moving files

## TODO List

### Phase 1 Enhancements (Not Yet Implemented)

1. **Filename date parsing** - Extract dates from filename patterns like:
   - `IMG_20231225_*.jpg`
   - `2023-12-25_*.jpg`
   - `DSC_20231225*.jpg`
   - Use as fallback between EXIF and mtime

2. **Path-based date extraction** - Parse dates from directory names:
   - `2007_09_24/`, `2008_0502/`, etc.
   - Year-based directories: `2012/`, `2013/`
   - Event names with dates

3. **Perceptual hash deduplication (Phase 2)** - After initial scan:
   - Group photos by similar perceptual hashes
   - Within each group, identify highest quality version
   - Mark lower quality versions as `best_available = false`
   - Consider keeping only best version or flagging others for deletion

4. **Path analysis for event/context extraction** - Mine path for:
   - Event names (birthdays, holidays, locations)
   - Album names
   - Source device hints (though not prioritized per requirements)

## Next Steps

1. ✅ Enhance existing `recovery` script with full metadata extraction
2. Test on `red-drive` subdirectory first (smaller, 9.6GB)
3. Run on full `RECOVERY` directory (239GB)
4. Review database and sample images at different confidence levels
5. Implement Phase 2 deduplication pass using perceptual hashes
6. Build web viewer when ready to manually curate
