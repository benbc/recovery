# Album Selection App

Throwaway app for selecting photos of Ben's daughter for an album.

## Current State

Working prototype with:
- Grid view: 200px thumbnails, 28 per page (4×7)
- Navigation: Space to advance, browser back to go back
- Preloading: Next page images prefetched in background
- Performance: Photo ID list cached at startup for fast pagination
- Filtering: `is_non_photo = 0` and not rejected in `duplicate_groups`
- Order: By `date_taken` ascending
- Photo count: ~30,852 photos across ~1,102 pages

## Remaining Work

### Selection functionality
- [ ] Click photo to toggle selection (colored border)
- [ ] Track selections in memory during session
- [ ] Show count in header: "X selected"

### Persistence
- [ ] Save selections to database on page forward
- [ ] New table: `album_selections` with photo_id
- [ ] Track last viewed page for resume
- [ ] Auto-resume: jump to first unchecked page on startup

### Export
- [ ] Copy selected photos to output directory (can be manual/script)

## Technical Notes

- Database: `organized/photos.db`
- Images served from: `organized/` directory (path field is relative to this)
- `path` = organized copy, `original_path` = original location
- Key fields: `is_non_photo`, `is_thumbnail`, `confidence_score`, `date_taken`
- 32,390 thumbnails included (originals lost, thumbnails are best available)
- Average image size: 820KB (preloading helps, could generate smaller thumbs later)

## Schema Reference

```sql
-- Key photo fields
photos.is_non_photo     -- 1 = rejected (minecraft, face crops, etc.)
photos.is_thumbnail     -- 1 = thumbnail (but may be best available)
photos.confidence_score -- 0-100, higher = more likely real photo
photos.date_taken       -- best guess date (exif > filename > mtime)
photos.date_source      -- 'exif', 'filename', or 'mtime'

-- Duplicate rejection
duplicate_groups.rejected -- 1 = rejected during dedup review
```
