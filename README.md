# Photo Recovery - Full Run

## Configuration

The script is now configured to process the **full ~/photo-recovery directory** (248GB, ~210k images).

**Key settings:**
- Source: `/home/ben/photo-recovery` (entire directory)
- Output: `./organized/`
- **Perceptual hashing: DISABLED** for 9x speedup
- Estimated runtime: **~40 minutes**

## Running the Script

### Option 1: Background (Recommended)
```bash
./run_full_recovery.sh
```

This runs in the background and survives disconnection.

### Option 2: Foreground
```bash
./recovery 2>&1 | tee recovery_full.log
```

### Option 3: In screen/tmux
```bash
screen -S photo-recovery
./recovery 2>&1 | tee recovery_full.log
# Ctrl-A D to detach
```

## Monitoring Progress

### Check if still running
```bash
ps aux | grep recovery | grep -v grep
```

### Monitor log file
```bash
tail -f recovery_full.log
```

### Check database progress
```bash
python3 -c "import sqlite3; conn = sqlite3.connect('organized/photos.db'); print('Photos processed:', conn.execute('SELECT COUNT(*) FROM photos').fetchone()[0])"
```

### Watch stats in real-time
```bash
watch -n 10 "python3 -c \"import sqlite3; conn = sqlite3.connect('organized/photos.db'); cursor = conn.execute('SELECT COUNT(*) FROM photos'); print('Total:', cursor.fetchone()[0]); cursor = conn.execute('SELECT CASE WHEN confidence_score >= 70 THEN \\\"High\\\" WHEN confidence_score >= 40 THEN \\\"Med\\\" ELSE \\\"Low\\\" END as b, COUNT(*) FROM photos GROUP BY b'); [print(f\\\"{r[0]}: {r[1]}\\\") for r in cursor]\""
```

## Expected Results

Based on red-drive test (scaled up):

- **Total images**: ~210,000
- **Processing rate**: ~87 images/sec (without perceptual hashing)
- **Runtime**: ~40 minutes
- **High confidence**: ~80% (personal photos with EXIF)
- **Medium confidence**: ~12% (video thumbnails, etc.)
- **Low confidence**: ~8% (tiny thumbnails, graphics)

## Output Structure

```
organized/
├── photos.db                    # SQLite database with all metadata
└── images/
    ├── high_confidence/         # Likely personal photos
    ├── medium_confidence/       # Uncertain
    └── low_confidence/          # Likely junk (kept for safety)
```

## After Completion

### 1. Validate Results
```bash
python3 validate.py
```

Interactive tool to browse samples, search, and spot-check.

### 2. View Statistics
```bash
./recovery   # Already in database, will show stats quickly
```

Or use the validate tool (option 5).

### 3. Next Steps

**Option A: Browse and review**
- Use validate.py to explore results
- Build simple webapp if needed

**Option B: Run Phase 2 deduplication**
If you want to remove logical duplicates (resized versions), run perceptual hashing in a second pass:
1. Edit `recovery` script: set `ENABLE_PERCEPTUAL_HASH = True`
2. Re-run (will be ~6 hours, only updates perceptual_hash field)
3. Run deduplication analysis

## Troubleshooting

### Script seems stuck
Some large/corrupt images take time. Check log for progress bars.

### Out of disk space
Each photo is hardlinked (not copied), so uses minimal extra space. But database can grow to ~1-2GB.

### Want to restart
```bash
rm -rf organized/
./recovery
```

Resume capability will skip already-processed files if you don't delete organized/.

## Files

- `recovery` - Main recovery script
- `validate.py` - Interactive validation tool
- `benchmark.py` - Performance testing
- `PLAN.md` - Full project plan and TODO list
- `recovery_full.log` - Log output
- `organized/photos.db` - SQLite database

## Performance Notes

Without perceptual hashing:
- File scanning: ~87 files/sec
- Image processing: ~87 images/sec (6.8ms per image for metadata)
- Bottleneck: Disk I/O and EXIF parsing

With perceptual hashing (Phase 2):
- Image processing: ~9 images/sec (65ms per image)
- Bottleneck: Perceptual hash computation (90% of time)
