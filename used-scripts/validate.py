#!/usr/bin/env python3
"""
Quick validation tool to spot-check photo recovery results
"""

import sqlite3
import subprocess
import sys
from pathlib import Path

DB_PATH = Path("organized/photos.db")

def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)

    print("Photo Recovery Validation Tool")
    print("=" * 60)
    print()

    while True:
        print("\nOptions:")
        print("  1. View random high confidence photos")
        print("  2. View random medium confidence photos")
        print("  3. View random low confidence photos")
        print("  4. Search by path/filename")
        print("  5. Show statistics")
        print("  6. Open random photo in viewer")
        print("  q. Quit")
        print()

        choice = input("Choose option: ").strip()

        if choice == 'q':
            break
        elif choice == '1':
            show_samples(conn, min_score=70, limit=20)
        elif choice == '2':
            show_samples(conn, min_score=40, max_score=69, limit=20)
        elif choice == '3':
            show_samples(conn, max_score=39, limit=20)
        elif choice == '4':
            search_term = input("Enter search term: ").strip()
            search_photos(conn, search_term)
        elif choice == '5':
            show_statistics(conn)
        elif choice == '6':
            open_random_photo(conn)

def show_samples(conn, min_score=0, max_score=100, limit=20):
    """Show sample photos from a confidence range"""
    cursor = conn.execute(f"""
        SELECT original_path, confidence_score, camera_make, camera_model,
               date_taken, date_source, width, height, is_thumbnail
        FROM photos
        WHERE confidence_score >= {min_score} AND confidence_score <= {max_score}
        ORDER BY RANDOM()
        LIMIT {limit}
    """)

    print(f"\n{limit} Random samples (score {min_score}-{max_score}):")
    print("-" * 60)

    for row in cursor:
        path, score, make, model, date, date_src, w, h, thumb = row
        camera = f"{make} {model}" if make and model else "No camera info"
        print(f"\nScore {score}: {Path(path).name}")
        print(f"  Path: {path}")
        print(f"  Camera: {camera}")
        print(f"  Date: {date} (source: {date_src})")
        print(f"  Size: {w}x{h}, Thumbnail: {thumb}")

def search_photos(conn, search_term):
    """Search for photos by path"""
    cursor = conn.execute("""
        SELECT original_path, confidence_score, camera_make, camera_model, date_taken
        FROM photos
        WHERE original_path LIKE ?
        ORDER BY confidence_score DESC
        LIMIT 50
    """, (f'%{search_term}%',))

    results = cursor.fetchall()
    if not results:
        print(f"\nNo photos found matching '{search_term}'")
        return

    print(f"\nFound {len(results)} photos matching '{search_term}':")
    print("-" * 60)

    for path, score, make, model, date in results:
        camera = f"{make} {model}" if make and model else "No camera"
        print(f"Score {score}: {Path(path).name} - {camera}")

def show_statistics(conn):
    """Show detailed statistics"""
    print("\n" + "=" * 60)
    print("STATISTICS")
    print("=" * 60)

    # Total
    cursor = conn.execute("SELECT COUNT(*) FROM photos")
    total = cursor.fetchone()[0]
    print(f"\nTotal photos: {total}")

    # By confidence
    print("\nConfidence breakdown:")
    cursor = conn.execute("""
        SELECT
            CASE
                WHEN confidence_score >= 70 THEN 'High (>= 70)'
                WHEN confidence_score >= 40 THEN 'Medium (40-69)'
                ELSE 'Low (< 40)'
            END as bucket,
            COUNT(*),
            ROUND(AVG(confidence_score), 1),
            MIN(confidence_score),
            MAX(confidence_score)
        FROM photos
        GROUP BY bucket
        ORDER BY AVG(confidence_score) DESC
    """)

    for bucket, count, avg, min_s, max_s in cursor:
        pct = (count / total) * 100
        print(f"  {bucket}: {count:4d} photos ({pct:5.1f}%) - avg: {avg}, range: {min_s}-{max_s}")

    # Date sources
    print("\nDate sources:")
    cursor = conn.execute("""
        SELECT date_source, COUNT(*)
        FROM photos
        GROUP BY date_source
        ORDER BY COUNT(*) DESC
    """)

    for source, count in cursor:
        pct = (count / total) * 100
        print(f"  {source}: {count:4d} photos ({pct:5.1f}%)")

    # Cameras
    print("\nTop cameras:")
    cursor = conn.execute("""
        SELECT camera_make || ' ' || camera_model as camera, COUNT(*)
        FROM photos
        WHERE camera_make IS NOT NULL
        GROUP BY camera
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)

    for camera, count in cursor:
        print(f"  {camera}: {count} photos")

    # Duplicates
    cursor = conn.execute("SELECT COUNT(DISTINCT perceptual_hash) FROM photos")
    unique = cursor.fetchone()[0]
    print(f"\nUnique perceptual hashes: {unique}")
    print(f"Potential duplicates: {total - unique}")

def open_random_photo(conn):
    """Open a random photo in the default image viewer"""
    bucket = input("Which bucket? (h)igh, (m)edium, (l)ow: ").strip().lower()

    if bucket == 'h':
        min_score, max_score = 70, 100
    elif bucket == 'm':
        min_score, max_score = 40, 69
    elif bucket == 'l':
        min_score, max_score = 0, 39
    else:
        print("Invalid choice")
        return

    cursor = conn.execute(f"""
        SELECT original_path, confidence_score
        FROM photos
        WHERE confidence_score >= {min_score} AND confidence_score <= {max_score}
        ORDER BY RANDOM()
        LIMIT 1
    """)

    result = cursor.fetchone()
    if not result:
        print("No photos in that range")
        return

    path, score = result
    print(f"\nOpening: {path}")
    print(f"Score: {score}")

    # Try to open with xdg-open (Linux) or open (Mac)
    try:
        subprocess.run(['xdg-open', path], check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        try:
            subprocess.run(['open', path], check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"\nCouldn't auto-open. Photo location: {path}")

if __name__ == "__main__":
    main()
