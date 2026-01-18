#!/usr/bin/env python3
# /// script
# dependencies = ["flask"]
# ///
"""
Web UI for reviewing duplicate photo groups.

This Flask app provides an interface to review duplicate groups,
allowing you to select which photos to keep (including multiple
if they're legitimately different).
"""

from flask import Flask, render_template, request, jsonify, send_file
import sqlite3
from pathlib import Path

app = Flask(__name__)

DB_PATH = Path("../organized/photos.db")
OUTPUT_ROOT = Path("../organized")

# Add number formatting filter
@app.template_filter('number_format')
def number_format(value):
    """Format number with commas."""
    try:
        return "{:,}".format(int(value))
    except (ValueError, TypeError):
        return value

def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    """Main page - show overview of duplicate groups."""
    conn = get_db()

    # Get statistics (all groups)
    cursor = conn.execute("""
        SELECT
            COUNT(DISTINCT group_id) as total_groups,
            COUNT(*) as total_photos,
            SUM(CASE WHEN is_suggested_keeper = 1 THEN 1 ELSE 0 END) as suggested_keeps
        FROM duplicate_groups
    """)
    stats = cursor.fetchone()

    # Get auto-resolved statistics
    cursor = conn.execute("""
        SELECT
            COUNT(DISTINCT group_id) as auto_resolved_groups,
            SUM(CASE WHEN is_suggested_keeper = 1 THEN 1 ELSE 0 END) as auto_keeps,
            SUM(CASE WHEN is_suggested_keeper = 0 THEN 1 ELSE 0 END) as auto_rejects
        FROM duplicate_groups
        WHERE auto_resolved = 1
    """)
    auto_stats = cursor.fetchone()

    # Get manual review statistics
    cursor = conn.execute("""
        SELECT
            COUNT(DISTINCT group_id) as manual_groups,
            COUNT(*) as manual_photos
        FROM duplicate_groups
        WHERE auto_resolved = 0
    """)
    manual_stats = cursor.fetchone()

    # Get group size distribution
    cursor = conn.execute("""
        SELECT group_size, COUNT(*) as count
        FROM (
            SELECT group_id, COUNT(*) as group_size
            FROM duplicate_groups
            GROUP BY group_id
        )
        GROUP BY group_size
        ORDER BY group_size DESC
    """)
    size_dist = cursor.fetchall()

    conn.close()

    return render_template('index.html',
                         stats=stats,
                         auto_stats=auto_stats,
                         manual_stats=manual_stats,
                         size_dist=size_dist)

@app.route('/groups')
def groups():
    """List all duplicate groups (excluding auto-resolved)."""
    conn = get_db()

    # Get only groups requiring manual review (not auto-resolved)
    cursor = conn.execute("""
        SELECT
            dg.group_id,
            dg.group_size,
            MIN(p.path) as preview_path
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.auto_resolved = 0
        GROUP BY dg.group_id
        ORDER BY dg.group_size DESC, dg.group_id
    """)
    groups = cursor.fetchall()

    conn.close()

    return render_template('groups.html', groups=groups)

@app.route('/group/<int:group_id>')
def group_detail(group_id):
    """Show details of a specific group."""
    conn = get_db()

    # Get all photos in this group
    cursor = conn.execute("""
        SELECT
            dg.photo_id,
            dg.rank_in_group,
            dg.group_size,
            dg.width,
            dg.height,
            dg.file_size,
            dg.quality_score,
            dg.is_suggested_keeper,
            p.path,
            p.original_path,
            p.confidence_score
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ?
        ORDER BY dg.rank_in_group
    """, (group_id,))
    photos = cursor.fetchall()

    # Get navigation info (prev/next group) - only for manual review groups
    cursor = conn.execute("""
        SELECT DISTINCT group_id
        FROM duplicate_groups
        WHERE auto_resolved = 0
        ORDER BY group_id
    """)
    manual_review_groups = [row['group_id'] for row in cursor.fetchall()]

    current_idx = manual_review_groups.index(group_id) if group_id in manual_review_groups else -1
    prev_group = manual_review_groups[current_idx - 1] if current_idx > 0 else None
    next_group = manual_review_groups[current_idx + 1] if current_idx < len(manual_review_groups) - 1 else None

    conn.close()

    return render_template('group_detail.html',
                         group_id=group_id,
                         photos=photos,
                         prev_group=prev_group,
                         next_group=next_group,
                         current_idx=current_idx + 1,
                         total_groups=len(manual_review_groups))

@app.route('/image/<path:filepath>')
def serve_image(filepath):
    """Serve an image file."""
    image_path = OUTPUT_ROOT / filepath
    if not image_path.exists():
        return "Image not found", 404
    return send_file(image_path)

@app.route('/thumbnail/<path:filepath>')
def serve_thumbnail(filepath):
    """Serve a thumbnail (for now, just the same image - could add real thumbnails later)."""
    return serve_image(filepath)

@app.route('/api/mark_keeps', methods=['POST'])
def mark_keeps():
    """Mark selected photos as keepers."""
    data = request.json
    group_id = data.get('group_id')
    keep_ids = data.get('keep_ids', [])

    if not group_id or not keep_ids:
        return jsonify({'error': 'Missing group_id or keep_ids'}), 400

    conn = get_db()

    # First, unmark all photos in this group
    conn.execute("""
        UPDATE duplicate_groups
        SET is_suggested_keeper = 0
        WHERE group_id = ?
    """, (group_id,))

    # Then mark the selected ones
    if keep_ids:
        placeholders = ','.join('?' * len(keep_ids))
        conn.execute(f"""
            UPDATE duplicate_groups
            SET is_suggested_keeper = 1
            WHERE group_id = ? AND photo_id IN ({placeholders})
        """, [group_id] + keep_ids)

    conn.commit()
    conn.close()

    return jsonify({'success': True})

@app.route('/api/stats')
def api_stats():
    """Get current statistics."""
    conn = get_db()

    cursor = conn.execute("""
        SELECT
            COUNT(DISTINCT group_id) as total_groups,
            COUNT(*) as total_photos,
            SUM(CASE WHEN is_suggested_keeper = 1 THEN 1 ELSE 0 END) as keeps,
            COUNT(*) - SUM(CASE WHEN is_suggested_keeper = 1 THEN 1 ELSE 0 END) as rejects
        FROM duplicate_groups
    """)
    stats = dict(cursor.fetchone())

    conn.close()

    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
