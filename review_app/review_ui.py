#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = ["flask"]
# ///
"""
Web UI for reviewing duplicate photo groups.

Groups with 2+ non-rejected photos need manual review.
Single-photo groups (after rejections) are considered resolved.
"""

from flask import Flask, render_template, request, jsonify, send_file
import sqlite3
from pathlib import Path

app = Flask(__name__)

DB_PATH = Path("../organized/photos.db")
OUTPUT_ROOT = Path("../organized")


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
    """Main page - show overview."""
    conn = get_db()

    # Total photos in duplicate groups
    cursor = conn.execute("""
        SELECT COUNT(*) as total_photos FROM duplicate_groups
    """)
    total_photos = cursor.fetchone()['total_photos']

    # Rejected photos
    cursor = conn.execute("""
        SELECT COUNT(*) as rejected FROM duplicate_groups WHERE rejected = 1
    """)
    rejected_count = cursor.fetchone()['rejected']

    # Groups needing review (2+ non-rejected photos)
    cursor = conn.execute("""
        SELECT COUNT(*) as count FROM (
            SELECT group_id
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        )
    """)
    groups_needing_review = cursor.fetchone()['count']

    # Photos in groups needing review
    cursor = conn.execute("""
        SELECT SUM(cnt) as total FROM (
            SELECT COUNT(*) as cnt
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        )
    """)
    row = cursor.fetchone()
    photos_needing_review = row['total'] if row['total'] else 0

    # Resolved groups (1 non-rejected photo remaining)
    cursor = conn.execute("""
        SELECT COUNT(*) as count FROM (
            SELECT group_id
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING COUNT(*) = 1
        )
    """)
    resolved_groups = cursor.fetchone()['count']

    # Group size distribution (non-rejected photos only, excluding single-photo groups)
    cursor = conn.execute("""
        SELECT remaining_photos, COUNT(*) as count
        FROM (
            SELECT group_id, COUNT(*) as remaining_photos
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        )
        GROUP BY remaining_photos
        ORDER BY remaining_photos DESC
    """)
    size_dist = cursor.fetchall()

    conn.close()

    return render_template('index.html',
                           total_photos=total_photos,
                           rejected_count=rejected_count,
                           groups_needing_review=groups_needing_review,
                           photos_needing_review=photos_needing_review,
                           resolved_groups=resolved_groups,
                           size_dist=size_dist)


@app.route('/groups')
def groups():
    """List groups needing review (2+ non-rejected photos)."""
    conn = get_db()

    cursor = conn.execute("""
        SELECT
            g.group_id,
            g.remaining_photos,
            MIN(p.path) as preview_path
        FROM (
            SELECT group_id, COUNT(*) as remaining_photos
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        ) g
        JOIN duplicate_groups dg ON g.group_id = dg.group_id AND dg.rejected = 0
        JOIN photos p ON dg.photo_id = p.id
        GROUP BY g.group_id
        ORDER BY g.remaining_photos DESC, g.group_id
    """)
    groups = cursor.fetchall()

    conn.close()

    return render_template('groups.html', groups=groups)


@app.route('/group/<int:group_id>')
def group_detail(group_id):
    """Show details of a specific group (non-rejected photos only)."""
    conn = get_db()

    # Get non-rejected photos in this group
    cursor = conn.execute("""
        SELECT
            dg.photo_id,
            dg.rank_in_group,
            dg.width,
            dg.height,
            dg.file_size,
            dg.quality_score,
            p.path,
            p.original_path,
            p.confidence_score
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ? AND dg.rejected = 0
        ORDER BY dg.quality_score DESC
    """, (group_id,))
    photos = cursor.fetchall()

    # Get navigation info (prev/next group needing review)
    cursor = conn.execute("""
        SELECT group_id FROM (
            SELECT group_id, COUNT(*) as cnt
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING cnt >= 2
        )
        ORDER BY group_id
    """)
    review_groups = [row['group_id'] for row in cursor.fetchall()]

    current_idx = review_groups.index(group_id) if group_id in review_groups else -1
    prev_group = review_groups[current_idx - 1] if current_idx > 0 else None
    next_group = review_groups[current_idx + 1] if current_idx < len(review_groups) - 1 else None

    conn.close()

    return render_template('group_detail.html',
                           group_id=group_id,
                           photos=photos,
                           prev_group=prev_group,
                           next_group=next_group,
                           current_idx=current_idx + 1,
                           total_groups=len(review_groups))


@app.route('/image/<path:filepath>')
def serve_image(filepath):
    """Serve an image file."""
    image_path = OUTPUT_ROOT / filepath
    if not image_path.exists():
        return "Image not found", 404
    return send_file(image_path)


@app.route('/api/reject', methods=['POST'])
def reject_photos():
    """Mark non-selected photos as rejected."""
    data = request.json
    group_id = data.get('group_id')
    keep_ids = data.get('keep_ids', [])

    if not group_id:
        return jsonify({'error': 'Missing group_id'}), 400

    if not keep_ids:
        return jsonify({'error': 'Must keep at least one photo'}), 400

    conn = get_db()

    # Mark photos NOT in keep_ids as rejected
    placeholders = ','.join('?' * len(keep_ids))
    conn.execute(f"""
        UPDATE duplicate_groups
        SET rejected = 1
        WHERE group_id = ? AND rejected = 0 AND photo_id NOT IN ({placeholders})
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
            COUNT(*) as total_photos,
            SUM(CASE WHEN rejected = 1 THEN 1 ELSE 0 END) as rejected
        FROM duplicate_groups
    """)
    stats = dict(cursor.fetchone())

    cursor = conn.execute("""
        SELECT COUNT(*) as groups_remaining FROM (
            SELECT group_id
            FROM duplicate_groups
            WHERE rejected = 0
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        )
    """)
    stats['groups_remaining'] = cursor.fetchone()['groups_remaining']

    conn.close()

    return jsonify(stats)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
