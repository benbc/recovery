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


# Undo stack: list of (group_id, [rejected_photo_ids])
# Kept in memory - survives page reloads but not server restarts
undo_stack = []
MAX_UNDO_STACK = 50


def get_next_review_group(conn, after_group_id=None):
    """Get the next group needing review, optionally after a specific group."""
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

    if not review_groups:
        return None

    if after_group_id is None:
        return review_groups[0]

    try:
        idx = review_groups.index(after_group_id)
        if idx < len(review_groups) - 1:
            return review_groups[idx + 1]
        return None  # Was the last group
    except ValueError:
        # after_group_id not in list (was just resolved), return first
        # Find first group > after_group_id
        for gid in review_groups:
            if gid > after_group_id:
                return gid
        return None


@app.route('/api/reject', methods=['POST'])
def reject_photos():
    """Mark non-selected photos as rejected."""
    data = request.json
    group_id = data.get('group_id')
    keep_ids = data.get('keep_ids', [])

    if not group_id:
        return jsonify({'error': 'Missing group_id'}), 400

    conn = get_db()

    # Get all non-rejected photo_ids in this group
    cursor = conn.execute("""
        SELECT photo_id FROM duplicate_groups
        WHERE group_id = ? AND rejected = 0
    """, [group_id])
    group_photo_ids = set(row['photo_id'] for row in cursor.fetchall())

    if not group_photo_ids:
        conn.close()
        return jsonify({'error': 'No photos to process in this group'}), 400

    # Validate keep_ids all belong to this group
    keep_ids_set = set(keep_ids)
    invalid_ids = keep_ids_set - group_photo_ids
    if invalid_ids:
        conn.close()
        return jsonify({'error': f'Invalid photo IDs for this group: {list(invalid_ids)[:5]}'}), 400

    # Compute which photos will be rejected
    reject_ids = list(group_photo_ids - keep_ids_set)

    if reject_ids:
        # Store in undo stack before making changes
        undo_stack.append((group_id, reject_ids))
        if len(undo_stack) > MAX_UNDO_STACK:
            undo_stack.pop(0)

        # Reject the photos
        placeholders = ','.join('?' * len(reject_ids))
        conn.execute(f"""
            UPDATE duplicate_groups
            SET rejected = 1
            WHERE photo_id IN ({placeholders})
        """, reject_ids)
        conn.commit()

    # Get next group needing review
    next_group = get_next_review_group(conn, group_id)

    conn.close()

    return jsonify({
        'success': True,
        'rejected_count': len(reject_ids),
        'next_group': next_group
    })


@app.route('/api/undo', methods=['POST'])
def undo_last():
    """Undo the last rejection action."""
    if not undo_stack:
        return jsonify({'error': 'Nothing to undo'}), 400

    group_id, rejected_ids = undo_stack.pop()

    conn = get_db()

    # Restore the rejected photos
    placeholders = ','.join('?' * len(rejected_ids))
    conn.execute(f"""
        UPDATE duplicate_groups
        SET rejected = 0
        WHERE photo_id IN ({placeholders})
    """, rejected_ids)
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'restored_count': len(rejected_ids),
        'group_id': group_id,
        'undo_remaining': len(undo_stack)
    })


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
