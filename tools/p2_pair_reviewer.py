#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
P2 Pair Reviewer - Review P2 pairs that aren't already in primary groups

Shows pages of 12 pairs at a time. Select good pairs to keep, then save.
Unselected pairs are discarded (split into singletons).

Only shows pairs where the two photos are NOT already in the same primary group.

Keyboard shortcuts:
  1-9, 0, -, = - Toggle pair selection (12 pairs per page)
  Enter/Space - Save selections and go to next page
  a - Select all pairs on page
  n - Select none (clear all)
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template_string, request, send_file, jsonify

# Paths
DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"
FILES_DIR = Path(__file__).parent.parent / "output" / "files"

app = Flask(__name__)

PAIRS_PER_PAGE = 12

# Map MIME types to extensions
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


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_file_path(photo_id: str, mime_type: str) -> Path:
    """Get the path to the linked file for a photo."""
    ext = MIME_TO_EXT.get(mime_type, ".bin")
    return FILES_DIR / photo_id[:2] / f"{photo_id}{ext}"


def init_tables():
    """Create tables for tracking review progress."""
    conn = get_connection()

    # Track reviewed pages
    conn.execute("""
        CREATE TABLE IF NOT EXISTS p2_pair_review_progress (
            id INTEGER PRIMARY KEY,
            last_reviewed_offset INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)

    # Initialize if empty
    cursor = conn.execute("SELECT COUNT(*) FROM p2_pair_review_progress")
    if cursor.fetchone()[0] == 0:
        conn.execute("INSERT INTO p2_pair_review_progress (id, last_reviewed_offset) VALUES (1, 0)")

    conn.commit()
    conn.close()


def get_pairs_to_review():
    """Get P2 pairs that need review (not already in same primary group)."""
    conn = get_connection()

    cursor = conn.execute("""
        WITH p2_pairs AS (
            -- P2 groups >= 814 with exactly 2 photos
            SELECT
                pg.group_id,
                MIN(pg.photo_id) as photo1,
                MAX(pg.photo_id) as photo2
            FROM p2_groups pg
            WHERE pg.group_id >= 814
            GROUP BY pg.group_id
            HAVING COUNT(*) = 2
        )
        SELECT
            p.group_id,
            p.photo1,
            p.photo2,
            dg1.group_id as primary1,
            dg2.group_id as primary2
        FROM p2_pairs p
        LEFT JOIN duplicate_groups dg1 ON p.photo1 = dg1.photo_id
        LEFT JOIN duplicate_groups dg2 ON p.photo2 = dg2.photo_id
        WHERE dg1.group_id IS NULL
           OR dg2.group_id IS NULL
           OR dg1.group_id != dg2.group_id
        ORDER BY p.group_id
    """)

    pairs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return pairs


def get_review_progress():
    """Get current review progress."""
    conn = get_connection()
    cursor = conn.execute("SELECT last_reviewed_offset FROM p2_pair_review_progress WHERE id = 1")
    offset = cursor.fetchone()[0]
    conn.close()
    return offset


def set_review_progress(offset):
    """Update review progress."""
    conn = get_connection()
    conn.execute(
        "UPDATE p2_pair_review_progress SET last_reviewed_offset = ?, updated_at = ? WHERE id = 1",
        (offset, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def get_photo_info(photo_id):
    """Get photo metadata."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            p.id, p.mime_type, p.width, p.height, p.file_size,
            GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM photos p
        JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE p.id = ?
        GROUP BY p.id
    """, (photo_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        photo = dict(row)
        photo['paths'] = photo['all_paths'].split('|') if photo['all_paths'] else []
        photo['resolution'] = f"{photo['width']}x{photo['height']}"
        if photo['paths']:
            photo['short_path'] = min(photo['paths'], key=len)
            photo['filename'] = Path(photo['short_path']).name
        else:
            photo['short_path'] = ''
            photo['filename'] = ''
        return photo
    return None


def get_pair_distance(photo1, photo2):
    """Get the distance between two photos."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT phash16_dist, colorhash_dist
        FROM photo_pairs
        WHERE (photo_id_1 = ? AND photo_id_2 = ?)
           OR (photo_id_1 = ? AND photo_id_2 = ?)
    """, (photo1, photo2, photo2, photo1))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else {'phash16_dist': '?', 'colorhash_dist': '?'}


def discard_pairs(group_ids):
    """Discard pairs by splitting them into singletons (deleting from p2_groups)."""
    if not group_ids:
        return

    conn = get_connection()
    # Just delete these groups - they become ungrouped
    placeholders = ','.join('?' * len(group_ids))
    conn.execute(f"DELETE FROM p2_groups WHERE group_id IN ({placeholders})", group_ids)
    conn.commit()
    conn.close()


# Cache pairs list
_pairs_cache = None

def get_pairs_list():
    global _pairs_cache
    if _pairs_cache is None:
        _pairs_cache = get_pairs_to_review()
    return _pairs_cache

def invalidate_cache():
    global _pairs_cache
    _pairs_cache = None


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>P2 Pair Reviewer - Page {{ page_num }}</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; }
        body {
            font-family: system-ui, sans-serif;
            padding: 10px;
            background: #1a1a1a;
            color: #eee;
            display: flex;
            flex-direction: column;
        }

        .header {
            display: flex;
            align-items: center;
            gap: 15px;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid #333;
            flex-wrap: wrap;
        }

        .nav { display: flex; align-items: center; gap: 8px; }
        .nav-btn {
            padding: 6px 14px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            font-size: 14px;
        }
        .nav-btn:hover { background: #444; }
        .nav-btn:disabled { opacity: 0.5; cursor: not-allowed; }

        .progress { color: #888; font-size: 0.9em; }

        .actions { display: flex; gap: 8px; margin-left: auto; }
        .action-btn {
            padding: 8px 16px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
        }
        .action-btn.save { background: #059669; color: white; }
        .action-btn.save:hover { background: #047857; }
        .action-btn.select-all { background: #2563eb; color: white; }
        .action-btn.select-all:hover { background: #1d4ed8; }
        .action-btn.select-none { background: #6b7280; color: white; }
        .action-btn.select-none:hover { background: #4b5563; }

        .selection-info {
            padding: 4px 12px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #059669; }

        .help {
            color: #666;
            font-size: 0.75em;
            padding: 4px 0;
        }
        .help kbd {
            background: #333;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: monospace;
        }

        .pairs-container { flex: 1; overflow-y: auto; }
        .pairs {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
            gap: 10px;
            padding: 5px 0;
        }

        .pair {
            background: #222;
            border-radius: 8px;
            padding: 8px;
            border: 3px solid transparent;
            cursor: pointer;
            transition: border-color 0.15s;
        }
        .pair:hover { border-color: #444; }
        .pair.selected { border-color: #059669; background: #1a2e1a; }

        .pair-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
            font-size: 0.8em;
        }
        .pair-num {
            background: #333;
            padding: 2px 8px;
            border-radius: 4px;
            font-weight: 600;
        }
        .pair.selected .pair-num { background: #059669; }
        .pair-dist { color: #888; }

        .pair-images {
            display: flex;
            gap: 6px;
        }
        .pair-photo {
            flex: 1;
            min-width: 0;
        }
        .pair-photo img {
            width: 100%;
            height: 140px;
            object-fit: contain;
            background: #111;
            border-radius: 4px;
        }
        .pair-photo .filename {
            font-size: 0.7em;
            color: #aaa;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            margin-top: 4px;
        }

        .toast {
            position: fixed;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: #333;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.3s;
            z-index: 100;
        }
        .toast.show { opacity: 1; }
        .toast.success { background: #059669; }
    </style>
</head>
<body>
    <div class="header">
        <div class="nav">
            <span style="min-width: 150px;">
                Page {{ page_num }} / {{ total_pages }}
            </span>
        </div>

        <div class="progress">
            {{ total_remaining }} pairs remaining
        </div>

        <div class="selection-info" id="selectionInfo">
            0 / {{ pairs|length }} selected
        </div>

        <div class="actions">
            <button class="action-btn select-none" onclick="selectNone()" title="Clear selection (n)">None</button>
            <button class="action-btn select-all" onclick="selectAll()" title="Select all (a)">All</button>
            <button class="action-btn save" onclick="saveAndNext()" title="Keep selected, discard rest, next page (Enter)">Save & Next</button>
        </div>
    </div>

    <div class="help">
        <kbd>1</kbd>-<kbd>9</kbd>, <kbd>0</kbd>, <kbd>-</kbd>, <kbd>=</kbd> toggle pair &nbsp;
        <kbd>a</kbd> select all &nbsp;
        <kbd>n</kbd> select none &nbsp;
        <kbd>Enter</kbd>/<kbd>Space</kbd> save & next
    </div>

    <div class="pairs-container">
        <div class="pairs">
            {% for pair in pairs %}
            <div class="pair" data-group-id="{{ pair.group_id }}" data-index="{{ loop.index0 }}" onclick="togglePair(this)">
                <div class="pair-header">
                    <span class="pair-num">{{ loop.index }}</span>
                    <span class="pair-dist">p16={{ pair.distance.phash16_dist }}, ch={{ pair.distance.colorhash_dist }}</span>
                </div>
                <div class="pair-images">
                    <div class="pair-photo">
                        <img src="/image/{{ pair.photo1.id }}" alt="">
                        <div class="filename" title="{{ pair.photo1.filename }}">{{ pair.photo1.filename }}</div>
                    </div>
                    <div class="pair-photo">
                        <img src="/image/{{ pair.photo2.id }}" alt="">
                        <div class="filename" title="{{ pair.photo2.filename }}">{{ pair.photo2.filename }}</div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const pairCount = {{ pairs|length }};
        const pageOffset = {{ offset }};
        const keyMap = ['1','2','3','4','5','6','7','8','9','0','-','='];

        let selectedGroups = new Set();

        function togglePair(el) {
            const groupId = parseInt(el.dataset.groupId);
            if (el.classList.contains('selected')) {
                el.classList.remove('selected');
                selectedGroups.delete(groupId);
            } else {
                el.classList.add('selected');
                selectedGroups.add(groupId);
            }
            updateSelectionInfo();
        }

        function toggleByIndex(index) {
            const pairs = document.querySelectorAll('.pair');
            if (index >= 0 && index < pairs.length) {
                togglePair(pairs[index]);
            }
        }

        function selectAll() {
            document.querySelectorAll('.pair').forEach(el => {
                el.classList.add('selected');
                selectedGroups.add(parseInt(el.dataset.groupId));
            });
            updateSelectionInfo();
        }

        function selectNone() {
            document.querySelectorAll('.pair').forEach(el => {
                el.classList.remove('selected');
            });
            selectedGroups.clear();
            updateSelectionInfo();
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            info.textContent = `${selectedGroups.size} / ${pairCount} selected`;
            if (selectedGroups.size > 0) {
                info.classList.add('has-selection');
            } else {
                info.classList.remove('has-selection');
            }
        }

        function showToast(message) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show success';
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        function saveAndNext() {
            // Get all group IDs on this page
            const allGroups = Array.from(document.querySelectorAll('.pair')).map(
                el => parseInt(el.dataset.groupId)
            );

            // Groups to discard = all - selected
            const toDiscard = allGroups.filter(g => !selectedGroups.has(g));

            fetch('/api/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    kept: Array.from(selectedGroups),
                    discarded: toDiscard,
                    next_offset: pageOffset + pairCount
                })
            })
            .then(r => r.json())
            .then(data => {
                showToast(`Kept ${selectedGroups.size}, discarded ${toDiscard.length}`);
                setTimeout(() => {
                    window.location.href = '/';
                }, 500);
            });
        }

        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT') return;

            const keyIndex = keyMap.indexOf(e.key);
            if (keyIndex !== -1) {
                toggleByIndex(keyIndex);
                return;
            }

            switch(e.key) {
                case 'a':
                    selectAll();
                    break;
                case 'n':
                    selectNone();
                    break;
                case ' ':
                case 'Enter':
                    e.preventDefault();
                    saveAndNext();
                    break;
            }
        });
    </script>
</body>
</html>
"""


DONE_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>P2 Pair Reviewer - Done</title>
    <style>
        body {
            font-family: system-ui, sans-serif;
            background: #1a1a1a;
            color: #eee;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
        }
        .done {
            text-align: center;
            padding: 40px;
            background: #222;
            border-radius: 12px;
        }
        .done h1 { color: #059669; margin-bottom: 10px; }
        .done p { color: #888; }
        a { color: #6cf; }
    </style>
</head>
<body>
    <div class="done">
        <h1>All Done!</h1>
        <p>All pairs have been reviewed.</p>
        <p><a href="/reset">Start over</a></p>
    </div>
</body>
</html>
"""


@app.template_filter('filesizeformat')
def filesizeformat(value):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


@app.route('/')
def index():
    """Show current page of pairs."""
    pairs = get_pairs_list()
    offset = get_review_progress()

    # Get current page
    page_pairs = pairs[offset:offset + PAIRS_PER_PAGE]

    if not page_pairs:
        return render_template_string(DONE_TEMPLATE)

    # Enrich with photo info and distances
    for pair in page_pairs:
        pair['photo1'] = get_photo_info(pair['photo1'])
        pair['photo2'] = get_photo_info(pair['photo2'])
        pair['distance'] = get_pair_distance(pair['photo1']['id'], pair['photo2']['id'])

    page_num = (offset // PAIRS_PER_PAGE) + 1
    total_pages = (len(pairs) + PAIRS_PER_PAGE - 1) // PAIRS_PER_PAGE

    return render_template_string(
        TEMPLATE,
        pairs=page_pairs,
        page_num=page_num,
        total_pages=total_pages,
        total_remaining=len(pairs) - offset,
        offset=offset,
    )


@app.route('/image/<photo_id>')
def serve_image(photo_id):
    """Serve an image file."""
    conn = get_connection()
    cursor = conn.execute("SELECT mime_type FROM photos WHERE id = ?", (photo_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return "Not found", 404

    file_path = get_file_path(photo_id, row['mime_type'])
    if not file_path.exists():
        return "File not found", 404

    return send_file(file_path, mimetype=row['mime_type'])


@app.route('/api/save', methods=['POST'])
def api_save():
    """Save selections and advance to next page."""
    data = request.json
    discarded = data.get('discarded', [])
    next_offset = data.get('next_offset', 0)

    # Discard unselected pairs
    discard_pairs(discarded)

    # Update progress
    set_review_progress(next_offset)

    # Invalidate cache since we modified data
    invalidate_cache()

    return jsonify({'success': True})


@app.route('/reset')
def reset():
    """Reset review progress."""
    set_review_progress(0)
    invalidate_cache()
    return app.redirect('/')


if __name__ == '__main__':
    init_tables()
    pairs = get_pairs_list()
    offset = get_review_progress()

    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Total pairs to review: {len(pairs)}")
    print(f"Already reviewed: {offset}")
    print(f"Remaining: {len(pairs) - offset}")
    print()
    print("Starting server at http://localhost:5004")
    print()
    print("Keyboard shortcuts:")
    print("  1-9, 0, -, = - Toggle pair by position")
    print("  a - Select all")
    print("  n - Select none")
    print("  Enter/Space - Save & next page")
    print()
    app.run(debug=True, port=5004)
