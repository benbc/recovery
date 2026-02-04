#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
P2 Group Splitter - Review and split secondary (P2) duplicate groups

For reviewing P2 groups with 2+ photos to split incorrectly grouped photos.
Supports splitting selected photos into a new group.

List order is stable during a session - new groups from splits are appended
to the end rather than re-sorted. Use /reload to refresh the list.

Keyboard shortcuts:
  1-9 - Toggle selection of photo by position
  s - Split selected into new group (appended to end of queue)
  Space/Enter - Mark reviewed and go to next group
  ←/→ - Navigate without marking reviewed
  u - Undo last action
  Escape - Clear selection
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
    """Create tables for tracking review progress and manual actions."""
    conn = get_connection()

    # Track which P2 groups have been reviewed
    conn.execute("""
        CREATE TABLE IF NOT EXISTS p2_group_review_status (
            group_id INTEGER PRIMARY KEY,
            reviewed_at TEXT,
            action TEXT  -- 'skip', 'split', or NULL if just navigated past
        )
    """)

    # Track manual actions for undo
    conn.execute("""
        CREATE TABLE IF NOT EXISTS p2_manual_group_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_type TEXT NOT NULL,  -- 'split'
            source_group_id INTEGER NOT NULL,
            photo_ids TEXT NOT NULL,  -- JSON array
            new_group_id INTEGER,  -- for splits
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def get_review_stats():
    """Get statistics about review progress."""
    conn = get_connection()

    # Count P2 groups with 2+ photos
    cursor = conn.execute("""
        SELECT COUNT(*) as total
        FROM (
            SELECT group_id
            FROM p2_groups
            GROUP BY group_id
            HAVING COUNT(*) >= 2
        )
    """)
    total = cursor.fetchone()['total']

    # Count reviewed groups
    cursor = conn.execute("""
        SELECT COUNT(*) as reviewed
        FROM p2_group_review_status
        WHERE reviewed_at IS NOT NULL
    """)
    reviewed = cursor.fetchone()['reviewed']

    conn.close()
    return {'total': total, 'reviewed': reviewed}


def get_groups_to_review():
    """Get all P2 groups with 2+ photos, ordered by size desc."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT group_id, COUNT(*) as photo_count
        FROM p2_groups
        GROUP BY group_id
        HAVING COUNT(*) >= 2
        ORDER BY photo_count DESC, group_id
    """)
    groups = [(row['group_id'], row['photo_count']) for row in cursor.fetchall()]
    conn.close()
    return groups


def get_group_photos(group_id: int):
    """Get photos in a P2 group."""
    conn = get_connection()

    cursor = conn.execute("""
        SELECT
            p.id, p.mime_type, p.width, p.height, p.file_size,
            p.perceptual_hash, p.dhash,
            GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM p2_groups pg
        JOIN photos p ON pg.photo_id = p.id
        JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE pg.group_id = ?
        GROUP BY p.id
        ORDER BY (p.width * p.height) DESC, p.file_size DESC
    """, (group_id,))

    photos = []
    for row in cursor.fetchall():
        photo = dict(row)
        photo['paths'] = photo['all_paths'].split('|') if photo['all_paths'] else []
        photo['resolution'] = f"{photo['width']}x{photo['height']}"
        photo['megapixels'] = round((photo['width'] or 0) * (photo['height'] or 0) / 1_000_000, 1)
        # Get shortest path for display
        if photo['paths']:
            photo['short_path'] = min(photo['paths'], key=len)
            photo['filename'] = Path(photo['short_path']).name
        else:
            photo['short_path'] = ''
            photo['filename'] = ''
        photos.append(photo)

    conn.close()
    return photos


def is_group_reviewed(group_id: int) -> bool:
    """Check if a P2 group has been reviewed."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT 1 FROM p2_group_review_status WHERE group_id = ? AND reviewed_at IS NOT NULL",
        (group_id,)
    )
    result = cursor.fetchone() is not None
    conn.close()
    return result


def mark_group_reviewed(group_id: int, action: str = 'skip'):
    """Mark a P2 group as reviewed."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO p2_group_review_status (group_id, reviewed_at, action)
        VALUES (?, ?, ?)
    """, (group_id, datetime.now().isoformat(), action))
    conn.commit()
    conn.close()


def split_photos(group_id: int, photo_ids: list[str]) -> int:
    """Split photos into a new P2 group. Returns new group ID."""
    conn = get_connection()

    # Get next group ID
    cursor = conn.execute("SELECT MAX(group_id) as max_id FROM p2_groups")
    new_group_id = (cursor.fetchone()['max_id'] or 0) + 1

    # Move photos to new group
    conn.executemany(
        "UPDATE p2_groups SET group_id = ? WHERE photo_id = ?",
        [(new_group_id, pid) for pid in photo_ids]
    )

    # Log the action
    conn.execute("""
        INSERT INTO p2_manual_group_actions (action_type, source_group_id, photo_ids, new_group_id, created_at)
        VALUES ('split', ?, ?, ?, ?)
    """, (group_id, json.dumps(photo_ids), new_group_id, datetime.now().isoformat()))

    conn.commit()
    conn.close()
    return new_group_id


def undo_last_action() -> dict | None:
    """Undo the last manual action. Returns action info or None."""
    conn = get_connection()

    # Get last action
    cursor = conn.execute("""
        SELECT * FROM p2_manual_group_actions
        ORDER BY id DESC LIMIT 1
    """)
    row = cursor.fetchone()

    if not row:
        conn.close()
        return None

    action = dict(row)
    photo_ids = json.loads(action['photo_ids'])

    if action['action_type'] == 'split':
        # Move photos back to original group
        conn.executemany(
            "UPDATE p2_groups SET group_id = ? WHERE photo_id = ?",
            [(action['source_group_id'], pid) for pid in photo_ids]
        )

    # Delete the action record
    conn.execute("DELETE FROM p2_manual_group_actions WHERE id = ?", (action['id'],))

    conn.commit()
    conn.close()

    return action


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>P2 Group Splitter - Group {{ group_id }}</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; overflow: hidden; }
        body {
            font-family: system-ui, sans-serif;
            padding: 10px;
            background: #1a1a1a;
            color: #eee;
            display: flex;
            flex-direction: column;
        }
        a { color: #6cf; text-decoration: none; }
        a:hover { text-decoration: underline; }

        .header {
            display: flex;
            align-items: center;
            gap: 15px;
            margin-bottom: 10px;
            flex-wrap: wrap;
            padding-bottom: 10px;
            border-bottom: 1px solid #333;
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

        .progress {
            color: #888;
            font-size: 0.9em;
        }
        .progress .reviewed { color: #4c4; }

        .actions {
            display: flex;
            gap: 8px;
            margin-left: auto;
        }
        .action-btn {
            padding: 8px 16px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
        }
        .action-btn:disabled { opacity: 0.4; cursor: not-allowed; }
        .action-btn.split { background: #2563eb; color: white; }
        .action-btn.split:hover:not(:disabled) { background: #1d4ed8; }
        .action-btn.skip { background: #059669; color: white; }
        .action-btn.skip:hover { background: #047857; }
        .action-btn.undo { background: #6b7280; color: white; }
        .action-btn.undo:hover { background: #4b5563; }

        .selection-info {
            padding: 4px 12px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #2563eb; }

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

        .photos-container { flex: 1; overflow-y: auto; padding: 5px 0; }
        .photos {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
            gap: 8px;
            align-content: start;
        }

        .photo {
            background: #222;
            border-radius: 8px;
            overflow: hidden;
            border: 3px solid transparent;
            cursor: pointer;
            transition: border-color 0.15s;
            position: relative;
        }
        .photo:hover { border-color: #444; }
        .photo.selected { border-color: #2563eb; }

        .photo-number {
            position: absolute;
            top: 4px;
            left: 4px;
            background: rgba(0,0,0,0.7);
            color: #fff;
            width: 24px;
            height: 24px;
            border-radius: 4px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 12px;
            font-weight: 600;
            z-index: 1;
        }
        .photo.selected .photo-number { background: #2563eb; }

        .photo img {
            width: 100%;
            height: 180px;
            object-fit: contain;
            background: #111;
            pointer-events: none;
        }

        .photo-info { padding: 6px 8px; font-size: 0.75em; }
        .photo-info .filename {
            color: #fff;
            font-weight: 500;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            margin-bottom: 2px;
        }
        .photo-info .dims { color: #aaa; margin-bottom: 2px; }
        .photo-info .path {
            color: #666;
            font-size: 0.85em;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
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
        .toast.error { background: #dc2626; }
    </style>
</head>
<body>
    <div class="header">
        <div class="nav">
            <button class="nav-btn" onclick="navigate('prev')" {{ 'disabled' if prev_group is none else '' }}>← Prev</button>
            <span style="min-width: 120px; text-align: center;">
                Group {{ group_idx + 1 }} / {{ total_groups }}
            </span>
            <button class="nav-btn" onclick="navigate('next')" {{ 'disabled' if next_group is none else '' }}>Next →</button>
        </div>

        <div class="progress">
            <span class="reviewed">{{ stats.reviewed }}</span> / {{ stats.total }} reviewed
        </div>

        <div class="selection-info" id="selectionInfo">
            {{ photo_count }} photos
        </div>

        <div class="actions">
            <button class="action-btn undo" onclick="undo()" title="Undo last action (u)">Undo</button>
            <button class="action-btn split" id="splitBtn" onclick="splitSelected()" disabled title="Split selected (s)">Split</button>
            <button class="action-btn skip" onclick="skipGroup()" title="Mark as reviewed and go next (Space/Enter)">Skip →</button>
        </div>
    </div>

    <div class="help">
        <kbd>1</kbd>-<kbd>9</kbd> toggle photo &nbsp;
        <kbd>2</kbd> on pair = quick split &nbsp;
        <kbd>s</kbd> split &nbsp;
        <kbd>Space</kbd>/<kbd>Enter</kbd> reviewed + next &nbsp;
        <kbd>←</kbd><kbd>→</kbd> navigate &nbsp;
        <kbd>u</kbd> undo &nbsp;
        <kbd>Esc</kbd> clear
    </div>

    <div class="photos-container">
        <div class="photos" id="photosGrid">
            {% for photo in photos %}
            <div class="photo"
                 data-id="{{ photo.id }}"
                 data-index="{{ loop.index }}"
                 onclick="toggleSelect(this, event)">
                <div class="photo-number">{{ loop.index }}</div>
                <img src="/image/{{ photo.id }}" alt="{{ photo.filename }}">
                <div class="photo-info">
                    <div class="filename" title="{{ photo.filename }}">{{ photo.filename }}</div>
                    <div class="dims">{{ photo.resolution }} · {{ photo.file_size | filesizeformat }}</div>
                    <div class="path" title="{{ photo.short_path }}">{{ photo.short_path }}</div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const groupId = {{ group_id }};
        const prevGroup = {{ prev_group if prev_group is not none else 'null' }};
        const nextGroup = {{ next_group if next_group is not none else 'null' }};
        const photoCount = {{ photo_count }};

        let selectedIds = new Set();
        let lastClickedIndex = null;

        function getPhotoElements() {
            return Array.from(document.querySelectorAll('.photo'));
        }

        function toggleSelectByIndex(index) {
            const photos = getPhotoElements();
            if (index >= 0 && index < photos.length) {
                toggleSelect(photos[index], { shiftKey: false });
            }
        }

        function toggleSelect(el, event) {
            const photos = getPhotoElements();
            const clickedIndex = photos.indexOf(el);

            if (event.shiftKey && lastClickedIndex !== null) {
                // Range select
                const start = Math.min(lastClickedIndex, clickedIndex);
                const end = Math.max(lastClickedIndex, clickedIndex);
                for (let i = start; i <= end; i++) {
                    photos[i].classList.add('selected');
                    selectedIds.add(photos[i].dataset.id);
                }
            } else {
                // Toggle single
                if (el.classList.contains('selected')) {
                    el.classList.remove('selected');
                    selectedIds.delete(el.dataset.id);
                } else {
                    el.classList.add('selected');
                    selectedIds.add(el.dataset.id);
                }
            }

            lastClickedIndex = clickedIndex;
            updateSelectionInfo();
        }

        function clearSelection() {
            document.querySelectorAll('.photo.selected').forEach(el => {
                el.classList.remove('selected');
            });
            selectedIds.clear();
            lastClickedIndex = null;
            updateSelectionInfo();
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            const splitBtn = document.getElementById('splitBtn');

            if (selectedIds.size > 0) {
                info.textContent = `${selectedIds.size} selected`;
                info.classList.add('has-selection');
                // Can only split if we leave at least 1 in original group
                splitBtn.disabled = selectedIds.size >= photoCount;
            } else {
                info.textContent = `${photoCount} photos`;
                info.classList.remove('has-selection');
                splitBtn.disabled = true;
            }
        }

        function showToast(message, type = '') {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show ' + type;
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        function navigate(direction) {
            const target = direction === 'prev' ? prevGroup : nextGroup;
            if (target !== null) {
                window.location.href = `/group/${target}`;
            }
        }

        function skipGroup() {
            fetch(`/api/review/${groupId}`, { method: 'POST' })
                .then(() => {
                    if (nextGroup !== null) {
                        window.location.href = `/group/${nextGroup}`;
                    } else {
                        showToast('All groups reviewed!', 'success');
                    }
                });
        }

        function splitSelected() {
            if (selectedIds.size === 0 || selectedIds.size >= photoCount) return;

            const count = selectedIds.size;
            fetch('/api/split', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    group_id: groupId,
                    photo_ids: Array.from(selectedIds)
                })
            })
            .then(r => r.json())
            .then(data => {
                showToast(`Split ${count} → group ${data.new_group_id} (queued at end)`, 'success');
                if (data.removed) {
                    // Group dropped to <2 photos, go to next
                    setTimeout(() => {
                        if (nextGroup !== null) {
                            window.location.href = `/group/${nextGroup}`;
                        } else {
                            window.location.href = '/';
                        }
                    }, 600);
                } else {
                    // Stay on current group, reload to show updated state
                    setTimeout(() => location.reload(), 500);
                }
            })
            .catch(() => showToast('Split failed', 'error'));
        }

        function undo() {
            fetch('/api/undo', { method: 'POST' })
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        showToast(`Undid ${data.action_type}`, 'success');
                        setTimeout(() => location.reload(), 500);
                    } else {
                        showToast('Nothing to undo', 'error');
                    }
                });
        }

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT') return;

            // Number keys 1-9 for quick selection
            if (e.key >= '1' && e.key <= '9') {
                const index = parseInt(e.key) - 1;

                // Quick split shortcut: press "2" with 2 photos and nothing selected
                // → splits into two singletons (removes group from queue)
                if (e.key === '2' && photoCount === 2 && selectedIds.size === 0) {
                    toggleSelectByIndex(1);  // Select photo 2
                    splitSelected();         // Split immediately
                    return;
                }

                toggleSelectByIndex(index);
                return;
            }

            switch(e.key) {
                case 'ArrowLeft':
                    navigate('prev');
                    break;
                case 'ArrowRight':
                    navigate('next');
                    break;
                case ' ':
                case 'Enter':
                    e.preventDefault();
                    skipGroup();
                    break;
                case 's':
                    splitSelected();
                    break;
                case 'u':
                    undo();
                    break;
                case 'Escape':
                    clearSelection();
                    break;
            }
        });
    </script>
</body>
</html>
"""


@app.template_filter('filesizeformat')
def filesizeformat(value):
    """Format file size in human-readable form."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


# Cache group list - stable order during session, new groups appended to end
_group_list = None

def get_group_list():
    """Get the cached group list, initializing if needed."""
    global _group_list
    if _group_list is None:
        _group_list = get_groups_to_review()
    return _group_list

def invalidate_cache():
    """Full cache reset - used for undo and explicit reload."""
    global _group_list
    _group_list = None

def append_group_to_list(group_id: int, photo_count: int):
    """Append a new group to the end of the list (for splits)."""
    global _group_list
    if _group_list is not None and photo_count >= 2:
        _group_list.append((group_id, photo_count))

def remove_group_from_list(group_id: int):
    """Remove a group from the list (when it drops below 2 photos)."""
    global _group_list
    if _group_list is not None:
        _group_list = [(gid, cnt) for gid, cnt in _group_list if gid != group_id]

def update_group_in_list(group_id: int, new_photo_count: int):
    """Update a group's count, or remove it if below 2."""
    global _group_list
    if _group_list is not None:
        if new_photo_count < 2:
            remove_group_from_list(group_id)
        else:
            _group_list = [(gid, new_photo_count if gid == group_id else cnt)
                          for gid, cnt in _group_list]

def get_group_photo_count(group_id: int) -> int:
    """Get the current number of photos in a P2 group."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT COUNT(*) as cnt FROM p2_groups WHERE group_id = ?",
        (group_id,)
    )
    result = cursor.fetchone()['cnt']
    conn.close()
    return result


@app.route('/')
def index():
    """Redirect to first unreviewed group, or first group."""
    groups = get_group_list()

    # Find first unreviewed group
    for group_id, _ in groups:
        if not is_group_reviewed(group_id):
            return app.redirect(f'/group/{group_id}')

    # All reviewed, go to first
    if groups:
        return app.redirect(f'/group/{groups[0][0]}')
    return "No groups to review"


@app.route('/group/<int:group_id>')
def show_group(group_id):
    """Show P2 group for review."""
    groups = get_group_list()
    group_ids = [g[0] for g in groups]

    if group_id not in group_ids:
        return "Group not found", 404

    group_idx = group_ids.index(group_id)
    prev_group = group_ids[group_idx - 1] if group_idx > 0 else None
    next_group = group_ids[group_idx + 1] if group_idx < len(group_ids) - 1 else None

    photos = get_group_photos(group_id)
    photo_count = len(photos)
    stats = get_review_stats()

    return render_template_string(
        TEMPLATE,
        group_id=group_id,
        group_idx=group_idx,
        total_groups=len(groups),
        prev_group=prev_group,
        next_group=next_group,
        photos=photos,
        photo_count=photo_count,
        stats=stats,
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


@app.route('/api/review/<int:group_id>', methods=['POST'])
def api_mark_reviewed(group_id):
    """Mark a P2 group as reviewed."""
    mark_group_reviewed(group_id, 'skip')
    return jsonify({'success': True})


@app.route('/api/split', methods=['POST'])
def api_split():
    """Split selected photos into a new P2 group."""
    data = request.json
    group_id = data['group_id']
    photo_ids = data['photo_ids']

    new_group_id = split_photos(group_id, photo_ids)

    # Update list: check remainder count, append new group if 2+ photos
    remainder_count = get_group_photo_count(group_id)
    new_group_count = len(photo_ids)

    update_group_in_list(group_id, remainder_count)
    append_group_to_list(new_group_id, new_group_count)

    return jsonify({
        'success': True,
        'new_group_id': new_group_id,
        'remainder_count': remainder_count,
        'removed': remainder_count < 2
    })


@app.route('/api/undo', methods=['POST'])
def api_undo():
    """Undo the last action."""
    action = undo_last_action()
    if action:
        invalidate_cache()
        return jsonify({'success': True, 'action_type': action['action_type']})
    return jsonify({'success': False})


@app.route('/reload')
def reload_groups():
    """Reload group list from database."""
    invalidate_cache()
    return app.redirect('/')


if __name__ == '__main__':
    init_tables()
    stats = get_review_stats()
    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"P2 groups to review: {stats['total']:,}")
    print(f"Already reviewed: {stats['reviewed']:,}")
    print()
    print("Starting server at http://localhost:5003")
    print()
    print("Keyboard shortcuts:")
    print("  1-9 - Toggle photo selection by position")
    print("  s - Split selected → new group (appended to end)")
    print("  Space/Enter - Mark reviewed + next")
    print("  ←/→ - Navigate (no mark)")
    print("  u - Undo last action")
    print("  Esc - Clear selection")
    print()
    print("List order is stable during session. /reload to refresh.")
    app.run(debug=True, port=5003)
