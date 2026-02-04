#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
#   "piexif",
#   "pillow",
# ]
# ///
"""
Date Browser - Browse groups and singletons by date.

Shows one representative photo per group, organized by date.
Click photos to select, then press 'm' to merge selected groups.

Keyboard shortcuts:
  ← / → - Previous / next date
  m - Merge selected groups
  Escape - Clear selection
"""

import sqlite3
import sys
from pathlib import Path

from flask import Flask, render_template_string, request, send_file, jsonify, redirect

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline2.date_utils import derive_date_for_group, derive_date, CONFIDENCE_TIERS

# Paths
DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"
FILES_DIR = Path(__file__).parent.parent / "output" / "files"

app = Flask(__name__)


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
    ext = MIME_TO_EXT.get(mime_type, ".bin")
    return FILES_DIR / photo_id[:2] / f"{photo_id}{ext}"


def get_date_for_display(date_value: str | None) -> str:
    """Extract just the date part for grouping (YYYY-MM-DD or partial)."""
    if not date_value:
        return "unknown"
    # Take first 10 chars max (YYYY-MM-DD)
    return date_value[:10]


def get_all_dates_with_groups(conn) -> list[str]:
    """Get all unique dates that have groups or singletons, sorted chronologically."""
    return get_cache(conn)['dates_list']


def get_groups_for_date(conn, target_date: str) -> list[dict]:
    """Get all groups and singletons for a given date."""
    return get_cache(conn)['by_date'].get(target_date, [])


def get_next_group_id(conn) -> int:
    """Get the next available group ID."""
    cursor = conn.execute("SELECT MAX(group_id) FROM composite_groups")
    row = cursor.fetchone()
    return (row[0] or 0) + 1


def merge_groups(conn, group_ids: list[str]) -> str:
    """
    Merge multiple groups into one. Returns the resulting group identifier.

    Groups are identified as:
    - 'G_<id>' for existing composite groups
    - 'S_<photo_id>' for singletons
    """
    if len(group_ids) < 2:
        return group_ids[0] if group_ids else None

    # Find the largest existing group to merge into, or create new
    target_group_id = None
    max_size = 0

    for gid in group_ids:
        if gid.startswith('G_'):
            group_num = int(gid[2:])
            cursor = conn.execute(
                "SELECT COUNT(*) FROM composite_groups WHERE group_id = ?",
                (group_num,)
            )
            size = cursor.fetchone()[0]
            if size > max_size:
                max_size = size
                target_group_id = group_num

    if target_group_id is None:
        target_group_id = get_next_group_id(conn)

    # Merge all into target
    for gid in group_ids:
        if gid.startswith('S_'):
            photo_id = gid[2:]
            # Check if already in target (shouldn't happen but be safe)
            cursor = conn.execute(
                "SELECT 1 FROM composite_groups WHERE photo_id = ?",
                (photo_id,)
            )
            if not cursor.fetchone():
                conn.execute(
                    "INSERT INTO composite_groups (photo_id, group_id) VALUES (?, ?)",
                    (photo_id, target_group_id)
                )
        elif gid.startswith('G_'):
            source_group_id = int(gid[2:])
            if source_group_id != target_group_id:
                conn.execute(
                    "UPDATE composite_groups SET group_id = ? WHERE group_id = ?",
                    (target_group_id, source_group_id)
                )

    conn.commit()
    return f"G_{target_group_id}"


# Cache for all computed dates (built at startup, invalidated on merge)
_cache = None  # Will be dict with 'dates_list', 'by_date', 'date_info'


def build_date_cache(conn) -> dict:
    """
    Build complete date cache at startup.

    Returns dict with:
    - dates_list: sorted list of unique dates
    - by_date: {date_str: [list of (group_id_str, photo_id, count, date_result)]}
    - date_info: {group_id_str: DateResult}
    """
    from collections import defaultdict

    by_date = defaultdict(list)
    date_info = {}

    # Get all composite groups with their photos
    cursor = conn.execute("""
        SELECT group_id, photo_id FROM composite_groups ORDER BY group_id
    """)
    group_photos = defaultdict(list)
    for row in cursor.fetchall():
        group_photos[row[0]].append(row[1])

    # Compute dates for groups
    for group_id, photo_ids in group_photos.items():
        result = derive_date_for_group(conn, group_id)
        group_id_str = f'G_{group_id}'
        date_info[group_id_str] = result
        date_str = get_date_for_display(result.date_value)
        by_date[date_str].append({
            'group_id': group_id_str,
            'representative_photo_id': photo_ids[0],
            'photo_count': len(photo_ids),
            'date_value': result.date_value,
            'confidence': result.confidence,
        })

    # Get singletons
    cursor = conn.execute("""
        SELECT kp.id
        FROM kept_photos kp
        LEFT JOIN composite_groups cg ON kp.id = cg.photo_id
        WHERE cg.photo_id IS NULL
    """)
    singleton_ids = [row[0] for row in cursor.fetchall()]

    # Compute dates for singletons
    for photo_id in singleton_ids:
        result = derive_date(conn, photo_id)
        group_id_str = f'S_{photo_id}'
        date_info[group_id_str] = result
        date_str = get_date_for_display(result.date_value)
        by_date[date_str].append({
            'group_id': group_id_str,
            'representative_photo_id': photo_id,
            'photo_count': 1,
            'date_value': result.date_value,
            'confidence': result.confidence,
        })

    # Sort dates (unknown last), skip dates with only one group/photo (nothing to merge)
    dates_list = sorted([d for d in by_date.keys() if d != "unknown" and len(by_date[d]) > 1])
    if "unknown" in by_date and len(by_date["unknown"]) > 1:
        dates_list.append("unknown")

    # Sort items within each date
    for date_str in by_date:
        by_date[date_str].sort(key=lambda x: (x['date_value'] or '', x['group_id']))

    return {
        'dates_list': dates_list,
        'by_date': dict(by_date),
        'date_info': date_info,
    }


def get_cache(conn) -> dict:
    global _cache
    if _cache is None:
        _cache = build_date_cache(conn)
    return _cache


def invalidate_cache():
    global _cache
    _cache = None


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Date Browser - {{ current_date }}</title>
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
            gap: 20px;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid #333;
            flex-wrap: wrap;
        }

        .date-nav {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .nav-btn {
            padding: 8px 16px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            font-size: 14px;
        }
        .nav-btn:hover { background: #444; }
        .nav-btn:disabled { opacity: 0.4; cursor: not-allowed; }

        .current-date {
            font-size: 1.5em;
            font-weight: 600;
            min-width: 150px;
            text-align: center;
        }

        .date-info {
            color: #888;
            font-size: 0.9em;
        }

        .selection-info {
            padding: 6px 14px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #059669; }

        .merge-btn {
            padding: 8px 20px;
            background: #2563eb;
            border: none;
            border-radius: 5px;
            color: white;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            margin-left: auto;
        }
        .merge-btn:hover { background: #1d4ed8; }
        .merge-btn:disabled { opacity: 0.4; cursor: not-allowed; }

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

        .groups-container { flex: 1; overflow-y: auto; }
        .groups {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 8px;
            padding: 5px 0;
        }

        .group {
            background: #222;
            border-radius: 8px;
            padding: 6px;
            border: 3px solid transparent;
            cursor: pointer;
            transition: border-color 0.15s;
        }
        .group:hover { border-color: #444; }
        .group.selected { border-color: #059669; background: #1a2e1a; }

        .group-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
            font-size: 0.75em;
        }
        .group-count {
            background: #333;
            padding: 2px 6px;
            border-radius: 4px;
        }
        .group.selected .group-count { background: #059669; }
        .group-confidence { color: #666; }
        .group-confidence.high { color: #22c55e; }
        .group-confidence.medium { color: #eab308; }
        .group-confidence.low { color: #ef4444; }

        .group img {
            width: 100%;
            height: 220px;
            object-fit: contain;
            background: #111;
            border-radius: 4px;
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

        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #666;
        }
        .empty-state h2 { color: #888; margin-bottom: 10px; }
    </style>
</head>
<body>
    <div class="header">
        <div class="date-nav">
            <button class="nav-btn" onclick="prevDate()" {{ 'disabled' if not has_prev else '' }} title="Previous date (←)">← Prev</button>
            <div class="current-date">{{ current_date }}</div>
            <button class="nav-btn" onclick="nextDate()" {{ 'disabled' if not has_next else '' }} title="Next date (→)">Next →</button>
        </div>

        <div class="date-info">
            {{ date_index + 1 }} / {{ total_dates }} dates
            &nbsp;|&nbsp;
            {{ date_range }}
        </div>

        <div class="selection-info" id="selectionInfo">
            0 selected
        </div>

        <button class="merge-btn" id="mergeBtn" onclick="mergeSelected()" disabled title="Merge selected (m)">
            Merge (m)
        </button>
    </div>

    <div class="help">
        <kbd>←</kbd> prev date &nbsp;
        <kbd>→</kbd> next date &nbsp;
        <kbd>m</kbd> merge selected &nbsp;
        <kbd>Esc</kbd> clear selection &nbsp;
        <kbd>dbl-click</kbd> view group
    </div>

    <div class="groups-container">
        {% if groups %}
        <div class="groups">
            {% for group in groups %}
            <div class="group"
                 data-group-id="{{ group.group_id }}"
                 onclick="toggleGroup(this)"
                 ondblclick="viewGroup(this)">
                <div class="group-header">
                    <span class="group-count">{{ group.photo_count }}</span>
                    <span class="group-confidence {{ group.confidence or '' }}">{{ group.confidence or '?' }}</span>
                </div>
                <img src="/image/{{ group.representative_photo_id }}" alt="">
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="empty-state">
            <h2>No photos on this date</h2>
        </div>
        {% endif %}
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const currentDate = {{ current_date | tojson }};
        const prevDateVal = {{ prev_date | tojson }};
        const nextDateVal = {{ next_date | tojson }};

        let selected = new Set();

        function toggleGroup(el) {
            const groupId = el.dataset.groupId;
            if (el.classList.contains('selected')) {
                el.classList.remove('selected');
                selected.delete(groupId);
            } else {
                el.classList.add('selected');
                selected.add(groupId);
            }
            updateSelectionInfo();
        }

        function viewGroup(el) {
            const groupId = el.dataset.groupId;
            window.location.href = '/group/' + encodeURIComponent(groupId);
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            info.textContent = `${selected.size} selected`;
            info.classList.toggle('has-selection', selected.size > 0);
            document.getElementById('mergeBtn').disabled = selected.size < 2;
        }

        function clearSelection() {
            document.querySelectorAll('.group.selected').forEach(el => {
                el.classList.remove('selected');
            });
            selected.clear();
            updateSelectionInfo();
        }

        function showToast(message) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show success';
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        function prevDate() {
            if (prevDateVal) {
                window.location.href = '/date/' + encodeURIComponent(prevDateVal);
            }
        }

        function nextDate() {
            if (nextDateVal) {
                window.location.href = '/date/' + encodeURIComponent(nextDateVal);
            }
        }

        function mergeSelected() {
            if (selected.size < 2) return;

            const groupIds = Array.from(selected);

            fetch('/api/merge', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ group_ids: groupIds })
            })
            .then(r => r.json())
            .then(data => {
                showToast(`Merged ${data.merged_count} groups`);
                // Reload same date to see updated groups
                setTimeout(() => {
                    window.location.href = '/date/' + encodeURIComponent(currentDate);
                }, 400);
            });
        }

        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT') return;

            switch(e.key) {
                case 'ArrowLeft':
                    if (prevDateVal) {
                        window.location.href = '/date/' + encodeURIComponent(prevDateVal);
                    }
                    break;
                case 'ArrowRight':
                    if (nextDateVal) {
                        window.location.href = '/date/' + encodeURIComponent(nextDateVal);
                    }
                    break;
                case 'm':
                    mergeSelected();
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


@app.route('/')
def index():
    """Redirect to first date."""
    conn = get_connection()
    dates = get_all_dates_with_groups(conn)
    conn.close()

    if dates:
        return redirect(f'/date/{dates[0]}')
    return "No dates found", 404


@app.route('/date/<path:date>')
def show_date(date: str):
    """Show groups for a specific date."""
    conn = get_connection()
    dates = get_all_dates_with_groups(conn)

    if date not in dates:
        conn.close()
        return f"Date not found: {date}", 404

    date_index = dates.index(date)
    groups = get_groups_for_date(conn, date)
    conn.close()

    # Navigation
    has_prev = date_index > 0
    has_next = date_index < len(dates) - 1
    prev_date = dates[date_index - 1] if has_prev else None
    next_date = dates[date_index + 1] if has_next else None

    # Date range
    if dates:
        first = dates[0] if dates[0] != "unknown" else (dates[1] if len(dates) > 1 else "?")
        last = dates[-1] if dates[-1] != "unknown" else (dates[-2] if len(dates) > 1 else "?")
        date_range = f"{first} to {last}"
    else:
        date_range = "?"

    return render_template_string(
        TEMPLATE,
        current_date=date,
        date_index=date_index,
        total_dates=len(dates),
        date_range=date_range,
        groups=groups,
        has_prev=has_prev,
        has_next=has_next,
        prev_date=prev_date,
        next_date=next_date,
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


@app.route('/api/split', methods=['POST'])
def api_split():
    """Split selected photos from a group into a new group (or singleton)."""
    data = request.json
    group_id = data.get('group_id')
    photo_ids = data.get('photo_ids', [])

    if not group_id or not photo_ids:
        return jsonify({'success': False, 'error': 'Missing group_id or photo_ids'})

    if not group_id.startswith('G_'):
        return jsonify({'success': False, 'error': 'Can only split composite groups'})

    gid = int(group_id[2:])

    conn = get_connection()

    # Get current group size
    cursor = conn.execute("SELECT COUNT(*) FROM composite_groups WHERE group_id = ?", (gid,))
    group_size = cursor.fetchone()[0]

    if len(photo_ids) >= group_size:
        conn.close()
        return jsonify({'success': False, 'error': 'Cannot split all photos from group'})

    if len(photo_ids) == 1:
        # Single photo - make it a singleton (remove from composite_groups)
        conn.execute("DELETE FROM composite_groups WHERE photo_id = ?", (photo_ids[0],))
    else:
        # Multiple photos - create new group
        cursor = conn.execute("SELECT MAX(group_id) FROM composite_groups")
        max_id = cursor.fetchone()[0] or 0
        new_group_id = max_id + 1

        # Move photos to new group
        placeholders = ",".join("?" * len(photo_ids))
        conn.execute(
            f"UPDATE composite_groups SET group_id = ? WHERE photo_id IN ({placeholders})",
            [new_group_id] + photo_ids
        )

    conn.commit()
    conn.close()

    # Invalidate cache
    invalidate_cache()

    return jsonify({'success': True, 'split_count': len(photo_ids)})


@app.route('/api/merge', methods=['POST'])
def api_merge():
    """Merge selected groups."""
    data = request.json
    group_ids = data.get('group_ids', [])

    if len(group_ids) < 2:
        return jsonify({'success': False, 'error': 'Need at least 2 groups'})

    conn = get_connection()
    result = merge_groups(conn, group_ids)
    conn.close()

    # Invalidate cache since groups changed
    invalidate_cache()

    return jsonify({
        'success': True,
        'merged_count': len(group_ids),
        'result_group': result,
    })


GROUP_DETAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Group {{ group_id }}</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: system-ui, sans-serif;
            padding: 20px;
            background: #1a1a1a;
            color: #eee;
            margin: 0;
        }
        .header {
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .back-btn, .split-btn {
            padding: 8px 16px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            text-decoration: none;
            font-size: 14px;
        }
        .back-btn:hover, .split-btn:hover { background: #444; }
        .split-btn { background: #2563eb; margin-left: auto; }
        .split-btn:hover { background: #1d4ed8; }
        .split-btn:disabled { opacity: 0.4; cursor: not-allowed; }
        h1 { margin: 0; font-size: 1.4em; }
        .selection-info {
            padding: 6px 14px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #059669; }
        .photos {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 12px;
        }
        .photo {
            cursor: pointer;
            border: 3px solid transparent;
            border-radius: 8px;
            padding: 4px;
        }
        .photo:hover { border-color: #444; }
        .photo.selected { border-color: #059669; background: #1a2e1a; }
        .photo img {
            width: 100%;
            height: 280px;
            object-fit: contain;
            background: #111;
            border-radius: 6px;
        }
        .photo-info {
            font-size: 0.8em;
            color: #888;
            margin-top: 4px;
            word-break: break-all;
        }
        .help {
            color: #666;
            font-size: 0.75em;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <div class="header">
        <a class="back-btn" href="/date/{{ back_date }}">← Back</a>
        <h1>Group {{ group_id }} ({{ photos | length }} photos)</h1>
        <div class="selection-info" id="selectionInfo">0 selected</div>
        <button class="split-btn" id="splitBtn" onclick="splitSelected()" disabled>Split Selected</button>
    </div>
    <div class="help">Click photos to select, then click "Split Selected" to move them to a new group</div>
    <div class="photos">
        {% for photo in photos %}
        <div class="photo" data-photo-id="{{ photo.id }}" onclick="togglePhoto(this)">
            <img src="/image/{{ photo.id }}" alt="">
            <div class="photo-info">{{ photo.path }}</div>
        </div>
        {% endfor %}
    </div>
    <script>
        const groupId = {{ group_id | tojson }};
        const totalPhotos = {{ photos | length }};
        let selected = new Set();

        function togglePhoto(el) {
            const photoId = el.dataset.photoId;
            if (el.classList.contains('selected')) {
                el.classList.remove('selected');
                selected.delete(photoId);
            } else {
                el.classList.add('selected');
                selected.add(photoId);
            }
            updateSelectionInfo();
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            info.textContent = `${selected.size} selected`;
            info.classList.toggle('has-selection', selected.size > 0);
            // Can split if at least 1 selected and not all selected
            document.getElementById('splitBtn').disabled = selected.size === 0 || selected.size === totalPhotos;
        }

        function splitSelected() {
            if (selected.size === 0 || selected.size === totalPhotos) return;

            const photoIds = Array.from(selected);

            fetch('/api/split', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ group_id: groupId, photo_ids: photoIds })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    // Reload to show updated group
                    window.location.reload();
                } else {
                    alert('Split failed: ' + data.error);
                }
            });
        }
    </script>
</body>
</html>
"""


@app.route('/group/<group_id>')
def show_group(group_id: str):
    """Show all photos in a group."""
    conn = get_connection()

    if group_id.startswith('G_'):
        # Composite group
        gid = int(group_id[2:])
        cursor = conn.execute("""
            SELECT cg.photo_id, pp.source_path
            FROM composite_groups cg
            JOIN photo_paths pp ON cg.photo_id = pp.photo_id
            WHERE cg.group_id = ?
            GROUP BY cg.photo_id
            ORDER BY pp.source_path
        """, (gid,))
    elif group_id.startswith('S_'):
        # Singleton
        photo_id = group_id[2:]
        cursor = conn.execute("""
            SELECT pp.photo_id, pp.source_path
            FROM photo_paths pp
            WHERE pp.photo_id = ?
            LIMIT 1
        """, (photo_id,))
    else:
        conn.close()
        return "Invalid group ID", 400

    photos = [{'id': row[0], 'path': row[1]} for row in cursor.fetchall()]

    # Get the date for back link
    cache = get_cache(conn)
    date_info = cache['date_info'].get(group_id)
    back_date = get_date_for_display(date_info.date_value) if date_info else 'unknown'

    conn.close()

    return render_template_string(
        GROUP_DETAIL_TEMPLATE,
        group_id=group_id,
        photos=photos,
        back_date=back_date,
    )


if __name__ == '__main__':
    conn = get_connection()

    # Check required tables exist
    cursor = conn.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='table' AND name='composite_groups'
    """)
    if cursor.fetchone()[0] == 0:
        print("ERROR: composite_groups table not found!")
        print("Run pipeline2 stage 3 first.")
        sys.exit(1)

    cursor = conn.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='view' AND name='kept_photos'
    """)
    if cursor.fetchone()[0] == 0:
        print("ERROR: kept_photos view not found!")
        sys.exit(1)

    print("Building date index (this may take a moment)...")
    dates = get_all_dates_with_groups(conn)

    cursor = conn.execute("SELECT COUNT(DISTINCT group_id) FROM composite_groups")
    group_count = cursor.fetchone()[0]

    cursor = conn.execute("""
        SELECT COUNT(*)
        FROM kept_photos kp
        LEFT JOIN composite_groups cg ON kp.id = cg.photo_id
        WHERE cg.photo_id IS NULL
    """)
    singleton_count = cursor.fetchone()[0]

    conn.close()

    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Dates with photos: {len(dates)}")
    print(f"Composite groups: {group_count}")
    print(f"Singletons: {singleton_count}")
    if dates:
        print(f"Date range: {dates[0]} to {dates[-1]}")
    print()
    print("Starting server at http://localhost:5006")
    print()
    print("Keyboard shortcuts:")
    print("  ← / → - Previous / next date")
    print("  m - Merge selected groups")
    print("  Escape - Clear selection")
    print()
    app.run(debug=True, port=5006)
