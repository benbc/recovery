#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
#   "pillow",
# ]
# ///
"""
Junk Reviewer - Review kept photos/groups and mark junk for deletion

Shows a grid of photos (one per group, plus ungrouped photos).
Navigate with arrow keys, space to mark for deletion, enter to save and continue.

Keyboard shortcuts:
  Arrow keys - Move focus in grid
  Space - Toggle delete mark on focused photo
  Enter - Save deletions and go to next page
  Escape - Clear all marks on current page
  d - Toggle delete on focused (same as space)
"""

import sqlite3
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template_string, request, send_file, jsonify

# Paths
DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"
FILES_DIR = Path(__file__).parent.parent / "output" / "files"

app = Flask(__name__)

PHOTOS_PER_PAGE = 15  # 5x3 grid

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
    """Create tables for tracking junk review."""
    conn = get_connection()

    # Track junk deletions
    conn.execute("""
        CREATE TABLE IF NOT EXISTS junk_deletions (
            photo_id TEXT PRIMARY KEY,
            group_id INTEGER,  -- NULL for ungrouped
            deleted_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def get_review_items():
    """
    Get all items to review: one representative photo per group + ungrouped photos.
    Returns list of (photo_id, group_id_or_none, mime_type, width, height, path).
    Excludes already-deleted items.
    """
    conn = get_connection()

    # Representative photo for each group (highest resolution kept photo)
    # Excludes groups where all photos are rejected
    group_photos = conn.execute("""
        WITH kept_in_groups AS (
            SELECT dg.group_id, dg.photo_id, p.width, p.height, p.mime_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY dg.group_id
                       ORDER BY (p.width * p.height) DESC, p.file_size DESC
                   ) as rn
            FROM duplicate_groups dg
            JOIN photos p ON dg.photo_id = p.id
            LEFT JOIN group_rejections gr ON dg.photo_id = gr.photo_id
            LEFT JOIN junk_deletions jd ON dg.photo_id = jd.photo_id
            WHERE gr.photo_id IS NULL AND jd.photo_id IS NULL
        )
        SELECT photo_id, group_id, mime_type, width, height
        FROM kept_in_groups
        WHERE rn = 1
        ORDER BY (width * height) DESC
    """).fetchall()

    # Ungrouped photos (not in any duplicate group, not individually rejected)
    ungrouped_photos = conn.execute("""
        SELECT p.id as photo_id, NULL as group_id, p.mime_type, p.width, p.height
        FROM photos p
        LEFT JOIN duplicate_groups dg ON p.id = dg.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        WHERE dg.photo_id IS NULL
          AND id.photo_id IS NULL
          AND jd.photo_id IS NULL
        ORDER BY (p.width * p.height) DESC
    """).fetchall()

    conn.close()

    # Combine: groups first (larger impact), then ungrouped
    items = []
    for row in group_photos:
        items.append({
            'photo_id': row['photo_id'],
            'group_id': row['group_id'],
            'mime_type': row['mime_type'],
            'width': row['width'],
            'height': row['height'],
            'is_group': True
        })
    for row in ungrouped_photos:
        items.append({
            'photo_id': row['photo_id'],
            'group_id': None,
            'mime_type': row['mime_type'],
            'width': row['width'],
            'height': row['height'],
            'is_group': False
        })

    return items


def get_stats():
    """Get review statistics."""
    conn = get_connection()

    # Count groups with kept photos
    groups = conn.execute("""
        SELECT COUNT(DISTINCT dg.group_id) as cnt
        FROM duplicate_groups dg
        LEFT JOIN group_rejections gr ON dg.photo_id = gr.photo_id
        LEFT JOIN junk_deletions jd ON dg.photo_id = jd.photo_id
        WHERE gr.photo_id IS NULL AND jd.photo_id IS NULL
    """).fetchone()['cnt']

    # Count ungrouped kept photos
    ungrouped = conn.execute("""
        SELECT COUNT(*) as cnt
        FROM photos p
        LEFT JOIN duplicate_groups dg ON p.id = dg.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        WHERE dg.photo_id IS NULL AND id.photo_id IS NULL AND jd.photo_id IS NULL
    """).fetchone()['cnt']

    # Count deleted
    deleted = conn.execute("SELECT COUNT(*) as cnt FROM junk_deletions").fetchone()['cnt']

    conn.close()

    return {
        'groups': groups,
        'ungrouped': ungrouped,
        'total': groups + ungrouped,
        'deleted': deleted
    }


def delete_items(items: list[dict]):
    """Mark items as junk (deleted)."""
    conn = get_connection()
    now = datetime.now().isoformat()

    for item in items:
        if item.get('group_id') is not None:
            # Delete entire group - mark all kept photos in the group
            conn.execute("""
                INSERT OR IGNORE INTO junk_deletions (photo_id, group_id, deleted_at)
                SELECT dg.photo_id, dg.group_id, ?
                FROM duplicate_groups dg
                LEFT JOIN group_rejections gr ON dg.photo_id = gr.photo_id
                WHERE dg.group_id = ? AND gr.photo_id IS NULL
            """, (now, item['group_id']))
        else:
            # Delete single ungrouped photo
            conn.execute("""
                INSERT OR IGNORE INTO junk_deletions (photo_id, group_id, deleted_at)
                VALUES (?, NULL, ?)
            """, (item['photo_id'], now))

    conn.commit()
    conn.close()


# Cache items list - stable for the session
_items_cache = None
_session_deleted = set()  # Track deletions within session (by group_id or photo_id)

def get_items_cached():
    global _items_cache
    if _items_cache is None:
        _items_cache = get_review_items()
    return _items_cache

def mark_session_deleted(items: list[dict]):
    """Mark items as deleted in the session (for stable pagination)."""
    for item in items:
        key = item.get('group_id') or item.get('photo_id')
        if key:
            _session_deleted.add(str(key))

def is_session_deleted(item: dict) -> bool:
    """Check if item was deleted in this session."""
    key = str(item.get('group_id') or item.get('photo_id'))
    return key in _session_deleted

def invalidate_cache():
    global _items_cache, _session_deleted
    _items_cache = None
    _session_deleted = set()


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Junk Reviewer - Page {{ page }}</title>
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

        .header {
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid #333;
        }

        .nav { display: flex; align-items: center; gap: 10px; }
        .nav-btn {
            padding: 6px 14px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
        }
        .nav-btn:hover { background: #444; }
        .nav-btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .nav-btn.danger { background: #dc2626; }
        .nav-btn.danger:hover { background: #b91c1c; }
        .nav-btn.primary { background: #059669; }
        .nav-btn.primary:hover { background: #047857; }

        .stats { color: #888; font-size: 0.9em; }
        .stats .deleted { color: #f87171; }

        .marked-count {
            padding: 4px 12px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .marked-count.has-marked { background: #dc2626; }

        .help {
            color: #666;
            font-size: 0.75em;
            margin-left: auto;
        }
        .help kbd {
            background: #333;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: monospace;
        }

        .grid-container { flex: 1; overflow-y: auto; padding: 5px 0; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 10px;
        }

        .item {
            background: #222;
            border-radius: 8px;
            overflow: hidden;
            border: 3px solid transparent;
            cursor: pointer;
            transition: border-color 0.1s, opacity 0.1s;
            position: relative;
        }
        .item:hover { border-color: #444; }
        .item.focused { border-color: #6cf; }
        .item.marked { border-color: #dc2626; opacity: 0.6; }
        .item.marked::after {
            content: '✕';
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 64px;
            color: #dc2626;
            text-shadow: 0 0 10px rgba(0,0,0,0.8);
            pointer-events: none;
        }

        .item img {
            width: 100%;
            height: 250px;
            object-fit: contain;
            background: #111;
            border-radius: 5px;
            image-orientation: from-image;
        }

        /* Lightbox for detail view */
        .lightbox {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0,0,0,0.95);
            z-index: 1000;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }
        .lightbox.open { display: flex; }
        .lightbox img {
            max-width: 90vw;
            max-height: 80vh;
            object-fit: contain;
            image-orientation: from-image;
        }
        .lightbox-info {
            margin-top: 15px;
            color: #888;
            text-align: center;
        }
        .lightbox-actions {
            margin-top: 15px;
            display: flex;
            gap: 10px;
        }
        .lightbox-actions button {
            padding: 10px 20px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
        }
        .lightbox-actions .delete-btn { background: #dc2626; color: white; }
        .lightbox-actions .delete-btn:hover { background: #b91c1c; }
        .lightbox-actions .close-btn { background: #333; color: white; }
        .lightbox-actions .close-btn:hover { background: #444; }
        .lightbox-hint {
            position: absolute;
            bottom: 20px;
            color: #666;
            font-size: 0.8em;
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

        .done-message {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 100%;
            color: #888;
            font-size: 1.2em;
        }
        .done-message .big { font-size: 2em; margin-bottom: 20px; }
    </style>
</head>
<body>
    {% if items %}
    <div class="header">
        <div class="nav">
            <button class="nav-btn" onclick="prevPage()" {{ 'disabled' if page == 1 else '' }}>&larr; Prev</button>
            <span>Page {{ page }} / {{ total_pages }}</span>
            <button class="nav-btn" onclick="nextPage()" {{ 'disabled' if page == total_pages else '' }}>Next &rarr;</button>
        </div>

        <div class="stats">
            {{ stats.total }} remaining
            {% if stats.deleted > 0 %}
            (<span class="deleted">{{ stats.deleted }} deleted</span>)
            {% endif %}
        </div>

        <div class="marked-count" id="markedCount">0 marked</div>

        <button class="nav-btn danger" id="deleteBtn" onclick="saveAndContinue()" disabled>
            Delete marked &amp; continue
        </button>

        <div class="help">
            <kbd>Click</kbd> toggle mark &nbsp;
            <kbd>Double-click</kbd> detail view &nbsp;
            <kbd>Enter</kbd> save &amp; next &nbsp;
            <kbd>Esc</kbd> clear marks
        </div>
    </div>

    <div class="grid-container">
        <div class="grid" id="grid">
            {% for item in items %}
            {% if not item.deleted %}
            <div class="item"
                 data-index="{{ loop.index0 }}"
                 data-photo-id="{{ item.photo_id }}"
                 data-group-id="{{ item.group_id or '' }}"
                 data-is-group="{{ 'true' if item.is_group else 'false' }}"
                 data-width="{{ item.width }}"
                 data-height="{{ item.height }}"
                 onclick="toggleMark(this)"
                 ondblclick="openLightbox(this, event)">
                <img src="/image/{{ item.photo_id }}" loading="lazy">
            </div>
            {% endif %}
            {% endfor %}
        </div>
    </div>
    {% else %}
    <div class="done-message">
        <div class="big">All done!</div>
        <div>{{ stats.deleted }} items marked as junk</div>
    </div>
    {% endif %}

    <div class="toast" id="toast"></div>

    <!-- Lightbox for detail view -->
    <div class="lightbox" id="lightbox" onclick="closeLightbox(event)">
        <img id="lightboxImg" src="" onclick="event.stopPropagation()">
        <div class="lightbox-info" id="lightboxInfo"></div>
        <div class="lightbox-actions" onclick="event.stopPropagation()">
            <button class="delete-btn" onclick="toggleMarkFromLightbox()">Mark for deletion (d)</button>
            <button class="close-btn" onclick="closeLightbox()">Close (Esc)</button>
        </div>
        <div class="lightbox-hint">Click outside or press Escape to close</div>
    </div>

    <script>
        let focusedIndex = 0;
        const markedIds = new Set();
        const gridCols = Math.floor(document.querySelector('.grid')?.offsetWidth / 188) || 4;

        function getItems() {
            return Array.from(document.querySelectorAll('.item'));
        }

        function updateFocus(newIndex) {
            const items = getItems();
            if (items.length === 0) return;

            // Clamp index
            newIndex = Math.max(0, Math.min(newIndex, items.length - 1));

            // Update visual focus
            items[focusedIndex]?.classList.remove('focused');
            items[newIndex]?.classList.add('focused');
            items[newIndex]?.scrollIntoView({ block: 'nearest' });

            focusedIndex = newIndex;
        }

        function toggleMark(el) {
            const photoId = el.dataset.photoId;
            const groupId = el.dataset.groupId;
            const key = groupId || photoId;

            if (el.classList.contains('marked')) {
                el.classList.remove('marked');
                markedIds.delete(key);
            } else {
                el.classList.add('marked');
                markedIds.add(key);
            }

            updateMarkedCount();
        }

        // Lightbox functions
        let currentLightboxItem = null;

        function openLightbox(el, event) {
            event.stopPropagation();
            currentLightboxItem = el;

            const photoId = el.dataset.photoId;
            const width = el.dataset.width;
            const height = el.dataset.height;
            const isGroup = el.dataset.isGroup === 'true';
            const isMarked = el.classList.contains('marked');

            document.getElementById('lightboxImg').src = `/image/${photoId}`;
            document.getElementById('lightboxInfo').textContent =
                `${width}x${height}${isGroup ? ' (group)' : ''}${isMarked ? ' - MARKED FOR DELETION' : ''}`;
            document.getElementById('lightbox').classList.add('open');
        }

        function closeLightbox(event) {
            if (event && event.target !== document.getElementById('lightbox')) return;
            document.getElementById('lightbox').classList.remove('open');
            currentLightboxItem = null;
        }

        function toggleMarkFromLightbox() {
            if (currentLightboxItem) {
                toggleMark(currentLightboxItem);
                // Update lightbox info
                const isMarked = currentLightboxItem.classList.contains('marked');
                const info = document.getElementById('lightboxInfo');
                const base = info.textContent.replace(' - MARKED FOR DELETION', '');
                info.textContent = base + (isMarked ? ' - MARKED FOR DELETION' : '');
            }
        }

        function updateMarkedCount() {
            const count = markedIds.size;
            const el = document.getElementById('markedCount');
            const btn = document.getElementById('deleteBtn');

            el.textContent = `${count} marked`;
            el.classList.toggle('has-marked', count > 0);
            btn.disabled = count === 0;
        }

        function showToast(message, type = '') {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show ' + type;
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        function saveAndContinue() {
            if (markedIds.size === 0) return;

            const items = getItems().filter(el => el.classList.contains('marked')).map(el => ({
                photo_id: el.dataset.photoId,
                group_id: el.dataset.groupId || null
            }));

            fetch('/api/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ items, page: {{ page }} })
            })
            .then(r => r.json())
            .then(data => {
                showToast(`Deleted ${data.count} items`, 'success');
                setTimeout(() => {
                    window.location.href = `/page/${data.next_page}`;
                }, 500);
            })
            .catch(() => showToast('Delete failed', 'error'));
        }

        function prevPage() {
            {% if page > 1 %}
            window.location.href = '/page/{{ page - 1 }}';
            {% endif %}
        }

        function nextPage() {
            {% if page < total_pages %}
            window.location.href = '/page/{{ page + 1 }}';
            {% endif %}
        }

        // Keyboard navigation
        document.addEventListener('keydown', (e) => {
            const lightboxOpen = document.getElementById('lightbox').classList.contains('open');

            // Lightbox-specific keys
            if (lightboxOpen) {
                if (e.key === 'Escape') {
                    e.preventDefault();
                    closeLightbox();
                } else if (e.key === 'd' || e.key === ' ') {
                    e.preventDefault();
                    toggleMarkFromLightbox();
                }
                return;
            }

            const items = getItems();
            if (items.length === 0) return;

            switch(e.key) {
                case 'ArrowRight':
                    e.preventDefault();
                    updateFocus(focusedIndex + 1);
                    break;
                case 'ArrowLeft':
                    e.preventDefault();
                    updateFocus(focusedIndex - 1);
                    break;
                case 'ArrowDown':
                    e.preventDefault();
                    updateFocus(focusedIndex + gridCols);
                    break;
                case 'ArrowUp':
                    e.preventDefault();
                    updateFocus(focusedIndex - gridCols);
                    break;
                case ' ':
                case 'd':
                    e.preventDefault();
                    toggleMark(items[focusedIndex]);
                    break;
                case 'Enter':
                    e.preventDefault();
                    if (markedIds.size > 0) {
                        saveAndContinue();
                    } else {
                        nextPage();
                    }
                    break;
                case 'Escape':
                    e.preventDefault();
                    items.forEach(el => el.classList.remove('marked'));
                    markedIds.clear();
                    updateMarkedCount();
                    break;
            }
        });

        // Preload next page images
        const preloadIds = {{ preload_ids | tojson }};
        preloadIds.forEach(id => {
            const img = new Image();
            img.src = `/image/${id}`;
        });
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    """Redirect to first page."""
    return app.redirect('/page/1')


@app.route('/page/<int:page>')
def show_page(page):
    """Show a page of items to review."""
    items = get_items_cached()
    stats = get_stats()

    total_pages = max(1, (len(items) + PHOTOS_PER_PAGE - 1) // PHOTOS_PER_PAGE)
    page = max(1, min(page, total_pages))

    start = (page - 1) * PHOTOS_PER_PAGE
    end = start + PHOTOS_PER_PAGE
    page_items = items[start:end]

    # Mark session-deleted items
    for item in page_items:
        item['deleted'] = is_session_deleted(item)

    # Get next page items for preloading
    next_page_items = []
    if page < total_pages:
        next_start = end
        next_end = next_start + PHOTOS_PER_PAGE
        next_page_items = [item for item in items[next_start:next_end] if not is_session_deleted(item)]

    return render_template_string(
        TEMPLATE,
        items=page_items,
        page=page,
        total_pages=total_pages,
        stats=stats,
        preload_ids=[item['photo_id'] for item in next_page_items],
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


@app.route('/api/delete', methods=['POST'])
def api_delete():
    """Delete marked items."""
    data = request.json
    items = data['items']
    current_page = data.get('page', 1)

    delete_items(items)
    mark_session_deleted(items)  # Track for stable pagination

    # Calculate next page
    all_items = get_items_cached()
    total_pages = max(1, (len(all_items) + PHOTOS_PER_PAGE - 1) // PHOTOS_PER_PAGE)
    next_page = min(current_page + 1, total_pages)

    return jsonify({'success': True, 'count': len(items), 'next_page': next_page})


if __name__ == '__main__':
    init_tables()
    stats = get_stats()
    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Items to review: {stats['total']:,} ({stats['groups']:,} groups, {stats['ungrouped']:,} ungrouped)")
    print(f"Already deleted: {stats['deleted']:,}")
    print()
    print("Starting server at http://localhost:5003")
    print()
    print("Keyboard shortcuts:")
    print("  Arrow keys - Navigate grid")
    print("  Space/d - Toggle delete mark")
    print("  Enter - Save deletions & next page (or just next if none marked)")
    print("  Escape - Clear all marks")
    app.run(debug=True, port=5003)
