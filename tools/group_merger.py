#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Group Merger - Browse ungrouped pairs and merge them into composite groups.

Shows pairs from the review_zone_pairs table (pre-extracted by extract_review_zone.py).
Order is stable (by photo_id) so you can resume after code changes.

Keyboard shortcuts:
  1-9, 0, -, = - Toggle pair selection
  Enter/Space - Save merges and go to next page
  a - Select all
  n - Select none
  b - Back to previous page
"""

import sqlite3
import sys
import time
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
    ext = MIME_TO_EXT.get(mime_type, ".bin")
    return FILES_DIR / photo_id[:2] / f"{photo_id}{ext}"


def get_next_group_id(conn) -> int:
    """Get the next available group ID."""
    cursor = conn.execute("SELECT MAX(group_id) FROM composite_groups")
    row = cursor.fetchone()
    return (row[0] or 0) + 1


def merge_groups(conn, group1: str, group2: str) -> str:
    """
    Merge two groups. Returns the resulting group identifier.

    Groups are identified as:
    - 'G_<id>' for existing composite groups
    - 'S_<photo_id>' for singletons
    """
    if group1 == group2:
        return group1

    g1_is_singleton = group1.startswith('S_')
    g2_is_singleton = group2.startswith('S_')

    if g1_is_singleton and g2_is_singleton:
        photo1 = group1[2:]
        photo2 = group2[2:]
        new_group_id = get_next_group_id(conn)
        conn.execute(
            "INSERT INTO composite_groups (photo_id, group_id) VALUES (?, ?)",
            (photo1, new_group_id)
        )
        conn.execute(
            "INSERT INTO composite_groups (photo_id, group_id) VALUES (?, ?)",
            (photo2, new_group_id)
        )
        conn.commit()
        return f"G_{new_group_id}"

    elif g1_is_singleton:
        photo1 = group1[2:]
        group2_id = int(group2[2:])
        conn.execute(
            "INSERT INTO composite_groups (photo_id, group_id) VALUES (?, ?)",
            (photo1, group2_id)
        )
        conn.commit()
        return group2

    elif g2_is_singleton:
        photo2 = group2[2:]
        group1_id = int(group1[2:])
        conn.execute(
            "INSERT INTO composite_groups (photo_id, group_id) VALUES (?, ?)",
            (photo2, group1_id)
        )
        conn.commit()
        return group1

    else:
        group1_id = int(group1[2:])
        group2_id = int(group2[2:])

        c1 = conn.execute(
            "SELECT COUNT(*) FROM composite_groups WHERE group_id = ?",
            (group1_id,)
        ).fetchone()[0]
        c2 = conn.execute(
            "SELECT COUNT(*) FROM composite_groups WHERE group_id = ?",
            (group2_id,)
        ).fetchone()[0]

        if c1 >= c2:
            target, source = group1_id, group2_id
        else:
            target, source = group2_id, group1_id

        conn.execute(
            "UPDATE composite_groups SET group_id = ? WHERE group_id = ?",
            (target, source)
        )
        conn.commit()
        return f"G_{target}"


def get_candidate_pairs(start_index: int = 0, seen_group_pairs: set = None):
    """
    Get candidate pairs for merging from review_zone_pairs table.

    Uses stable ordering (photo_id_1, photo_id_2) so position is resumable.

    Returns:
        (pairs, new_index, seen_group_pairs)
    """
    if seen_group_pairs is None:
        seen_group_pairs = set()

    conn = get_connection()

    # Load current group membership
    cursor = conn.execute("SELECT photo_id, group_id FROM composite_groups")
    photo_to_group = {row['photo_id']: row['group_id'] for row in cursor.fetchall()}

    # Query review zone pairs with stable ordering
    # Fetch in batches to handle filtering
    batch_size = 100
    pairs = []
    current_offset = start_index

    while len(pairs) < PAIRS_PER_PAGE:
        cursor = conn.execute("""
            SELECT photo_id_1, photo_id_2, phash16_dist, colorhash_dist
            FROM review_zone_pairs
            ORDER BY photo_id_1, photo_id_2
            LIMIT ? OFFSET ?
        """, (batch_size, current_offset))
        rows = cursor.fetchall()

        if not rows:
            break  # No more pairs

        for row in rows:
            current_offset += 1
            photo1, photo2 = row['photo_id_1'], row['photo_id_2']

            # Get groups
            g1 = photo_to_group.get(photo1)
            g2 = photo_to_group.get(photo2)

            # Skip if same group
            if g1 is not None and g1 == g2:
                continue

            # Build group identifiers
            group1 = f"G_{g1}" if g1 is not None else f"S_{photo1}"
            group2 = f"G_{g2}" if g2 is not None else f"S_{photo2}"

            # Normalize group pair order for deduplication
            gp = tuple(sorted([group1, group2]))

            # Skip if we've seen this group-pair
            if gp in seen_group_pairs:
                continue

            seen_group_pairs.add(gp)
            pairs.append({
                'photo1': photo1,
                'photo2': photo2,
                'group1': group1,
                'group2': group2,
                'phash16_dist': row['phash16_dist'],
                'colorhash_dist': row['colorhash_dist'],
            })

            if len(pairs) >= PAIRS_PER_PAGE:
                break

    conn.close()
    return pairs, current_offset, seen_group_pairs


# Session state
_session = {
    'seen_group_pairs': set(),
    'page_num': 1,
    'history': [],  # Stack of (page_num, index, seen_group_pairs) for back navigation
    'index': 0,  # Current position in ordered list
}


def reset_session():
    """Reset session state."""
    _session['seen_group_pairs'] = set()
    _session['page_num'] = 1
    _session['history'] = []
    _session['index'] = 0


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Group Merger - Page {{ page_num }}</title>
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

        .nav-group {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .nav-btn {
            padding: 6px 12px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            font-size: 13px;
        }
        .nav-btn:hover { background: #444; }
        .nav-btn:disabled { opacity: 0.4; cursor: not-allowed; }

        .page-input {
            width: 60px;
            padding: 5px 8px;
            background: #333;
            border: 1px solid #444;
            border-radius: 5px;
            color: #eee;
            font-size: 13px;
            text-align: center;
        }
        .page-input:focus { outline: none; border-color: #2563eb; }

        .page-info { color: #888; font-size: 0.9em; }

        .selection-info {
            padding: 4px 12px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #059669; }

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
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 8px;
            padding: 5px 0;
        }

        .pair {
            background: #222;
            border-radius: 8px;
            padding: 6px;
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
            margin-bottom: 4px;
            font-size: 0.75em;
        }
        .pair-num {
            background: #333;
            padding: 2px 8px;
            border-radius: 4px;
            font-weight: 600;
        }
        .pair.selected .pair-num { background: #059669; }
        .pair-dist { color: #666; font-size: 0.9em; }

        .pair-images {
            display: flex;
            gap: 4px;
        }
        .pair-photo {
            flex: 1;
            min-width: 0;
        }
        .pair-photo img {
            width: 100%;
            height: 180px;
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
        <div class="nav-group">
            <button class="nav-btn" onclick="goBack()" {{ 'disabled' if not has_history else '' }} title="Back (b)">← Back</button>
            <span class="page-info">Page</span>
            <input type="number" class="page-input" id="pageInput" value="{{ page_num }}" min="1"
                   onkeydown="if(event.key==='Enter'){goToPage();event.preventDefault();}">
            <button class="nav-btn" onclick="goToPage()" title="Go to page (g)">Go</button>
            <span class="page-info">(index {{ index }})</span>
        </div>

        <div class="selection-info" id="selectionInfo">
            0 / {{ pairs|length }} selected
        </div>

        <div class="actions">
            <button class="action-btn select-none" onclick="selectNone()" title="Clear (n)">None</button>
            <button class="action-btn select-all" onclick="selectAll()" title="Select all (a)">All</button>
            <button class="action-btn save" onclick="saveAndNext()" title="Merge selected & next (Enter)">Merge & Next</button>
        </div>
    </div>

    <div class="help">
        <kbd>1</kbd>-<kbd>9</kbd>, <kbd>0</kbd>, <kbd>-</kbd>, <kbd>=</kbd> toggle &nbsp;
        <kbd>a</kbd> all &nbsp;
        <kbd>n</kbd> none &nbsp;
        <kbd>b</kbd> back &nbsp;
        <kbd>g</kbd> go to page &nbsp;
        <kbd>Enter</kbd> merge & next
    </div>

    <div class="pairs-container">
        {% if pairs %}
        <div class="pairs">
            {% for pair in pairs %}
            <div class="pair"
                 data-photo1="{{ pair.photo1 }}"
                 data-photo2="{{ pair.photo2 }}"
                 data-group1="{{ pair.group1 }}"
                 data-group2="{{ pair.group2 }}"
                 data-index="{{ loop.index0 }}"
                 onclick="togglePair(this)">
                <div class="pair-header">
                    <span class="pair-num">{{ loop.index }}</span>
                    <span class="pair-dist">p{{ pair.phash16_dist }} c{{ pair.colorhash_dist }}</span>
                </div>
                <div class="pair-images">
                    <div class="pair-photo">
                        <img src="/image/{{ pair.photo1 }}" alt="">
                    </div>
                    <div class="pair-photo">
                        <img src="/image/{{ pair.photo2 }}" alt="">
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="empty-state">
            <h2>No more pairs</h2>
            <p>All remaining pairs are already in the same group, or you've reached the end.</p>
        </div>
        {% endif %}
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const keyMap = ['1','2','3','4','5','6','7','8','9','0','-','='];
        const pairCount = {{ pairs|length }};
        let selected = new Set();

        // Preload next page images
        const preloadIds = {{ preload_ids | tojson }};
        preloadIds.forEach(id => {
            const img = new Image();
            img.src = '/image/' + id;
        });

        function togglePair(el) {
            const idx = parseInt(el.dataset.index);
            if (el.classList.contains('selected')) {
                el.classList.remove('selected');
                selected.delete(idx);
            } else {
                el.classList.add('selected');
                selected.add(idx);
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
            document.querySelectorAll('.pair').forEach((el, i) => {
                el.classList.add('selected');
                selected.add(i);
            });
            updateSelectionInfo();
        }

        function selectNone() {
            document.querySelectorAll('.pair').forEach(el => {
                el.classList.remove('selected');
            });
            selected.clear();
            updateSelectionInfo();
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            info.textContent = `${selected.size} / ${pairCount} selected`;
            info.classList.toggle('has-selection', selected.size > 0);
        }

        function showToast(message) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show success';
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        function goBack() {
            window.location.href = '/back';
        }

        function goToPage() {
            const page = parseInt(document.getElementById('pageInput').value);
            if (page > 0) {
                window.location.href = '/goto/' + page;
            }
        }

        function saveAndNext() {
            const pairs = document.querySelectorAll('.pair');
            const toMerge = [];

            selected.forEach(idx => {
                const el = pairs[idx];
                toMerge.push({
                    photo1: el.dataset.photo1,
                    photo2: el.dataset.photo2,
                    group1: el.dataset.group1,
                    group2: el.dataset.group2,
                });
            });

            fetch('/api/merge', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pairs: toMerge })
            })
            .then(r => r.json())
            .then(data => {
                showToast(`Merged ${data.merged} pairs`);
                setTimeout(() => {
                    window.location.href = '/next';
                }, 400);
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
                case 'b':
                    goBack();
                    break;
                case 'g':
                    document.getElementById('pageInput').focus();
                    document.getElementById('pageInput').select();
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


@app.route('/')
def index():
    """Show current page of candidate pairs."""
    pairs, new_index, seen = get_candidate_pairs(
        _session['index'],
        _session['seen_group_pairs'].copy()
    )

    # Update session for next page
    _session['index'] = new_index
    _session['seen_group_pairs'] = seen

    # Prefetch next page for preloading
    next_pairs, _, _ = get_candidate_pairs(
        new_index,
        seen.copy()
    )
    preload_ids = []
    for p in next_pairs:
        preload_ids.extend([p['photo1'], p['photo2']])

    return render_template_string(
        TEMPLATE,
        pairs=pairs,
        page_num=_session['page_num'],
        index=_session['index'],
        has_history=len(_session['history']) > 0,
        preload_ids=preload_ids,
    )


@app.route('/next')
def next_page():
    """Advance to next page, saving current state for back navigation."""
    _session['history'].append({
        'page_num': _session['page_num'],
        'index': _session['index'],
        'seen_group_pairs': _session['seen_group_pairs'].copy(),
    })
    _session['page_num'] += 1
    return app.redirect('/')


@app.route('/back')
def back_page():
    """Go back to previous page."""
    if _session['history']:
        state = _session['history'].pop()
        _session['page_num'] = state['page_num']
        _session['index'] = state['index']
        _session['seen_group_pairs'] = state['seen_group_pairs']
    return app.redirect('/')


@app.route('/goto/<int:page>')
def goto_page(page):
    """Jump to a specific page number."""
    if page < 1:
        page = 1

    # Reset and scan to the target page
    _session['index'] = 0
    _session['seen_group_pairs'] = set()
    _session['page_num'] = 1
    _session['history'] = []

    # Skip forward to the target page
    for _ in range(page - 1):
        pairs, new_index, seen = get_candidate_pairs(
            _session['index'],
            _session['seen_group_pairs']
        )
        if not pairs:
            break
        _session['index'] = new_index
        _session['seen_group_pairs'] = seen
        _session['page_num'] += 1

    return app.redirect('/')


@app.route('/reset')
def reset():
    """Reset to beginning."""
    reset_session()
    return app.redirect('/')


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


@app.route('/api/merge', methods=['POST'])
def api_merge():
    """Merge selected pairs."""
    data = request.json
    pairs = data.get('pairs', [])

    conn = get_connection()
    merged = 0

    group_mapping = {}

    for pair in pairs:
        group1 = pair['group1']
        group2 = pair['group2']

        while group1 in group_mapping:
            group1 = group_mapping[group1]
        while group2 in group_mapping:
            group2 = group_mapping[group2]

        if group1 != group2:
            result = merge_groups(conn, group1, group2)
            if result != group1:
                group_mapping[group1] = result
            if result != group2:
                group_mapping[group2] = result
            merged += 1

    conn.close()

    # Update seen_group_pairs to account for merges
    new_seen = set()
    for gp in _session['seen_group_pairs']:
        g1, g2 = gp
        while g1 in group_mapping:
            g1 = group_mapping[g1]
        while g2 in group_mapping:
            g2 = group_mapping[g2]
        if g1 != g2:
            new_seen.add(tuple(sorted([g1, g2])))
    _session['seen_group_pairs'] = new_seen

    return jsonify({'success': True, 'merged': merged})


if __name__ == '__main__':
    # Check review_zone_pairs table exists
    conn = get_connection()
    cursor = conn.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='table' AND name='review_zone_pairs'
    """)
    if cursor.fetchone()[0] == 0:
        print("ERROR: review_zone_pairs table not found!")
        print("Run: tools/run extract_review_zone")
        sys.exit(1)

    cursor = conn.execute("SELECT COUNT(*) FROM review_zone_pairs")
    pair_count = cursor.fetchone()[0]
    conn.close()

    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Review zone pairs: {pair_count}")
    print()
    print("Starting server at http://localhost:5005")
    print()
    print("Keyboard shortcuts:")
    print("  1-9, 0, -, = - Toggle pair")
    print("  a - Select all")
    print("  n - Select none")
    print("  b - Back to previous page")
    print("  g - Focus page input")
    print("  Enter/Space - Merge & next")
    print()
    app.run(debug=True, port=5005)
