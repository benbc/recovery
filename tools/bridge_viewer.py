#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Bridge Viewer - Browse group pairs that might need merging

Shows pairs of groups ordered by how many "bridges" (unlinked pairs that satisfy
should_group) exist between them. Groups with many bridges are likely the same
scene that got artificially split.
"""

import sqlite3
from pathlib import Path
from collections import defaultdict

from flask import Flask, render_template_string, send_file, request

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


def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate hamming distance between two hex hash strings."""
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count("1")


def should_group(phash_dist: int, dhash_dist: int) -> bool:
    """Determine if two photos should be grouped based on thresholds."""
    if phash_dist <= 10:
        return True
    elif phash_dist <= 12:
        return dhash_dist < 22
    elif phash_dist <= 14:
        return dhash_dist <= 17
    return False


def get_group_photos(conn, group_id: int) -> list[dict]:
    """Get all photos in a group with their hashes."""
    cursor = conn.execute("""
        SELECT p.id, p.mime_type, p.width, p.height, p.perceptual_hash, p.dhash
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ?
        ORDER BY (p.width * p.height) DESC
    """, (group_id,))
    return [dict(row) for row in cursor.fetchall()]


def get_blockers_between_groups(conn, group1_id: int, group2_id: int, limit: int = 6) -> list[dict]:
    """Find the worst blocking pairs between two groups."""
    group1_photos = get_group_photos(conn, group1_id)
    group2_photos = get_group_photos(conn, group2_id)

    blockers = []
    for p1 in group1_photos:
        if not p1['perceptual_hash'] or not p1['dhash']:
            continue
        for p2 in group2_photos:
            if not p2['perceptual_hash'] or not p2['dhash']:
                continue

            phash_dist = hamming_distance(p1['perceptual_hash'], p2['perceptual_hash'])
            dhash_dist = hamming_distance(p1['dhash'], p2['dhash'])

            if not should_group(phash_dist, dhash_dist):
                blockers.append({
                    'photo1': p1['id'],
                    'photo2': p2['id'],
                    'phash_dist': phash_dist,
                    'dhash_dist': dhash_dist,
                })

    # Sort by worst (highest phash) first
    blockers.sort(key=lambda x: (-x['phash_dist'], -x['dhash_dist']))
    return blockers[:limit]


def get_group_pairs_by_bridges():
    """
    Find all pairs of groups with bridges between them.

    Returns list of dicts with group pair info, ordered by bridge count (descending).
    """
    conn = get_connection()

    # Get all unlinked pairs between different groups
    cursor = conn.execute("""
        SELECT
            up.photo_id_1, up.photo_id_2,
            up.phash_dist, up.dhash_dist,
            dg1.group_id as group1,
            dg2.group_id as group2
        FROM unlinked_pairs up
        JOIN duplicate_groups dg1 ON up.photo_id_1 = dg1.photo_id
        JOIN duplicate_groups dg2 ON up.photo_id_2 = dg2.photo_id
        WHERE up.reason = 'different_groups'
    """)

    # Aggregate by group pairs
    group_pairs = defaultdict(lambda: {
        'bridges': [],
        'min_phash': 999,
        'max_phash': 0,
        'avg_phash': 0,
    })

    for row in cursor:
        g1, g2 = row['group1'], row['group2']
        # Normalize order
        if g1 > g2:
            g1, g2 = g2, g1

        key = (g1, g2)
        group_pairs[key]['bridges'].append({
            'photo1': row['photo_id_1'],
            'photo2': row['photo_id_2'],
            'phash_dist': row['phash_dist'],
            'dhash_dist': row['dhash_dist'],
        })
        group_pairs[key]['min_phash'] = min(group_pairs[key]['min_phash'], row['phash_dist'])
        group_pairs[key]['max_phash'] = max(group_pairs[key]['max_phash'], row['phash_dist'])

    # Calculate averages and get representative photos for each group
    result = []
    for (g1, g2), data in group_pairs.items():
        bridge_count = len(data['bridges'])
        avg_phash = sum(b['phash_dist'] for b in data['bridges']) / bridge_count

        # Get representative photo for each group (highest resolution)
        cursor = conn.execute("""
            SELECT p.id, p.mime_type, p.width, p.height
            FROM duplicate_groups dg
            JOIN photos p ON dg.photo_id = p.id
            WHERE dg.group_id = ?
            ORDER BY (p.width * p.height) DESC
            LIMIT 1
        """, (g1,))
        rep1 = dict(cursor.fetchone())

        cursor = conn.execute("""
            SELECT p.id, p.mime_type, p.width, p.height
            FROM duplicate_groups dg
            JOIN photos p ON dg.photo_id = p.id
            WHERE dg.group_id = ?
            ORDER BY (p.width * p.height) DESC
            LIMIT 1
        """, (g2,))
        rep2 = dict(cursor.fetchone())

        # Get group sizes
        cursor = conn.execute("SELECT COUNT(*) as size FROM duplicate_groups WHERE group_id = ?", (g1,))
        size1 = cursor.fetchone()['size']
        cursor = conn.execute("SELECT COUNT(*) as size FROM duplicate_groups WHERE group_id = ?", (g2,))
        size2 = cursor.fetchone()['size']

        # Get blockers between the groups
        blockers = get_blockers_between_groups(conn, g1, g2, limit=6)

        result.append({
            'group1': g1,
            'group2': g2,
            'size1': size1,
            'size2': size2,
            'bridge_count': bridge_count,
            'blocker_count': len(get_blockers_between_groups(conn, g1, g2, limit=9999)),
            'min_phash': data['min_phash'],
            'max_phash': data['max_phash'],
            'avg_phash': round(avg_phash, 1),
            'rep1': rep1,
            'rep2': rep2,
            'blockers': blockers,
        })

    conn.close()

    # Sort by bridge count (descending), then by min phash (ascending)
    result.sort(key=lambda x: (-x['bridge_count'], x['min_phash']))

    return result


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Bridge Viewer - Group Pairs</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; }
        body { font-family: system-ui, sans-serif; padding: 10px; background: #1a1a1a; color: #eee; display: flex; flex-direction: column; overflow: hidden; }
        a { color: #6cf; text-decoration: none; }
        a:hover { text-decoration: underline; }

        .header { display: flex; align-items: center; gap: 20px; margin-bottom: 10px; flex-wrap: wrap; }
        .nav { display: flex; align-items: center; gap: 10px; }
        .nav a { padding: 6px 12px; background: #333; border-radius: 5px; }
        .nav a:hover { background: #444; text-decoration: none; }
        .stats { color: #888; font-size: 0.9em; }
        .help { color: #666; font-size: 0.8em; }

        .jump-form { display: flex; gap: 5px; align-items: center; }
        .jump-form input { width: 80px; padding: 4px 8px; background: #333; border: 1px solid #444; color: #eee; border-radius: 4px; }
        .jump-form button { padding: 4px 12px; background: #333; border: none; color: #eee; border-radius: 4px; cursor: pointer; }
        .jump-form button:hover { background: #444; }

        .pair-info { margin-bottom: 10px; padding: 8px 12px; background: #222; border-radius: 5px; display: flex; gap: 20px; flex-wrap: wrap; align-items: center; }
        .pair-info .item { }
        .pair-info .label { color: #888; font-size: 0.85em; }
        .pair-info .value { font-size: 1.1em; }
        .pair-info .value.highlight { color: #4c4; font-weight: bold; }

        .main-content { flex: 1; display: flex; gap: 15px; overflow: hidden; min-height: 0; }

        .groups-panel { display: flex; gap: 15px; flex-shrink: 0; }
        .group-card { background: #222; border-radius: 8px; overflow: hidden; width: 280px; }
        .group-card .img-wrapper { height: 200px; display: flex; align-items: center; justify-content: center; background: #111; }
        .group-card img { max-width: 100%; max-height: 100%; object-fit: contain; }
        .group-card .info { padding: 10px; font-size: 0.9em; }
        .group-card .info .group-id { font-size: 1.1em; margin-bottom: 4px; }
        .group-card .info .size { color: #888; }

        .arrow { display: flex; align-items: center; font-size: 2em; color: #4c4; padding: 0 10px; }

        .bridges-panel { flex: 1; background: #222; border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; min-width: 0; }
        .bridges-header { padding: 10px 12px; background: #333; font-size: 0.9em; flex-shrink: 0; }
        .bridges-list { flex: 1; overflow-y: auto; }
        .bridge { display: flex; gap: 10px; padding: 8px 12px; border-bottom: 1px solid #333; align-items: center; }
        .bridge:last-child { border-bottom: none; }
        .bridge img { width: 70px; height: 52px; object-fit: contain; background: #111; border-radius: 4px; }
        .bridge .arrow-small { color: #4c4; font-size: 1em; }
        .bridge .arrow-small.blocker { color: #c44; font-weight: bold; }
        .bridge .distances { font-size: 0.85em; color: #888; margin-left: auto; }
        .bridge .distances span { margin-left: 10px; }
        .bridge .distances .phash { color: #6cf; }
        .bridge .distances .dhash { color: #c9c; }
    </style>
</head>
<body>
    <div class="header">
        <div class="nav">
            {% if prev_idx is not none %}
            <a id="prev" href="/pair/{{ prev_idx }}">&larr; Prev</a>
            {% endif %}
            <span>Pair {{ idx + 1 }} / {{ total }}</span>
            {% if next_idx is not none %}
            <a id="next" href="/pair/{{ next_idx }}">Next &rarr;</a>
            {% endif %}
        </div>
        <form class="jump-form" action="/jump" method="get">
            <input type="number" name="idx" placeholder="Jump to" min="1" max="{{ total }}">
            <button type="submit">Go</button>
        </form>
        <div class="stats">Group pairs sorted by bridge count (most bridges first)</div>
        <div class="help">Keys: &larr;/&rarr; navigate</div>
    </div>

    <div class="pair-info">
        <div class="item">
            <div class="label">Bridges</div>
            <div class="value highlight">{{ pair.bridge_count }}</div>
        </div>
        <div class="item">
            <div class="label">Blockers</div>
            <div class="value" style="color: #c44;">{{ pair.blocker_count }}</div>
        </div>
        <div class="item">
            <div class="label">Bridge pHash</div>
            <div class="value">{{ pair.min_phash }} - {{ pair.max_phash }}</div>
        </div>
        <div class="item">
            <div class="label">Combined Size</div>
            <div class="value">{{ pair.size1 + pair.size2 }} photos</div>
        </div>
        <div class="item">
            <div class="label">Group Links</div>
            <div class="value">
                <a href="http://localhost:5001/group/{{ pair.group1 }}" target="_blank">{{ pair.group1 }}</a>
                &nbsp;|&nbsp;
                <a href="http://localhost:5001/group/{{ pair.group2 }}" target="_blank">{{ pair.group2 }}</a>
            </div>
        </div>
    </div>

    <div class="main-content">
        <div class="groups-panel">
            <div class="group-card">
                <div class="img-wrapper">
                    <img src="/image/{{ pair.rep1.id }}">
                </div>
                <div class="info">
                    <div class="group-id">Group <a href="http://localhost:5001/group/{{ pair.group1 }}" target="_blank">{{ pair.group1 }}</a></div>
                    <div class="size">{{ pair.size1 }} photos</div>
                </div>
            </div>

            <div class="arrow">↔</div>

            <div class="group-card">
                <div class="img-wrapper">
                    <img src="/image/{{ pair.rep2.id }}">
                </div>
                <div class="info">
                    <div class="group-id">Group <a href="http://localhost:5001/group/{{ pair.group2 }}" target="_blank">{{ pair.group2 }}</a></div>
                    <div class="size">{{ pair.size2 }} photos</div>
                </div>
            </div>
        </div>

        <div class="bridges-panel">
            <div class="bridges-header">Worst {{ pair.blockers|length }} of {{ pair.blocker_count }} blocker(s) preventing merge</div>
            <div class="bridges-list">
                {% for b in pair.blockers %}
                <div class="bridge">
                    <img src="/image/{{ b.photo1 }}" title="{{ b.photo1[:20] }}...">
                    <span class="arrow-small blocker">✕</span>
                    <img src="/image/{{ b.photo2 }}" title="{{ b.photo2[:20] }}...">
                    <div class="distances">
                        <span class="phash">pHash: {{ b.phash_dist }}</span>
                        <span class="dhash">dHash: {{ b.dhash_dist }}</span>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>

    <script>
        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT') return;
            if (e.key === 'ArrowLeft') {
                const prev = document.getElementById('prev');
                if (prev) prev.click();
            } else if (e.key === 'ArrowRight') {
                const next = document.getElementById('next');
                if (next) next.click();
            }
        });
    </script>
</body>
</html>
"""

_pairs = None


def get_pairs():
    global _pairs
    if _pairs is None:
        _pairs = get_group_pairs_by_bridges()
    return _pairs


@app.route('/')
def index():
    return app.redirect('/pair/0')


@app.route('/jump')
def jump():
    """Jump to a specific pair by index."""
    idx = int(request.args.get('idx', 1)) - 1
    pairs = get_pairs()
    if 0 <= idx < len(pairs):
        return app.redirect(f'/pair/{idx}')
    return app.redirect('/')


@app.route('/pair/<int:idx>')
def show_pair(idx):
    pairs = get_pairs()
    if not pairs:
        return "No group pairs with bridges found"

    idx = max(0, min(idx, len(pairs) - 1))
    pair = pairs[idx]

    prev_idx = idx - 1 if idx > 0 else None
    next_idx = idx + 1 if idx < len(pairs) - 1 else None

    return render_template_string(
        TEMPLATE,
        pair=pair,
        idx=idx,
        total=len(pairs),
        prev_idx=prev_idx,
        next_idx=next_idx,
    )


@app.route('/image/<photo_id>')
def serve_image(photo_id):
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


@app.route('/reload')
def reload_data():
    """Reload data from database."""
    global _pairs
    _pairs = None
    return app.redirect('/')


if __name__ == '__main__':
    print(f"Database: {DB_PATH}")
    print("Loading group pairs with bridges...")
    pairs = get_pairs()
    print(f"Found {len(pairs):,} group pairs with bridges")

    if pairs:
        # Show distribution
        by_count = defaultdict(int)
        for p in pairs:
            by_count[p['bridge_count']] += 1
        print("\nBridge count distribution:")
        for count in sorted(by_count.keys(), reverse=True)[:10]:
            print(f"  {count} bridges: {by_count[count]} group pairs")

    print()
    print("Starting server at http://localhost:5003")
    print("Group pairs sorted by bridge count (most bridges first)")
    app.run(debug=True, port=5003)
