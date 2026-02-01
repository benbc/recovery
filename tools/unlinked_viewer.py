#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Unlinked Pairs Viewer - Review pairs that satisfy should_group() but were separated

Shows pairs that could have been grouped but ended up in different groups,
along with the "blocking" photos that prevented the merge and all relevant
hamming distances.
"""

import sqlite3
from pathlib import Path

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
    PHASH_SAFE_GROUP = 10
    PHASH_BORDERLINE_12 = 12
    PHASH_BORDERLINE_14 = 14
    DHASH_EXCLUDE_AT_12 = 22
    DHASH_INCLUDE_AT_14 = 17

    if phash_dist <= PHASH_SAFE_GROUP:
        return True
    elif phash_dist <= PHASH_BORDERLINE_12:
        return dhash_dist < DHASH_EXCLUDE_AT_12
    elif phash_dist <= PHASH_BORDERLINE_14:
        return dhash_dist <= DHASH_INCLUDE_AT_14
    else:
        return False


def get_unlinked_pairs():
    """Get all unlinked pairs ordered by distance."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            up.photo_id_1, up.photo_id_2,
            up.phash_dist, up.dhash_dist, up.reason,
            p1.mime_type as mime1, p1.width as w1, p1.height as h1,
            p1.perceptual_hash as phash1, p1.dhash as dhash1,
            p2.mime_type as mime2, p2.width as w2, p2.height as h2,
            p2.perceptual_hash as phash2, p2.dhash as dhash2,
            dg1.group_id as group1,
            dg2.group_id as group2
        FROM unlinked_pairs up
        JOIN photos p1 ON up.photo_id_1 = p1.id
        JOIN photos p2 ON up.photo_id_2 = p2.id
        LEFT JOIN duplicate_groups dg1 ON up.photo_id_1 = dg1.photo_id
        LEFT JOIN duplicate_groups dg2 ON up.photo_id_2 = dg2.photo_id
        ORDER BY up.phash_dist, up.dhash_dist
    """)
    pairs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return pairs


def get_group_photos(group_id: int) -> list[dict]:
    """Get all photos in a group with their hashes."""
    if group_id is None:
        return []
    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            p.id, p.mime_type, p.width, p.height,
            p.perceptual_hash, p.dhash
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        WHERE dg.group_id = ?
        ORDER BY (p.width * p.height) DESC
    """, (group_id,))
    photos = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return photos


def get_all_blocking_pairs(group1_id: int, group2_id: int) -> list[dict]:
    """
    Find ALL blocking pairs between two groups.

    Returns list of blocking pairs with photos from both groups and their distances.
    These are the pairs that prevented the groups from merging.
    """
    if group1_id is None or group2_id is None:
        return []

    group1_photos = get_group_photos(group1_id)
    group2_photos = get_group_photos(group2_id)
    blocking = []

    for p1 in group1_photos:
        if not p1['perceptual_hash'] or not p1['dhash']:
            continue
        for p2 in group2_photos:
            if not p2['perceptual_hash'] or not p2['dhash']:
                continue

            phash_dist = hamming_distance(p1['perceptual_hash'], p2['perceptual_hash'])
            dhash_dist = hamming_distance(p1['dhash'], p2['dhash'])

            if not should_group(phash_dist, dhash_dist):
                blocking.append({
                    'photo1_id': p1['id'],
                    'photo1_mime': p1['mime_type'],
                    'photo1_width': p1['width'],
                    'photo1_height': p1['height'],
                    'photo2_id': p2['id'],
                    'photo2_mime': p2['mime_type'],
                    'photo2_width': p2['width'],
                    'photo2_height': p2['height'],
                    'phash_dist': phash_dist,
                    'dhash_dist': dhash_dist,
                })

    # Sort by phash distance (show worst blockers first)
    blocking.sort(key=lambda x: (x['phash_dist'], x['dhash_dist']), reverse=True)
    return blocking


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Unlinked Pairs Viewer</title>
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

        .pair-info { margin-bottom: 10px; padding: 8px 12px; background: #222; border-radius: 5px; display: flex; gap: 20px; flex-wrap: wrap; }
        .pair-info .item { }
        .pair-info .label { color: #888; font-size: 0.85em; }
        .pair-info .value { font-size: 1.1em; }
        .pair-info .value.good { color: #4c4; }
        .pair-info .value.warn { color: #cc4; }
        .pair-info .value.bad { color: #c44; }

        .main-content { flex: 1; display: flex; gap: 10px; overflow: hidden; min-height: 0; }

        .side { flex: 1; display: flex; flex-direction: column; gap: 10px; overflow: hidden; min-width: 0; }
        .side h3 { margin: 0; padding: 8px 12px; background: #333; border-radius: 5px 5px 0 0; font-size: 0.9em; }

        .main-photo { background: #222; border-radius: 8px; overflow: hidden; flex-shrink: 0; }
        .main-photo .img-wrapper { height: 250px; display: flex; align-items: center; justify-content: center; background: #111; }
        .main-photo img { max-width: 100%; max-height: 100%; object-fit: contain; }
        .main-photo .photo-info { padding: 8px; font-size: 0.85em; }
        .main-photo .id { font-family: monospace; color: #6cf; margin-bottom: 4px; }
        .main-photo .dims { color: #888; }
        .main-photo .group-link { margin-top: 4px; }

        .blockers { flex: 1; background: #222; border-radius: 0 0 5px 5px; overflow-y: auto; min-height: 0; }
        .blockers-header { padding: 8px 12px; background: #2a2a2a; font-size: 0.85em; color: #888; border-bottom: 1px solid #333; position: sticky; top: 0; }
        .no-blockers { padding: 20px; text-align: center; color: #666; }

        .blocking-pair { display: flex; gap: 10px; padding: 8px 12px; border-bottom: 1px solid #333; align-items: center; }
        .blocking-pair:last-child { border-bottom: none; }
        .bp-photos { display: flex; align-items: center; gap: 8px; }
        .bp-photos img { width: 100px; height: 75px; object-fit: contain; background: #111; border-radius: 4px; }
        .bp-arrow { color: #c44; font-size: 1.2em; font-weight: bold; }
        .bp-info { flex: 1; font-size: 0.85em; }
        .bp-info .distances { color: #c44; font-weight: bold; }
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
        <div class="stats">Closest unlinked pairs first (these are potential false negatives)</div>
        <div class="help">Keys: &larr;/&rarr; navigate</div>
    </div>

    <div class="pair-info">
        <div class="item">
            <div class="label">pHash Distance</div>
            <div class="value {% if pair.phash_dist <= 10 %}good{% elif pair.phash_dist <= 14 %}warn{% else %}bad{% endif %}">{{ pair.phash_dist }}</div>
        </div>
        <div class="item">
            <div class="label">dHash Distance</div>
            <div class="value {% if pair.dhash_dist <= 17 %}good{% elif pair.dhash_dist <= 22 %}warn{% else %}bad{% endif %}">{{ pair.dhash_dist }}</div>
        </div>
        <div class="item">
            <div class="label">Reason</div>
            <div class="value">{{ pair.reason }}</div>
        </div>
        <div class="item">
            <div class="label">Photo 1 Group</div>
            <div class="value">{% if pair.group1 is not none %}<a href="http://localhost:5001/group/{{ pair.group1 }}">{{ pair.group1 }}</a>{% else %}(none){% endif %}</div>
        </div>
        <div class="item">
            <div class="label">Photo 2 Group</div>
            <div class="value">{% if pair.group2 is not none %}<a href="http://localhost:5001/group/{{ pair.group2 }}">{{ pair.group2 }}</a>{% else %}(none){% endif %}</div>
        </div>
    </div>

    <div class="main-content">
        <!-- Left side: The two unlinked photos -->
        <div class="side">
            <div class="main-photo">
                <div class="img-wrapper">
                    <img src="/image/{{ pair.photo_id_1 }}">
                </div>
                <div class="photo-info">
                    <div class="id">{{ pair.photo_id_1[:24] }}...</div>
                    <div class="dims">{{ pair.w1 }}x{{ pair.h1 }}</div>
                    {% if pair.group1 is not none %}
                    <div class="group-link">In group <a href="http://localhost:5001/group/{{ pair.group1 }}">{{ pair.group1 }}</a></div>
                    {% else %}
                    <div class="group-link" style="color: #666;">(singleton - no group)</div>
                    {% endif %}
                </div>
            </div>

            <div class="main-photo">
                <div class="img-wrapper">
                    <img src="/image/{{ pair.photo_id_2 }}">
                </div>
                <div class="photo-info">
                    <div class="id">{{ pair.photo_id_2[:24] }}...</div>
                    <div class="dims">{{ pair.w2 }}x{{ pair.h2 }}</div>
                    {% if pair.group2 is not none %}
                    <div class="group-link">In group <a href="http://localhost:5001/group/{{ pair.group2 }}">{{ pair.group2 }}</a></div>
                    {% else %}
                    <div class="group-link" style="color: #666;">(singleton - no group)</div>
                    {% endif %}
                </div>
            </div>
        </div>

        <!-- Right side: All blocking pairs between the two groups -->
        <div class="side">
            <h3>Blocking pairs between Group {{ pair.group1 if pair.group1 is not none else '?' }} and Group {{ pair.group2 if pair.group2 is not none else '?' }}</h3>
            <div class="blockers">
                {% if blocking_pairs %}
                <div class="blockers-header">{{ blocking_pairs|length }} pair(s) prevent group merge</div>
                {% for bp in blocking_pairs %}
                <div class="blocking-pair">
                    <div class="bp-photos">
                        <img src="/image/{{ bp.photo1_id }}" title="{{ bp.photo1_id[:20] }}...">
                        <span class="bp-arrow">↔</span>
                        <img src="/image/{{ bp.photo2_id }}" title="{{ bp.photo2_id[:20] }}...">
                    </div>
                    <div class="bp-info">
                        <div class="distances">pHash: {{ bp.phash_dist }}, dHash: {{ bp.dhash_dist }}</div>
                    </div>
                </div>
                {% endfor %}
                {% else %}
                <div class="no-blockers">
                    {% if pair.group1 is none or pair.group2 is none %}
                    One or both photos have no group
                    {% else %}
                    No blocking pairs found (BUG!)
                    {% endif %}
                </div>
                {% endif %}
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
        _pairs = get_unlinked_pairs()
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
        return "No unlinked pairs found"

    idx = max(0, min(idx, len(pairs) - 1))
    pair = pairs[idx]

    prev_idx = idx - 1 if idx > 0 else None
    next_idx = idx + 1 if idx < len(pairs) - 1 else None

    # Find all blocking pairs between the two groups
    blocking_pairs = get_all_blocking_pairs(pair['group1'], pair['group2'])

    return render_template_string(
        TEMPLATE,
        pair=pair,
        idx=idx,
        total=len(pairs),
        prev_idx=prev_idx,
        next_idx=next_idx,
        blocking_pairs=blocking_pairs,
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
    """Reload pairs from database."""
    global _pairs
    _pairs = None
    return app.redirect('/')


if __name__ == '__main__':
    pairs = get_pairs()
    print(f"Database: {DB_PATH}")
    print(f"Unlinked pairs: {len(pairs):,}")
    if pairs:
        # Count by reason
        reasons = {}
        for p in pairs:
            reasons[p['reason']] = reasons.get(p['reason'], 0) + 1
        for reason, count in sorted(reasons.items()):
            print(f"  {reason}: {count:,}")
    print()
    print("Starting server at http://localhost:5002")
    print("Pairs are sorted by distance (closest first)")
    print("These are potential false negatives - photos that might belong together")
    app.run(debug=True, port=5002)
