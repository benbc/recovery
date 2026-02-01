#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Unlinked Pairs Viewer - Review pairs that satisfy should_group() but were separated

Shows pairs that could have been grouped but ended up in different groups
(or as singletons), ordered by distance (closest pairs first) for validation.
"""

import sqlite3
from pathlib import Path

from flask import Flask, render_template_string, send_file

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


def get_unlinked_pairs():
    """Get all unlinked pairs ordered by distance."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            up.photo_id_1, up.photo_id_2,
            up.phash_dist, up.dhash_dist, up.reason,
            p1.mime_type as mime1, p1.width as w1, p1.height as h1,
            p2.mime_type as mime2, p2.width as w2, p2.height as h2
        FROM unlinked_pairs up
        JOIN photos p1 ON up.photo_id_1 = p1.id
        JOIN photos p2 ON up.photo_id_2 = p2.id
        ORDER BY up.phash_dist, up.dhash_dist
    """)
    pairs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return pairs


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Unlinked Pairs Viewer</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; overflow: hidden; }
        body { font-family: system-ui, sans-serif; padding: 10px; background: #1a1a1a; color: #eee; display: flex; flex-direction: column; }

        .header { display: flex; align-items: center; gap: 20px; margin-bottom: 10px; flex-wrap: wrap; }
        .nav { display: flex; align-items: center; gap: 10px; }
        .nav a { padding: 6px 12px; background: #333; border-radius: 5px; color: #6cf; text-decoration: none; }
        .nav a:hover { background: #444; }
        .stats { color: #888; font-size: 0.9em; }
        .help { color: #666; font-size: 0.8em; }

        .pair-info { margin-bottom: 10px; padding: 8px 12px; background: #222; border-radius: 5px; display: flex; gap: 20px; flex-wrap: wrap; }
        .pair-info .item { }
        .pair-info .label { color: #888; font-size: 0.85em; }
        .pair-info .value { font-size: 1.1em; }
        .pair-info .value.good { color: #4c4; }
        .pair-info .value.warn { color: #cc4; }
        .pair-info .value.bad { color: #c44; }

        .photos-container { flex: 1; display: flex; gap: 10px; overflow: hidden; }
        .photo { flex: 1; background: #222; border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; }
        .photo .img-wrapper { flex: 1; display: flex; align-items: center; justify-content: center; background: #111; overflow: hidden; }
        .photo img { max-width: 100%; max-height: 100%; object-fit: contain; }
        .photo-info { padding: 8px; font-size: 0.85em; height: 60px; }
        .photo-info .id { font-family: monospace; color: #6cf; margin-bottom: 4px; }
        .photo-info .dims { color: #888; }
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
        <div class="stats">Showing closest unlinked pairs first</div>
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
    </div>

    <div class="photos-container">
        <div class="photo">
            <div class="img-wrapper">
                <img src="/image/{{ pair.photo_id_1 }}">
            </div>
            <div class="photo-info">
                <div class="id">{{ pair.photo_id_1[:20] }}...</div>
                <div class="dims">{{ pair.w1 }}x{{ pair.h1 }}</div>
            </div>
        </div>
        <div class="photo">
            <div class="img-wrapper">
                <img src="/image/{{ pair.photo_id_2 }}">
            </div>
            <div class="photo-info">
                <div class="id">{{ pair.photo_id_2[:20] }}...</div>
                <div class="dims">{{ pair.w2 }}x{{ pair.h2 }}</div>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener('keydown', (e) => {
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


@app.route('/pair/<int:idx>')
def show_pair(idx):
    pairs = get_pairs()
    if not pairs:
        return "No unlinked pairs found"

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


if __name__ == '__main__':
    pairs = get_pairs()
    print(f"Database: {DB_PATH}")
    print(f"Unlinked pairs: {len(pairs):,}")
    print()
    print("Starting server at http://localhost:5002")
    print("Pairs are sorted by distance (closest first)")
    app.run(debug=True, port=5002)
