#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Group Viewer - Browse duplicate groups

Shows all photos in each duplicate group for manual review.
Navigate between groups to understand what's being grouped together.
"""

import sqlite3
from pathlib import Path

from flask import Flask, render_template_string, request, send_file

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


def get_group_stats():
    """Get statistics about groups."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            COUNT(DISTINCT group_id) as num_groups,
            COUNT(*) as total_photos,
            MIN(group_id) as min_group,
            MAX(group_id) as max_group
        FROM duplicate_groups
    """)
    stats = dict(cursor.fetchone())

    # Get size distribution
    cursor = conn.execute("""
        SELECT COUNT(*) as size, COUNT(*) as num_groups
        FROM duplicate_groups
        GROUP BY group_id
        ORDER BY size DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    stats['max_size'] = row['size'] if row else 0

    conn.close()
    return stats


def get_group_ids_by_size():
    """Get all group IDs ordered by size (largest first)."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT group_id, COUNT(*) as size
        FROM duplicate_groups
        GROUP BY group_id
        ORDER BY size DESC, group_id
    """)
    groups = [(row['group_id'], row['size']) for row in cursor.fetchall()]
    conn.close()
    return groups


def get_group_photos(group_id: int):
    """Get all photos in a group with their metadata."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            p.id, p.mime_type, p.width, p.height, p.file_size,
            p.perceptual_hash, p.dhash, p.has_exif,
            GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE dg.group_id = ?
        GROUP BY p.id
        ORDER BY (p.width * p.height) DESC, p.file_size DESC
    """, (group_id,))

    photos = []
    for row in cursor.fetchall():
        photo = dict(row)
        photo['paths'] = photo['all_paths'].split('|') if photo['all_paths'] else []
        photo['resolution'] = f"{photo['width']}x{photo['height']}"
        photo['megapixels'] = round((photo['width'] or 0) * (photo['height'] or 0) / 1_000_000, 1)
        photos.append(photo)

    conn.close()
    return photos


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Group Viewer - Group {{ group_id }}</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; overflow: hidden; }
        body { font-family: system-ui, sans-serif; padding: 10px; background: #1a1a1a; color: #eee; display: flex; flex-direction: column; }
        a { color: #6cf; text-decoration: none; }
        a:hover { text-decoration: underline; }

        .header { display: flex; align-items: center; gap: 20px; margin-bottom: 10px; flex-wrap: wrap; }
        .nav { display: flex; align-items: center; gap: 10px; }
        .nav a { padding: 6px 12px; background: #333; border-radius: 5px; }
        .nav a:hover { background: #444; text-decoration: none; }
        .stats { color: #888; font-size: 0.9em; }
        .help { color: #666; font-size: 0.8em; }

        .group-info { margin-bottom: 10px; padding: 8px 12px; background: #222; border-radius: 5px; display: flex; gap: 20px; flex-wrap: wrap; }
        .group-info .item { }
        .group-info .label { color: #888; font-size: 0.85em; }
        .group-info .value { font-size: 1.1em; }

        .photos-container { flex: 1; overflow-y: auto; }
        .photos { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; align-content: start; }
        .photo { background: #222; border-radius: 8px; overflow: hidden; }
        .photo img { width: 100%; height: 200px; object-fit: contain; background: #111; cursor: pointer; }
        .photo.expanded img { height: auto; max-height: 80vh; }
        .photo-info { padding: 8px; font-size: 0.8em; }
        .photo-info .id { font-family: monospace; color: #6cf; cursor: pointer; margin-bottom: 4px; }
        .photo-info .id:hover { text-decoration: underline; }
        .photo-info .dims { color: #fff; margin-bottom: 2px; }
        .photo-info .meta { color: #888; margin-bottom: 4px; }
        .photo-info .paths { color: #666; font-size: 0.9em; max-height: 60px; overflow-y: auto; }
        .photo-info .path { margin: 2px 0; word-break: break-all; }

        .photo.best { border: 2px solid #4c4; }
        .photo.thumbnail { border: 2px solid #c44; opacity: 0.7; }

        .jump-form { display: flex; gap: 5px; align-items: center; }
        .jump-form input { width: 80px; padding: 4px 8px; background: #333; border: 1px solid #444; color: #eee; border-radius: 4px; }
        .jump-form button { padding: 4px 12px; background: #333; border: none; color: #eee; border-radius: 4px; cursor: pointer; }
        .jump-form button:hover { background: #444; }
    </style>
</head>
<body>
    <div class="header">
        <div class="nav">
            {% if prev_group is not none %}
            <a id="prev" href="/group/{{ prev_group }}">&larr; Prev</a>
            {% endif %}
            <span>Group {{ group_idx + 1 }} / {{ total_groups }}</span>
            {% if next_group is not none %}
            <a id="next" href="/group/{{ next_group }}">Next &rarr;</a>
            {% endif %}
        </div>
        <form class="jump-form" action="/jump" method="get">
            <input type="number" name="idx" placeholder="Jump to" min="1" max="{{ total_groups }}">
            <button type="submit">Go</button>
        </form>
        <div class="stats">{{ photos|length }} photos in group</div>
        <div class="help">Keys: &larr;/&rarr; navigate, click image to expand</div>
    </div>

    <div class="group-info">
        <div class="item">
            <div class="label">Group ID</div>
            <div class="value">{{ group_id }}</div>
        </div>
        <div class="item">
            <div class="label">Photos</div>
            <div class="value">{{ photos|length }}</div>
        </div>
        <div class="item">
            <div class="label">Resolutions</div>
            <div class="value">{{ photos|map(attribute='resolution')|unique|join(', ') }}</div>
        </div>
    </div>

    <div class="photos-container">
        <div class="photos">
            {% for photo in photos %}
            <div class="photo {% if loop.first %}best{% endif %}">
                <img src="/image/{{ photo.id }}" alt="{{ photo.id }}" onclick="this.parentElement.classList.toggle('expanded')">
                <div class="photo-info">
                    <div class="id" onclick="navigator.clipboard.writeText('{{ photo.id }}')" title="Click to copy">{{ photo.id[:16] }}...</div>
                    <div class="dims"><strong>{{ photo.resolution }}</strong> ({{ photo.megapixels }}MP) &middot; {{ photo.file_size | filesizeformat }}</div>
                    <div class="meta">
                        {% if photo.has_exif %}EXIF{% endif %}
                        {% if photo.perceptual_hash %}pHash: {{ photo.perceptual_hash[:8] }}...{% endif %}
                    </div>
                    <div class="paths">
                        {% for path in photo.paths[:3] %}
                        <div class="path">{{ path }}</div>
                        {% endfor %}
                        {% if photo.paths|length > 3 %}
                        <div class="path">... and {{ photo.paths|length - 3 }} more</div>
                        {% endif %}
                    </div>
                </div>
            </div>
            {% endfor %}
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


@app.template_filter('filesizeformat')
def filesizeformat(value):
    """Format file size in human-readable form."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


@app.template_filter('unique')
def unique_filter(values):
    """Return unique values preserving order."""
    seen = set()
    result = []
    for v in values:
        if v not in seen:
            seen.add(v)
            result.append(v)
    return result


# Cache group list (recomputed on first request)
_group_list = None

def get_group_list():
    global _group_list
    if _group_list is None:
        _group_list = get_group_ids_by_size()
    return _group_list


@app.route('/')
def index():
    """Redirect to first group."""
    groups = get_group_list()
    if groups:
        return app.redirect(f'/group/{groups[0][0]}')
    return "No groups found"


@app.route('/jump')
def jump():
    """Jump to a specific group by index."""
    idx = int(request.args.get('idx', 1)) - 1
    groups = get_group_list()
    if 0 <= idx < len(groups):
        return app.redirect(f'/group/{groups[idx][0]}')
    return app.redirect('/')


@app.route('/group/<int:group_id>')
def show_group(group_id):
    """Show all photos in a group."""
    groups = get_group_list()
    group_ids = [g[0] for g in groups]

    if group_id not in group_ids:
        return "Group not found", 404

    group_idx = group_ids.index(group_id)
    prev_group = group_ids[group_idx - 1] if group_idx > 0 else None
    next_group = group_ids[group_idx + 1] if group_idx < len(group_ids) - 1 else None

    photos = get_group_photos(group_id)

    return render_template_string(
        TEMPLATE,
        group_id=group_id,
        group_idx=group_idx,
        total_groups=len(groups),
        prev_group=prev_group,
        next_group=next_group,
        photos=photos,
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


@app.route('/reload')
def reload_groups():
    """Reload group list from database."""
    global _group_list
    _group_list = None
    return app.redirect('/')


if __name__ == '__main__':
    stats = get_group_stats()
    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Groups: {stats['num_groups']:,}")
    print(f"Photos in groups: {stats['total_photos']:,}")
    print()
    print("Starting server at http://localhost:5001")
    print("Groups are sorted by size (largest first)")
    app.run(debug=True, port=5001)
