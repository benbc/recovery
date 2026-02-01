#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Rule Viewer - Browse photos by classification rule

A simple web UI to audit Stage 2 individual classification results.
Shows sample photos for each rule so you can verify they're working correctly.
"""

import sqlite3
from pathlib import Path

from flask import Flask, send_file, render_template_string

# Paths
DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"
FILES_DIR = Path(__file__).parent.parent / "output" / "files"

app = Flask(__name__)

# Map MIME types to extensions (for finding linked files)
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


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Rule Viewer</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; overflow: hidden; }
        body { font-family: system-ui, sans-serif; padding: 10px; background: #1a1a1a; color: #eee; display: flex; flex-direction: column; }
        a { color: #6cf; }
        .rules { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 6px; }
        .rule { padding: 5px 10px; background: #333; border-radius: 5px; text-decoration: none; color: #eee; font-size: 0.85em; }
        .rule:hover { background: #444; }
        .rule.active { background: #264; }
        .rule .count { color: #888; font-size: 0.9em; }
        .photos { flex: 1; display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 8px; overflow-y: auto; align-content: start; }
        .photo { background: #222; border-radius: 8px; overflow: hidden; position: relative; }
        .photo img { width: 100%; height: 200px; object-fit: contain; background: #111; }
        .photo .photo-id { font-family: monospace; color: #888; cursor: pointer; padding: 4px 8px; font-size: 0.75em; }
        .photo .photo-id:hover { color: #6cf; }
        .photo .hover-info { display: none; position: absolute; top: 0; left: 0; right: 0; background: rgba(0,0,0,0.85); padding: 8px; font-size: 0.75em; max-height: 200px; overflow-y: auto; }
        .photo:hover .hover-info { display: block; }
        .photo .hover-info .dims { color: #fff; margin-bottom: 5px; }
        .photo .hover-info .paths { color: #aaa; }
        .photo .hover-info .path { margin: 2px 0; word-break: break-all; }
        .nav { flex-shrink: 0; padding: 6px 0; display: flex; align-items: center; gap: 10px; font-size: 0.9em; }
        .nav a { padding: 3px 8px; background: #333; border-radius: 4px; text-decoration: none; }
        .nav a:hover { background: #444; }
        .decision-reject { border-left: 4px solid #c44; }
        .decision-separate { border-left: 4px solid #4c4; }
    </style>
</head>
<body>
    <div class="rules">
        {% for rule in rules %}
        <a href="/rule/{{ rule.decision }}/{{ rule.rule_name }}"
           class="rule {% if active_rule == rule.rule_name %}active{% endif %}">
            {{ rule.rule_name }}
            <span class="count">({{ rule.count }})</span>
        </a>
        {% endfor %}
    </div>

    {% if photos %}
    <div class="nav">
        {% if offset > 0 %}
        <a id="prev" href="/rule/{{ active_decision }}/{{ active_rule }}?offset={{ offset - limit }}">&larr;</a>
        {% endif %}
        <span>{{ offset + 1 }}-{{ offset + photos|length }} / {{ total }}</span>
        {% if offset + limit < total %}
        <a id="next" href="/rule/{{ active_decision }}/{{ active_rule }}?offset={{ offset + limit }}">&rarr;</a>
        {% endif %}
    </div>

    <div class="photos">
        {% for photo in photos %}
        <div class="photo decision-{{ active_decision }}">
            <img src="/image/{{ photo.id }}" alt="{{ photo.id }}">
            <div class="hover-info">
                <div class="dims"><strong>{{ photo.width }}x{{ photo.height }}</strong> &middot; {{ photo.file_size | filesizeformat }}</div>
                <div class="paths">
                    {% for path in photo.paths %}
                    <div class="path">{{ path }}</div>
                    {% endfor %}
                </div>
            </div>
            <div class="photo-id" onclick="navigator.clipboard.writeText('{{ photo.id }}')" title="Click to copy">{{ photo.id[:16] }}...</div>
        </div>
        {% endfor %}
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
    {% endif %}
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


@app.route('/')
def index():
    conn = get_connection()
    cursor = conn.execute("""
        SELECT decision, rule_name, COUNT(*) as count
        FROM individual_decisions
        GROUP BY decision, rule_name
        ORDER BY count DESC
    """)
    rules = list(cursor.fetchall())
    conn.close()

    return render_template_string(TEMPLATE, rules=rules, photos=None, active_rule=None)


@app.route('/rule/<decision>/<rule_name>')
def show_rule(decision, rule_name):
    from flask import request

    limit = 15
    offset = int(request.args.get('offset', 0))

    conn = get_connection()

    # Get all rules for nav
    cursor = conn.execute("""
        SELECT decision, rule_name, COUNT(*) as count
        FROM individual_decisions
        GROUP BY decision, rule_name
        ORDER BY count DESC
    """)
    rules = list(cursor.fetchall())

    # Get total count for this rule
    cursor = conn.execute("""
        SELECT COUNT(*) FROM individual_decisions
        WHERE decision = ? AND rule_name = ?
    """, (decision, rule_name))
    total = cursor.fetchone()[0]

    # Get photos for this rule
    cursor = conn.execute("""
        SELECT p.id, p.mime_type, p.width, p.height, p.file_size,
               GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM photos p
        JOIN individual_decisions d ON p.id = d.photo_id
        JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE d.decision = ? AND d.rule_name = ?
        GROUP BY p.id
        ORDER BY p.id
        LIMIT ? OFFSET ?
    """, (decision, rule_name, limit, offset))

    photos = []
    for row in cursor.fetchall():
        photo = dict(row)
        photo['paths'] = photo['all_paths'].split('|') if photo['all_paths'] else []
        photos.append(photo)

    conn.close()

    return render_template_string(
        TEMPLATE,
        rules=rules,
        photos=photos,
        active_rule=rule_name,
        active_decision=decision,
        total=total,
        offset=offset,
        limit=limit
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
    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print()
    print("Starting server at http://localhost:5000")
    app.run(debug=True, port=5000)
