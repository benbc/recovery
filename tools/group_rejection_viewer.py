#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Group Rejection Viewer - Browse Stage 5 group rejections by rule

Shows rejected photos alongside their group members, with clear indication
of which photo(s) caused the rejection (the "original" that was kept).
"""

import re
import sqlite3
from pathlib import Path

from flask import Flask, send_file, render_template_string, request

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


def _resolution(photo: dict) -> int:
    """Get resolution (width * height) for a photo."""
    return (photo.get("width") or 0) * (photo.get("height") or 0)


def _is_thumbnail_path(path: str) -> bool:
    """Check if path indicates a thumbnail."""
    path_lower = path.lower()
    return "/thumbnails/" in path_lower or "thumb_" in path_lower.split("/")[-1]


def _is_previews_path(path: str) -> bool:
    """Check if path is in a Previews folder."""
    return "/previews/" in path.lower()


def _is_iphoto_library(path: str) -> bool:
    """Check if path is from iPhoto Library."""
    return ".photolibrary/" in path.lower()


def _is_photos_library(path: str) -> bool:
    """Check if path is from Photos Library."""
    return ".photoslibrary/" in path.lower()


def _is_auto_generated_name(filename: str) -> bool:
    """Check if filename looks like a camera or software generated name."""
    stem = Path(filename).stem.upper()

    # Camera-generated patterns
    camera_patterns = [
        r"^IMG_\d+$",
        r"^IMG_E\d+$",
        r"^DSC_?\d+$",
        r"^DSCN?\d+$",
        r"^P\d{7}$",
        r"^\d{8}_\d+$",
        r"^\d{8}-\d+$",
        r"^PHOTO-\d{4}-\d{2}-\d{2}",
    ]

    # Software-generated patterns (thumbnails, previews, etc.)
    software_patterns = [
        r"^THUMB_",  # Thumbnail prefix
        r"_\d+$",  # Resolution suffix like _1024
    ]

    if any(re.match(p, stem) for p in camera_patterns):
        return True
    if any(re.search(p, stem) for p in software_patterns):
        return True
    return False


def _has_semantic_name(path: str) -> bool:
    """Check if photo has a human-assigned name."""
    filename = Path(path).name
    return not _is_auto_generated_name(filename)


def identify_originals(group: list[dict], rejected_ids: set, rule_name: str) -> set:
    """
    Identify which photos are the 'originals' that caused a rejection.
    Returns set of photo IDs that should be marked as originals.

    Only marks photos as "original" if they plausibly caused a specific rejection.
    """
    originals = set()

    # Get max resolution among rejected photos (for comparison)
    rejected_photos = [p for p in group if p["id"] in rejected_ids]
    max_rejected_res = max((_resolution(p) for p in rejected_photos), default=0)

    if rule_name == "THUMBNAIL":
        # Original must be: non-thumbnail AND larger than rejected thumbnails
        for photo in group:
            if photo["id"] in rejected_ids:
                continue
            paths = photo.get("all_paths", "").split("|")
            is_thumb = any(_is_thumbnail_path(p) for p in paths if p)
            photo_res = _resolution(photo)
            # Only mark as original if it's larger than rejected AND not a thumbnail
            if not is_thumb and photo_res > max_rejected_res:
                originals.add(photo["id"])

    elif rule_name == "PREVIEW":
        # Original must be: non-preview AND larger than rejected previews
        for photo in group:
            if photo["id"] in rejected_ids:
                continue
            paths = photo.get("all_paths", "").split("|")
            is_preview = any(_is_previews_path(p) for p in paths if p)
            photo_res = _resolution(photo)
            if not is_preview and photo_res > max_rejected_res:
                originals.add(photo["id"])

    elif rule_name == "IPHOTO_COPY":
        # Original must be: in Photos.app library AND same resolution as a rejected iPhoto copy
        rejected_resolutions = {_resolution(p) for p in rejected_photos}
        for photo in group:
            if photo["id"] in rejected_ids:
                continue
            paths = photo.get("all_paths", "").split("|")
            is_photos = any(_is_photos_library(p) for p in paths if p)
            photo_res = _resolution(photo)
            if is_photos and photo_res in rejected_resolutions:
                originals.add(photo["id"])

    elif rule_name == "DERIVATIVE":
        # Original must be: larger than rejected derivatives
        for photo in group:
            if photo["id"] in rejected_ids:
                continue
            photo_res = _resolution(photo)
            if photo_res > max_rejected_res:
                originals.add(photo["id"])

    elif rule_name == "HUMAN_SELECTED":
        # All non-rejected photos are the "kept" ones for this rule
        for photo in group:
            if photo["id"] not in rejected_ids:
                originals.add(photo["id"])

    return originals


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Group Rejection Viewer</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; overflow: hidden; }
        body { font-family: system-ui, sans-serif; padding: 10px; background: #1a1a1a; color: #eee; display: flex; flex-direction: column; }
        a { color: #6cf; }

        .rules { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 6px; flex-shrink: 0; }
        .rule { padding: 5px 10px; background: #333; border-radius: 5px; text-decoration: none; color: #eee; font-size: 0.85em; }
        .rule:hover { background: #444; }
        .rule.active { background: #264; }
        .rule .count { color: #888; font-size: 0.9em; }

        .legend { display: flex; gap: 15px; margin-bottom: 8px; font-size: 0.8em; flex-shrink: 0; }
        .legend-item { display: flex; align-items: center; gap: 5px; }
        .legend-box { width: 16px; height: 16px; border-radius: 3px; }
        .legend-rejected { background: #622; border: 2px solid #c44; }
        .legend-original { background: #252; border: 2px solid #4c4; }
        .legend-other { background: #333; border: 2px solid #555; }

        .nav { flex-shrink: 0; padding: 6px 0; display: flex; align-items: center; gap: 10px; font-size: 0.9em; }
        .nav a { padding: 3px 8px; background: #333; border-radius: 4px; text-decoration: none; }
        .nav a:hover { background: #444; }

        .groups { flex: 1; overflow-y: auto; }
        .group { background: #222; border-radius: 8px; margin-bottom: 12px; padding: 10px; }
        .group-header { font-size: 0.85em; color: #888; margin-bottom: 8px; }
        .group-photos { display: flex; flex-wrap: wrap; gap: 8px; }

        .photo { width: 220px; border-radius: 6px; overflow: hidden; position: relative; }
        .photo img { width: 100%; height: 160px; object-fit: contain; background: #111; }
        .photo-info { padding: 6px; font-size: 0.75em; }
        .photo-dims { color: #aaa; }
        .photo-id { font-family: monospace; color: #666; cursor: pointer; margin-top: 2px; }
        .photo-id:hover { color: #6cf; }

        .photo.rejected { background: #411; border: 2px solid #c44; }
        .photo.rejected .badge { background: #c44; }
        .photo.original { background: #241; border: 2px solid #4c4; }
        .photo.original .badge { background: #4c4; }
        .photo.other { background: #333; border: 2px solid #555; }

        .badge { position: absolute; top: 4px; left: 4px; padding: 2px 6px; border-radius: 3px; font-size: 0.7em; font-weight: bold; color: white; }

        .hover-info { display: none; position: absolute; top: 0; left: 0; right: 0; background: rgba(0,0,0,0.9); padding: 8px; font-size: 0.7em; max-height: 160px; overflow-y: auto; }
        .photo:hover .hover-info { display: block; }
        .hover-info .path { color: #aaa; margin: 2px 0; word-break: break-all; }
    </style>
</head>
<body>
    <div class="rules">
        <a href="/" class="rule {% if not active_rule %}active{% endif %}">All Rules</a>
        {% for rule in rules %}
        <a href="/rule/{{ rule.rule_name }}"
           class="rule {% if active_rule == rule.rule_name %}active{% endif %}">
            {{ rule.rule_name }}
            <span class="count">({{ rule.count }})</span>
        </a>
        {% endfor %}
    </div>

    {% if groups %}
    <div class="legend">
        <div class="legend-item"><div class="legend-box legend-rejected"></div> Rejected</div>
        <div class="legend-item"><div class="legend-box legend-original"></div> Kept (original)</div>
        <div class="legend-item"><div class="legend-box legend-other"></div> Remaining</div>
    </div>

    <div class="nav">
        {% if offset > 0 %}
        <a id="prev" href="{{ request.path }}?offset={{ offset - limit }}{% if random_order %}&random=1{% endif %}">&larr;</a>
        {% endif %}
        <span>Groups {{ offset + 1 }}-{{ offset + groups|length }} / {{ total_groups }}</span>
        {% if offset + limit < total_groups %}
        <a id="next" href="{{ request.path }}?offset={{ offset + limit }}{% if random_order %}&random=1{% endif %}">&rarr;</a>
        {% endif %}
        &nbsp;|&nbsp;
        {% if random_order %}
        <a href="{{ request.path }}">Sequential</a>
        {% else %}
        <a href="{{ request.path }}?random=1">Random</a>
        {% endif %}
    </div>

    <div class="groups">
        {% for group in groups %}
        <div class="group">
            <div class="group-header">
                Group #{{ group.group_id }} &middot; {{ group.photos|length }} photos
                {% if group.rejection_rules %}
                &middot; Rules: {{ group.rejection_rules|join(", ") }}
                {% endif %}
            </div>
            <div class="group-photos">
                {% for photo in group.photos %}
                <div class="photo {{ photo.status }}">
                    {% if photo.status == 'rejected' %}
                    <div class="badge">REJECTED</div>
                    {% elif photo.status == 'original' %}
                    <div class="badge">KEPT</div>
                    {% endif %}
                    <img src="/image/{{ photo.id }}" alt="{{ photo.id }}">
                    <div class="hover-info">
                        {% for path in photo.paths %}
                        <div class="path">{{ path }}</div>
                        {% endfor %}
                    </div>
                    <div class="photo-info">
                        <div class="photo-dims">{{ photo.width }}x{{ photo.height }} &middot; {{ photo.file_size | filesizeformat }}</div>
                        <div class="photo-id" onclick="navigator.clipboard.writeText('{{ photo.id }}')" title="Click to copy">{{ photo.id[:12] }}...</div>
                    </div>
                </div>
                {% endfor %}
            </div>
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
    {% else %}
    <p>No group rejections found. Run Stage 5 first.</p>
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


def get_rule_counts(conn):
    """Get count of rejections per rule, ordered by application order."""
    cursor = conn.execute("""
        SELECT rule_name, COUNT(*) as count
        FROM group_rejections
        GROUP BY rule_name
    """)
    counts = {row[0]: row[1] for row in cursor.fetchall()}
    # Return in application order as dicts for template compatibility
    return [{"rule_name": rule, "count": counts.get(rule, 0)} for rule in RULE_ORDER if rule in counts]


def get_groups_with_rejections(conn, rule_name=None, limit=10, offset=0, random_order=False):
    """Get groups that have rejections, optionally filtered by rule."""
    order = "RANDOM()" if random_order else "group_id"
    if rule_name:
        cursor = conn.execute(f"""
            SELECT DISTINCT group_id FROM group_rejections
            WHERE rule_name = ?
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """, (rule_name, limit, offset))
    else:
        cursor = conn.execute(f"""
            SELECT DISTINCT group_id FROM group_rejections
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """, (limit, offset))
    return [row[0] for row in cursor.fetchall()]


def get_total_groups(conn, rule_name=None):
    """Get total number of groups with rejections."""
    if rule_name:
        cursor = conn.execute("""
            SELECT COUNT(DISTINCT group_id) FROM group_rejections
            WHERE rule_name = ?
        """, (rule_name,))
    else:
        cursor = conn.execute("SELECT COUNT(DISTINCT group_id) FROM group_rejections")
    return cursor.fetchone()[0]


# Rule order - must match pipeline/rules/group.py
RULE_ORDER = ["THUMBNAIL", "PREVIEW", "IPHOTO_COPY", "DERIVATIVE", "SAME_RES_DUPLICATE"]


def get_earlier_rules(rule_name: str) -> list[str]:
    """Get rules that run before the given rule."""
    if rule_name not in RULE_ORDER:
        return []
    idx = RULE_ORDER.index(rule_name)
    return RULE_ORDER[:idx]


def get_group_data(conn, group_id, filter_rule=None):
    """Get all photos in a group with rejection info.

    When filter_rule is set, only shows photos that would have been visible
    to that rule (i.e., not already rejected by earlier rules).
    """
    # Get all photos in the group
    cursor = conn.execute("""
        SELECT
            p.*,
            dg.group_id,
            GROUP_CONCAT(pp.source_path, '|') as all_paths
        FROM duplicate_groups dg
        JOIN photos p ON dg.photo_id = p.id
        JOIN photo_paths pp ON p.id = pp.photo_id
        WHERE dg.group_id = ?
        GROUP BY p.id
        ORDER BY (p.width * p.height) DESC
    """, (group_id,))
    photos = [dict(row) for row in cursor.fetchall()]

    # Get rejections for this group
    cursor = conn.execute("""
        SELECT photo_id, rule_name FROM group_rejections
        WHERE group_id = ?
    """, (group_id,))
    rejections = {row[0]: row[1] for row in cursor.fetchall()}

    # When filtering by rule, exclude photos rejected by earlier rules
    if filter_rule:
        earlier_rules = set(get_earlier_rules(filter_rule))
        # Filter out photos rejected by earlier rules
        photos = [p for p in photos if rejections.get(p["id"]) not in earlier_rules]
        # Only show rejections from this rule
        filtered_rejections = {pid: rule for pid, rule in rejections.items() if rule == filter_rule}
    else:
        filtered_rejections = rejections

    # Get the set of rules applied to visible photos
    visible_rejection_rules = {rejections[p["id"]] for p in photos if p["id"] in rejections}

    # Determine which rule to use for identifying originals
    display_rule = filter_rule if filter_rule else (RULE_ORDER[0] if visible_rejection_rules else None)

    rejected_ids = set(filtered_rejections.keys())
    original_ids = identify_originals(photos, rejected_ids, display_rule) if display_rule else set()

    # Add status and paths to each photo
    for photo in photos:
        photo["paths"] = photo.get("all_paths", "").split("|") if photo.get("all_paths") else []
        if photo["id"] in rejected_ids:
            photo["status"] = "rejected"
            photo["rejection_rule"] = filtered_rejections[photo["id"]]
        elif photo["id"] in original_ids:
            photo["status"] = "original"
        else:
            photo["status"] = "other"

    return {
        "group_id": group_id,
        "photos": photos,
        "rejection_rules": sorted(visible_rejection_rules),
    }


@app.route('/')
def index():
    conn = get_connection()
    rules = get_rule_counts(conn)

    limit = 10
    offset = int(request.args.get('offset', 0))
    random_order = request.args.get('random', '0') == '1'
    total_groups = get_total_groups(conn)
    group_ids = get_groups_with_rejections(conn, limit=limit, offset=offset, random_order=random_order)

    groups = [get_group_data(conn, gid) for gid in group_ids]
    conn.close()

    return render_template_string(
        TEMPLATE,
        rules=rules,
        groups=groups,
        active_rule=None,
        total_groups=total_groups,
        offset=offset,
        limit=limit,
        random_order=random_order,
        request=request,
    )


@app.route('/rule/<rule_name>')
def show_rule(rule_name):
    conn = get_connection()
    rules = get_rule_counts(conn)

    limit = 10
    offset = int(request.args.get('offset', 0))
    random_order = request.args.get('random', '0') == '1'
    total_groups = get_total_groups(conn, rule_name)
    group_ids = get_groups_with_rejections(conn, rule_name, limit=limit, offset=offset, random_order=random_order)

    # Get group data, filtering to only those with visible photos
    groups = []
    for gid in group_ids:
        group_data = get_group_data(conn, gid, filter_rule=rule_name)
        if group_data["photos"]:  # Only include if has visible photos
            groups.append(group_data)
    conn.close()

    return render_template_string(
        TEMPLATE,
        rules=rules,
        groups=groups,
        active_rule=rule_name,
        total_groups=total_groups,
        offset=offset,
        limit=limit,
        random_order=random_order,
        request=request,
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
    print("Starting server at http://localhost:5001")
    app.run(debug=True, port=5001)
