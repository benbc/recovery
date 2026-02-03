#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Threshold Tuner (Kept Photos Only)

Like threshold_tuner.py but only looks at "kept" photos - those not in
junk_deletions, group_rejections, or individual_decisions.

Helps tune thresholds for finding additional groups among remaining photos.
"""

import random
import sqlite3
from collections import defaultdict
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

# Sample size for finding pairs
SAMPLE_SIZE = 5000

# Cache for sampled pairs
_pair_cache = {
    "phash": None,
    "dhash": None,
}


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_file_path(photo_id: str, mime_type: str) -> Path:
    """Get the path to the linked file for a photo."""
    ext = MIME_TO_EXT.get(mime_type, ".bin")
    return FILES_DIR / photo_id[:2] / f"{photo_id}{ext}"


def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate hamming distance between two hex hash strings."""
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count("1")


def get_kept_photos_query():
    """Return SQL to filter to kept photos only."""
    return """
        SELECT p.id, p.perceptual_hash as phash, p.dhash, p.mime_type,
               p.width, p.height, p.file_size,
               dg.group_id as primary_group,
               sg.secondary_group_id as secondary_group
        FROM photos p
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        LEFT JOIN duplicate_groups dg ON p.id = dg.photo_id
        LEFT JOIN secondary_groups sg ON p.id = sg.photo_id
        WHERE jd.photo_id IS NULL
        AND gr.photo_id IS NULL
        AND id.photo_id IS NULL
        AND p.perceptual_hash IS NOT NULL
        AND p.dhash IS NOT NULL
    """


def sample_pairs(mode: str) -> dict[int, list]:
    """
    Sample photo pairs and group by hamming distance.

    Only shows pairs that could be JOINS - different primary groups or ungrouped.
    Never shows pairs within the same primary group.

    Modes:
    - phash: Group by pHash distance
    - dhash: Group by dHash distance
    """
    conn = get_connection()

    query = get_kept_photos_query() + """
        ORDER BY RANDOM()
        LIMIT ?
    """

    cursor = conn.execute(query, (SAMPLE_SIZE,))
    photos = [dict(row) for row in cursor.fetchall()]
    conn.close()

    print(f"Sampled {len(photos)} photos for {mode} comparison")

    # Count grouped vs ungrouped
    grouped = sum(1 for p in photos if p["primary_group"] is not None)
    print(f"  {grouped} in primary groups, {len(photos) - grouped} ungrouped")

    # Compute all pair distances within sample
    # ONLY keep pairs that could be joins (different groups or ungrouped)
    pairs_by_distance = defaultdict(list)
    total_pairs = 0
    skipped_same_group = 0

    for i in range(len(photos)):
        for j in range(i + 1, len(photos)):
            total_pairs += 1

            # Skip pairs in the same primary group - we're looking for joins
            pg1 = photos[i]["primary_group"]
            pg2 = photos[j]["primary_group"]
            if pg1 is not None and pg1 == pg2:
                skipped_same_group += 1
                continue

            phash_dist = hamming_distance(photos[i]["phash"], photos[j]["phash"])
            dhash_dist = hamming_distance(photos[i]["dhash"], photos[j]["dhash"])

            # Determine distance key based on mode
            if mode == "dhash":
                dist = dhash_dist
            else:
                dist = phash_dist

            # Only keep pairs up to distance 20
            if dist <= 20:
                p1 = dict(photos[i])
                p2 = dict(photos[j])
                # Add both distances to each photo for display
                p1["phash_dist"] = phash_dist
                p1["dhash_dist"] = dhash_dist
                p2["phash_dist"] = phash_dist
                p2["dhash_dist"] = dhash_dist
                pairs_by_distance[dist].append((p1, p2))

    print(f"Computed {total_pairs:,} pair distances")
    print(f"  Skipped {skipped_same_group:,} same-group pairs")
    print(f"  Kept {total_pairs - skipped_same_group:,} cross-group pairs")
    for dist in sorted(pairs_by_distance.keys()):
        print(f"  Distance {dist}: {len(pairs_by_distance[dist])} pairs")

    # Shuffle pairs within each distance level
    for dist in pairs_by_distance:
        random.shuffle(pairs_by_distance[dist])

    return pairs_by_distance


def get_pairs(mode: str) -> dict[int, list]:
    """Get or compute cached pairs for mode."""
    if _pair_cache[mode] is None:
        print(f"\nComputing {mode} pairs (this may take a moment)...")
        _pair_cache[mode] = sample_pairs(mode)
    return _pair_cache[mode]


def get_photo_paths(photo_id: str) -> list[str]:
    """Get all source paths for a photo."""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT source_path FROM photo_paths WHERE photo_id = ? LIMIT 5",
        (photo_id,)
    )
    paths = [row["source_path"] for row in cursor.fetchall()]
    conn.close()
    return paths


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Threshold Tuner (Kept) - {{ mode }}</title>
    <style>
        * { box-sizing: border-box; }
        html, body { height: 100%; margin: 0; overflow: hidden; }
        body { font-family: system-ui, sans-serif; padding: 10px; background: #1a1a1a; color: #eee; display: flex; flex-direction: column; }
        a { color: #6cf; text-decoration: none; }
        a:hover { text-decoration: underline; }

        .header { display: flex; align-items: center; gap: 20px; margin-bottom: 10px; flex-wrap: wrap; }
        .mode-toggle { display: flex; gap: 5px; }
        .mode-toggle a { padding: 6px 12px; background: #333; border-radius: 5px; }
        .mode-toggle a:hover { background: #444; text-decoration: none; }
        .mode-toggle a.active { background: #264; }

        .distances { display: flex; gap: 5px; flex-wrap: wrap; }
        .dist-btn { padding: 6px 12px; background: #333; border-radius: 5px; font-size: 0.9em; }
        .dist-btn:hover { background: #444; text-decoration: none; }
        .dist-btn.active { background: #264; }
        .dist-btn .count { color: #888; font-size: 0.85em; }

        .nav-info { display: flex; align-items: center; gap: 15px; margin: 8px 0; font-size: 0.9em; }
        .nav-info .pair-nav { display: flex; align-items: center; gap: 8px; }
        .nav-info a { padding: 4px 10px; background: #333; border-radius: 4px; }
        .nav-info a:hover { background: #444; text-decoration: none; }
        .help { color: #888; font-size: 0.85em; }

        .pair-container { flex: 1; display: flex; gap: 15px; overflow: hidden; }
        .photo-card { flex: 1; background: #222; border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; }
        .photo-card .img-wrapper { height: calc(100% - 140px); background: #111; }
        .photo-card img { width: 100%; height: 100%; object-fit: contain; image-orientation: from-image; }
        .photo-info { height: 140px; padding: 10px; font-size: 0.85em; overflow-y: auto; flex-shrink: 0; }
        .photo-info .photo-id { font-family: monospace; color: #6cf; cursor: pointer; margin-bottom: 4px; }
        .photo-info .photo-id:hover { text-decoration: underline; }
        .photo-info .dims { color: #fff; margin-bottom: 5px; }
        .photo-info .group-info { margin-bottom: 5px; }
        .photo-info .group-info .grouped { color: #8f8; }
        .photo-info .group-info .ungrouped { color: #f88; }
        .photo-info .paths { color: #aaa; }
        .photo-info .path { margin: 3px 0; word-break: break-all; font-size: 0.9em; }

        .no-pairs { text-align: center; padding: 50px; color: #888; }

        .verdict { text-align: center; padding: 15px; margin: 10px 0; background: #222; border-radius: 8px; }
        .verdict-title { font-size: 1.2em; margin-bottom: 8px; }
        .verdict-distances { font-family: monospace; font-size: 1.1em; margin-bottom: 8px; }
        .verdict-hint { color: #888; font-size: 0.9em; }

        .stats { background: #222; padding: 10px 15px; border-radius: 5px; font-size: 0.85em; color: #aaa; }
    </style>
</head>
<body>
    <div class="header">
        <div class="mode-toggle">
            <a href="/phash/{{ distance }}/{{ pair_idx }}" class="{% if mode == 'phash' %}active{% endif %}">pHash</a>
            <a href="/dhash/{{ distance }}/{{ pair_idx }}" class="{% if mode == 'dhash' %}active{% endif %}">dHash</a>
        </div>
        <div class="stats">
            Kept photos: {{ stats.kept }} | Ungrouped: {{ stats.ungrouped }} | In primary groups: {{ stats.in_primary }}
        </div>
    </div>

    <div class="distances">
        {% for dist, count in distance_counts %}
        <a href="/{{ mode }}/{{ dist }}/0" class="dist-btn {% if dist == distance %}active{% endif %}">
            {{ dist }} <span class="count">({{ count }})</span>
        </a>
        {% endfor %}
    </div>

    {% if pair %}
    <div class="nav-info">
        <div class="pair-nav">
            {% if pair_idx > 0 %}
            <a id="prev" href="/{{ mode }}/{{ distance }}/{{ pair_idx - 1 }}">&larr; Prev</a>
            {% endif %}
            <span>Pair {{ pair_idx + 1 }} / {{ total_pairs }}</span>
            {% if pair_idx < total_pairs - 1 %}
            <a id="next" href="/{{ mode }}/{{ distance }}/{{ pair_idx + 1 }}">Next &rarr;</a>
            {% endif %}
        </div>
        <span class="help">Keys: &larr;/&rarr; pairs, &uarr;/&darr; distance, p/d hash mode, r resample</span>
    </div>

    <div class="pair-container">
        <div class="photo-card">
            <div class="img-wrapper">
                <img src="/image/{{ pair.0.id }}" alt="Photo 1">
            </div>
            <div class="photo-info">
                <div class="photo-id" onclick="navigator.clipboard.writeText('{{ pair.0.id }}')" title="Click to copy">{{ pair.0.id[:16] }}...</div>
                <div class="dims"><strong>{{ pair.0.width }}x{{ pair.0.height }}</strong> &middot; {{ pair.0.file_size | filesizeformat }}</div>
                <div class="group-info">
                    {% if pair.0.primary_group is not none %}
                    <span class="grouped">Primary: {{ pair.0.primary_group }}</span>
                    {% else %}
                    <span class="ungrouped">Ungrouped</span>
                    {% endif %}
                    {% if pair.0.secondary_group is not none %}
                    &middot; <span class="grouped">Secondary: {{ pair.0.secondary_group }}</span>
                    {% endif %}
                </div>
                <div class="paths">
                    {% for path in pair.0.paths %}
                    <div class="path">{{ path }}</div>
                    {% endfor %}
                </div>
            </div>
        </div>
        <div class="photo-card">
            <div class="img-wrapper">
                <img src="/image/{{ pair.1.id }}" alt="Photo 2">
            </div>
            <div class="photo-info">
                <div class="photo-id" onclick="navigator.clipboard.writeText('{{ pair.1.id }}')" title="Click to copy">{{ pair.1.id[:16] }}...</div>
                <div class="dims"><strong>{{ pair.1.width }}x{{ pair.1.height }}</strong> &middot; {{ pair.1.file_size | filesizeformat }}</div>
                <div class="group-info">
                    {% if pair.1.primary_group is not none %}
                    <span class="grouped">Primary: {{ pair.1.primary_group }}</span>
                    {% else %}
                    <span class="ungrouped">Ungrouped</span>
                    {% endif %}
                    {% if pair.1.secondary_group is not none %}
                    &middot; <span class="grouped">Secondary: {{ pair.1.secondary_group }}</span>
                    {% endif %}
                </div>
                <div class="paths">
                    {% for path in pair.1.paths %}
                    <div class="path">{{ path }}</div>
                    {% endfor %}
                </div>
            </div>
        </div>
    </div>

    <div class="verdict">
        <div class="verdict-distances">pHash: {{ pair.0.phash_dist }} | dHash: {{ pair.0.dhash_dist }}</div>
        <div class="verdict-title">
            Should these be joined? (different groups or ungrouped)
        </div>
        <div class="verdict-hint">
            Current thresholds: pHash ≤10 group, pHash 11-12 if dHash <22, pHash 13-14 if dHash ≤17
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
            } else if (e.key === 'ArrowUp') {
                const distances = {{ distance_list | tojson }};
                const idx = distances.indexOf({{ distance }});
                if (idx > 0) {
                    window.location.href = '/{{ mode }}/' + distances[idx - 1] + '/0';
                }
            } else if (e.key === 'ArrowDown') {
                const distances = {{ distance_list | tojson }};
                const idx = distances.indexOf({{ distance }});
                if (idx < distances.length - 1) {
                    window.location.href = '/{{ mode }}/' + distances[idx + 1] + '/0';
                }
            } else if (e.key === 'p') {
                window.location.href = '/phash/{{ distance }}/0';
            } else if (e.key === 'd') {
                window.location.href = '/dhash/{{ distance }}/0';
            } else if (e.key === 'r') {
                window.location.href = '/resample/{{ mode }}';
            }
        });
    </script>
    {% else %}
    <div class="no-pairs">
        <p>No pairs found at distance {{ distance }}</p>
        <p>Try a different distance level or <a href="/resample/{{ mode }}">resample</a></p>
    </div>
    {% endif %}
</body>
</html>
"""


def get_stats():
    """Get stats about kept photos."""
    conn = get_connection()

    kept = conn.execute("""
        SELECT COUNT(*) FROM photos p
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        WHERE jd.photo_id IS NULL AND gr.photo_id IS NULL AND id.photo_id IS NULL
        AND p.perceptual_hash IS NOT NULL AND p.dhash IS NOT NULL
    """).fetchone()[0]

    in_primary = conn.execute("""
        SELECT COUNT(*) FROM photos p
        LEFT JOIN junk_deletions jd ON p.id = jd.photo_id
        LEFT JOIN group_rejections gr ON p.id = gr.photo_id
        LEFT JOIN individual_decisions id ON p.id = id.photo_id
        JOIN duplicate_groups dg ON p.id = dg.photo_id
        WHERE jd.photo_id IS NULL AND gr.photo_id IS NULL AND id.photo_id IS NULL
    """).fetchone()[0]

    conn.close()

    return {
        "kept": kept,
        "in_primary": in_primary,
        "ungrouped": kept - in_primary,
    }


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
    """Redirect to default view."""
    return app.redirect('/phash/0/0')


@app.route('/resample/<mode>')
def resample(mode):
    """Clear cache and resample pairs."""
    if mode in _pair_cache:
        _pair_cache[mode] = None
    return app.redirect(f'/{mode}/0/0')


@app.route('/<mode>/<int:distance>/<int:pair_idx>')
def show_pair(mode, distance, pair_idx):
    """Show a specific pair at a specific distance."""
    if mode not in ("phash", "dhash"):
        return "Invalid mode", 400

    pairs_by_dist = get_pairs(mode)
    stats = get_stats()

    # Build distance counts for nav
    distance_counts = [(d, len(pairs_by_dist.get(d, []))) for d in range(0, 21)]
    distance_counts = [(d, c) for d, c in distance_counts if c > 0]
    distance_list = [d for d, c in distance_counts]

    # Get pairs at requested distance
    pairs = pairs_by_dist.get(distance, [])
    total_pairs = len(pairs)

    pair = None
    if pairs and 0 <= pair_idx < len(pairs):
        p1, p2 = pairs[pair_idx]
        # Add paths
        p1["paths"] = get_photo_paths(p1["id"])
        p2["paths"] = get_photo_paths(p2["id"])
        pair = (p1, p2)

    return render_template_string(
        TEMPLATE,
        mode=mode,
        distance=distance,
        pair_idx=pair_idx,
        pair=pair,
        total_pairs=total_pairs,
        distance_counts=distance_counts,
        distance_list=distance_list,
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


if __name__ == '__main__':
    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Sample size: {SAMPLE_SIZE} photos")
    print()

    stats = get_stats()
    print(f"Kept photos: {stats['kept']:,}")
    print(f"  In primary groups: {stats['in_primary']:,}")
    print(f"  Ungrouped: {stats['ungrouped']:,}")
    print()

    print("Starting server at http://localhost:5003")
    app.run(debug=True, port=5003)
