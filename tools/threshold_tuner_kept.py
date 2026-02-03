#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
# ]
# ///
"""
Threshold Tuner (Kept Photos Only)

Shows pairs at each phash16 distance level, ordered by colorhash.
Helps visualize how colorhash discriminates within phash16 levels.
"""

import sqlite3
from pathlib import Path

import json

from flask import Flask, render_template_string, send_file, request

# Paths
DB_PATH = Path(__file__).parent.parent / "output" / "photos.db"
FILES_DIR = Path(__file__).parent.parent / "output" / "files"
RATINGS_FILE = Path(__file__).parent.parent / "output" / "threshold_ratings.json"

app = Flask(__name__)


def load_ratings() -> dict:
    """Load ratings from file."""
    if RATINGS_FILE.exists():
        with open(RATINGS_FILE) as f:
            return json.load(f)
    return {}


def save_ratings(ratings: dict):
    """Save ratings to file."""
    with open(RATINGS_FILE, "w") as f:
        json.dump(ratings, f, indent=2)


def get_rating(phash16: int, colorhash: int) -> int | None:
    """Get rating for a coordinate."""
    ratings = load_ratings()
    key = f"{phash16},{colorhash}"
    return ratings.get(key)


def set_rating(phash16: int, colorhash: int, rating: int):
    """Set rating for a coordinate."""
    ratings = load_ratings()
    key = f"{phash16},{colorhash}"
    ratings[key] = rating
    save_ratings(ratings)

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


def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate hamming distance between two hex hash strings."""
    xor = int(hash1, 16) ^ int(hash2, 16)
    return bin(xor).count("1")


def has_pairs_table():
    """Check if photo_pairs table exists and has data."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='photo_pairs'
    """)
    exists = cursor.fetchone() is not None
    if exists:
        count = conn.execute("SELECT COUNT(*) FROM photo_pairs").fetchone()[0]
        exists = count > 0
    conn.close()
    return exists


# Cache for dynamically sampled pairs
_dynamic_cache = None


def sample_pairs_dynamically(sample_size: int = 3000):
    """Sample pairs dynamically from photos with extended hashes."""
    global _dynamic_cache

    conn = get_connection()
    cursor = conn.execute("""
        SELECT
            kp.id, kp.perceptual_hash as phash, kp.dhash,
            kp.phash_16, kp.colorhash,
            dg.group_id as primary_group, kp.mime_type
        FROM kept_photos_with_hashes kp
        LEFT JOIN duplicate_groups dg ON kp.id = dg.photo_id
        ORDER BY RANDOM()
        LIMIT ?
    """, (sample_size,))
    photos = [dict(row) for row in cursor.fetchall()]
    conn.close()

    print(f"Sampled {len(photos)} photos with extended hashes")

    # Compute all cross-group pairs
    from collections import defaultdict
    pairs_by_phash16 = defaultdict(list)

    for i in range(len(photos)):
        for j in range(i + 1, len(photos)):
            p1, p2 = photos[i], photos[j]

            # Skip same-group pairs
            if p1["primary_group"] is not None and p1["primary_group"] == p2["primary_group"]:
                continue

            phash16_dist = hamming_distance(p1["phash_16"], p2["phash_16"])
            colorhash_dist = hamming_distance(p1["colorhash"], p2["colorhash"])

            pairs_by_phash16[phash16_dist].append({
                "photo_id_1": p1["id"],
                "photo_id_2": p2["id"],
                "phash16_dist": phash16_dist,
                "colorhash_dist": colorhash_dist,
            })

    # Sort each distance level by colorhash
    for dist in pairs_by_phash16:
        pairs_by_phash16[dist].sort(key=lambda x: x["colorhash_dist"])

    _dynamic_cache = pairs_by_phash16

    total = sum(len(v) for v in pairs_by_phash16.values())
    print(f"Computed {total:,} cross-group pairs")

    return pairs_by_phash16


def get_dynamic_pairs():
    """Get or compute dynamically sampled pairs."""
    global _dynamic_cache
    if _dynamic_cache is None:
        sample_pairs_dynamically()
    return _dynamic_cache


def get_distribution(mode: str):
    """Get count of cross-group pairs at each distance for the given mode."""
    if mode == "phash16":
        dist_col = "phash16_dist"
    else:
        dist_col = "colorhash_dist"

    if has_pairs_table():
        conn = get_connection()
        cursor = conn.execute(f"""
            SELECT {dist_col} as dist, COUNT(*) as cnt
            FROM photo_pairs
            WHERE same_primary_group = 0
            GROUP BY {dist_col}
            ORDER BY {dist_col}
        """)
        result = [(row['dist'], row['cnt']) for row in cursor.fetchall()]
        conn.close()
        return result
    else:
        # Use dynamic cache
        pairs = get_dynamic_pairs()
        from collections import defaultdict
        counts = defaultdict(int)
        for phash16_dist, pair_list in pairs.items():
            for p in pair_list:
                if mode == "phash16":
                    counts[phash16_dist] += 1
                else:
                    counts[p["colorhash_dist"]] += 1
        return [(d, counts[d]) for d in sorted(counts.keys())]


def get_pairs_at_distance(mode: str, distance: int, limit: int = 200):
    """Get pairs at a specific distance, ordered by the other hash."""
    if mode == "phash16":
        filter_col = "phash16_dist"
        order_col = "colorhash_dist"
    else:
        filter_col = "colorhash_dist"
        order_col = "phash16_dist"

    if has_pairs_table():
        conn = get_connection()
        cursor = conn.execute(f"""
            SELECT
                pp.photo_id_1, pp.photo_id_2,
                pp.phash_dist, pp.dhash_dist, pp.phash16_dist, pp.colorhash_dist,
                p1.mime_type as mime1, p2.mime_type as mime2
            FROM photo_pairs pp
            JOIN photos p1 ON pp.photo_id_1 = p1.id
            JOIN photos p2 ON pp.photo_id_2 = p2.id
            WHERE pp.same_primary_group = 0
            AND pp.{filter_col} = ?
            ORDER BY pp.{order_col} ASC
            LIMIT ?
        """, (distance, limit))
        pairs = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return pairs
    else:
        # Use dynamic cache
        all_pairs = get_dynamic_pairs()
        result = []
        for phash16_dist, pair_list in all_pairs.items():
            for p in pair_list:
                if mode == "phash16" and phash16_dist == distance:
                    result.append(p)
                elif mode == "colorhash" and p["colorhash_dist"] == distance:
                    result.append(p)
        # Sort by the other hash
        if mode == "phash16":
            result.sort(key=lambda x: x["colorhash_dist"])
        else:
            result.sort(key=lambda x: x["phash16_dist"])
        return result[:limit]


def get_stats():
    """Get basic stats."""
    conn = get_connection()

    total_pairs = 0
    cross_group = 0
    with_extended = 0

    try:
        total_pairs = conn.execute("SELECT COUNT(*) FROM photo_pairs").fetchone()[0]
        cross_group = conn.execute(
            "SELECT COUNT(*) FROM photo_pairs WHERE same_primary_group = 0"
        ).fetchone()[0]
    except:
        pass

    try:
        with_extended = conn.execute("SELECT COUNT(*) FROM extended_hashes").fetchone()[0]
    except:
        pass

    conn.close()
    return {
        "total_pairs": total_pairs,
        "cross_group": cross_group,
        "with_extended": with_extended,
    }


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Threshold Tuner - {{ stratify_label }} {{ distance }}</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: system-ui, sans-serif;
            margin: 0;
            padding: 10px;
            background: #1a1a1a;
            color: #eee;
        }
        a { color: #6cf; text-decoration: none; }
        a:hover { text-decoration: underline; }

        .header {
            display: flex;
            align-items: center;
            gap: 15px;
            margin-bottom: 10px;
            flex-wrap: wrap;
        }
        .title { font-size: 1.2em; font-weight: bold; }
        .stats { color: #888; font-size: 0.9em; }
        .mode-toggle {
            display: flex;
            gap: 5px;
        }
        .mode-toggle a {
            padding: 4px 10px;
            background: #333;
            border-radius: 4px;
        }
        .mode-toggle a:hover { background: #444; text-decoration: none; }
        .mode-toggle a.active { background: #264; }

        .distances {
            display: flex;
            gap: 4px;
            flex-wrap: wrap;
            margin-bottom: 15px;
        }
        .dist-btn {
            padding: 4px 8px;
            background: #333;
            border-radius: 4px;
            font-size: 0.85em;
        }
        .dist-btn:hover { background: #444; text-decoration: none; }
        .dist-btn.active { background: #264; }
        .dist-btn .count { color: #666; font-size: 0.8em; }

        .pairs-container {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .pair-row {
            display: flex;
            align-items: center;
            gap: 10px;
            background: #252525;
            padding: 6px;
            border-radius: 6px;
        }

        .order-label {
            font-size: 1.4em;
            font-weight: bold;
            width: 50px;
            text-align: center;
            flex-shrink: 0;
        }
        .order-low { color: #4f4; }
        .order-mid { color: #ff4; }
        .order-high { color: #f44; }

        .pair-images {
            display: flex;
            gap: 4px;
        }
        .pair-images img {
            height: 100px;
            width: auto;
            max-width: 150px;
            object-fit: contain;
            image-orientation: from-image;
            background: #111;
            border-radius: 4px;
        }

        .no-data {
            text-align: center;
            padding: 50px;
            color: #888;
        }

        .help {
            color: #666;
            font-size: 0.85em;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="mode-toggle">
            <a href="/phash16/{{ distance }}" class="{% if mode == 'phash16' %}active{% endif %}">pHash16</a>
            <a href="/colorhash/{{ distance }}" class="{% if mode == 'colorhash' %}active{% endif %}">cHash</a>
        </div>
        <span class="title">{{ stratify_label }} = {{ distance }}</span>
        <span class="stats">
            {{ pairs|length }} pairs | ordered by {{ order_label }}
            {% if is_sampled %}| <a href="/resample/{{ mode }}">resample</a>{% endif %}
        </span>
    </div>

    {% if distance_counts %}
    <div class="distances">
        {% for dist, count in distance_counts %}
        <a href="/{{ mode }}/{{ dist }}" class="dist-btn {% if dist == distance %}active{% endif %}">
            {{ dist }} <span class="count">({{ count }})</span>
        </a>
        {% endfor %}
    </div>
    {% endif %}

    {% if pairs %}
    <div class="pairs-container">
        {% for pair in pairs %}
        <div class="pair-row">
            <div class="order-label {% if pair[order_key] <= 3 %}order-low{% elif pair[order_key] <= 7 %}order-mid{% else %}order-high{% endif %}">
                {{ pair[order_key] }}
            </div>
            <div class="pair-images">
                <img src="/image/{{ pair.photo_id_1 }}" loading="lazy">
                <img src="/image/{{ pair.photo_id_2 }}" loading="lazy">
            </div>
        </div>
        {% endfor %}
    </div>
    <div class="help">
        Ordered by {{ order_label }} (green=low, yellow=mid, red=high).
        &larr;/&rarr; navigate, m toggle mode, r resample.
    </div>
    {% else %}
    <div class="no-data">
        {% if not stats.total_pairs and is_sampled %}
        <p>No pairs at {{ stratify_label }} = {{ distance }}. Try another distance.</p>
        {% else %}
        <p>No cross-group pairs at {{ stratify_label }} distance {{ distance }}</p>
        {% endif %}
    </div>
    {% endif %}

    <script>
        const distances = {{ distance_list | tojson }};
        const currentIdx = distances.indexOf({{ distance }});
        const mode = '{{ mode }}';

        document.addEventListener('keydown', (e) => {
            if (e.key === 'ArrowLeft' && currentIdx > 0) {
                window.location.href = '/' + mode + '/' + distances[currentIdx - 1];
            } else if (e.key === 'ArrowRight' && currentIdx < distances.length - 1) {
                window.location.href = '/' + mode + '/' + distances[currentIdx + 1];
            } else if (e.key === 'r') {
                window.location.href = '/resample/' + mode;
            } else if (e.key === 'm') {
                const newMode = mode === 'phash16' ? 'colorhash' : 'phash16';
                window.location.href = '/' + newMode + '/{{ distance }}';
            }
        });
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    """Redirect to 2D explorer at a point with data."""
    # Cross-group pairs typically have phash16 >= 60, most at 100+
    # Start at 90 which is near the low end where potential duplicates live
    return app.redirect('/2d/90/2')


@app.route('/resample/<mode>')
def resample(mode):
    """Clear dynamic cache and resample."""
    global _dynamic_cache
    _dynamic_cache = None
    if mode == "colorhash":
        return app.redirect('/colorhash/0')
    return app.redirect('/phash16/20')


@app.route('/<mode>/<int:distance>')
def show_distance(mode, distance):
    """Show all pairs at a specific distance."""
    if mode not in ("phash16", "colorhash"):
        return "Invalid mode", 400

    stats = get_stats()
    is_sampled = not has_pairs_table()

    distance_counts = get_distribution(mode)
    distance_list = [d for d, c in distance_counts]
    pairs = get_pairs_at_distance(mode, distance, limit=200)

    # Determine labels based on mode
    if mode == "phash16":
        stratify_label = "pHash16"
        order_label = "cHash"
        order_key = "colorhash_dist"
    else:
        stratify_label = "cHash"
        order_label = "pHash16"
        order_key = "phash16_dist"

    return render_template_string(
        TEMPLATE,
        mode=mode,
        distance=distance,
        pairs=pairs,
        distance_counts=distance_counts,
        distance_list=distance_list,
        stats=stats,
        is_sampled=is_sampled,
        stratify_label=stratify_label,
        order_label=order_label,
        order_key=order_key,
    )


def get_pairs_at_2d_point(phash16: int, colorhash: int, limit: int = 24):
    """Get pairs in a region around (phash16, colorhash) coordinate."""
    # Use a small range to get enough pairs
    p_range = 2  # phash16 ± 2
    c_range = 1  # colorhash ± 1

    if has_pairs_table():
        conn = get_connection()
        cursor = conn.execute("""
            SELECT
                pp.photo_id_1, pp.photo_id_2,
                pp.phash16_dist, pp.colorhash_dist
            FROM photo_pairs pp
            WHERE pp.same_primary_group = 0
            AND pp.phash16_dist BETWEEN ? AND ?
            AND pp.colorhash_dist BETWEEN ? AND ?
            ORDER BY ABS(pp.phash16_dist - ?) + ABS(pp.colorhash_dist - ?)
            LIMIT ?
        """, (phash16 - p_range, phash16 + p_range,
              colorhash - c_range, colorhash + c_range,
              phash16, colorhash, limit))
        pairs = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return pairs
    else:
        # Use dynamic cache
        all_pairs = get_dynamic_pairs()
        result = []
        for phash16_dist, pair_list in all_pairs.items():
            if abs(phash16_dist - phash16) <= p_range:
                for p in pair_list:
                    if abs(p["colorhash_dist"] - colorhash) <= c_range:
                        result.append(p)
        # Sort by distance from target point
        result.sort(key=lambda p: abs(p["phash16_dist"] - phash16) + abs(p["colorhash_dist"] - colorhash))
        return result[:limit]


def get_2d_counts(include_same_group=False):
    """Get counts at each (phash16, colorhash) point for heatmap."""
    if has_pairs_table():
        conn = get_connection()
        if include_same_group:
            # Get both cross-group and same-group counts
            cursor = conn.execute("""
                SELECT phash16_dist, colorhash_dist, same_primary_group, COUNT(*) as cnt
                FROM photo_pairs
                GROUP BY phash16_dist, colorhash_dist, same_primary_group
            """)
            cross_counts = {}
            same_counts = {}
            for row in cursor.fetchall():
                key = (row['phash16_dist'], row['colorhash_dist'])
                if row['same_primary_group']:
                    same_counts[key] = row['cnt']
                else:
                    cross_counts[key] = row['cnt']
            conn.close()
            return cross_counts, same_counts
        else:
            cursor = conn.execute("""
                SELECT phash16_dist, colorhash_dist, COUNT(*) as cnt
                FROM photo_pairs
                WHERE same_primary_group = 0
                GROUP BY phash16_dist, colorhash_dist
            """)
            counts = {(row['phash16_dist'], row['colorhash_dist']): row['cnt']
                      for row in cursor.fetchall()}
            conn.close()
            return counts
    else:
        all_pairs = get_dynamic_pairs()
        from collections import defaultdict
        counts = defaultdict(int)
        for phash16_dist, pair_list in all_pairs.items():
            for p in pair_list:
                counts[(phash16_dist, p["colorhash_dist"])] += 1
        if include_same_group:
            return dict(counts), {}  # No same-group data in dynamic cache
        return dict(counts)


TEMPLATE_2D = """
<!DOCTYPE html>
<html>
<head>
    <title>2D Threshold Explorer - ({{ phash16 }}, {{ colorhash }})</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: system-ui, sans-serif;
            margin: 0;
            padding: 10px;
            background: #1a1a1a;
            color: #eee;
        }

        .header {
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }
        .coords {
            font-size: 1.3em;
            font-weight: bold;
        }
        .stats { color: #888; }
        a { color: #6cf; text-decoration: none; }

        .rating-buttons {
            display: flex;
            gap: 5px;
        }
        .rating-btn {
            padding: 4px 10px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.9em;
        }
        .rating-btn:hover { opacity: 0.8; }
        .rating-1 { background: #2a5; color: #fff; }
        .rating-2 { background: #5a5; color: #fff; }
        .rating-3 { background: #aa5; color: #000; }
        .rating-4 { background: #a55; color: #fff; }
        .rating-5 { background: #a22; color: #fff; }
        .rating-btn.active { outline: 2px solid #fff; }

        .pairs-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin-bottom: 15px;
        }

        .pair-cell {
            display: flex;
            gap: 4px;
            background: #252525;
            padding: 6px;
            border-radius: 4px;
        }
        .pair-cell img {
            height: 90px;
            width: auto;
            max-width: 120px;
            object-fit: contain;
            image-orientation: from-image;
            background: #111;
        }

        .nav-hint {
            color: #666;
            font-size: 0.9em;
        }

        .no-data {
            text-align: center;
            padding: 30px;
            color: #888;
        }
    </style>
</head>
<body>
    <div class="header">
        <span class="coords">pHash16={{ phash16 }}, cHash={{ colorhash }}</span>
        <span class="stats">{{ pairs|length }} pairs</span>
        <div class="rating-buttons">
            <button class="rating-btn rating-1 {% if current_rating == 1 %}active{% endif %}" onclick="rate(1)">1</button>
            <button class="rating-btn rating-2 {% if current_rating == 2 %}active{% endif %}" onclick="rate(2)">2</button>
            <button class="rating-btn rating-3 {% if current_rating == 3 %}active{% endif %}" onclick="rate(3)">3</button>
            <button class="rating-btn rating-4 {% if current_rating == 4 %}active{% endif %}" onclick="rate(4)">4</button>
            <button class="rating-btn rating-5 {% if current_rating == 5 %}active{% endif %}" onclick="rate(5)">5</button>
        </div>
        <a href="/dist">map</a>
        <a href="/ratings">ratings</a>
        <a href="/auto/{{ phash16 }}/{{ colorhash }}">auto-scan</a>
    </div>

    {% if pairs %}
    <div class="pairs-grid">
        {% for pair in pairs %}
        <div class="pair-cell">
            <img src="/image/{{ pair.photo_id_1 }}" loading="lazy">
            <img src="/image/{{ pair.photo_id_2 }}" loading="lazy">
        </div>
        {% endfor %}
    </div>
    {% else %}
    <div class="no-data">No pairs at this point</div>
    {% endif %}

    <div class="nav-hint">
        Arrow keys: &uarr;&darr; pHash16, &larr;&rarr; cHash. Shift for bigger jumps.<br>
        Keys 1-5 to rate. 1=all good, 2=few FP, 3=mixed, 4=few FN, 5=all bad<br>
        'n' = next cell with data, 's' = start auto-scan from here
    </div>

    <script>
        const cellsWithData = {{ cells_with_data | tojson }};
        const ratings = {{ ratings_json | tojson }};

        function rate(r) {
            fetch('/rate/{{ phash16 }}/{{ colorhash }}/' + r, {method: 'POST'})
                .then(() => {
                    // Auto-advance to next unrated cell after rating
                    window.location.href = '/auto/{{ phash16 }}/{{ colorhash }}';
                });
        }

        function findNextCell(p, c) {
            // Find next cell with data, scanning row by row
            for (let checkP = p; checkP <= 150; checkP++) {
                let startC = (checkP === p) ? c + 1 : 0;
                for (let checkC = startC; checkC <= 14; checkC++) {
                    if (cellsWithData[checkP + ',' + checkC]) {
                        return [checkP, checkC];
                    }
                }
            }
            return null;
        }

        document.addEventListener('keydown', (e) => {
            let p = {{ phash16 }};
            let c = {{ colorhash }};
            const step = e.shiftKey ? 5 : 1;

            if (e.key >= '1' && e.key <= '5') {
                rate(parseInt(e.key));
                return;
            }

            if (e.key === 'n') {
                const next = findNextCell(p, c);
                if (next) {
                    window.location.href = '/2d/' + next[0] + '/' + next[1];
                }
                return;
            }

            if (e.key === 's') {
                window.location.href = '/auto/' + p + '/' + c;
                return;
            }

            if (e.key === 'ArrowUp') { p -= step; }
            else if (e.key === 'ArrowDown') { p += step; }
            else if (e.key === 'ArrowLeft') { c -= step; }
            else if (e.key === 'ArrowRight') { c += step; }
            else { return; }

            if (p < 0) p = 0;
            if (c < 0) c = 0;

            window.location.href = '/2d/' + p + '/' + c;
        });
    </script>
</body>
</html>
"""


@app.route('/2d/<int:phash16>/<int:colorhash>')
def show_2d(phash16, colorhash):
    """Show pairs at a specific 2D coordinate."""
    pairs = get_pairs_at_2d_point(phash16, colorhash, limit=24)
    current_rating = get_rating(phash16, colorhash)
    ratings = load_ratings()

    # Get cells with data for navigation
    counts = get_2d_counts()
    cells_with_data = {f"{k[0]},{k[1]}": True for k in counts.keys()}

    return render_template_string(
        TEMPLATE_2D,
        phash16=phash16,
        colorhash=colorhash,
        pairs=pairs,
        current_rating=current_rating,
        cells_with_data=cells_with_data,
        ratings_json=ratings,
    )


@app.route('/rate/<int:phash16>/<int:colorhash>/<int:rating>', methods=['POST'])
def rate_coordinate(phash16, colorhash, rating):
    """Record a rating for a coordinate."""
    if 1 <= rating <= 5:
        set_rating(phash16, colorhash, rating)
    return "OK"


def find_next_unrated_cell(counts, ratings, start_p, start_c):
    """Find next cell with data that hasn't been rated."""
    for p in range(start_p, 151):
        start = start_c + 1 if p == start_p else 0
        for c in range(start, 15):
            if (p, c) in counts and f"{p},{c}" not in ratings:
                return (p, c)
    return None


def should_skip_row(ratings, p, max_c=14):
    """Check if we should skip to next row (2+ consecutive '5' ratings)."""
    consecutive_bad = 0
    for c in range(max_c + 1):
        r = ratings.get(f"{p},{c}")
        if r == 5:
            consecutive_bad += 1
            if consecutive_bad >= 2:
                return True
        else:
            consecutive_bad = 0
    return False


@app.route('/auto/<int:phash16>/<int:colorhash>')
def auto_scan(phash16, colorhash):
    """Auto-navigate to next interesting cell."""
    counts = get_2d_counts()
    ratings = load_ratings()

    # If current row has 2+ consecutive bad ratings, skip to next row
    if should_skip_row(ratings, phash16):
        # Find first cell with data in next row
        for p in range(phash16 + 1, 151):
            for c in range(15):
                if (p, c) in counts and f"{p},{c}" not in ratings:
                    return app.redirect(f'/2d/{p}/{c}')

    # Otherwise find next unrated cell
    next_cell = find_next_unrated_cell(counts, ratings, phash16, colorhash)
    if next_cell:
        return app.redirect(f'/2d/{next_cell[0]}/{next_cell[1]}')

    # No more cells
    return app.redirect('/dist')


TEMPLATE_RATINGS = """
<!DOCTYPE html>
<html>
<head>
    <title>Threshold Ratings</title>
    <style>
        body {
            font-family: monospace;
            background: #1a1a1a;
            color: #eee;
            padding: 20px;
        }
        a { color: #6cf; }
        table { border-collapse: collapse; margin-top: 15px; }
        th, td { padding: 8px 12px; text-align: left; border: 1px solid #333; }
        th { background: #333; }
        .r1 { color: #2a5; }
        .r2 { color: #5a5; }
        .r3 { color: #aa5; }
        .r4 { color: #a55; }
        .r5 { color: #a22; }
    </style>
</head>
<body>
    <h2>Recorded Ratings</h2>
    <p>1=all good, 2=few FP, 3=mixed, 4=few FN, 5=all bad</p>
    <p><a href="/">Back to explorer</a></p>

    {% if ratings %}
    <table>
        <tr><th>pHash16</th><th>cHash</th><th>Rating</th><th></th></tr>
        {% for coord, rating in ratings_sorted %}
        <tr>
            <td>{{ coord.split(',')[0] }}</td>
            <td>{{ coord.split(',')[1] }}</td>
            <td class="r{{ rating }}">{{ rating }}</td>
            <td><a href="/2d/{{ coord.split(',')[0] }}/{{ coord.split(',')[1] }}">view</a></td>
        </tr>
        {% endfor %}
    </table>
    {% else %}
    <p>No ratings recorded yet. Use keys 1-5 while viewing pairs.</p>
    {% endif %}
</body>
</html>
"""


@app.route('/ratings')
def show_ratings():
    """Show all recorded ratings."""
    ratings = load_ratings()
    # Sort by phash16 then colorhash
    ratings_sorted = sorted(
        ratings.items(),
        key=lambda x: (int(x[0].split(',')[0]), int(x[0].split(',')[1]))
    )
    return render_template_string(
        TEMPLATE_RATINGS,
        ratings=ratings,
        ratings_sorted=ratings_sorted,
    )


TEMPLATE_DIST = """
<!DOCTYPE html>
<html>
<head>
    <title>2D Distribution</title>
    <style>
        body {
            font-family: monospace;
            background: #1a1a1a;
            color: #eee;
            padding: 20px;
        }
        a { color: #6cf; }
        h3 { margin-top: 30px; }
        table { border-collapse: collapse; }
        td {
            width: 30px;
            height: 20px;
            text-align: center;
            font-size: 10px;
            position: relative;
        }
        .header { background: #333; font-weight: bold; }
        .has-data { cursor: pointer; }
        .heat-0 { background: #111; }
        .heat-1 { background: #1a3a1a; }
        .heat-2 { background: #2a5a2a; }
        .heat-3 { background: #3a7a3a; }
        .heat-4 { background: #5a9a5a; }
        .heat-5 { background: #7aba7a; }
        .same-group { background: #224; }
        .same-heat-1 { background: #113; }
        .same-heat-2 { background: #226; }
        .same-heat-3 { background: #339; }
        .same-heat-4 { background: #44c; }
        .same-heat-5 { background: #55f; }
        .rated { border: 2px solid; }
        .rated-1 { border-color: #2a5; }
        .rated-2 { border-color: #5a5; }
        .rated-3 { border-color: #aa5; }
        .rated-4 { border-color: #a55; }
        .rated-5 { border-color: #a22; }
        .legend { margin-top: 15px; }
        .legend span { margin-right: 15px; }
        .tables { display: flex; gap: 40px; flex-wrap: wrap; }
        .table-section h3 { margin-top: 0; }
    </style>
</head>
<body>
    <h2>2D Distribution: pHash16 (rows) × cHash (cols)</h2>
    <p>Click a cell to explore. Colored borders show your ratings. <a href="/auto/{{ min_phash16 }}/0">Start auto-scan</a></p>

    <div class="tables">
    <div class="table-section">
    <h3>Cross-group pairs (green = more pairs)</h3>
    <table>
        <tr>
            <td class="header">p\\c</td>
            {% for c in range(max_colorhash + 1) %}
            <td class="header">{{ c }}</td>
            {% endfor %}
        </tr>
        {% for p in range(min_phash16, max_phash16 + 1, 2) %}
        <tr>
            <td class="header">{{ p }}</td>
            {% for c in range(max_colorhash + 1) %}
            {% set count = cross_counts.get((p, c), 0) + cross_counts.get((p+1, c), 0) %}
            {% set r = ratings.get(p|string + ',' + c|string) %}
            {% if count > 0 %}
            <td class="has-data heat-{{ [5, (count // 100) + 1] | min }} {% if r %}rated rated-{{ r }}{% endif %}" onclick="window.location='/2d/{{ p }}/{{ c }}'">
                {{ count if count < 1000 else '1k+' }}
            </td>
            {% else %}
            <td class="heat-0 {% if r %}rated rated-{{ r }}{% endif %}">·</td>
            {% endif %}
            {% endfor %}
        </tr>
        {% endfor %}
    </table>
    </div>

    {% if same_counts %}
    <div class="table-section">
    <h3>Same-group pairs (blue = more pairs)</h3>
    <p style="color:#888;font-size:0.9em">These are known duplicates - should cluster in "good" zone</p>
    <table>
        <tr>
            <td class="header">p\\c</td>
            {% for c in range(max_colorhash + 1) %}
            <td class="header">{{ c }}</td>
            {% endfor %}
        </tr>
        {% for p in range(min_phash16, max_phash16 + 1, 2) %}
        <tr>
            <td class="header">{{ p }}</td>
            {% for c in range(max_colorhash + 1) %}
            {% set count = same_counts.get((p, c), 0) + same_counts.get((p+1, c), 0) %}
            {% set r = ratings.get(p|string + ',' + c|string) %}
            {% if count > 0 %}
            <td class="same-group same-heat-{{ [5, (count // 10) + 1] | min }} {% if r %}rated rated-{{ r }}{% endif %}">
                {{ count if count < 1000 else '1k+' }}
            </td>
            {% else %}
            <td class="heat-0 {% if r %}rated rated-{{ r }}{% endif %}">·</td>
            {% endif %}
            {% endfor %}
        </tr>
        {% endfor %}
    </table>
    </div>
    {% endif %}
    </div>

    <div class="legend">
        <span class="rated-1" style="color:#2a5">■ 1=good</span>
        <span class="rated-2" style="color:#5a5">■ 2=few FP</span>
        <span class="rated-3" style="color:#aa5">■ 3=mixed</span>
        <span class="rated-4" style="color:#a55">■ 4=few FN</span>
        <span class="rated-5" style="color:#a22">■ 5=bad</span>
    </div>
</body>
</html>
"""


@app.route('/dist')
def show_distribution():
    """Show 2D distribution heatmap."""
    result = get_2d_counts(include_same_group=True)

    if isinstance(result, tuple):
        cross_counts, same_counts = result
    else:
        cross_counts, same_counts = result, {}

    if not cross_counts and not same_counts:
        return "No data yet. Waiting for sampling.", 200

    all_keys = set(cross_counts.keys()) | set(same_counts.keys())
    min_phash16 = min(k[0] for k in all_keys)
    max_phash16 = min(max(k[0] for k in all_keys), 140)  # Cap display
    max_colorhash = min(max(k[1] for k in all_keys), 14)  # Cap display
    ratings = load_ratings()

    return render_template_string(
        TEMPLATE_DIST,
        cross_counts=cross_counts,
        same_counts=same_counts,
        min_phash16=min_phash16,
        max_phash16=max_phash16,
        max_colorhash=max_colorhash,
        ratings=ratings,
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
    print()

    stats = get_stats()
    print(f"Extended hashes: {stats['with_extended']:,}")
    print(f"Total pairs: {stats['total_pairs']:,}")
    print(f"Cross-group pairs: {stats['cross_group']:,}")
    print()

    if not has_pairs_table():
        print("NOTE: photo_pairs table not found.")
        print("Run './run2 --stage 1b' after hashing completes.")
        print()

    print("Starting server at http://localhost:5003")
    app.run(debug=True, port=5003)
