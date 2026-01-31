#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "flask",
# ]
# ///
"""
Photo album selection app - prototype.
Browse all non-rejected photos in date order, paginated grid view.
"""

import sqlite3
from pathlib import Path
from flask import Flask, send_from_directory, render_template_string

app = Flask(__name__)

DB_PATH = Path(__file__).parent.parent / "organized" / "photos.db"
PHOTOS_PER_PAGE = 28  # 4 rows of 7

# Cache of all eligible photo IDs, sorted by date
PHOTO_CACHE = None

def get_photo_cache():
    """Load and cache all eligible photo IDs sorted by date."""
    global PHOTO_CACHE
    if PHOTO_CACHE is None:
        conn = get_db()
        cursor = conn.execute("""
            SELECT p.id FROM photos p
            WHERE p.is_non_photo = 0
              AND NOT EXISTS (
                SELECT 1 FROM duplicate_groups dg
                WHERE dg.photo_id = p.id AND dg.rejected = 1
              )
            ORDER BY p.date_taken ASC, p.id
        """)
        PHOTO_CACHE = [row['id'] for row in cursor.fetchall()]
        conn.close()
        print(f"Cached {len(PHOTO_CACHE)} photo IDs")
    return PHOTO_CACHE

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Album Selection</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #1a1a1a;
            color: #fff;
        }
        .header {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            height: 36px;
            background: #000;
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0 20px;
            font-size: 14px;
            color: #888;
            z-index: 100;
        }
        .header .page-info { color: #fff; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, 200px);
            gap: 4px;
            justify-content: center;
            padding: 44px 8px 8px 8px;
        }
        .photo {
            width: 200px;
            height: 200px;
            object-fit: contain;
            background: #2a2a2a;
            cursor: pointer;
        }
        .photo:hover {
            outline: 2px solid #666;
        }
    </style>
</head>
<body>
    <div class="header">
        <span class="date">{{ date_display }}</span>
        <span class="page-info">Page {{ page }} of {{ total_pages }}</span>
        <span class="selected">0 selected</span>
    </div>
    <div class="grid">
        {% for photo in photos %}
        <img src="/image/{{ photo['path'] }}" class="photo" alt="">
        {% endfor %}
    </div>
    <script>
        // Preload next page images
        {% if page < total_pages %}
        fetch('/?page={{ page + 1 }}')
            .then(r => r.text())
            .then(html => {
                const parser = new DOMParser();
                const doc = parser.parseFromString(html, 'text/html');
                doc.querySelectorAll('.photo').forEach(img => {
                    const preload = new Image();
                    preload.src = img.src;
                });
            });
        {% endif %}

        document.addEventListener('keydown', (e) => {
            if (e.code === 'Space') {
                e.preventDefault();
                {% if page < total_pages %}
                window.location.href = '/?page={{ page + 1 }}';
                {% endif %}
            }
        });
    </script>
</body>
</html>
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/')
def index():
    from flask import request
    page = int(request.args.get('page', 1))

    # Use cached photo list
    photo_ids = get_photo_cache()
    total_photos = len(photo_ids)
    total_pages = (total_photos + PHOTOS_PER_PAGE - 1) // PHOTOS_PER_PAGE

    # Clamp page
    page = max(1, min(page, total_pages))
    offset = (page - 1) * PHOTOS_PER_PAGE

    # Get slice of photo IDs for this page
    page_ids = photo_ids[offset:offset + PHOTOS_PER_PAGE]

    # Fetch just these photos
    conn = get_db()
    placeholders = ','.join('?' * len(page_ids))
    cursor = conn.execute(f"""
        SELECT id, path, date_taken FROM photos
        WHERE id IN ({placeholders})
    """, page_ids)

    # Re-sort to match original order (IN doesn't preserve order)
    photos_by_id = {row['id']: dict(row) for row in cursor.fetchall()}
    photos = [photos_by_id[pid] for pid in page_ids]

    # Format date for display
    date_display = ""
    if photos and photos[0]['date_taken']:
        dt = photos[0]['date_taken']
        # date_taken is stored as ISO string
        if dt:
            try:
                year_month = dt[:7]  # "2012-06"
                date_display = year_month
            except:
                pass

    conn.close()

    return render_template_string(TEMPLATE,
        photos=photos,
        page=page,
        total_pages=total_pages,
        date_display=date_display
    )


@app.route('/image/<path:filepath>')
def serve_image(filepath):
    """Serve an image file."""
    # filepath is relative to organized/
    full_path = Path(__file__).parent.parent / "organized" / filepath
    return send_from_directory(full_path.parent, full_path.name)


if __name__ == '__main__':
    print(f"Database: {DB_PATH}")
    print(f"Photos per page: {PHOTOS_PER_PAGE}")
    app.run(debug=True, port=5001)
