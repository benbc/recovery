#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = [
#   "flask",
#   "piexif",
#   "pillow",
# ]
# ///
"""
Date Browser - Browse groups and singletons by date.

Shows one representative photo per group, organized by date.
Click photos to select, then press 'm' to merge selected groups.

Keyboard shortcuts:
  ← / → - Previous / next date
  m - Merge selected groups
  Escape - Clear selection
"""

import sqlite3
import sys
from pathlib import Path

from flask import Flask, render_template_string, request, send_file, jsonify, redirect

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline2.date_utils import derive_date_for_group, derive_date, CONFIDENCE_TIERS

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


def get_date_for_display(date_value: str | None) -> str:
    """Extract just the date part for grouping (YYYY-MM-DD or partial)."""
    if not date_value:
        return "unknown"
    # Take first 10 chars max (YYYY-MM-DD)
    return date_value[:10]


def get_quality(width: int | None, height: int | None) -> str:
    """Determine photo quality based on resolution."""
    if not width or not height:
        return "unknown"
    pixels = width * height
    if pixels >= 2_000_000:
        return "high"
    elif pixels >= 500_000:
        return "medium"
    else:
        return "low"


def get_photo_dimensions(conn, photo_id: str) -> tuple[int | None, int | None]:
    """Get width and height for a photo."""
    cursor = conn.execute("SELECT width, height FROM photos WHERE id = ?", (photo_id,))
    row = cursor.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def get_all_dates_with_groups(conn) -> list[str]:
    """Get all unique dates that have groups or singletons, sorted chronologically."""
    return get_cache(conn)['dates_list']


def get_groups_for_date(conn, target_date: str) -> list[dict]:
    """Get all groups and singletons for a given date."""
    return get_cache(conn)['by_date'].get(target_date, [])


def get_next_group_id(conn) -> int:
    """Get the next available group ID."""
    cursor = conn.execute("SELECT MAX(group_id) FROM composite_groups")
    row = cursor.fetchone()
    return (row[0] or 0) + 1


def merge_groups(conn, group_ids: list[str]) -> str:
    """
    Merge multiple groups into one. Returns the resulting group identifier.

    Groups are identified as:
    - 'G_<id>' for existing composite groups
    - 'S_<photo_id>' for singletons
    """
    if len(group_ids) < 2:
        return group_ids[0] if group_ids else None

    # Find the largest existing group to merge into, or create new
    target_group_id = None
    max_size = 0

    for gid in group_ids:
        if gid.startswith('G_'):
            group_num = int(gid[2:])
            cursor = conn.execute(
                "SELECT COUNT(*) FROM composite_groups WHERE group_id = ?",
                (group_num,)
            )
            size = cursor.fetchone()[0]
            if size > max_size:
                max_size = size
                target_group_id = group_num

    if target_group_id is None:
        target_group_id = get_next_group_id(conn)

    # Merge all into target
    for gid in group_ids:
        if gid.startswith('S_'):
            photo_id = gid[2:]
            # Check if already in target (shouldn't happen but be safe)
            cursor = conn.execute(
                "SELECT 1 FROM composite_groups WHERE photo_id = ?",
                (photo_id,)
            )
            if not cursor.fetchone():
                conn.execute(
                    "INSERT INTO composite_groups (photo_id, group_id) VALUES (?, ?)",
                    (photo_id, target_group_id)
                )
        elif gid.startswith('G_'):
            source_group_id = int(gid[2:])
            if source_group_id != target_group_id:
                conn.execute(
                    "UPDATE composite_groups SET group_id = ? WHERE group_id = ?",
                    (target_group_id, source_group_id)
                )

    conn.commit()
    return f"G_{target_group_id}"


# Cache for all computed dates (built at startup, invalidated on merge)
_cache = None  # Will be dict with 'dates_list', 'by_date', 'date_info'


def build_date_cache(conn) -> dict:
    """
    Build complete date cache at startup.

    Returns dict with:
    - dates_list: sorted list of unique dates
    - by_date: {date_str: [list of (group_id_str, photo_id, count, date_result)]}
    - date_info: {group_id_str: DateResult}
    """
    from collections import defaultdict

    by_date = defaultdict(list)
    date_info = {}

    # Pre-fetch all photo dimensions for quality calculation
    cursor = conn.execute("SELECT id, width, height FROM photos")
    photo_dims = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    # Get all composite groups with their photos
    cursor = conn.execute("""
        SELECT group_id, photo_id FROM composite_groups ORDER BY group_id
    """)
    group_photos = defaultdict(list)
    for row in cursor.fetchall():
        group_photos[row[0]].append(row[1])

    # Compute dates for groups
    for group_id, photo_ids in group_photos.items():
        result = derive_date_for_group(conn, group_id)
        group_id_str = f'G_{group_id}'
        date_info[group_id_str] = result
        date_str = get_date_for_display(result.date_value)
        rep_photo = photo_ids[0]
        w, h = photo_dims.get(rep_photo, (None, None))
        by_date[date_str].append({
            'group_id': group_id_str,
            'representative_photo_id': rep_photo,
            'photo_count': len(photo_ids),
            'date_value': result.date_value,
            'confidence': result.confidence,
            'quality': get_quality(w, h),
        })

    # Get singletons
    cursor = conn.execute("""
        SELECT kp.id
        FROM kept_photos kp
        LEFT JOIN composite_groups cg ON kp.id = cg.photo_id
        WHERE cg.photo_id IS NULL
    """)
    singleton_ids = [row[0] for row in cursor.fetchall()]

    # Compute dates for singletons
    for photo_id in singleton_ids:
        result = derive_date(conn, photo_id)
        group_id_str = f'S_{photo_id}'
        date_info[group_id_str] = result
        date_str = get_date_for_display(result.date_value)
        w, h = photo_dims.get(photo_id, (None, None))
        by_date[date_str].append({
            'group_id': group_id_str,
            'representative_photo_id': photo_id,
            'photo_count': 1,
            'date_value': result.date_value,
            'confidence': result.confidence,
            'quality': get_quality(w, h),
        })

    # Sort dates (unknown last)
    dates_list = sorted([d for d in by_date.keys() if d != "unknown"])
    if "unknown" in by_date:
        dates_list.append("unknown")

    # Sort items within each date
    for date_str in by_date:
        by_date[date_str].sort(key=lambda x: (x['date_value'] or '', x['group_id']))

    return {
        'dates_list': dates_list,
        'by_date': dict(by_date),
        'date_info': date_info,
    }


def get_cache(conn) -> dict:
    global _cache
    if _cache is None:
        _cache = build_date_cache(conn)
    return _cache


def invalidate_cache():
    global _cache
    _cache = None


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Date Browser - {{ current_date }}</title>
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
            gap: 20px;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid #333;
            flex-wrap: wrap;
        }

        .date-nav {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .nav-btn {
            padding: 8px 16px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            font-size: 14px;
        }
        .nav-btn:hover { background: #444; }
        .nav-btn:disabled { opacity: 0.4; cursor: not-allowed; }

        .current-date {
            font-size: 1.5em;
            font-weight: 600;
            min-width: 150px;
            text-align: center;
        }

        .date-info {
            color: #888;
            font-size: 0.9em;
        }

        .selection-info {
            padding: 6px 14px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #059669; }

        .merge-btn {
            padding: 8px 20px;
            background: #2563eb;
            border: none;
            border-radius: 5px;
            color: white;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            margin-left: auto;
        }
        .merge-btn:hover { background: #1d4ed8; }
        .merge-btn:disabled { opacity: 0.4; cursor: not-allowed; }

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

        .groups-container { flex: 1; overflow-y: auto; }
        .groups {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 8px;
            padding: 5px 0;
        }

        .group {
            background: #222;
            border-radius: 8px;
            padding: 6px;
            border: 3px solid transparent;
            cursor: pointer;
            transition: border-color 0.15s;
        }
        .group:hover { border-color: #444; }
        .group.selected { border-color: #059669; background: #1a2e1a; }

        .group-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
            font-size: 0.75em;
        }
        .group-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
            font-size: 0.75em;
        }
        .group-icons {
            display: flex;
            gap: 6px;
            align-items: center;
        }
        .stack-icon {
            font-size: 1.1em;
            opacity: 0.7;
        }
        .star-btn {
            background: #333;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 1.1em;
            padding: 3px 6px;
            opacity: 0.7;
            transition: opacity 0.15s, background 0.15s;
        }
        .star-btn:hover { opacity: 1; background: #444; }
        .star-btn.starred { opacity: 1; color: #fbbf24; background: #433; }
        .quality {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
        }
        .quality.high { background: #22c55e; }
        .quality.medium { background: #eab308; }
        .quality.low { background: #ef4444; }

        .group img {
            width: 100%;
            height: 220px;
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
        <a class="nav-btn" href="/" title="Back to calendar">📅</a>
        <div class="date-nav">
            <button class="nav-btn" onclick="prevDate()" {{ 'disabled' if not has_prev else '' }} title="Previous date (←)">← Prev</button>
            <div class="current-date">{{ current_date }}</div>
            <button class="nav-btn" onclick="nextDate()" {{ 'disabled' if not has_next else '' }} title="Next date (→)">Next →</button>
        </div>

        <div class="date-info">
            {{ date_index + 1 }} / {{ total_dates }} dates
        </div>

        <div class="selection-info" id="selectionInfo">
            0 selected
        </div>

        <button class="merge-btn" id="mergeBtn" onclick="mergeSelected()" disabled title="Merge selected (m)">
            Merge (m)
        </button>
        <a class="nav-btn" href="/favourites" title="Favourites" style="color: #fbbf24;">★</a>
    </div>

    <div class="help">
        <kbd>←</kbd> prev date &nbsp;
        <kbd>→</kbd> next date &nbsp;
        <kbd>m</kbd> merge selected &nbsp;
        <kbd>Esc</kbd> clear selection &nbsp;
        <kbd>dbl-click</kbd> view group
    </div>

    <div class="groups-container">
        {% if groups %}
        <div class="groups">
            {% for group in groups %}
            <div class="group"
                 data-group-id="{{ group.group_id }}"
                 onclick="toggleGroup(this)"
                 ondblclick="viewGroup(this)">
                <div class="group-header">
                    <div class="group-icons">
                        {% if group.group_id.startswith('G_') %}
                        <span class="stack-icon" title="Group ({{ group.photo_count }} photos)">&#x1F4DA;</span>
                        {% else %}
                        <button class="star-btn {{ 'starred' if group.representative_photo_id in favourites else '' }}"
                                onclick="event.stopPropagation(); toggleStar('{{ group.representative_photo_id }}', this)"
                                title="Add to favourites">★</button>
                        {% endif %}
                    </div>
                    <span class="quality {{ group.quality }}" title="{{ group.quality }} resolution"></span>
                </div>
                <img src="/image/{{ group.representative_photo_id }}" alt="">
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="empty-state">
            <h2>No photos on this date</h2>
        </div>
        {% endif %}
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const currentDate = {{ current_date | tojson }};
        const prevDateVal = {{ prev_date | tojson }};
        const nextDateVal = {{ next_date | tojson }};

        let selected = new Set();

        function toggleGroup(el) {
            const groupId = el.dataset.groupId;
            if (el.classList.contains('selected')) {
                el.classList.remove('selected');
                selected.delete(groupId);
            } else {
                el.classList.add('selected');
                selected.add(groupId);
            }
            updateSelectionInfo();
        }

        function viewGroup(el) {
            const groupId = el.dataset.groupId;
            window.location.href = '/group/' + encodeURIComponent(groupId);
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            info.textContent = `${selected.size} selected`;
            info.classList.toggle('has-selection', selected.size > 0);
            document.getElementById('mergeBtn').disabled = selected.size < 2;
        }

        function clearSelection() {
            document.querySelectorAll('.group.selected').forEach(el => {
                el.classList.remove('selected');
            });
            selected.clear();
            updateSelectionInfo();
        }

        function showToast(message) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show success';
            setTimeout(() => toast.classList.remove('show'), 2000);
        }

        function prevDate() {
            if (prevDateVal) {
                window.location.href = '/date/' + encodeURIComponent(prevDateVal);
            }
        }

        function nextDate() {
            if (nextDateVal) {
                window.location.href = '/date/' + encodeURIComponent(nextDateVal);
            }
        }

        function mergeSelected() {
            if (selected.size < 2) return;

            const groupIds = Array.from(selected);

            fetch('/api/merge', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ group_ids: groupIds })
            })
            .then(r => r.json())
            .then(data => {
                showToast(`Merged ${data.merged_count} groups`);
                // Reload same date to see updated groups
                setTimeout(() => {
                    window.location.href = '/date/' + encodeURIComponent(currentDate);
                }, 400);
            });
        }

        function toggleStar(photoId, btn) {
            const isStarred = btn.classList.contains('starred');
            fetch('/api/favourite', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ photo_id: photoId, starred: !isStarred })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    btn.classList.toggle('starred', data.starred);
                }
            });
        }

        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT') return;

            switch(e.key) {
                case 'ArrowLeft':
                    if (prevDateVal) {
                        window.location.href = '/date/' + encodeURIComponent(prevDateVal);
                    }
                    break;
                case 'ArrowRight':
                    if (nextDateVal) {
                        window.location.href = '/date/' + encodeURIComponent(nextDateVal);
                    }
                    break;
                case 'm':
                    mergeSelected();
                    break;
                case 'Escape':
                    clearSelection();
                    break;
            }
        });
    </script>
</body>
</html>
"""


CALENDAR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Photo Calendar - {{ year }}</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: system-ui, sans-serif;
            padding: 20px;
            background: #1a1a1a;
            color: #eee;
            margin: 0;
        }
        .header {
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 20px;
        }
        .nav-btn {
            padding: 8px 16px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            text-decoration: none;
            font-size: 14px;
        }
        .nav-btn:hover { background: #444; }
        .nav-btn:disabled { opacity: 0.4; cursor: not-allowed; }
        h1 { margin: 0; font-size: 1.8em; font-weight: 600; }
        .fav-link {
            margin-left: auto;
            color: #fbbf24;
            text-decoration: none;
            font-size: 1.2em;
        }
        .fav-link:hover { opacity: 0.8; }
        .year-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 12px;
            max-width: 900px;
        }
        @media (max-width: 900px) {
            .year-grid { grid-template-columns: repeat(3, 1fr); }
        }
        @media (max-width: 600px) {
            .year-grid { grid-template-columns: repeat(2, 1fr); }
        }
        .month {
            background: #222;
            border-radius: 6px;
            padding: 8px;
        }
        .month-name {
            font-weight: 600;
            font-size: 0.85em;
            margin-bottom: 6px;
            text-align: center;
        }
        .days-header {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 1px;
            margin-bottom: 2px;
        }
        .day-header {
            text-align: center;
            font-size: 0.6em;
            color: #666;
        }
        .days {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 1px;
        }
        .day {
            aspect-ratio: 1;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.65em;
            border-radius: 3px;
            color: #666;
            text-decoration: none;
        }
        .day.has-photos {
            background: #059669;
            color: #fff;
            cursor: pointer;
            font-weight: 500;
        }
        .day.has-photos:hover {
            background: #047857;
        }
    </style>
</head>
<body>
    <div class="header">
        {% if has_prev %}
        <a class="nav-btn" href="/calendar/{{ year - 1 }}">← {{ year - 1 }}</a>
        {% else %}
        <button class="nav-btn" disabled>← Prev</button>
        {% endif %}
        <h1>{{ year }}</h1>
        {% if has_next %}
        <a class="nav-btn" href="/calendar/{{ year + 1 }}">{{ year + 1 }} →</a>
        {% else %}
        <button class="nav-btn" disabled>Next →</button>
        {% endif %}
        <a class="fav-link" href="/favourites" title="Favourites">★ Favourites</a>
    </div>
    <div class="year-grid">
        {% for month in months %}
        <div class="month">
            <div class="month-name">{{ month.name }}</div>
            <div class="days-header">
                <div class="day-header">M</div>
                <div class="day-header">T</div>
                <div class="day-header">W</div>
                <div class="day-header">T</div>
                <div class="day-header">F</div>
                <div class="day-header">S</div>
                <div class="day-header">S</div>
            </div>
            <div class="days">
                {% for day in month.days %}
                {% if day.num == 0 %}
                <div class="day"></div>
                {% elif day.date in photo_dates %}
                <a class="day has-photos" href="/date/{{ day.date }}">{{ day.num }}</a>
                {% else %}
                <div class="day">{{ day.num }}</div>
                {% endif %}
                {% endfor %}
            </div>
        </div>
        {% endfor %}
    </div>
</body>
</html>
"""


def build_calendar_data(year: int) -> list[dict]:
    """Build calendar data for a year - 12 months with day grids."""
    import calendar
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    months = []
    for month_num in range(1, 13):
        cal = calendar.Calendar(firstweekday=0)  # Monday first
        days = []
        for day in cal.itermonthdays(year, month_num):
            if day == 0:
                days.append({'num': 0, 'date': None})
            else:
                date_str = f"{year}-{month_num:02d}-{day:02d}"
                days.append({'num': day, 'date': date_str})
        months.append({'name': month_names[month_num - 1], 'days': days})
    return months


@app.route('/')
def index():
    """Redirect to calendar for earliest year with photos."""
    conn = get_connection()
    dates = get_all_dates_with_groups(conn)
    conn.close()

    if dates:
        # Find earliest year
        for d in dates:
            if d != 'unknown' and len(d) >= 4:
                year = int(d[:4])
                return redirect(f'/calendar/{year}')
    return redirect('/calendar/2020')


@app.route('/calendar/<int:year>')
def show_calendar(year: int):
    """Show calendar view for a year."""
    conn = get_connection()
    dates = get_all_dates_with_groups(conn)
    conn.close()

    # Get set of dates with photos (just YYYY-MM-DD format)
    photo_dates = {d for d in dates if d != 'unknown' and len(d) == 10}

    # Find year range
    years_with_photos = set()
    for d in dates:
        if d != 'unknown' and len(d) >= 4:
            try:
                years_with_photos.add(int(d[:4]))
            except ValueError:
                pass

    min_year = min(years_with_photos) if years_with_photos else year
    max_year = max(years_with_photos) if years_with_photos else year

    months = build_calendar_data(year)

    return render_template_string(
        CALENDAR_TEMPLATE,
        year=year,
        months=months,
        photo_dates=photo_dates,
        has_prev=year > min_year,
        has_next=year < max_year,
    )


@app.route('/date/<path:date>')
def show_date(date: str):
    """Show groups for a specific date."""
    conn = get_connection()
    dates = get_all_dates_with_groups(conn)

    if date not in dates:
        conn.close()
        return f"Date not found: {date}", 404

    date_index = dates.index(date)
    groups = get_groups_for_date(conn, date)
    favourites = get_favourites(conn)
    conn.close()

    # Navigation
    has_prev = date_index > 0
    has_next = date_index < len(dates) - 1
    prev_date = dates[date_index - 1] if has_prev else None
    next_date = dates[date_index + 1] if has_next else None

    # Date range
    if dates:
        first = dates[0] if dates[0] != "unknown" else (dates[1] if len(dates) > 1 else "?")
        last = dates[-1] if dates[-1] != "unknown" else (dates[-2] if len(dates) > 1 else "?")
        date_range = f"{first} to {last}"
    else:
        date_range = "?"

    return render_template_string(
        TEMPLATE,
        current_date=date,
        date_index=date_index,
        total_dates=len(dates),
        date_range=date_range,
        groups=groups,
        favourites=favourites,
        has_prev=has_prev,
        has_next=has_next,
        prev_date=prev_date,
        next_date=next_date,
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


@app.route('/api/split', methods=['POST'])
def api_split():
    """Split selected photos from a group into a new group (or singleton)."""
    data = request.json
    group_id = data.get('group_id')
    photo_ids = data.get('photo_ids', [])

    if not group_id or not photo_ids:
        return jsonify({'success': False, 'error': 'Missing group_id or photo_ids'})

    if not group_id.startswith('G_'):
        return jsonify({'success': False, 'error': 'Can only split composite groups'})

    gid = int(group_id[2:])

    conn = get_connection()

    # Get the date for this group BEFORE splitting (for redirect)
    cache = get_cache(conn)
    date_info = cache['date_info'].get(group_id)
    back_date = get_date_for_display(date_info.date_value) if date_info else 'unknown'

    # Get current group size
    cursor = conn.execute("SELECT COUNT(*) FROM composite_groups WHERE group_id = ?", (gid,))
    group_size = cursor.fetchone()[0]

    if len(photo_ids) >= group_size:
        conn.close()
        return jsonify({'success': False, 'error': 'Cannot split all photos from group'})

    if len(photo_ids) == 1:
        # Single photo - make it a singleton (remove from composite_groups)
        conn.execute("DELETE FROM composite_groups WHERE photo_id = ?", (photo_ids[0],))
    else:
        # Multiple photos - create new group
        cursor = conn.execute("SELECT MAX(group_id) FROM composite_groups")
        max_id = cursor.fetchone()[0] or 0
        new_group_id = max_id + 1

        # Move photos to new group
        placeholders = ",".join("?" * len(photo_ids))
        conn.execute(
            f"UPDATE composite_groups SET group_id = ? WHERE photo_id IN ({placeholders})",
            [new_group_id] + photo_ids
        )

    # Check if original group now has only 1 photo - if so, make it a singleton too
    cursor = conn.execute("SELECT COUNT(*), photo_id FROM composite_groups WHERE group_id = ?", (gid,))
    row = cursor.fetchone()
    group_dissolved = row[0] == 1
    if group_dissolved:
        # Only 1 photo left - convert to singleton
        conn.execute("DELETE FROM composite_groups WHERE group_id = ?", (gid,))

    conn.commit()
    conn.close()

    # Invalidate cache
    invalidate_cache()

    return jsonify({
        'success': True,
        'split_count': len(photo_ids),
        'group_dissolved': group_dissolved,
        'redirect_date': back_date,
    })


@app.route('/api/favourite', methods=['POST'])
def api_favourite():
    """Add or remove a photo from favourites."""
    data = request.json
    photo_id = data.get('photo_id')
    starred = data.get('starred', True)

    if not photo_id:
        return jsonify({'success': False, 'error': 'Missing photo_id'})

    conn = get_connection()

    if starred:
        conn.execute(
            "INSERT OR IGNORE INTO favourite_photos (photo_id) VALUES (?)",
            (photo_id,)
        )
    else:
        conn.execute("DELETE FROM favourite_photos WHERE photo_id = ?", (photo_id,))

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'starred': starred})


def get_favourites(conn) -> set:
    """Get all favourited photo IDs."""
    cursor = conn.execute("SELECT photo_id FROM favourite_photos")
    return {row[0] for row in cursor.fetchall()}


@app.route('/api/merge', methods=['POST'])
def api_merge():
    """Merge selected groups."""
    data = request.json
    group_ids = data.get('group_ids', [])

    if len(group_ids) < 2:
        return jsonify({'success': False, 'error': 'Need at least 2 groups'})

    conn = get_connection()
    result = merge_groups(conn, group_ids)
    conn.close()

    # Invalidate cache since groups changed
    invalidate_cache()

    return jsonify({
        'success': True,
        'merged_count': len(group_ids),
        'result_group': result,
    })


GROUP_DETAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Group {{ group_id }}</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: system-ui, sans-serif;
            padding: 20px;
            background: #1a1a1a;
            color: #eee;
            margin: 0;
        }
        .header {
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .back-btn, .split-btn {
            padding: 8px 16px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            text-decoration: none;
            font-size: 14px;
        }
        .back-btn:hover, .split-btn:hover { background: #444; }
        .split-btn { background: #2563eb; margin-left: auto; }
        .split-btn:hover { background: #1d4ed8; }
        .split-btn:disabled { opacity: 0.4; cursor: not-allowed; }
        h1 { margin: 0; font-size: 1.4em; }
        .selection-info {
            padding: 6px 14px;
            background: #333;
            border-radius: 5px;
            font-size: 0.9em;
        }
        .selection-info.has-selection { background: #059669; }
        .photos {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 12px;
        }
        .photo {
            cursor: pointer;
            border: 3px solid transparent;
            border-radius: 8px;
            padding: 4px;
        }
        .photo:hover { border-color: #444; }
        .photo.selected { border-color: #059669; background: #1a2e1a; }
        .photo img {
            width: 100%;
            height: 280px;
            object-fit: contain;
            background: #111;
            border-radius: 6px;
        }
        .photo-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
        }
        .star-btn {
            background: #333;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 1.3em;
            padding: 4px 8px;
            opacity: 0.7;
            transition: opacity 0.15s, background 0.15s;
        }
        .star-btn:hover { opacity: 1; background: #444; }
        .star-btn.starred { opacity: 1; color: #fbbf24; background: #433; }
        .quality {
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }
        .quality.high { background: #22c55e; }
        .quality.medium { background: #eab308; }
        .quality.low { background: #ef4444; }
        .photo-info {
            font-size: 0.8em;
            color: #888;
            margin-top: 4px;
            word-break: break-all;
        }
        .help {
            color: #666;
            font-size: 0.75em;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <div class="header">
        <a class="back-btn" href="/date/{{ back_date }}">← Back</a>
        <h1>Group {{ group_id }} ({{ photos | length }} photos)</h1>
        <div class="selection-info" id="selectionInfo">0 selected</div>
        <button class="split-btn" id="splitBtn" onclick="splitSelected()" disabled>Split Selected</button>
    </div>
    <div class="help">Click photos to select for splitting. Click ★ to add to favourites.</div>
    <div class="photos">
        {% for photo in photos %}
        <div class="photo" data-photo-id="{{ photo.id }}" onclick="togglePhoto(this)">
            <div class="photo-header">
                <span class="quality {{ photo.quality }}" title="{{ photo.quality }} resolution"></span>
                <button class="star-btn {{ 'starred' if photo.id in favourites else '' }}"
                        onclick="event.stopPropagation(); toggleStar('{{ photo.id }}', this)"
                        title="Add to favourites">★</button>
            </div>
            <img src="/image/{{ photo.id }}" alt="">
            <div class="photo-info">{{ photo.path }}</div>
        </div>
        {% endfor %}
    </div>
    <script>
        const groupId = {{ group_id | tojson }};
        const totalPhotos = {{ photos | length }};
        let selected = new Set();

        function togglePhoto(el) {
            const photoId = el.dataset.photoId;
            if (el.classList.contains('selected')) {
                el.classList.remove('selected');
                selected.delete(photoId);
            } else {
                el.classList.add('selected');
                selected.add(photoId);
            }
            updateSelectionInfo();
        }

        function updateSelectionInfo() {
            const info = document.getElementById('selectionInfo');
            info.textContent = `${selected.size} selected`;
            info.classList.toggle('has-selection', selected.size > 0);
            // Can split if at least 1 selected and not all selected
            document.getElementById('splitBtn').disabled = selected.size === 0 || selected.size === totalPhotos;
        }

        function splitSelected() {
            if (selected.size === 0 || selected.size === totalPhotos) return;

            const photoIds = Array.from(selected);

            fetch('/api/split', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ group_id: groupId, photo_ids: photoIds })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    if (data.group_dissolved) {
                        // Group no longer exists, go back to date
                        window.location.href = '/date/' + encodeURIComponent(data.redirect_date);
                    } else {
                        // Reload to show updated group
                        window.location.reload();
                    }
                } else {
                    alert('Split failed: ' + data.error);
                }
            });
        }

        function toggleStar(photoId, btn) {
            const isStarred = btn.classList.contains('starred');
            fetch('/api/favourite', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ photo_id: photoId, starred: !isStarred })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    btn.classList.toggle('starred', data.starred);
                }
            });
        }
    </script>
</body>
</html>
"""


FAVOURITES_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Favourites</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: system-ui, sans-serif;
            padding: 20px;
            background: #1a1a1a;
            color: #eee;
            margin: 0;
        }
        .header {
            display: flex;
            align-items: center;
            gap: 20px;
            margin-bottom: 20px;
        }
        .back-btn {
            padding: 8px 16px;
            background: #333;
            border: none;
            border-radius: 5px;
            color: #eee;
            cursor: pointer;
            text-decoration: none;
        }
        .back-btn:hover { background: #444; }
        h1 { margin: 0; font-size: 1.4em; }
        .photos {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 12px;
        }
        .photo {
            background: #222;
            border-radius: 8px;
            padding: 6px;
        }
        .photo-header {
            display: flex;
            justify-content: flex-end;
            margin-bottom: 4px;
        }
        .star-btn {
            background: none;
            border: none;
            cursor: pointer;
            font-size: 1.3em;
            padding: 2px 6px;
            color: #fbbf24;
        }
        .star-btn:hover { opacity: 0.7; }
        .photo img {
            width: 100%;
            height: 280px;
            object-fit: contain;
            background: #111;
            border-radius: 6px;
        }
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="header">
        <a class="back-btn" href="/">← Calendar</a>
        <h1>Favourites ({{ photos | length }})</h1>
    </div>
    {% if photos %}
    <div class="photos">
        {% for photo in photos %}
        <div class="photo" data-photo-id="{{ photo.id }}">
            <div class="photo-header">
                <button class="star-btn" onclick="removeStar('{{ photo.id }}', this)" title="Remove from favourites">&#x2605;</button>
            </div>
            <img src="/image/{{ photo.id }}" alt="">
        </div>
        {% endfor %}
    </div>
    {% else %}
    <div class="empty-state">
        <h2>No favourites yet</h2>
        <p>Click the ★ on photos to add them here</p>
    </div>
    {% endif %}
    <script>
        function removeStar(photoId, btn) {
            fetch('/api/favourite', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ photo_id: photoId, starred: false })
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    btn.closest('.photo').remove();
                }
            });
        }
    </script>
</body>
</html>
"""


@app.route('/favourites')
def show_favourites():
    """Show all favourited photos."""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT f.photo_id
        FROM favourite_photos f
        JOIN photos p ON f.photo_id = p.id
        ORDER BY f.created_at DESC
    """)
    photos = [{'id': row[0]} for row in cursor.fetchall()]
    conn.close()

    return render_template_string(FAVOURITES_TEMPLATE, photos=photos)


@app.route('/group/<group_id>')
def show_group(group_id: str):
    """Show all photos in a group."""
    conn = get_connection()

    if group_id.startswith('G_'):
        # Composite group
        gid = int(group_id[2:])
        cursor = conn.execute("""
            SELECT cg.photo_id, pp.source_path, p.width, p.height
            FROM composite_groups cg
            JOIN photo_paths pp ON cg.photo_id = pp.photo_id
            JOIN photos p ON cg.photo_id = p.id
            WHERE cg.group_id = ?
            GROUP BY cg.photo_id
            ORDER BY pp.source_path
        """, (gid,))
    elif group_id.startswith('S_'):
        # Singleton
        photo_id = group_id[2:]
        cursor = conn.execute("""
            SELECT pp.photo_id, pp.source_path, p.width, p.height
            FROM photo_paths pp
            JOIN photos p ON pp.photo_id = p.id
            WHERE pp.photo_id = ?
            LIMIT 1
        """, (photo_id,))
    else:
        conn.close()
        return "Invalid group ID", 400

    photos = [{'id': row[0], 'path': row[1], 'quality': get_quality(row[2], row[3])} for row in cursor.fetchall()]

    # Get the date for back link
    cache = get_cache(conn)
    date_info = cache['date_info'].get(group_id)
    back_date = get_date_for_display(date_info.date_value) if date_info else 'unknown'

    favourites = get_favourites(conn)
    conn.close()

    return render_template_string(
        GROUP_DETAIL_TEMPLATE,
        group_id=group_id,
        photos=photos,
        favourites=favourites,
        back_date=back_date,
    )


if __name__ == '__main__':
    conn = get_connection()

    # Check required tables exist
    cursor = conn.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='table' AND name='composite_groups'
    """)
    if cursor.fetchone()[0] == 0:
        print("ERROR: composite_groups table not found!")
        print("Run pipeline2 stage 3 first.")
        sys.exit(1)

    cursor = conn.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='view' AND name='kept_photos'
    """)
    if cursor.fetchone()[0] == 0:
        print("ERROR: kept_photos view not found!")
        sys.exit(1)

    # Create favourites table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS favourite_photos (
            photo_id TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    print("Building date index (this may take a moment)...")
    dates = get_all_dates_with_groups(conn)

    cursor = conn.execute("SELECT COUNT(DISTINCT group_id) FROM composite_groups")
    group_count = cursor.fetchone()[0]

    cursor = conn.execute("""
        SELECT COUNT(*)
        FROM kept_photos kp
        LEFT JOIN composite_groups cg ON kp.id = cg.photo_id
        WHERE cg.photo_id IS NULL
    """)
    singleton_count = cursor.fetchone()[0]

    conn.close()

    print(f"Database: {DB_PATH}")
    print(f"Files: {FILES_DIR}")
    print(f"Dates with photos: {len(dates)}")
    print(f"Composite groups: {group_count}")
    print(f"Singletons: {singleton_count}")
    if dates:
        print(f"Date range: {dates[0]} to {dates[-1]}")
    print()
    print("Starting server at http://localhost:5006")
    print()
    print("Keyboard shortcuts:")
    print("  ← / → - Previous / next date")
    print("  m - Merge selected groups")
    print("  Escape - Clear selection")
    print()
    app.run(debug=True, port=5006)
