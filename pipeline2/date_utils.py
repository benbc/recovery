"""Date derivation utilities for groups and singletons.

Implements the date selection algorithm from PLAN.md:
1. Gather all date sources from all group members (including rejected ones)
2. Filter out unusable dates
3. Select by confidence tier (high > medium > low), earliest within tier
4. Flag conflicts if high-confidence dates disagree by >1 year

Date sources are computed dynamically from photos and photo_paths tables,
not from a pre-populated photo_date_sources table.
"""

import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add project root for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils.metadata import (
    get_exif_date_confidence,
    parse_date_from_filename,
    parse_date_from_path,
)


# Confidence tiers in priority order
CONFIDENCE_TIERS = ["high", "medium", "low"]


@dataclass
class DateResult:
    """Result of date derivation for a photo or group."""

    date_value: Optional[str]  # ISO date/datetime or partial (e.g., "2004-12")
    confidence: Optional[str]  # 'high', 'medium', 'low'
    source_type: Optional[str]  # Which source provided the date
    has_conflict: bool  # True if high-confidence dates disagree by >1 year
    conflict_dates: list[str]  # The conflicting dates if has_conflict


@dataclass
class GroupDateSources:
    """All usable date sources for a group, organized by confidence tier."""

    high: list[tuple[str, str]]  # (date_value, source_type)
    medium: list[tuple[str, str]]
    low: list[tuple[str, str]]


def _parse_year(date_value: str) -> Optional[int]:
    """Extract year from a date value (full or partial)."""
    if not date_value:
        return None
    try:
        # Handle full ISO datetime or date
        if "T" in date_value:
            return datetime.fromisoformat(date_value).year
        # Handle YYYY-MM-DD
        if len(date_value) >= 4:
            return int(date_value[:4])
    except (ValueError, TypeError):
        pass
    return None


def _date_sort_key(date_value: str) -> str:
    """
    Return a sort key that orders dates chronologically.

    Partial dates sort before more specific dates in the same period.
    E.g., "2004" < "2004-06" < "2004-06-15" < "2004-06-15T10:00:00"
    """
    # Pad partial dates to sort correctly
    # "2004" -> "2004-00-00T00:00:00"
    # "2004-12" -> "2004-12-00T00:00:00"
    if not date_value:
        return "9999-99-99T99:99:99"  # Sort empty last

    parts = date_value.replace("T", "-").split("-")
    while len(parts) < 6:
        parts.append("00")

    return "-".join(parts[:3]) + "T" + ":".join(parts[3:6])


def _prefer_specific_dates(dates: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Filter out partial dates when a more specific consistent date exists.

    If we have both "2004" and "2004-06-15", remove "2004" since the more
    specific date is consistent with it and provides better information.

    If we have "2004" and "2005-03-10", keep both (different years).
    """
    if len(dates) <= 1:
        return dates

    date_values = [d[0] for d in dates]
    result = []

    for date_value, source_type in dates:
        # Check if there's a more specific date that this one is a prefix of
        is_dominated = False
        for other in date_values:
            if (
                other != date_value
                and len(other) > len(date_value)
                and other.startswith(date_value)
            ):
                # This date is a prefix of a more specific date
                is_dominated = True
                break

        if not is_dominated:
            result.append((date_value, source_type))

    return result


def _expand_to_primary_groups(conn: sqlite3.Connection, photo_ids: set[str]) -> set[str]:
    """
    Expand a set of photo IDs to include all members of their primary groups.

    This includes rejected members, since they may have EXIF data that
    kept members lost (e.g., IPHOTO_COPY rejection).
    """
    result = set(photo_ids)

    for photo_id in photo_ids:
        cursor = conn.execute(
            "SELECT group_id FROM duplicate_groups WHERE photo_id = ?",
            (photo_id,),
        )
        row = cursor.fetchone()
        if row:
            primary_group_id = row[0]
            cursor = conn.execute(
                "SELECT photo_id FROM duplicate_groups WHERE group_id = ?",
                (primary_group_id,),
            )
            for r in cursor.fetchall():
                result.add(r[0])

    return result


def _get_composite_group_photo_ids(conn: sqlite3.Connection, group_id: int) -> set[str]:
    """
    Get all photo IDs for a composite group, including rejected primary group members.
    """
    cursor = conn.execute(
        "SELECT photo_id FROM composite_groups WHERE group_id = ?",
        (group_id,),
    )
    group_photos = {row[0] for row in cursor.fetchall()}

    return _expand_to_primary_groups(conn, group_photos)


def _fetch_date_sources(conn: sqlite3.Connection, photo_ids: set[str]) -> GroupDateSources:
    """
    Dynamically compute date sources for a set of photo IDs from photos and photo_paths.

    Sources:
    - date_taken from photos table (confidence based on date_source field)
    - Filename patterns from photo_paths (dynamically parsed)
    - Path patterns from photo_paths (dynamically parsed)
    """
    if not photo_ids:
        return GroupDateSources(high=[], medium=[], low=[])

    by_tier: dict[str, list[tuple[str, str]]] = {
        "high": [],
        "medium": [],
        "low": [],
    }

    placeholders = ",".join("?" * len(photo_ids))

    # Get stored date from photos table
    cursor = conn.execute(
        f"""
        SELECT id, date_taken, date_source, has_exif
        FROM photos
        WHERE id IN ({placeholders})
        """,
        list(photo_ids),
    )

    for row in cursor.fetchall():
        date_taken = row[1]
        date_source = row[2]
        has_exif = row[3]

        if date_taken:
            # Determine confidence based on date_source
            # Note: We treat EXIF as medium because we can't distinguish
            # camera-originated EXIF (reliable) from software-added EXIF (less reliable)
            if date_source == "exif":
                by_tier["medium"].append((date_taken, "exif"))
            elif date_source == "filename":
                by_tier["medium"].append((date_taken, "filename"))
            elif date_source == "mtime":
                by_tier["low"].append((date_taken, "mtime"))
            else:
                # Unknown source, treat as medium
                by_tier["medium"].append((date_taken, date_source or "unknown"))

    # Get path-based dates from photo_paths table (dynamically parsed)
    cursor = conn.execute(
        f"""
        SELECT photo_id, source_path, filename
        FROM photo_paths
        WHERE photo_id IN ({placeholders})
        """,
        list(photo_ids),
    )

    for row in cursor.fetchall():
        source_path = row[1]
        filename = row[2]

        # Filename date (may find dates not captured in original scan)
        filename_result = parse_date_from_filename(filename)
        if filename_result:
            date_value, _ = filename_result
            by_tier["medium"].append((date_value, "filename"))

        # Path date (semantic patterns like "Xmas 2004", "April 2010")
        path_result = parse_date_from_path(source_path)
        if path_result:
            date_value, confidence, _ = path_result
            if confidence in by_tier:
                by_tier[confidence].append((date_value, "path_semantic"))

    return GroupDateSources(
        high=by_tier["high"],
        medium=by_tier["medium"],
        low=by_tier["low"],
    )


def _derive_date_from_sources(sources: GroupDateSources) -> DateResult:
    """
    Select the best date from grouped date sources.

    Picks earliest date from highest available confidence tier,
    preferring specific dates over partial ones when consistent.
    """
    # Check for conflicts in high-confidence dates
    has_conflict = False
    conflict_dates: list[str] = []

    if len(sources.high) >= 2:
        years = set()
        for date_value, _ in sources.high:
            year = _parse_year(date_value)
            if year:
                years.add(year)

        if years and (max(years) - min(years)) > 1:
            has_conflict = True
            conflict_dates = sorted(set(d for d, _ in sources.high))

    # Select earliest from highest available tier
    for tier in CONFIDENCE_TIERS:
        dates = getattr(sources, tier)
        if dates:
            # Prefer specific dates over partial ones when consistent
            dates = _prefer_specific_dates(list(dates))
            # Sort by date value to get earliest
            dates.sort(key=lambda x: _date_sort_key(x[0]))
            earliest_date, source_type = dates[0]

            return DateResult(
                date_value=earliest_date,
                confidence=tier,
                source_type=source_type,
                has_conflict=has_conflict,
                conflict_dates=conflict_dates,
            )

    # No usable dates found
    return DateResult(
        date_value=None,
        confidence=None,
        source_type=None,
        has_conflict=False,
        conflict_dates=[],
    )


# --- Public API ---


def get_all_photo_ids_for_group(
    conn: sqlite3.Connection, photo_id: str
) -> set[str]:
    """
    Get all photo IDs that should contribute date sources for a given photo.

    For a photo in a composite group: returns ALL photos in the original primary
    group (including rejected ones), plus any photos linked via the composite group.

    For a singleton: returns just the photo itself.

    The key insight from PLAN.md: "A photo rejected by IPHOTO_COPY may have the
    only EXIF date in the group" - so we include rejected group members.
    """
    result = {photo_id}

    # Check if photo is in a composite group
    cursor = conn.execute(
        "SELECT group_id FROM composite_groups WHERE photo_id = ?",
        (photo_id,),
    )
    row = cursor.fetchone()

    if row:
        composite_group_id = row[0]

        # Get all photos in the composite group
        cursor = conn.execute(
            "SELECT photo_id FROM composite_groups WHERE group_id = ?",
            (composite_group_id,),
        )
        for r in cursor.fetchall():
            result.add(r[0])

    # Expand to include all primary group members (including rejected)
    result = _expand_to_primary_groups(conn, result)

    return result


def get_group_date_sources(conn: sqlite3.Connection, group_id: int) -> GroupDateSources:
    """
    Get all usable date sources for a composite group, organized by tier.

    Useful for reviewing what dates are available before selection.

    Args:
        conn: Database connection
        group_id: The composite group ID

    Returns:
        GroupDateSources with dates organized by confidence tier
    """
    photo_ids = _get_composite_group_photo_ids(conn, group_id)
    return _fetch_date_sources(conn, photo_ids)


def derive_date_for_group(conn: sqlite3.Connection, group_id: int) -> DateResult:
    """
    Derive the best date for a composite group.

    Gathers date sources from all photos in the group, plus any rejected
    members from their original primary groups.

    Args:
        conn: Database connection
        group_id: The composite group ID

    Returns:
        DateResult with the derived date, confidence, source type, and conflict info
    """
    sources = get_group_date_sources(conn, group_id)
    return _derive_date_from_sources(sources)


def derive_date(conn: sqlite3.Connection, photo_id: str) -> DateResult:
    """
    Derive the best date for a photo or group.

    For grouped photos, gathers date sources from all group members (including
    rejected ones), then selects the earliest date from the highest confidence tier.

    Args:
        conn: Database connection
        photo_id: The photo ID to derive a date for

    Returns:
        DateResult with the derived date, confidence, source type, and conflict info
    """
    photo_ids = get_all_photo_ids_for_group(conn, photo_id)
    sources = _fetch_date_sources(conn, photo_ids)
    return _derive_date_from_sources(sources)


def derive_date_for_photo_ids(
    conn: sqlite3.Connection, photo_ids: set[str]
) -> DateResult:
    """
    Derive the best date for an explicit set of photo IDs.

    This is useful when you already know the set of photos to consider
    (e.g., from a manual group adjustment).

    Args:
        conn: Database connection
        photo_ids: Set of photo IDs to gather dates from

    Returns:
        DateResult with the derived date, confidence, source type, and conflict info
    """
    sources = _fetch_date_sources(conn, photo_ids)
    return _derive_date_from_sources(sources)
