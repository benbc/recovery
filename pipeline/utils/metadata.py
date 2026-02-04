"""EXIF extraction and date parsing utilities."""

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import piexif
from PIL import Image


# Known suspect date patterns (camera defaults, etc.)
SUSPECT_DATES = [
    ("2012-12-31", "23:00:00"),  # Nikon COOLPIX default
]


def _decode_exif_string(value) -> Optional[str]:
    """Decode an EXIF value to string."""
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    return value.strip() if value else None


def _parse_exif_datetime(value) -> Optional[str]:
    """Parse EXIF datetime string to ISO format, or None if invalid."""
    s = _decode_exif_string(value)
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y:%m:%d %H:%M:%S")
        return dt.isoformat()
    except ValueError:
        return None


def extract_exif(file_path: Path) -> dict:
    """
    Extract EXIF data from an image file.

    Returns a dict with:
    - exif_make: str or None
    - exif_model: str or None
    - exif_software: str or None
    - exif_datetime: str (ISO format) or None - 0th IFD DateTime (modification)
    - exif_datetime_original: str (ISO format) or None - when photo was taken
    - exif_datetime_digitized: str (ISO format) or None - when digitized
    """
    result = {
        "exif_make": None,
        "exif_model": None,
        "exif_software": None,
        "exif_datetime": None,
        "exif_datetime_original": None,
        "exif_datetime_digitized": None,
    }

    try:
        exif_dict = piexif.load(str(file_path))

        # 0th IFD fields
        ifd_0th = exif_dict.get("0th", {})
        result["exif_make"] = _decode_exif_string(ifd_0th.get(piexif.ImageIFD.Make))
        result["exif_model"] = _decode_exif_string(ifd_0th.get(piexif.ImageIFD.Model))
        result["exif_software"] = _decode_exif_string(ifd_0th.get(piexif.ImageIFD.Software))
        result["exif_datetime"] = _parse_exif_datetime(ifd_0th.get(piexif.ImageIFD.DateTime))

        # Exif IFD fields
        ifd_exif = exif_dict.get("Exif", {})
        result["exif_datetime_original"] = _parse_exif_datetime(
            ifd_exif.get(piexif.ExifIFD.DateTimeOriginal)
        )
        result["exif_datetime_digitized"] = _parse_exif_datetime(
            ifd_exif.get(piexif.ExifIFD.DateTimeDigitized)
        )

    except Exception:
        pass

    return result


def extract_dimensions(file_path: Path) -> tuple[Optional[int], Optional[int]]:
    """Extract image dimensions without loading the full image."""
    try:
        with Image.open(file_path) as img:
            return img.size
    except Exception:
        return None, None


def get_exif_date_confidence(exif_data: dict, date_value: str) -> str:
    """
    Determine confidence level for an EXIF date.

    Returns: 'high', 'medium', 'low', 'suspect', or 'unusable'
    """
    if not date_value:
        return "unusable"

    # Check for known suspect patterns
    for suspect_date, suspect_time in SUSPECT_DATES:
        if date_value.startswith(suspect_date) and suspect_time in date_value:
            return "unusable"

    # Check for obviously wrong dates
    try:
        dt = datetime.fromisoformat(date_value)
        if dt.year < 1990 or dt.year > 2030:
            return "suspect"
    except ValueError:
        return "suspect"

    has_make_model = bool(exif_data.get("exif_make") or exif_data.get("exif_model"))
    has_software = bool(exif_data.get("exif_software"))

    if has_make_model:
        # Camera-originated EXIF
        return "high"
    elif has_software:
        # Software-added EXIF (no camera info)
        return "low"
    else:
        # Unknown origin
        return "medium"


def parse_date_from_filename(filename: str) -> Optional[tuple[str, str]]:
    """
    Extract date from common filename patterns.

    Returns (iso_date, raw_match) or None.

    Supports patterns like:
    - IMG_20231225_123456.jpg
    - 20231225_123456.jpg
    - 2023-12-25_anything.jpg
    - DSC_20231225.jpg
    - Screen Shot 2023-05-14 at 09.48.46.png
    """
    patterns = [
        # YYYYMMDD_HHMMSS or YYYYMMDD-HHMMSS
        (r"(\d{4})(\d{2})(\d{2})[_-](\d{2})(\d{2})(\d{2})", True),
        # YYYY-MM-DD_HH.MM.SS (macOS screenshot)
        (r"(\d{4})-(\d{2})-(\d{2})[_ ](?:at )?(\d{2})\.(\d{2})\.(\d{2})", True),
        # YYYY-MM-DD_HHMMSS
        (r"(\d{4})-(\d{2})-(\d{2})[_-](\d{2})(\d{2})(\d{2})", True),
        # YYYYMMDD alone (must not be followed by more digits)
        (r"(\d{4})(\d{2})(\d{2})(?!\d)", False),
        # YYYY-MM-DD alone
        (r"(\d{4})-(\d{2})-(\d{2})(?!\d)", False),
    ]

    for pattern, has_time in patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                groups = match.groups()
                year, month, day = int(groups[0]), int(groups[1]), int(groups[2])

                # Sanity check
                if not (1990 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31):
                    continue

                if has_time and len(groups) >= 6:
                    hour, minute, second = int(groups[3]), int(groups[4]), int(groups[5])
                    if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                        dt = datetime(year, month, day, hour, minute, second)
                        return dt.isoformat(), match.group(0)
                else:
                    dt = datetime(year, month, day)
                    return dt.date().isoformat(), match.group(0)
            except (ValueError, IndexError):
                continue

    return None


def parse_date_from_path(source_path: str) -> Optional[tuple[str, str, str]]:
    """
    Extract date from path patterns.

    Returns (iso_date_or_partial, confidence, raw_match) or None.

    Handles:
    - Semantic: "Xmas 2004", "April 2010", "Wedding day"
    - YYMMDD folders: "101122 Rose ballet" -> 2010-11-22
    - Library import dates are NOT extracted (they're misleading)
    """
    # Skip library paths - their dates are import dates, not photo dates
    if ".photolibrary" in source_path or ".photoslibrary" in source_path:
        return None

    # YYMMDD at start of folder name (e.g., "101122 Rose end of term Ballet Class")
    match = re.search(r"/(\d{2})(\d{2})(\d{2})\s+[^/]+/", source_path)
    if match:
        yy, mm, dd = int(match.group(1)), int(match.group(2)), int(match.group(3))
        # Assume 20xx for years 00-30, 19xx for 31-99
        year = 2000 + yy if yy <= 30 else 1900 + yy
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            try:
                dt = datetime(year, mm, dd)
                return dt.date().isoformat(), "medium", match.group(0)
            except ValueError:
                pass

    # Month + Year patterns
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }

    # "April 2010" or "2010 April" or "May 08"
    for month_name, month_num in months.items():
        # Month + 4-digit year
        pattern = rf"\b({month_name})\s+(\d{{4}})\b"
        match = re.search(pattern, source_path, re.IGNORECASE)
        if match:
            year = int(match.group(2))
            if 1990 <= year <= 2030:
                return f"{year}-{month_num:02d}", "medium", match.group(0)

        # 4-digit year + Month
        pattern = rf"\b(\d{{4}})\s+({month_name})\b"
        match = re.search(pattern, source_path, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            if 1990 <= year <= 2030:
                return f"{year}-{month_num:02d}", "medium", match.group(0)

        # Month + 2-digit year (e.g., "May 08" meaning May 2008)
        pattern = rf"\b({month_name})\s+(\d{{2}})\b"
        match = re.search(pattern, source_path, re.IGNORECASE)
        if match:
            yy = int(match.group(2))
            # 00-30 → 2000-2030, 31-99 → 1931-1999
            year = 2000 + yy if yy <= 30 else 1900 + yy
            if 1990 <= year <= 2030:
                return f"{year}-{month_num:02d}", "medium", match.group(0)

    # "Xmas 2004" or "Christmas 2004" -> December of that year
    match = re.search(r"\b(xmas|christmas)\s+(\d{4})\b", source_path, re.IGNORECASE)
    if match:
        year = int(match.group(2))
        if 1990 <= year <= 2030:
            return f"{year}-12", "medium", match.group(0)

    # Just a year in the path (lower confidence)
    # Match "2004" or "/2004/" but not in library paths
    match = re.search(r"/(\d{4})/", source_path)
    if match:
        year = int(match.group(1))
        if 1990 <= year <= 2030:
            return f"{year}", "low", match.group(0)

    return None


def get_file_mtime(file_path: Path) -> Optional[str]:
    """Get file modification time as ISO format string."""
    try:
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        return mtime.isoformat()
    except Exception:
        return None
