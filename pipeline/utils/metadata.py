"""EXIF extraction and date parsing utilities."""

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import piexif
from PIL import Image


def extract_exif(file_path: Path) -> dict:
    """
    Extract EXIF data from an image file.

    Returns a dict with available fields:
    - date_taken: datetime or None
    - camera_make: str or None
    - camera_model: str or None
    - has_exif: bool
    """
    result = {
        "date_taken": None,
        "camera_make": None,
        "camera_model": None,
        "has_exif": False,
    }

    try:
        exif_dict = piexif.load(str(file_path))

        # Camera make
        if piexif.ImageIFD.Make in exif_dict.get("0th", {}):
            make = exif_dict["0th"][piexif.ImageIFD.Make]
            if isinstance(make, bytes):
                make = make.decode("utf-8", errors="ignore")
            result["camera_make"] = make.strip() if make else None
            if result["camera_make"]:
                result["has_exif"] = True

        # Camera model
        if piexif.ImageIFD.Model in exif_dict.get("0th", {}):
            model = exif_dict["0th"][piexif.ImageIFD.Model]
            if isinstance(model, bytes):
                model = model.decode("utf-8", errors="ignore")
            result["camera_model"] = model.strip() if model else None
            if result["camera_model"]:
                result["has_exif"] = True

        # Date taken (prefer DateTimeOriginal over DateTime)
        date_fields = [
            (piexif.ExifIFD.DateTimeOriginal, "Exif"),
            (piexif.ExifIFD.DateTimeDigitized, "Exif"),
            (piexif.ImageIFD.DateTime, "0th"),
        ]

        for field, ifd in date_fields:
            if field in exif_dict.get(ifd, {}):
                date_str = exif_dict[ifd][field]
                if isinstance(date_str, bytes):
                    date_str = date_str.decode("utf-8", errors="ignore")
                try:
                    result["date_taken"] = datetime.strptime(
                        date_str.strip(), "%Y:%m:%d %H:%M:%S"
                    )
                    result["has_exif"] = True
                    break
                except ValueError:
                    continue

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


def parse_date_from_filename(filename: str) -> Optional[datetime]:
    """
    Extract date from common filename patterns.

    Supports patterns like:
    - IMG_20231225_123456.jpg
    - 20231225_123456.jpg
    - 2023-12-25_anything.jpg
    - DSC_20231225.jpg
    """
    patterns = [
        # YYYYMMDD_HHMMSS or YYYYMMDD-HHMMSS
        (r"(\d{4})(\d{2})(\d{2})[_-](\d{2})(\d{2})(\d{2})", True),
        # YYYY-MM-DD_HHMMSS
        (r"(\d{4})-(\d{2})-(\d{2})[_-](\d{2})(\d{2})(\d{2})", True),
        # YYYYMMDD alone
        (r"(\d{4})(\d{2})(\d{2})", False),
        # YYYY-MM-DD alone
        (r"(\d{4})-(\d{2})-(\d{2})", False),
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
                        return datetime(year, month, day, hour, minute, second)
                else:
                    return datetime(year, month, day)
            except (ValueError, IndexError):
                continue

    return None


def determine_date(
    file_path: Path, exif_data: dict
) -> tuple[Optional[datetime], Optional[str]]:
    """
    Determine the best date for a photo.

    Returns (date, source) where source is 'exif', 'filename', or 'mtime'.
    """
    # Try EXIF first (most reliable)
    if exif_data.get("date_taken"):
        return exif_data["date_taken"], "exif"

    # Try filename
    filename_date = parse_date_from_filename(file_path.name)
    if filename_date:
        return filename_date, "filename"

    # Fall back to file mtime
    try:
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        return mtime, "mtime"
    except Exception:
        return None, None
