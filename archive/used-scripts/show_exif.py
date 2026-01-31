#!/usr/bin/env -S uv run --script
# /// script
# requires-python = '>=3.13'
# dependencies = ['pillow']
# ///
"""Show EXIF data from images."""

import sys
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

for path in sys.argv[1:]:
    print(f"=== {path.split('/')[-1]} ===")
    img = Image.open(path)
    exif_data = img._getexif()

    if exif_data:
        for tag_id, value in sorted(exif_data.items()):
            tag = TAGS.get(tag_id, tag_id)
            # Skip binary data
            if isinstance(value, bytes) and len(value) > 50:
                value = f"<{len(value)} bytes>"
            print(f"  {tag}: {value}")
    else:
        print("  No EXIF data")
    print()
