#!/usr/bin/env python3
# /// script
# dependencies = ["pillow", "imagehash"]
# ///
from PIL import Image
import imagehash

img = Image.open('organized/images/high_confidence/c9f1551a9a057c9710013b295e2532d5ab16306d04a8e12176050b8b2a4717ab.jpg')
print(f'Image mode: {img.mode}, size: {img.size}')
phash = imagehash.phash(img)
print(f'Perceptual hash: {phash}')
