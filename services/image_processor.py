from PIL import Image
import os
from typing import List, Tuple

def resize_images(image_paths: List[str], size: Tuple[int, int] = (1080, 1920)) -> List[str]:
    processed = []

    for path in image_paths:
        img = Image.open(path)

        # 🔥 FIX: Convert to RGB (important)
        if img.mode != "RGB":
            img = img.convert("RGB")

        img = img.resize(size)

        new_path = path.replace(".jpg", "_processed.jpg")
        img.save(new_path, format="JPEG")

        processed.append(new_path)

    return processed