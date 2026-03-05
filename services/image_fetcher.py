
import requests
import os
import uuid
import cv2
import sqlite3
from typing import List, Dict
from db import DB_PATH
import numpy as np
import imagehash
from PIL import Image

def get_template_hash(image_path):
    with Image.open(image_path) as img:
        img.thumbnail((256,256))
        return str(imagehash.phash(img))


def meme_exists(reddit_post_id: str, template_hash: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # First check reddit post id
    cursor.execute(
        "SELECT 1 FROM memes WHERE reddit_post_id = ? LIMIT 1",
        (reddit_post_id,)
    )

    if cursor.fetchone():
        conn.close()
        return True

    # Now check similar templates
    cursor.execute("SELECT template FROM memes")

    existing_hash = imagehash.hex_to_hash(template_hash)

    for row in cursor.fetchall():
        db_hash = imagehash.hex_to_hash(row[0])

        if existing_hash - db_hash <= 6:  # similarity threshold
            conn.close()
            return True

    conn.close()
    return False


def save_meme_to_db(meme: Dict):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO memes (
                meme_id,
                reddit_post_id,
                subreddit,
                title,
                ups,
                template,
                image_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            meme["meme_id"],
            meme["reddit_post_id"],
            meme["subreddit"],
            meme["title"],
            meme["ups"],
            meme["template"],
            meme["image_path"]
        ))

        conn.commit()

    except sqlite3.IntegrityError:
        pass

    finally:
        conn.close()

def extract_reddit_id(post_link: str):
    if not post_link:
        return None
    return post_link.rstrip("/").split("/")[-1]

def fetch_memes(
    subreddits: List[str],
    per_subreddit: int = 10,
    min_upvotes: int = 0
) -> List[Dict]:

    save_dir = "storage/memes"
    os.makedirs(save_dir, exist_ok=True)

    downloaded = []
    used_urls = set()

    for subreddit in subreddits:
        url = f"https://meme-api.com/gimme/{subreddit}/{per_subreddit}"

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
        except requests.RequestException:
            continue

        data = response.json()
        posts = data.get("memes", [])

        for post in posts:
            meme_url = post.get("url")
            reddit_post_id = extract_reddit_id(post.get("postLink"))
            ups = post.get("ups", 0)

            if not meme_url or meme_url in used_urls:
                continue

            if post.get("nsfw"):
                continue

            if ups < min_upvotes:
                continue

            if not meme_url.lower().endswith((".jpg", ".jpeg", ".png")):
                continue

            try:
                img_response = requests.get(meme_url, stream=True, timeout=20)
                img_response.raise_for_status()
            except requests.RequestException:
                continue

            content_type = img_response.headers.get("Content-Type", "").lower()

            # 🚫 Reject unwanted formats
            if not (
                "image/jpeg" in content_type
                or "image/jpg" in content_type
                or "image/png" in content_type
            ):
                continue

            # Extra safety: reject gif/webp explicitly
            if "gif" in content_type or "webp" in content_type:
                continue

            meme_id = str(uuid.uuid4())
            filename = os.path.normpath(os.path.join(save_dir, f"{meme_id}.jpg"))

            # --- Decode safely using OpenCV ---
            file_bytes = img_response.content
            image_array = np.asarray(bytearray(file_bytes), dtype=np.uint8)
            img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

            # 🚫 If OpenCV cannot decode, skip
            if img is None:
                continue

            # --- Save re-encoded clean JPG ---
            cv2.imwrite(filename, img)

            # 🚫 Ensure file size reasonable
            if not os.path.exists(filename) or os.path.getsize(filename) < 5000:
                if os.path.exists(filename):
                    os.remove(filename)
                continue

            used_urls.add(meme_url)

            hashImage = get_template_hash(filename)

            if meme_exists(reddit_post_id, hashImage):
                os.remove(filename)
                continue  # avoid reposting same Reddit meme
            

            meme_data = {
                "meme_id": meme_id,
                "reddit_post_id": reddit_post_id,
                "subreddit": subreddit,
                "title": post.get("title"),
                "ups": ups,
                "template": hashImage,
                "image_path": filename
            }

            # Validate image really exists and is readable
            if not os.path.exists(filename):
                continue

            test_img = cv2.imread(filename)
            if test_img is None:
                os.remove(filename)
                continue

            # Only now save to DB
            save_meme_to_db(meme_data)
            downloaded.append(meme_data)

    return downloaded