import requests
import os
import uuid
import sqlite3
import tempfile
import logging
from typing import List, Dict
from db import get_connection
import imagehash
from PIL import Image, ImageFile
import time
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

ImageFile.LOAD_TRUNCATED_IMAGES = True
logger = logging.getLogger(__name__)

# -----------------------------
# CONFIG — set these via env vars
# -----------------------------
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "MemeBot/1.0")

SORT_ORDER = ["hot", "top", "rising", "controversial", "new"]   # tried in order if count is low
TOP_TIMEFRAME = "week"         # for 'top': hour | day | week | month | year | all


def _get_reddit_token() -> str | None:
    """OAuth2 client_credentials — no user login needed."""
    try:
        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": REDDIT_USER_AGENT},
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception as e:
        logger.error(f"Reddit auth failed: {e}")
        return None


def _fetch_reddit_posts(
    subreddit: str,
    token: str,
    sort: str,
    limit: int,
    timeframe: str = "day",
) -> list:
    """Fetch raw posts from Reddit API for one subreddit + sort."""
    params = {"limit": limit}
    if sort == "top":
        params["t"] = timeframe

    try:
        r = requests.get(
            f"https://oauth.reddit.com/r/{subreddit}/{sort}",
            headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": REDDIT_USER_AGENT,
            },
            params=params,
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("children", [])
    except Exception as e:
        logger.warning(f"r/{subreddit} [{sort}] fetch failed: {e}")
        return []


# -----------------------------
# DB HELPERS (unchanged)
# -----------------------------

def load_existing_posts():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT reddit_post_id FROM memes WHERE reddit_post_id IS NOT NULL")
    posts = {row[0] for row in cursor.fetchall()}
    conn.close()
    return posts


def load_existing_hashes():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT template FROM memes WHERE template IS NOT NULL")
    hashes = [imagehash.hex_to_hash(row[0]) for row in cursor.fetchall()]
    conn.close()
    return hashes


def is_duplicate(hash_value, existing_hashes, threshold=5):
    return any(hash_value - db_hash <= threshold for db_hash in existing_hashes)


def save_meme_to_db(meme: Dict):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO memes (meme_id, reddit_post_id, subreddit, title, ups, template, image_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            meme["meme_id"], meme["reddit_post_id"], meme["subreddit"],
            meme["title"], meme["ups"], meme["template"], meme["image_path"]
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


# -----------------------------
# IMAGE PROCESSING (unchanged)
# -----------------------------

def stream_to_temp_file(response, max_bytes=5 * 1024 * 1024) -> str | None:
    total = 0
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as tmp:
            tmp_path = tmp.name
            for chunk in response.iter_content(chunk_size=64 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    return None
                tmp.write(chunk)
        return tmp_path
    except Exception:
        return None


def process_image_from_file(tmp_path: str, save_path: str, max_dim: int = 2000):
    try:
        with Image.open(tmp_path) as img:
            if img.format not in ("JPEG", "PNG"):
                return None, False
            w, h = img.size
            if w < 100 or h < 100:
                return None, False
            if w * h > 40_000_000:
                return None, False
            if img.format == "JPEG" and max(w, h) > max_dim:
                scale = max_dim / max(w, h)
                img.draft("RGB", (int(w * scale), int(h * scale)))
            img_rgb = img.convert("RGB")
            if max(img_rgb.size) > max_dim:
                img_rgb.thumbnail((max_dim, max_dim), Image.LANCZOS)
            img_rgb.save(save_path, "JPEG", quality=85, optimize=True)
            thumb = img_rgb.copy()
            thumb.thumbnail((256, 256))
            return thumb, True
    except Exception:
        return None, False


def _log_subreddit_stats(subreddit: str, sort: str, stats: dict, saved: int, total: int):
    skip_parts = ", ".join(f"{r}: {c}" for r, c in stats.items() if c > 0)
    skip_str = f" | skipped — {skip_parts}" if skip_parts else ""
    logger.info(f"r/{subreddit} [{sort}]: {saved}/{total} saved{skip_str}")


# -----------------------------
# MAIN FETCHER
# -----------------------------

def fetch_memes(
    subreddits: List[str],
    per_subreddit: int = 10,
    min_upvotes: int = 0,
) -> List[Dict]:

    save_dir = "storage/memes"
    os.makedirs(save_dir, exist_ok=True)

    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        logger.error("REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set.")
        return []

    token = _get_reddit_token()
    if not token:
        return []

    downloaded = []
    used_urls = set()
    existing_posts = load_existing_posts()
    existing_hashes = load_existing_hashes()

    headers = {"User-Agent": REDDIT_USER_AGENT}

    for subreddit in subreddits:
        saved_this_sub = 0
        logger.info(f"Targeting {per_subreddit} memes for r/{subreddit}")

        for sort in SORT_ORDER:
            # Check if we already hit the quota from a previous sort order
            if saved_this_sub >= per_subreddit:
                break
            
            # Fetch maximum allowed (100) to ensure we find enough high-quality memes
            posts = _fetch_reddit_posts(
                subreddit, token, sort,
                limit=100, 
                timeframe=TOP_TIMEFRAME,
            )

            time.sleep(1)  # rate limit between each API call

            if not posts:
                continue


            stats = defaultdict(int)
            saved_in_sort = 0

            for child in posts:
                # BREAK if quota met mid-loop
                if saved_this_sub >= per_subreddit:
                    break

                post = child.get("data", {})
                meme_url = post.get("url", "")
                reddit_post_id = post.get("id")
                ups = post.get("ups", 0)

                # --- FILTERS ---
                if not meme_url or meme_url in used_urls:
                    stats["bad_url"] += 1; continue
                if post.get("over_18"):
                    stats["nsfw"] += 1; continue
                if ups < min_upvotes:
                    stats["low_upvotes"] += 1; continue
                if reddit_post_id in existing_posts:
                    stats["duplicate_post"] += 1; continue
                if not meme_url.lower().endswith((".jpg", ".jpeg", ".png")):
                    stats["bad_extension"] += 1; continue

                # --- DOWNLOAD & PROCESS ---
                tmp_path = None
                try:
                    img_response = None
                    for _ in range(2):
                        try:
                            img_response = requests.get(
                                meme_url, headers=headers, stream=True, timeout=15
                            )
                            img_response.raise_for_status()
                            break
                        except requests.RequestException:
                            img_response = None

                    if not img_response:
                        stats["download_failed"] += 1; continue

                    tmp_path = stream_to_temp_file(img_response)
                    img_response.close()

                    if not tmp_path:
                        stats["too_large"] += 1; continue

                    meme_id = str(uuid.uuid4())
                    save_path = os.path.normpath(os.path.join(save_dir, f"{meme_id}.jpg"))

                    thumb, success = process_image_from_file(tmp_path, save_path)
                    
                    if not success or thumb is None:
                        stats["bad_image"] += 1; continue

                    # Perceptual Hash Duplicate Check
                    hash_obj = imagehash.phash(thumb)
                    if is_duplicate(hash_obj, existing_hashes):
                        if os.path.exists(save_path): os.remove(save_path)
                        stats["duplicate_hash"] += 1; continue

                    # --- SUCCESS: SAVE TO DATA STRUCTURES ---
                    meme_data = {
                        "meme_id": meme_id,
                        "reddit_post_id": reddit_post_id,
                        "subreddit": subreddit,
                        "title": post.get("title"),
                        "ups": ups,
                        "template": str(hash_obj),
                        "image_path": save_path,
                    }

                    save_meme_to_db(meme_data)
                    downloaded.append(meme_data)
                    
                    used_urls.add(meme_url)
                    existing_posts.add(reddit_post_id)
                    existing_hashes.append(hash_obj)
                    
                    saved_in_sort += 1
                    saved_this_sub += 1

                except Exception as e:
                    logger.error(f"Error processing {meme_url}: {e}")
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        os.remove(tmp_path)

            _log_subreddit_stats(subreddit, sort, stats, saved_in_sort, len(posts))

        if saved_this_sub < per_subreddit:
            logger.warning(f"Could only find {saved_this_sub}/{per_subreddit} memes for r/{subreddit}")

    return downloaded