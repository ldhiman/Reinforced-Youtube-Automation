import uuid
import logging
import time
from contextlib import contextmanager
from typing import List

from db import init_db, get_connection
from services.image_fetcher import fetch_memes
from services.quality_evaluator import evaluate_memes
from services.selector import select_memes
from services.video_editor import insert_memes
from services.telegram_sender import upload_to_telegram
from services.feedback_agent import run_feedback
from services.reinforcement import update_weights


# -----------------------------
# CONFIG
# -----------------------------

UPLOAD_COUNT = 1  # Increase to 5 when scaling

HIGH_PRIORITY_SUBREDDITS: List[str] = [
    "dankmemes",
    "wholesomememes",
    "memes",
    "funny",
    "terriblefacebookmemes",
]

MEDIUM_PRIORITY_SUBREDDITS: List[str] = [
    "MinecraftMemes",
    "LeagueOfMemes",
    "HistoryMemes",
    "me_irl",
]

LOW_PRIORITY_SUBREDDITS: List[str] = [
    "ProgrammerHumor",
    "HolUp",
    "comedyheaven",
    "ComedyCemetery",
    "okbuddyretard",
]

# (subreddits, per_sub, min_upvotes)
FETCH_CONFIG = [
    (HIGH_PRIORITY_SUBREDDITS,   10, 1500),
    (MEDIUM_PRIORITY_SUBREDDITS, 3, 1000),
    (LOW_PRIORITY_SUBREDDITS,    1,  500),
]

RUN_FEEDBACK_AFTER_UPLOAD = False
RUN_REINFORCEMENT_AFTER_FEEDBACK = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# -----------------------------
# DB HELPERS
# -----------------------------

@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def register_video(video_id: str, meme_id: str, hook_style: str, video_length: float):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO videos (video_id, meme_id, hook_style, music_type, video_length)
            VALUES (?, ?, ?, ?, ?)
            """,
            (video_id, meme_id, hook_style, "default", video_length),
        )


def mark_meme_used(meme_id: str):
    with get_db() as conn:
        conn.execute(
            "UPDATE memes SET used = 1, status = 'rendered' WHERE meme_id = ?",
            (meme_id,),
        )


def build_description(meme_id: str, subreddit: str, base_caption: str = "Follow for more 🔥") -> str:
    meta_block = (
        "\n\n---META---\n"
        f"id={meme_id}\n"
        f"sub={subreddit}\n"
        "---END---"
    )
    return f"{base_caption}\n\n#shorts #memes{meta_block}"


# -----------------------------
# PIPELINE STEPS
# -----------------------------

def fetch_all_memes():
    for subreddits, per_sub, min_upvotes in FETCH_CONFIG:
        count = len(fetch_memes(subreddits, per_sub, min_upvotes))
        logging.info(f"Got {count} Meme Downloaded for {subreddits}")


def process_meme(meme: dict) -> None:
    """Render, upload, and record a single meme."""
    meme_id = meme["meme_id"]
    start = time.monotonic()

    video_path = insert_memes(meme_paths=[meme["image_path"]])
    video_length = 3.0

    video_id = str(uuid.uuid4())
    title = meme.get("title") or "Relatable Meme"
    description = build_description(meme_id=meme_id, subreddit=meme["subreddit"])

    upload_to_telegram(video_path=video_path, title=title, description=description)

    register_video(video_id=video_id, meme_id=meme_id, hook_style="default", video_length=video_length)
    mark_meme_used(meme_id)

    logger.info(f"Completed meme {meme_id} in {time.monotonic() - start:.1f}s")


# -----------------------------
# MAIN PIPELINE
# -----------------------------

def main():
    logger.info("Initializing database...")
    init_db()

    logger.info("Fetching new memes...")
    # fetch_all_memes()

    logger.info("Evaluating memes...")
    evaluate_memes()

    logger.info("Selecting memes for today...")
    selected = select_memes(UPLOAD_COUNT)

    if not selected:
        logger.info("No memes selected. Exiting.")
        return

    succeeded, failed = 0, 0
    for meme in selected:
        logger.info(f"Processing meme: {meme['meme_id']}")
        try:
            process_meme(meme)
            succeeded += 1
        except Exception as e:
            logger.error(f"Error processing meme {meme['meme_id']}: {e}", exc_info=True)
            failed += 1

    logger.info(f"Pipeline complete — {succeeded} uploaded, {failed} failed.")

    if RUN_FEEDBACK_AFTER_UPLOAD:
        logger.info("Running feedback agent...")
        run_feedback()

        if RUN_REINFORCEMENT_AFTER_FEEDBACK:
            logger.info("Updating reinforcement weights...")
            update_weights()


if __name__ == "__main__":
    main()