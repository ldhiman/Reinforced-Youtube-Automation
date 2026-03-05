import uuid
import sqlite3
import logging
from typing import List

from db import init_db, DB_PATH
from services.image_fetcher import fetch_memes
from services.quality_evaluator import evaluate_memes
from services.selector import select_memes
from services.video_editor import insert_memes
from services.telegram_sender import upload_to_telegram
from services.feedback_agent import run_feedback
from services.reinforcement import update_weights
import datetime
import random

random.seed(datetime.date.today().toordinal())

# -----------------------------
# CONFIG
# -----------------------------
ALL_SUBREDDITS = [
    "memes",
    "dankmemes",
    "me_irl",
    "wholesomememes",
    "okbuddyretard",
    "funny",
    "comedyheaven",
    "ComedyCemetery",
    "terriblefacebookmemes",
    "MinecraftMemes",
    "LeagueOfMemes",
    "HistoryMemes",
    "ProgrammerHumor",
    "HolUp",
]

MEMES_PER_SUB = 8
DAILY_UPLOAD_COUNT = 3   # Change to 5 when scaling
MIN_UPVOTES = 100

SUBREDDITS = random.sample(ALL_SUBREDDITS, 12)


RUN_FEEDBACK_AFTER_UPLOAD = False
RUN_REINFORCEMENT_AFTER_FEEDBACK = False


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------------
# DB HELPERS
# -----------------------------
def register_video(video_id: str, meme_id: str, hook_style: str, video_length: float):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO videos (
            video_id,
            meme_id,
            hook_style,
            music_type,
            video_length
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        video_id,
        meme_id,
        hook_style,
        "default",
        video_length
    ))

    conn.commit()
    conn.close()


def mark_meme_used(meme_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE memes SET used = 1, status = 'rendered' WHERE meme_id = ?",
        (meme_id,)
    )

    conn.commit()
    conn.close()


def build_description(
    meme_id: str,
    subreddit: str,
    hook_style: str,
    music_type: str,
    video_length: float,
    base_caption: str = "Follow for more 🔥"
) -> str:

    meta_block = (
        "\n\n---META---\n"
        f"id={meme_id}\n"
        f"sub={subreddit}\n"
        f"hook={hook_style}\n"
        f"music={music_type}\n"
        f"length={video_length}\n"
        "---END---"
    )

    hashtags = "\n\n#shorts #memes"

    return f"{base_caption}{hashtags}{meta_block}"

# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():

    logger.info("Initializing database...")
    init_db()

    logger.info("Fetching new memes...")
    fetch_memes(
        subreddits=SUBREDDITS,
        per_subreddit=MEMES_PER_SUB,
        min_upvotes=MIN_UPVOTES
    )

    logger.info("Evaluating memes...")
    evaluate_memes()

    logger.info("Selecting memes for today...")
    selected = select_memes(DAILY_UPLOAD_COUNT)

    if not selected:
        logger.info("No memes selected.")
        return

    for meme in selected:

        try:
            logger.info(f"Rendering meme: {meme['meme_id']}")

            video_path = insert_memes(
                meme_paths=[meme["image_path"]],
            )

            video_id = str(uuid.uuid4())

            title = meme.get("title", "Relatable Meme")
            description = build_description(
                meme_id=meme["meme_id"],
                subreddit=meme["subreddit"],
                hook_style="default",
                music_type="default",
                video_length=3.0
            )

            logger.info("Uploading to Telegram...")
            upload_to_telegram(
                video_path=video_path,
                title=title,
                description=description,
            )

            register_video(
                video_id=video_id,
                meme_id=meme["meme_id"],
                hook_style="default",
                video_length=6.0
            )

            mark_meme_used(meme["meme_id"])

            logger.info(f"Completed meme: {meme['meme_id']}")

        except Exception as e:
            logger.error(f"Error processing meme {meme['meme_id']}: {e}")
            continue

    # Optional feedback loop
    if RUN_FEEDBACK_AFTER_UPLOAD:
        logger.info("Running feedback agent...")
        run_feedback()

        if RUN_REINFORCEMENT_AFTER_FEEDBACK:
            logger.info("Updating reinforcement weights...")
            update_weights()


if __name__ == "__main__":
    main()