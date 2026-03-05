import sqlite3
import os
import re
from db import DB_PATH
from services.feedback_agent import get_services

META_REGEX = r"id=([a-f0-9\-]+)"


def get_uploaded_meme_ids():

    youtube, _ = get_services()

    request = youtube.search().list(
        part="snippet",
        forMine=True,
        type="video",
        maxResults=50,
        order="date"
    )

    response = request.execute()

    uploaded_ids = set()

    for item in response.get("items", []):

        desc = item["snippet"]["description"]

        match = re.search(META_REGEX, desc)

        if match:
            uploaded_ids.add(match.group(1))

    return uploaded_ids


def cleanup():

    uploaded_meme_ids = get_uploaded_meme_ids()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT v.video_id, v.meme_id
        FROM videos v
    """)

    rows = cursor.fetchall()

    removed = 0

    for video_id, meme_id in rows:

        if meme_id not in uploaded_meme_ids:

            # if video_path and os.path.exists(video_path):
            #     os.remove(video_path)

            cursor.execute(
                "DELETE FROM videos WHERE video_id = ?",
                (video_id,)
            )

            removed += 1

    conn.commit()
    conn.close()

    print(f"Removed {removed} unused videos")


if __name__ == "__main__":
    cleanup()