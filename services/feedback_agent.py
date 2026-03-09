import re
import logging
from datetime import datetime, timedelta, timezone

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from db import get_connection
from services.reward import calculate_reward

logger = logging.getLogger(__name__)

MIN_AGE_DAYS  = 3   # analytics need 2-day delay; 3 is safe
REFETCH_HOURS = 12  # don't re-hit API if fetched recently

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]
TOKEN_FILE = "token.json"

logging.basicConfig(level=logging.INFO)

def get_services():
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return (
        build("youtube", "v3", credentials=creds),
        build("youtubeAnalytics", "v2", credentials=creds),
    )


def extract_meme_id(description: str):
    match = re.search(r"id=([a-f0-9\-]+)", description)
    return match.group(1) if match else None


def get_recent_videos(youtube) -> list[dict]:
    """
    Uses videos().list instead of search().list — 1 quota unit vs 100.
    Pulls from uploads playlist of the authenticated channel.
    """
    # Step 1: get uploads playlist ID (1 unit)
    channel_resp = youtube.channels().list(
        part="contentDetails", mine=True
    ).execute()

    uploads_playlist = (
        channel_resp["items"][0]["contentDetails"]
        ["relatedPlaylists"]["uploads"]
    )

    # Step 2: get recent uploads (1 unit per page)
    playlist_resp = youtube.playlistItems().list(
        part="snippet",
        playlistId=uploads_playlist,
        maxResults=25,
    ).execute()

    now     = datetime.now(timezone.utc)
    videos  = []

    for item in playlist_resp.get("items", []):
        snippet      = item["snippet"]
        video_id     = snippet["resourceId"]["videoId"]
        published_at = snippet["publishedAt"]
        description  = snippet.get("description", "")

        published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        age_days     = (now - published_dt).days

        if age_days < MIN_AGE_DAYS:
            logger.debug(f"Skipping {video_id} — only {age_days}d old")
            continue

        meme_id = extract_meme_id(description)
        if not meme_id:
            logger.debug(f"Skipping {video_id} — no meme_id in description")
            continue

        videos.append({"video_id": video_id, "meme_id": meme_id})

    return videos


def _already_fetched_recently(cursor, video_id: str) -> bool:
    """Returns True if this video was updated within REFETCH_HOURS."""
    cursor.execute("""
        SELECT fetched_at FROM analytics
        WHERE video_id = ?
    """, (video_id,))
    row = cursor.fetchone()

    if not row or not row[0]:
        return False

    fetched_at = datetime.fromisoformat(row[0])
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
    return age_hours < REFETCH_HOURS


def get_video_analytics(analytics, video_id: str) -> dict | None:
    # Dynamic window: from 90 days ago to 2 days ago (safe analytics window)
    end_date   = (datetime.now() - timedelta(days=2)).date()
    start_date = (datetime.now() - timedelta(days=90)).date()

    try:
        response = analytics.reports().query(
            ids="channel==MINE",
            startDate=start_date.strftime("%Y-%m-%d"),
            endDate=end_date.strftime("%Y-%m-%d"),
            metrics="engagedViews,views,likes,averageViewDuration,averageViewPercentage",
            filters=f"video=={video_id}",
        ).execute()
    except Exception as e:
        logger.warning(f"Analytics fetch failed for {video_id}: {e}")
        return None

    rows = response.get("rows")
    if not rows:
        return None

    engaged_views, views, likes, avg_watch_time, avg_view_pct = rows[0]

    return {
        "views":           int(views),
        "likes":           int(likes),
        "engaged_views":   int(engaged_views),
        "avg_watch_time":  float(avg_watch_time),
        "completion_rate": float(avg_view_pct) / 100.0,
    }


def save_analytics(cursor, video_id: str, meme_id: str, video_length, metrics: dict):

    reward = calculate_reward(
        views=metrics["views"],
        likes=metrics["likes"],
        avg_watch_time=metrics["avg_watch_time"],
        completion_rate=metrics["completion_rate"],
        video_length=video_length,
    )

    cursor.execute("""
        INSERT INTO analytics (video_id, views, likes, avg_watch_time, completion_rate, reward)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            views            = excluded.views,
            likes            = excluded.likes,
            avg_watch_time   = excluded.avg_watch_time,
            completion_rate  = excluded.completion_rate,
            reward           = excluded.reward,
            fetched_at       = CURRENT_TIMESTAMP
    """, (
        video_id,
        metrics["views"],
        metrics["likes"],
        metrics["avg_watch_time"],
        metrics["completion_rate"],
        reward,
    ))

    logger.info(f"Saved analytics for {video_id} — views={metrics['views']} reward={reward:.3f}")


def run_feedback():
    youtube, analytics = get_services()
    videos = get_recent_videos(youtube)
    logger.info(f"Found {len(videos)} eligible videos.")

    
    conn = get_connection()
    cursor = conn.cursor()

    skipped = saved = 0

    for video in videos:
        vid_id  = video["video_id"]
        meme_id = video["meme_id"]

        metrics = get_video_analytics(analytics, vid_id)

        if not metrics or metrics["views"] == 0:
            logger.debug(f"No analytics yet for {vid_id}")
            continue


        cursor.execute(
            "SELECT video_id, video_length FROM videos WHERE meme_id = ?",
            (meme_id,)
        )

        row = cursor.fetchone()

        if not row:
            logger.warning(f"No video found for meme {meme_id}")
            continue

        video_db_id, video_length = row

        
        if _already_fetched_recently(cursor, video_db_id):
            logger.debug(f"Skipping {video_db_id} — fetched within {REFETCH_HOURS}h")
            skipped += 1
            continue

        save_analytics(cursor, video_db_id, meme_id, video_length, metrics)
        saved += 1

    conn.commit()
    conn.close()  

    logger.info(f"Feedback complete — {saved} updated, {skipped} skipped.")


if __name__ == "__main__":
    run_feedback()