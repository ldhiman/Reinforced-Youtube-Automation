import re
import sqlite3
from datetime import datetime, timedelta

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from db import get_connection
from services.reward import calculate_reward

from datetime import timezone

MIN_AGE_DAYS = 1


# ---- CONFIG ----
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

TOKEN_FILE = "token.json"


# ---- AUTH ----
def get_services():
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    # Auto refresh if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())

    youtube = build("youtube", "v3", credentials=creds)
    analytics = build("youtubeAnalytics", "v2", credentials=creds)

    return youtube, analytics


# ---- META PARSER ----
def extract_meme_id(description: str):
    match = re.search(r"id=([a-f0-9\-]+)", description)
    return match.group(1) if match else None


def get_recent_videos(youtube):
    request = youtube.search().list(
        part="snippet",
        forMine=True,
        type="video",
        maxResults=25,
        order="date"
    )

    response = request.execute()

    videos = []

    now = datetime.now(timezone.utc)

    for item in response.get("items", []):
        video_id = item["id"]["videoId"]
        description = item["snippet"]["description"]
        published_at = item["snippet"]["publishedAt"]

        # Convert published time to datetime
        published_dt = datetime.fromisoformat(
            published_at.replace("Z", "+00:00")
        )

        age_days = (now - published_dt).days

        if age_days < MIN_AGE_DAYS:
            print(f"Skipping {video_id} (only {age_days} days old)")
            continue

        meme_id = extract_meme_id(description)

        if meme_id:
            videos.append({
                "video_id": video_id,
                "meme_id": meme_id
            })

    return videos

def get_realtime_data(youtube, video_id):
    """Fetches real-time views and likes (no 3-day delay)"""
    request = youtube.videos().list(
        part="statistics",
        id=video_id
    )
    response = request.execute()
    
    if not response.get("items"):
        return None
        
    stats = response["items"][0]["statistics"]
    print(video_id, stats)
    return {
        "views": int(stats.get("viewCount", 0)),
        "likes": int(stats.get("likeCount", 0))
    }

# ---- FETCH ANALYTICS ----
def get_video_analytics(analytics, video_id):
    end_date = (datetime.now() - timedelta(days=2)).date()
    start_date = datetime(2026, 2, 1).date()
    response = analytics.reports().query(
        ids="channel==MINE",
        startDate=start_date.strftime('%Y-%m-%d'),
        endDate=end_date.strftime('%Y-%m-%d'),
        metrics="engagedViews,views,likes,averageViewDuration,averageViewPercentage",
        filters=f"video=={video_id}"
    ).execute()

    print(video_id, response)

    rows = response.get("rows")


    if not rows:
        return None

    engaged_views, views, likes, avg_watch_time, avg_view_percent = rows[0]

    completion_rate = avg_view_percent / 100.0

    return {
        "views": views,
        "likes": likes,
        "engaged_views": engaged_views,
        "avg_watch_time": avg_watch_time,
        "completion_rate": completion_rate
    }


# ---- SAVE TO DB ----
def save_analytics(video_id, meme_id, metrics):
    conn = get_connection()
    cursor = conn.cursor()

    # get video length from DB if stored (optional)
    video_length = 4  # default assumption for Shorts

    reward = calculate_reward(
        views=metrics["views"],
        likes=metrics["likes"],
        avg_watch_time=metrics["avg_watch_time"],
        completion_rate=metrics["completion_rate"],
        video_length=video_length
    )

    # In feedback_agent.py -> save_analytics()
    cursor.execute("""
        INSERT INTO analytics (
            video_id,
            views,
            likes,
            avg_watch_time,
            completion_rate,
            reward
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            views=excluded.views,
            likes=excluded.likes,
            avg_watch_time=excluded.avg_watch_time,
            completion_rate=excluded.completion_rate,
            reward=excluded.reward,
            fetched_at=CURRENT_TIMESTAMP  -- Manually update the timestamp on refresh
    """, (
        video_id,
        metrics["views"],
        metrics["likes"],
        metrics["avg_watch_time"],
        metrics["completion_rate"],
        reward
    ))

    conn.commit()
    conn.close()


# ---- MAIN RUNNER ----
def run_feedback():
    youtube, analytics = get_services()

    videos = get_recent_videos(youtube)

    print(f"Found {len(videos)} recent videos with meme_ids.")

    for video in videos:
        get_realtime_data(youtube, video["video_id"])
        metrics = get_video_analytics(analytics, video["video_id"])

        if metrics:
            print(f"Processing video {video['video_id']} with meme_id {video['meme_id']}")
            if metrics and metrics["views"] > 0:
                save_analytics(video["video_id"], video["meme_id"], metrics)
            else:
                print(f"Skipping DB save for {video['video_id']}: Analytics data not yet processed by YouTube.")
            


if __name__ == "__main__":
    run_feedback()