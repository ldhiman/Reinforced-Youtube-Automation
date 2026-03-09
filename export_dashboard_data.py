import json
import os
from db import get_connection

OUTPUT = "dashboard/data.json"

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

conn = get_connection()
cursor = conn.cursor()

# ─────────────────────────────────────────
# Basic KPIs
# ─────────────────────────────────────────

cursor.execute("SELECT COUNT(*) FROM memes")
total_memes = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM videos")
total_videos = cursor.fetchone()[0]

cursor.execute("SELECT ROUND(AVG(reward),4) FROM analytics WHERE reward IS NOT NULL")
avg_reward = cursor.fetchone()[0]

cursor.execute("SELECT SUM(views) FROM analytics")
total_views = cursor.fetchone()[0] or 0


# ─────────────────────────────────────────
# Subreddit performance
# ─────────────────────────────────────────

cursor.execute("""
SELECT
    m.subreddit,
    COUNT(v.video_id) AS videos,
    ROUND(AVG(a.reward),4) AS avg_reward
FROM memes m
LEFT JOIN videos v ON m.meme_id = v.meme_id
LEFT JOIN analytics a ON v.video_id = a.video_id
GROUP BY m.subreddit
ORDER BY videos DESC
""")

subreddits = [
    {
        "subreddit": r[0],
        "videos": r[1],
        "avg_reward": r[2]
    }
    for r in cursor.fetchall()
]


# ─────────────────────────────────────────
# Top performing videos
# ─────────────────────────────────────────

cursor.execute("""
SELECT
    v.video_id,
    m.subreddit,
    a.views,
    a.likes,
    ROUND(a.reward,4)
FROM analytics a
JOIN videos v ON a.video_id = v.video_id
JOIN memes m ON v.meme_id = m.meme_id
ORDER BY a.reward DESC
LIMIT 10
""")

top_videos = [
    {
        "video_id": r[0],
        "subreddit": r[1],
        "views": r[2],
        "likes": r[3],
        "reward": r[4]
    }
    for r in cursor.fetchall()
]


# ─────────────────────────────────────────
# Reward trend (last 20 updates)
# ─────────────────────────────────────────

cursor.execute("""
SELECT reward
FROM analytics
WHERE reward IS NOT NULL
ORDER BY fetched_at DESC
LIMIT 20
""")

reward_trend = [round(r[0],4) for r in cursor.fetchall()]


# ─────────────────────────────────────────
# Active subreddits
# ─────────────────────────────────────────

active_sources = sum(1 for s in subreddits if s["videos"] > 0)


# ─────────────────────────────────────────
# Final JSON
# ─────────────────────────────────────────

data = {
    "total_memes": total_memes,
    "total_videos": total_videos,
    "total_views": total_views,
    "avg_reward": avg_reward,
    "active_sources": active_sources,
    "subreddits": subreddits,
    "top_videos": top_videos,
    "reward_trend": reward_trend
}


# ─────────────────────────────────────────
# Safe JSON write
# ─────────────────────────────────────────

temp_file = OUTPUT + ".tmp"

with open(temp_file, "w") as f:
    json.dump(data, f, indent=2)

os.replace(temp_file, OUTPUT)

print("Dashboard data exported →", OUTPUT)

conn.close()