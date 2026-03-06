import json
from db import get_connection
import os

OUTPUT = "dashboard/data.json"

if not os.path.exists(os.path.dirname(OUTPUT)):
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

conn = get_connection()
cursor = conn.cursor()

# total memes
cursor.execute("SELECT COUNT(*) FROM memes")
total_memes = cursor.fetchone()[0]

# total videos
cursor.execute("SELECT COUNT(*) FROM videos")
total_videos = cursor.fetchone()[0]

# avg reward
cursor.execute("SELECT AVG(reward) FROM analytics WHERE reward IS NOT NULL")
avg_reward = cursor.fetchone()[0]

# subreddit performance
cursor.execute("""
SELECT
m.subreddit,
COUNT(v.video_id) AS videos,
AVG(a.reward) AS avg_reward
FROM memes m
LEFT JOIN videos v ON m.meme_id = v.meme_id
LEFT JOIN analytics a ON v.video_id = a.video_id
GROUP BY m.subreddit
ORDER BY videos DESC
""")

subreddits = [
    {"subreddit": r[0], "videos": r[1], "avg_reward": r[2]}
    for r in cursor.fetchall()
]

data = {
    "total_memes": total_memes,
    "total_videos": total_videos,
    "avg_reward": avg_reward,
    "subreddits": subreddits
}

with open(OUTPUT, "w+") as f:
    json.dump(data, f, indent=2)

print("Dashboard data exported")

conn.close()