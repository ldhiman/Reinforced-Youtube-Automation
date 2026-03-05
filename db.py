import sqlite3

DB_PATH = "meme_ai.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys = ON;")

    # Meme table
    cursor.execute("""
CREATE TABLE IF NOT EXISTS memes (
    meme_id TEXT PRIMARY KEY,
    reddit_post_id TEXT UNIQUE,
    subreddit TEXT,
    title TEXT,
    ups INTEGER,
    image_path TEXT,
    humor_score REAL,
    status TEXT DEFAULT 'new',
    relatability_score REAL,
    trend_score REAL,
    final_score REAL,
    used INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

    # Video table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        video_id TEXT PRIMARY KEY,
        meme_id TEXT,
        hook_style TEXT,
        music_type TEXT,
        video_length REAL,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (meme_id) REFERENCES memes(meme_id)
    )
    """)

    # Analytics table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS analytics (
        video_id TEXT PRIMARY KEY,
        views INTEGER,
        likes INTEGER,
        avg_watch_time REAL,
        completion_rate REAL,
        reward REAL,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (video_id) REFERENCES videos(video_id)
    )
    """)

    # Subreddit weights (for reinforcement)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS subreddit_weights (
        subreddit TEXT PRIMARY KEY,
        weight REAL
    )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS template_weights (
            template TEXT PRIMARY KEY,
            weight REAL
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_videos_meme
ON videos(meme_id);

    """)


    cursor.execute("""

CREATE INDEX IF NOT EXISTS idx_analytics_video
ON analytics(video_id);           

    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_subreddit 
        ON memes(subreddit)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_used 
        ON memes(used)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_used_score
        ON memes(used, final_score)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_template
        ON memes(template);
    """)

    cursor.execute("""
        ALTER TABLE memes ADD COLUMN template TEXT;                   
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_template
        ON memes(template);
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()