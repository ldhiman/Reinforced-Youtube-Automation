import sqlite3

DB_PATH = "meme_ai.db"

LATEST_SCHEMA_VERSION = 5


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def get_db_version(cursor):
    cursor.execute("PRAGMA user_version")
    return cursor.fetchone()[0]


def set_db_version(cursor, version):
    cursor.execute(f"PRAGMA user_version = {version}")


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    current_version = get_db_version(cursor)

    if current_version == 0:
        print("Creating fresh database schema...")

        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='memes'
        """)
        table_exists = cursor.fetchone()

        if table_exists:
            print("Existing database detected. Migrating schema.")
            set_db_version(cursor, 1)
            conn.commit()
            current_version = 1
            
        else:
            cursor.execute("""
            CREATE TABLE memes (
                meme_id TEXT PRIMARY KEY,
                reddit_post_id TEXT UNIQUE,
                subreddit TEXT,
                title TEXT,
                ups INTEGER,
                template TEXT,
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

            cursor.execute("""
            CREATE TABLE videos (
                video_id TEXT PRIMARY KEY,
                meme_id TEXT,
                hook_style TEXT,
                music_type TEXT,
                video_length REAL,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (meme_id) REFERENCES memes(meme_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE analytics (
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

            cursor.execute("""
            CREATE TABLE subreddit_weights (
                subreddit TEXT PRIMARY KEY,
                weight REAL
            )
            """)

            cursor.execute("""
            CREATE TABLE template_weights (
                template TEXT PRIMARY KEY,
                weight REAL
            )
            """)

            set_db_version(cursor, 1)
            current_version = 1

    # -----------------------
    # Migration: Version 2
    # -----------------------
    if current_version < 2:
        print("Upgrading DB to version 2")

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_subreddit
        ON memes(subreddit)
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_used
        ON memes(used)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_memes_final_score
            ON memes(final_score)
        """)

        set_db_version(cursor, 2)
        current_version = 2

    # -----------------------
    # Migration: Version 3
    # -----------------------
    if current_version < 3:
        print("Upgrading DB to version 3")

        # Add template column if missing
        try:
            cursor.execute("""
            ALTER TABLE memes ADD COLUMN template TEXT
            """)
        except sqlite3.OperationalError:
            pass  # column already exists

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_template
        ON memes(template)
        """)

        set_db_version(cursor, 3)
        current_version = 3

    # -----------------------
    # Migration: Version 4
    # -----------------------
    if current_version < 4:
        print("Upgrading DB to version 4")

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_memes_used_score
        ON memes(used, final_score)
        """)

        set_db_version(cursor, 4)
        current_version = 4

    # -----------------------
    # Migration: Version 5
    # -----------------------
    if current_version < 5:
        print("Upgrading DB to version 5")

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_videos_meme
        ON videos(meme_id)
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_analytics_video
        ON analytics(video_id)
        """)

        set_db_version(cursor, 5)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()