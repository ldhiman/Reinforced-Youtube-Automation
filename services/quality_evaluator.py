import sqlite3
from db import get_connection
import math
import time


DEFAULT_SCORE = 1.0

# -----------------------------
# Configurable weights
# -----------------------------
SUB_WEIGHT = 0.5
TEMPLATE_WEIGHT = 0.3
UPVOTE_WEIGHT = 0.15
FRESHNESS_WEIGHT = 0.05


# -----------------------------
# Load weights once
# -----------------------------
def load_subreddit_weights(cursor):

    cursor.execute("SELECT subreddit, weight FROM subreddit_weights")

    return {
        row[0]: row[1]
        for row in cursor.fetchall()
    }


def load_template_weights(cursor):

    cursor.execute("SELECT template, weight FROM template_weights")

    return {
        row[0]: row[1]
        for row in cursor.fetchall()
    }


# -----------------------------
# Freshness scoring
# -----------------------------
def compute_freshness(created_at):

    if not created_at:
        return 1.0

    age_hours = (time.time() - created_at) / 3600

    return max(0.5, 1 / (1 + age_hours / 24))


# -----------------------------
# Meme scoring
# -----------------------------
def evaluate_memes():

    conn = get_connection()
    cursor = conn.cursor()

    subreddit_weights = load_subreddit_weights(cursor)
    template_weights = load_template_weights(cursor)

    cursor.execute("""
        SELECT meme_id, subreddit, template, ups, strftime('%s', created_at)
        FROM memes
        WHERE final_score IS NULL
    """)

    rows = cursor.fetchall()

    updates = []

    for meme_id, subreddit, template, ups, created_ts in rows:

        subreddit_weight = subreddit_weights.get(subreddit, DEFAULT_SCORE)

        template_weight = template_weights.get(template, DEFAULT_SCORE)

        upvote_score = math.log1p(ups) / 8

        freshness_score = compute_freshness(int(created_ts) if created_ts else None)

        predicted_score = (
            subreddit_weight * SUB_WEIGHT +
            template_weight * TEMPLATE_WEIGHT +
            upvote_score * UPVOTE_WEIGHT +
            freshness_score * FRESHNESS_WEIGHT
        )

        predicted_score = max(0.1, min(5.0, predicted_score))

        updates.append((predicted_score, meme_id))

    cursor.executemany("""
        UPDATE memes
        SET final_score = ?
        WHERE meme_id = ?
    """, updates)

    conn.commit()
    conn.close()

    print(f"Evaluated {len(rows)} memes")