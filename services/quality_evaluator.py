import sqlite3
from db import get_connection
import math


DEFAULT_SCORE = 1.0

def get_template_weight(cursor, template):
    cursor.execute(
        "SELECT weight FROM template_weights WHERE template = ?",
        (template,)
    )

    row = cursor.fetchone()
    return row[0] if row else DEFAULT_SCORE

def get_subreddit_weight(cursor, subreddit: str) -> float:
    cursor.execute(
        "SELECT weight FROM subreddit_weights WHERE subreddit = ?",
        (subreddit,)
    )
    row = cursor.fetchone()

    if row:
        return row[0]

    return DEFAULT_SCORE


def evaluate_memes():
    """
    Score memes purely based on learned subreddit weights.
    No heuristics. No Reddit upvotes.
    Pure analytics-driven.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT meme_id, subreddit, template, ups
        FROM memes
        WHERE final_score IS NULL
    """)

    rows = cursor.fetchall()

    updates = []

    for meme_id, subreddit, template, ups in rows:
        
        subreddit_weight = get_subreddit_weight(cursor, subreddit)
        template_weight = get_template_weight(cursor, template)

        # Reddit signal
        upvote_score = math.log1p(ups) / 10

        predicted_score = (
            subreddit_weight * 0.5 +
            template_weight * 0.3 +
            upvote_score * 0.2
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

    print(f"Evaluated {len(rows)} memes using analytics-only scoring.")