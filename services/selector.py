import sqlite3
import random
from collections import defaultdict
from db import DB_PATH
import os
import random

EXPLORATION_RATE = 0.15
TOP_K = 5


# ---------------------------
# Fetch subreddit weights
# ---------------------------
def get_subreddit_weights(cursor):
    cursor.execute("SELECT subreddit, weight FROM subreddit_weights")
    rows = cursor.fetchall()
    return {sub: weight for sub, weight in rows}


# ---------------------------
# Fetch available memes grouped
# ---------------------------
def fetch_memes_grouped(cursor):
    cursor.execute("""
        SELECT meme_id, subreddit, final_score, image_path
        FROM memes
        WHERE final_score IS NOT NULL
          AND used = 0
    """)

    rows = cursor.fetchall()

    grouped = defaultdict(list)

    for meme_id, subreddit, score, image_path in rows:
        grouped[subreddit].append({
            "meme_id": meme_id,
            "subreddit": subreddit,
            "final_score": score,
            "image_path": image_path
        })

    # Sort each subreddit by score descending
    for subreddit in grouped:
        grouped[subreddit].sort(
            key=lambda x: x["final_score"],
            reverse=True
        )

    return grouped


# ---------------------------
# Weighted subreddit selection
# ---------------------------
def weighted_subreddit_choice(grouped, weights):
    available_subs = list(grouped.keys())

    if not available_subs:
        return None

    weighted_list = []
    for sub in available_subs:
        weight = weights.get(sub, 1.0)
        weighted_list.append(max(weight, 0.01))  # prevent zero probability

    return random.choices(available_subs, weights=weighted_list, k=1)[0]


# ---------------------------
# Main Selection Function
# ---------------------------
def select_memes(daily_count: int = 5):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    remove_missing_files(cursor)

    grouped = fetch_memes_grouped(cursor)
    weights = get_subreddit_weights(cursor)

    selected = []
    used_local = set()

    if not grouped:
        conn.close()
        return []

    while len(selected) < daily_count and grouped:
        subreddit = weighted_subreddit_choice(grouped, weights)

        if not subreddit:
            break

        memes = grouped.get(subreddit, [])

        # Remove already locally selected memes
        memes = [m for m in memes if m["meme_id"] not in used_local]

        if not memes:
            # remove empty subreddit
            grouped.pop(subreddit, None)
            continue

        if random.random() < EXPLORATION_RATE:
            chosen = random.choice(memes[:TOP_K])   # exploration
        else:
            chosen = memes[0] # exploitation
        selected.append(chosen)
        used_local.add(chosen["meme_id"])

    conn.close()
    return selected

def remove_missing_files(cursor):
    cursor.execute("""
        SELECT meme_id, image_path FROM memes
        WHERE used = 0
    """)
    
    rows = cursor.fetchall()
    
    for meme_id, path in rows:
        if not path or not os.path.exists(path):
            print(f"Cleaning missing file entry: {meme_id}")
            cursor.execute(
                "DELETE FROM memes WHERE meme_id = ?",
                (meme_id,)
            )