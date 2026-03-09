import random
import os
from collections import defaultdict
from db import get_connection

EXPLORATION_RATE = 0.15
TOP_K = 5


# ---------------------------
# Fetch subreddit weights
# ---------------------------
def get_subreddit_weights(cursor):

    cursor.execute("SELECT subreddit, weight FROM subreddit_weights")

    return {
        subreddit: weight
        for subreddit, weight in cursor.fetchall()
    }


# ---------------------------
# Fetch available memes grouped
# ---------------------------
def fetch_memes_grouped(cursor):

    cursor.execute("""
        SELECT meme_id, subreddit, final_score, image_path
        FROM memes
        WHERE final_score IS NOT NULL
          AND used = 0
        ORDER BY final_score DESC
    """)

    grouped = defaultdict(list)

    for meme_id, subreddit, score, image_path in cursor.fetchall():

        grouped[subreddit].append({
            "meme_id": meme_id,
            "subreddit": subreddit,
            "final_score": score,
            "image_path": image_path
        })

    return grouped


# ---------------------------
# Weighted subreddit selection
# ---------------------------
def weighted_subreddit_choice(grouped, weights):

    available = list(grouped.keys())

    if not available:
        return None

    probs = [
        max(weights.get(sub, 0.5), 0.05)
        for sub in available
    ]

    return random.choices(available, weights=probs, k=1)[0]


# ---------------------------
# Remove missing files
# ---------------------------
def remove_missing_files(cursor):

    cursor.execute("""
        SELECT meme_id, image_path
        FROM memes
        WHERE used = 0
    """)

    rows = cursor.fetchall()

    removed = 0

    for meme_id, path in rows:

        if not path or not os.path.exists(path):

            cursor.execute(
                "DELETE FROM memes WHERE meme_id = ?",
                (meme_id,)
            )

            removed += 1

    if removed:
        print(f"Cleaned {removed} missing meme files")


# ---------------------------
# Main Selection Function
# ---------------------------
def select_memes(daily_count: int = 5):

    conn = get_connection()
    cursor = conn.cursor()

    remove_missing_files(cursor)
    conn.commit()

    grouped = fetch_memes_grouped(cursor)
    weights = get_subreddit_weights(cursor)

    selected = []
    used_memes = set()

    if not grouped:
        conn.close()
        return []

    print("Total available memes:", sum(len(v) for v in grouped.values()))
    print("Subreddit distribution:", {k: len(v) for k,v in grouped.items()})

    attempts = 0
    MAX_ATTEMPTS = daily_count * 10

    while len(selected) < daily_count and attempts < MAX_ATTEMPTS:

        attempts += 1

        subreddit = weighted_subreddit_choice(grouped, weights)

        if not subreddit:
            break

        available_memes = [
            m for m in grouped[subreddit]
            if m["meme_id"] not in used_memes
        ]

        if not available_memes:
            grouped.pop(subreddit, None)
            continue

        if random.random() < EXPLORATION_RATE:

            sample_pool = (
                available_memes[:TOP_K]
                if len(available_memes) > TOP_K
                else available_memes
            )

            chosen = random.choice(sample_pool)

        else:
            chosen = available_memes[0]

        selected.append(chosen)

        used_memes.add(chosen["meme_id"])

        # soft diversity
        weights[subreddit] = weights.get(subreddit, 1.0) * 0.5

    conn.close()

    return selected


if __name__ == "__main__":
    print(select_memes(3))