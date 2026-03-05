import sqlite3
from collections import defaultdict
from db import get_connection


LEARNING_RATE = 0.08
MIN_WEIGHT = 0.2
MAX_WEIGHT = 4.0
MIN_SAMPLES = 5
RECENT_DAYS = 21
VIEW_SMOOTH_K = 2000  # confidence smoothing

def fetch_template_rewards(cursor):
    cursor.execute(f"""
        SELECT m.template, a.reward, a.views
        FROM analytics a
        JOIN videos v ON a.video_id = v.video_id
        JOIN memes m ON v.meme_id = m.meme_id
        WHERE a.reward IS NOT NULL
        AND a.fetched_at >= datetime('now', '-{RECENT_DAYS} days')
    """)

    rows = cursor.fetchall()

    grouped = defaultdict(list)

    for template, reward, views in rows:
        grouped[template].append((reward, views))

    return grouped

def fetch_reward_data(cursor):
    cursor.execute(f"""
        SELECT m.subreddit, a.reward, a.views
        FROM analytics a
        JOIN videos v ON a.video_id = v.video_id
        JOIN memes m ON v.meme_id = m.meme_id
        WHERE a.reward IS NOT NULL
        AND a.fetched_at >= datetime('now', '-{RECENT_DAYS} days')
    """)

    rows = cursor.fetchall()

    grouped = defaultdict(list)

    for subreddit, reward, views in rows:
        grouped[subreddit].append({
            "reward": reward,
            "views": views
        })

    return grouped


def get_global_average(grouped):
    all_rewards = []

    for rewards in grouped.values():
        for r in rewards:
            all_rewards.append(r["reward"])

    if not all_rewards:
        return 0.0

    return sum(all_rewards) / len(all_rewards)


def compute_subreddit_stats(rewards):
    total_views = sum(r["views"] for r in rewards)
    avg_reward = sum(r["reward"] for r in rewards) / len(rewards)

    confidence = total_views / (total_views + VIEW_SMOOTH_K)

    return avg_reward, confidence

def update_template_weights():

    conn = get_connection()
    cursor = conn.cursor()

    grouped = fetch_template_rewards(cursor)

    print(f"Updating weights for {len(grouped)} subreddits")

    if not grouped:
        conn.close()
        return
    
    # compute global average reward
    all_rewards = []
    for rewards in grouped.values():
        for r, _ in rewards:
            all_rewards.append(r)

    global_avg = sum(all_rewards) / len(all_rewards)

    for template, rewards in grouped.items():

        if len(rewards) < MIN_SAMPLES:
            continue

        total_views = sum(v for _, v in rewards)
        avg_reward = sum(r for r, _ in rewards) / len(rewards)

        confidence = total_views / (total_views + VIEW_SMOOTH_K)

        cursor.execute(
            "SELECT weight FROM template_weights WHERE template = ?",
            (template,)
        )

        row = cursor.fetchone()
        old_weight = row[0] if row else 1.0

        delta = avg_reward - global_avg
        adjusted_delta = delta * confidence

        new_weight = old_weight + LEARNING_RATE * adjusted_delta

        new_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, new_weight))

        cursor.execute("""
            INSERT INTO template_weights (template, weight)
            VALUES (?, ?)
            ON CONFLICT(template)
            DO UPDATE SET weight = excluded.weight
        """, (template, new_weight))

    conn.commit()
    conn.close()

def update_weights():
    conn = get_connection()
    cursor = conn.cursor()

    grouped = fetch_reward_data(cursor)

    print(f"Updating weights for {len(grouped)} subreddits")

    if not grouped:
        print("No reward data yet.")
        conn.close()
        return

    global_avg = get_global_average(grouped)

    print(f"\nGlobal Avg Reward: {round(global_avg, 4)}\n")

    for subreddit, rewards in grouped.items():

        if len(rewards) < MIN_SAMPLES:
            print(f"{subreddit} skipped (only {len(rewards)} samples)")
            continue

        sub_avg, confidence = compute_subreddit_stats(rewards)

        cursor.execute(
            "SELECT weight FROM subreddit_weights WHERE subreddit = ?",
            (subreddit,)
        )
        row = cursor.fetchone()
        old_weight = row[0] if row else 1.0

        delta = sub_avg - global_avg

        # 🔥 Confidence-weighted update
        adjusted_delta = delta * confidence

        new_weight = old_weight + LEARNING_RATE * adjusted_delta

        new_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, new_weight))

        cursor.execute("""
            INSERT INTO subreddit_weights (subreddit, weight)
            VALUES (?, ?)
            ON CONFLICT(subreddit)
            DO UPDATE SET weight = excluded.weight
        """, (subreddit, new_weight))

        print(
            f"{subreddit} | "
            f"Old: {round(old_weight,3)} → "
            f"New: {round(new_weight,3)} | "
            f"Conf: {round(confidence,3)} | "
            f"Samples: {len(rewards)} | "
            f"Sub Avg: {round(sub_avg,4)}"
        )

    conn.commit()
    conn.close()

    
    update_template_weights()


if __name__ == "__main__":
    update_weights()