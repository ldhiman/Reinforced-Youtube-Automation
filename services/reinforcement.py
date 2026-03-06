import logging
from collections import defaultdict
from db import get_connection

logger = logging.getLogger(__name__)

LEARNING_RATE  = 0.08
MIN_WEIGHT     = 0.2
MAX_WEIGHT     = 4.0
MIN_SAMPLES    = 3        # raise to 3-5 once you have more data
RECENT_DAYS    = 21
VIEW_SMOOTH_K  = 2000


def _fetch_grouped(cursor, group_col: str) -> dict:
    """Generic fetch — groups analytics by subreddit or template."""
    cursor.execute(f"""
        SELECT m.{group_col}, a.reward, a.views
        FROM analytics a
        JOIN videos v  ON a.video_id = v.video_id
        JOIN memes  m  ON v.meme_id  = m.meme_id
        WHERE a.reward     IS NOT NULL
          AND a.fetched_at >= datetime('now', '-{RECENT_DAYS} days')
    """)
    grouped = defaultdict(list)
    for key, reward, views in cursor.fetchall():
        grouped[key].append({"reward": float(reward), "views": int(views)})
    return grouped


def _global_avg(grouped: dict) -> float:
    all_rewards = [r["reward"] for rows in grouped.values() for r in rows]
    return sum(all_rewards) / len(all_rewards) if all_rewards else 0.0


def _update_weight_table(cursor, table: str, key_col: str, grouped: dict, global_avg: float):
    updated = skipped = 0

    for key, rewards in grouped.items():
        if len(rewards) < MIN_SAMPLES:
            logger.debug(f"[{table}] '{key}' skipped — only {len(rewards)} sample(s)")
            skipped += 1
            continue

        total_views = sum(r["views"] for r in rewards)
        avg_reward  = sum(r["reward"] for r in rewards) / len(rewards)
        confidence  = total_views / (total_views + VIEW_SMOOTH_K)

        cursor.execute(
            f"SELECT weight FROM {table} WHERE {key_col} = ?", (key,)
        )
        row        = cursor.fetchone()
        old_weight = row[0] if row else 1.0

        delta      = (avg_reward - global_avg) * confidence
        new_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, old_weight + LEARNING_RATE * delta))

        cursor.execute(f"""
            INSERT INTO {table} ({key_col}, weight) VALUES (?, ?)
            ON CONFLICT({key_col}) DO UPDATE SET weight = excluded.weight
        """, (key, round(new_weight, 4)))

        logger.info(
            f"[{table}] '{key}' | "
            f"{round(old_weight, 3)} → {round(new_weight, 3)} | "
            f"conf={round(confidence, 3)} | "
            f"samples={len(rewards)} | "
            f"avg_reward={round(avg_reward, 4)}"
        )
        updated += 1

    logger.info(f"[{table}] {updated} updated, {skipped} skipped (< {MIN_SAMPLES} samples)")


def update_weights():
    conn   = get_connection()
    cursor = conn.cursor()

    try:
        # --- Subreddit weights ---
        sub_grouped = _fetch_grouped(cursor, "subreddit")
        if not sub_grouped:
            logger.warning("No reward data yet — run feedback_agent first.")
            return

        global_avg = _global_avg(sub_grouped)
        logger.info(f"Global avg reward: {round(global_avg, 4)} | subreddits: {len(sub_grouped)}")
        _update_weight_table(cursor, "subreddit_weights", "subreddit", sub_grouped, global_avg)

        # --- Template weights (only useful with larger datasets) ---
        tpl_grouped = _fetch_grouped(cursor, "template")
        if tpl_grouped:
            tpl_global_avg = _global_avg(tpl_grouped)
            _update_weight_table(cursor, "template_weights", "template", tpl_grouped, tpl_global_avg)
        else:
            logger.debug("No template data yet, skipping template weights.")

        conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    update_weights()