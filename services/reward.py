import math

CONFIDENCE_K    = 500   # raise to 2000+ once you have videos with 1k+ views
VIRAL_THRESHOLD = 1000  # views above this get a bonus multiplier


def calculate_reward(
    views: int,
    likes: int,
    avg_watch_time: float,
    completion_rate: float,
    video_length: float,
) -> float:

    if views <= 0 or video_length <= 0:
        return 0.0

    # Clamp inputs to valid ranges
    completion_rate = max(0.0, min(1.0, completion_rate))
    like_ratio      = likes / views

    # completion_rate already IS avg_watch_time / video_length from YouTube
    # so only use one of them — completion_rate is more reliable
    base_reward = (
        completion_rate * 0.55 +
        like_ratio      * 0.25 +
        min(like_ratio * 3, 0.20)  # bonus for unusually high like ratio, capped
    )

    # Confidence: low-view videos are discounted to avoid overfitting on flukes
    confidence = views / (views + CONFIDENCE_K)

    # Viral bonus: log-scaled so 10k views isn't 10x a 1k view video
    viral_bonus = math.log1p(max(0, views - VIRAL_THRESHOLD)) / 100

    reward = base_reward * confidence + viral_bonus

    return round(max(0.0, min(1.0, reward)), 4)