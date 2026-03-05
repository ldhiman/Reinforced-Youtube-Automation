CONFIDENCE_K = 500  # smoothing factor


def calculate_reward(
    views: int,
    likes: int,
    avg_watch_time: float,
    completion_rate: float,
    video_length: float
) -> float:

    if views <= 0 or video_length <= 0:
        return 0.0

    like_ratio = likes / views
    normalized_watch = avg_watch_time / video_length

    base_reward = (
        completion_rate * 0.5 +
        normalized_watch * 0.3 +
        like_ratio * 0.2
    )

    # Confidence weighting
    confidence = views / (views + CONFIDENCE_K)

    weighted_reward = base_reward * confidence

    return round(weighted_reward, 4)