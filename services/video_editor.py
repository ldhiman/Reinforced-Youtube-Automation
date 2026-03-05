import cv2
import uuid
import os
import subprocess
import numpy as np
import logging
from typing import List

logger = logging.getLogger(__name__)

def apply_ken_burns_effect(frame: np.ndarray, image: np.ndarray, progress: float, 
                           start_scale: float, end_scale: float, start_x: float, start_y: float, end_x: float, end_y: float, opacity: float = 1.0):
    """
    Applies a Ken Burns effect (zoom and pan) to an image and overlays it on a frame.
    Uses affine transformations for sub-pixel smoothness and high-quality interpolation.
    """
    # Ensure image has alpha channel for proper border handling
    if image.shape[2] == 3:
        image = cv2.cvtColor(image, cv2.COLOR_BGR2BGRA)

    h, w = image.shape[:2]
    h_bg, w_bg = frame.shape[:2]

    # Apply Smoothstep easing for more natural movement
    progress = progress * progress * (3 - 2 * progress)

    # Interpolate scale and position
    current_scale = start_scale + (end_scale - start_scale) * progress
    current_x = start_x + (end_x - start_x) * progress
    current_y = start_y + (end_y - start_y) * progress

    # Calculate the affine transformation matrix
    # We want the center of the source image (w/2, h/2) to map to (current_x, current_y)
    tx = current_x - (w / 2) * current_scale
    ty = current_y - (h / 2) * current_scale
    
    M = np.float32([
        [current_scale, 0, tx],
        [0, current_scale, ty]
    ])

    # Warp the image using Lanczos interpolation for best quality
    warped = cv2.warpAffine(image, M, (w_bg, h_bg), flags=cv2.INTER_LANCZOS4, borderMode=cv2.BORDER_CONSTANT, borderValue=(0, 0, 0, 0))

    # Copy the warped image to the frame using the alpha channel as a mask
    mask = warped[:, :, 3] > 0
    frame[mask] = warped[mask, :3]

def prepare_meme(path: str, max_w: int, max_h: int) -> np.ndarray:
    """
    Loads, resizes, and prepares a meme image for overlaying.
    """
    img = cv2.imread(path, cv2.IMREAD_UNCHANGED)
    if img is None:
        raise ValueError(f"Could not load image: {path}")

    h, w = img.shape[:2]
    scale = min(max_w / w, max_h / h)
    new_w, new_h = int(w * scale), int(h * scale)
    return cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)

def insert_memes(
    meme_paths: List[str],
    bg_video: str = "./services/bg_video.mp4",
    bg_audio: str = "./services/bg_audio.mpeg"
) -> str:

    output_dir = "storage/outputs"
    os.makedirs(output_dir, exist_ok=True)

    temp_video_path = os.path.join(output_dir, f"{uuid.uuid4()}_temp.mp4")
    final_output_path = os.path.join(output_dir, f"{uuid.uuid4()}.mp4")

    if not os.path.exists(bg_video):
        raise FileNotFoundError(f"Background video not found: {bg_video}")

    cap = cv2.VideoCapture(bg_video)
    if not cap.isOpened():
        raise RuntimeError("Could not open background video.")

    fps = cap.get(cv2.CAP_PROP_FPS) or 24
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(temp_video_path, fourcc, fps, (width, height))

    margin = 30
    meme_duration = 3.0
    total_frames = int(meme_duration * fps)
    max_w, max_h = width - margin * 2, height - margin * 2

    center_x, center_y = width / 2, height / 2

    frames_written = 0

    for meme_path in meme_paths:

        if not os.path.exists(meme_path):
            logger.error(f"Meme file missing: {meme_path}")
            continue

        try:
            meme = prepare_meme(meme_path, max_w, max_h)
        except Exception as e:
            logger.error(f"Failed to load meme {meme_path}: {e}")
            continue

        zoom_in = np.random.choice([True, False])
        start_scale = 1.0 if zoom_in else 1.1
        end_scale = 1.1 if zoom_in else 1.0

        pan_x = np.random.randint(-20, 21)
        pan_y = np.random.randint(-20, 21)

        brightness_factor = np.random.uniform(0.98, 1.02)

        for i in range(total_frames):

            ret, frame = cap.read()
            if not ret:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                ret, frame = cap.read()
                if not ret:
                    break

            progress = i / total_frames

            bg_scale = 1 + 0.05 * progress
            bg_w, bg_h = int(width * bg_scale), int(height * bg_scale)

            bg_resized = cv2.resize(frame, (bg_w, bg_h), interpolation=cv2.INTER_LINEAR)

            x_offset = (bg_w - width) // 2
            y_offset = (bg_h - height) // 2
            frame = bg_resized[y_offset:y_offset + height, x_offset:x_offset + width]

            frame = cv2.convertScaleAbs(frame, alpha=brightness_factor, beta=0)

            apply_ken_burns_effect(
                frame, meme, progress,
                start_scale=start_scale,
                end_scale=end_scale,
                start_x=center_x,
                start_y=center_y,
                end_x=center_x + pan_x,
                end_y=center_y + pan_y,
                opacity=1.0
            )

            out.write(frame)
            frames_written += 1

    cap.release()
    out.release()

    # 🔴 Critical safety check
    if frames_written == 0:
        logger.error("No frames written. Aborting video generation.")
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        raise RuntimeError("Video rendering failed — no frames produced.")

    if not os.path.exists(bg_audio):
        logger.error("Background audio missing.")
        return temp_video_path

    merge_command = [
        "ffmpeg", "-y",
        "-i", temp_video_path,
        "-stream_loop", "-1",
        "-i", bg_audio,
        "-map", "0:v:0?",   # safer mapping
        "-map", "1:a:0?",
        "-c:v", "libx264",
        "-preset", "slow",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        final_output_path
    ]

    try:
        subprocess.run(merge_command, check=True)
        os.remove(temp_video_path)
    except subprocess.CalledProcessError as e:
        logger.error("FFmpeg failed.")
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        raise RuntimeError("FFmpeg processing failed.") from e

    return final_output_path