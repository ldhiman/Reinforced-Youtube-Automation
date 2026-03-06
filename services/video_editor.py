import cv2
import uuid
import os
import subprocess
import numpy as np
import logging
from typing import List

logger = logging.getLogger(__name__)

KB_PRESETS = [
    (1.00, 1.12, 0, 0, 0, 0, "zoom_in_center"),
    (1.12, 1.00, 0, 0, 0, 0, "zoom_out_center"),
]

_last_preset_index: int = -1

# -----------------------------
# Vignette — precomputed once per resolution
# -----------------------------
_vignette_cache: dict = {}

def _get_vignette(width: int, height: int, strength: float = 0.6) -> np.ndarray:
    """Returns a float32 vignette mask (H, W, 1), cached per resolution."""
    key = (width, height, strength)
    if key not in _vignette_cache:
        cx, cy = width / 2, height / 2
        Y, X = np.ogrid[:height, :width]
        # Normalised distance from center (0=center, 1=corner)
        dist = np.sqrt(((X - cx) / cx) ** 2 + ((Y - cy) / cy) ** 2)
        # Smooth falloff: 1 at center, (1-strength) at corners
        mask = 1.0 - strength * np.clip(dist, 0, 1) ** 1.8
        _vignette_cache[key] = mask.astype(np.float32)[..., np.newaxis]
    return _vignette_cache[key]


def _apply_vignette(frame: np.ndarray, strength: float = 0.6) -> np.ndarray:
    h, w = frame.shape[:2]
    mask = _get_vignette(w, h, strength)
    return np.clip(frame.astype(np.float32) * mask, 0, 255).astype(np.uint8)


# -----------------------------
# Flash cut — white flash that fades out over N frames
# -----------------------------
def _apply_flash(frame: np.ndarray, flash_progress: float) -> np.ndarray:
    """
    flash_progress: 0.0 = full flash, 1.0 = no flash.
    Blends white over the frame with decreasing opacity.
    """
    intensity = max(0.0, 1.0 - flash_progress)  # fades from 1 → 0
    if intensity < 0.01:
        return frame
    white = np.full_like(frame, 255)
    return cv2.addWeighted(frame, 1.0 - intensity * 0.75, white, intensity * 0.75, 0)


# -----------------------------
# Progress bar — segmented with glow
# -----------------------------
def _draw_progress_bar(
    frame: np.ndarray,
    progress: float,          # 0.0 → 1.0 within current meme
    meme_index: int,          # which meme we're on (0-based)
    total_memes: int,         # total memes in this video
) -> None:
    """
    Draws a segmented progress bar — one segment per meme.
    Completed segments are solid white, current segment fills left→right,
    future segments are dim. Includes a glow line above the bar.
    """
    h, w = frame.shape[:2]
    bar_h      = 6
    glow_h     = 2
    bar_y      = h - bar_h
    glow_y     = bar_y - glow_h
    gap        = 4           # pixels between segments
    bar_color  = (255, 255, 255)
    dim_color  = (80, 80, 80)
    glow_color = (180, 180, 255)

    seg_w = (w - gap * (total_memes - 1)) // total_memes

    for i in range(total_memes):
        x0 = i * (seg_w + gap)
        x1 = x0 + seg_w

        if i < meme_index:
            # Fully completed segment
            cv2.rectangle(frame, (x0, bar_y), (x1, h), bar_color, -1)

        elif i == meme_index:
            # Dim background of current segment
            cv2.rectangle(frame, (x0, bar_y), (x1, h), dim_color, -1)
            # Fill proportional to progress
            fill_x = x0 + int(seg_w * progress)
            if fill_x > x0:
                cv2.rectangle(frame, (x0, bar_y), (fill_x, h), bar_color, -1)
            # Glow line above active fill
            if fill_x > x0:
                cv2.rectangle(frame, (x0, glow_y), (fill_x, bar_y), glow_color, -1)

        else:
            # Future segment — dim
            cv2.rectangle(frame, (x0, bar_y), (x1, h), dim_color, -1)


# -----------------------------
# Ken Burns
# -----------------------------
def _pick_preset(exclude_index: int = -1) -> tuple:
    global _last_preset_index
    candidates = [i for i in range(len(KB_PRESETS)) if i != exclude_index]
    idx = np.random.choice(candidates)
    _last_preset_index = idx
    preset = KB_PRESETS[idx]
    logger.debug(f"Ken Burns preset: {preset[-1]}")
    return preset


def apply_ken_burns_effect(
    frame: np.ndarray,
    image: np.ndarray,
    progress: float,
    start_scale: float,
    end_scale: float,
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
):
    if image.shape[2] == 3:
        image = cv2.cvtColor(image, cv2.COLOR_BGR2BGRA)

    h, w = image.shape[:2]
    h_bg, w_bg = frame.shape[:2]

    progress = progress * progress * (3 - 2 * progress)  # smoothstep

    current_scale = start_scale + (end_scale - start_scale) * progress
    current_x     = start_x + (end_x - start_x) * progress
    current_y     = start_y + (end_y - start_y) * progress

    tx = current_x - (w / 2) * current_scale
    ty = current_y - (h / 2) * current_scale

    M = np.float32([[current_scale, 0, tx], [0, current_scale, ty]])

    warped = cv2.warpAffine(
        image, M, (w_bg, h_bg),
        flags=cv2.INTER_LANCZOS4,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=(0, 0, 0, 0),
    )

    mask = warped[:, :, 3] > 0
    frame[mask] = warped[mask, :3]


def prepare_meme(path: str, max_w: int, max_h: int) -> np.ndarray:
    img = cv2.imread(path, cv2.IMREAD_UNCHANGED)
    if img is None:
        raise ValueError(f"Could not load image: {path}")
    h, w = img.shape[:2]
    scale = min(max_w / w, max_h / h)
    return cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)


# -----------------------------
# MAIN
# -----------------------------
def insert_memes(
    meme_paths: List[str],
    bg_video: str = "./services/bg_video.mp4",
    bg_audio: str = "./services/bg_audio.mpeg",
) -> str:

    output_dir = "storage/outputs"
    os.makedirs(output_dir, exist_ok=True)

    temp_video_path   = os.path.join(output_dir, f"{uuid.uuid4()}_temp.mp4")
    final_output_path = os.path.join(output_dir, f"{uuid.uuid4()}.mp4")

    if not os.path.exists(bg_video):
        raise FileNotFoundError(f"Background video not found: {bg_video}")

    cap = cv2.VideoCapture(bg_video)
    if not cap.isOpened():
        raise RuntimeError("Could not open background video.")

    fps    = cap.get(cv2.CAP_PROP_FPS) or 24
    width  = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out    = cv2.VideoWriter(temp_video_path, fourcc, fps, (width, height))

    margin          = 30
    meme_duration   = 3.0
    total_frames    = int(meme_duration * fps)
    max_w, max_h    = width - margin * 2, height - margin * 2
    center_x, center_y = width / 2, height / 2

    # Flash lasts this many frames at meme entry
    flash_frames    = max(3, int(fps * 0.12))  # ~3 frames at 24fps, scales with fps

    frames_written  = 0
    last_preset     = -1
    total_memes     = len(meme_paths)

    for meme_index, meme_path in enumerate(meme_paths):
        if not os.path.exists(meme_path):
            logger.error(f"Meme file missing: {meme_path}")
            continue

        try:
            meme = prepare_meme(meme_path, max_w, max_h)
        except Exception as e:
            logger.error(f"Failed to load meme {meme_path}: {e}")
            continue

        preset      = _pick_preset(exclude_index=last_preset)
        last_preset = KB_PRESETS.index(preset)

        s_scale, e_scale, sx_off, sy_off, ex_off, ey_off, label = preset
        brightness_factor = np.random.uniform(0.98, 1.02)

        logger.debug(f"Rendering '{os.path.basename(meme_path)}' | preset='{label}'")

        for i in range(total_frames):
            ret, frame = cap.read()
            if not ret:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                ret, frame = cap.read()
                if not ret:
                    break

            progress = i / total_frames

            # Background subtle zoom
            bg_scale   = 1 + 0.05 * progress
            bg_w, bg_h = int(width * bg_scale), int(height * bg_scale)
            bg_resized = cv2.resize(frame, (bg_w, bg_h), interpolation=cv2.INTER_LINEAR)
            x_off      = (bg_w - width) // 2
            y_off      = (bg_h - height) // 2
            frame      = bg_resized[y_off:y_off + height, x_off:x_off + width]

            frame = cv2.convertScaleAbs(frame, alpha=brightness_factor, beta=0)

            # Ken Burns meme overlay
            apply_ken_burns_effect(
                frame, meme, progress,
                start_scale=s_scale, end_scale=e_scale,
                start_x=center_x + sx_off, start_y=center_y + sy_off,
                end_x=center_x + ex_off,   end_y=center_y + ey_off,
            )

            # Vignette — draws eye to center, away from edges
            frame = _apply_vignette(frame, strength=0.55)

            # Flash cut — only on first N frames of each meme entry
            if i < flash_frames:
                flash_progress = i / flash_frames  # 0→1 as flash fades
                frame = _apply_flash(frame, flash_progress)

            # Segmented progress bar with glow
            _draw_progress_bar(frame, progress, meme_index, total_memes)

            out.write(frame)
            frames_written += 1

    cap.release()
    out.release()

    if frames_written == 0:
        logger.error("No frames written. Aborting.")
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
        "-map", "0:v:0?",
        "-map", "1:a:0?",
        "-c:v", "libx264",
        "-preset", "slow",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        final_output_path,
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