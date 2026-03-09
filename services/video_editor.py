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

    # 1. Create the mask from the alpha channel
    mask = warped[:, :, 3] > 0
    
    # 2. Extract the RGB channels from the warped meme
    meme_rgb = warped[:, :, :3]
    
    # 3. Copy the meme onto the background frame only where the mask is active
    frame[mask] = meme_rgb[mask]
    
    # 4. Optional: Add that slight brightness boost to the meme itself to make it "pop"
    frame[mask] = cv2.add(frame[mask], np.array([12, 12, 12], dtype=np.uint8))

def enhance_meme_visibility(image: np.ndarray) -> np.ndarray:
    """Applies local contrast and saturation boost to make memes stand out."""
    if image is None: return None
    
    # Drop Alpha channel if it exists for processing
    has_alpha = image.shape[2] == 4
    if has_alpha:
        alpha = image[:, :, 3]
        bgr = cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)
    else:
        bgr = image.copy()

    # Convert to LAB for lightness-only contrast enhancement (CLAHE)
    lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    
    # ClipLimit 2.0 makes the text and image pop without looking "deep fried"
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    cl = clahe.apply(l)
    
    enhanced = cv2.merge((cl, a, b))
    enhanced = cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)
    
    # Boost saturation slightly for mobile screens
    hsv = cv2.cvtColor(enhanced, cv2.COLOR_BGR2HSV).astype("float32")
    hsv[:, :, 1] *= 1.15 
    hsv[:, :, 1] = np.clip(hsv[:, :, 1], 0, 255)
    enhanced = cv2.cvtColor(hsv.astype("uint8"), cv2.COLOR_HSV2BGR)

    if has_alpha:
        enhanced = cv2.cvtColor(enhanced, cv2.COLOR_BGR2BGRA)
        enhanced[:, :, 3] = alpha
        
    return enhanced

def _normalize_brightness(frame: np.ndarray, target_brightness: float = 135.0) -> np.ndarray:
    """
    Adjusts the frame brightness to a consistent target level.
    128.0 is neutral; 135-145 is the 'YouTube Sweet Spot' for vibrant content.
    """
    # Convert to LAB color space
    lab = cv2.cvtColor(frame, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)

    # Calculate current average brightness
    avg_l = np.mean(l)
    
    # Calculate the ratio needed to hit the target
    # We use a cap to prevent extreme over-exposure
    ratio = target_brightness / (avg_l + 1e-6)
    ratio = np.clip(ratio, 0.85, 1.25) # Stay within safe 'CapCut' style limits

    # Apply the shift
    l = cv2.multiply(l, ratio)
    l = np.clip(l, 0, 255).astype(np.uint8)

    # Merge back
    enhanced_lab = cv2.merge((l, a, b))
    return cv2.cvtColor(enhanced_lab, cv2.COLOR_LAB2BGR)

def prepare_meme(path: str, max_w: int, max_h: int) -> np.ndarray:
    img = cv2.imread(path, cv2.IMREAD_UNCHANGED)
    if img is None:
        return None
    
    # Ensure it has 4 channels for the Ken Burns mask logic
    if img.shape[2] == 3:
        img = cv2.cvtColor(img, cv2.COLOR_BGR2BGRA)

    img = enhance_meme_visibility(img)
    
    h, w = img.shape[:2]
    scale = min(max_w / w, max_h / h)
    resized = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_LANCZOS4)
    
    # Sharpness boost
    gaussian = cv2.GaussianBlur(resized, (0, 0), 2.0)
    resized = cv2.addWeighted(resized, 1.5, gaussian, -0.5, 0)
    
    return resized # <--- MUST HAVE THIS

def sharpen(frame, strength=0.6):

    blur = cv2.GaussianBlur(frame, (0,0), 1.2)

    return cv2.addWeighted(frame, 1 + strength, blur, -strength, 0)

def add_grain(frame, strength=4):

    noise = np.random.normal(0, strength, frame.shape).astype(np.int16)

    frame = frame.astype(np.int16) + noise

    return np.clip(frame, 0, 255).astype(np.uint8)

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

    margin_w = width * 0.15  # 15% side margins
    margin_h = height * 0.20 # 20% top/bottom margins for UI
    meme_duration   = 3.0
    total_frames    = int(meme_duration * fps)
    max_w, max_h = width - int(margin_w * 2), height - int(margin_h * 2)
    center_x, center_y = width // 2, height // 2

    # Flash lasts this many frames at meme entry
    flash_frames    = max(3, int(fps * 0.12))  # ~3 frames at 24fps, scales with fps

    frames_written  = 0
    last_preset     = -1
    total_memes     = len(meme_paths)

    total_video_duration = (total_memes * total_frames) / fps


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

            is_last_frame = (meme_index == total_memes - 1) and (i == total_frames - 1)

            progress = i / total_frames

            # Background subtle zoom
            bg_scale = 1 + 0.07 * (progress ** 1.5)
            bg_w, bg_h = int(width * bg_scale), int(height * bg_scale)
            bg_resized = cv2.resize(frame, (bg_w, bg_h), interpolation=cv2.INTER_LANCZOS4)
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
            frame = _normalize_brightness(frame, target_brightness=140.0)
            frame = _apply_vignette(frame, strength=0.55)
            frame = sharpen(frame, 0.4)
            frame = add_grain(frame, 1)

            # Flash cut — only on first N frames of each meme entry
            if i < int(fps * 0.12) and meme_index > 0:
                frame = _apply_flash(frame, i / (fps * 0.12))

            # Segmented progress bar with glow
            _draw_progress_bar(frame, progress, meme_index, total_memes)

            # Viral Color Grade: Boost contrast, reduce muddy greens
            frame = cv2.convertScaleAbs(frame, alpha=1.05, beta=2)
            # Subtle Blue-Shift (YouTube likes cool-tones for tech/meme content)
            frame[:, :, 0] = cv2.add(frame[:, :, 0], 3) # Blue
            
            # Final Sharpening pass for 1080p clarity
            frame = sharpen(frame, 0.3)
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
        "-preset", "veryslow",
        "-crf", "15",
        "-tune", "film",
        "-profile:v", "high",
        "-level", "4.2",
        "-threads", "0",
        "-pix_fmt", "yuv420p",
        "-colorspace", "bt709",
        "-color_trc", "bt709",
        "-color_primaries", "bt709",
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-b:a", "256k",
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