# =========================================================================
# 👁️ VISION INSTRUMENT: Zero-Disk I/O Media Processor
# =========================================================================
# PURPOSE: Bridge between raw Telegram media URLs and The Visionary VLM.
#   - Evades anti-hotlinking (spoofed User-Agent/Referer via FFmpeg env)
#   - Streams video directly into RAM (no disk writes)
#   - Extracts top-3 keyframes via scene-change delta detection
#   - Compresses and encodes frames to Base64 data URLs
#
# HARD CONSTRAINTS:
#   - ZERO disk I/O (serverless constraint)
#   - Graceful failure: dead links / 403s return [] silently
#   - Max 3 keyframes per media file to limit VLM token usage
# =========================================================================

import os
import base64
import logging
from typing import Dict, List, Tuple

import cv2
import numpy as np
import requests
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


class MediaProcessor:
    """
    Zero-disk I/O video/image processor for IMINT pipeline.

    Streams media URLs directly into RAM, extracts keyframes via
    scene-change detection, and returns Base64-encoded JPEG frames
    ready for OpenRouter VLM consumption.
    """

    # --- CONFIGURATION ---
    MAX_KEYFRAMES: int = 3
    MAX_EDGE_PX: int = 768          # Longest edge after downscale
    JPEG_QUALITY: int = 80          # JPEG compression quality (0-100)
    MAX_FRAMES_TO_SCAN: int = 300   # Safety cap: stop scanning after N frames (~5 min @ 1fps)

    # Anti-hotlinking: spoofed browser headers injected into FFmpeg via env
    _FFMPEG_OPTIONS: str = (
        "user_agent;Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        "|headers;Referer: https://t.me/\r\n"
    )

    def __init__(self) -> None:
        """Initialize MediaProcessor and set FFmpeg anti-hotlinking env."""
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = self._FFMPEG_OPTIONS
        logger.info("MediaProcessor initialized (anti-hotlinking headers set).")

    def extract_keyframes(self, media_url: str) -> List[Dict]:
        """
        Extract top keyframes from a media URL (video or image).

        For videos: streams via FFmpeg, samples at 1 FPS, selects the top 3
        frames with the highest scene-change deltas.
        For images: downloads, compresses, and returns a single Base64 frame.

        Args:
            media_url: Direct URL to the media file (image or video).

        Returns:
            List of dicts, each containing:
              - base64_data: Base64 data URL string
              - delta_score: float (scene-change magnitude, 0 for images)
              - frame_index: int (source frame position)
              - selection_reason: str (why this frame was chosen)
            Empty list on failure.
        """
        if not media_url or not isinstance(media_url, str):
            return []

        if media_url.startswith("https://t.me/"):
            resolved_url = self._resolve_telegram_url(media_url)
            if not resolved_url:
                logger.warning(f"MediaProcessor: Failed to resolve Telegram CDN for {media_url}")
                return []
            media_url = resolved_url

        try:
            # Attempt to open the stream via FFmpeg backend
            cap = cv2.VideoCapture(media_url, cv2.CAP_FFMPEG)

            if not cap.isOpened():
                logger.warning(f"MediaProcessor: Failed to open stream: {media_url[:80]}...")
                return []

            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

            # --- SINGLE IMAGE DETECTION ---
            # If FPS is 0 or total_frames <= 1, treat as a static image.
            if fps <= 0 or total_frames <= 1:
                return self._process_single_image(cap)

            # --- VIDEO PROCESSING ---
            return self._process_video_stream(cap, fps)

        except Exception as e:
            logger.error(f"MediaProcessor: Exception processing {media_url[:80]}...: {e}")
            return []

    def _process_video_stream(self, cap: cv2.VideoCapture, fps: float) -> List[Dict]:
        """
        Process a video stream: sample at 1 FPS, rank by scene-change delta,
        return top MAX_KEYFRAMES frames as enriched dicts.
        """
        # Frame sampling interval: 1 frame per second
        sample_interval = max(1, int(round(fps)))

        prev_gray = None
        frame_index: int = 0
        sampled_count: int = 0

        # Priority queue: (delta_score, frame_index, frame_bgr)
        # We keep track of top-N by delta score
        top_frames: List[Tuple[float, int, np.ndarray]] = []

        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                frame_index += 1

                # Sample at 1 FPS
                if frame_index % sample_interval != 0:
                    continue

                sampled_count += 1
                if sampled_count > self.MAX_FRAMES_TO_SCAN:
                    break

                # Convert to grayscale for delta calculation
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                if prev_gray is not None:
                    # Scene-change delta: mean absolute pixel difference
                    diff = cv2.absdiff(gray, prev_gray)
                    delta_score = float(np.mean(diff))

                    # Maintain top-N frames
                    if len(top_frames) < self.MAX_KEYFRAMES:
                        top_frames.append((delta_score, frame_index, frame.copy()))
                    else:
                        # Replace the lowest-scoring frame if this one is better
                        min_idx = min(range(len(top_frames)), key=lambda i: top_frames[i][0])
                        if delta_score > top_frames[min_idx][0]:
                            top_frames[min_idx] = (delta_score, frame_index, frame.copy())

                prev_gray = gray

        finally:
            cap.release()

        if not top_frames:
            # Fallback: if no deltas were computed (e.g., 1-frame video),
            # return empty
            return []

        # Sort by delta score descending (highest scene change first)
        top_frames.sort(key=lambda x: x[0], reverse=True)

        # Compress, encode, and build enriched result dicts
        result: List[Dict] = []
        for rank, (score, f_idx, frame_bgr) in enumerate(top_frames):
            b64 = self._compress_and_encode(frame_bgr)
            if b64:
                # Determine selection reason label
                reason = "Max Delta / Scene Change" if rank == 0 else "High Delta / Scene Change"
                result.append({
                    "base64_data": b64,
                    "delta_score": round(score, 2),
                    "frame_index": f_idx,
                    "selection_reason": reason
                })

        logger.info(
            f"MediaProcessor: Extracted {len(result)} keyframes "
            f"from {sampled_count} sampled frames."
        )
        return result

    def _process_single_image(self, cap: cv2.VideoCapture) -> List[Dict]:
        """Process a single image (or 1-frame stream) and return as enriched dict."""
        try:
            ret, frame = cap.read()
            if not ret or frame is None:
                return []

            b64 = self._compress_and_encode(frame)
            if not b64:
                return []

            return [{
                "base64_data": b64,
                "delta_score": 0.0,
                "frame_index": 1,
                "selection_reason": "Single Frame / Static Image"
            }]

        finally:
            cap.release()

    def _compress_and_encode(self, frame: np.ndarray) -> str | None:
        """
        Downscale frame to MAX_EDGE_PX, encode to JPEG, return as Base64 data URL.

        Returns:
            Base64 data URL string, or None on failure.
        """
        try:
            h, w = frame.shape[:2]

            # Downscale: longest edge to MAX_EDGE_PX, maintain aspect ratio
            if max(h, w) > self.MAX_EDGE_PX:
                scale = self.MAX_EDGE_PX / max(h, w)
                new_w = int(w * scale)
                new_h = int(h * scale)
                frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)

            # Encode to JPEG in memory
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, self.JPEG_QUALITY]
            success, buffer = cv2.imencode('.jpg', frame, encode_params)

            if not success:
                return None

            # Convert to Base64 data URL (OpenRouter VLM format)
            b64_str = base64.b64encode(buffer.tobytes()).decode('utf-8')
            return f"data:image/jpeg;base64,{b64_str}"

        except Exception as e:
            logger.error(f"MediaProcessor: Compression error: {e}")
            return None

    def _resolve_telegram_url(self, tme_url: str) -> str | None:
        """
        Resolves a public t.me post URL to its direct CDN video/image link.
        Uses the ?embed=1 endpoint to scrape the direct src without login.
        """
        embed_url = tme_url + "?embed=1"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://t.me/"
        }
        
        try:
            r = requests.get(embed_url, headers=headers, timeout=10)
            if r.status_code != 200:
                return None
                
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # 1. Try to find a video source
            video_tag = soup.find('video')
            if video_tag and video_tag.has_attr('src'):
                return video_tag['src']
                
            # 2. Try to find a photo background image
            photo_tags = soup.find_all('a', class_='tgme_widget_message_photo_wrap')
            if photo_tags:
                style = photo_tags[0].get('style', '')
                match = re.search(r"url\('([^']+)'\)", style)
                if match:
                    return match.group(1)
                    
            return None
            
        except Exception as e:
            logger.error(f"MediaProcessor: Error resolving {tme_url}: {e}")
            return None
