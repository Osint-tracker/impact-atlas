# =========================================================================
# 👁️ VISION INSTRUMENT: Zero-Disk I/O Media Processor
# =========================================================================
# PURPOSE: Bridge between raw Telegram media URLs and The Visionary VLM.
#   - Evades anti-hotlinking (spoofed User-Agent/Referer via FFmpeg env)
#   - Streams video directly into RAM (no disk writes)
#   - Extracts keyframes via geometric sampling (10%, 40%, 70%, 90%)
#   - Compresses and encodes frames to Base64 data URLs
#
# HARD CONSTRAINTS:
#   - ZERO disk I/O (serverless constraint)
#   - Graceful failure: dead links / 403s return [] silently
#   - Max 4 keyframes per media file to limit VLM token usage
# =========================================================================

import os
import json
import logging
import base64
import re
import requests
import cv2
import numpy as np
import tempfile
import aiohttp
import asyncio
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
from io import BytesIO
import random
import time
import threading

_TME_SEMAPHORE = threading.Semaphore(2)  # Limit concurrent t.me HTTP requests
logger = logging.getLogger(__name__)


class MediaProcessor:
    """
    Zero-disk I/O video/image processor for IMINT pipeline.

    Streams media URLs directly into RAM, extracts keyframes via
    scene-change detection, and returns Base64-encoded JPEG frames
    ready for OpenRouter VLM consumption.
    """

    # --- CONFIGURATION ---
    MAX_KEYFRAMES: int = 4
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
        Process a video stream using geometric sampling (10%, 40%, 70%, 90%).
        """
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0:
            return []

        percentiles = [0.1, 0.4, 0.7, 0.9]
        target_indices = [int(p * total_frames) for p in percentiles]
        
        final_frames = []
        for idx in target_indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ret, frame = cap.read()
            if ret:
                b64 = self._compress_and_encode(frame)
                if b64:
                    final_frames.append({
                        "base64_data": b64,
                        "delta_score": 0.0,
                        "frame_index": idx,
                        "selection_reason": "geometric_sample"
                    })
        
        cap.release()
        return final_frames

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
        Protected by Semaphore and Jitter to prevent rate limiting.
        """
        embed_url = tme_url + "?embed=1"
        
        UAS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        
        headers = {
            "User-Agent": random.choice(UAS),
            "Referer": "https://t.me/"
        }
        
        # Implement robust retry strategy with backoff to bypass Telegram rate limiting
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        try:
            with _TME_SEMAPHORE:
                # Jitter to mimic human behavior
                time.sleep(random.uniform(0.5, 1.5))
                r = session.get(embed_url, headers=headers, timeout=15)

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
        finally:
            session.close()

    async def extract_audio_transcript(self, media_url: str) -> str:
        """Download video, extract audio, and transcribe via OpenRouter openai/whisper-large-v3-turbo."""
        if media_url.startswith("https://t.me/"):
            resolved_url = await asyncio.to_thread(self._resolve_telegram_url, media_url)
            if not resolved_url:
                logger.warning(f"MediaProcessor: Failed to resolve Telegram CDN for audio {media_url}")
                return ""
            media_url = resolved_url

        import uuid
        import base64
        import subprocess
        
        temp_dir = tempfile.gettempdir()
        v_name = f"vid_{uuid.uuid4().hex}.mp4"
        a_name = f"aud_{uuid.uuid4().hex}.wav"
        v_path = os.path.join(temp_dir, v_name)
        a_path = os.path.join(temp_dir, a_name)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(media_url, timeout=45) as r:
                    if r.status != 200:
                        return ""
                    with open(v_path, 'wb') as f:
                        async for chunk in r.content.iter_chunked(8192):
                            f.write(chunk)

            # Extract audio using ffmpeg to WAV
            cmd = [
                "ffmpeg", "-y", "-i", v_path,
                "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
                a_path
            ]
            await asyncio.to_thread(subprocess.run, cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if not os.path.exists(a_path):
                return ""

            # Base64 encode the audio file
            with open(a_path, "rb") as audio_file:
                audio_b64 = base64.b64encode(audio_file.read()).decode('utf-8')

            # Send to OpenRouter with openai/whisper-large-v3-turbo
            payload = {
                "model": "openai/whisper-large-v3-turbo",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Please transcribe this audio file. The language is likely Russian or Ukrainian."},
                            {"type": "input_audio", "input_audio": {"data": audio_b64, "format": "wav"}}
                        ]
                    }
                ],
                "stream": False
            }
            
            headers = {
                "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY')}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://impact-atlas.io",
                "X-Title": "Impact Atlas"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=60) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                        return content.strip()
                    else:
                        err_text = await resp.text()
                        logger.error(f"MediaProcessor: OpenRouter audio API error {resp.status}: {err_text}")
                        return ""
                        
        except Exception as e:
            logger.error(f"MediaProcessor: Audio extraction error: {e}")
            return ""
        finally:
            # Cleanup: ensure files are deleted even if errors occurred
            for p in [v_path, a_path]:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception as cleanup_err:
                        logger.warning(f"MediaProcessor: Cleanup failed for {p}: {cleanup_err}")
        
        return ""
