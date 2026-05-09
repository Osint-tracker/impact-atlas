import os
import sqlite3
import json
import asyncio
import aiohttp
import cv2
import base64
import numpy as np
import subprocess
import tempfile
import random
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional

load_dotenv()

# Configuration
DB_PATH = r"c:\Users\lucag\.vscode\cli\osint-tracker\war_tracker_v2\data\raw_events.db"
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_AUDIO_URL = "https://openrouter.ai/api/v1/audio/transcriptions"
EMBEDDING_URL = "https://openrouter.ai/api/v1/embeddings"

# Mandatory Models
MODEL_WHISPER = "openai/whisper-large-v3-turbo"
MODEL_VISION = "qwen/qwen3-vl-235b-a22b-instruct"
MODEL_EMBEDDING = "openai/text-embedding-3-large"

# Concurrency Schema (Replicating ai_agent.py)
API_SEMAPHORE = asyncio.Semaphore(50)
BATCH_SIZE = 50 
MAX_RES_PX = 768

async def _call_api_with_backoff(session: aiohttp.ClientSession, url: str, method: str = "POST", **kwargs):
    """Replicated backoff logic from ai_agent.py."""
    max_attempts = 4
    base_delay = 2.0
    
    for attempt in range(1, max_attempts + 1):
        try:
            async with session.request(method, url, **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()
                
                # Transient errors
                if resp.status in [429, 502, 503, 504]:
                    if attempt == max_attempts:
                        print(f"      [ERROR] API Retries exhausted: Status {resp.status}")
                        return None
                    wait_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    print(f"      [RETRY] API Transient Error ({resp.status}). Retrying in {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"      [ERROR] API Permanent Error: Status {resp.status} - {await resp.text()}")
                    return None
        except Exception as e:
            if attempt == max_attempts:
                print(f"      [ERROR] API Retries exhausted: {e}")
                return None
            wait_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            await asyncio.sleep(wait_time)
    return None

async def get_embedding(text: str, session: aiohttp.ClientSession) -> Optional[List[float]]:
    """Fetch 1536-dim embedding from OpenRouter."""
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL_EMBEDDING,
        "input": [text],
        "dimensions": 1536
    }
    data = await _call_api_with_backoff(session, EMBEDDING_URL, json=payload, headers=headers)
    if data and 'data' in data:
        return data['data'][0]['embedding']
    return None

async def analyze_multimodal(frames_b64: List[str], audio_transcript: str, source_name: str, date_published: str, session: aiohttp.ClientSession) -> Optional[str]:
    """Generate tactical description using Qwen-VL via OpenRouter."""
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
    
    content = [
        {"type": "text", "text": f"TACTICAL INTELLIGENCE MISSION:\nAnalyze these frames and audio transcript to generate a clinical, technical description of the event. Focus on equipment, units, and kinetic activity.\n\nSHADOW CONTEXT:\n- Source: {source_name}\n- Date: {date_published}\n\nAUDIO TRANSCRIPT: {audio_transcript or '[No Audio]'}"}
    ]
    
    for f in frames_b64:
        content.append({
            "type": "image_url",
            "image_url": {"url": f}
        })

    payload = {
        "model": MODEL_VISION,
        "messages": [{"role": "user", "content": content}]
    }

    data = await _call_api_with_backoff(session, OPENROUTER_URL, json=payload, headers=headers)
    if data and 'choices' in data:
        return data['choices'][0]['message']['content']
    return None

def extract_geometric_frames(url: str) -> List[str]:
    """Extract frames at 10%, 40%, 70%, 90% using OpenCV."""
    frames_b64 = []
    cap = cv2.VideoCapture(url)
    if not cap.isOpened():
        return []

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    if total_frames <= 0:
        ret, frame = cap.read()
        if ret: frames_b64.append(encode_frame(frame))
        cap.release()
        return frames_b64

    percentiles = [0.1, 0.4, 0.7, 0.9]
    target_frames = [int(p * total_frames) for p in percentiles]
    for f_idx in target_frames:
        cap.set(cv2.CAP_PROP_POS_FRAMES, f_idx)
        ret, frame = cap.read()
        if ret: frames_b64.append(encode_frame(frame))
    cap.release()
    return frames_b64

def encode_frame(frame) -> str:
    h, w = frame.shape[:2]
    if max(h, w) > MAX_RES_PX:
        scale = MAX_RES_PX / max(h, w)
        frame = cv2.resize(frame, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return f"data:image/jpeg;base64,{base64.b64encode(buffer).decode('utf-8')}"

async def transcribe_audio(url: str, session: aiohttp.ClientSession) -> str:
    """Download video, extract audio, and transcribe via OpenRouter Whisper."""
    temp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    temp_audio = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
    
    try:
        async with session.get(url, timeout=30) as resp:
            if resp.status != 200: return ""
            temp_video.write(await resp.read())
            temp_video.close()

        cmd = [
            "ffmpeg", "-y", "-i", temp_video.name,
            "-vn", "-acodec", "libmp3lame", "-q:a", "9",
            "-ar", "16000", "-ac", "1", temp_audio.name
        ]
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await process.communicate()

        if not os.path.exists(temp_audio.name) or os.path.getsize(temp_audio.name) < 100:
            return ""

        # OpenRouter Whisper Call
        headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
        data = aiohttp.FormData()
        data.add_field('file', open(temp_audio.name, 'rb'), filename='audio.mp3')
        data.add_field('model', MODEL_WHISPER)

        res_data = await _call_api_with_backoff(session, OPENROUTER_AUDIO_URL, data=data, headers=headers)
        if res_data:
            return res_data.get('text', "")
                
    except Exception as e:
        print(f"Transcription exception: {e}")
    finally:
        if os.path.exists(temp_video.name): os.remove(temp_video.name)
        if os.path.exists(temp_audio.name): os.remove(temp_audio.name)
    return "" 

async def process_record(record, session: aiohttp.ClientSession, conn: sqlite3.Connection):
    async with API_SEMAPHORE:
        event_hash, url, media_urls, source_name, date_published = record
        media_url = url
        if media_urls:
            try:
                mu = json.loads(media_urls)
                if mu and len(mu) > 0: media_url = mu[0]
            except: pass

        print(f"[*] Processing {event_hash} -> {media_url}")
        frames = await asyncio.to_thread(extract_geometric_frames, media_url)
        if not frames: return

        transcript = await transcribe_audio(media_url, session)
        analysis = await analyze_multimodal(frames, transcript, source_name, date_published, session)
        if not analysis: return

        vector = await get_embedding(analysis, session)
        if not vector: return

        # Thread-safe DB update
        def update_db():
            c = conn.cursor()
            c.execute("UPDATE raw_signals SET text_content = ?, embedding_vector = ?, is_embedded = 1 WHERE event_hash = ?", 
                     (analysis, json.dumps(vector), event_hash))
            conn.commit()

        await asyncio.to_thread(update_db)
        print(f"[+] Successfully rescued {event_hash}")

async def main():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    
    cursor.execute("SELECT event_hash, url, media_urls, source_name, date_published FROM raw_signals WHERE is_embedded = 4")
    records = cursor.fetchall()
    print(f"Found {len(records)} records for rescue. Using Concurrency: 50")

    async with aiohttp.ClientSession() as session:
        tasks = [process_record(r, session, conn) for r in records]
        await asyncio.gather(*tasks)

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
