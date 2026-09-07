"""Manual diagnostic for the OpenRouter audio transcription endpoint.

This script intentionally makes a live API call and is not part of automated
test discovery. Run it directly only with a configured OPENROUTER_API_KEY.
"""

import base64
import os

import requests
from dotenv import load_dotenv


def main() -> None:
    """Submit a minimal audio payload to OpenRouter for manual diagnostics."""
    load_dotenv()
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise SystemExit("OPENROUTER_API_KEY is required to run this diagnostic.")

    audio_data = base64.b64decode(
        "//NExAAAAANIAAAAAExBTUUzLjEwMKqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
    )
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {api_key}"},
            files={"file": ("test.mp3", audio_data, "audio/mpeg")},
            data={"model": "openai/whisper-large-v3-turbo"},
            timeout=30,
        )
        print(response.status_code, response.text)
    except requests.RequestException as error:
        raise SystemExit(f"OpenRouter request failed: {error}") from error


if __name__ == "__main__":
    main()
