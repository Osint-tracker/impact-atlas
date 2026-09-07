"""Manual diagnostic for the OpenRouter audio chat endpoint.

This script intentionally makes a live API call and is not part of automated
test discovery. Run it directly only with a configured OPENROUTER_API_KEY.
"""

import json
import os
import urllib.error
import urllib.request

from dotenv import load_dotenv


def main() -> None:
    """Submit a minimal audio payload to OpenRouter for manual diagnostics."""
    load_dotenv()
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise SystemExit("OPENROUTER_API_KEY is required to run this diagnostic.")

    data = json.dumps({
        "model": "openai/whisper-large-v3-turbo",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "Please transcribe this audio file."},
                {"type": "input_audio", "inputAudio": {
                    "data": "//NExAAAAANIAAAAAExBTUUzLjEwMKqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
                    "format": "mp3",
                }},
            ],
        }],
    })
    request = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=data.encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            print(response.read().decode())
    except urllib.error.HTTPError as error:
        print(error.read().decode())
    except urllib.error.URLError as error:
        raise SystemExit(f"OpenRouter request failed: {error.reason}") from error


if __name__ == "__main__":
    main()
