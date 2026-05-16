import os, requests, base64
from dotenv import load_dotenv
load_dotenv()

audio_data = base64.b64decode('//NExAAAAANIAAAAAExBTUUzLjEwMKqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq')

resp = requests.post(
    "https://openrouter.ai/api/v1/audio/transcriptions",
    headers={"Authorization": "Bearer " + os.getenv("OPENROUTER_API_KEY")},
    files={"file": ("test.mp3", audio_data, "audio/mpeg")},
    data={"model": "openai/whisper-large-v3-turbo"}
)
print(resp.status_code, resp.text)
