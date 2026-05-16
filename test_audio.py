import os, json, urllib.request, urllib.error
from dotenv import load_dotenv
load_dotenv()
data = json.dumps({
    'model': 'openai/whisper-large-v3-turbo',
    'messages': [{
        'role': 'user',
        'content': [
            {'type': 'text', 'text': 'Please transcribe this audio file.'},
            {'type': 'input_audio', 'inputAudio': {'data': '//NExAAAAANIAAAAAExBTUUzLjEwMKqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq', 'format': 'mp3'}}
        ]
    }]
})
req = urllib.request.Request(
    'https://openrouter.ai/api/v1/chat/completions',
    data=data.encode('utf-8'),
    headers={'Authorization': 'Bearer ' + os.getenv('OPENROUTER_API_KEY'), 'Content-Type': 'application/json'}
)
try:
    print(urllib.request.urlopen(req).read().decode())
except urllib.error.HTTPError as e:
    print(e.read().decode())
