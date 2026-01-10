"""Check GeoJSON export content"""
import json

with open('assets/data/events.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)

if data['features']:
    props = data['features'][0]['properties']
    print("=== First Event Properties ===")
    print(f"ai_reasoning: {str(props.get('ai_reasoning', 'MISSING'))[:100]}...")
    print(f"sources_list: {props.get('sources_list', 'MISSING')}")
    print(f"reliability: {props.get('reliability')}")
    print(f"bias_score: {props.get('bias_score')}")
else:
    print("No features in GeoJSON")
