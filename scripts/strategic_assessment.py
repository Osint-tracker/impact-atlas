import json
import os
import requests
import time
from datetime import datetime, timezone
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMPAIGN_REPORTS_PATH = os.path.join(BASE_DIR, '../assets/data/campaign_reports.json')
EVENTS_GEOJSON_PATH = os.path.join(BASE_DIR, '../assets/data/events.geojson')
OUTPUT_PATH = os.path.join(BASE_DIR, '../assets/data/strategic_assessments.json')

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
MODEL_ID = "deepseek/deepseek-v4-flash"

def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(path: str, data: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_ai_assessment(campaign_name: str, stats: Dict, recent_events: List[Dict]) -> Dict:
    if not OPENROUTER_API_KEY:
        print("   [ERR] No OPENROUTER_API_KEY found.")
        return {}

    events_context = "\n".join([
        f"- {e.get('date')}: {e.get('title')} (Target: {e.get('target_type')})"
        for e in recent_events[:10]
    ])

    prompt = f"""
Analyze the following OSINT data for the military campaign: "{campaign_name}".
Provide a professional military intelligence assessment.

STATS:
- Total Events: {stats.get('total_events')}
- Weekly T.I.E. Cumulative: {stats.get('weekly_tie_cumulative')}
- Cumulative Effect Vector: {stats.get('sum_vec_e')}
- Status: {stats.get('status')}

RECENT KEY EVENTS:
{events_context}

Format your response STICTLY as a JSON object with these exact keys:
- situational_overview: A concise summary of the current state of the campaign.
- tactical_focus: Identification of the main target types and geographic areas under pressure.
- operational_trend: Analysis of whether the campaign is intensifying, stable, or decelerating.
- strategic_outlook: Prediction of the likely development in the next 72-96 hours.

Keep each section professional, clinical, and data-driven. Do not use flowery language.
"""

    try:
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json"
            },
            data=json.dumps({
                "model": MODEL_ID,
                "messages": [
                    {"role": "system", "content": "You are The Strategist, a strict military intelligence officer providing tactical assessments."},
                    {"role": "user", "content": prompt}
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.1
            })
        )
        response.raise_for_status()
        result = response.json()
        content = result['choices'][0]['message']['content']
        return json.loads(content)
    except Exception as e:
        print(f"   [ERR] AI Generation failed for {campaign_name}: {e}")
        return {}

def main():
    print(f"--- STRATEGIC ASSESSMENT ENGINE ---")
    reports = load_json(CAMPAIGN_REPORTS_PATH)
    events_payload = load_json(EVENTS_GEOJSON_PATH)

    if not reports or not events_payload:
        print("   [ERR] Missing input files.")
        return

    features = events_payload.get('features', [])
    campaigns = reports.get('campaigns', [])
    
    assessments = {}

    for campaign in campaigns:
        cid = campaign.get('campaign_id')
        name = campaign.get('name')
        print(f" > Processing Campaign: {name} ({cid})...")

        # Extract recent events for this campaign
        campaign_events = []
        for feat in features:
            props = feat.get('properties', {})
            if props.get('campaign_id') == cid:
                campaign_events.append(props)
        
        # Sort by date descending
        campaign_events.sort(key=lambda x: x.get('date', ''), reverse=True)

        # Get AI Assessment
        assessment = get_ai_assessment(name, campaign, campaign_events)
        if assessment:
            assessments[cid] = assessment
            print(f"   [OK] Assessment generated.")
        
        # Rate limiting safety
        time.sleep(1)

    final_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_used": MODEL_ID,
        "assessments": assessments
    }

    save_json(OUTPUT_PATH, final_payload)
    print(f"--- DONE! Saved to {OUTPUT_PATH} ---")

if __name__ == "__main__":
    main()
