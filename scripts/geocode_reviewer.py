import sqlite3
import json
import time
import os
import sys
import logging
from openai import OpenAI
import requests
from dotenv import load_dotenv

# Ensure unicode output for Windows
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    logging.error("Missing OPENROUTER_API_KEY in .env")
    sys.exit(1)

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY
)

MODEL = "deepseek/deepseek-v3.2"  # Strict V3.2 as per user rules
BATCH_SIZE = 20
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../war_tracker_v2/data/raw_events.db')

# --- GEOLOCATOR (Photon API - Free / No Rate Limits) ---
def photon_geocode(location_name: str):
    try:
        url = f"https://photon.komoot.io/api/?q={location_name}&limit=1"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data and data.get('features'):
            coords = data['features'][0]['geometry']['coordinates']
            # Photon returns [lon, lat]
            return {'lat': coords[1], 'lon': coords[0]}
    except Exception as e:
        logging.error(f"Photon Geocode Error for {location_name}: {e}")
    return None

def main():
    if not os.path.exists(DB_PATH):
        logging.error(f"Database not found at {DB_PATH}")
        sys.exit(1)
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # We query events that have an ai_report_json where we can find an extracted location
    cursor.execute("""
        SELECT event_id, full_text_dossier, description, ai_report_json 
        FROM unique_events 
        WHERE ai_report_json IS NOT NULL
    """)
    rows = cursor.fetchall()
    
    events_to_review = []
    
    for row in rows:
        event_id, dossier, desc, ai_json_str = row
        try:
            ai_data = json.loads(ai_json_str)
            tactics = ai_data.get('tactics', {})
            geo = tactics.get('geo_location', {})
            explicit = geo.get('explicit', {})
            inferred = geo.get('inferred', {})
            
            # The currently assigned location mapping
            loc_name = inferred.get('toponym') or inferred.get('landmark') or explicit.get('lat') or "None/Unknown"

            current_lat = explicit.get('lat') if explicit and explicit.get('lat') else inferred.get('lat')
            current_lon = explicit.get('lon') if explicit and explicit.get('lon') else inferred.get('lon')
            
            # Keep context concise to save tokens
            context = (dossier or desc or "")[:800]
            
            events_to_review.append({
                "event_id": event_id,
                "extracted_location": str(loc_name),
                "current_lat": current_lat,
                "current_lon": current_lon,
                "context_snippet": context,
                "raw_ai_json": ai_json_str  # Keep to update later
            })
        except Exception:
            continue

    logging.info(f"Loaded {len(events_to_review)} events for AI review.")
    
    system_prompt = """
    You are an expert Military Geography Analyst. 
    You are provided with a batch of extracted military events.
    For each event, evaluate if the current 'extracted_location', 'current_lat', and 'current_lon' truly represent the KINETIC IMPACT POINT according to the 'context_snippet'.
    Watch out for METONYMY: "Moscow stated..." does not mean the strike happened in Moscow.
    Watch out for GENERIC COUNTRIES: Do not geocode generic "Ukraine" or "Russia", these often inappropriately default to the capital (Kyiv/Moscow). Replace generic countries with the exact village/town, or return null if impossible to know.
    If the event is out-of-theater (e.g. Romania, USA, Germany), it should either not be geocoded or be precisely geocoded to its actual location, NOT Kyiv.
    If the event currently has no coordinates missing (lat/lon null) but SHOULD be geocoded based on the context, provide the correct_location_name.
    
    OUTPUT strictly a JSON array of objects, one for each event_id, in this format:
    [
      {
        "event_id": "...",
        "is_correct": true,
        "correct_location_name": null
      },
      {
        "event_id": "...",
        "is_correct": false,
        "correct_location_name": "Actual Village Name, Region"  // Or null if it should genuinely be un-geocoded (like a political statement, out of theater etc)
      }
    ]
    Do not output any markdown code blocks, just raw JSON array.
    """

    # Process in batches
    for i in range(0, len(events_to_review), BATCH_SIZE):
        batch = events_to_review[i:i+BATCH_SIZE]
        logging.info(f"Processing batch {i//BATCH_SIZE + 1} ({len(batch)} events)...")
        
        prompt_data = [{"event_id": e["event_id"], "extracted_location": e["extracted_location"], "context_snippet": e["context_snippet"]} for e in batch]
        
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(prompt_data, indent=2)}
                ],
                temperature=0, # Strict adherence
            )
            
            result_text = response.choices[0].message.content.strip()
            # Clean possible markdown arrays
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.lower().startswith("json"):
                    result_text = result_text[4:]
            
            results = json.loads(result_text.strip())
            
            updates_made = 0
            for res in results:
                if not res.get("is_correct") and res.get("correct_location_name"):
                    correct_name = res.get("correct_location_name")
                    event_id = res.get("event_id")
                    
                    # Target the original event
                    target_event = next((x for x in batch if x["event_id"] == event_id), None)
                    if not target_event: continue
                        
                    # Geocode the new true location
                    new_coords = photon_geocode(correct_name)
                    if new_coords:
                        logging.info(f"Event {event_id}: Corrected '{target_event['extracted_location']}' -> '{correct_name}' @ {new_coords}")
                        
                        # Apply to JSON
                        ai_data = json.loads(target_event["raw_ai_json"])
                        tactics = ai_data.get('tactics', {})
                        geo = tactics.get('geo_location', {})
                        if 'inferred' not in geo: geo['inferred'] = {}
                        
                        geo['inferred']['toponym'] = correct_name
                        geo['inferred']['lat'] = new_coords['lat']
                        geo['inferred']['lon'] = new_coords['lon']
                        
                        # Force explicit to null if we are fuzzy matching
                        if 'explicit' in geo:
                            geo['explicit'] = {'lat': None, 'lon': None}
                            
                        # Update DB
                        cursor.execute("UPDATE unique_events SET ai_report_json = ? WHERE event_id = ?", (json.dumps(ai_data), event_id))
                        updates_made += 1
                    else:
                        logging.warning(f"Event {event_id}: AI suggested '{correct_name}' but Photon could not geocode it. Setting to NULL to avoid misplacement.")
                        
                        ai_data = json.loads(target_event["raw_ai_json"])
                        geo = ai_data.get('tactics', {}).get('geo_location', {})
                        if 'inferred' in geo:
                            geo['inferred']['lat'] = None
                            geo['inferred']['lon'] = None
                        if 'explicit' in geo:
                            geo['explicit'] = {'lat': None, 'lon': None}
                        cursor.execute("UPDATE unique_events SET ai_report_json = ? WHERE event_id = ?", (json.dumps(ai_data), event_id))
                        updates_made += 1
                        
            if updates_made > 0:
                conn.commit()
                logging.info(f"Batch completed. Applied {updates_made} corrections.")
            
        except Exception as e:
            logging.error(f"Error processing batch: {e}")
            
    conn.close()
    logging.info("Fully automated review complete.")

if __name__ == "__main__":
    main()
