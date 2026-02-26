import sys
import os
import json
import sqlite3

# Add the project root to sys.path so scripts.geo_instrument can be found
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now we can import ai_agent cleanly
from ai_agent import SuperSquadAgent

DB_PATH = os.path.join(project_root, 'war_tracker_v2', 'data', 'raw_events.db')

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def run_vl_backfill():
    print("🚀 Starting Visionary Layer Backfill...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    
    # Select events that have local file paths in media_urls (signaled by 'data/media/' or similar, or just not starting with http)
    cursor.execute("""
        SELECT * FROM unique_events 
        WHERE media_urls LIKE '%\%' OR media_urls LIKE '%data/media/%' or media_urls LIKE '%.mp4%' or media_urls LIKE '%.jpg%'
    """)
    rows = cursor.fetchall()
    
    print(f"🎯 Found {len(rows)} events with local media ready for VLM.")
    if not rows:
        print("No local media found. Did the downloader script finish?")
        return
        
    agent = SuperSquadAgent()
    
    success_count = 0
    for row in rows:
        event_id = row['event_id']
        print(f"\n--- Processing Event {event_id} ---")
        
        # We need a dummy soldier data because Visionary uses it for context
        soldier_data = {}
        try:
            if row.get('ai_report_json'):
                old_report = json.loads(row['ai_report_json'])
                # The pipeline expects soldier_data from _step_2_the_soldier
                # We can mock the essential parts from the old report
                soldier_data = {
                    'titan_assessment': old_report.get('titan_assessment', {}),
                    'target_vector': old_report.get('target_vector', {}),
                    'tactics': old_report.get('tactics', {})
                }
        except:
            pass
            
        print(f"Media URLs: {row['media_urls']}")
        
        # Process this specific row through the agent's main row processor
        # Note: We can't just call _step_visionary, we need MediaProcessor to extract frames.
        # But we only want to run Visionary, so we instantiate MediaProcessor here directly.
        
        media_urls = json.loads(row['media_urls'])
        from instruments.vision_instrument import MediaProcessor
        media_proc = MediaProcessor()
        
        all_frame_dicts = []
        for m_url in media_urls:
            if 'data/media' in m_url.replace('\\', '/'): # only process the local ones we downloaded
                print(f"Extracting frames from: {m_url}")
                frames = media_proc.extract_keyframes(str(m_url))
                all_frame_dicts.extend(frames)
                if len(all_frame_dicts) >= 4:
                    all_frame_dicts = all_frame_dicts[:4]
                    break
                    
        if all_frame_dicts:
            print(f"📸 Extracted {len(all_frame_dicts)} keyframes. Invoking VLM...")
            visionary_out = agent._step_visionary(soldier_data, all_frame_dicts)
            
            if visionary_out:
                print("✅ VLM Output generated!")
                
                # Merge back into the ai_report_json and save to DB
                try:
                    full_report = json.loads(row['ai_report_json']) if row.get('ai_report_json') else {}
                    if 'tactics' not in full_report:
                        full_report['tactics'] = {}
                    
                    full_report['tactics']['visionary_report'] = visionary_out
                    full_report['tactics']['media_processed'] = True
                    
                    # Update effect score if required (following main loop logic)
                    imint_damage = visionary_out.get('kinetic_effect', {}).get('damage_level')
                    imint_confidence = visionary_out.get('visual_confirmation', {}).get('confidence_score', 0)
                    if imint_damage and imint_damage in ['CATASTROPHIC', 'SEVERE', 'MODERATE', 'LIGHT', 'NONE'] and imint_confidence >= 0.5:
                        mapping = {'CATASTROPHIC': 9, 'SEVERE': 8, 'MODERATE': 6, 'LIGHT': 4, 'NONE': 1}
                        if imint_damage in mapping:
                            if 'titan_assessment' not in full_report:
                                full_report['titan_assessment'] = {}
                            full_report['titan_assessment']['effect_score'] = mapping[imint_damage]
                            full_report['titan_assessment']['effect_source'] = 'VISIONARY_IMINT'
                            
                    new_json = json.dumps(full_report)
                    cursor.execute("UPDATE unique_events SET ai_report_json = ? WHERE event_id = ?", (new_json, event_id))
                    conn.commit()
                    success_count += 1
                    print(f"💾 Saved Visionary report to DB for {event_id}")
                except Exception as e:
                    print(f"❌ Error saving report: {e}")
            else:
                print("⚠️ VLM returned None.")
        else:
            print("⚠️ No frames extracted.")

    conn.close()
    print(f"\n🎉 Backfill Complete. Processed {success_count} events successfully.")

if __name__ == "__main__":
    run_vl_backfill()
