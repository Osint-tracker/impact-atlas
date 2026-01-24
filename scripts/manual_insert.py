
import sqlite3
import json
import time
import os
import uuid

DB_PATH = 'war_tracker_v2/data/raw_events.db'

def insert_event():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    event_id = f"fus_{int(time.time()*1000)}"
    title = "47th Brigade Repels Assault near Ocheretyne"
    desc = "The 47th Mechanized Brigade (Magura) successfully repelled a Russian mechanized assault near Ocheretyne. Footage shows Bradley IFVs engaging enemy armor. The Russian 30th Motorized Rifle Brigade reportedly suffered losses including 3 BTR-82A vehicles."
    
    # Real AI Report Structure
    ai_report = {
        "timestamp_generated": "2025-12-20T14:30:00",
        "editorial": {
            "title_en": title,
            "description_en": desc
        },
        "tactics": {
            "geo_location": {
                "explicit": {
                    "lat": 48.238, 
                    "lon": 37.613  # Ocheretyne
                },
                "inferred": {"toponym_raw": "Ocheretyne"}
            },
            "event_analysis": {
                "classification": "GROUND_CLASH",
                "summary_en": desc
            }
        },
        "military_units_detected": [
            {
                "unit_name": "47th Mechanized Brigade",
                "unit_id": "UA_47_MECH_BDE",
                "faction": "UA",
                "type": "MECH_INF",
                "status": "ENGAGED"
            },
            {
                "unit_name": "30th Motorized Rifle Brigade",
                "unit_id": "RU_30_MR_BDE",
                "faction": "RU",
                "type": "MOT_RIFLE",
                "status": "ENGAGED"
            }
        ],
        "titan_assessment": {
            "kinetic_score": 7,
            "target_score": 6,
            "effect_score": 7,
            "target_type_category": "INFANTRY"
        }
    }
    
    ai_json = json.dumps(ai_report)
    
    cursor.execute("""
        INSERT INTO unique_events (
            event_id, title, description, 
            last_seen_date, tie_score, kinetic_score, target_score, effect_score,
            reliability, bias_score, ai_analysis_status, ai_report_json, urls_list
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event_id, title, desc, 
        "2026-01-23", 72.0, 7, 6, 7, 
        85, 2.0, "COMPLETED", ai_json, 
        "https://t.me/DeepStateUA/12345 | https://t.me/ButusovPlus/5678"
    ))
    
    conn.commit()
    print(f"Inserted Event: {event_id}")
    conn.close()

if __name__ == "__main__":
    insert_event()
