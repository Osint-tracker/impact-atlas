"""
Automated Data Pipeline Runner
Resets status, runs AI agent, and generates output without manual intervention.
"""
import sqlite3
import os
import subprocess
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def reset_db():
    print("[1/3] Resetting database status...")
    if not os.path.exists(DB_PATH):
        print(f"[ERR] Database not found: {DB_PATH}")
        return False
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE unique_events SET ai_analysis_status = 'PENDING'")
    print(f"   [OK] Reset {cursor.rowcount} events to PENDING")
    conn.commit()
    conn.close()
    return True

def run_agent():
    print("\n[2/3] Running AI Agent (Processing events)...")
    # Using subprocess to ensure clean environment and avoid import side effects
    result = subprocess.run(["python", "scripts/ai_agent.py"], cwd=os.path.join(BASE_DIR, '..'))
    if result.returncode != 0:
        print("[ERR] AI Agent failed")
        return False
    return True

def run_export():
    print("\n[3/3] Generating Output...")
    result = subprocess.run(["python", "scripts/generate_output.py"], cwd=os.path.join(BASE_DIR, '..'))
    if result.returncode != 0:
        print("[ERR] Export failed")
        return False
    return True

if __name__ == "__main__":
    if reset_db():
        if run_agent():
            if run_export():
                print("\n[DONE] PIPELINE COMPLETE!")
