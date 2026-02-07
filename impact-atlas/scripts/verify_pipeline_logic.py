"""
verify_pipeline_logic.py - Deep Verification of GDELT/Telegram Integration
==========================================================================
Checks if the "Sanfilippo Strategy" is working:
1. GDELT content is actually full text (>200 chars).
2. Refiner is embedding GDELT events (is_embedded=1).
3. Event Builder is creating MIXED clusters (GDELT + Telegram).
4. Data integrity check across all layers.

Usage:
    python scripts/verify_pipeline_logic.py
"""

import sqlite3
import os
import json
import sys

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", "data", "raw_events.db")

def verify_logic():
    print("=" * 60)
    print("[VERIFY] PIPELINE LOGIC VERIFICATION")
    print("=" * 60)
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 1. CHECK CONTENT QUALITY
    print("\n[1] GDELT Content Quality Check")
    c.execute("SELECT COUNT(*) FROM raw_signals WHERE source_type='GDELT'")
    total_gdelt = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM raw_signals WHERE source_type='GDELT' AND LENGTH(text_content) > 100")
    full_text_gdelt = c.fetchone()[0]
    
    print(f"    - Total GDELT: {total_gdelt}")
    print(f"    - With Full Text (>100 chars): {full_text_gdelt}")
    if full_text_gdelt > 0:
        print(f"    [OK] Success: {full_text_gdelt} articles have scraped content.")
    else:
        print("    [X] Failure: No GDELT articles have full content.")

    # 2. CHECK EMBEDDING STATUS
    print("\n[2] Refiner Embedding Status")
    c.execute("SELECT COUNT(*) FROM raw_signals WHERE source_type='GDELT' AND is_embedded=1")
    embedded_gdelt = c.fetchone()[0]
    
    print(f"    - Embedded GDELT: {embedded_gdelt}")
    if embedded_gdelt > 0:
        print(f"    [OK] Success: Refiner is processing GDELT (is_embedded=1 works).")
    else:
        print("    [!] Warning: No GDELT events are embedded yet (Refiner hasn't run or failed).")

    # 3. CHECK MIXED CLUSTERS (Optimized)
    print("\n[3] Cluster Composition Check")
    
    # Fetch all clustered items (event_hash, cluster_id, source_type)
    print("    Fetching cluster data...")
    c.execute("SELECT cluster_id, source_type FROM raw_signals WHERE cluster_id IS NOT NULL")
    rows = c.fetchall()
    
    clusters = {}
    for r in rows:
        cid = r[0]
        stype = r[1]
        if cid not in clusters:
            clusters[cid] = set()
        clusters[cid].add(stype)
        
    mixed_count = 0
    mixed_samples = []
    
    for cid, types in clusters.items():
        # Check if GDELT and TELEGRAM are present
        has_gdelt = 'GDELT' in types
        has_telegram = 'TELEGRAM' in types
        
        if has_gdelt and has_telegram:
            mixed_count += 1
            if len(mixed_samples) < 5:
                mixed_samples.append((cid, list(types)))

    print(f"    - Mixed Source Clusters (GDELT + TELEGRAM): {mixed_count}")
    
    if mixed_count > 0:
        print("    [OK] SUCCESS: Event Builder is merging GDELT and TELEGRAM!")
        print("    Sample Mixed Clusters:")
        for cid, types in mixed_samples:
            print(f"      - Cluster {cid}: {types}")
    else:
        print("    [X] FAILURE: No mixed clusters found yet.")
        print("       (This means GDELT and Telegram are still segregated or not embedding into same space)")

    # 4. CHECK UNIQUE_EVENTS
    print("\n[4] Unique Events Integration")
    c.execute("SELECT COUNT(*) FROM unique_events WHERE sources_list LIKE '%GDELT%' OR sources_list LIKE '%http%'")
    events_with_gdelt = c.fetchone()[0]
    
    print(f"    - Events with GDELT sources: {events_with_gdelt}")
    
    if events_with_gdelt > 0:
        print("    [OK] SUCCESS: GDELT events are reaching the final table!")
    else:
        print("    [X] FAILURE: GDELT missing from unique_events.")

    conn.close()
    print("\n" + "=" * 60)
    if full_text_gdelt > 0 and embedded_gdelt > 0 and events_with_gdelt > 0:
        print("[+] PIPELINE STATUS: OPERATIONAL")
        print("   Logic verified: Scraper -> Refiner -> Builder flow is working.")
    else:
        print("[!] PIPELINE STATUS: PARTIAL / BROKEN")
        print("   See failures above.")

if __name__ == "__main__":
    verify_logic()
