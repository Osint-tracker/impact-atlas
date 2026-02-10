"""Quick DB probe to understand available data."""
import sqlite3, json

conn = sqlite3.connect(r'war_tracker_v2/data/raw_events.db')
cur = conn.cursor()

# Count by status
cur.execute("SELECT ai_analysis_status, COUNT(*) FROM unique_events GROUP BY ai_analysis_status")
print("=== STATUS COUNTS ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# Sample completed event
cur.execute("""
    SELECT title, last_seen_date, tie_score, kinetic_score, target_score, effect_score,
           reliability, bias_score, tie_status, severity_score
    FROM unique_events 
    WHERE ai_analysis_status='COMPLETED' AND title IS NOT NULL AND title != ''
    ORDER BY last_seen_date DESC LIMIT 3
""")
print("\n=== SAMPLE EVENTS ===")
for r in cur.fetchall():
    print(f"  Title: {r[0][:60]}")
    print(f"  Date: {r[1]}, TIE: {r[2]}, K:{r[3]} T:{r[4]} E:{r[5]}")
    print(f"  Reliability: {r[6]}, Bias: {r[7]}, Status: {r[8]}, Severity: {r[9]}")
    print()

# Sample AI report JSON keys
cur.execute("""
    SELECT ai_report_json FROM unique_events 
    WHERE ai_analysis_status='COMPLETED' AND ai_report_json IS NOT NULL LIMIT 1
""")
row = cur.fetchone()
if row and row[0]:
    d = json.loads(row[0])
    print("=== AI REPORT JSON KEYS ===")
    print(f"  Top-level: {list(d.keys())}")
    if 'classification' in d:
        print(f"  Classification: {d['classification']}")
    if 'tactics' in d:
        print(f"  Tactics keys: {list(d['tactics'].keys())}")

# Classification distribution
cur.execute("""
    SELECT json_extract(ai_report_json, '$.classification') as cls, COUNT(*) 
    FROM unique_events 
    WHERE ai_analysis_status='COMPLETED' AND ai_report_json IS NOT NULL
    GROUP BY cls ORDER BY COUNT(*) DESC LIMIT 10
""")
print("\n=== CLASSIFICATION DISTRIBUTION ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# TIE score distribution
cur.execute("""
    SELECT 
        CASE 
            WHEN tie_score >= 70 THEN 'CRITICAL (70+)'
            WHEN tie_score >= 40 THEN 'HIGH (40-69)'
            WHEN tie_score >= 20 THEN 'MEDIUM (20-39)'
            ELSE 'LOW (0-19)'
        END as bracket, COUNT(*)
    FROM unique_events WHERE ai_analysis_status='COMPLETED'
    GROUP BY bracket ORDER BY bracket
""")
print("\n=== TIE SCORE DISTRIBUTION ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# Date range
cur.execute("""
    SELECT MIN(last_seen_date), MAX(last_seen_date) 
    FROM unique_events WHERE ai_analysis_status='COMPLETED'
""")
dates = cur.fetchone()
print(f"\n=== DATE RANGE ===")
print(f"  From: {dates[0]} To: {dates[1]}")

conn.close()
