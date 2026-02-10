import sqlite3, json
conn = sqlite3.connect('war_tracker_v2/data/raw_events.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Deep probe: strategy, context_analysis, editorial, titan_metrics
cur.execute("""SELECT ai_report_json FROM unique_events 
               WHERE ai_analysis_status='COMPLETED' AND tie_score >= 70
               ORDER BY tie_score DESC LIMIT 1""")
row = cur.fetchone()
if row and row['ai_report_json']:
    d = json.loads(row['ai_report_json'])
    
    print("=== STRATEGY ===")
    strat = d.get('strategy', {})
    if isinstance(strat, dict):
        for k, v in strat.items():
            print(f"  {k}: {str(v)[:200]}")
    else:
        print(f"  {str(strat)[:300]}")
    
    print("\n=== CONTEXT ANALYSIS ===")
    ctx = d.get('context_analysis', {})
    if isinstance(ctx, dict):
        for k, v in ctx.items():
            print(f"  {k}: {str(v)[:200]}")
    else:
        print(f"  {str(ctx)[:300]}")
    
    print("\n=== EDITORIAL ===")
    ed = d.get('editorial', {})
    if isinstance(ed, dict):
        for k, v in ed.items():
            print(f"  {k}: {str(v)[:200]}")
    else:
        print(f"  {str(ed)[:300]}")
    
    print("\n=== TITAN METRICS ===")
    tm = d.get('titan_metrics', {})
    if isinstance(tm, dict):
        for k, v in tm.items():
            print(f"  {k}: {str(v)[:200]}")
    else:
        print(f"  {str(tm)[:300]}")
    
    print("\n=== TACTICS.EVENT_ANALYSIS ===")
    ta = d.get('tactics', {}).get('event_analysis', {})
    if isinstance(ta, dict):
        for k, v in ta.items():
            print(f"  {k}: {str(v)[:200]}")
    else:
        print(f"  {str(ta)[:300]}")
    
    print("\n=== TACTICS.TITAN_ASSESSMENT ===")
    tt = d.get('tactics', {}).get('titan_assessment', {})
    if isinstance(tt, dict):
        for k, v in tt.items():
            print(f"  {k}: {str(v)[:200]}")
    else:
        print(f"  {str(tt)[:300]}")
    
    print("\n=== TACTICS.ACTORS ===")
    ac = d.get('tactics', {}).get('actors', {})
    print(f"  {str(ac)[:400]}")
    
    print("\n=== SCORES ===")
    sc = d.get('scores', {})
    print(f"  {sc}")
    
    print("\n=== RELIABILITY_REASONING ===")
    rr = d.get('reliability_reasoning', '')
    print(f"  {str(rr)[:400]}")

conn.close()
