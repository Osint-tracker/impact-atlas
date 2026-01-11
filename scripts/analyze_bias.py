import sqlite3
import os
import pandas as pd
import json

# Correct Path to DB (Nested)
DB_PATH = os.path.join(os.path.dirname(__file__), '../war_tracker_v2/data/raw_events.db')

def analyze_bias():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    
    print("Loading data from DB...")
    try:
        # Select full text dossier to check for telegram links too
        df = pd.read_sql_query("SELECT event_id, sources_list, full_text_dossier, ai_analysis_status FROM unique_events", conn)
    except Exception as e:
        print(f"SQL Error: {e}")
        return
    finally:
        conn.close()

    if df.empty:
        print("No data found.")
        return

    print(f"   Total Events: {len(df)}")

    # Flatten source names
    all_sources = []
    
    def decode_sources(row):
        try:
            # It's a string representation of a list: '["DeepStateUA"]'
            raw = str(row['sources_list'])
            if raw and raw != 'None':
                s_list = json.loads(raw)
                if isinstance(s_list, list):
                    return s_list
                return [str(s_list)]
            return []
        except:
            return []

    df['parsed_sources'] = df.apply(decode_sources, axis=1)

    # Explode to see top sources
    exploded = df.explode('parsed_sources')
    top_sources = exploded['parsed_sources'].value_counts().head(30)
    
    print("\n--- TOP 30 SOURCE NAMES ---")
    print(top_sources)

    # CATEGORIZATION LOGIC (Improved)
    def categorize(row):
        # 1. Check Source Names
        s_names = [s.lower() for s in row['parsed_sources']]
        # Common Telegram channels often tracked
        telegram_keywords = ['deepstate', 'rybar', 'wargonzo', 'romanov', 'two_majors', 'telegram', 't.me']
        
        for name in s_names:
            if any(k in name for k in telegram_keywords):
                return 'Telegram'

        # 2. Check Dossier Links if available (Secondary check)
        # If the dossier text mentions t.me links
        if row['full_text_dossier']:
            txt = str(row['full_text_dossier']).lower()
            if 't.me/' in txt:
                return 'Telegram (Link Detected)'
        
        return 'Web/Other (ACLED/Map)'

    df['Category'] = df.apply(categorize, axis=1)

    print("\n--- SOURCE DISTRIBUTION (ALL EVENTS) ---")
    total_counts = df['Category'].value_counts()
    print(total_counts)

    print("\n--- ANALYZED EVENTS (AI FINISHED) ---")
    analyzed_df = df[df['ai_analysis_status'] != 'PENDING']
    analyzed_counts = analyzed_df['Category'].value_counts()
    print(analyzed_counts)

    print("\n--- CONVERSION RATE (Analyzed / Total) ---")
    for cat in total_counts.index:
        total = total_counts[cat]
        analyzed = analyzed_counts.get(cat, 0)
        rate = (analyzed / total) * 100
        print(f"{cat}: {rate:.1f}% ({analyzed}/{total})")

if __name__ == "__main__":
    analyze_bias()
