import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')


def reset_missing_tie():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Reset stato a 'PENDING' solo per chi ha tie_score = 0
    print("🔄 Resettaggio eventi vecchi per ricalcolo AI...")
    cursor.execute("""
        UPDATE unique_events 
        SET ai_analysis_status = 'PENDING' 
        WHERE tie_score = 0 OR tie_score IS NULL
    """)

    count = cursor.rowcount
    conn.commit()
    conn.close()

    print(f"✅ {count} eventi resettati a PENDING.")
    print("👉 Ora lancia 'ai_agent.py' per fargli ricalcolare il T.I.E. di questi eventi.")


if __name__ == "__main__":
    reset_missing_tie()
