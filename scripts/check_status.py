import sqlite3
import os

# Percorso diretto al tuo DB
DB_PATH = 'C:/Users/lucag/.vscode/cli/osint-tracker/war_tracker_v2/data/raw_events.db'


def main():
    if not os.path.exists(DB_PATH):
        print(f"❌ ERRORE: Database non trovato in {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Conta eventi pronti
    cursor.execute(
        "SELECT count(*) FROM unique_events WHERE ai_analysis_status IN ('COMPLETED', 'VERIFIED')")
    completed = cursor.fetchone()[0]

    # Conta totali
    cursor.execute("SELECT count(*) FROM unique_events")
    total = cursor.fetchone()[0]

    conn.close()

    print(f"📊 STATO DATABASE:")
    print(f"   - Totale eventi nel DB: {total}")
    print(f"   - Pronti per la mappa (COMPLETED): {completed}")

    if completed == 0:
        print("\n👉 DEVI LANCIARE L'AI AGENT! (ai_agent.py)")
    else:
        print("\n👉 DEVI SOLO ESPORTARE! (generate_output.py)")


if __name__ == "__main__":
    main()
