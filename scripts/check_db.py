import sqlite3
import os
import json

# Percorso DB
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')


def inspect_db():
    print(f"🔍 ISPEZIONE DATABASE: {DB_PATH}")

    if not os.path.exists(DB_PATH):
        print("❌ Database non trovato.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Per accedere ai nomi delle colonne
    cursor = conn.cursor()

    # 1. ELENCA LE COLONNE ESISTENTI
    print("\n📋 COLONNE NELLA TABELLA 'unique_events':")
    cursor.execute("PRAGMA table_info(unique_events)")
    columns = [row[1] for row in cursor.fetchall()]
    print(columns)

    if 'tie_score' in columns:
        print("\n✅ La colonna 'tie_score' ESISTE.")
    else:
        print("\n❌ La colonna 'tie_score' NON ESISTE (Ecco il motivo dell'errore!).")

    # 2. CERCA I DATI DENTRO IL JSON (IL TESORO NASCOSTO)
    print("\n🕵️  CERCO 'tie_score' DENTRO 'ai_report_json'...")
    try:
        cursor.execute(
            "SELECT event_id, ai_report_json FROM unique_events WHERE ai_report_json IS NOT NULL LIMIT 1")
        row = cursor.fetchone()

        if row:
            data = json.loads(row['ai_report_json'])
            print(f"\n📄 Esempio Evento ID: {row['event_id']}")

            # Verifichiamo se le chiavi esistono nel JSON
            if 'tie_score' in data:
                print(
                    f"   ✅ TROVATO NEL JSON! tie_score = {data['tie_score']}")
            else:
                print("   ⚠️ tie_score non trovato nella root del JSON.")

            if 'titan_metrics' in data:
                print(
                    f"   ✅ TROVATO NEL JSON! titan_metrics = {data['titan_metrics']}")
            else:
                print("   ⚠️ titan_metrics non trovato.")

            print("\n💡 CONCLUSIONE:")
            print(
                "I dati esistono nel JSON, ma mancano le colonne dedicate per l'export veloce.")
        else:
            print("⚠️ Nessun evento con AI Report trovato.")

    except Exception as e:
        print(f"Errore lettura: {e}")

    conn.close()


if __name__ == "__main__":
    inspect_db()
