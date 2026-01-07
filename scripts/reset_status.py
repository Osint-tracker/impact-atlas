
import sqlite3
import os

# Configurazione Percorso DB
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Percorso relativo al progetto come visto in ai_agent.py
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def reset_events():
    """
    Riporta tutti gli eventi allo stato 'PENDING' per forzare una 
    nuova analisi completa da parte dell'AI Agent.
    """
    print(f"🔌 Connessione al database: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Errore: Il database non esiste percorso specificato.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Contiamo prima quanti eventi ci sono
        cursor.execute("SELECT COUNT(*) FROM unique_events")
        total_events = cursor.fetchone()[0]
        
        print(f"📊 Totale eventi nel DB: {total_events}")
        
        # Reset stato
        print("🔄 Reset dello stato a 'PENDING' in corso...")
        
        # Opzionale: Se vuoi cancellare anche i vecchi dati per essere sicuro che sia tutto nuovo:
        # cursor.execute("""
        #     UPDATE unique_events 
        #     SET ai_analysis_status = 'PENDING',
        #         ai_report_json = NULL, 
        #         tie_score = NULL 
        # """)
        
        # Per ora facciamo solo lo stato, così sovrascrive
        cursor.execute("UPDATE unique_events SET ai_analysis_status = 'PENDING'")
        modified_count = cursor.rowcount
        
        conn.commit()
        print(f"✅ Successo! {modified_count} eventi impostati su 'PENDING'.")
        print("🚀 Ora puoi eseguire: python scripts/ai_agent.py")

    except sqlite3.Error as e:
        print(f"❌ Errore SQLite: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    confirm = input("⚠️  Sei sicuro di voler resettare TUTTI gli eventi per la ri-analisi AI? (s/n): ")
    if confirm.lower() == 's':
        reset_events()
    else:
        print("❌ Operazione annullata.")
