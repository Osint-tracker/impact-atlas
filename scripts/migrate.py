import sqlite3
import os

# Percorso del database (basato sui log dei tuoi script precedenti)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def inspect_database():
    print("=" * 50)
    print(f"🔍 ISPEZIONE SCHEMA DATABASE")
    print(f"📂 Percorso: {DB_PATH}")
    print("=" * 50 + "\n")
    
    if not os.path.exists(DB_PATH):
        print("❌ Errore: Il file del database non esiste in questo percorso.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Recupera i nomi di tutte le tabelle nel DB
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if not tables:
        print("⚠️ Il database è vuoto (nessuna tabella trovata).")
        conn.close()
        return

    # Itera su ogni tabella e recupera lo schema delle colonne
    for table_name in tables:
        table = table_name[0]
        print(f"📦 TABELLA: {table}")
        print("-" * 40)
        
        # PRAGMA table_info restituisce: cid, name, type, notnull, dflt_value, pk
        cursor.execute(f"PRAGMA table_info({table});")
        columns = cursor.fetchall()
        
        for col in columns:
            col_name = col[1]
            col_type = col[2] if col[2] else "UNKNOWN"
            is_pk = " (PRIMARY KEY)" if col[5] else ""
            print(f"  🔹 {col_name.ljust(25)} | Tipo: {col_type}{is_pk}")
        print("\n")

    conn.close()
    print("✅ Ispezione completata.")

if __name__ == "__main__":
    inspect_database()