import sqlite3
import sys
import os
import time

# Aggiunge il path corrente per importare il sensore
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from layer1_sensor import TitanSensor
except ImportError:
    print("❌ ERRORE: Manca il file 'scripts/layer1_sensor.py'. Crealo prima di procedere.")
    sys.exit(1)

# PERCORSO CORRETTO DEL DB GIGANTE
DB_PATH = os.path.join('war_tracker_v2', 'data', 'raw_events.db')


def apply_titan_protocol():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database non trovato in: {DB_PATH}")
        return

    sensor = TitanSensor()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print(f"📡 Connesso a: {DB_PATH}")
    print("🔧 Aggiunta colonne metriche alla tabella 'raw_signals' (se mancano)...")

    # 1. Aggiungiamo le colonne per i punteggi se non esistono
    columns_to_add = [
        ("tie_score", "REAL"),
        ("k_metric", "REAL"),
        ("t_metric", "REAL"),
        ("e_metric", "REAL")
    ]

    for col_name, col_type in columns_to_add:
        try:
            cursor.execute(
                f"ALTER TABLE raw_signals ADD COLUMN {col_name} {col_type} DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # La colonna esiste già, andiamo avanti

    # 2. Leggiamo i dati (Solo Hash e Testo)
    print("📥 Lettura eventi da 'raw_signals'...")
    cursor.execute("SELECT event_hash, text_content FROM raw_signals")
    rows = cursor.fetchall()
    total_rows = len(rows)

    print(f"🚀 Inizio elaborazione TITAN-10 su {total_rows} eventi.")
    print("   Questo potrebbe richiedere tempo. Premi Ctrl+C per interrompere (i salvataggi sono a blocchi).")

    start_time = time.time()
    updated_count = 0

    # Usiamo una transazione unica per blocchi di 1000 update (molto più veloce)
    batch_updates = []

    for row in rows:
        evt_hash, text = row

        # Analisi Layer 1
        metrics = sensor.analyze_text(text)

        # Prepariamo i dati per l'update
        batch_updates.append((
            metrics['tie_score'],
            metrics['k_metric'],
            metrics['t_metric'],
            metrics['e_metric'],
            evt_hash
        ))

        updated_count += 1

        # Eseguiamo il commit ogni 1000 righe per velocità
        if len(batch_updates) >= 1000:
            cursor.executemany("""
                UPDATE raw_signals 
                SET tie_score = ?, k_metric = ?, t_metric = ?, e_metric = ?
                WHERE event_hash = ?
            """, batch_updates)
            conn.commit()
            batch_updates = []  # Svuota il buffer

            # Progress Bar semplice
            elapsed = time.time() - start_time
            rate = updated_count / elapsed
            print(
                f"   ...Processati {updated_count}/{total_rows} ({rate:.1f} eventi/sec)")

    # Commit finale per i rimanenti
    if batch_updates:
        cursor.executemany("""
            UPDATE raw_signals 
            SET tie_score = ?, k_metric = ?, t_metric = ?, e_metric = ?
            WHERE event_hash = ?
        """, batch_updates)
        conn.commit()

    conn.close()
    print(
        f"\n✅ COMPLETATO. {updated_count} eventi aggiornati con metriche TITAN-10.")


if __name__ == "__main__":
    apply_titan_protocol()
