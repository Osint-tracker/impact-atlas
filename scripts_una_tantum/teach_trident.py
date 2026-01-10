import sqlite3
import json
import os

DB_PATH = 'osint_tracker.db'


def start_lesson():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, predicted_phase, confidence_score, signals_context 
        FROM prediction_history 
        WHERE verification_status = 'PENDING'
        ORDER BY created_at DESC
    """)
    rows = cursor.fetchall()

    if not rows:
        print("📭 Nessuna previsione da correggere. Esegui prima 'trident_orchestrator.py'!")
        return

    print(
        f"👨‍🏫 SESSIONE DI ADDESTRAMENTO: {len(rows)} previsioni da verificare.\n")

    count = 0
    for row in rows:
        pred_id, phase, conf, context_raw = row
        context = json.loads(context_raw)

        # Recupera il testo salvato (o mette un placeholder se manca ancora)
        snippet = context.get(
            'text_snippet', '⚠️ TESTO MANCANTE (Dati vecchi)')
        targets = context.get('targets', [])

        print(f"🔹 ID: {pred_id}")
        print(f"   📜 NEWS: {snippet}...")  # QUI VEDRAI IL TESTO
        print(f"   🎯 TARGET RILEVATI: {targets}")
        print(f"   🤖 IL SISTEMA DICE: {phase} (Conf: {int(conf*100)}%)")

        choice = input(
            "   ✅ È corretto? (s = Si / n = No / skip = Salta): ").strip().lower()
        print("-" * 40)

        new_status = 'PENDING'
        if choice == 's':
            new_status = 'CONFIRMED'
            count += 1
        elif choice == 'n':
            new_status = 'FALSE_POSITIVE'
        elif choice == 'skip':
            continue

        if new_status != 'PENDING':
            cursor.execute("""
                UPDATE prediction_history 
                SET verification_status = ?, verified_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            """, (new_status, pred_id))

    conn.commit()
    conn.close()
    print(f"🎓 Lezione finita. Hai validato {count} eventi.")


if __name__ == "__main__":
    start_lesson()
