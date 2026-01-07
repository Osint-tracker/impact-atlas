import random
import os

# --- CONFIGURAZIONE ---
# Il percorso del tuo file originale
# Nota: "scripts" è la cartella, "training_dataset 4o mini.jsonl" è il file
input_path = os.path.join("scripts", "training_dataset 4o mini.jsonl")

# I nomi dei due file che verranno creati
output_train = "training_set_final.jsonl"
output_val = "validation_set.jsonl"

# Quante righe vuoi spostare nel file di validazione
VAL_SIZE = 200


def split_data():
    # 1. Controllo se il file esiste
    if not os.path.exists(input_path):
        print(f"❌ ERRORE: Non trovo il file: {input_path}")
        print(
            "Assicurati di lanciare questo script dalla cartella principale del progetto.")
        return

    print(f"📖 Leggo il file: {input_path}...")

    # 2. Leggo tutte le righe
    with open(input_path, 'r', encoding='utf-8') as f:
        # Legge e pulisce eventuali righe vuote
        lines = [line.strip() for line in f if line.strip()]

    total_lines = len(lines)
    print(f"   Trovate {total_lines} righe totali.")

    # Controllo di sicurezza
    if total_lines <= VAL_SIZE:
        print("⚠️  ERRORE: Hai meno righe di quelle richieste per la validazione!")
        return

    # 3. Mescolo le righe a caso (Shuffle)
    print("🔀 Mescolo le righe casualmente...")
    random.shuffle(lines)

    # 4. Taglio il mazzo
    # Le ultime 200 righe diventano la Validazione
    val_data = lines[-VAL_SIZE:]
    # Tutto il resto (dall'inizio fino alle ultime 200 escluse) è il Training
    train_data = lines[:-VAL_SIZE]

    # 5. Salvo i due file
    print(f"💾 Salvo i file...")

    with open(output_train, 'w', encoding='utf-8') as f:
        f.write('\n'.join(train_data))

    with open(output_val, 'w', encoding='utf-8') as f:
        f.write('\n'.join(val_data))

    # Riepilogo finale
    print("-" * 40)
    print("✅ OPERAZIONE COMPLETATA!")
    print(f"📘 File Training:   {output_train} -> {len(train_data)} righe")
    print(f"📙 File Validation: {output_val}   -> {len(val_data)} righe")
    print("-" * 40)
    print("Ora carica questi due file su OpenAI nei rispettivi slot.")


if __name__ == "__main__":
    split_data()
