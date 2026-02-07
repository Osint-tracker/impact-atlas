# run_tracker.ps1

# 1. Imposta la cartella DI LAVORO (La cartella principale del progetto, non il file)
Set-Location "C:\Users\lucag\.vscode\cli\osint-tracker"

# 2. Log di avvio
echo "--- Avvio Tracker: $(Get-Date) ---" >> tracker_log.txt

# 3. Esegui l'Agente Python
# NOTA: Assicurati che il percorso 'assets/scripts/...' sia giusto.
# Se il tuo file è solo dentro 'scripts', togli 'assets/'.
python scripts/osint_agent.py

# 4. Git Push Automatizzato
git add assets/data/events.geojson
git commit -m "🤖 Auto-update da Locale: $(Get-Date -Format 'yyyy-MM-dd HH:mm')"

# Tentativo di push
try {
    git push origin main
    echo "✅ Push completato con successo" >> tracker_log.txt
} catch {
    echo "❌ Errore Push: $_" >> tracker_log.txt
}
