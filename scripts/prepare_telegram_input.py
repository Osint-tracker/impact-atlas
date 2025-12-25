import pandas as pd
import os
import re
from deep_translator import GoogleTranslator

# --- CONFIGURAZIONE ---
INPUT_FILE = "scripts/telegram_fresh_data.csv"
OUTPUT_FILE = "scripts/Telegram_AI.csv"
TARGET_LANG = 'it'  # 'it' per Italiano, 'en' per Inglese


def clean_text(text):
    """Pulisce il testo da Markdown e Link."""
    if not isinstance(text, str):
        return ""
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)  # Rimuove link MD
    text = re.sub(r'http\S+', '', text)  # Rimuove URL
    text = re.sub(r'[\*\_\`]', '', text)  # Rimuove caratteri speciali
    return re.sub(r'\s+', ' ', text).strip()  # Rimuove spazi extra


def translate_title(text):
    """Traduce un testo breve (Titolo) nella lingua target."""
    try:
        # Traduciamo solo i primi 200 caratteri per velocità ed evitare errori
        short_text = text[:200]
        translated = GoogleTranslator(
            source='auto', target=TARGET_LANG).translate(short_text)
        return translated
    except Exception:
        return text  # Se fallisce, restituisce l'originale


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Errore: {INPUT_FILE} non trovato.")
        return

    print("🔄 Caricamento dati Telegram...")
    df_tg = pd.read_csv(INPUT_FILE)
    print(
        f"   Trovati {len(df_tg)} messaggi. Pulizia e Traduzione in corso (pazienta qualche secondo)...")

    # --- DATAFRAME TARGET ---
    columns = [
        "Title", "Date", "Type", "Location", "Latitude", "Longitude",
        "Source", "ACLED_Original_Source", "Archived", "Verification",
        "Description", "Notes", "Video", "Intensity", "Actor",
        "Bias dominante", "Location Precision", "Aggregated Sources",
        "Reliability", "Bias Score"
    ]
    df_out = pd.DataFrame(columns=columns)

    # --- ELABORAZIONE ---

    # 1. Date
    df_out['Date'] = pd.to_datetime(df_tg['Date']).dt.strftime('%Y-%m-%d')

    # 2. Pulizia Testi
    print("   🧹 Pulizia testi...")
    cleaned_full_text = df_tg['Raw_Text'].apply(clean_text)

    # 3. Traduzione Titoli (Lento ma utile)
    print(f"   🌍 Traduzione titoli in {TARGET_LANG.upper()}...")
    # Applichiamo la traduzione riga per riga
    # Nota: Se hai 4000 righe ci metterà un po'. Per testare puoi aggiungere .head(50)
    df_out['Title'] = cleaned_full_text.apply(translate_title)

    # 4. Note (Testo Originale Completo per l'AI)
    df_out['Notes'] = cleaned_full_text

    # 5. Altri campi
    df_out['Location'] = "Ukraine (See Notes)"
    df_out['Latitude'] = 0.0
    df_out['Longitude'] = 0.0
    df_out['ACLED_Original_Source'] = df_tg['Source_Channel']
    df_out['Source'] = df_tg['Link']
    df_out['Type'] = "Telegram Intel"
    df_out['Actor'] = "Unknown"
    df_out['Archived'] = "No"
    df_out['Verification'] = "Unconfirmed"
    df_out['Description'] = ""
    df_out['Video'] = "Check Link"
    df_out['Intensity'] = 0.0
    df_out['Bias dominante'] = "Neutral"
    df_out['Location Precision'] = "UNKNOWN"
    df_out['Reliability'] = 0
    df_out['Bias Score'] = 0

    # --- SALVATAGGIO ---
    df_out.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Fatto! File salvato in: {OUTPUT_FILE}")
    print(f"   Esempio Titolo Tradotto: {df_out['Title'].iloc[0]}")


if __name__ == "__main__":
    main()
