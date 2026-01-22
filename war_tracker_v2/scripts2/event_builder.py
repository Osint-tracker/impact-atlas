import re
import sqlite3
import pandas as pd
import os
import json
import time
from typing import List, Dict

# --- CONFIGURAZIONE AVANZATA ---
MAX_ARTICLES_PER_EVENT = 12       # Target ideale per Qwen 72B
# Scarta tweet o snippet troppo brevi (spazzatura) - lowered for GDELT
MIN_TEXT_LENGTH = 45
# Tronca articoli infiniti per non bruciare la memoria di Qwen
MAX_CHAR_PER_ARTICLE = 6000
# Scrive su disco ogni 500 cluster (più veloce)
COMMIT_BATCH_SIZE = 500

# --- PERCORSI ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "raw_events.db")


class EventBuilder:
    def __init__(self):
        print(f"[*] Inizializzazione Event Builder (Advanced Mode)...")

        # Connessione DB
        self.conn = sqlite3.connect(DB_PATH)
        # Attiviamo WAL mode per performance elevate in scrittura/lettura concorrente
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.cursor = self.conn.cursor()

        self.create_unique_events_table()

    def create_unique_events_table(self):
        """Crea la tabella target se non esiste."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS unique_events (
                event_id TEXT PRIMARY KEY,
                first_seen_date TEXT,
                last_seen_date TEXT,
                article_count INTEGER,
                sources_list TEXT,
                urls_list TEXT,
                full_text_dossier TEXT,
                ai_analysis_status TEXT DEFAULT 'PENDING',
                ai_json_output TEXT,
                severity_score INTEGER
            )
        """)
        self.conn.commit()

    def fetch_clusters(self):
        """Recupera i cluster ID pronti. Esclude quelli già processati per permettere resume."""
        print("[*] Recupero cluster dal DB...")

        # Ottimizzazione: Prendiamo solo i cluster che non sono già in unique_events
        # (O se vuoi sovrascrivere tutto, togli la clausola EXCEPT)
        query = """
            SELECT DISTINCT cluster_id 
            FROM raw_signals 
            WHERE is_embedded = 1 AND cluster_id IS NOT NULL
        """
        try:
            df = pd.read_sql_query(query, self.conn)
            return df['cluster_id'].tolist()
        except Exception as e:
            print(f"Errore nel fetch cluster: {e}")
            return []

    def get_articles_for_cluster(self, cluster_id: str) -> pd.DataFrame:
        """Scarica raw data per il cluster specificato."""
        query = """
            SELECT source_name, date_published, text_content, url
            FROM raw_signals
            WHERE cluster_id = ?
        """
        return pd.read_sql_query(query, self.conn, params=(cluster_id,))

    def clean_and_rank_articles(self, df: pd.DataFrame) -> pd.DataFrame:

        # 1. Pulizia base e Parsing Date
        df['text_content'] = df['text_content'].fillna("").astype(str)
        df['source_name'] = df['source_name'].fillna("Unknown")
        # Conversione date necessaria per l'ordinamento
        df['date_published'] = pd.to_datetime(
            df['date_published'], errors='coerce')

        # 2. Filtro lunghezza
        df['text_len'] = df['text_content'].str.len()
        df = df[df['text_len'] >= MIN_TEXT_LENGTH].copy()

        if df.empty:
            return pd.DataFrame()

        # --- CONFIGURAZIONE KEYWORD ---
        # A. LIVELLO GEO (Massima priorità per la mappa)
        geo_keywords = [
            'coordinate', 'geolocated', 'geolocation', 'lat', 'lon',
            'oblast', 'region', 'settlement', 'village', 'district',
            'frontline', 'axis', 'direction', 'km away', 'south of', 'north of'
        ]

        # B. LIVELLO HARDWARE (Dettagli tecnici = Alta affidabilità)
        tech_keywords = [
            'tank', 'apc', 'ifv', 'bmp', 'btr', 't-72', 't-90', 'leopard', 'bradley', 'abrams',
            'artillery', 'mlrs', 'himars', 'grad', 'hurricane', 'tornado', 'howitzer',
            'iskander', 'kinzhal', 'kalibr', 'storm shadow', 'atacms', 's-300', 's-400', 'patriot',
            'uav', 'drone', 'shahed', 'geran', 'fpv', 'su-34', 'su-35', 'mig-31', 'f-16', 'bomber'
        ]

        # C. LIVELLO KINETIC (Azione fisica)
        action_keywords = [
            'shelled', 'shelling', 'strike', 'struck', 'hit', 'destroyed', 'damaged',
            'intercepted', 'shot down', 'repelled', 'advance', 'captured', 'seized',
            'explosion', 'blast', 'debris', 'crater', 'fire', 'detonation'
        ]

        # D. LIVELLO UMANO/UNITÀ
        unit_keywords = [
            'brigade', 'battalion', 'regiment', 'division', 'group', 'special forces',
            'gru', 'sbu', 'gsu', 'killed', 'injured', 'wounded', 'kia', 'wia', 'casualties',
            'personnel', 'civilians', 'dead'
        ]

        # E. NOISE (Fuffa Politica/Clickbait da penalizzare)
        noise_keywords = [
            'condemn', 'meeting', 'summit', 'negotiation', 'peace talks',
            'statement', 'concern', 'discussion', 'potential', 'escalation risk',
            'opinion', 'columnist', 'shocking', 'click here'
        ]

        # --- CALCOLO SCORE ---
        def calculate_smart_score(text):
            text_lower = text.lower()
            score = 0.0

            digit_count = sum(c.isdigit() for c in text)
            if len(text) > 0:
                score += (digit_count / len(text)) * 300

            if re.search(r'\d{1,2}\.\d{3,},\s*\d{1,2}\.\d{3,}', text):
                score += 50.0

            # [NEW] Pattern Bonus: Quantità Specifiche (es. "5 tanks", "12 killed")
            # Indica report tattico ad alta precisione, prioritario per l'Analista.
            if re.search(r'\d+\s+(?:tank|drone|uav|missile|rocket|soldier|troop|killed|wounded|dead|km|mile)', text_lower):
                score += 10.0
            
            # [NEW] Pattern Bonus: Orari specifici (es. "at 05:30", "14:00")
            if re.search(r'\d{1,2}:\d{2}', text):
                score += 5.0

            for word in geo_keywords:
                if word in text_lower:
                    score += 3.0
            for word in tech_keywords:
                if word in text_lower:
                    score += 2.5
            for word in unit_keywords:
                if word in text_lower:
                    score += 2.0
            for word in action_keywords:
                if word in text_lower:
                    score += 1.5
            for word in noise_keywords:
                if word in text_lower:
                    score -= 4.0

            score += min(len(text) / 1000, 5.0)
            return score

        df['quality_score'] = df['text_content'].apply(calculate_smart_score)

        # --- SELEZIONE STRATEGICA ---
        final_selection = []
        taken_indices = set()

        # FASE A: IL PIÙ RECENTE
        if not df['date_published'].isna().all():
            latest_article = df.sort_values(
                'date_published', ascending=False).iloc[0]
            # --- CORREZIONE QUI SOTTO: .to_dict() ---
            final_selection.append(latest_article.to_dict())
            taken_indices.add(latest_article.name)

        # FASE B: I MIGLIORI PER QUALITÀ
        df_sorted = df.sort_values('quality_score', ascending=False)
        df_sorted = df_sorted[~df_sorted.index.isin(taken_indices)]

        unique_sources = df_sorted.drop_duplicates(subset=['source_name'])

        slots_remaining = MAX_ARTICLES_PER_EVENT - len(final_selection)
        if slots_remaining > 0:
            best_unique = unique_sources.head(slots_remaining)
            final_selection.extend(best_unique.to_dict('records'))
            for idx in best_unique.index:
                taken_indices.add(idx)

        # FASE C: RIEMPIMENTO
        if len(final_selection) < MAX_ARTICLES_PER_EVENT:
            slots_remaining = MAX_ARTICLES_PER_EVENT - len(final_selection)
            remaining_df = df_sorted[~df_sorted.index.isin(taken_indices)]
            if not remaining_df.empty:
                fillers = remaining_df.head(slots_remaining)
                final_selection.extend(fillers.to_dict('records'))

        return pd.DataFrame(final_selection)

    def build_dossier_text(self, df: pd.DataFrame) -> str:
        """Formatta il testo per l'AI Agent con Deduplicazione Paragrafi."""
        dossier = ""
        seen_paragraphs_hash = set()

        for i, row in enumerate(df.itertuples()):
            # Tronchiamo testi eccessivi per sicurezza
            text_content = str(row.text_content)
            
            # --- DEDUPLICAZIONE PARAGRAFI ---
            paragraphs = text_content.split('\n')
            unique_text_blocks = []
            
            for p in paragraphs:
                clean_p = p.strip()
                if not clean_p: continue
                
                # Normalizziamo per confronto (ignore case/spaces)
                p_hash = hash(clean_p.lower())
                
                if p_hash in seen_paragraphs_hash:
                    # Se paragraph già visto in articoli precedenti di questo dossier, skippa (Riduce token)
                    continue
                
                seen_paragraphs_hash.add(p_hash)
                unique_text_blocks.append(clean_p)
            
            # Ricostruiamo il testo pulito
            full_clean_text = "\n".join(unique_text_blocks)
            
            was_truncated = False
            if len(full_clean_text) > MAX_CHAR_PER_ARTICLE:
                final_text = full_clean_text[:MAX_CHAR_PER_ARTICLE]
                was_truncated = True
            else:
                final_text = full_clean_text

            # Header chiaro per aiutare Qwen a distinguere le fonti
            dossier += f"### REPORT {i+1} ###\n"
            dossier += f"SOURCE: {row.source_name}\n"
            dossier += f"DATE: {row.date_published}\n"
            dossier += f"CONTENT:\n{final_text}"

            if was_truncated:
                dossier += "\n... [TEXT TRUNCATED FOR BREVITY]"

            dossier += "\n" + ("-" * 40) + "\n\n"

        return dossier

    def run(self):
        start_time = time.time()
        clusters = self.fetch_clusters()
        total_clusters = len(clusters)

        print(f"[*] Trovati {total_clusters} cluster da analizzare.")

        if total_clusters == 0:
            print("[!] Nessun cluster nuovo trovato. Esco.")
            return

        processed_count = 0
        skipped_count = 0

        for idx, c_id in enumerate(clusters):
            # 1. Get Dati
            raw_df = self.get_articles_for_cluster(c_id)
            if raw_df.empty:
                skipped_count += 1
                continue

            # 2. Logica di Selezione
            top_articles = self.clean_and_rank_articles(raw_df)

            if top_articles.empty:
                # Significa che tutti gli articoli erano troppo corti (< MIN_TEXT_LENGTH)
                skipped_count += 1
                continue

            # 3. Preparazione Dati Output
            dossier_text = self.build_dossier_text(top_articles)

            sources_json = json.dumps(top_articles['source_name'].tolist())
            urls_json = json.dumps(top_articles['url'].tolist())

            # Date (gestione sicura anche se formati misti, prendiamo min/max stringa per ora)
            dates = top_articles['date_published'].sort_values()
            first_date = str(dates.iloc[0]) if not dates.empty else ""
            last_date = str(dates.iloc[-1]) if not dates.empty else ""

            # Contiamo tutti gli articoli originali del cluster
            article_count = len(raw_df)

            # 4. SQL UPSERT
            try:
                self.cursor.execute("""
                    INSERT INTO unique_events (
                        event_id, first_seen_date, last_seen_date, article_count,
                        sources_list, urls_list, full_text_dossier, ai_analysis_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
                    ON CONFLICT(event_id) DO UPDATE SET
                        article_count = excluded.article_count,
                        full_text_dossier = excluded.full_text_dossier,
                        last_seen_date = excluded.last_seen_date,
                        sources_list = excluded.sources_list,
                        urls_list = excluded.urls_list,
                        ai_analysis_status = 'PENDING' 
                """, (c_id, first_date, last_date, article_count, sources_json, urls_json, dossier_text))

                processed_count += 1

            except Exception as e:
                print(f"[ERR] Errore salvataggio cluster {c_id}: {e}")

            # 5. Commit Batch (Efficiente)
            if processed_count % COMMIT_BATCH_SIZE == 0:
                self.conn.commit()
                elapsed = time.time() - start_time
                rate = processed_count / elapsed
                print(
                    f"   [PROGRESS] Processati: {processed_count}/{total_clusters} ({rate:.1f} eventi/sec)")

        self.conn.commit()
        self.conn.close()
        print(f"\n[*] COMPLETATO.")
        print(f"    - Eventi Creati/Aggiornati: {processed_count}")
        print(f"    - Cluster Scartati (vuoti/corti): {skipped_count}")


if __name__ == "__main__":
    builder = EventBuilder()
    builder.run()
