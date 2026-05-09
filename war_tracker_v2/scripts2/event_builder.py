import re
import sqlite3
import pandas as pd
import os
import json
import time
import sys
import io
import uuid
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict
from io import BytesIO
import requests

# Force UTF-8 encoding for stdout/stderr to handle emojis on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

try:
    from PIL import Image
    import imagehash
    PHASH_AVAILABLE = True
except Exception:
    PHASH_AVAILABLE = False

# --- CONFIGURAZIONE AVANZATA ---
MAX_ARTICLES_PER_EVENT = 12       # Target ideale per Qwen 72B
MIN_TEXT_LENGTH = 0
MAX_CHAR_PER_ARTICLE = 6000
COMMIT_BATCH_SIZE = 500

# --- PERCORSI ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "raw_events.db")

class EventBuilder:
    def __init__(self):
        print(f"[*] Inizializzazione Event Builder (Advanced Mode)...")
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.cursor = self.conn.cursor()

        self.perform_vector_clustering()
        self.create_unique_events_table()

    def perform_vector_clustering(self):
        """Greedy Vector Clustering (Cosine Similarity >= 0.85 within 48h)."""
        print("[*] Esecuzione Vector Clustering (Greedy Engine)...")
        
        cutoff = (datetime.now() - timedelta(days=7)).isoformat()
        query = """
            SELECT event_hash, date_published, embedding_vector
            FROM raw_signals
            WHERE is_embedded = 1 AND cluster_id IS NULL
              AND date_published >= ?
        """
        rows = self.cursor.execute(query, (cutoff,)).fetchall()
        if not rows:
            print("[*] Nessun nuovo vettore da raggruppare.")
            return

        records = []
        for h, d, v in rows:
            try:
                vec = np.array(json.loads(v), dtype=np.float32)
                norm = np.linalg.norm(vec)
                if norm > 0: vec /= norm
                dt = pd.to_datetime(d, errors='coerce')
                if pd.isna(dt): continue
                if dt.tzinfo is not None:
                    dt = dt.tz_localize(None)
                records.append({'hash': h, 'date': dt, 'vec': vec})
            except:
                continue

        if not records: return
        records.sort(key=lambda x: x['date'])
        
        clusters = [] 
        THRESHOLD = 0.85
        WINDOW = timedelta(hours=48)

        for rec in records:
            matched = False
            for c in reversed(clusters):
                if rec['date'] - c['latest_date'] > WINDOW:
                    continue
                
                similarity = np.dot(rec['vec'], c['centroid'])
                if similarity >= THRESHOLD:
                    c['members'].append(rec['hash'])
                    c['latest_date'] = max(c['latest_date'], rec['date'])
                    n = len(c['members'])
                    c['centroid'] = (c['centroid'] * (n-1) + rec['vec']) / n
                    norm = np.linalg.norm(c['centroid'])
                    if norm > 0: c['centroid'] /= norm
                    matched = True
                    break
            
            if not matched:
                clusters.append({
                    'id': str(uuid.uuid4()),
                    'latest_date': rec['date'],
                    'centroid': rec['vec'],
                    'members': [rec['hash']]
                })

        for c in clusters:
            cid = c['id']
            for h in c['members']:
                self.cursor.execute("UPDATE raw_signals SET cluster_id = ? WHERE event_hash = ?", (cid, h))
        
        self.conn.commit()
        print(f"[+] Clustering completato. Creati {len(clusters)} gruppi da {len(records)} record.")

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
                severity_score INTEGER,
                media_urls TEXT,
                operational_sector TEXT,
                image_phash TEXT,
                source_reputation_score REAL,
                lat REAL,
                lon REAL
            )
        """)

        for ddl in [
            "ALTER TABLE unique_events ADD COLUMN media_urls TEXT",
            "ALTER TABLE unique_events ADD COLUMN operational_sector TEXT",
            "ALTER TABLE unique_events ADD COLUMN image_phash TEXT",
            "ALTER TABLE unique_events ADD COLUMN source_reputation_score REAL",
            "ALTER TABLE unique_events ADD COLUMN lat REAL",
            "ALTER TABLE unique_events ADD COLUMN lon REAL"
        ]:
            try:
                self.cursor.execute(ddl)
            except sqlite3.OperationalError:
                pass

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sources_reputation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT UNIQUE,
                score INTEGER DEFAULT 50,
                last_verified TEXT
            )
        """)
        self.conn.commit()

    def fetch_clusters(self):
        """Recupera i cluster ID pronti IN MODO INCREMENTALE."""
        query = """
            SELECT r.cluster_id
            FROM (
                SELECT cluster_id, COUNT(*) as curr_count
                FROM raw_signals
                WHERE is_embedded = 1 AND cluster_id IS NOT NULL
                GROUP BY cluster_id
            ) r
            LEFT JOIN unique_events u ON r.cluster_id = u.event_id
            WHERE u.event_id IS NULL 
               OR r.curr_count > COALESCE(u.article_count, 0)
        """
        try:
            df = pd.read_sql_query(query, self.conn)
            return df['cluster_id'].tolist()
        except Exception as e:
            print(f"Errore nel fetch cluster: {e}")
            return []

    def get_articles_for_cluster(self, cluster_id: str) -> pd.DataFrame:
        query = """
            SELECT source_name, date_published, text_content, url, media_urls
            FROM raw_signals
            WHERE cluster_id = ?
        """
        return pd.read_sql_query(query, self.conn, params=(cluster_id,))

    def clean_and_rank_articles(self, df: pd.DataFrame) -> pd.DataFrame:
        df['text_content'] = df['text_content'].fillna("").astype(str)
        df['source_name'] = df['source_name'].fillna("Unknown")
        df['date_published'] = pd.to_datetime(df['date_published'], errors='coerce')
        df = df.dropna(subset=['date_published']).copy()

        df['text_len'] = df['text_content'].str.len()
        df = df[df['text_len'] >= MIN_TEXT_LENGTH].copy()

        if df.empty:
            return pd.DataFrame()

        def calculate_smart_score(text):
            text_lower = text.lower()
            score = 0.0
            digit_count = sum(c.isdigit() for c in text)
            if len(text) > 0: score += (digit_count / len(text)) * 300
            if re.search(r'\d{1,2}\.\d{3,},\s*\d{1,2}\.\d{3,}', text): score += 50.0
            if re.search(r'\d+\s+(?:tank|drone|uav|missile|rocket|soldier|troop|killed|wounded|dead|km|mile)', text_lower): score += 10.0
            score += min(len(text) / 1000, 5.0)
            return score

        df['quality_score'] = df['text_content'].apply(calculate_smart_score)
        final_selection = []
        taken_indices = set()

        if not df['date_published'].isna().all():
            latest_article = df.sort_values('date_published', ascending=False).iloc[0]
            final_selection.append(latest_article.to_dict())
            taken_indices.add(latest_article.name)

        df_sorted = df.sort_values('quality_score', ascending=False)
        df_sorted = df_sorted[~df_sorted.index.isin(taken_indices)]
        unique_sources = df_sorted.drop_duplicates(subset=['source_name'])

        slots_remaining = MAX_ARTICLES_PER_EVENT - len(final_selection)
        if slots_remaining > 0:
            best_unique = unique_sources.head(slots_remaining)
            final_selection.extend(best_unique.to_dict('records'))
            for idx in best_unique.index: taken_indices.add(idx)

        if len(final_selection) < MAX_ARTICLES_PER_EVENT:
            slots_remaining = MAX_ARTICLES_PER_EVENT - len(final_selection)
            remaining_df = df_sorted[~df_sorted.index.isin(taken_indices)]
            if not remaining_df.empty:
                fillers = remaining_df.head(slots_remaining)
                final_selection.extend(fillers.to_dict('records'))

        return pd.DataFrame(final_selection)

    def build_dossier_text(self, df: pd.DataFrame) -> str:
        dossier = ""
        seen_paragraphs_hash = set()
        for i, row in enumerate(df.itertuples()):
            text_content = str(row.text_content)
            paragraphs = text_content.split('\n')
            unique_text_blocks = []
            for p in paragraphs:
                clean_p = p.strip()
                if not clean_p: continue
                p_hash = hash(clean_p.lower())
                if p_hash in seen_paragraphs_hash: continue
                seen_paragraphs_hash.add(p_hash)
                unique_text_blocks.append(clean_p)
            
            full_clean_text = "\n".join(unique_text_blocks)
            was_truncated = False
            if len(full_clean_text) > MAX_CHAR_PER_ARTICLE:
                final_text = full_clean_text[:MAX_CHAR_PER_ARTICLE]
                was_truncated = True
            else:
                final_text = full_clean_text

            dossier += f"### REPORT {i+1} ###\nSOURCE: {row.source_name}\nDATE: {row.date_published}\nCONTENT:\n{final_text}"
            if was_truncated: dossier += "\n... [TEXT TRUNCATED]"
            dossier += "\n" + ("-" * 40) + "\n\n"
        return dossier

    def compute_cluster_phash(self, media_urls: List[str]) -> str | None:
        if not PHASH_AVAILABLE or not media_urls: return None
        headers = {"User-Agent": "Mozilla/5.0"}
        for url in media_urls:
            if not isinstance(url, str) or not url.startswith('http'): continue
            try:
                resp = requests.get(url, timeout=5, headers=headers)
                if resp.status_code != 200: continue
                img = Image.open(BytesIO(resp.content)).convert('RGB')
                return str(imagehash.phash(img))
            except Exception: continue
        return None

    def run(self):
        start_time = time.time()
        clusters = self.fetch_clusters()
        total_clusters = len(clusters)
        print(f"[*] Trovati {total_clusters} cluster da analizzare.")
        if total_clusters == 0: return

        processed_count = 0
        skipped_count = 0

        for idx, c_id in enumerate(clusters):
            raw_df = self.get_articles_for_cluster(c_id)
            if raw_df.empty:
                skipped_count += 1
                continue

            top_articles = self.clean_and_rank_articles(raw_df)
            if top_articles.empty:
                skipped_count += 1
                continue

            dossier_text = self.build_dossier_text(top_articles)
            sources_json = json.dumps(top_articles['source_name'].tolist())
            urls_json = json.dumps(top_articles['url'].tolist())

            all_media = []
            if 'media_urls' in top_articles.columns:
                for mu_str in top_articles['media_urls'].dropna():
                    try:
                        mu_list = json.loads(mu_str)
                        if isinstance(mu_list, list): all_media.extend(mu_list)
                    except: pass
            unique_media = list(set(all_media))
            media_urls_json = json.dumps(unique_media)
            image_phash = self.compute_cluster_phash(unique_media)

            dates = top_articles['date_published'].sort_values()
            first_date = dates.iloc[0].isoformat() if not dates.empty and pd.notna(dates.iloc[0]) else ""
            last_date = dates.iloc[-1].isoformat() if not dates.empty and pd.notna(dates.iloc[-1]) else ""
            article_count = len(raw_df)

            try:
                self.cursor.execute("""
                    INSERT INTO unique_events (
                        event_id, first_seen_date, last_seen_date, article_count,
                        sources_list, urls_list, full_text_dossier, ai_analysis_status,
                        media_urls, image_phash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
                    ON CONFLICT(event_id) DO UPDATE SET
                        article_count = excluded.article_count,
                        full_text_dossier = excluded.full_text_dossier,
                        last_seen_date = excluded.last_seen_date,
                        sources_list = excluded.sources_list,
                        urls_list = excluded.urls_list,
                        media_urls = excluded.media_urls,
                        image_phash = COALESCE(excluded.image_phash, unique_events.image_phash),
                        ai_analysis_status = 'PENDING' 
                """, (
                    c_id, first_date, last_date, article_count,
                    sources_json, urls_json, dossier_text, media_urls_json, image_phash
                ))
                processed_count += 1
            except Exception as e:
                print(f"[ERR] Errore cluster {c_id}: {e}")

            if processed_count % COMMIT_BATCH_SIZE == 0:
                self.conn.commit()
                print(f"   [PROGRESS] {processed_count}/{total_clusters}")

        self.conn.commit()
        self.conn.close()
        print(f"[*] COMPLETATO. Eventi: {processed_count}, Scartati: {skipped_count}")

if __name__ == "__main__":
    builder = EventBuilder()
    builder.run()
