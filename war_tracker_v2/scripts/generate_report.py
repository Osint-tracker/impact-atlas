import sqlite3
import datetime
import os
import re
from fpdf import FPDF
from fpdf.enums import XPos, YPos

# ==========================================
# 🦅 CONFIGURAZIONE STRATEGIC INSIGHT v4.1
# ==========================================

DB_PATH = r'C:\Users\lucag\.vscode\cli\osint-tracker\war_tracker_v2\data\raw_events.db'

# Palette "NATO Dark Mode"
C_DARK = (20, 25, 30)      # Header Background
C_ACCENT = (59, 130, 246)    # Blue Intelligence
C_TEXT = (30, 30, 30)      # Testo corpo
C_MUTED = (100, 100, 100)   # Metadata

# Severity Colors
C_CRIT = (220, 38, 38)        # Rosso Allarme
C_HIGH = (234, 88, 12)        # Arancio Forte
C_MED = (202, 138, 4)        # Giallo/Oro
C_LOW = (16, 185, 129)       # Verde

# ==========================================
# 🧠 INTELLIGENCE ENGINE (DATA PROCESSING)
# ==========================================


class IntelEngine:
    def __init__(self, db_path):
        self.db_path = db_path

    def clean_text(self, text):
        """Pulisce il testo da rumore Telegram/Markdown"""
        if not text:
            return "Nessun dato disponibile."
        # Rimuove link markdown [testo](url)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        # Rimuove URL nudi
        text = re.sub(r'http\S+', '', text)
        # Rimuove caratteri markdown
        text = text.replace('**', '').replace('__', '').replace('`', '')
        # Rimuove newline eccessivi
        text = text.replace('\n', ' ').strip()
        return text

    def fetch_data(self):
        if not os.path.exists(self.db_path):
            print("❌ DB File not found.")
            return {'critical': [], 'timeline': [], 'stats': {'total': 0, 'threat_lvl': 'N/A'}}

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # 1. Trova Tabella
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r['name'] for r in cur.fetchall()]
        table = 'unique_events' if 'unique_events' in tables else (
            'events' if 'events' in tables else None)

        if not table:
            conn.close()
            return {'critical': [], 'timeline': [], 'stats': {'total': 0, 'threat_lvl': 'ERR'}}

        # 2. Analizza Colonne Disponibili
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r['name'] for r in cur.fetchall()]

        # Mapping dinamico colonne (Fallbacks sicuri)
        # Cerchiamo la data
        c_date = next(
            (c for c in ['first_seen_date', 'date', 'created_at'] if c in cols), None)
        # Cerchiamo lo score
        c_score = next(
            (c for c in ['severity_score', 'intensity_score', 'score'] if c in cols), None)
        # Cerchiamo il testo
        c_text = next(
            (c for c in ['full_text_dossier', 'content', 'text'] if c in cols), None)
        # Cerchiamo il titolo (SE ESISTE)
        c_title = 'title' if 'title' in cols else None

        # Se mancano colonne fondamentali, usiamo dei default nella query per non rompere SQL
        sql_date = c_date if c_date else "CURRENT_TIMESTAMP"
        sql_score = c_score if c_score else "0"
        sql_text = c_text if c_text else "''"
        # Se non c'è titolo, seleziona stringa vuota
        sql_title = c_title if c_title else "''"

        print(
            f"ℹ️ Mapping: Date={sql_date}, Text={sql_text}, Title={'MISSING (Auto-Gen)' if not c_title else c_title}")

        # 3. Estrazione Dati
        # Query unica più ampia, poi filtriamo in Python per sicurezza
        query = f"""
            SELECT {sql_title} as title_col, 
                   {sql_date} as date_col, 
                   {sql_text} as text_col, 
                   {sql_score} as score_col
            FROM {table}
            ORDER BY {sql_date} DESC
            LIMIT 20
        """

        try:
            cur.execute(query)
            rows = cur.fetchall()
        except Exception as e:
            print(f"❌ SQL Error: {e}")
            conn.close()
            return {'critical': [], 'timeline': [], 'stats': {'total': 0, 'threat_lvl': 'SQL_ERR'}}

        processed_events = []
        for r in rows:
            # Pulizia e gestione sicura
            txt = self.clean_text(r['text_col'])

            # GENERAZIONE TITOLO INTELLIGENTE
            # Se la colonna titolo c'era, usala. Altrimenti genera dai primi caratteri del testo.
            if c_title and r['title_col']:
                final_title = r['title_col']
            else:
                # Prendi le prime 8 parole o 60 caratteri
                words = txt.split(' ')[:8]
                final_title = " ".join(words) + "..."
                # Rimuovi eventuali caratteri non alfanumerici all'inizio
                final_title = re.sub(r'^[^a-zA-Z0-9]+', '', final_title)
                if not final_title:
                    final_title = "INTEL FRAGMENT"

            score = int(r['score_col']) if r['score_col'] is not None else 0

            processed_events.append({
                'title': final_title[:80],  # Tronca per sicurezza layout
                'date': str(r['date_col'])[:16],
                'text': txt,
                'score': score
            })

        conn.close()

        # 4. Separazione Critical / Timeline
        critical = [e for e in processed_events if e['score'] >= 7]
        # Timeline sono gli altri, o duplicati se servono (qui escludiamo i critici per pulizia)
        timeline = [e for e in processed_events if e not in critical]

        # Ordina critici per gravità
        critical.sort(key=lambda x: x['score'], reverse=True)
        # Prendi solo top 3 critici
        critical = critical[:3]

        stats = {
            'total': len(processed_events),
            'threat_lvl': "CRITICAL" if len(critical) > 0 else ("ELEVATED" if len(processed_events) > 5 else "LOW")
        }

        return {'critical': critical, 'timeline': timeline, 'stats': stats}

# ==========================================
# 📄 BRIEFING RENDERER
# ==========================================


class BriefingDoc(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=15)
        self.set_margins(15, 15, 15)

        script_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            self.add_font('Roboto', '', os.path.join(
                script_dir, 'fonts', 'Roboto-Regular.ttf'))
            self.add_font('Roboto', 'B', os.path.join(
                script_dir, 'fonts', 'Roboto-Bold.ttf'))
            self.add_font('JBM', '', os.path.join(
                script_dir, 'fonts', 'JetBrainsMono-Regular.ttf'))
            self.add_font('JBM', 'B', os.path.join(
                script_dir, 'fonts', 'JetBrainsMono-Bold.ttf'))
        except:
            print("⚠️ Fonts missing. Using fallback (Helvetica).")

    def header(self):
        if self.page_no() == 1:
            return
        self.set_fill_color(240, 240, 240)
        self.rect(0, 0, 210, 10, 'F')
        self.set_y(3)
        self.set_font('JBM', '', 7)
        self.set_text_color(150, 150, 150)
        self.cell(
            0, 4, f"IMPACT ATLAS // SITREP // PAGE {self.page_no()}", align='R')
        self.ln(10)

    def page_one(self, data):
        self.add_page()

        # --- HEADER ---
        self.set_fill_color(*C_DARK)
        self.rect(0, 0, 210, 35, 'F')

        self.set_y(10)
        self.set_font('JBM', 'B', 20)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "DAILY SITREP", align='L',
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.set_font('JBM', '', 9)
        self.set_text_color(150, 170, 190)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M Z")
        self.cell(0, 5, f"GENERATED: {dt} | CLEARANCE: EYES ONLY", align='L')

        # Status Box
        self.set_xy(140, 10)
        self.set_font('JBM', 'B', 10)
        threat = data['stats']['threat_lvl']
        col = C_CRIT if threat == "CRITICAL" else C_MED
        self.set_text_color(*col)
        self.cell(55, 5, f"THREAT LEVEL: {threat}",
                  align='R', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.set_y(45)

        # --- SECTION 1: FLASH TRAFFIC (CRITICAL) ---
        if data['critical']:
            self.set_font('JBM', 'B', 12)
            self.set_text_color(*C_CRIT)
            self.cell(0, 8, "/// FLASH TRAFFIC - PRIORITY INTERCEPTS",
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_draw_color(*C_CRIT)
            self.line(15, self.get_y(), 195, self.get_y())
            self.ln(3)

            for item in data['critical']:
                y_start = self.get_y()
                self.set_fill_color(255, 245, 245)
                self.rect(15, y_start, 180, 25, 'F')

                self.set_fill_color(*C_CRIT)
                self.rect(15, y_start, 2, 25, 'F')

                self.set_xy(19, y_start + 2)
                self.set_font('JBM', 'B', 9)
                self.set_text_color(*C_CRIT)
                self.cell(
                    100, 5, f"SEVERITY {item['score']}/10", new_x=XPos.RIGHT)

                self.set_font('JBM', '', 8)
                self.set_text_color(*C_MUTED)
                self.cell(0, 5, item['date'], align='R',
                          new_x=XPos.LMARGIN, new_y=YPos.NEXT)

                self.set_x(19)
                self.set_font('Roboto', 'B', 10)
                self.set_text_color(*C_TEXT)
                self.cell(0, 6, item['title'].upper(),
                          new_x=XPos.LMARGIN, new_y=YPos.NEXT)

                self.set_x(19)
                self.set_font('Roboto', '', 9)
                self.set_text_color(60, 60, 60)
                summary = item['text'][:140] + "..."
                self.multi_cell(170, 4, summary)

                self.set_y(y_start + 28)

        # --- SECTION 2: OPERATIONAL TIMELINE ---
        self.ln(5)
        self.set_font('JBM', 'B', 12)
        self.set_text_color(*C_ACCENT)
        self.cell(0, 8, "/// OPERATIONAL TIMELINE",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*C_ACCENT)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(5)

        col_w_date = 35
        col_w_sev = 15
        col_w_desc = 130

        self.set_font('JBM', 'B', 8)
        self.set_text_color(*C_MUTED)
        self.cell(col_w_date, 6, "TIMESTAMP", 0)
        self.cell(col_w_sev, 6, "LVL", 0)
        self.cell(col_w_desc, 6, "INTELLIGENCE SUMMARY",
                  0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

        for item in data['timeline']:
            if self.get_y() > 260:
                self.add_page()
                self.ln(10)

            self.set_font('JBM', '', 8)
            self.set_text_color(*C_MUTED)
            self.cell(col_w_date, 6, item['date'][5:], 0)

            s = item['score']
            col = C_HIGH if s >= 6 else (C_MED if s >= 4 else C_LOW)
            self.set_font('JBM', 'B', 8)
            self.set_text_color(*col)
            self.cell(col_w_sev, 6, str(s), 0)

            self.set_font('Roboto', '', 9)
            self.set_text_color(*C_TEXT)

            full_str = f"{item['title'].upper()} // {item['text'][:100]}..."

            x = self.get_x()
            y = self.get_y()
            self.multi_cell(col_w_desc, 5, full_str)

            h_diff = self.get_y() - y
            self.set_xy(15, y + h_diff + 3)

            self.set_draw_color(240, 240, 240)
            self.line(15, self.get_y()-1.5, 195, self.get_y()-1.5)


def main():
    print(">> ACQUIRING INTELLIGENCE...")
    engine = IntelEngine(DB_PATH)
    data = engine.fetch_data()

    if not data['critical'] and not data['timeline']:
        print("❌ Nessun dato trovato. Popola il DB.")
        return

    print(
        f">> PROCESSING: {len(data['critical'])} Critical, {len(data['timeline'])} Events.")

    pdf = BriefingDoc()
    pdf.page_one(data)

    if not os.path.exists('reports'):
        os.makedirs('reports')
    out = "reports/sitrep_v4.pdf"

    try:
        pdf.output(out)
        print(f"✅ SITREP READY: {out}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
