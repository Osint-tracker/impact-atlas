"""
Military Intelligence SITREP Generator (v5.0)
Robust, production-ready PDF briefing generator for Impact Atlas.

Features:
- Pathlib-based relative paths (no hardcoded Windows paths)
- Context manager for database connections
- Matplotlib chart integration (Threat Gauge)
- Smart keyword highlighting
- Proper page break handling
- Footer with page numbers
- Timestamped output filenames
- Python logging

Author: Impact Atlas Team
Date: February 2026
"""

import sqlite3
import datetime
import os
import re
import tempfile
import logging
from pathlib import Path

from fpdf import FPDF
from fpdf.enums import XPos, YPos

# Optional: Matplotlib for charts
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend for server-side
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# =============================================================================
# CONFIGURATION CLASS
# =============================================================================

class Config:
    """Centralized configuration for SITREP generation."""
    
    # Paths (relative to script location)
    SCRIPT_DIR = Path(__file__).parent.absolute()
    DB_PATH = SCRIPT_DIR / '..' / 'data' / 'raw_events.db'
    FONTS_DIR = SCRIPT_DIR / 'fonts'
    REPORTS_DIR = SCRIPT_DIR / 'reports'
    
    # Color Palette (NATO Dark Mode)
    C_DARK = (20, 25, 30)        # Header Background
    C_ACCENT = (59, 130, 246)    # Blue Intelligence
    C_TEXT = (30, 30, 30)        # Body Text
    C_MUTED = (100, 100, 100)    # Metadata
    
    # Severity Colors
    C_CRIT = (220, 38, 38)       # Red Alert
    C_HIGH = (234, 88, 12)       # Orange High
    C_MED = (202, 138, 4)        # Yellow/Gold
    C_LOW = (16, 185, 129)       # Green
    
    # Military Keywords for Highlighting
    KEYWORDS = [
        'missile', 'strike', 'target', 'casualties', 'drone', 'attack',
        'artillery', 'explosion', 'offensive', 'defensive', 'destroyed',
        'kill', 'impact', 'hit', 'launch', 'intercept', 'breach'
    ]
    
    # PDF Settings
    PAGE_MARGIN = 15
    FOOTER_HEIGHT = 15


# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('SITREP')


# =============================================================================
# CHART GENERATION (Matplotlib)
# =============================================================================

def generate_threat_gauge(stats: dict) -> str | None:
    """
    Generate a threat gauge pie chart.
    Returns path to temporary PNG file, or None if unavailable.
    """
    if not MATPLOTLIB_AVAILABLE:
        logger.warning("Matplotlib not available. Skipping chart generation.")
        return None
    
    try:
        fig, ax = plt.subplots(figsize=(2.5, 2.5), facecolor='#0f172a')
        
        critical = stats.get('critical_count', 0)
        normal = stats.get('normal_count', 1)
        
        sizes = [critical, normal]
        colors = ['#dc2626', '#334155']  # Red for critical, slate for normal
        explode = (0.05, 0) if critical > 0 else (0, 0)
        
        wedges, texts = ax.pie(
            sizes, 
            explode=explode,
            colors=colors,
            startangle=90,
            wedgeprops={'linewidth': 2, 'edgecolor': '#1e293b'}
        )
        
        # Center text
        ax.text(0, 0, f'{critical}', ha='center', va='center', 
                fontsize=20, fontweight='bold', color='#dc2626' if critical > 0 else '#64748b')
        ax.text(0, -0.3, 'CRITICAL', ha='center', va='center', 
                fontsize=7, color='#94a3b8')
        
        ax.set_title('THREAT INDEX', color='#f8fafc', fontsize=9, pad=10)
        
        # Save to temp file
        tmp_path = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp_path, dpi=150, transparent=True, bbox_inches='tight')
        plt.close(fig)
        
        logger.info(f"Chart generated: {tmp_path}")
        return tmp_path
        
    except Exception as e:
        logger.error(f"Chart generation failed: {e}")
        return None


def generate_timeline_histogram(events: list) -> str | None:
    """
    Generate events-per-day histogram.
    Returns path to temporary PNG file.
    """
    if not MATPLOTLIB_AVAILABLE or not events:
        return None
    
    try:
        # Aggregate by date
        date_counts = {}
        for e in events:
            date_key = e.get('date', '')[:10]  # YYYY-MM-DD
            if date_key:
                date_counts[date_key] = date_counts.get(date_key, 0) + 1
        
        if not date_counts:
            return None
        
        dates = sorted(date_counts.keys())[-7:]  # Last 7 days
        counts = [date_counts.get(d, 0) for d in dates]
        
        fig, ax = plt.subplots(figsize=(3, 1.5), facecolor='#0f172a')
        ax.set_facecolor('#0f172a')
        
        bars = ax.bar(range(len(dates)), counts, color='#3b82f6', edgecolor='#1e293b')
        
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[5:] for d in dates], fontsize=6, color='#64748b')
        ax.tick_params(axis='y', colors='#64748b', labelsize=6)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#334155')
        ax.spines['bottom'].set_color('#334155')
        
        tmp_path = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp_path, dpi=150, transparent=True, bbox_inches='tight')
        plt.close(fig)
        
        return tmp_path
        
    except Exception as e:
        logger.error(f"Histogram generation failed: {e}")
        return None


# =============================================================================
# INTELLIGENCE ENGINE (Data Processing)
# =============================================================================

class IntelEngine:
    """Fetches and processes intelligence data from database."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
    
    def clean_text(self, text: str) -> str:
        """Clean text from Telegram/Markdown noise."""
        if not text:
            return "No data available."
        
        # Remove markdown links [text](url)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        # Remove bare URLs
        text = re.sub(r'http\S+', '', text)
        # Remove markdown characters
        text = text.replace('**', '').replace('__', '').replace('`', '')
        # Collapse excessive newlines
        text = re.sub(r'\n+', ' ', text).strip()
        
        return text
    
    def highlight_keywords(self, text: str) -> str:
        """
        Mark military keywords for later highlighting in PDF.
        Returns text with keywords wrapped in special markers.
        """
        for kw in Config.KEYWORDS:
            # Case-insensitive replacement, preserve original case
            pattern = re.compile(re.escape(kw), re.IGNORECASE)
            text = pattern.sub(f'**{kw.upper()}**', text)
        return text
    
    def fetch_data(self) -> dict:
        """
        Fetch intelligence data from database.
        Uses context manager for safe connection handling.
        """
        # Validate database existence
        if not self.db_path.exists():
            logger.error(f"Database not found: {self.db_path}")
            return self._empty_response('DB_NOT_FOUND')
        
        # Use context manager for safe connection handling
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return self._process_database(conn)
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            return self._empty_response('DB_ERROR')
    
    def _empty_response(self, error_code: str = 'N/A') -> dict:
        """Return empty data structure."""
        return {
            'critical': [],
            'timeline': [],
            'stats': {
                'total': 0,
                'critical_count': 0,
                'normal_count': 0,
                'threat_lvl': error_code
            }
        }
    
    def _process_database(self, conn: sqlite3.Connection) -> dict:
        """Process database and extract intelligence."""
        cur = conn.cursor()
        
        # 1. Find target table
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r['name'] for r in cur.fetchall()]
        table = 'unique_events' if 'unique_events' in tables else (
            'events' if 'events' in tables else None)
        
        if not table:
            logger.error(f"No valid table found. Available: {tables}")
            return self._empty_response('NO_TABLE')
        
        logger.info(f"Using table: {table}")
        
        # 2. Auto-detect columns (graceful fallback)
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r['name'] for r in cur.fetchall()]
        logger.debug(f"Available columns: {cols}")
        
        # Dynamic column mapping with fallbacks
        c_date = next((c for c in ['first_seen_date', 'last_seen_date', 'date', 'created_at'] if c in cols), None)
        c_score = next((c for c in ['severity_score', 'intensity_score', 'tie_score', 'score'] if c in cols), None)
        c_text = next((c for c in ['full_text_dossier', 'content', 'text', 'description'] if c in cols), None)
        c_title = 'title' if 'title' in cols else None
        
        logger.info(f"Column mapping: date={c_date}, score={c_score}, text={c_text}, title={c_title or 'AUTO-GEN'}")
        
        # Build safe SQL with defaults
        sql_date = c_date if c_date else "CURRENT_TIMESTAMP"
        sql_score = c_score if c_score else "0"
        sql_text = c_text if c_text else "''"
        sql_title = c_title if c_title else "''"
        
        # 3. Extract data
        query = f"""
            SELECT {sql_title} as title_col, 
                   {sql_date} as date_col, 
                   {sql_text} as text_col, 
                   {sql_score} as score_col
            FROM {table}
            ORDER BY {sql_date} DESC
            LIMIT 30
        """
        
        try:
            cur.execute(query)
            rows = cur.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Query failed: {e}")
            return self._empty_response('SQL_ERROR')
        
        # 4. Process rows
        processed_events = []
        for r in rows:
            txt = self.clean_text(r['text_col'])
            
            # Smart title generation
            if c_title and r['title_col']:
                final_title = r['title_col']
            else:
                words = txt.split(' ')[:8]
                final_title = " ".join(words) + "..."
                final_title = re.sub(r'^[^a-zA-Z0-9]+', '', final_title)
                if not final_title:
                    final_title = "INTEL FRAGMENT"
            
            score = int(r['score_col']) if r['score_col'] is not None else 0
            
            processed_events.append({
                'title': final_title[:80],
                'date': str(r['date_col'])[:16],
                'text': txt,
                'text_highlighted': self.highlight_keywords(txt),
                'score': score
            })
        
        # 5. Separate Critical / Timeline
        critical = [e for e in processed_events if e['score'] >= 7]
        timeline = [e for e in processed_events if e not in critical]
        
        critical.sort(key=lambda x: x['score'], reverse=True)
        critical = critical[:3]
        
        stats = {
            'total': len(processed_events),
            'critical_count': len([e for e in processed_events if e['score'] >= 7]),
            'normal_count': len([e for e in processed_events if e['score'] < 7]),
            'threat_lvl': "CRITICAL" if len(critical) > 0 else ("ELEVATED" if len(processed_events) > 5 else "LOW")
        }
        
        logger.info(f"Processed {stats['total']} events ({stats['critical_count']} critical)")
        
        return {'critical': critical, 'timeline': timeline, 'stats': stats}


# =============================================================================
# PDF DOCUMENT RENDERER
# =============================================================================

class BriefingDoc(FPDF):
    """Custom FPDF class for Military Intelligence SITREPs."""
    
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=Config.FOOTER_HEIGHT + 5)
        self.set_margins(Config.PAGE_MARGIN, Config.PAGE_MARGIN, Config.PAGE_MARGIN)
        self._total_pages = '{nb}'
        self._load_fonts()
    
    def _load_fonts(self):
        """Load custom fonts with robust error handling."""
        fonts_dir = Config.FONTS_DIR
        
        fonts = [
            ('Roboto', '', 'Roboto-Regular.ttf'),
            ('Roboto', 'B', 'Roboto-Bold.ttf'),
            ('JBM', '', 'JetBrainsMono-Regular.ttf'),
            ('JBM', 'B', 'JetBrainsMono-Bold.ttf'),
        ]
        
        for family, style, filename in fonts:
            font_path = fonts_dir / filename
            if font_path.exists():
                try:
                    self.add_font(family, style, str(font_path))
                    logger.debug(f"Loaded font: {filename}")
                except Exception as e:
                    logger.warning(f"Failed to load {filename}: {e}")
            else:
                logger.warning(f"Font not found: {font_path}")
        
        logger.info("Font loading complete (using fallbacks for missing fonts)")
    
    def header(self):
        """Render page header (except page 1)."""
        if self.page_no() == 1:
            return
        
        self.set_fill_color(240, 240, 240)
        self.rect(0, 0, 210, 10, 'F')
        self.set_y(3)
        self.set_font('Helvetica', '', 7)  # Safe fallback
        self.set_text_color(150, 150, 150)
        self.cell(0, 4, f"IMPACT ATLAS // SITREP // PAGE {self.page_no()}", align='R')
        self.ln(10)
    
    def footer(self):
        """Render page footer with page numbers and disclaimer."""
        self.set_y(-Config.FOOTER_HEIGHT)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(100, 100, 100)
        
        # Page numbers
        self.cell(0, 4, f"Page {self.page_no()}/{{nb}}", align='C')
        self.ln(3)
        
        # Disclaimer
        self.set_font('Helvetica', 'I', 6)
        self.set_text_color(150, 150, 150)
        self.cell(0, 4, "CONFIDENTIAL // ACADEMIC USE ONLY // IMPACT ATLAS", align='C')
    
    def check_page_break(self, height: float = 30):
        """Check if we need a page break before rendering content."""
        if self.get_y() + height > 297 - Config.FOOTER_HEIGHT - 10:
            self.add_page()
            return True
        return False
    
    def render_highlighted_text(self, text: str, width: float):
        """
        Render text with keyword highlighting.
        Keywords marked with ** are rendered in bold red.
        """
        # Split by ** markers
        parts = re.split(r'\*\*([^*]+)\*\*', text)
        
        for i, part in enumerate(parts):
            if i % 2 == 1:  # Keyword (odd indices)
                self.set_font('Roboto', 'B', 9)
                self.set_text_color(*Config.C_CRIT)
                self.write(5, part)
                self.set_font('Roboto', '', 9)
                self.set_text_color(60, 60, 60)
            else:
                self.write(5, part)
    
    def page_one(self, data: dict, chart_path: str = None):
        """Render the main SITREP page."""
        self.add_page()
        self.alias_nb_pages()
        
        # --- HEADER ---
        self.set_fill_color(*Config.C_DARK)
        self.rect(0, 0, 210, 35, 'F')
        
        self.set_y(10)
        self.set_font('Helvetica', 'B', 20)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, "DAILY SITREP", align='L', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        self.set_font('Helvetica', '', 9)
        self.set_text_color(150, 170, 190)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M Z")
        self.cell(0, 5, f"GENERATED: {dt} | CLEARANCE: EYES ONLY", align='L')
        
        # Embed chart (if available)
        if chart_path and os.path.exists(chart_path):
            try:
                self.image(chart_path, x=155, y=5, w=45)
            except Exception as e:
                logger.warning(f"Failed to embed chart: {e}")
        
        # Status Box
        self.set_xy(140, 10)
        self.set_font('Helvetica', 'B', 10)
        threat = data['stats']['threat_lvl']
        col = Config.C_CRIT if threat == "CRITICAL" else Config.C_MED
        self.set_text_color(*col)
        self.cell(55, 5, f"THREAT LEVEL: {threat}", align='R', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        self.set_y(45)
        
        # --- SECTION 1: FLASH TRAFFIC (CRITICAL) ---
        if data['critical']:
            self.set_font('Helvetica', 'B', 12)
            self.set_text_color(*Config.C_CRIT)
            self.cell(0, 8, "/// FLASH TRAFFIC - PRIORITY INTERCEPTS", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_draw_color(*Config.C_CRIT)
            self.line(15, self.get_y(), 195, self.get_y())
            self.ln(3)
            
            for item in data['critical']:
                self.check_page_break(35)
                
                y_start = self.get_y()
                self.set_fill_color(255, 245, 245)
                self.rect(15, y_start, 180, 28, 'F')
                
                self.set_fill_color(*Config.C_CRIT)
                self.rect(15, y_start, 2, 28, 'F')
                
                self.set_xy(19, y_start + 2)
                self.set_font('Helvetica', 'B', 9)
                self.set_text_color(*Config.C_CRIT)
                self.cell(100, 5, f"SEVERITY {item['score']}/10", new_x=XPos.RIGHT)
                
                self.set_font('Helvetica', '', 8)
                self.set_text_color(*Config.C_MUTED)
                self.cell(0, 5, item['date'], align='R', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                
                self.set_x(19)
                self.set_font('Helvetica', 'B', 10)
                self.set_text_color(*Config.C_TEXT)
                self.cell(0, 6, item['title'].upper()[:60], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                
                self.set_x(19)
                self.set_font('Roboto', '', 9)
                self.set_text_color(60, 60, 60)
                summary = item.get('text_highlighted', item['text'])[:150] + "..."
                self.render_highlighted_text(summary, 170)
                
                self.set_y(y_start + 32)
        
        # --- SECTION 2: OPERATIONAL TIMELINE ---
        self.ln(5)
        self.check_page_break(20)
        
        self.set_font('Helvetica', 'B', 12)
        self.set_text_color(*Config.C_ACCENT)
        self.cell(0, 8, "/// OPERATIONAL TIMELINE", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*Config.C_ACCENT)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(5)
        
        col_w_date = 35
        col_w_sev = 15
        col_w_desc = 130
        
        self.set_font('Helvetica', 'B', 8)
        self.set_text_color(*Config.C_MUTED)
        self.cell(col_w_date, 6, "TIMESTAMP", 0)
        self.cell(col_w_sev, 6, "LVL", 0)
        self.cell(col_w_desc, 6, "INTELLIGENCE SUMMARY", 0, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)
        
        for item in data['timeline']:
            self.check_page_break(15)
            
            self.set_font('Helvetica', '', 8)
            self.set_text_color(*Config.C_MUTED)
            self.cell(col_w_date, 6, item['date'][5:] if len(item['date']) > 5 else item['date'], 0)
            
            s = item['score']
            col = Config.C_HIGH if s >= 6 else (Config.C_MED if s >= 4 else Config.C_LOW)
            self.set_font('Helvetica', 'B', 8)
            self.set_text_color(*col)
            self.cell(col_w_sev, 6, str(s), 0)
            
            self.set_font('Roboto', '', 9)
            self.set_text_color(*Config.C_TEXT)
            
            full_str = f"{item['title'].upper()[:40]} // {item['text'][:80]}..."
            
            y = self.get_y()
            self.multi_cell(col_w_desc, 5, full_str)
            h_diff = self.get_y() - y
            self.set_xy(15, y + h_diff + 3)
            
            self.set_draw_color(240, 240, 240)
            self.line(15, self.get_y() - 1.5, 195, self.get_y() - 1.5)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point for SITREP generation."""
    logger.info("=" * 50)
    logger.info("SITREP GENERATOR v5.0")
    logger.info("=" * 50)
    
    # Initialize engine
    engine = IntelEngine(Config.DB_PATH)
    data = engine.fetch_data()
    
    if not data['critical'] and not data['timeline']:
        logger.error("No data found. Populate the database first.")
        return
    
    logger.info(f"Processing: {len(data['critical'])} Critical, {len(data['timeline'])} Timeline events")
    
    # Generate charts
    chart_path = generate_threat_gauge(data['stats'])
    
    # Generate PDF
    pdf = BriefingDoc()
    pdf.page_one(data, chart_path)
    
    # Ensure reports directory exists
    Config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Timestamped filename (avoids PermissionError if file is open)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Config.REPORTS_DIR / f"sitrep_{timestamp}.pdf"
    
    try:
        pdf.output(str(output_path))
        logger.info(f"✅ SITREP generated: {output_path}")
    except PermissionError:
        logger.error(f"Cannot write to {output_path}. Close any open PDFs.")
    except Exception as e:
        logger.error(f"PDF generation failed: {e}")
    finally:
        # Clean up temp chart
        if chart_path and os.path.exists(chart_path):
            try:
                os.remove(chart_path)
            except:
                pass


if __name__ == "__main__":
    main()
