"""
Impact Atlas - Military Intelligence SITREP Generator (v6.0)
------------------------------------------------------------
A professional-grade intelligence briefing generator for the Impact Atlas platform.
Produces multi-page PDF reports with:
- Slate & Amber design system (Dark Mode)
- TIE (Target-Kinetic-Effect) vector analysis
- Strategic sector classification
- Matplotlib-based tactical analytics
- Flash Traffic & Operational Timeline

Author: Impact Atlas Team
Date: February 2026
"""

import sqlite3
import datetime
import os
import re
import math
import json
import logging
import tempfile
from pathlib import Path
from collections import defaultdict, Counter

from fpdf import FPDF
from fpdf.enums import XPos, YPos

# Optional: Matplotlib for charts
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("WARNING: Matplotlib not found. Charts will be disabled.")

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Centralized configuration aligned with Slate & Amber Design System."""
    
    # Paths
    SCRIPT_DIR = Path(__file__).parent.absolute()
    DB_PATH = SCRIPT_DIR.parent / 'data' / 'raw_events.db'
    FONTS_DIR = SCRIPT_DIR / 'fonts'
    REPORTS_DIR = SCRIPT_DIR / 'reports'
    
    # Color Palette (RGB)
    C_BG_DARK = (15, 23, 42)     # #0f172a (Slate 900)
    C_BG_CARD = (30, 41, 59)     # #1e293b (Slate 800)
    C_TEXT_PRI = (241, 245, 249) # #f1f5f9 (Slate 100)
    C_TEXT_SEC = (148, 163, 184) # #94a3b8 (Slate 400)
    C_TEXT_MUTED = (100, 116, 139) # #64748b (Slate 500)
    
    C_ACCENT   = (245, 158, 11)  # #f59e0b (Amber 500)
    C_CRIT     = (239, 68, 68)   # #ef4444 (Red 500)
    C_HIGH     = (249, 115, 22)  # #f97316 (Orange 500)
    C_MED      = (234, 179, 8)   # #eab308 (Yellow 500)
    C_LOW      = (100, 116, 139) # #64748b (Slate 500)
    
    # Hex Colors for Matplotlib
    HEX_BG      = '#0f172a'
    HEX_ACCENT  = '#f59e0b'
    HEX_TEXT    = '#f1f5f9'
    HEX_GRID    = '#334155'
    
    # Fonts
    FONT_HEAD = 'Roboto'
    FONT_BODY = 'Roboto'
    FONT_MONO = 'JBM'  # JetBrains Mono
    
    # Layout
    PAGE_MARGIN = 15
    FOOTER_HEIGHT = 15

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger('SITREP')

# =============================================================================
# ANALYTICS HELPERS
# =============================================================================

def classify_sector(lat, lon, target_type):
    """Derive strategic sector from geography and target (Project Standard)."""
    target_type = (target_type or '').lower()
    
    # Priority 1: Energy
    if any(k in target_type for k in ['power', 'grid', 'dam', 'plant', 'energy']):
        return 'ENERGY_COERCION'
        
    # Priority 2: Deep Strikes
    if lat and lon:
        try:
            flat, flon = float(lat), float(lon)
            if flat > 50.0 and flon > 36.0: return 'DEEP_STRIKES_RU'
        except: pass
        
    # Priority 3: Geography
    if lat and lon:
        try:
            flat, flon = float(lat), float(lon)
            if flon <= 36.0 and flat < 48.0: return 'SOUTHERN_FRONT'
            if flon > 36.0 and flat < 50.0: return 'EASTERN_FRONT'
        except: pass
        
    return 'EASTERN_FRONT' # Detailed fallback

# =============================================================================
# INTELLIGENCE ENGINE
# =============================================================================

class IntelEngine:
    """Fetches and processes intelligence data with full TIE integration."""
    
    def __init__(self, db_path):
        self.db_path = db_path
        
    def fetch_data(self, days=14):
        """Fetch completed events from the last N days."""
        if not self.db_path.exists():
            logger.error(f"DB not found at {self.db_path}")
            return None
            
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Calculate date threshold
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        
        query = f"""
            SELECT 
                event_id, last_seen_date, title, description,
                tie_score, kinetic_score, target_score, effect_score,
                reliability, bias_score, ai_report_json,
                ai_summary, urls_list
            FROM unique_events
            WHERE ai_analysis_status = 'COMPLETED'
            AND last_seen_date >= '{cutoff}'
            ORDER BY tie_score DESC, last_seen_date DESC
        """
        
        try:
            cur.execute(query)
            rows = cur.fetchall()
        except sqlite3.Error as e:
            logger.error(f"SQL Error: {e}")
            conn.close()
            return None
            
        events = []
        for r in rows:
            # Parse JSON for deep metrics
            ai_data = {}
            if r['ai_report_json']:
                try: ai_data = json.loads(r['ai_report_json'])
                except: pass
            
            # Extract Geo
            lat, lon = None, None
            try:
                geo = ai_data.get('tactics', {}).get('geo_location', {}).get('explicit', {})
                lat, lon = geo.get('lat'), geo.get('lon')
            except: pass
            
            # Determine Classification
            cls = ai_data.get('classification') or 'UNKNOWN'
            
            # Determine Sector
            tgt = ai_data.get('target_type') or ''
            sector = classify_sector(lat, lon, tgt)
            
            # Clean Metrics
            tie = float(r['tie_score'] or 0)
            k = float(r['kinetic_score'] or 0)
            t = float(r['target_score'] or 0)
            e = float(r['effect_score'] or 0)
            rel = int(r['reliability'] or 50)
            
            events.append({
                'id': r['event_id'],
                'date': str(r['last_seen_date'])[:16],
                'title': r['title'] or 'Unknown Event',
                'desc': r['description'] or r['ai_summary'] or '',
                'tie': tie,
                'k': k, 't': t, 'e': e,
                'rel': rel,
                'bias': r['bias_score'],
                'cls': cls,
                'sector': sector,
                'lat': lat, 'lon': lon,
                'sources': r['urls_list']
            })
            
        conn.close()
        
        # Compute Stats
        total = len(events)
        crit_count = len([e for e in events if e['tie'] >= 70])
        avg_tie = sum(e['tie'] for e in events) / total if total else 0
        avg_rel = sum(e['rel'] for e in events) / total if total else 0
        
        # Distributions
        cls_dist = Counter(e['cls'] for e in events)
        sector_dist = Counter(e['sector'] for e in events)
        tie_dist = {
            'CRIT': len([e for e in events if e['tie'] >= 70]),
            'HIGH': len([e for e in events if 40 <= e['tie'] < 70]),
            'MED': len([e for e in events if 20 <= e['tie'] < 40]),
            'LOW': len([e for e in events if e['tie'] < 20]),
        }

        # Separating Critical and Timeline
        critical_events = [e for e in events if e['tie'] >= 70]
        # Fallback if no critical events
        if not critical_events and events:
            critical_events = sorted(events, key=lambda x: x['tie'], reverse=True)[:3]

        timeline_events = [e for e in events if e['id'] not in [c['id'] for c in critical_events]]
        
        return {
            'events': events,
            'critical': critical_events[:5], # Top 5 critical
            'timeline': timeline_events,
            'stats': {
                'total': total,
                'critical_count': crit_count,
                'avg_tie': avg_tie,
                'avg_reliability': avg_rel,
                'cls_dist': cls_dist,
                'sector_dist': sector_dist,
                'tie_dist': tie_dist
            }
        }

# =============================================================================
# CHART GENERATION
# =============================================================================

class ChartGenerator:
    """Generates analytics charts using Matplotlib with Slate & Amber theme."""
    
    @staticmethod
    def _setup_plot():
        """Apply global style settings."""
        plt.style.use('dark_background')
        plt.rcParams['axes.facecolor'] = Config.HEX_BG
        plt.rcParams['figure.facecolor'] = Config.HEX_BG
        plt.rcParams['text.color'] = Config.HEX_TEXT
        plt.rcParams['axes.labelcolor'] = Config.HEX_TEXT
        plt.rcParams['xtick.color'] = Config.HEX_TEXT
        plt.rcParams['ytick.color'] = Config.HEX_TEXT
        plt.rcParams['axes.edgecolor'] = Config.HEX_GRID
        plt.rcParams['grid.color'] = Config.HEX_GRID
        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['font.sans-serif'] = ['Arial', 'Roboto', 'DejaVu Sans']
        
    @staticmethod
    def generate_threat_gauge(stats):
        """Donut chart for Threat Level."""
        if not MATPLOTLIB_AVAILABLE: return None
        
        ChartGenerator._setup_plot()
        fig, ax = plt.subplots(figsize=(3, 3))
        
        # Data
        val = stats['avg_tie']
        
        # Gauge background and value
        wedges, _ = ax.pie([val, 100-val], startangle=90, colors=[Config.HEX_ACCENT, '#1e293b'],
                           wedgeprops={'width': 0.3, 'edgecolor': Config.HEX_BG})
        
        # Center Text
        ax.text(0, 0, f"{int(val)}", ha='center', va='center', fontsize=24, fontweight='bold', color=Config.HEX_TEXT)
        ax.text(0, -0.25, "AVG TIE", ha='center', va='center', fontsize=8, color='#94a3b8')
        
        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp

    @staticmethod
    def generate_tie_distribution(stats):
        """Horizontal bar chart for TIE levels."""
        if not MATPLOTLIB_AVAILABLE: return None
        
        ChartGenerator._setup_plot()
        fig, ax = plt.subplots(figsize=(5, 3))
        
        dist = stats['tie_dist']
        labels = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
        values = [dist['CRIT'], dist['HIGH'], dist['MED'], dist['LOW']]
        colors = ['#ef4444', '#f97316', '#eab308', '#64748b']
        
        y_pos = np.arange(len(labels))
        bars = ax.barh(y_pos, values, color=colors, height=0.6)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Events Count', fontsize=8)
        
        # Value labels
        for i, v in enumerate(values):
            ax.text(v + 1, i, str(v), va='center', fontsize=8, color=Config.HEX_TEXT)
        
        # Remove spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp

    @staticmethod
    def generate_sector_pie(stats):
        """Donut chart for Strategic Sectors."""
        if not MATPLOTLIB_AVAILABLE: return None
        
        ChartGenerator._setup_plot()
        fig, ax = plt.subplots(figsize=(4, 4))
        
        data = stats['sector_dist']
        labels = list(data.keys()) or ['No Data']
        values = list(data.values()) or [1]
        
        # Custom colors for sectors
        colors = ['#f59e0b', '#3b82f6', '#ef4444', '#10b981', '#8b5cf6']
        
        wedges, texts, autotexts = ax.pie(values, labels=labels, autopct='%1.1f%%',
                                          startangle=140, colors=colors[:len(labels)],
                                          wedgeprops={'width': 0.4, 'edgecolor': Config.HEX_BG},
                                          textprops={'color': Config.HEX_TEXT, 'fontsize': 8},
                                          pctdistance=0.85)
        
        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp
        
    @staticmethod
    def generate_classification_bar(stats):
        """Horizontal bar for Classifications."""
        if not MATPLOTLIB_AVAILABLE: return None
        
        ChartGenerator._setup_plot()
        fig, ax = plt.subplots(figsize=(5, 3))
        
        data = stats['cls_dist']
        # Sort by value
        sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=False) # Ascending for horiz bar
        labels = [k.replace('_', ' ') for k, v in sorted_items]
        values = [v for k, v in sorted_items]
        
        y_pos = np.arange(len(labels))
        ax.barh(y_pos, values, color=Config.HEX_ACCENT, alpha=0.9, height=0.6)
        
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=7)
        ax.set_xlabel('Count', fontsize=8)
        
        # Remove spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp

    @staticmethod
    def generate_timeline(events):
        """Bar chart for last 14 days."""
        if not MATPLOTLIB_AVAILABLE: return None
        
        ChartGenerator._setup_plot()
        fig, ax = plt.subplots(figsize=(10, 3))
        
        # Aggregate by date
        dates = [e['date'][:10] for e in events]
        counts = Counter(dates)
        
        # Sort dates
        sorted_dates = sorted(counts.keys())
        sorted_counts = [counts[d] for d in sorted_dates]
        
        if not sorted_dates:
             return None

        x_pos = np.arange(len(sorted_dates))
        ax.bar(x_pos, sorted_counts, color=Config.HEX_ACCENT, alpha=0.8)
        
        ax.set_xticks(x_pos)
        ax.set_xticklabels([d[5:] for d in sorted_dates], rotation=45, fontsize=8)
        
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_ylabel('Events', fontsize=8)
        
        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp

# =============================================================================
# PDF RENDERER
# =============================================================================

class BriefingDoc(FPDF):
    """Custom FPDF class for Impact Atlas SITREPs."""
    
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=Config.FOOTER_HEIGHT + 5)
        self.set_margins(Config.PAGE_MARGIN, Config.PAGE_MARGIN, Config.PAGE_MARGIN)
        self._total_pages = '{nb}'
        self._load_fonts()
    
    def _load_fonts(self):
        """Load custom fonts."""
        # Clean up font handling
        fonts = [
            ('Roboto', '', 'Roboto-Regular.ttf'),
            ('Roboto', 'B', 'Roboto-Bold.ttf'),
            ('JBM', '', 'JetBrainsMono-Regular.ttf'),
            ('JBM', 'B', 'JetBrainsMono-Bold.ttf'),
        ]
        
        for family, style, filename in fonts:
            font_path = Config.FONTS_DIR / filename
            if font_path.exists():
                try:
                    self.add_font(family, style, str(font_path))
                except Exception as e:
                    logger.warning(f"Font Load Error {filename}: {e}")
            else:
                logger.warning(f"Font Missing: {font_path}")
        
    def header(self):
        """Header for Pages 2+"""
        if self.page_no() == 1: return
        
        # Dark Background Strip
        self.set_fill_color(*Config.C_BG_DARK)
        self.rect(0, 0, 210, 15, 'F')
        
        self.set_y(5)
        self.set_font(Config.FONT_HEAD, 'B', 10)
        self.set_text_color(*Config.C_ACCENT)
        self.cell(0, 5, "IMPACT ATLAS // SITREP", align='L')
        
        self.set_xy(0, 5)
        self.set_font(Config.FONT_HEAD, '', 8)
        self.set_text_color(*Config.C_TEXT_SEC)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M Z")
        self.cell(200, 5, f"{dt}", align='R')
        self.ln(15)

    def footer(self):
        """Footer with Disclaimer."""
        self.set_y(-Config.FOOTER_HEIGHT)
        self.set_font(Config.FONT_HEAD, '', 8)
        self.set_text_color(*Config.C_TEXT_MUTED)
        
        self.cell(0, 4, f"Page {self.page_no()}/{{nb}}", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font(Config.FONT_HEAD, '', 7)
        self.cell(0, 4, "ACADEMIC USE ONLY. IMPACT ATLAS.", align='C')

    def draw_badge(self, text, bg_color, text_color=(255,255,255), x=None, y=None, w=20, h=6):
        """Draws a rounded badge."""
        if x is None: x = self.get_x()
        if y is None: y = self.get_y()
        
        self.set_fill_color(*bg_color)
        self.rect(x, y, w, h, 'F')
        
        self.set_xy(x, y+1)
        self.set_font(Config.FONT_HEAD, 'B', 7)
        self.set_text_color(*text_color)
        self.cell(w, 4, text, align='C')
        self.set_text_color(*Config.C_TEXT_PRI) # Reset

    def page_cover(self, data, charts):
        """Render Page 1: Executive Summary & Metrics."""
        self.add_page()
        
        # --- HERO HEADER ---
        self.set_fill_color(*Config.C_BG_DARK)
        self.rect(0, 0, 210, 60, 'F')
        
        # Border Accent
        self.set_fill_color(*Config.C_ACCENT)
        self.rect(0, 58, 210, 2, 'F')
        
        self.set_y(20)
        self.set_font(Config.FONT_HEAD, 'B', 24)
        self.set_text_color(*Config.C_ACCENT)
        self.cell(0, 10, "IMPACT ATLAS", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        self.set_font(Config.FONT_HEAD, '', 14)
        self.set_text_color(*Config.C_TEXT_PRI)
        self.cell(0, 10, "DAILY INTELLIGENCE BRIEFING", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        self.set_font(Config.FONT_MONO, '', 9)
        self.set_text_color(*Config.C_TEXT_SEC)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M Zulu")
        self.cell(0, 6, f"GENERATED: {dt} | CLEARANCE: EYES ONLY", align='C')
        
        self.set_y(70)
        
        # --- METRICS GRID ---
        col_width = 85
        spacing = 10
        
        # LEFT: Threat Gauge
        if charts.get('threat'):
            self.image(charts['threat'], x=25, y=70, w=60)
        
        # RIGHT: Key Stats
        x_start = 110
        y_start = 75
        self.set_xy(x_start, y_start)
        
        stats = data['stats']
        
        def stat_row(label, value, color=Config.C_TEXT_PRI):
            self.set_x(x_start)
            self.set_font(Config.FONT_HEAD, '', 10)
            self.set_text_color(*Config.C_TEXT_SEC)
            self.cell(40, 8, label)
            self.set_font(Config.FONT_MONO, 'B', 10)
            self.set_text_color(*color)
            self.cell(30, 8, str(value), align='R', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            
        stat_row("Total Events", stats['total'])
        stat_row("Critical Events", stats['critical_count'], Config.C_CRIT)
        stat_row("Avg TIE Score", f"{stats['avg_tie']:.1f}", Config.C_ACCENT)
        stat_row("Avg Reliability", f"{stats['avg_reliability']:.1f}%")
        
        # --- EXECUTIVE SUMMARY ---
        self.set_y(135)
        self.set_font(Config.FONT_HEAD, 'B', 14)
        self.set_text_color(*Config.C_BG_DARK) # Black for header? No, dark slate.
        self.set_text_color(20, 30, 40)
        self.cell(0, 10, "/// EXECUTIVE SUMMARY", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        
        self.set_draw_color(*Config.C_ACCENT)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(5)
        
        self.set_font(Config.FONT_BODY, '', 11)
        self.set_text_color(40, 40, 40)
        
        # Auto-generate summary text
        crit_txt = f"{stats['critical_count']} CRITICAL EVENTS detected in the last 14 days."
        sector_top = stats['sector_dist'].most_common(1)
        sector_txt = f"Primary activity concentrated in {sector_top[0][0]}" if sector_top else ""
        class_top = stats['cls_dist'].most_common(1)
        class_txt = f"dominated by {class_top[0][0]} operations." if class_top else ""
        
        summary_text = (
            f"This SITREP covers operational developments across the theater. {crit_txt} "
            f"{sector_txt} {class_txt} "
            "Analysis indicates continued high kinetic intensity in contested zones. "
            "Reliability of incoming intelligence remains stable. "
        )
        self.multi_cell(0, 6, summary_text)
        
        # --- CHARTS ROW ---
        y_charts = 180
        if charts.get('sector'):
            self.set_xy(20, y_charts)
            self.set_font(Config.FONT_HEAD, 'B', 9)
            self.cell(80, 5, "STRATEGIC SECTORS", align='C')
            self.image(charts['sector'], x=25, y=y_charts+5, w=70)
            
        if charts.get('classification'):
            self.set_xy(110, y_charts)
            self.cell(80, 5, "TACTICAL CLASSIFICATION", align='C')
            self.image(charts['classification'], x=115, y=y_charts+5, w=80)

    def page_flash(self, data):
        """Render Page 2: Flash Traffic (Critical Events)."""
        self.add_page()
        
        self.set_font(Config.FONT_HEAD, 'B', 14)
        self.set_text_color(*Config.C_CRIT)
        self.cell(0, 10, "/// FLASH TRAFFIC - PRIORITY INTERCEPTS", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*Config.C_CRIT)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(5)
        
        for item in data['critical']:
            # Card Background
            y_start = self.get_y()
            if y_start > 250: 
                self.add_page()
                y_start = self.get_y()
                
            # Left Stripe
            self.set_fill_color(*Config.C_CRIT)
            self.rect(15, y_start, 2, 35, 'F')
            
            # Content
            self.set_xy(20, y_start)
            
            # Header: TIE Score | Date
            self.set_font(Config.FONT_MONO, 'B', 10)
            self.set_text_color(*Config.C_CRIT)
            self.cell(25, 5, f"TIE: {int(item['tie'])}")
            
            self.set_font(Config.FONT_MONO, '', 8)
            self.set_text_color(*Config.C_TEXT_MUTED)
            self.cell(0, 5, f" | {item['date']} | {item['sector']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            
            # Title
            self.set_x(20)
            self.set_font(Config.FONT_HEAD, 'B', 11)
            self.set_text_color(20, 20, 20)
            self.cell(0, 6, item['title'].upper()[:75], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            
            # Vectors
            self.set_x(20)
            self.set_font(Config.FONT_MONO, 'B', 8)
            self.set_text_color(*Config.C_BG_DARK)
            self.cell(30, 5, f"K:{int(item['k'])} T:{int(item['t'])} E:{int(item['e'])}")
            self.set_font(Config.FONT_MONO, '', 7)
            self.set_text_color(*Config.C_TEXT_MUTED)
            self.cell(0, 5, f"Reliability: {item['rel']}%", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            
            # Description
            self.set_x(20)
            self.set_font(Config.FONT_BODY, '', 9)
            self.set_text_color(50, 50, 50)
            self.multi_cell(0, 5, item['desc'][:250] + "...")
            
            self.ln(5)

    def page_timeline(self, data):
        """Render Page 3+: Operational Timeline."""
        self.add_page()
        
        self.set_font(Config.FONT_HEAD, 'B', 14)
        self.set_text_color(*Config.C_BG_DARK)
        self.cell(0, 10, "/// OPERATIONAL TIMELINE", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*Config.C_BG_DARK)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(5)
        
        # Table Header
        self.set_fill_color(240, 240, 240)
        self.rect(15, self.get_y(), 180, 8, 'F')
        self.set_font(Config.FONT_MONO, 'B', 8)
        self.set_text_color(50, 50, 50)
        self.cell(30, 8, "TIME", 0, 0)
        self.cell(15, 8, "TIE", 0, 0) # Score
        self.cell(25, 8, "VECTORS", 0, 0) # K/T/E
        self.cell(110, 8, "EVENT / SECTOR", 0, 1)
        
        # Rows
        for i, item in enumerate(data['timeline']):
            if self.get_y() > 270:
                self.add_page()
                # Redraw Header
                self.set_fill_color(240, 240, 240)
                self.rect(15, self.get_y(), 180, 8, 'F')
                self.set_font(Config.FONT_MONO, 'B', 8)
                self.cell(30, 8, "TIME", 0, 0)
                self.cell(15, 8, "TIE", 0, 0)
                self.cell(25, 8, "VECTORS", 0, 0)
                self.cell(110, 8, "EVENT / SECTOR", 0, 1)

            # Date
            self.set_font(Config.FONT_MONO, '', 7)
            self.set_text_color(*Config.C_TEXT_MUTED)
            self.cell(30, 6, item['date'][5:16], 0, 0)
            
            # TIE Badge
            tie = int(item['tie'])
            if tie >= 70: col = Config.C_CRIT
            elif tie >= 40: col = Config.C_HIGH
            elif tie >= 20: col = Config.C_MED
            else: col = Config.C_LOW
            
            self.set_font(Config.FONT_MONO, 'B', 8)
            self.set_text_color(*col)
            self.cell(15, 6, f"{tie}", 0, 0)
            
            # Vectors
            self.set_font(Config.FONT_MONO, '', 7)
            self.set_text_color(*Config.C_TEXT_MUTED)
            self.cell(25, 6, f"K{int(item['k'])}/T{int(item['t'])}/E{int(item['e'])}", 0, 0)
            
            # Title & Sector
            self.set_font(Config.FONT_HEAD, '', 8)
            self.set_text_color(20, 20, 20)
            title = item['title'][:55]
            sector = item['sector'].replace('_', ' ')
            self.cell(110, 6, f"{title} [{sector}]", 0, 1)
            
            # Thin separator
            self.set_draw_color(240, 240, 240)
            self.line(15, self.get_y(), 195, self.get_y())

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("="*60)
    print("IMPACT ATLAS SITREP GENERATOR v6.0")
    print("="*60)
    
    # 1. Initialize Engine
    engine = IntelEngine(Config.DB_PATH)
    print("[1/4] Fetching intelligence data...")
    data = engine.fetch_data(days=30) # Fetch last 30 days for better context
    
    if not data or not data['events']:
        print("ERROR: No data found.")
        return
        
    print(f"      Found {data['stats']['total']} events ({data['stats']['critical_count']} critical)")
    
    # 2. Generate Charts
    print("[2/4] Generating tactical analytics...")
    charts = {}
    charts['threat'] = ChartGenerator.generate_threat_gauge(data['stats'])
    charts['tie_dist'] = ChartGenerator.generate_tie_distribution(data['stats'])
    charts['sector'] = ChartGenerator.generate_sector_pie(data['stats'])
    charts['classification'] = ChartGenerator.generate_classification_bar(data['stats'])
    charts['timeline'] = ChartGenerator.generate_timeline(data['events'])
    
    # 3. Render PDF
    print("[3/4] Rendering briefing document...")
    pdf = BriefingDoc()
    
    # Page 1: Cover
    pdf.page_cover(data, charts)
    
    # Page 2: Flash Traffic
    if data['critical']:
        pdf.page_flash(data)
    
    # Page 3: Timeline
    pdf.page_timeline(data)
    
    # 4. Save
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Config.REPORTS_DIR / f"sitrep_{timestamp}.pdf"
    
    # Ensure dir exists
    Config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        pdf.output(str(output_path))
        print(f"[4/4] SUCCESS: Report saved to {output_path}")
    except PermissionError:
        print("ERROR: Permission denied. Close the PDF and try again.")
    except Exception as e:
        print(f"ERROR: PDF Generation failed: {e}")
        
    # Cleanup Charts
    for path in charts.values():
        if path and os.path.exists(path):
            try: os.remove(path)
            except: pass

if __name__ == "__main__":
    main()
