"""
Impact Atlas - Military Intelligence SITREP Generator (v7.0)
------------------------------------------------------------
NATO-grade intelligence briefing with actionable analysis.

4-page structure:
  1. SITUATION OVERVIEW - KPIs, Operational Tempo, TIE Distribution
  2. STRATEGIC ASSESSMENT - Sectors, Deep Strikes, Target Types, Bias
  3. FLASH TRAFFIC - Critical events with strategic value assessments
  4. SIGNIFICANT EVENTS LOG - Compact table of high-impact events

Author: Impact Atlas Team
Date: February 2026
"""

import sqlite3
import datetime
import os
import json
import logging
import tempfile
from pathlib import Path
from collections import Counter

from fpdf import FPDF
from fpdf.enums import XPos, YPos

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

# =============================================================================
# CONFIG
# =============================================================================

class C:
    """Slate & Amber palette + layout constants."""
    DIR      = Path(__file__).parent.absolute()
    DB       = DIR.parent / 'data' / 'raw_events.db'
    FONTS    = DIR / 'fonts'
    OUT      = DIR / 'reports'

    # RGB
    BG       = (15, 23, 42)
    CARD     = (30, 41, 59)
    TEXT     = (241, 245, 249)
    TEXT2    = (148, 163, 184)
    MUTED    = (100, 116, 139)
    AMBER    = (245, 158, 11)
    RED      = (239, 68, 68)
    ORANGE   = (249, 115, 22)
    YELLOW   = (234, 179, 8)
    SLATE5   = (100, 116, 139)
    GREEN    = (34, 197, 94)
    WHITE    = (255, 255, 255)

    # Hex (matplotlib)
    H_BG     = '#0f172a'
    H_AMBER  = '#f59e0b'
    H_TEXT   = '#f1f5f9'
    H_GRID   = '#334155'

    # Fonts
    F_H = 'Roboto'
    F_M = 'JBM'

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger('SITREP')

# =============================================================================
# SECTOR CLASSIFIER
# =============================================================================

def classify_sector(lat, lon, target_type):
    tt = (target_type or '').lower()
    if any(k in tt for k in ['power', 'grid', 'dam', 'plant', 'energy', 'refinery', 'oil']):
        return 'ENERGY'
    if lat and lon:
        try:
            la, lo = float(lat), float(lon)
            if la > 50.0 and lo > 36.0: return 'DEEP STRIKE'
            if lo <= 36.0 and la < 48.0: return 'SOUTHERN'
            if lo > 36.0 and la < 50.0: return 'EASTERN'
        except: pass
    return 'EASTERN'

# =============================================================================
# INTEL ENGINE
# =============================================================================

class IntelEngine:
    def __init__(self, db):
        self.db = db

    def fetch(self, days=7):
        if not self.db.exists():
            log.error(f"DB missing: {self.db}")
            return None

        conn = sqlite3.connect(str(self.db))
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")

        try:
            rows = conn.execute(f"""
                SELECT event_id, last_seen_date, title, description,
                       tie_score, kinetic_score, target_score, effect_score,
                       reliability, bias_score, ai_report_json, ai_summary
                FROM unique_events
                WHERE ai_analysis_status = 'COMPLETED'
                  AND last_seen_date >= '{cutoff}'
                ORDER BY tie_score DESC, last_seen_date DESC
            """).fetchall()
        except sqlite3.Error as e:
            log.error(f"SQL: {e}")
            conn.close()
            return None

        events = []
        for r in rows:
            ai = {}
            if r['ai_report_json']:
                try: ai = json.loads(r['ai_report_json'])
                except: pass

            strat = ai.get('strategy', {})
            if not isinstance(strat, dict): strat = {}
            tactics = ai.get('tactics', {})
            if not isinstance(tactics, dict): tactics = {}
            titan = ai.get('titan_metrics', tactics.get('titan_assessment', {}))
            if not isinstance(titan, dict): titan = {}
            scores = ai.get('scores', {})
            if not isinstance(scores, dict): scores = {}

            # Geo
            lat, lon = None, None
            try:
                geo = tactics.get('geo_location', strat.get('geo_location', {}))
                if isinstance(geo, dict):
                    exp = geo.get('explicit', {})
                    if isinstance(exp, dict):
                        lat, lon = exp.get('lat'), exp.get('lon')
            except: pass

            # Target type
            tgt_cat = titan.get('target_type_category', '')
            if isinstance(tgt_cat, list): tgt_cat = ', '.join(tgt_cat)

            # Actors
            actors = tactics.get('actors', strat.get('actors', {}))
            aggressor = ''
            if isinstance(actors, dict):
                agg = actors.get('aggressor', {})
                if isinstance(agg, dict): aggressor = agg.get('side', '')

            # Units
            units_raw = tactics.get('military_units_detected', strat.get('military_units_detected', []))
            units = []
            if isinstance(units_raw, list):
                for u in units_raw[:5]:
                    if isinstance(u, dict):
                        units.append(f"{u.get('unit_name','?')} ({u.get('faction','?')})")

            # Event category
            ea = tactics.get('event_analysis', strat.get('event_analysis', {}))
            if not isinstance(ea, dict): ea = {}
            evt_cat = strat.get('event_category', ea.get('event_category', ''))

            events.append({
                'id':       r['event_id'],
                'date':     str(r['last_seen_date'] or '')[:16],
                'title':    r['title'] or 'Unknown Event',
                'desc':     r['description'] or r['ai_summary'] or '',
                'tie':      float(r['tie_score'] or 0),
                'k':        float(r['kinetic_score'] or 0),
                't':        float(r['target_score'] or 0),
                'e':        float(r['effect_score'] or 0),
                'rel':      int(r['reliability'] or 50),
                'sector':   classify_sector(lat, lon, tgt_cat),
                'tgt_cat':  tgt_cat or 'UNKNOWN',
                'deep':     bool(titan.get('is_deep_strike', False)),
                'aggressor': aggressor,
                'units':    units,
                'evt_cat':  evt_cat or 'UNKNOWN',
                'strat_val': strat.get('strategic_value_assessment', ''),
                'signal':   strat.get('implicit_signal', ''),
                'bias':     scores.get('dominant_bias', ''),
            })

        conn.close()

        if not events: return None

        total = len(events)
        crit = [e for e in events if e['tie'] >= 70]
        if not crit: crit = sorted(events, key=lambda x: x['tie'], reverse=True)[:3]

        high_events = [e for e in events if e['tie'] >= 40]
        if len(high_events) < 5:
            high_events = sorted(events, key=lambda x: x['tie'], reverse=True)[:20]
        high_events.sort(key=lambda x: x['date'], reverse=True)

        return {
            'events':   events,
            'critical': crit[:5],
            'timeline': high_events[:30],
            'stats': {
                'total':      total,
                'crit_count': len([e for e in events if e['tie'] >= 70]),
                'deep_count': len([e for e in events if e['deep']]),
                'avg_tie':    sum(e['tie'] for e in events) / total,
                'avg_rel':    sum(e['rel'] for e in events) / total,
                'tie_dist': {
                    'CRITICAL': len([e for e in events if e['tie'] >= 70]),
                    'HIGH':     len([e for e in events if 40 <= e['tie'] < 70]),
                    'MEDIUM':   len([e for e in events if 20 <= e['tie'] < 40]),
                    'LOW':      len([e for e in events if e['tie'] < 20]),
                },
                'sectors':   Counter(e['sector'] for e in events),
                'tgt_types': Counter(e['tgt_cat'] for e in events),
                'evt_cats':  Counter(e['evt_cat'] for e in events),
                'bias':      Counter(e['bias'] for e in events if e['bias']),
                'aggressors': Counter(e['aggressor'] for e in events if e['aggressor']),
            }
        }

# =============================================================================
# CHARTS (compact, inline)
# =============================================================================

class Charts:
    @staticmethod
    def _style():
        plt.style.use('dark_background')
        for k, v in {
            'axes.facecolor': C.H_BG, 'figure.facecolor': C.H_BG,
            'text.color': C.H_TEXT, 'axes.labelcolor': C.H_TEXT,
            'xtick.color': C.H_TEXT, 'ytick.color': C.H_TEXT,
            'axes.edgecolor': C.H_GRID, 'grid.color': C.H_GRID,
        }.items():
            plt.rcParams[k] = v

    @staticmethod
    def tempo(events):
        """7-day operational tempo bar chart. Compact."""
        if not HAS_MPL: return None
        Charts._style()
        fig, ax = plt.subplots(figsize=(4.5, 1.5))

        dates = [e['date'][:10] for e in events if len(e['date']) >= 10]
        counts = Counter(dates)
        sd = sorted(counts.keys())[-7:] if len(counts) > 7 else sorted(counts.keys())
        vals = [counts.get(d, 0) for d in sd]

        if not sd: plt.close(fig); return None

        ax.bar(range(len(sd)), vals, color=C.H_AMBER, alpha=0.85)
        ax.set_xticks(range(len(sd)))
        ax.set_xticklabels([d[5:] for d in sd], fontsize=6, rotation=30)
        ax.set_ylabel('Events', fontsize=7)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='y', labelsize=6)

        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp

    @staticmethod
    def tie_dist(stats):
        """TIE distribution horizontal bar. Compact."""
        if not HAS_MPL: return None
        Charts._style()
        fig, ax = plt.subplots(figsize=(4.5, 1.5))

        d = stats['tie_dist']
        labels = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
        vals = [d[l] for l in labels]
        colors = ['#ef4444', '#f97316', '#eab308', '#64748b']

        ax.barh(range(4), vals, color=colors, height=0.55)
        ax.set_yticks(range(4))
        ax.set_yticklabels(labels, fontsize=7)
        ax.invert_yaxis()
        for i, v in enumerate(vals):
            ax.text(v + 0.5, i, str(v), va='center', fontsize=7, color=C.H_TEXT)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='x', labelsize=6)

        tmp = tempfile.mktemp(suffix='.png')
        plt.savefig(tmp, dpi=150, bbox_inches='tight', transparent=True)
        plt.close(fig)
        return tmp

# =============================================================================
# PDF RENDERER
# =============================================================================

class Doc(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(True, margin=18)
        self.set_margins(15, 15, 15)
        for fam, sty, fn in [
            (C.F_H, '', 'Roboto-Regular.ttf'), (C.F_H, 'B', 'Roboto-Bold.ttf'),
            (C.F_M, '', 'JetBrainsMono-Regular.ttf'), (C.F_M, 'B', 'JetBrainsMono-Bold.ttf'),
        ]:
            p = C.FONTS / fn
            if p.exists():
                try: self.add_font(fam, sty, str(p))
                except: pass

    def header(self):
        if self.page_no() == 1: return
        self.set_fill_color(*C.BG)
        self.rect(0, 0, 210, 12, 'F')
        self.set_fill_color(*C.AMBER)
        self.rect(0, 12, 210, 0.6, 'F')
        self.set_y(3)
        self.set_font(C.F_H, 'B', 8)
        self.set_text_color(*C.AMBER)
        self.cell(90, 5, "IMPACT ATLAS // SITREP")
        self.set_font(C.F_M, '', 7)
        self.set_text_color(*C.TEXT2)
        self.cell(90, 5, datetime.datetime.now().strftime("%Y-%m-%d %H:%M Z"), align='R')
        self.set_y(18)

    def footer(self):
        self.set_y(-12)
        self.set_font(C.F_H, '', 7)
        self.set_text_color(*C.MUTED)
        self.cell(0, 4, f"Page {self.page_no()}/{{nb}}  |  ACADEMIC USE ONLY. IMPACT ATLAS.", align='C')

    # ── Helpers ──────────────────────────────────────────────────

    def section_title(self, text, color=C.BG):
        self.set_font(C.F_H, 'B', 12)
        self.set_text_color(*color)
        self.cell(0, 8, f"/// {text}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*C.AMBER)
        self.line(15, self.get_y(), 195, self.get_y())
        self.ln(4)

    def kpi_card(self, x, y, label, value, color=C.AMBER):
        """Draw a small KPI card at absolute position."""
        w, h = 40, 22
        self.set_fill_color(*C.CARD)
        self.rect(x, y, w, h, 'F')
        # Amber top stripe
        self.set_fill_color(*color)
        self.rect(x, y, w, 2, 'F')
        # Value
        self.set_xy(x, y + 4)
        self.set_font(C.F_M, 'B', 14)
        self.set_text_color(*C.TEXT)
        self.cell(w, 8, str(value), align='C', new_x=XPos.LEFT, new_y=YPos.NEXT)
        # Label
        self.set_x(x)
        self.set_font(C.F_H, '', 7)
        self.set_text_color(*C.TEXT2)
        self.cell(w, 5, label, align='C')

    # ── PAGE 1: SITUATION OVERVIEW ───────────────────────────────

    def page_overview(self, data, charts):
        self.add_page()
        s = data['stats']

        # Hero Banner
        self.set_fill_color(*C.BG)
        self.rect(0, 0, 210, 50, 'F')
        self.set_fill_color(*C.AMBER)
        self.rect(0, 48, 210, 2, 'F')

        self.set_y(14)
        self.set_font(C.F_H, 'B', 22)
        self.set_text_color(*C.AMBER)
        self.cell(0, 10, "IMPACT ATLAS", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font(C.F_H, '', 11)
        self.set_text_color(*C.TEXT)
        self.cell(0, 7, "SITUATION REPORT  //  DAILY INTELLIGENCE BRIEFING", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font(C.F_M, '', 7)
        self.set_text_color(*C.TEXT2)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M Zulu")
        self.cell(0, 5, f"GENERATED {dt}  |  EYES ONLY", align='C')

        # KPI Cards (2x2)
        y0 = 56
        gap = 44
        x0 = 15
        self.kpi_card(x0,          y0, "TOTAL EVENTS",    s['total'])
        self.kpi_card(x0 + gap,    y0, "CRITICAL",        s['crit_count'], C.RED)
        self.kpi_card(x0 + gap*2,  y0, "DEEP STRIKES",   s['deep_count'], C.ORANGE)
        self.kpi_card(x0 + gap*3,  y0, "AVG RELIABILITY", f"{s['avg_rel']:.0f}%", C.GREEN)

        # Charts Row
        y_charts = 84
        if charts.get('tempo'):
            self.set_xy(15, y_charts)
            self.set_font(C.F_H, 'B', 8)
            self.set_text_color(*C.BG)
            self.cell(85, 5, "OPERATIONAL TEMPO (7 DAYS)")
            self.image(charts['tempo'], x=15, y=y_charts + 5, w=85)

        if charts.get('tie'):
            self.set_xy(110, y_charts)
            self.set_font(C.F_H, 'B', 8)
            self.set_text_color(*C.BG)
            self.cell(85, 5, "THREAT LEVEL DISTRIBUTION")
            self.image(charts['tie'], x=110, y=y_charts + 5, w=85)

        # ----- EXECUTIVE SUMMARY (data-driven) -----
        self.set_y(120)
        self.section_title("EXECUTIVE SUMMARY")

        self.set_font(C.F_H, '', 10)
        self.set_text_color(40, 40, 40)

        # Build dynamic summary from actual data
        lines = []
        lines.append(f"This SITREP covers {s['total']} intelligence events detected in the reporting period.")

        if s['crit_count']:
            lines.append(f"{s['crit_count']} events assessed as CRITICAL (TIE >= 70), requiring immediate attention.")

        if s['deep_count']:
            lines.append(f"{s['deep_count']} deep strike operations detected against targets inside Russian territory.")

        # Top sectors
        top_sectors = s['sectors'].most_common(3)
        if top_sectors:
            sec_str = ', '.join([f"{k} ({v})" for k, v in top_sectors])
            lines.append(f"Primary activity by sector: {sec_str}.")

        # Top target types
        top_tgt = [(k,v) for k,v in s['tgt_types'].most_common(5) if k != 'UNKNOWN']
        if top_tgt:
            tgt_str = ', '.join([f"{k.replace('_',' ')} ({v})" for k, v in top_tgt[:3]])
            lines.append(f"Most targeted categories: {tgt_str}.")

        # Bias
        top_bias = s['bias'].most_common(2)
        if top_bias:
            bias_str = ', '.join([f"{k} ({v})" for k, v in top_bias])
            lines.append(f"Source bias profile: {bias_str}.")

        # Avg TIE
        lines.append(f"Average Threat Index: {s['avg_tie']:.1f}/100. Average Source Reliability: {s['avg_rel']:.0f}%.")

        self.multi_cell(0, 5.5, '\n'.join(lines))

        # ----- SECTOR BREAKDOWN TABLE -----
        self.ln(4)
        self.section_title("SECTOR ACTIVITY")

        # Table header
        self.set_fill_color(230, 232, 236)
        self.rect(15, self.get_y(), 180, 7, 'F')
        self.set_font(C.F_M, 'B', 7)
        self.set_text_color(50, 50, 50)
        self.cell(45, 7, "SECTOR", new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(25, 7, "EVENTS", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(25, 7, "CRITICAL", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(25, 7, "DEEP STRIKE", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(30, 7, "AVG TIE", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(30, 7, "KEY TARGET", align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Group stats by sector
        sector_data = {}
        for e in data['events']:
            sec = e['sector']
            if sec not in sector_data:
                sector_data[sec] = {'count': 0, 'crit': 0, 'deep': 0, 'tie_sum': 0, 'tgts': []}
            sector_data[sec]['count'] += 1
            if e['tie'] >= 70: sector_data[sec]['crit'] += 1
            if e['deep']: sector_data[sec]['deep'] += 1
            sector_data[sec]['tie_sum'] += e['tie']
            if e['tgt_cat'] != 'UNKNOWN': sector_data[sec]['tgts'].append(e['tgt_cat'])

        for sec in sorted(sector_data, key=lambda x: sector_data[x]['count'], reverse=True):
            sd = sector_data[sec]
            avg = sd['tie_sum'] / sd['count'] if sd['count'] else 0
            top_tgt = Counter(sd['tgts']).most_common(1)
            tgt_label = top_tgt[0][0].replace('_', ' ')[:15] if top_tgt else '-'

            self.set_font(C.F_M, '', 7)
            self.set_text_color(30, 30, 30)
            self.cell(45, 6, sec, new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(25, 6, str(sd['count']), align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

            self.set_text_color(*C.RED if sd['crit'] > 0 else C.MUTED)
            self.cell(25, 6, str(sd['crit']), align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

            self.set_text_color(*C.ORANGE if sd['deep'] > 0 else C.MUTED)
            self.cell(25, 6, str(sd['deep']), align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

            # Color avg TIE
            if avg >= 70: tc = C.RED
            elif avg >= 40: tc = C.ORANGE
            else: tc = C.MUTED
            self.set_text_color(*tc)
            self.cell(30, 6, f"{avg:.1f}", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

            self.set_text_color(60, 60, 60)
            self.cell(30, 6, tgt_label, align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # ── PAGE 2: FLASH TRAFFIC ────────────────────────────────────

    def page_flash(self, data):
        self.add_page()
        self.section_title("FLASH TRAFFIC  -  PRIORITY INTERCEPTS", C.RED)

        for item in data['critical']:
            y0 = self.get_y()
            if y0 > 240:
                self.add_page()
                y0 = self.get_y()

            # Left accent stripe
            self.set_fill_color(*C.RED)
            self.rect(15, y0, 2, 42, 'F')

            # Header line
            self.set_xy(20, y0)
            self.set_font(C.F_M, 'B', 9)
            self.set_text_color(*C.RED)
            self.cell(20, 5, f"TIE {int(item['tie'])}")
            self.set_font(C.F_M, '', 7)
            self.set_text_color(*C.MUTED)
            self.cell(0, 5, f" | {item['date']} | {item['sector']} | {item['evt_cat'].replace('_',' ')}",
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            # Title
            self.set_x(20)
            self.set_font(C.F_H, 'B', 10)
            self.set_text_color(20, 20, 20)
            self.cell(0, 6, item['title'][:80], new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            # Vectors + Reliability
            self.set_x(20)
            self.set_font(C.F_M, 'B', 7)
            self.set_text_color(*C.BG)
            self.cell(25, 4, f"K:{int(item['k'])} T:{int(item['t'])} E:{int(item['e'])}")
            self.set_font(C.F_M, '', 7)
            self.set_text_color(*C.MUTED)
            rel_txt = f"REL:{item['rel']}%"
            if item['bias']:
                rel_txt += f"  BIAS:{item['bias']}"
            self.cell(0, 4, rel_txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            # Strategic Value Assessment - THE KEY INTEL
            strat_val = item.get('strat_val', '')
            if strat_val:
                self.set_x(20)
                self.set_font(C.F_H, '', 8)
                self.set_text_color(40, 50, 60)
                # Truncate to fit
                self.multi_cell(170, 4, strat_val[:300])

            self.ln(4)

    # ── PAGE 3: SIGNIFICANT EVENTS LOG ───────────────────────────

    def page_log(self, data):
        self.add_page()
        self.section_title("SIGNIFICANT EVENTS LOG")

        # Header
        self.set_fill_color(230, 232, 236)
        self.rect(15, self.get_y(), 180, 7, 'F')
        self.set_font(C.F_M, 'B', 7)
        self.set_text_color(50, 50, 50)
        self.cell(22, 7, "DATE", new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(12, 7, "TIE", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(22, 7, "K/T/E", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(30, 7, "CATEGORY", new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.cell(94, 7, "EVENT", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        for item in data['timeline']:
            if self.get_y() > 270:
                self.add_page()
                self.set_fill_color(230, 232, 236)
                self.rect(15, self.get_y(), 180, 7, 'F')
                self.set_font(C.F_M, 'B', 7)
                self.set_text_color(50, 50, 50)
                self.cell(22, 7, "DATE", new_x=XPos.RIGHT, new_y=YPos.TOP)
                self.cell(12, 7, "TIE", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
                self.cell(22, 7, "K/T/E", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
                self.cell(30, 7, "CATEGORY", new_x=XPos.RIGHT, new_y=YPos.TOP)
                self.cell(94, 7, "EVENT", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            # Date
            self.set_font(C.F_M, '', 6)
            self.set_text_color(*C.MUTED)
            self.cell(22, 5, item['date'][5:16] if len(item['date']) > 5 else item['date'], new_x=XPos.RIGHT, new_y=YPos.TOP)

            # TIE (color coded)
            tie = int(item['tie'])
            if tie >= 70: tc = C.RED
            elif tie >= 40: tc = C.ORANGE
            elif tie >= 20: tc = C.YELLOW
            else: tc = C.MUTED
            self.set_font(C.F_M, 'B', 7)
            self.set_text_color(*tc)
            self.cell(12, 5, str(tie), align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

            # K/T/E
            self.set_font(C.F_M, '', 6)
            self.set_text_color(*C.MUTED)
            self.cell(22, 5, f"{int(item['k'])}/{int(item['t'])}/{int(item['e'])}", align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)

            # Category
            cat = item['evt_cat'].replace('_', ' ')[:18]
            self.set_font(C.F_M, '', 6)
            self.set_text_color(60, 60, 60)
            self.cell(30, 5, cat, new_x=XPos.RIGHT, new_y=YPos.TOP)

            # Title
            self.set_font(C.F_H, '', 7)
            self.set_text_color(20, 20, 20)
            self.cell(94, 5, item['title'][:60], new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            # Separator
            self.set_draw_color(230, 232, 236)
            self.line(15, self.get_y(), 195, self.get_y())

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("IMPACT ATLAS SITREP GENERATOR v7.0")
    print("=" * 60)

    engine = IntelEngine(C.DB)
    print("[1/4] Fetching intelligence...")
    data = engine.fetch(days=7)
    if not data:
        print("      No recent data. Extending to 30 days...")
        data = engine.fetch(days=30)
    if not data:
        print("ERROR: No data available.")
        return

    s = data['stats']
    print(f"      {s['total']} events | {s['crit_count']} critical | {s['deep_count']} deep strikes")

    print("[2/4] Generating charts...")
    ch = {}
    ch['tempo'] = Charts.tempo(data['events'])
    ch['tie']   = Charts.tie_dist(s)

    print("[3/4] Rendering PDF...")
    pdf = Doc()
    pdf.page_overview(data, ch)
    pdf.page_flash(data)
    pdf.page_log(data)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = C.OUT / f"sitrep_{ts}.pdf"
    C.OUT.mkdir(parents=True, exist_ok=True)

    try:
        pdf.output(str(out))
        print(f"[4/4] SUCCESS: {out}")
    except Exception as e:
        print(f"ERROR: {e}")

    for p in ch.values():
        if p and os.path.exists(p):
            try: os.remove(p)
            except: pass

if __name__ == "__main__":
    main()
