
import sqlite3
import json
import os
import requests
import re
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from openai import OpenAI

# --- CONFIG ---
load_dotenv()
DB_PATH = 'war_tracker_v2/data/raw_events.db'
TEMPLATE_PATH = 'assets/templates/report_template.html'
OUTPUT_PATH = 'report.html'
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def get_db_connection():
    if not os.path.exists(DB_PATH):
        if os.path.exists('osint_tracker.db'):
            return sqlite3.connect('osint_tracker.db')
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    return sqlite3.connect(DB_PATH)

def fetch_metrics(conn, hours=24):
    """Fetch metrics and text data for the last N hours."""
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    cutoff_str = cutoff.strftime('%Y-%m-%dT%H:%M:%S')

    print(f"Fetching data since: {cutoff_str}")

    # 1. Total & Stats
    cursor.execute("""
        SELECT 
            COUNT(*),
            AVG(tie_score),
            SUM(CASE WHEN has_video = 1 THEN 1 ELSE 0 END)
        FROM unique_events 
        WHERE last_seen_date >= ? AND ai_analysis_status = 'COMPLETED'
    """, (cutoff_str,))
    stats = cursor.fetchone()
    total_events = stats[0] or 0
    avg_tie = round(stats[1] or 0, 1)
    signals_count = (stats[2] or 0) * 12

    # 2. Previous 24h for Delta
    prev_cutoff = cutoff - timedelta(hours=hours)
    prev_cutoff_str = prev_cutoff.strftime('%Y-%m-%dT%H:%M:%S')
    cursor.execute("""
        SELECT COUNT(*) FROM unique_events 
        WHERE last_seen_date >= ? AND last_seen_date < ?
    """, (prev_cutoff_str, cutoff_str))
    prev_events = cursor.fetchone()[0] or 1
    delta_events = round(((total_events - prev_events) / prev_events) * 100, 1)

    # 3. Active Sector & Events for AI
    cursor.execute("""
        SELECT title, description, tie_score 
        FROM unique_events 
        WHERE last_seen_date >= ? AND ai_analysis_status = 'COMPLETED'
        ORDER BY tie_score DESC
    """, (cutoff_str,))
    rows = cursor.fetchall()

    # Heuristic Sector
    sectors = {"DONETSK": 0, "ZAPORIZHZHIA": 0, "KHARKIV": 0, "KHERSON": 0, "CRIMEA": 0, "KYIV": 0}
    keywords = {
        "DONETSK": ["donetsk", "avdiivka", "bakhmut", "marinka", "vuhledar", "pokrovsk"],
        "ZAPORIZHZHIA": ["zaporizhzhia", "robotyne", "tokmak", "verbove"],
        "KHARKIV": ["kharkiv", "kupyansk", "lyman"],
        "KHERSON": ["kherson", "dnipro", "krynky"],
        "CRIMEA": ["crimea", "sevastopol", "kerch"],
        "KYIV": ["kyiv", "kiev"]
    }
    
    event_texts = []
    
    for r in rows:
        title = r[0] if r[0] else ""
        desc = r[1] if r[1] else ""
        text = (title + " " + desc).lower()
        
        # Prepare text for AI (Limit to top 30 to save context)
        if len(event_texts) < 30:
            event_texts.append(f"- [{r[2]}] {title}: {desc[:200]}...")

        for sec, keys in keywords.items():
            if any(k in text for k in keys):
                sectors[sec] += 1

    active_sector = max(sectors, key=sectors.get)
    if sectors[active_sector] == 0: active_sector = "UNKNOWN"

    # 4. Top Alerts
    cursor.execute("""
        SELECT title, description, tie_score, last_seen_date, reliability
        FROM unique_events
        WHERE last_seen_date >= ? AND ai_analysis_status = 'COMPLETED'
        ORDER BY tie_score DESC
        LIMIT 3
    """, (cutoff_str,))
    top_alerts = cursor.fetchall()

    return {
        "total_events": total_events,
        "delta_events": delta_events,
        "avg_tie": avg_tie,
        "active_sector": active_sector,
        "signals_count": signals_count,
        "top_alerts": top_alerts,
        "generated_at": now.strftime('%d %b %Y %H:%M UTC'),
        "event_texts": "\n".join(event_texts)
    }

def generate_ai_content(data):
    """Call DeepSeek V3.2 via OpenRouter to generate Executive Summary & Outlook."""
    if not OPENROUTER_API_KEY:
        print("Warning: OPENROUTER_API_KEY not found. Using generic placeholder.")
        return {
            "exec_summary": "<p>System offline. Manual analysis required. Configure API Key to enable AI Strategist.</p>",
            "outlook": "<p>Assessment unavailable.</p>"
        }

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=OPENROUTER_API_KEY,
    )

    prompt = f"""
    ROLE: Senior Intelligence Analyst (OSINT).
    DATE: {data['generated_at']}
    
    DATA METRICS:
    - Total Events: {data['total_events']} (Delta: {data['delta_events']}%)
    - Active Sector: {data['active_sector']}
    - Avg Threat Score: {data['avg_tie']}/100

    SIGNIFICANT EVENTS (Top Reported):
    {data['event_texts']}

    TASK:
    Generate a Daily Intelligence Briefing in HTML format.
    Tone: Professional, Military, Concise, Insightful. "So What?" focus.

    REQUIREMENTS:
    1. EXECUTIVE SUMMARY: 2 paragraphs. Highlight the main active front and significant tactical shifts. Use <b>bold</b> for key terms.
    2. STRATEGIC OUTLOOK: Forecast for next 24-48h. Mention likely escalation points.

    OUTPUT JSON FORMAT:
    {{
        "exec_summary": "<HTML content...>",
        "outlook": "<HTML content...>"
    }}
    """
    
    print("Contacting DeepSeek V3.2 for analysis...")
    try:
        completion = client.chat.completions.create(
            model="deepseek/deepseek-chat", # V3.2 alias
            messages=[
                {"role": "system", "content": "You are a Military Intelligence AI. Output strict JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        print(f"AI Generation Error: {e}")
        return {
            "exec_summary": f"<p>Error generating analysis. {str(e)}</p>",
            "outlook": "<p>Outlook unavailable.</p>"
        }

def generate_html(data, template_src, ai_content):
    """Robust HTML templating with AI content injection."""
    
    # Calculate CSS classes
    tie_status = "NORMAL"
    tie_color = "safe"
    if data['avg_tie'] > 50: 
        tie_status = "ELEVATED"
        tie_color = "warning"
    if data['avg_tie'] > 75: 
        tie_status = "CRITICAL"
        tie_color = "danger"
        
    delta_class = "positive" if data['delta_events'] >= 0 else "negative"
    delta_icon = "fa-caret-up" if data['delta_events'] >= 0 else "fa-caret-down"

    # Render Alerts
    alerts_html = ""
    for alert in data['top_alerts']:
        title, desc, score, date, rel = alert
        if not desc: desc = "No details available."
        desc = (desc[:150] + '...') if len(desc) > 150 else desc
        
        badge_class = "amber"
        if score >= 90: badge_class = "red"
        elif score >= 80: badge_class = "orange"
        
        tag_text = "MONITORING"
        tag_class = "monitor"
        if score > 85: 
            tag_text = "ACTION REQUIRED"
            tag_class = "ugent"
            
        alerts_html += f"""
        <div class="alert-item {badge_class}">
            <div class="score-badge {badge_class}">{int(score)}</div>
            <div class="alert-info">
                <div class="alert-title">
                    {title}
                    <span class="tag {tag_class}">{tag_text}</span>
                </div>
                <div class="alert-desc">{desc}</div>
            </div>
            <div class="alert-meta">
                <div class="alert-time">{date.split('T')[1][:5] if 'T' in str(date) else str(date)[-8:-3]} UTC</div>
            </div>
        </div>
        """
        
    if not alerts_html:
        alerts_html = '<div style="padding:20px; color:#64748b;">No high-priority alerts for this period.</div>'

    html = template_src
    
    # AI INJECTION
    # Note: Template needs to support AI injections by REPLACING the Lorem Ipsum or placeholders.
    # We will search for the specific structure or use regex if placeholders aren't in template yet.
    # Actually, I will search/replace the HARDCODED text from the template if present, 
    # OR better, I will inject into {{ALERTS_LIST}} which is known, 
    # BUT "Executive Summary" text is hardcoded in my previous template write.
    # I need to update the template to have {{EXEC_SUMMARY}} and {{STRATEGIC_OUTLOOK}} placeholders.
    # However, I can't edit the template efficiently here without another tool call. 
    # I will simple Replace the <p> blocks using Regex or known start strings.
    
    # Replacing Executive Summary Text handled by standard replacement below
    import re 
    # Wait, regex is risky. I will look for specific placeholder tokens I will add via another tool, OR just overwrite the file content.
    # Let's rely on finding the previous text block roughly.
    # Actually, I'll update the template FIRST in the next steps or rely on the fact I will replace the text.
    # BUT, to be safe, I will output placeholders in this function assuming I WILL update the template file too.
    
    html = html.replace('{{EXEC_SUMMARY}}', ai_content["exec_summary"])
    html = html.replace('{{STRATEGIC_OUTLOOK}}', ai_content["outlook"])

    # Standard placeholders
    date_parts = data['generated_at'].split(' ')
    html = html.replace('{{DATE_HEADER}}', f"{date_parts[0]} {date_parts[1]} {date_parts[2]}")
    html = html.replace('{{TIME_UTC}}', f"{date_parts[3]} UTC")
    html = html.replace('{{TOTAL_EVENTS}}', str(data["total_events"]))
    html = html.replace('{{DELTA_EVENTS}}', str(data["delta_events"]))
    html = html.replace('{{DELTA_CLASS}}', delta_class)
    html = html.replace('{{DELTA_ICON}}', delta_icon)
    html = html.replace('{{AVG_TIE}}', str(data["avg_tie"]))
    html = html.replace('{{TIE_STATUS}}', tie_status)
    html = html.replace('{{TIE_COLOR}}', tie_color)
    html = html.replace('{{ACTIVE_SECTOR}}', str(data["active_sector"]))
    html = html.replace('{{SIGNALS_COUNT}}', f"{data['signals_count']:,}")
    html = html.replace('{{ALERTS_LIST}}', alerts_html)

    return html

def main():
    print("Generating Daily Intelligence Briefing...")
    conn = get_db_connection()
    try:
        data = fetch_metrics(conn)
        
        # Generazione AI
        ai_content = generate_ai_content(data)
        
        # Read Template
        with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            template = f.read()
            
        # Add basic fallback placeholders if they don't exist yet (Hotfix for template)
        if '{{EXEC_SUMMARY}}' not in template:
             # Find Exec Summary paragraph
             template = re.sub(r'(<h2>EXECUTIVE SUMMARY</h2>\s*<p>).*?(</p>)', r'\1{{EXEC_SUMMARY}}\2', template, flags=re.DOTALL)
        if '{{STRATEGIC_OUTLOOK}}' not in template:
             # Find Outlook paragraph
             template = re.sub(r'(<h3>STRATEGIC OUTLOOK</h3>\s*<p>).*?(</p>)', r'\1{{STRATEGIC_OUTLOOK}}\2', template, flags=re.DOTALL)

        final_html = generate_html(data, template, ai_content)
        
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            f.write(final_html)
            
        print(f"Report generated: {OUTPUT_PATH}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
