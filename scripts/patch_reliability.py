import sqlite3
import json
import os
import re
import ast

# =============================================================================
# 🛠️ CONFIGURAZIONE
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')


def parse_list_field(field_value):
    """Helper per leggere le liste di URL dal DB"""
    if not field_value:
        return []
    try:
        if isinstance(field_value, list):
            return field_value
        text = str(field_value).strip()
        if text.startswith('[') and text.endswith(']'):
            return ast.literal_eval(text)
        if ' ||| ' in text:
            return text.split(' ||| ')
        return [text]
    except:
        return []


def calculate_retroactive_score(row, current_json):
    """
    Ricalcola lo score con Base 40 (Standard Telegram).
    """
    text_dossier = (row['full_text_dossier'] or "").lower()
    description = (current_json.get('editorial', {}).get(
        'description_it', "") or "").lower()
    full_content = f"{text_dossier} {description}"

    urls = parse_list_field(row['urls_list'])

    # --- ALGORITMO DI SCORING V2 (Base 40) ---
    score = 40  # <--- BASELINE ALZATA A 40 (Non Verificato Standard)
    reasons = []

    # 1. CORROBORATION
    is_merged = '[merged' in text_dossier or row['ai_analysis_status'] == 'MERGED'

    if is_merged:
        score += 20
        reasons.append("Cluster Fusione (+20)")
    elif len(urls) > 1:
        score += 10
        reasons.append("Fonti Multiple (+10)")

    if len(urls) >= 3:
        score += 10
        reasons.append("Alta Densità Fonti (+10)")

    # 2. VISUAL EVIDENCE
    # Parole chiave che indicano IMINT (Intelligence Immagini)
    visual_keywords = ['video', 'footage', 'filmato', 'ripreso', 'geolocat',
                       'foto', 'photo', 'image', 'drone view', 'visual confirmation']
    if any(k in full_content for k in visual_keywords):
        score += 20
        reasons.append("Evidenza Visiva (+20)")

    # 3. SPECIFICITY
    coord_pattern = r'\d{2}\.\d{3,}'
    unit_pattern = r'\d+(?:th|nd|rd|st|a|°)?\s+(?:brigad|regiment|battalion|brigata|reggimento)'

    if re.search(coord_pattern, full_content) or re.search(unit_pattern, full_content):
        score += 5  # Bonus ridotto leggermente per non inflazionare
        reasons.append("Alta Specificità (+5)")

    # 4. CROSS-VERIFICATION
    pro_ru = ['rybar', 'two majors', 'wargonzo']
    pro_ua = ['deepstate', 'sternenko', 'butusov']

    has_ru = any(k in full_content for k in pro_ru)
    has_ua = any(k in full_content for k in pro_ua)

    if has_ru and has_ua:
        score += 30
        reasons.append("Cross-Verifica RU/UA (+30)")
    elif 'isw' in full_content or 'mod uk' in full_content:
        score += 20
        reasons.append("Fonte Istituzionale (+20)")

    # 5. PENALTIES (Solo se esplicito)
    # Abbassa a 30 solo se c'è puzza di rumor
    rumor_keywords = ['non confermat', 'unconfirmed',
                      'voce', 'rumor', 'sembrerebbe', 'reportedly']
    if any(k in full_content for k in rumor_keywords):
        score -= 10
        reasons.append("Rumor/Non Confermato (-10)")

    # CAP (Min 30, Max 95)
    # Assicuriamo che non scenda sotto 30 a meno di penalità gravi
    final_score = max(30, min(95, score))

    # Se non ci sono bonus o malus, resta 40 pulito
    if not reasons:
        reasons.append("Standard Telegram (Base)")

    return final_score, ", ".join(reasons)


def main():
    print("🚑 AVVIO PATCH RETROATTIVA RELIABILITY (BASE 40)...")
    print(f"   📂 Database: {DB_PATH}")

    if not os.path.exists(DB_PATH):
        print("❌ DB non trovato.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT event_id, ai_report_json, full_text_dossier, urls_list, ai_analysis_status
        FROM unique_events 
        WHERE ai_report_json IS NOT NULL 
        AND ai_analysis_status IN ('COMPLETED', 'VERIFIED')
    """)
    rows = cursor.fetchall()

    print(f"   🔍 Ricalcolo su {len(rows)} eventi...")
    updates_count = 0

    for row in rows:
        try:
            current_json = json.loads(row['ai_report_json'])

            new_score, reason = calculate_retroactive_score(row, current_json)

            # Aggiorniamo SEMPRE per essere sicuri di sovrascrivere i 30 sbagliati di prima
            if 'scores' not in current_json:
                current_json['scores'] = {}

            current_json['scores']['reliability'] = new_score
            current_json['reliability_reasoning'] = f"[AUTO-PATCH V2] {reason}"

            cursor.execute("""
                UPDATE unique_events 
                SET ai_report_json = ? 
                WHERE event_id = ?
            """, (json.dumps(current_json), row['event_id']))

            # Stampa solo se diverso da 40 per non intasare il log
            if new_score != 40:
                print(
                    f"      ✅ {new_score}% : {row['event_id'][:6]}... ({reason})")

            updates_count += 1

        except Exception as e:
            continue

    conn.commit()
    conn.close()
    print(f"\n✅ FINITO. Ricalibrati {updates_count} eventi.")


if __name__ == "__main__":
    main()
