
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import asyncio
import random
import feedparser
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from geopy.distance import geodesic
import hashlib
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# Configurazioni da environment
from dotenv import load_dotenv
load_dotenv()

TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH', '')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')

# Path per sessione Telegram
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE_PATH = os.path.join(SCRIPT_DIR, 'osint_session')

# Liste canali (le tue originali)
TELEGRAM_CHANNELS = [
    'deepstatemap', 'rybar', 'WarMonitors', 'CinCA_AFU', 'britishmi6',
    'noel_reports', 'MAKS23_NAFO', 'Majakovsk73', 'fighter_bomber',
    'lost_armour', 'GeoConfirmed', 'PlayfraOSINT', 'karymat',
    'DeepStateUA', 'parabellumcommunity', 'DroneBomber', 'bpo_com',
    'ukrliberation', 'strelkovii', 'stanislav_osman', 'BallDontLieDude',
    'dariodangelo', 'officer_33'
]

TWITTER_ACCOUNTS = [
    'DefenceHQ', 'ISW', 'Osinttechnical', 'ChrisO_wiki', 'Tatarigami_UA',
    'clement_molin', 'Mylovanov', '414magyarbirds', 'wartranslated',
    'Maks_NAFO_FELLA', 'Playfra0', 'Rebel44CZ'
]

NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.lucabased.xyz"
]

# ==========================================
# 📊 CONFIGURAZIONE BIAS HARDCODED
# ==========================================

SOURCE_BIAS_DATABASE = {
    # PRO-UKRAINIAN SOURCES
    'deepstatemap': {'bias': 'PRO_UA', 'reliability': 0.85, 'type': 'UA_MILITARY'},
    'DeepStateUA': {'bias': 'PRO_UA', 'reliability': 0.85, 'type': 'UA_MILITARY'},
    'CinCA_AFU': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_MILITARY'},
    'MAKS23_NAFO': {'bias': 'PRO_UA', 'reliability': 0.75, 'type': 'UA_ACTIVIST'},
    'Tatarigami_UA': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_ANALYST'},
    'ukrliberation': {'bias': 'PRO_UA', 'reliability': 0.65, 'type': 'UA_NEWS'},
    'britishmi6': {'bias': 'PRO_UA', 'reliability': 0.70, 'type': 'PARODY'},
    'DroneBomber': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_NEWS'},
    'karymat': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_NEWS'},
    'stanislav_osman': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_MILITARY'},
    'officer_33': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_MILITARY'},


    # PRO-RUSSIAN SOURCES
    'rybar': {'bias': 'PRO_RU', 'reliability': 0.70, 'type': 'RU_MILITARY'},
    'fighter_bomber': {'bias': 'PRO_RU', 'reliability': 0.65, 'type': 'RU_MILITARY'},
    'strelkovii': {'bias': 'PRO_RU', 'reliability': 0.60, 'type': 'RU_ANALYST'},
    'lost_armour': {'bias': 'PRO_RU', 'reliability': 0.60, 'type': 'RU_ANALYST'},


    # NEUTRAL/WESTERN SOURCES
    'GeoConfirmed': {'bias': 'NEUTRAL', 'reliability': 0.90, 'type': 'OSINT'},
    'Osinttechnical': {'bias': 'NEUTRAL', 'reliability': 0.88, 'type': 'OSINT'},
    'WarMonitors': {'bias': 'NEUTRAL', 'reliability': 0.85, 'type': 'AGGREGATOR'},
    'ISW': {'bias': 'WESTERN_MEDIA', 'reliability': 0.85, 'type': 'THINK_TANK'},
    'DefenceHQ': {'bias': 'WESTERN_MEDIA', 'reliability': 0.95, 'type': 'OFFICIAL'},
    'noel_reports': {'bias': 'WESTERN_MEDIA', 'reliability': 0.75, 'type': 'JOURNALIST'},
    'ChrisO_wiki': {'bias': 'WESTERN_MEDIA', 'reliability': 0.80, 'type': 'ANALYST'},
    'BallDontLieDude': {'bias': 'NEUTRAL', 'reliability': 0.90, 'type': 'ANALYST'},
    'Majakovsk73': {'bias': 'ANALYST', 'reliability': 0.85, 'type': 'BLOGGER'},
    'PlayfraOSINT': {'bias': 'NEUTRAL', 'reliability': 0.70, 'type': 'OSINT'},
    'dariodangelo': {'bias': 'NEUTRAL', 'reliability': 0.70, 'type': 'BLOGGER'}
}

# ==========================================
# 📐 DATA STRUCTURES
# ==========================================


@dataclass
class SourceIntelligence:
    """Metadati sulla fonte e il suo bias"""
    source_name: str
    platform: str
    bias: str  # PRO_UA, PRO_RU, NEUTRAL, WESTERN_MEDIA
    reliability: float  # 0.0 - 1.0
    source_type: str  # MILITARY, OSINT, ANALYST, etc.
    original_url: str
    scrape_timestamp: str


@dataclass
class LocationData:
    """Dati geolocalizzazione con precisione"""
    lat: float
    lon: float
    precision: str  # "exact", "city", "region", "country", "unknown"
    location_name: str
    confidence: float  # 0.0 - 1.0


@dataclass
class EventReference:
    """Singola fonte che riporta l'evento"""
    source_intelligence: SourceIntelligence
    original_text: str
    media_urls: List[str]
    verification_status: str  # "verified", "unverified", "disputed"


@dataclass
class AggregatedEvent:
    """Evento aggregato da più fonti"""
    event_id: str
    title: str
    unified_summary: str
    category: str  # "MILITARY" o "CIVIL"
    subcategory: str  # "ground", "air", "drone", "politics", "economy", etc.
    location: LocationData
    intensity: float
    nsfw: bool
    date: str
    timestamp: int
    references: List[EventReference]

    # Filtri per il frontend
    filters: List[str]  # ["military", "pro_ua", "high_intensity"]

    # Metadati aggregazione
    source_count: int
    dominant_bias: str
    confidence_score: float

# ==========================================
# 🧠 AI ANALYSIS ENGINE (REFACTORED)
# ==========================================


def get_source_bias(source_name: str, platform: str) -> Dict:
    """
    Recupera bias hardcoded o usa AI come fallback.
    """
    source_key = source_name.lower().replace('@', '')

    if source_key in SOURCE_BIAS_DATABASE:
        return SOURCE_BIAS_DATABASE[source_key]

    # Fallback: analisi AI (implementata sotto)
    return {
        'bias': 'UNKNOWN',
        'reliability': 0.50,
        'type': 'UNCLASSIFIED'
    }


def analyze_with_ai_v2(text: str, source: str, platform: str, media_url: Optional[str] = None) -> Optional[Dict]:
    """
    Versione refactored con categorizzazione MILITARY/CIVIL e location precision.
    """
    if len(text) < 30:
        return None

    print(f"   🤖 AI Analizza ({len(text)} chars) da {source}...")

    # Recupera bias hardcoded
    source_bias_data = get_source_bias(source, platform)

    prompt = f"""
Sei un Senior OSINT Analyst specializzato nel conflitto Russia-Ucraina.
Analizza questo rapporto da "{source}" ({platform}):

TEXT: "{text}"

SOURCE BIAS (già classificato): {source_bias_data['bias']}

TASK CRITICI:

1. **CATEGORY CLASSIFICATION (MANDATORY)**:
   - "MILITARY": combattimenti, movimenti truppe, attacchi, perdite militari
   - "CIVIL": politica, economia, vita quotidiana, infrastrutture non-militari
   
2. **SUBCATEGORY**: [ground, air, drone, missile, artillery, naval, strategic, politics, economy, infrastructure, humanitarian]

3. **GEOLOCATION WITH PRECISION**:
   - Se menzione città specifica → lat/lon + precision="city"
   - Se solo regione → coordinate centrali + precision="region"  
   - Se generico "fronte sud" → precision="front_sector"
   - Se impossibile → lat=0, lon=0, precision="unknown"
   
4. **NSFW CHECK**: true se descrive violenza grafica (corpi, sangue, torture)

5. **INTENSITY**: 0.1 (minore) → 1.0 (nucleare/strategico)

6. **VERIFICATION**: 
   - "verified" se video/foto geolocate
   - "unverified" se solo testo
   - "disputed" se contraddetto da altre fonti

OUTPUT JSON (STRICT):
{{
    "valid": true,
    "category": "MILITARY",
    "subcategory": "drone",
    "title": "Breve titolo italiano (max 60 char)",
    "summary": "Descrizione completa tradotta in italiano professionale",
    "location": {{
        "lat": 50.4501,
        "lon": 30.5234,
        "precision": "city",
        "location_name": "Kyiv",
        "confidence": 0.85
    }},
    "intensity": 0.7,
    "nsfw": false,
    "verification_status": "unverified"
}}

Se il testo NON riguarda la guerra Russia-Ucraina, restituisci: {{"valid": false}}
"""

    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )

        raw = response.choices[0].message.content.replace(
            "```json", "").replace("```", "").strip()
        data = json.loads(raw)

        if not data.get('valid', False):
            return None

        # Costruisci oggetto SourceIntelligence
        source_intel = SourceIntelligence(
            source_name=source,
            platform=platform,
            bias=source_bias_data['bias'],
            reliability=source_bias_data['reliability'],
            source_type=source_bias_data['type'],
            original_url=f"https://t.me/{source}" if platform == "Telegram" else f"https://x.com/{source}",
            scrape_timestamp=datetime.now().isoformat()
        )

        # Aggiungi metadati
        data['source_intelligence'] = asdict(source_intel)
        data['original_text'] = text[:200]  # Primi 200 char
        data['media_urls'] = [media_url] if media_url else []

        return data

    except Exception as e:
        print(f"   ❌ Errore AI: {e}")
        return None

# ==========================================
# 🔄 EVENT AGGREGATION ENGINE
# ==========================================


def calculate_event_similarity(event1: Dict, event2: Dict) -> float:
    """
    Calcola similarità tra due eventi per aggregazione.
    Returns: score 0.0-1.0
    """
    score = 0.0

    # 1. Distanza geografica (se disponibile)
    if event1['location']['precision'] != 'unknown' and event2['location']['precision'] != 'unknown':
        try:
            dist_km = geodesic(
                (event1['location']['lat'], event1['location']['lon']),
                (event2['location']['lat'], event2['location']['lon'])
            ).kilometers

            if dist_km < 5:
                score += 0.4
            elif dist_km < 20:
                score += 0.2
        except:
            pass

    # 2. Differenza temporale (stesso giorno = alta similarità)
    time_diff = abs(event1['timestamp'] - event2['timestamp'])
    if time_diff < 3600:  # 1 ora
        score += 0.3
    elif time_diff < 86400:  # stesso giorno
        score += 0.2

    # 3. Stessa categoria e subcategoria
    if event1['category'] == event2['category']:
        score += 0.2
        if event1['subcategory'] == event2['subcategory']:
            score += 0.1

    return score


def aggregate_events(raw_events: List[Dict], similarity_threshold: float = 0.6) -> List[AggregatedEvent]:
    """
    Aggrega eventi simili in singoli report multi-source.
    """
    print(f"\n🔄 Aggregazione di {len(raw_events)} eventi grezzi...")

    aggregated = []
    processed = set()

    for i, event1 in enumerate(raw_events):
        if i in processed:
            continue

        # Trova eventi simili
        cluster = [event1]
        cluster_indices = {i}

        for j, event2 in enumerate(raw_events[i+1:], start=i+1):
            if j in processed:
                continue

            similarity = calculate_event_similarity(event1, event2)

            if similarity >= similarity_threshold:
                cluster.append(event2)
                cluster_indices.add(j)

        processed.update(cluster_indices)

        # Crea evento aggregato
        agg_event = create_aggregated_event(cluster)
        aggregated.append(agg_event)

    print(
        f"   ✅ Creati {len(aggregated)} eventi aggregati (da {len(raw_events)} grezzi)")
    return aggregated


def create_aggregated_event(cluster: List[Dict]) -> AggregatedEvent:
    """
    Crea un singolo evento aggregato da più report.
    """
    # Usa evento con reliability più alta come base
    cluster.sort(key=lambda x: x['source_intelligence']
                 ['reliability'], reverse=True)
    primary = cluster[0]

    # Genera ID univoco basato su location + timestamp
    event_id = hashlib.md5(
        f"{primary['location']['lat']}{primary['location']['lon']}{primary['timestamp']}".encode()
    ).hexdigest()[:12]

    # Costruisci references
    references = []
    for event in cluster:
        ref = EventReference(
            source_intelligence=SourceIntelligence(
                **event['source_intelligence']),
            original_text=event['original_text'],
            media_urls=event.get('media_urls', []),
            verification_status=event.get('verification_status', 'unverified')
        )
        references.append(ref)

    # Calcola bias dominante
    bias_counts = {}
    for ref in references:
        bias = ref.source_intelligence.bias
        bias_counts[bias] = bias_counts.get(bias, 0) + 1
    dominant_bias = max(bias_counts, key=bias_counts.get)

    # Calcola confidence basato su:
    # - Numero di fonti
    # - Reliability media
    # - Presenza di verifiche
    avg_reliability = sum(
        r.source_intelligence.reliability for r in references) / len(references)
    verified_count = sum(
        1 for r in references if r.verification_status == "verified")
    confidence = min(
        1.0, (avg_reliability + (len(references) * 0.1) + (verified_count * 0.2)))

    # Genera filtri per frontend
    filters = []
    filters.append(primary['category'].lower())  # "military" o "civil"
    filters.append(dominant_bias.lower())  # "pro_ua", "pro_ru", "neutral"
    if primary['intensity'] >= 0.8:
        filters.append('critical')
    elif primary['intensity'] >= 0.6:
        filters.append('high_intensity')
    if primary['nsfw']:
        filters.append('nsfw')

    # Estrai i dati location con valori di default di sicurezza
    loc_data = primary.get('location', {})

    safe_location = LocationData(
        lat=loc_data.get('lat', 0.0),
        lon=loc_data.get('lon', 0.0),
        precision=loc_data.get('precision', 'unknown'),
        location_name=loc_data.get('location_name', 'Unknown Location'),
        confidence=loc_data.get('confidence', 0.5)
    )

    return AggregatedEvent(
        event_id=event_id,
        title=primary['title'],
        unified_summary=primary['summary'],
        category=primary['category'],
        subcategory=primary['subcategory'],
        location=safe_location,  # <--- Ora usiamo l'oggetto sicuro
        intensity=primary['intensity'],
        nsfw=primary['nsfw'],
        date=datetime.fromtimestamp(
            primary['timestamp'] / 1000).strftime("%Y-%m-%d"),
        timestamp=primary['timestamp'],
        references=references,
        filters=filters,
        source_count=len(references),
        dominant_bias=dominant_bias,
        confidence_score=confidence
    )

# ==========================================
# 💾 GEOJSON EXPORT
# ==========================================


def export_to_geojson(events: List[AggregatedEvent], output_path: str):
    """
    Esporta eventi aggregati in GeoJSON con nuova struttura.
    """
    features = []

    for event in events:
        # Skip eventi senza location valida
        if event.location.precision == "unknown":
            print(f"   ⚠️ Skip {event.title} (location unknown)")
            continue

        # Serializza references
        # Serializza references (CORRETTO PER export_to_geojson)
        references_json = [
            {
                'source': {
                    'name': ref.source_intelligence.source_name,
                    'platform': ref.source_intelligence.platform,
                    'bias': ref.source_intelligence.bias,
                    'reliability': ref.source_intelligence.reliability
                },
                'url': ref.source_intelligence.original_url,  # <--- URL CORRETTO QUI
                'text_preview': ref.original_text[:150] + "...",
                'media': ref.media_urls,
                'verification': ref.verification_status
            }
            for ref in event.references  # <--- USA SOLO "event"
        ]

        properties = {
            'event_id': event.event_id,
            'title': event.title,
            'description': event.unified_summary,
            'category': event.category,
            'type': event.subcategory,
            'intensity': event.intensity,
            'nsfw': event.nsfw,
            'date': event.date,
            'timestamp': event.timestamp,

            # NUOVI CAMPI CHIAVE
            'location_precision': event.location.precision,
            'location_name': event.location.location_name,
            'location_confidence': event.location.confidence,

            'source_count': event.source_count,
            'dominant_bias': event.dominant_bias,
            'confidence_score': event.confidence_score,

            'filters': event.filters,
            'references': references_json,

            # Backward compatibility
            'actor_code': 'UKR' if 'pro_ua' in event.filters else ('RUS' if 'pro_ru' in event.filters else 'UNK')
        }

        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [event.location.lon, event.location.lat]
            },
            'properties': properties
        }

        features.append(feature)

    geojson = {
        'type': 'FeatureCollection',
        'metadata': {
            'generated': datetime.now().isoformat(),
            'total_events': len(features),
            'version': '2.0'
        },
        'features': features
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)

    print(f"✅ Esportati {len(features)} eventi in {output_path}")

# ==========================================
# 🚀 MAIN WORKFLOW (REFACTORED)
# ==========================================


def get_google_sheet_client():
    """Connette al Google Sheet usando le credenziali esistenti"""
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']

    # Percorso del file credenziali (lo stesso che hai sistemato prima)
    creds_path = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'service_account.json')

    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    client = gspread.authorize(creds)

    # URL del tuo Sheet (preso dal tuo file ai_agent.py)
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
    return client.open_by_url(SHEET_URL).get_worksheet(0)


async def main():
    """
    Main workflow con nuova logica aggregazione.
    """
    print("=== 🌍 IMPACT ATLAS v2.0 - INTELLIGENCE AGGREGATOR ===\n")

    # 1. Carica eventi esistenti e crea set di ID già processati
    DATA_FILE = "events.geojson"
    existing_events = []
    existing_source_ids = set()  # ID delle fonti originali (non event_id aggregati)

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            old_geojson = json.load(f)
            existing_events = old_geojson.get('features', [])

            # Estrai gli ID delle fonti originali dalle references
            for feat in existing_events:
                refs = feat['properties'].get('references', [])
                for ref in refs:
                    source = ref.get('source', {})
                    # Crea ID univoco: platform_sourcename_hash
                    source_id = f"{source.get('platform')}_{source.get('name')}_{hash(ref.get('text_preview', ''))}"
                    existing_source_ids.add(source_id)

        print(
            f"📂 Caricati {len(existing_events)} eventi esistenti ({len(existing_source_ids)} fonti)")

    # 2. SCRAPING REALE
    print("\n🔍 Avvio scraping multi-source...")

    # Telegram
    tg_items = await scrape_telegram_v2(existing_source_ids)

    # Twitter RSS
    tw_items = scrape_twitter_rss_v2(existing_source_ids)

    # Combina tutti i raw items
    all_raw_items = tg_items + tw_items

    print(f"\n📊 Raccolti {len(all_raw_items)} nuovi report grezzi")

    if not all_raw_items:
        print("💤 Nessun nuovo evento da processare")
        return

    # 3. Analisi AI su ogni raw item
    print("\n🤖 Analisi AI in corso...")
    analyzed_events = []

    for idx, item in enumerate(all_raw_items, 1):
        print(f"   [{idx}/{len(all_raw_items)}] Analisi: {item['source']}...")

        result = analyze_with_ai_v2(
            text=item['text'],
            source=item['source'],
            platform=item['platform'],
            media_url=item.get('media_url')
        )

        if result:
            # Aggiungi timestamp se mancante
            if 'timestamp' not in result:
                result['timestamp'] = int(datetime.now().timestamp() * 1000)
            analyzed_events.append(result)
            print(f"      ✅ {result['category']}: {result['title'][:50]}...")
        else:
            print(f"      ⚠️ Scartato (non rilevante)")

    print(
        f"\n✅ Analizzati {len(analyzed_events)} eventi validi su {len(all_raw_items)} totali")

    if not analyzed_events:
        print("💤 Nessun evento rilevante dopo analisi AI")
        return

    # 4. Aggregazione eventi simili
    aggregated = aggregate_events(analyzed_events, similarity_threshold=0.6)

    # 5. Merge con eventi esistenti
    print(f"\n💾 Salvataggio di {len(aggregated)} eventi aggregati...")

    # Converti vecchi eventi in features se necessario
    all_features = existing_events.copy()

    # Aggiungi nuovi eventi aggregati
    for agg_event in aggregated:
        # Skip se location unknown
        if agg_event.location.precision == "unknown":
            print(f"   ⚠️ Skip {agg_event.title} (location unknown)")
            continue

        # Serializza references
        # Serializza references (CORRETTO PER MAP.JS)
        # Serializza references (CORRETTO PER main)
        references_json = [
            {
                'source': {
                    'name': ref.source_intelligence.source_name,
                    'platform': ref.source_intelligence.platform,
                    'bias': ref.source_intelligence.bias,
                    'reliability': ref.source_intelligence.reliability
                },
                'url': ref.source_intelligence.original_url,  # <--- URL CORRETTO QUI
                'text_preview': ref.original_text[:150] + "...",
                'media': ref.media_urls,
                'verification': ref.verification_status
            }
            for ref in agg_event.references  # <--- USA SOLO "agg_event"
        ]

        properties = {
            'event_id': agg_event.event_id,
            'title': agg_event.title,
            'description': agg_event.unified_summary,
            'category': agg_event.category,
            'type': agg_event.subcategory,
            'intensity': agg_event.intensity,
            'nsfw': agg_event.nsfw,
            'date': agg_event.date,
            'timestamp': agg_event.timestamp,
            'location_precision': agg_event.location.precision,
            'location_name': agg_event.location.location_name,
            'location_confidence': agg_event.location.confidence,
            'source_count': agg_event.source_count,
            'dominant_bias': agg_event.dominant_bias,
            'confidence_score': agg_event.confidence_score,
            'filters': agg_event.filters,
            'references': references_json,
            'actor_code': 'UKR' if 'pro_ua' in agg_event.filters else ('RUS' if 'pro_ru' in agg_event.filters else 'UNK')
        }

        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [agg_event.location.lon, agg_event.location.lat]
            },
            'properties': properties
        }

        all_features.append(feature)

    # 6. Export finale
    geojson = {
        'type': 'FeatureCollection',
        'metadata': {
            'generated': datetime.now().isoformat(),
            'total_events': len(all_features),
            'version': '2.0'
        },
        'features': all_features
    }

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)

    print(
        f"✅ DATABASE AGGIORNATO: {len(all_features)} eventi totali ({len(aggregated)} nuovi)")

# ... (dopo aver generato aggregated events) ...

    # --- NUOVO: SALVATAGGIO SU GOOGLE SHEETS ---
    if aggregated:
        print(
            f"\n📝 Scrittura di {len(aggregated)} nuovi eventi su Google Sheets...")
        try:
            worksheet = get_google_sheet_client()

            new_rows = []
            for agg in aggregated:
                # Mappatura dei dati dell'Agente nelle colonne del tuo Sheet
                # Ordine Colonne: Title, Date, Type, Location, Latitude, Longitude, Source,
                # Archived, Verification, Description, Notes, Video, Intensity, Actor

                # Prepara la fonte (link principale)
                primary_ref = agg.references[0] if agg.references else None
                source_link = primary_ref.source_intelligence.original_url if primary_ref else "AI Aggregation"

                row = [
                    agg.title,                          # Title
                    agg.date,                           # Date
                    agg.subcategory.title(),            # Type
                    agg.location.location_name,         # Location
                    str(agg.location.lat),              # Latitude
                    str(agg.location.lon),              # Longitude
                    source_link,                        # Source
                    "FALSE",                            # Archived (è nuovo)
                    "AI_Generated",                     # Verification
                    agg.unified_summary,                # Description
                    # Notes
                    f"Sources: {agg.source_count} | Confidence: {int(agg.confidence_score*100)}%",
                    # Video (o aggiungi logica se c'è)
                    "",
                    str(agg.intensity),                 # Intensity
                    "RUS" if "pro_ru" in agg.filters else (
                        "UKR" if "pro_ua" in agg.filters else "UNK")  # Actor
                ]
                new_rows.append(row)

            # Scrive tutto in una volta (più veloce)
            worksheet.append_rows(new_rows)
            print("✅ Salvataggio su Google Sheet completato con successo!")

        except Exception as e:
            print(f"❌ ERRORE SCRITTURA SHEET: {e}")
            print("   (Gli eventi sono stati salvati solo nel JSON locale)")

# ==========================================
# 🕵️ SCRAPERS ADATTATI (VERSIONE V2)
# ==========================================


async def scrape_telegram_v2(existing_source_ids: set) -> List[Dict]:
    """
    Versione adattata che restituisce raw items invece di eventi processati.
    """
    print("\n📡 Telegram Scraper Avviato...")
    raw_items = []

    client = TelegramClient(
        SESSION_FILE_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH)

    try:
        await client.connect()

        if not await client.is_user_authorized():
            print("⚠️ Autenticazione richiesta!")
            phone = input("Inserisci il tuo numero di telefono (+39...): ")
            await client.send_code_request(phone)

            try:
                code = input("Inserisci il codice ricevuto: ")
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                pw = input("Inserisci la password 2FA: ")
                await client.sign_in(password=pw)

        print("   ✅ Login effettuato")

        channels = TELEGRAM_CHANNELS.copy()
        random.shuffle(channels)

        for i, channel in enumerate(channels):
            print(f"   [{i+1}/{len(channels)}] Scansione @{channel}...")

            try:
                msg_count = 0
                async for msg in client.iter_messages(channel, limit=15):
                    if not msg.text or len(msg.text) < 50:
                        continue

                    # Crea ID univoco per questa fonte
                    source_id = f"telegram_{channel}_{msg.id}"

                    if source_id in existing_source_ids:
                        continue

                    # Estrai media URL se presente
                    media_url = None
                    if msg.photo:
                        try:
                            media_url = f"https://t.me/{channel}/{msg.id}"
                        except:
                            pass

                    raw_items.append({
                        'source': channel,
                        'platform': 'Telegram',
                        'text': msg.text,
                        'media_url': media_url,
                        'original_id': source_id,
                        'source_url': f"https://t.me/{channel}/{msg.id}"
                    })

                    existing_source_ids.add(source_id)
                    msg_count += 1

                    await asyncio.sleep(random.uniform(0.5, 1.5))

                if msg_count > 0:
                    print(f"      ↳ Raccolti {msg_count} nuovi messaggi")

            except FloodWaitError as e:
                print(f"      🛑 FLOOD WAIT: {e.seconds}s")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                print(f"      ⚠️ Errore su {channel}: {e}")

            await asyncio.sleep(random.uniform(5, 10))

    except Exception as e:
        print(f"❌ Errore Telegram: {e}")
    finally:
        try:
            await client.disconnect()
        except:
            pass

    return raw_items


def scrape_twitter_rss_v2(existing_source_ids: set) -> List[Dict]:
    """
    Versione adattata che restituisce raw items.
    """
    print("\n🐦 Twitter RSS Scraper Avviato...")
    raw_items = []

    for user in TWITTER_ACCOUNTS:
        print(f"   🔍 Scansione @{user}...")
        success = False

        for instance in NITTER_INSTANCES:
            try:
                url = f"{instance}/{user}/rss"
                feed = feedparser.parse(url)

                if feed.bozo or not feed.entries:
                    continue

                print(f"      ✅ {instance}: {len(feed.entries)} tweet trovati")

                for entry in feed.entries[:10]:
                    source_id = f"twitter_{user}_{entry.id}"

                    if source_id in existing_source_ids:
                        continue

                    text = entry.summary.replace("<br>", "\n")

                    # Estrai immagine
                    img_url = None
                    if 'img src="' in entry.summary:
                        try:
                            img_url = entry.summary.split(
                                'img src="')[1].split('"')[0]
                        except:
                            pass

                    raw_items.append({
                        'source': user,
                        'platform': 'Twitter',
                        'text': text,
                        'media_url': img_url,
                        'original_id': source_id,
                        'source_url': entry.link
                    })

                    existing_source_ids.add(source_id)

                success = True
                break

            except Exception as e:
                continue

        if not success:
            print(f"      ⚠️ Impossibile raggiungere @{user}")

    return raw_items


if __name__ == '__main__':
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
