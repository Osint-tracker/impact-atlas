import sqlite3
import requests
from bs4 import BeautifulSoup
import os
import json
import re
import time
import math
import logging
from datetime import datetime, timedelta
from openai import OpenAI
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

geolocator = Nominatim(user_agent="ai_agent_fixer_v2")

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SUPER_SQUAD")

# --- LOAD ENV ---
load_dotenv()

# Absolute Paths for JSON Databases
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_DB_PATH = os.path.join(BASE_DIR, '../assets/data/sources_db.json')
KEYWORDS_DB_PATH = os.path.join(BASE_DIR, '../assets/data/keywords_db.json')

# --- PROTOCOL CONSTANTS (ELASTIC MODE) ---
# LOGICA: Il valore base è l'importanza STRATEGICA.
# Per arrivare a 1.0 (Massimo), serve un danno CRITICAL (x1.5).
# Esempio: Airbase (0.7) * Critical (1.5) = 1.05 -> 1.0
# Esempio: Airbase (0.7) * Light (0.5) = 0.35 (Corretto per scaramucce)

INTENSITY_DB = {
    # TIER A (0.8 - 1.0) - Esistenziali (Solo questi partono altissimi)
    "CRITICAL_NUCLEAR": 1.0,  # Se succede, è la fine. Base 1.0.
    "CRITICAL_DAM": 0.9,      # Disastro ambientale immediato.

    # TIER B (0.6 - 0.75) - Strategici (Richiedono danno serio per diventare Rossi)
    "MIL_AIRBASE": 0.7,             # Era 1.0 -> Ora serve distruggerla per avere 1.0
    "IND_DEFENSE_PLANT": 0.7,
    "INFRA_STRATEGIC_BRIDGE": 0.7,  # Es. Ponte di Crimea
    "MIL_SHIP": 0.7,                # Incrociatore
    "INFRA_REFINERY": 0.65,
    "MIL_EW_RADAR": 0.65,
    "INFRA_GENERATION": 0.65,       # Centrali elettriche

    # TIER C (0.4 - 0.55) - Operativi (Importanti ma rimpiazzabili)
    "MIL_AMMO_DEPOT": 0.55,
    "MIL_MLRS_STRATEGIC": 0.55,     # HIMARS / Patriot
    "MIL_HQ": 0.5,
    "MIL_AIR_DEFENSE_LONG": 0.5,    # S-300/400
    "INFRA_FUEL_DEPOT": 0.45,

    # TIER D (0.25 - 0.35) - Tattici (Il grosso della guerra)
    "MIL_ARTILLERY": 0.35,
    "MIL_APC_TANK": 0.35,
    "MIL_MLRS_TACTICAL": 0.35,      # Grad
    "IND_FACTORY": 0.3,
    "MIL_AIR_DEFENSE_SHORT": 0.3,   # Strela / Manpads
    "INFRA_LOGISTICS": 0.25,        # Magazzini generici

    # TIER E (0.05 - 0.2) - Minori / Civili
    "INFRA_GRID_LOCAL": 0.2,        # Cabina elettrica di quartiere
    "MIL_VEHICLE_LIGHT": 0.15,      # Jeep / Camion
    "MIL_TRENCH": 0.1,              # Posizione di fanteria
    "MIL_PERSONNEL_OPEN": 0.1,      # Fanteria allo scoperto
    "CIV_PUBLIC": 0.1,
    "CIV_COMMERCIAL": 0.1,
    "CIV_RESIDENTIAL": 0.1,
    "OPEN_FIELD": 0.05,
    "UNKNOWN": 0.0
}

DAMAGE_MODIFIERS = {
    "CRITICAL": 1.5,  # DISTRUTTO: Boost per raggiungere 1.0
    "HEAVY": 1.2,     # DANNI SERI: Boost moderato
    # DANNI LIEVI: Dimezza il valore (Cruciale per le scaramucce!)
    "LIGHT": 0.5,
    "NONE": 0.0,      # NESSUN DANNO
    "UNKNOWN": 0.5    # INCERTO: Dimezza (Meglio sottostimare che allarmare)
}
# --- SYSTEM PROMPTS ---

SOLDIER_SYSTEM_PROMPT = """
### SYSTEM PROMPT: THE TACTICAL ANALYST

**ROLE**
You are a Military Intelligence Sensor. Your goal is NOT to write a story, but to EXTRACT structured data from raw reports.
Your task is to convert a CLUSTER of raw, noisy, multi-lingual telegram messages (RU/UA/EN) into a single, rigorous JSON INTELLIGENCE REPORT.
You must adhere to the **TITAN-10** scoring protocol for Kinetic, Target, and Effect assessment.

**INPUT DATA**
You will receive a "Cluster Object" containing:
1.  `reference_timestamp`: The ISO timestamp of the newest message (the anchor time).
2.  `raw_messages`: A list of text snippets from different sources about the same event.

**CORE DIRECTIVES (NON-NEGOTIABLE)**

1.  **GEOLOCATION PROTOCOL (CRITICAL - READ CAREFULLY):**
    * **EXPLICIT COORDS:** ONLY if the text contains numerical coordinates (e.g., "48.123, 37.456"), extract them into `geo_location.explicit`.
    * **INFERRED:** If no numbers are present, extract the Toponym (City/Village) and the specific landmark (e.g., "School No.3", "Industrial Zone") into `geo_location.inferred`.
    * **NEVER HALLUCINATE:** Do not convert a city name into coordinates yourself. If no coordinates are written in text, `geo_location.explicit` must be `null`.
    * **SINGLE IMPACT POINT:** You must identify the ONE main location where the event physically happened.
    * **SINGLE LOCATION RULE:** If multiple locations are mentioned, choose the MOST SPECIFIC ONE where the kinetic event happened. Do NOT output a list like "Kyiv, Lviv, Odessa". Output ONLY "Kyiv".
    * **SPECIFICITY:** If text says "Explosion in Odesa", output "Odesa". If it says "Odesa region", output "Odesa region".

2.  **TIME RECONSTRUCTION:**
    * Analyze time references relative to `reference_timestamp`.
    * "Tonight" -> Same date as reference.
    * "Yesterday" -> Reference date minus 1 day.
    * Output the estimated event time in ISO format.

3.  **SLANG DECODING (Glossary):**
    * "Bird", "Mavic", "Baba Yaga" -> TYPE: "UAV/Drone"
    * "Box", "Armor" -> TYPE: "Armored Vehicle"
    * "200" -> KILLED / "300" -> WOUNDED.
    * "Cotton" (Bavovna) -> Explosion.
    * "47th", "3rd Assault", "82nd" -> MILITARY UNITS.

4.  **ORBAT EXTRACTION (MILITARY UNITS):**
    * Identify specific military units mentioned.
    * Normalize ID: "47th Brigade" -> "UA_47_MECH_BDE" (if unsure use generic "UA_47_BDE").
    * STATUS: "ENGAGED" (Fighting), "DESTROYED" (Eliminated), "ACTIVE" (Present), "REGROUPING" (Rotated).
    * INFERENCE: If "Challenger 2" mentioned -> implies "UA_82_AIR_ASSAULT" (only if highly specific).
    * OUTPUT FIELD: `military_units_detected`: [{ `unit_name`: "47th Brigade", `unit_id`: "UA_47_MECH_BDE", `faction`: "UA", `type`: "MECH_INF", `status`: "ENGAGED" }]

**PROTOCOL "TITAN-10": INTENSITY SCORING STANDARDS**
Assign scores (1-10) based STRICTLY on these definitions. Do not guess.

**VECTOR K: KINETIC MAGNITUDE (The Physics)**
- 1: Small Arms (Rifles), Sniper.
- 2: Light Mortars (60-82mm), Grenade drops.
- 3: Heavy Mortars (120mm), SPG-9, Single FPV drone.
- 4: Tube Artillery (155mm) - Single/Platoon.
- 5: MLRS (Grad) - Partial packet, Tank shelling.
- 6: Precision Strike (GMLRS/HIMARS - Single).
- 7: Heavy Strike (Iskander, Storm Shadow - Single), Glide Bomb (KAB-500).
- 8: Massive Strike (Heavy MLRS Salvo, Missile Wave >3).
- 9: Strategic Bombing (Tu-95 Salvo), Thermobaric (TOS-1A).
- 10: WMD / Dam Breach / Massive Ammo Detonation (Secondaries > 1km).

**VECTOR T: TARGET TIER (The Value)**
- 1: Empty Terrain, Open Field, Abandoned structures.
- 2: Civilian Residential (Low Value), Private Vehicles.
- 3: Infantry Positions (Foxholes), Light Trucks.
- 4: Tactical Logistics (Fuel trucks, Ammo crates), Mortar Pits.
- 5: Heavy Armor (Tanks, IFVs), Artillery Positions.
- 6: Advanced Systems (EW Stations, Radar, SAM Short-range).
- 7: Operational HQ (Bn/Bde level), Key Bridges (Tactical).
- 8: Strategic Air Defense (S-300/400, Patriot), Airfields, Substations.
- 9: Strategic Industry (Refineries, Factories), High Command.
- 10: National Leadership, Nuclear Silos, Capital Gov District.

**VECTOR E: EFFECT / OUTCOME (The Reality)**
- 1: FAILURE / INTERCEPTED / UNKNOWN EFFECT.
- 2: NEGLIGIBLE. Missed by >50m, paint scratch.
- 3: SUPPRESSION. Target forced to move/hide.
- 4: LIGHT DAMAGE. Mobility kill (repairable), WIA.
- 5: MODERATE DAMAGE. Mission kill (needs factory repair).
- 6: SEVERE DAMAGE. Structural breach, fire ignited.
- 7: DESTRUCTION (Single). Asset destroyed/burned out.
- 8: DESTRUCTION (Group). Multiple assets destroyed.
- 9: ANNIHILATION. Vaporized, catastrophic secondaries.
- 10: TOTAL ERASE. Area uninhabitable.

**OUTPUT JSON SCHEMA**
Return ONLY valid JSON:
{
  "event_analysis": {
    "is_kinetic_military_event": true,
    "confidence_level": "HIGH | MEDIUM | LOW",
    "summary_en": "Concise tactical summary (max 20 words)"
  },
  "visual_evidence": boolean,
  "timing": { "estimated_event_timestamp": "ISO_STRING | null" },
  "geo_location": {
    "explicit": { "lat": null, "lon": null },
    "inferred": { "toponym_raw": "SINGLE_CITY_NAME", "spatial_relation": "string" }
  },
  "titan_assessment": {
     "kinetic_score": INTEGER (1-10),
     "target_score": INTEGER (1-10),
     "effect_score": INTEGER (1-10),
     "target_type_category": "STRING (e.g. LOGISTICS, INFANTRY, ENERGY)",
     "is_deep_strike": BOOLEAN (True if >30km behind front),
     "new_tech_used": BOOLEAN
  },
  "actors": {
    "aggressor": { "side": "RU | UA | UNKNOWN" },
    "target": { "side": "RU | UA | CIVILIAN" }
  },
  "military_units_detected": [
      {
          "unit_name": "String (Raw Name)",
          "unit_id": "String (Normalized ID)",
          "faction": "UA | RU",
          "type": "ARMORED | INFANTRY | ARTILLERY | AIRBORNE | SOF",
          "status": "ACTIVE | ENGAGED | DESTROYED | REGROUPING"
      }
  ]
}
"""


class SuperSquadAgent:

    def __init__(self):
        # 1. API Keys
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

        # MODIFICA: Carichiamo Serper
        self.serper_api_key = os.getenv("SERPER_API_KEY")
        if not self.serper_api_key:
            print("⚠️ ATTENZIONE: Manca SERPER_API_KEY nel file .env")

        if not self.openai_api_key or not self.openrouter_api_key:
            raise ValueError("❌ ERROR: API Keys missing")

        # 2. Initialize Clients
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        self.openrouter_client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.openrouter_api_key,
        )
        self.brain_client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),
            default_headers={
                "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
                "X-Title": "OSINT Tracker"
            }
        )
        self.router_client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),
            default_headers={
                "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
                "X-Title": "OSINT Tracker"
            }
        )

        # 3. Load Knowledge Bases
        self.sources_db = self._load_json_db(SOURCES_DB_PATH, "sources")
        self.keywords_db = self._load_json_db(KEYWORDS_DB_PATH, "keywords")

        print("✅ Super Squad Agent Initialized (Engine: Google Serper Time-Machine).")

    # =========================================================================
    # 🗺️ ACLED FULL SOURCE MAP (180+ SOURCES)
    # =========================================================================
    ACLED_SOURCE_MAP = {
        # --- 1. ISTITUZIONI MILITARI & GOVERNATIVE (UFFICIALI) ---
        "Ministry of Defence of Ukraine": "mil.gov.ua",
        "Ministry of Defence of Russia": "mil.ru",
        "General Staff of the Armed Forces of Ukraine": "facebook.com/GeneralStaff.ua",
        "National Guard of Ukraine": "ngu.gov.ua",
        "State Border Guard Service of Ukraine": "dpsu.gov.ua",
        "SBU": "ssu.gov.ua",
        "Police Forces of Ukraine Press Service": "npu.gov.ua",
        "State Emergency Service of Ukraine": "dsns.gov.ua",
        "National Police of Ukraine": "npu.gov.ua",
        "Prosecutor General's Office of Ukraine": "gp.gov.ua",
        "Ministry of Reintegration of Temporarily Occupied Territories": "minre.gov.ua",
        "Belgorod Governor": "belregion.ru",
        "Kursk Governor": "rkursk.ru",
        "Bryansk Governor": "bryanskobl.ru",
        "Voronezh Governor": "govvrn.ru",

        # --- 2. ENTI SEPARATISTI / OCCUPAZIONE (DPR/LPR) ---
        "DPR Armed Forces Press Service": "dan-news.ru",  # Agenzia ufficiale DPR
        "LPR People's Militia Press Service": "lug-info.com",  # Agenzia ufficiale LPR
        "DPR Ministry of Emergency Situations": "dnmchs.ru",
        "LPR Ministry of Emergency Situations": "mchs-lnr.su",
        "DPR JCCC": "dnr-sckk.ru",
        "LPR JCCC": "cxid.info",  # Spesso ripubblicato qui

        # --- 3. AGENZIE DI STAMPA & TV UCRAINE (NAZIONALI) ---
        "Suspilne Media": "suspilne.media",
        "Suspilne": "suspilne.media",
        "24 Channel": "24tv.ua",
        "Ukrinform": "ukrinform.ua",
        "Unian": "unian.net",
        "RBC-Ukraine": "rbc.ua",
        "Ukrainska Pravda": "pravda.com.ua",
        "NV": "nv.ua",
        "Novoye Vremya Ukraine": "nv.ua",
        "Novoye Vremya": "nv.ua",
        "Censor.NET": "censor.net",
        "Espreso.TV": "espreso.tv",
        "Hromadske": "hromadske.ua",
        "TSN": "tsn.ua",
        "LB.ua": "lb.ua",
        "Focus": "focus.ua",
        "Gordon": "gordonua.com",
        "Zn.ua": "zn.ua",
        "Liga.net": "liga.net",
        "Interfax-Ukraine": "interfax.com.ua",
        "Segodnya": "segodnya.ua",
        "Fakty i Kommentarii": "fakty.ua",
        "Obozrevatel": "obozrevatel.com",
        "Strana.ua": "strana.today",  # Spesso bloccato, ma proviamo
        "Telegraf": "telegraf.com.ua",
        "Apostrophe": "apostrophe.ua",
        "Gazeta.ua": "gazeta.ua",
        "Glavcom": "glavcom.ua",
        "Vikna": "vikna.tv",
        "5 Kanal": "5.ua",
        "Pryamiy": "prm.ua",
        "Babel": "babel.ua",
        "Rubryka": "rubryka.com",
        "Texty": "texty.org.ua",
        "Slidstvo.Info": "slidstvo.info",

        # --- 4. MEDIA REGIONALI UCRAINI (CRUCIALI PER ACLED) ---
        "061.ua": "061.ua",  # Zaporizhzhia
        "Inform.zp.ua": "inform.zp.ua",  # Zaporizhzhia
        "Zaxid": "zaxid.net",  # Lviv/Ovest
        "Dumskaya": "dumskaya.net",  # Odesa
        "Odesa Journal": "odessa-journal.com",
        "Most": "most.ks.ua",  # Kherson
        "Kherson News": "khersonline.net",
        "Novosti N": "novosti-n.org",  # Mykolaiv
        "News of Donbas": "novosti.dn.ua",
        "Donbas News": "novosti.dn.ua",
        "Ostrov": "ostro.org",  # Donbas
        "Krym Realii": "ru.krymr.com",  # Crimea
        "Black Sea News": "blackseanews.net",
        "Voice of Crimea": "voicecrimea.com.ua",
        "Qirim News": "qirim.news",

        # --- 5. MEDIA RUSSI (UFFICIALI & INDIPENDENTI) ---
        "TASS": "tass.ru",
        "ITAR-TASS": "tass.ru",
        "RIA Novosti": "ria.ru",
        "Kommersant": "kommersant.ru",
        "Interfax": "interfax.ru",
        "Lenta.ru": "lenta.ru",
        "Izvestia": "iz.ru",
        "Komsomolskaya Pravda": "kp.ru",
        "Moskovskij Komsomolets": "mk.ru",
        "Argumenty I Fakty": "aif.ru",
        "Rossiyskaya Gazeta": "rg.ru",
        "Vedomosti": "vedomosti.ru",
        "Regnum": "regnum.ru",
        "Gazeta.ru": "gazeta.ru",
        "Fontanka": "fontanka.ru",
        "Meduza": "meduza.io",  # Indipendente (Riga)
        "Mediazona": "zona.media",
        "MediaZone": "zona.media",
        "Novaya Gazeta": "novayagazeta.ru",
        "The Moscow Times": "themoscowtimes.com",
        "TV Rain": "tvrain.tv",
        "Dozhd": "tvrain.tv",
        "OVD Info": "ovd.info",
        "The Insider": "theins.ru",
        "Istories": "istories.media",
        "Proekt": "proekt.media",
        "Holod": "holod.media",
        "Sota": "sotaproject.com",
        "Activatica": "activatica.org",
        "Rosbalt": "rosbalt.ru",
        "Caucasian Knot": "kavkaz-uzel.eu",
        "7x7": "7x7-journal.ru",

        # --- 6. OSINT, ONG & ANALISTI ---
        "Institute for the Study of War": "understandingwar.org",
        "ISW": "understandingwar.org",
        # Difficile da cercare testualmente, ma proviamo
        "Deep State": "deepstatemap.live",
        "Centre for Information Resilience": "info-res.org",
        "Bellingcat": "bellingcat.com",
        "Conflict Intelligence Team": "citeam.org",
        "InformNapalm": "informnapalm.org",
        "Militarnyi": "mil.in.ua",
        "Defense Express": "defence-ua.com",
        "Sprotyv": "sprotyv.mod.gov.ua",  # National Resistance Center
        "Kharkiv Human Rights Protection Group": "khpg.org",
        "ZMINA": "zmina.info",
        "Human Rights Watch": "hrw.org",
        "HRW": "hrw.org",
        "Amnesty International": "amnesty.org",
        "OSCE": "osce.org",
        "UN Human Rights Monitoring Mission": "ukraine.un.org",
        "Insecurity Insight": "insecurityinsight.org",
        "Crew Against Torture": "pytkam.net",
        "SOVA": "sova-center.ru",
        "DIGNITY": "dignity.dk",

        # --- 7. MEDIA INTERNAZIONALI (Copertura Ucraina) ---
        "Radio Liberty": "radiosvoboda.org",  # UA Service
        "RFE/RL": "rferl.org",
        "BBC News": "bbc.com",
        "BBC Ukrainian": "bbc.com/ukrainian",
        "CNN": "cnn.com",
        "Reuters": "reuters.com",
        "AFP": "afp.com",
        "Associated Press": "apnews.com",
        "New York Times": "nytimes.com",
        "NYT": "nytimes.com",
        "Washington Post": "washingtonpost.com",
        "The Guardian": "theguardian.com",
        "Al Jazeera": "aljazeera.com",
        "Deutsche Welle": "dw.com",
        "Voice of America": "ukrainian.voanews.com",

        # --- 8. TELEGRAM CHANNELS (SOLO QUELLI CON SITI MIRROR/WEB) ---
        # Nota: La maggior parte dei TG puri sarà gestita dal fallback "Name Search"
        "WarGonzo": "t.me/wargonzo",  # Non indicizzabile bene, ma lo lasciamo per reference
        "Rybar": "rybar.ru",  # Ha un sito!
        "Kotsnews": "kp.ru",  # Reporter di KP

        # --- AGGREGATORI CHE NON CANCELLANO MAI ---
        "Liveuamap": "liveuamap.com",
        "Ukr.net": "ukr.net",         # Storico news feed ucraino
        "DeepState": "deepstatemap.live",
        "Understanding War": "understandingwar.org",

        # --- 9. MEDIA BIELORUSSI ---
        "Belsat": "belsat.eu",
        "Charter-97": "charter97.org",
        "Nashaniva": "nashaniva.com",
        "Zerkalo": "zerkalo.io",
        "Nexta": "t.me/nexta_live",  # Principalmente TG
        "Hajun": "motolko.help",  # Belarus Hajun project

    }

    # =========================================================================
    # 🗂️ TAXONOMY
    # =========================================================================
    # (Drone, Missile, ecc.)
    EVENT_TYPES = [
        "Missile Strike",        # Iskander, Kinzhal, Kalibr, S-300
        "Drone Strike",          # Shahed, FPV, Lancet
        "Airstrike",             # KAB, FAB, Su-34, Bombing
        "Artillery Shelling",    # Grad, Mortar, Howitzer
        "Ground Clash",          # Battle, Assault, Skirmish, Shooting
        "Naval Engagement",      # Sea Drone, Ship hit
        "IED / Explosion",       # Mines, Car bombs, Partisan sabotage
        "Political / Unrest",    # Arrests, Protests
        "Civil / Accident",      # Fires, Train crash, Infrastructure failure
        "Strategic Development"  # Troop movement, Commander changes
    ]
    # =========================================================================
    # 🧠 CORE INTELLIGENCE LOGIC (Event Context & Fingerprints)
    # =========================================================================

    def _init_event_context(self, row, acled_source):
        """Crea l'oggetto 'Dossier' per tracciare l'indagine su questo evento."""
        return {
            "title": row.get('Title') or row.get('notes'),
            "date": row.get('Date'),
            "location": row.get('Location'),
            "acled_source_raw": acled_source,

            # Evidence Buckets (Dove accumuliamo le prove)
            "sniper_results": [],     # Risultati da site:dominio
            "fallback_results": [],   # Risultati da ricerca generica

            # Decision Finale
            "status": "PENDING",      # FOUND_ORIGINAL / CORROBORATED / NOT_FOUND
            "verification_method": None,
            "best_link": None,
            "confidence_score": 0.0,
            "ai_summary": ""
        }

    def _load_json_db(self, path, key_name):
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    content = data.get(key_name, [])

                    # Normalize list to dict for fast lookup
                    if isinstance(content, list):
                        db_dict = {}
                        for item in content:
                            key = item.get('domain') or item.get('word')
                            if key:
                                db_dict[key.lower().replace('www.', '')] = item
                        return db_dict
                    return content if isinstance(content, dict) else {}
            else:
                print(f"⚠️ DB File not found: {path}")
                return {}
        except Exception as e:
            print(f"❌ Error loading DB {path}: {e}")
            return {}

    # =========================================================================
    # 🛡️ STEP 0: THE BOUNCER v2.0 (Hybrid: Regex + AI)
    # =========================================================================

    def _is_obvious_junk(self, text):
        """
        LAYER 1: Filtro meccanico a costo zero.
        Ritorna (True, "motivo") se è spazzatura ovvia.
        """
        t = text.lower()

        # 1. Errori Tecnici / Pagine Vuote
        if len(t) < 50:
            return True, "Text too short"
        if "404 not found" in t or "enable cookies" in t or "captcha" in t:
            return True, "Technical Error Page"

        # 2. Blacklist Aggressiva (Crypto, Casino, Porn)
        # Usiamo word boundaries (\b) per evitare falsi positivi parziali
        junk_patterns = [
            r"\b(bitcoin|crypto|nft|ethereum|wallet|binance)\b",  # Crypto
            r"\b(casino|slot\s?machine|poker|betting|bonus)\b",   # Gambling
            r"\b(dating|hot\s?girls|sexy|porn|xxx)\b",            # Adult
            r"\b(viagra|cialis|weight\s?loss)\b",                 # Pharma Spam
            r"\b(subscribe\s?to\s?view|accedi\s?per|login)\b"     # Paywall hard
        ]

        for pattern in junk_patterns:
            if re.search(pattern, t):
                return True, f"Regex Blacklist: {pattern}"

        # 3. Filtro Immobiliare/Commerciale (Contestuale)
        # Se parla di affitto/vendita MA NON di danni/bombe
        commercial_keywords = ["vendesi", "affittasi", "in vendita",
                               "immobiliare", "real estate", "sconto", "promo"]
        war_keywords = ["bomb", "missil", "colpit", "distrutto",
                        "esplosione", "strike", "attack", "damage"]

        if any(cw in t for cw in commercial_keywords):
            # Se è commerciale, lo salviamo SOLO se c'è una parola di guerra
            if not any(wk in t for wk in war_keywords):
                return True, "Commercial/Real Estate Spam"

        return False, None

    def _step_0_the_bouncer(self, text):
        print("   🛡️ Step 0: The Bouncer v2.0 analyzing...")

        # --- FASE 1: FILTRO MECCANICO (Gratis) ---
        is_junk, reason = self._is_obvious_junk(text)
        if is_junk:
            print(f"      🗑️ REJECTED by Regex Sentry: {reason}")
            return {"is_relevant": False, "reason": reason}

        # --- FASE 2: FILTRO SEMANTICO (AI) ---
        # Se siamo qui, il testo potrebbe essere valido. Chiediamo all'AI.

        # Tagliamo a 3000 caratteri (più contesto del precedente 2000)
        preview_text = text[:3000]

        prompt = f"""
        ROLE: Elite Military Intelligence Filter.
        TASK: Binary Classification (RELEVANT / IRRELEVANT).
        
        CONTEXT: We are tracking the Russia-Ukraine war. We need KINETIC EVENTS (Strikes, Battles, Movements) or SIGNIFICANT STRATEGIC NEWS.
        
        INPUT TEXT:
        "{preview_text}"

        ⚠️ CRITERIA FOR "IRRELEVANT" (Reject these):
        1. **General Politics:** "Putin signed a decree", "Zelensky met Biden" (UNLESS it involves immediate weapon delivery or escalation).
        2. **Opinion/Rants:** Telegram bloggers complaining without reporting a specific event.
        3. **Fundraising:** "Donate to this card", "Buy drones for our boys".
        4. **Generic News:** Sports, Weather, unrelated Crime.
        5. **Duplicate/Vague:** "Loud noises reported" (without location or confirmation).

        ✅ CRITERIA FOR "RELEVANT" (Keep these):
        1. **Kinetic Action:** Shelling, Explosions, Drone Strikes, Air Defense active.
        2. **Movement:** Troop columns, equipment transfer (trains/convoys).
        3. **Damage:** Infrastructure hit, power outages caused by strikes.
        4. **Logistics:** Bridges hit, Ammo depots destroyed.
        
        STRICT WORD LIMIT: The "reason" MUST be extremely short (5-10 words max).
        OUTPUT JSON: {{ "is_relevant": boolean, "confidence": float (0.0-1.0), "reason": "5-10 words explanation" }}
        """

        try:
            if not hasattr(self, 'router_client'):
                return {"is_relevant": True, "reason": "Client Error - Fallback"}

            response = self.router_client.chat.completions.create(
                model="qwen/qwen2.5-vl-32b-instruct",
                messages=[
                    {"role": "system", "content": "Output valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )

            content = response.choices[0].message.content.strip()

            # Pulizia standard
            if "```" in content:
                content = content.split("```json")[1].split("```")[0].strip(
                ) if "json" in content else content.split("```")[1].strip()

            data = json.loads(content)
            
            # Fail-safe: troncamento forzato se l'AI è prolissa (max 10 parole)
            if data.get('reason'):
                words = data['reason'].split()
                if len(words) > 10:
                    data['reason'] = " ".join(words[:10]) + "..."

            # Debug Log
            if data.get('is_relevant'):
                print(
                    f"      ✅ Bouncer Approved (Conf: {data.get('confidence')}): {data.get('reason')}")
            else:
                print(f"      ⛔ Bouncer Blocked: {data.get('reason')}")

            return data

        except Exception as e:
            print(f"      ⚠️ BOUNCER EXCEPTION: {e}")
            # In caso di dubbio (errore API), lasciamo passare per non perdere dati
            return {"is_relevant": True, "reason": "Error Fallback"}

    def _step_titan_classifier(self, text):
        """
        Chiama il modello Fine-Tuned per ottenere la classificazione precisa.
        """
        print("   ⚡ Step 1.5: Titan Fine-Tuned is classifying...")

        # System Prompt Rinforzato (Quello validato prima)
        system_prompt = """You are a military intelligence analyst. Output strict JSON.
CRITICAL CLASSIFICATION RULES:
1. NOISE FILTER: If the text is a summary, historical analysis, political opinion, or static map, classify as NULL.
2. MANOUVRE PRIORITY: If text mentions territorial change (captured, retreated, entered), classify as MANOUVRE.
3. SHAPING PRIORITY: Strikes on deep rear targets, capitals, infrastructure, logistics -> SHAPING (OFFENSIVE/COERCIVE).
4. ATTRITION: Only for static fighting/shelling."""

        try:
            response = self.openai_client.chat.completions.create(
                model="ft:gpt-4o-mini-2024-07-18:personal:osint-analyst-v4--clean:Cv5yHxTJ",  # IL TUO ID
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text[:15000]}
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            if "classification" in result:
                result["classification"] = result["classification"].upper()
            return result
        except Exception as e:
            print(f"   ⚠️ Titan Error: {e}")
            return {"classification": "UNKNOWN", "reasoning": "Error", "confidence": 0}

    # =========================================================================
    # 🧠 STEP 1: THE BRAIN (DeepSeek V3 via OpenRouter)
    # =========================================================================
    def _step_1_the_brain(self, text, metadata):
        """
        Role: Strategy & Context.
        Analysis of raw clusters to determine relevance, actors, and bias.
        """
        print("   🧠 Step 1: The Brain (DeepSeek V3) analyzing strategy...")

        brain_prompt = f"""
### SYSTEM INSTRUCTIONS: INTELLIGENCE JUDGE & CORRECTOR

**ROLE**
You are a Senior Intelligence Officer. Your task is to VALIDATE and CORRECT the raw extraction performed by a subordinate unit ("The Soldier") against raw intercepts.

**INPUT DATA**
1. **RAW SOURCE (Cluster):** "{text[:15000]}"
2. **SOLDIER'S EXTRACTION (To Verify):** {json.dumps(metadata)}
3. **CONTEXT NOTE:** The RAW TEXT below may contain multiple reports merged together (separated by '|||'). Treat them as corroborating sources for a single event.
RAW TEXT:
"{text[:5000]}"

**PROTOCOL 1: DATE & LOGIC VALIDATION (FLEXIBLE)**
   - Check the `Target Date` provided in metadata against the text.
   - **ALLOW:** +/- 7 days flexibility to account for delayed reporting, weekly summaries, or confirmation delays.
   - **REJECT:** Events clearly from a different month (unless month-end transition), previous years, "Anniversaries", "Recaps of the year".
   - *Logic:* If date mismatch > 7 days -> `verification_status: false`.

**PROTOCOL 2: VALIDATION & CORRECTION (THE FALLBACK)**
   - Compare `SOLDIER'S EXTRACTION` with `RAW SOURCE`.
   - **Location Check:** Does the location found by the Soldier match the text? If Soldier says "Odessa" but text says "Kyiv" -> **CORRECT IT**.
   - **Hallucination Check:** Did the Soldier invent coordinates (e.g. 0.0, 0.0) or numbers not in text? -> **CORRECT THEM** (set to null if not found).
   - **Missed Info:** If Soldier missed key details -> **ADD THEM**.

**PROTOCOL 3: VALIDATION & CORRECTION**
    - **Location Check:** Does the location found by the Soldier match the text?
    - **THE HEADQUARTERS TRAP:** If text says "Moscow reported..." or "Kyiv announced...", the event happened on the FRONT, NOT in the capital.
    -> IF Soldier put [55.75, 37.61] (Moscow) for a tank battle -> CHANGE TO `null` (Region Level).
    - **THE POLITICAL TRAP:** If event is about MONEY, AID, SANCTIONS, or DIPLOMACY (e.g., "Portugal sends funds"):
    -> SET `geo_location.explicit` to `null`.
    -> Political events DO NOT have precise coordinates.

**PROTOCOL 4: LOGICAL SANITY CHECK (CRITICAL)**
    - **Abrams/F-16 in Moscow?** IMPOSSIBLE. If a frontline weapon is destroyed in a capital city (far from front), it is a hallucination identifying the HQ instead of the battlefield. -> REMOVE COORDINATES.
    - **Generals Killed:** Only accept explicit coordinates if verified. Otherwise use City/Region level.



 **TARGET CLASSIFICATION:** Map target to exactly ONE category:
     * `REFINERY` (Fuel, Oil depots)
     * `ELECTRICAL_SUBSTATION` (Transformers, Grid - NOT Nuclear)
     * `INFRASTRUCTURE` (Bridges, Ports, Railways)
     * `MILITARY_BASE` (Airfields, Barracks, Ammo)
     * `CIVILIAN_FACILITY` (Schools, Hotels, Residential)
     * `CITY` (Generic city strike)
     * `REGION` (Wide area/Unknown)
     * `POLITICAL_EVENT` (Dichiarazioni, Incontri, Sanzioni)
     * `LOGISTICS_NON_KINETIC` (Sequestri, Blocchi doganali)

 **BIAS & SIGNAL:**
     * `BIAS SCORE`: Estimate political lean (-10 Pro-RU to +10 Pro-UA).
     * `IMPLICIT SIGNAL`: What is the tactical goal? (e.g. "Terror bombing", "Logistics").

     === 🧮 RELIABILITY SCORING ALGORITHM (STRICT) ===
        Start with BASE SCORE: 30
        Then ADD points for each condition met (Max 95):

        1. **CORROBORATION (+20):** - IF text contains "[MERGED" OR lists >1 distinct source URL -> ADD 20.
           - IF >3 distinct sources -> ADD 10 more.

        2. **VISUAL EVIDENCE (+20):**
           - IF text describes specific video/photo footage (e.g., "geolocated footage shows", "drone video captures") -> ADD 20.

        3. **CROSS-VERIFICATION (+30):**
           - IF sources include BOTH Pro-RU (e.g. Rybar, Two Majors) AND Pro-UA (e.g. DeepState, Sternenko) channels -> ADD 30.
           - IF confirmed by Neutral/Official source (e.g. ISW, MoD) -> ADD 30.

        4. **SPECIFICITY (+10):**
           - IF specific coordinates, unit names (e.g. "47th Brigade"), or exact equipment counts are provided -> ADD 10.

        *PENALTIES:*
        - IF tone is highly emotional/propagandistic -> SUBTRACT 10.
        - IF "unconfirmed" or "rumors" is explicitly stated -> SET SCORE TO MAX 30.

        OBJECTIVES:
        1. Calculate Reliability based exclusively on the algorithm above.
        2. Hallucination Check & Strategic Analysis.
        3. Bias Estimation (-10 to +10).

**OUTPUT SCHEMA (JSON ONLY)**
{{
    "verification_status": boolean,
    "rejection_reason": "null or string (e.g. 'Fundraising')",
    "correction_notes": "String explaining corrections (e.g. 'Fixed wrong Actor from UKR to RUS')",
    "verified_data": {{
        "actor": "RUS | UKR | UNK",
        "reliability_score": int (0-100, based on calculation),
            "reliability_reasoning": "string (Explain the math: 'Base 30 + 20 Visual + 10 Specificity')",
            "is_hallucination": boolean,
            "correction_notes": "string",
            "ai_bias_estimate": int (-10 to 10),
            "location_precision_category": "string (EXACT_COORDINATES, CITY_LEVEL, REGION_LEVEL)",
            "strategic_value_assessment": "string",
            "event_category": "string"
        "implicit_signal": "Tactical summary",
        "location_precision_category": "ENUM from Protocol 4",
        "corrected_coordinates": {{ "lat": float, "lon": float }} // Only if you found better ones
    }}
}}
"""

        try:
            # USIAMO IL MODELLO REASONER (V3.2 STANDARD)
            # Questo modello supporta JSON mode E ha il "Thinking Process"
            response = self.brain_client.chat.completions.create(
                model="deepseek/deepseek-v3.2",

                messages=[
                    {"role": "system", "content": "You are a strategic reasoning engine. Output valid JSON only."},
                    {"role": "user", "content": brain_prompt}
                ],

                # --- INTEGRAZIONE THINKING MODE ---
                extra_body={
                    "thinking": {
                        "type": "enabled",
                        "budget_tokens": 4096  # Limita il ragionamento per controllare i costi
                    }
                },

                temperature=0.0,
                response_format={"type": "json_object"}
            )

            # Parsing della risposta
            content = response.choices[0].message.content.strip()

            # Gestione markdown code blocks (DeepSeek a volte li mette anche in JSON mode)
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            brain_json = json.loads(content)

            # MAPPING INTELLIGENTE PER IL DB
            # DeepSeek ora calcola 'reliability_score', ma il DB potrebbe aspettarsi 'reliability' dentro 'scores'
            final_reliability = brain_json.get('reliability_score', 40)

            # (Opzionale) Stampa di debug per vedere se funziona
            print(
                f"      📊 Reliability Calcolata: {final_reliability}% ({brain_json.get('reliability_reasoning', '')})")

            # Assicurati che questo valore finisca nel JSON finale salvato nel DB
            # Se la tua struttura è complessa, potresti doverlo iniettare manualmente nel posto giusto
            brain_json['scores'] = {
                'reliability': final_reliability,
                # Assumi che il Brain calcoli anche questo o usa logica separata
                'intensity': brain_json.get('intensity', 0)
            }

            # --- SALVATAGGIO DEL "PENSIERO NASCOSTO" (AMNESIA FIX) ---
            try:
                msg = response.choices[0].message
                reasoning_trace = getattr(msg, 'reasoning_content', None)

                if reasoning_trace:
                    # Salviamo i primi 1500 caratteri per non appesantire troppo il DB
                    brain_json['_hidden_reasoning_trace'] = reasoning_trace[:1500] + "..."
            except Exception:
                pass  # Se non c'è il trace, pazienza

            return brain_json

        except Exception as e:
            print(f"      ❌ Brain Malfunction: {e}")
            # Fallback sicuro in caso di crash
            return {
                "is_hallucination": False,
                "ai_bias_estimate": 0,
                "location_precision_category": "UNKNOWN",
                "strategic_value_assessment": f"Error in Brain processing: {str(e)}",
                "event_category": "UNCERTAIN"
            }

    # =========================================================================
    # 🔧 AI MECHANIC: JSON REPAIR (GPT-4o-mini)
    # =========================================================================
    def _repair_json_with_ai(self, broken_text, error_context):
        """
        Chiama un modello veloce (GPT-4o-mini) per correggere errori di sintassi JSON.
        """
        print(f"   🔧 Activating JSON Mechanic (Error: {error_context})...")

        repair_prompt = f"""
        TASK: Fix the malformed JSON string below.
        ERROR: {error_context}

        RULES:
        1. Return ONLY the valid JSON object.
        2. Do not add markdown backticks.
        3. Fix syntax errors (missing brackets, quotes, trailing commas).
        4. Maintain all data fields exactly as they are.

        BROKEN JSON:
        {broken_text}
        """

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": repair_prompt}],
                temperature=0.0
            )
            fixed_text = response.choices[0].message.content.strip()

            # Rimuoviamo eventuali backticks residui
            if "```" in fixed_text:
                fixed_text = fixed_text.replace(
                    "```json", "").replace("```", "").strip()

            return json.loads(fixed_text)

        except Exception as e:
            print(f"   ❌ JSON Mechanic Failed: {e}")
            return None

    # =========================================================================
    # 🧮 LAYER 1 ENGINE: T.I.E. CALCULATOR
    # =========================================================================
    def _calculate_tie(self, titan_data, visual_confirmed):
        """
        Calculates the Target Impact Estimate (T.I.E.) based on TITAN-10 vectors.
        Returns Dictionary with Value and Status.
        """
        # 1. Sanity Check & Clamping (1-10)
        try:
            k = max(1, min(10, int(titan_data.get('kinetic_score', 1))))
            t = max(1, min(10, int(titan_data.get('target_score', 1))))
            e = max(1, min(10, int(titan_data.get('effect_score', 1))))
        except:
            k, t, e = 1, 1, 1

        # 2. PROTOCOL "DEFERRED" (Sospensione del Giudizio)
        # Se l'effetto è basso/ignoto (<=2) E non c'è video, non diamo un voto alto.
        # "Meglio un buco che una bugia".
        if e <= 2 and not visual_confirmed:
            return {
                "value": 0,
                "status": "DEFERRED",
                "reason": "Low effect confidence & No visual evidence",
                "vectors": {"k": k, "t": t, "e": e}
            }

        # 3. CALCOLO MATEMATICO T.I.E.
        # Formula: (Target^1.6) * (Effect / 10) -> Il "COSA" pesa più del "COME".
        # Esempio: Target 10 (S-400), Effect 10 -> 10^1.6 (39.8) * 1.0 = 39.8
        # Esempio: Target 2 (Casa), Effect 10 -> 2^1.6 (3.0) * 1.0 = 3.0
        strategic_weight = (pow(t, 1.6)) * (e / 10.0)

        # Fattore Cinetico Logaritmico (Moltiplicatore di scala)
        # K=1 -> 1.0 | K=10 -> 2.15
        kinetic_mult = 1.0 + (math.log(k) / 2.0)

        # Calcolo Raw (Fattore 2.5 per scalare verso 100)
        raw_tie = strategic_weight * kinetic_mult * 2.5

        # 4. BONUS CONTESTUALI
        if titan_data.get('is_deep_strike'):
            raw_tie *= 1.25  # Deep strike vale di più (logistica/rischio)

        if visual_confirmed:
            raw_tie *= 1.10  # Bonus affidabilità

        # Cap a 100
        final_value = int(min(100, raw_tie))

        return {
            "value": final_value,
            "status": "VALID",
            "reason": "Sufficient data points",
            "vectors": {"k": k, "t": t, "e": e}
        }

    # =========================================================================
    # 🤖 STEP 2: THE SOLDIER v2.1 (With Auto-Repair)
    # =========================================================================

    def _step_2_the_soldier(self, cluster_data):
        """
        Role: Strict Extraction from Cluster with Fallback Repair.
        """
        print("   🤖 Step 2: The Soldier analyzing cluster...")

        messages_list = cluster_data.get('raw_messages', [])
        if not messages_list:
            return None

        combined_text = "\n--- NEW SOURCE MESSAGE ---\n".join(messages_list)
        ref_time = cluster_data.get(
            'reference_timestamp') or datetime.now().isoformat()

        user_content = f"""
        REFERENCE TIMESTAMP: {ref_time}
        CLUSTER DATA:
        {combined_text[:25000]}
        """

        raw_response_text = ""

        try:
            # 1. Chiamata Principale (Qwen/DeepSeek)
            response = self.openrouter_client.chat.completions.create(
                model="qwen/qwen-2.5-72b-instruct",
                messages=[
                    {"role": "system", "content": SOLDIER_SYSTEM_PROMPT},
                    {"role": "user", "content": user_content}
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            raw_response_text = response.choices[0].message.content

            # 2. Parsing Standard
            parsed_data = self._clean_and_parse_json(raw_response_text)

            # 3. AUTO-REPAIR CHECK
            # Se _clean_and_parse_json ritorna None, significa che il JSON è rotto.
            if not parsed_data:
                print("   ⚠️ JSON Syntax Error detected. Calling Mechanic...")
                parsed_data = self._repair_json_with_ai(
                    raw_response_text, "Invalid JSON format")

            # Se anche il meccanico fallisce, ci arrendiamo
            if not parsed_data:
                print("   ❌ Soldier Failed (Unfixable JSON).")
                return None

            # 4. SAFETY LAYER (Validazione Coordinate)
            geo = parsed_data.get("geo_location", {})
            explicit = geo.get("explicit")
            if explicit:
                lat = explicit.get('lat')
                lon = explicit.get('lon')
                is_invalid_lat = lat in [0, 0.0, "0", None, "null"]
                is_invalid_lon = lon in [0, 0.0, "0", None, "null"]

                if is_invalid_lat or is_invalid_lon:
                    parsed_data["geo_location"]["explicit"] = None

                    # 5. SANITY CHECK SUL NOME CITTÀ (Inferred)
            # Se l'AI ha scritto "Kyiv, Ukraine" o "Kyiv, Lviv", teniamo solo la prima parte.
            try:
                raw_loc = parsed_data.get("geo_location", {}).get(
                    "inferred", {}).get("toponym_raw")
                if raw_loc and isinstance(raw_loc, str):
                    # Pulisce liste (prende il primo elemento)
                    if "," in raw_loc:
                        clean_loc = raw_loc.split(",")[0].strip()
                        parsed_data["geo_location"]["inferred"]["toponym_raw"] = clean_loc

                    # Pulisce liste con 'and' (es. "Kyiv and Lviv")
                    if " and " in raw_loc.lower():
                        clean_loc = raw_loc.lower().split(
                            " and ")[0].strip().title()  # Rimette maiuscola
                        parsed_data["geo_location"]["inferred"]["toponym_raw"] = clean_loc
            except:
                pass

            return parsed_data

        except Exception as e:
            print(f"   ⚠️ Soldier Exception: {e}")
            # Tentativo disperato di riparazione se l'errore è avvenuto durante il parsing iniziale
            if raw_response_text:
                print("   🔧 Attempting emergency repair on raw text...")
                return self._repair_json_with_ai(raw_response_text, str(e))
            return None

    # =========================================================================
    # 🌍 STEP GEO-VERIFIER: Anti-Hallucination Geolocation Validator
    # =========================================================================

    # Liste di riferimento per il Sanity Check
    SUSPICIOUS_CAPITALS = ["Moscow", "Kyiv", "Kiev", "Washington", "London", 
                           "Brussels", "Beijing", "Ankara", "Tehran", "Minsk",
                           "Kremlin", "White House", "Pentagon"]
    
    FRONTLINE_KEYWORDS = ["front", "frontline", "line of contact", "trench", 
                          "mortar", "grad", "howitzer", "artillery", "dugout",
                          "assault", "infantry", "mechanized", "trenchline",
                          "фронт", "окоп", "передова", "лінія зіткнення"]

    def _step_geo_verifier(self, location_name: str, context_text: str):
        """
        🌍 GEO-VERIFIER: Validates and corrects geolocation extracted by Soldier.
        
        Protects against:
        1. Metonymy Errors ("Moscow says" != "Strike on Moscow")
        2. Typos / OCR Errors
        3. Ambiguous Places (Multiple cities with same name)
        
        Returns: dict with {'lat': float, 'lon': float} or None
        """
        from geopy.geocoders import Nominatim
        import time
        
        if not location_name or not isinstance(location_name, str):
            return None
        
        location_name = location_name.strip()
        print(f"      🌍 Geo-Verifier: Validating '{location_name}'...")
        
        # =====================================================================
        # STEP 1: SANITY CHECK (Local Python Logic - Zero API Cost)
        # =====================================================================
        clean_loc_lower = location_name.lower()
        is_suspicious = any(cap.lower() in clean_loc_lower for cap in self.SUSPICIOUS_CAPITALS)
        
        if is_suspicious:
            # Check if context implies frontline combat (metonymy detection)
            context_lower = context_text.lower()
            is_frontline_event = any(kw in context_lower for kw in self.FRONTLINE_KEYWORDS)
            
            if is_frontline_event:
                print(f"      ⚠️ METONYMY DETECTED: '{location_name}' mentioned but context is frontline combat.")
                print(f"         → Skipping capital city. Triggering AI correction...")
                
                # Trigger AI correction to find the REAL target
                corrected_location = self._ai_correct_location(location_name, context_text)
                if corrected_location:
                    location_name = corrected_location
                    print(f"      ✅ AI Corrected Location: '{corrected_location}'")
                else:
                    print(f"      ❌ AI could not determine real location. Returning None.")
                    return None
        
        # =====================================================================
        # STEP 2: GEOPY LOOKUP (Get Candidates)
        # =====================================================================
        geolocator = Nominatim(user_agent="osint-tracker-geo-verifier/1.0", timeout=10)
        
        try:
            # Search with priority for UA/RU
            candidates = geolocator.geocode(
                location_name, 
                exactly_one=False, 
                limit=5,
                country_codes=['ua', 'ru']
            )
            
            # Fallback: global search if no results in UA/RU
            if not candidates:
                candidates = geolocator.geocode(
                    location_name,
                    exactly_one=False,
                    limit=5
                )
                
            if not candidates:
                print(f"      ⚠️ Geopy: No results for '{location_name}'")
                return None
                
            # Format candidates for AI
            candidates_list = []
            for i, loc in enumerate(candidates):
                candidates_list.append({
                    'id': i,
                    'display_name': loc.address,
                    'lat': loc.latitude,
                    'lon': loc.longitude
                })
            
            print(f"      📍 Geopy: Found {len(candidates_list)} candidates")
            
        except Exception as e:
            print(f"      ❌ Geopy Error: {e}")
            return None
        
        # =====================================================================
        # STEP 3: AI RERANKING & VALIDATION (DeepSeek Call)
        # =====================================================================
        if len(candidates_list) == 1:
            # Single result - verify it's within war zone
            result = candidates_list[0]
            if self._is_in_war_zone(result['lat'], result['lon']):
                return {'lat': result['lat'], 'lon': result['lon']}
            else:
                print(f"      ⚠️ Single result outside war zone. Rejecting.")
                return None
        
        # Multiple candidates - use AI to pick the best one
        try:
            rerank_result = self._ai_rerank_geo_candidates(
                location_name, context_text, candidates_list
            )
            
            if not rerank_result:
                # Fallback: use first result in war zone
                for c in candidates_list:
                    if self._is_in_war_zone(c['lat'], c['lon']):
                        print(f"      📍 Fallback: Using first war-zone candidate")
                        return {'lat': c['lat'], 'lon': c['lon']}
                return None
            
            # Handle WRONG_EXTRACTION response
            if rerank_result.get('status') == 'WRONG_EXTRACTION':
                corrected_name = rerank_result.get('correct_name')
                if corrected_name and corrected_name != location_name:
                    print(f"      🔄 AI says wrong target. Re-geocoding: '{corrected_name}'")
                    time.sleep(0.5)  # Rate limiting
                    return self._step_geo_verifier(corrected_name, context_text)
                return None
            
            # Handle selected_id response
            selected_id = rerank_result.get('selected_id')
            if selected_id is not None and 0 <= selected_id < len(candidates_list):
                chosen = candidates_list[selected_id]
                print(f"      ✅ AI Selected: {chosen['display_name'][:50]}...")
                return {'lat': chosen['lat'], 'lon': chosen['lon']}
            
        except Exception as e:
            print(f"      ⚠️ AI Reranking Error: {e}")
        
        # Ultimate fallback
        for c in candidates_list:
            if self._is_in_war_zone(c['lat'], c['lon']):
                return {'lat': c['lat'], 'lon': c['lon']}
        
        return None
    
    def _is_in_war_zone(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within the Ukraine/Russia war zone bounding box."""
        MIN_LAT, MAX_LAT = 44.0, 60.0
        MIN_LON, MAX_LON = 22.0, 55.0
        return MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON
    
    def _ai_correct_location(self, wrong_location: str, context_text: str) -> str:
        """
        Uses DeepSeek to extract the REAL kinetic target from context
        when a metonymy error is detected.
        """
        prompt = f"""
CONTEXT: {context_text[:3000]}

The extraction agent identified "{wrong_location}" as the target location.
However, this appears to be a METONYMY ERROR (e.g., "Moscow reports..." != strike ON Moscow).

TASK: Identify the ACTUAL kinetic target location mentioned in the text.
Look for:
- City/town names near the frontline
- Oblast/region names
- Specific facilities (airports, depots, bases)

OUTPUT FORMAT (JSON only):
{{"correct_location": "Actual City/Place Name"}}

If you cannot determine the real location, output:
{{"correct_location": null}}
"""
        try:
            response = self.brain_client.chat.completions.create(
                model="deepseek/deepseek-chat-v3-0324",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            return result.get('correct_location')
        except Exception as e:
            print(f"      ❌ AI Correction Error: {e}")
            return None
    
    def _ai_rerank_geo_candidates(self, location_name: str, context_text: str, 
                                   candidates: list) -> dict:
        """
        Uses DeepSeek to select the best geolocation candidate based on context.
        """
        candidates_json = json.dumps(candidates, ensure_ascii=False, indent=2)
        
        prompt = f"""
CONTEXT: {context_text[:2500]}

The extraction agent identified target: "{location_name}"
Geopy found these candidates:
{candidates_json}

TASK:
1. VERIFY: Is "{location_name}" the ACTUAL kinetic target in the text? 
   Or is it just a government/source announcing something?
2. IF WRONG TARGET: Output {{"status": "WRONG_EXTRACTION", "correct_name": "Actual Place Name"}}
3. IF CORRECT TARGET: Pick the best Candidate ID from the list based on:
   - Proximity to known frontline areas
   - Match with context (oblast mentioned, nearby landmarks)
   - Preference for Ukrainian/Russian locations over global matches

OUTPUT (JSON only, no explanation):
{{"selected_id": <0-4>}} 
OR 
{{"selected_id": null}} if none match
OR 
{{"status": "WRONG_EXTRACTION", "correct_name": "..."}}
"""
        try:
            response = self.brain_client.chat.completions.create(
                model="deepseek/deepseek-chat-v3-0324",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"      ❌ AI Rerank Error: {e}")
            return None

    # 🧮 STEP 3: THE CALCULATOR (Python Deterministic Engine)

    def _clean_and_parse_json(self, raw_text):
        """
        Pulisce l'output dell'AI cercando il primo '{' e l'ultimo '}'
        per ignorare testo introduttivo o conclusivo.
        """
        try:
            # 1. Pulizia base
            text = raw_text.strip()

            # 2. Rimuove i backticks del markdown (es. ```json ... ```)
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]

            # 3. Estrazione Chirurgica: Cerca la prima { e l'ultima }
            start = text.find('{')
            end = text.rfind('}')

            if start != -1 and end != -1:
                text = text[start: end + 1]

            # 4. Parsing
            return json.loads(text)

        except json.JSONDecodeError as e:
            print(f"   ⚠️ JSON Parsing Error: {e}")
            # Debug per vedere cosa ha scritto
            print(f"   RAW OUTPUT: {raw_text[:100]}...")
            return None

    def _step_3_the_calculator(self, soldier_data, brain_data, source_name, text):
        """
        Role: Pure Math & Multi-Source Aggregation.
        Uses DIRECT LOOKUP + AMPLIFIED MODIFIERS with HARD CAP at 1.0.
        """
        print("   🧮 Step 3: The Calculator (Amplified Physics Engine)...")

        # --- 1. INTENSITY CALCULATION ---
        target_data = soldier_data.get("actors", {}).get("target", {})

        # Le chiavi arrivano dirette dal Soldato (es. "MIL_AMMO_DEPOT")
        raw_type = target_data.get("type", "UNKNOWN")
        raw_damage = target_data.get("status_after_event", "UNKNOWN")

        # Fallback intelligente: Se il soldato sbaglia e inventa una chiave non nel DB
        if raw_type not in INTENSITY_DB:
            # Mappiamo le categorie del Brain sulle tue chiavi DB come salvagente
            brain_cat = brain_data.get("location_precision_category")
            brain_map = {
                "REFINERY": "INFRA_REFINERY",
                "MILITARY_BASE": "MIL_AIRBASE",
                "ELECTRICAL_SUBSTATION": "INFRA_GRID_LOCAL",
                "INFRASTRUCTURE": "INFRA_LOGISTICS"
            }
            # Se nemmeno il brain aiuta, usiamo UNKNOWN (che ora vale 0.2 se hai aggiornato il DB, o 0.0)
            raw_type = brain_map.get(brain_cat, "UNKNOWN")

        # A. PRELIEVO VALORI
        # Usa 0.2 come default se la chiave non esiste, per non avere zeri brutti
        v_target = INTENSITY_DB.get(raw_type, 0.2)
        m_damage = DAMAGE_MODIFIERS.get(
            raw_damage, 1.0)  # Default 1.0 (neutro)

        # B. CALCOLO CON CAP A 1.0
        # Esempio: Deposito (0.75) * Critical (1.5) = 1.125 -> Diventa 1.0
        # Esempio: Deposito (0.75) * Heavy (1.2) = 0.9 -> Resta 0.9
        raw_intensity = v_target * m_damage
        intensity_score = round(min(1.0, raw_intensity), 2)

        # Debug per verifica
        print(
            f"      🔧 CALC: {raw_type}({v_target}) x {raw_damage}({m_damage}) = {raw_intensity:.2f} -> Capped: {intensity_score}")

        # 2. SOURCE LOOKUP & AGGREGATION (FIX CHIRURGICO PER LISTE)
        if isinstance(source_name, list):
            sources_to_check = source_name
        else:
            sources_to_check = [str(source_name)] if source_name else []

        total_reliability = 0
        total_bias = 0
        valid_sources_count = 0

        # Ciclo su tutte le fonti per fare la media
        for src in sources_to_check:
            # Normalize source name for DB lookup
            src_clean = src.lower().strip().replace(
                'www.', '').replace('https://', '').split('/')[0]

            source_data = self.sources_db.get(src_clean)

            if not source_data:
                # Fuzzy fallback
                for k, v in self.sources_db.items():
                    if k in src_clean:
                        source_data = v
                        break

            # Se ancora non trovata, valori default
            if not source_data:
                current_rel = 40  # Default Tier D
                current_bias = 0
            else:
                current_rel = source_data.get("reliability", 40)
                current_bias = source_data.get("bias", 0)

            total_reliability += current_rel
            total_bias += current_bias
            valid_sources_count += 1

        # Calcolo Medie (Base Score)
        if valid_sources_count > 0:
            avg_base_reliability = total_reliability / valid_sources_count
            avg_base_bias = total_bias / valid_sources_count
        else:
            avg_base_reliability = 40
            avg_base_bias = 0

        # 3. RELIABILITY CALCULATION (4 Factors)
        # Factor A: Base Score (Ora usiamo la media calcolata sopra)
        r_base = avg_base_reliability

        # Factor B: Visual Evidence (+20%) (INVARIATO)
        r_visual = 20 if soldier_data.get("visual_evidence") else 0

        # Factor C: Semantic Penalty (-25%) (INVARIATO)
        speculative_words = ["rumor", "unconfirmed",
                             "allegedly", "possibly", "claimed"]
        text_lower = text.lower()
        r_semantic = - \
            25 if any(w in text_lower for w in speculative_words) else 0

        # Factor D: Corroboration (FIX: Cluster Bonus)
        # Se c'è più di 1 fonte, diamo un bonus (+10 per ogni fonte extra, max 20)
        r_corroboration = 0
        if valid_sources_count > 1:
            r_corroboration = min(20, (valid_sources_count - 1) * 10)

        final_reliability = max(
            0, min(100, int(r_base + r_visual + r_semantic + r_corroboration)))

        # 4. BIAS CALCULATION (HBC Formula)
        # B_base: Source Bias (Ora usiamo la media calcolata sopra)
        b_base = avg_base_bias

        # S_ai: Brain Estimate (-10 to +10) (INVARIATO)
        s_ai = brain_data.get("ai_bias_estimate", 0)

        # S_sem: Semantic Keyword Scoring (INVARIATO)
        s_sem_raw = 0
        for keyword, score in self.keywords_db.items():
            val = score.get('score', 0) if isinstance(score, dict) else score
            if keyword.lower() in text_lower:
                s_sem_raw += val

        # Clamp S_sem between -10 and 10 for safety
        s_sem = max(-10, min(10, s_sem_raw))

        # M_rel: Reliability Multiplier (INVARIATO)
        m_rel = max(0.2, 1.2 - (final_reliability / 100.0))

        # Final Formula (INVARIATO - usa le nuove variabili medie)
        raw_bias_score = (b_base * 2 * 0.4) + (s_ai * 0.4) + (s_sem * m_rel)
        final_bias_score = round(max(-10, min(10, raw_bias_score)), 1)

        # Labeling (INVARIATO)
        if final_bias_score <= -3:
            dom_bias = "Pro-Russia"
        elif final_bias_score >= 3:
            dom_bias = "Pro-Ukraine"
        else:
            dom_bias = "Neutral"

        return {
            "intensity": intensity_score,
            "reliability": final_reliability,
            "bias_score": final_bias_score,
            "dominant_bias": dom_bias
        }

    # =========================================================================
    # 📰 STEP 4: THE JOURNALIST (GPT-4o-mini via OpenAI)
    # =========================================================================
    def _step_4_the_journalist(self, text, brain_data, soldier_data):
        """
        Role: Description & Title.
        Generates Master English content and translates to Italian.
        Strictly enforces NEUTRAL, ASEPTIC, UN-BIASED terminology.
        """
        print("   📰 Step 4: The Journalist (4o-mini) writing neutral summary...")

        # Recuperiamo chi sono gli attori per aiutare l'AI a non confondersi
        aggressor = soldier_data.get('actors', {}).get(
            'aggressor', {}).get('side', 'Unknown')
        target = soldier_data.get('actors', {}).get(
            'target', {}).get('side', 'Unknown')

        prompt = f"""
        ROLE: You are a historical archivist for the United Nations (UN).
        Your job is to rewrite raw, biased war reports into NEUTRAL, FACTUAL database entries.

        INPUT CONTEXT (Raw Telegram Text):
        {text[:2000]}

        DETECTED ACTORS:
        - Aggressor Side: {aggressor}
        - Target Side: {target}

        ⚠️ "DE-BIASING" RULES (STRICT):
        1. **SOURCE BIAS REMOVAL:** The source text is BIASED (e.g., Ukrainian sources call Russians "The Enemy", "Orcs", "Occupiers").
           - YOU MUST REPLACE "The Enemy" with the specific army name (e.g., "Russian Forces").
           - YOU MUST REPLACE "Our troops" with "Ukrainian Forces".

        2. **FORBIDDEN WORDS (Blacklist):**
           - NEVER use: "Enemy", "Foe", "Hero", "Terrorist", "Liberated", "Glorious", "Horde", "Criminals".
           - USE INSTEAD: "Adversary forces", "Personnel", "Retook control", "Advanced", "Group", "Units".

        3. **TONE:** Cold, clinical, robotic. No adjectives like "Brutal", "Massive", "Cynical". Just numbers and facts.

        4. **TRANSLATION RULE (ITALIAN):**
           - "Enemy" -> "Forze Russe" (o "Forze Ucraine" based on context). NEVER "Il nemico".
           - "Our defenders" -> "Le forze di difesa ucraine".

        OUTPUT REQUIREMENTS:
        1. **Title (EN):** [Who] [Action] [Where]. (e.g. "Russian Infantry Attack Repelled near Sotnytskyi Kozachok").
        2. **Description (EN):** Max 80 words. Focus on kinetics: movements, clashes, casualties.
        3. **Italian Translation:** RIGOROUSLY NEUTRAL.

        OUTPUT JSON:
        {{
            "title_en": "String",
            "description_en": "String",
            "title_it": "String",
            "description_it": "String"
        }}
        """

        try:
            # 1. Chiamata API (Temperatura 0 = Robotico)
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system",
                        "content": "You are a neutral database engine. JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0
            )

            # 2. Parsing
            result_text = response.choices[0].message.content
            parsed_data = self._clean_and_parse_json(result_text)

            if not parsed_data:
                return self._get_error_journalist_response()

            # 3. BARRIERA MECCANICA (Python Post-Processing)
            # Funzione interna di pulizia
            def sanitize_string(s):
                if not s:
                    return ""
                replacements = {
                    "il nemico": "le forze avversarie",
                    "del nemico": "delle forze avversarie",
                    "al nemico": "alle forze avversarie",
                    "col nemico": "con le forze avversarie",
                    "i nostri": "le truppe ucraine",
                    "le nostre": "le truppe ucraine",
                    "liberato": "preso il controllo di",
                    "orchi": "soldati russi",
                    "terroristi": "incursori"
                }
                s_lower = s.lower()
                for bad, good in replacements.items():
                    if bad in s_lower:
                        pattern = re.compile(re.escape(bad), re.IGNORECASE)
                        s = pattern.sub(good, s)
                return s

            # Applica sanitizzazione ai campi italiani
            parsed_data['title_it'] = sanitize_string(
                parsed_data.get('title_it', ''))
            parsed_data['description_it'] = sanitize_string(
                parsed_data.get('description_it', ''))

            return parsed_data

        except Exception as e:
            print(f"   ❌ Journalist Critical Error: {e}")
            return self._get_error_journalist_response()

    def _get_error_journalist_response(self):
        return {
            "title_en": "Event Processing Error",
            "description_en": "Data could not be summarized neutrally.",
            "title_it": "Errore Elaborazione Evento",
            "description_it": "Impossibile riassumere i dati in modo neutrale."
        }

# =========================================================================
# 🏰 STEP 5: THE STRATEGIST (DeepSeek-V3 via OpenRouter)
# =========================================================================


def _step_5_the_strategist(client_or, final_report):
    """
    THE STRATEGIST (DeepSeek-V3.2 via OpenRouter)
    Generates high-level strategic insight in EN and IT.
    """
    print("   ♟️  Step 5: The Strategist is assessing impact (Dual Lang)...")

    # 1. Prepare Data
    editorial = final_report.get('editorial', {})
    metrics = final_report.get('titan_metrics', {})
    strategy = final_report.get('strategy', {})
    
    # [FIX] Recuperiamo i dati tattici dal Soldier (poiché titan_analysis è stato rimosso)
    tactics = final_report.get('tactics', {})
    
    # Tactical Dossier ARRICCHITO
    dossier = f"""
    EVENT: {editorial.get('title_en')}
    CONTEXT: {editorial.get('description_en')}
    
    === TACTICAL CLASSIFICATION ===
    CATEGORY: {tactics.get('target_category', 'UNKNOWN')}
    REASONING: {tactics.get('target_status', 'N/A')}
    
    T.I.E. METRICS (0-10):
    - KINETIC (Violence/Intensity): {metrics.get('kinetic_score')}
    - TARGET (Strategic Value): {metrics.get('target_score')}
    - EFFECT (Success/Damage): {metrics.get('effect_score')}
    """

    # 2. The Prompt (English for better reasoning)
    system_prompt = """
    You are a Senior Intelligence Analyst for a conflict monitor.
    Your task: Generate a "Strategic Assessment" for the provided event.

    CRITICAL RULES:
    1. NO SUMMARIES. Do not repeat what happened. Focus strictly on "So What?".
    2. ANALYZE CONSEQUENCES. Explain the operational or strategic implication.
    3. USE METRICS. Use the T.I.E. scores to guide your assessment.
    4. GLOBAL CONTEXT. Mention how this fits into the broader war.
    5. BREVITY. Maximum 3 sentences per language. Tone: Cold, Professional, Direct.

    OUTPUT FORMAT (Strictly follow this):
    [EN] <Insight in English>
    [IT] <Insight in Italian>
    """

    try:
        response = client_or.chat.completions.create(
            model="deepseek/deepseek-chat",  # [FIX] Nome modello corretto (v3 standard)
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": dossier}
            ],
            temperature=0.1,
            max_tokens=100
        )

        insight_raw = response.choices[0].message.content.strip()
        print(f"      🧠 Strategist Output:\n{insight_raw}")
        return insight_raw

    except Exception as e:
        print(f"      ⚠️ Strategist Error: {e}")
        return "[EN] Analysis unavailable.\n[IT] Analisi non disponibile."

    # =========================================================================
    # 🔄 MAIN PROCESS FLOW
    # =========================================================================


def perform_search(self, query, event_date_str=None):
    """
    Motore Google (Serper) con TIME MACHINE BLINDATA.
    Se la data non è leggibile, ABORTISCE la ricerca per sicurezza.
    """
    if not self.serper_api_key:
        print("   ❌ Errore: SERPER_API_KEY mancante.")
        return "", "unknown", []

    url = "https://google.serper.dev/search"
    date_filter = ""

    # --- LOGICA DI SICUREZZA TEMPORALE ---
    if event_date_str:
        # 1. Pulizia preliminare della data
        clean_date = str(event_date_str).strip().replace(
            '.', '/').replace('-', '/')
        dt = None

        # 2. Lista estesa di formati per non fallire
        formats = [
            '%d/%m/%Y',  # 25/12/2023
            '%Y/%m/%d',  # 2023/12/25
            '%d-%m-%Y',  # 25-12-2023
            '%d/%m/%y',  # 25/12/23
            '%Y%m%d',    # 20231225
            '%m/%d/%Y',  # 12/25/2023 (US style)
            '%d %b %Y'   # 25 Dec 2023
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(clean_date, fmt)
                break  # Trovato!
            except ValueError:
                continue

        if dt:
            # DATA VALIDA -> Attivo Time Machine
            start_date = (dt - timedelta(days=30)).strftime('%Y-%m-%d')
            end_date = (dt + timedelta(days=30)).strftime('%Y-%m-%d')
            date_filter = f" after:{start_date} before:{end_date}"
        else:
            # DATA INVALIDA -> SAFETY STOP!
            # Questo impedisce di cercare "a caso" e trovare news sbagliate
            print(
                f"   🛑 DATA ILLEGIBILE: '{event_date_str}' -> BLOCCO RICERCA.")
            return "", "skipped_bad_date", []

    # Se non c'è proprio la data nel DB (None), è rischioso cercare.
    # Decommenta le due righe sotto se vuoi bloccare anche questi casi:
    # else:
    #     return "", "skipped_no_date", []

    final_query = f"{query}{date_filter}"
    print(f"   🗓️ Time Machine: '{final_query}'")

    payload = json.dumps({
        "q": final_query,
        "num": 10,
        "gl": "us",      # US per indice globale
        "hl": "en"       # Inglese per max compatibilità
    })

    headers = {
        'X-API-KEY': self.serper_api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request(
            "POST", url, headers=headers, data=payload)
        results = response.json()

        candidates = results.get("news", []) + results.get("organic", [])

        if not candidates:
            return "", "unknown", []

        text_snippets = []
        urls = []

        for item in candidates[:5]:
            link = item.get('link')
            snippet_date = item.get('date', 'Unknown Date')
            title = item.get('title', '')
            body = item.get('snippet', '')

            entry = f"SOURCE: {link}\nGOOGLE_DATE: {snippet_date}\nTITLE: {title}\nTEXT: {body}\n---"
            text_snippets.append(entry)
            urls.append(link)

        return "\n".join(text_snippets), (urls[0] if urls else "unknown"), urls

    except Exception as e:
        print(f"   ❌ Serper Error: {e}")
        return "", "unknown", []

    # ... (questo è l'ultimo metodo corretto della classe)


def parse_date_strict(self, date_str):
    """
    Tenta di interpretare la data con ogni formato umanamente possibile.
    Se fallisce, restituisce None (segnale di STOP).
    """
    if not date_str:
        return None

    date_str = str(date_str).strip().replace('.', '/').replace('-', '/')

    # Lista estesa dei formati accettati
    formats = [
        '%d/%m/%Y',  # 25/12/2023
        '%Y/%m/%d',  # 2023/12/25
        '%d/%m/%y',  # 25/12/23
        '%m/%d/%Y',  # 12/25/2023 (US)
        '%Y%m%d',    # 20231225
        '%d %b %Y',  # 25 Dec 2023
        '%d %B %Y'   # 25 December 2023
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


def fetch_url_text(self, url):
    """Scrape specific URL"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            return ' '.join([p.get_text() for p in soup.find_all(['p', 'h1', 'h2'])])[:6000]
    except Exception:
        pass
    return None

    # =========================================================================
    # 🎯 HELPER: CLASSIFICAZIONE TIPO ATTACCO (Priorità Cinetica)
    # =========================================================================
    # NOTA: Ho aggiunto l'indentazione qui sotto per farlo rientrare nella classe


def _classify_event_type(self, text):
    """
    Determina il "Type" basandosi su regex keywords (Priorità Utente).
    """
    if not text:
        return "Unknown"
    t = text.lower()

    # --- PRIORITÀ 1: EVENTI MILITARI CINETICI ---
    if re.search(r"naval|sea|ship|boat|maritime|vessel", t):
        return "Naval Engagement"
    if re.search(r"drone|uav|loitering|kamikaze|quadcopter|unmanned", t):
        return "Drone Strike"
    if re.search(r"missile|rocket|ballistic|cruise|himars|mlrs", t):
        return "Missile Strike"
    if re.search(r"air|jet|plane|bombing|airstrike|su-", t):
        return "Airstrike"
    if re.search(r"artillery|shelling|mortar|howitzer|grad|cannon", t):
        return "Artillery Shelling"
    if re.search(r"ied|mine|landmine|vbied|explosion|trap", t):
        return "IED / Explosion"
    if re.search(r"clash|firefight|skirmish|ambush|raid|attack|ground|shooting|sniper", t):
        return "Ground Clash"

    # --- PRIORITÀ 2: CONTESTO CIVILE E POLITICO ---
    if re.search(r"politic|protest|riot|demonstration|diploma|unrest|arrest", t):
        return "Political / Unrest"

    # --- PRIORITÀ 3: CIVIL / ACCIDENT ---
    if re.search(r"civil|accident|crash|fire|infrastructure|logistics|humanitarian", t):
        return "Civil / Accident"

    return "Others"

    # ---------------------------------------------------------
    # 1. SMART QUERY (La funzione che hai appena modificato)
    # ---------------------------------------------------------


def _generate_event_fingerprints(self, title, location, date_str):
    """
    Analizza l'evento per estrarre 'Impronte Digitali' uniche (Fingerprints)
    invece di keyword generiche.
    """
    system_prompt = """
        You are an elite OSINT Analyst specializing in the Ukraine War (2022-2025).
        Your goal is to extract SEARCH FINGERPRINTS to find a specific historical event.

        DEFINITION:
        Search fingerprints are highly specific, low-noise elements that uniquely identify an event and survive reposting.
        They are NOT generic words like "battle", "shelling", or "attack".
        Focus on:
        - Units (e.g., "72nd Brigade", "DPR Battalion")
        - NO: "Battle", "Attack", "War" (Too generic)
        - YES: "72nd Brigade", "Tochka-U", "Pontoon bridge", "School No. 3", "Mayor Fedorov"

        INSTRUCTIONS:
        1. Extract 5–8 fingerprints maximum.
        2. Provide them in Ukrainian, Russian, and English.
        3. Prefer noun phrases over sentences.
        4. Avoid generic military terminology.


        INPUT DATA:
        Event: {title}
        Location: {location}
        Date: {date}

        OUTPUT JSON FORMAT:
        {
            "ua": [...],
            "ru": [...],
            "en": [...]
        }
        """

    user_content = f"Event: {title}\nLocation: {location}\nDate: {date_str}"

    try:
        response = self.openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}  # Forza output JSON
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"   ⚠️ Fingerprint Error: {e}")
        # Fallback di emergenza
        return {"ua": [location], "ru": [location], "en": [location]}


def build_sniper_query(self, fingerprints_list, date, domain):
    """
    Costruisce una query Google Dork precisa: site:domain + "keyword" + "data"
    """
    try:
        # Converte YYYY-MM-DD in "DD Month YYYY" (es. "24 February 2022")
        # Questo formato è molto più efficace per la ricerca di news
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        date_str = date_obj.strftime("%d %B %Y")
    except Exception as e:
        # Fallback se la data non è parseabile
        date_str = date

    # Prende solo le prime 3 keyword per non confondere Google
    terms = fingerprints_list[:3]

    # Le mette tra virgolette per forzare la corrispondenza esatta
    quoted = " ".join([f'"{t}"' for t in terms])

    return f'site:{domain} {quoted} "{date_str}"'

    # ---------------------------------------------------------
    # 2. PROCESS ROW (La funzione principale corretta)
    # ---------------------------------------------------------


def process_row(self, row):
    """ Orchestrates the Super Squad Pipeline with Quality-First Cross-Referencing."""

    # 1. Estrazione Dati Iniziali
    # Cerchiamo il titolo ovunque, dando priorità alle note ACLED (che sono più descrittive)
    title = row.get('Title') or row.get('notes') or row.get('Event')
    if not title:
        return None

    # --- Helper Fallback (Copia questo blocco così com'è) ---
    def create_fallback_entry(reason):
        print(f"   ⚠️ Fallback Triggered: {reason}")
        return {
            "Title": title,
            "Date": row.get("Date"),
            "Type": row.get("Type", "Unknown"),
            "Location": row.get("Location") or "Unknown",
            "Latitude": row.get("Latitude", 0.0),
            "Longitude": row.get("Longitude", 0.0),
            "Source": row.get("Source") or "Search Failed",
            "Archived": "No",
            "Verification": "Unconfirmed",
            "Description": "",
            "Notes": f"AUTO-SKIPPED: {reason}",
            "Video": "No",
            "Intensity": 0.0,
            "Actor": row.get("Actor", "Unknown"),
            "Bias dominante": "Neutral",
            "Location Precision": "UNKNOWN",
            "Aggregated Sources": "",
            "Reliability": 0,
            "Bias Score": 0
        }

    print(f"\n🚀 Processing Event: {title[:50]}...")

    # A. SMART QUERY (Generazione Keyword)
    smart_query_text = self._generate_event_fingerprints(
        title, row.get('Location', ''), row.get('Date', ''))

    # =====================================================================
    # NUOVA LOGICA IBRIDA: FINGERPRINTS + SNIPER + ARCHEOLOGO (FIXED)
    # =====================================================================

    # 1. SETUP DOSSIER & FINGERPRINTS
    acled_source = row.get('ACLED_Original_Source') or row.get('source')
    event_ctx = self._init_event_context(row, acled_source)

    # Inizializziamo subito per sicurezza (evita "undefined variable")
    event_ctx["source_name"] = "Unknown"

    print(f"\n🚀 Investigating: {event_ctx['title'][:50]}...")

    # Generazione Impronte Digitali
    fingerprints = self._generate_event_fingerprints(
        event_ctx['title'], event_ctx['location'], event_ctx['date'])
    print(f"   🔍 Fingerprints (UA): {fingerprints.get('ua', [])}")

    # ---------------------------------------------------------------------
    # 2. FASE SNIPER (Target: Fonte Originale)
    # ---------------------------------------------------------------------
    target_domain = None
    if acled_source:
        for name, domain in self.ACLED_SOURCE_MAP.items():
            if name.lower() in str(acled_source).lower():
                target_domain = domain
                break

    if target_domain:
        sniper_query = self.build_sniper_query(
            fingerprints.get('ua', []),
            event_ctx['date'],
            target_domain
        )
        print(f"   🎯 Sniper Attempt: {sniper_query}")

        s_text, s_source, s_urls = self.perform_search(
            sniper_query, event_ctx['date'])

        if s_urls:
            event_ctx["status"] = "ORIGINAL_FOUND"
            event_ctx["verification_method"] = "SNIPER"
            event_ctx["best_link"] = s_urls[0]
            # Salviamo la fonte trovata!
            event_ctx["source_name"] = s_source
            event_ctx["confidence_score"] = 0.95
            event_ctx["sniper_results"] = s_text
            print("   ✅ Sniper Hit! Original source confirmed.")

            # ---------------------------------------------------------------------
    # 3. FASE ARCHEOLOGO (MODIFICATA: Corroborazione FORZATA)
    # ---------------------------------------------------------------------
    # ORA ESEGUIAMO SEMPRE QUESTA FASE per popolare "Aggregated Sources",
    # anche se lo Sniper ha già trovato il link Telegram originale.

    print("   🌍 Activating Archeologist Protocol (Broad Search)...")

    # Assicuriamoci che la lista URL esista
    if "all_urls" not in event_ctx:
        event_ctx["all_urls"] = []

    # Costruzione Query
    ua_keys = " OR ".join(
        [f'"{k}"' for k in fingerprints.get('ua', [])[:4]])
    ru_keys = " OR ".join(
        [f'"{k}"' for k in fingerprints.get('ru', [])[:4]])

    arch_query = f"({ua_keys} OR {ru_keys}) {event_ctx['location']}"
    arch_query += " (news OR report OR новини OR новости)"

    # Eseguiamo la ricerca
    a_text, a_source, a_urls = self.perform_search(
        arch_query, event_ctx['date'])

    # LOGICA DI UNIONE RISULTATI
    if a_urls:
        # Aggiungiamo i nuovi URL alla lista esistente
        event_ctx["all_urls"].extend(a_urls)

        # CASO A: Avevamo già trovato la fonte originale (Sniper Success)
        if event_ctx["status"] == "ORIGINAL_FOUND":
            # Upgrade dello status!
            event_ctx["status"] = "CONFIRMED_MULTI_SOURCE"
            event_ctx["confidence_score"] = 0.99
            # Salviamo il testo dell'articolo come fallback se necessario
            if not event_ctx.get("sniper_results"):
                event_ctx["fallback_results"] = a_text
            print(
                f"   ✅ News Corroboration! Added {len(a_urls)} external sources.")

        # CASO B: Non avevamo trovato nulla prima
        elif event_ctx["status"] == "PENDING":
            event_ctx["status"] = "CORROBORATED"
            event_ctx["verification_method"] = "ARCHEOLOGIST"
            event_ctx["best_link"] = a_urls[0]
            event_ctx["source_name"] = a_source
            event_ctx["confidence_score"] = 0.70
            event_ctx["fallback_results"] = a_text
            print(f"   ✅ Event Corroborated. Found {len(a_urls)} sources.")

    else:
        # Se l'archeologo non trova nulla...
        if event_ctx["status"] == "PENDING":
            # ...e non avevamo trovato nulla nemmeno prima: ALLORA è perso.
            event_ctx["status"] = "NOT_FOUND"
            event_ctx["confidence_score"] = 0.1
            print("   ❌ Event Lost in Digital Rot.")

    # ---------------------------------------------------------------------
    # 4. PONTE DI COMPATIBILITÀ & FILTRO INTELLIGENTE FONTI
    # ---------------------------------------------------------------------
    if event_ctx["status"] == "NOT_FOUND":
        return create_fallback_entry("Acled Only (Digital Rot - No web verification)")

    # Assegnazione variabili principali
    primary_link = event_ctx["best_link"]
    primary_source = event_ctx["source_name"]

    # --- LOGICA DI FILTRAGGIO AVANZATO PER "AGGREGATED SOURCES" ---
    raw_urls = event_ctx.get("all_urls", [])
    final_aggregated_urls = []
    seen_domains = set()

    # 1. Aggiungiamo sempre il Primary Link (se esiste)
    if primary_link:
        final_aggregated_urls.append(primary_link)
        # Estraiamo il dominio per evitare duplicati dello stesso sito
        try:
            from urllib.parse import urlparse
            domain = urlparse(primary_link).netloc.replace("www.", "")
            seen_domains.add(domain)
        except:
            pass

    # 2. Analizziamo gli altri candidati
    for url in raw_urls:
        # Ci fermiamo se abbiamo già 5 fonti valide
        if len(final_aggregated_urls) >= 5:
            break

        # Saltiamo se è lo stesso link del primario
        if url == primary_link:
            continue

        try:
            domain = urlparse(url).netloc.replace("www.", "")

            # A. FILTRO DUPLICATI DI DOMINIO
            # Se abbiamo già una fonte da "pravda.com.ua", magari evitiamo di metterne
            # altre 3 dello stesso sito per dare varietà.
            # (Se preferisci averne più dello stesso sito, commenta queste due righe sotto)
            if domain in seen_domains:
                continue

            # B. FILTRO SPAM / IRRILEVANTI (Blacklist al volo)
            # Scartiamo domini che sappiamo non contenere notizie utili
            junk_domains = ["facebook.com", "twitter.com",
                            "instagram.com", "youtube.com", "google.com", "t.me"]
            if any(junk in domain for junk in junk_domains):
                # Eccezione: accettiamo link diretti a post specifici, non home page
                if len(url) < 40:  # Link troppo corto = probabilmente homepage inutile
                    continue

            # C. PROMOZIONE FONTI AFFIDABILI
            # Se il dominio è nella nostra mappa, è oro colato.
            is_trusted = False
            for trusted_name, trusted_domain in self.ACLED_SOURCE_MAP.items():
                if trusted_domain in domain:
                    is_trusted = True
                    break

            # Se è fidato lo prendiamo subito, altrimenti lo prendiamo ma teniamo d'occhio il limite
            final_aggregated_urls.append(url)
            seen_domains.add(domain)

        except Exception:
            continue  # Se l'URL è malformato, ignoralo

    # Assegnamo la lista pulita alla variabile 'urls'
    urls = final_aggregated_urls

    # Log di verifica per te
    if len(urls) > 1:
        print(
            f"   📚 Aggregated Sources: {len(urls)} links selected (Filtered from {len(raw_urls)})")

    # Recuperiamo il testo per il Deep Verify (snippet di ricerca)
    search_text = event_ctx.get("sniper_results") or event_ctx.get(
        "fallback_results") or ""

    # Scarichiamo contenuto SOLO del link migliore (Primary)
    # Nota: Non facciamo scraping degli altri 4 per non rallentare l'agente del 500%
    deep_content = ""
    if primary_link:
        print(f"   🕵️ Deep Verifying Best Source: {primary_link}...")
        deep_content = self.fetch_url_text(primary_link)
        if deep_content:
            deep_content = f"=== VERIFIED CONTENT ({event_ctx['verification_method']}) ===\n{deep_content[:15000]}"

    # D. COSTRUZIONE CONTESTO
    combined_text = f"""
        === ACLED SOURCE NOTES ===
        {title}

        {deep_content}

        === WEB SEARCH CONTEXT (SNIPPETS) ===
        {search_text[:3000]}
        """
    # =====================================================================
    # ⚡ STEP 1: TITAN GATEKEEPER (Fine-Tuned)
    # =====================================================================
    # Chiamiamo prima lo specialista per capire se vale la pena procedere.
    titan_result = self._step_titan_classifier(combined_text)

    # HARD FILTER: Se Titan dice NULL, abortiamo subito.
    if titan_result.get('classification') == 'NULL':
        print(f"   🗑️ Titan Rejected: {titan_result.get('reasoning')}")
        return create_fallback_entry("Titan Filtered (Noise/Political)")

    print(
        f"   🤖 TITAN CLASS: {titan_result.get('classification')} (Conf: {titan_result.get('confidence')})")

    # 2. RUN PIPELINE
    # Step 1: Brain (Now analyzes the hybrid context)
    brain_out = self._step_1_the_brain(
        combined_text, {"Title": title, "Date": row.get("Date")})

    # --- MODIFICA 3: Sostituisci i controlli su brain_out ---
    if not brain_out:
        return create_fallback_entry("AI Analysis Failed (Brain Error)")

    if not brain_out.get("is_relevant", True):
        return create_fallback_entry("Deemed Irrelevant by AI")

    # Adattiamo i dati della singola riga al formato "Cluster" richiesto dal nuovo prompt
    # --- INIZIO BLOCCO LAYER 1 ---
    soldier_input_packet = {
        "reference_timestamp": row.get("Date"),
        "raw_messages": [combined_text]
    }

    # 1. Chiamata al Soldato
    # Nota: Usiamo 'soldier_input_packet' perché è il formato richiesto dalla funzione.
    soldier_out = self._step_2_the_soldier(soldier_input_packet)

    # 2. Controllo Fallimento Soldier
    if not soldier_out:
        print("   ⚠️ Soldier failed. Skipping.")
        return create_fallback_entry("AI Analysis Failed (Soldier Error)")

    # =========================================================================
    # 🌍 GEO-VERIFIER INTEGRATION: Validate/Correct Soldier's Location
    # =========================================================================
    # Extract location from Soldier output
    geo_data = soldier_out.get('geo_location', {})
    explicit_coords = geo_data.get('explicit')
    inferred_loc = geo_data.get('inferred', {})
    toponym_raw = inferred_loc.get('toponym_raw')
    
    verified_coords = None
    
    # Priority 1: Use explicit coordinates if valid
    if explicit_coords:
        ex_lat = explicit_coords.get('lat')
        ex_lon = explicit_coords.get('lon')
        if ex_lat and ex_lon and ex_lat not in [0, 0.0, "0", None] and ex_lon not in [0, 0.0, "0", None]:
            # Validate explicit coords are in war zone
            if self._is_in_war_zone(float(ex_lat), float(ex_lon)):
                verified_coords = {'lat': float(ex_lat), 'lon': float(ex_lon)}
                print(f"      📍 Using Soldier's explicit coords: ({ex_lat}, {ex_lon})")
            else:
                print(f"      ⚠️ Soldier's explicit coords outside war zone. Triggering Geo-Verifier...")
    
    # Priority 2: If no valid explicit coords, run Geo-Verifier on toponym
    if not verified_coords and toponym_raw:
        verified_coords = self._step_geo_verifier(toponym_raw, combined_text)
        if verified_coords:
            # Update soldier_out with verified coordinates
            if 'geo_location' not in soldier_out:
                soldier_out['geo_location'] = {}
            soldier_out['geo_location']['verified'] = verified_coords
            print(f"      📍 Geo-Verifier Success: ({verified_coords['lat']}, {verified_coords['lon']})")
    
    # Priority 3: Fallback to event_ctx location if available
    if not verified_coords and event_ctx.get('location'):
        fallback_loc = event_ctx.get('location')
        verified_coords = self._step_geo_verifier(fallback_loc, combined_text)
        if verified_coords:
            print(f"      📍 Fallback Geo-Verifier: {fallback_loc} -> ({verified_coords['lat']}, {verified_coords['lon']})")
    
    # Store verified coordinates for later use
    soldier_out['verified_coordinates'] = verified_coords

    # =========================================================================
    # 🪖 ORBAT TRACKER UPDATE
    # =========================================================================
    units_detected = soldier_out.get('military_units_detected', [])
    if units_detected:
        # Use verified coords if available, else explicit, else row
        final_lat = verified_coords.get('lat') if verified_coords else (soldier_out.get("geo_location", {}).get("explicit", {}).get("lat"))
        final_lon = verified_coords.get('lon') if verified_coords else (soldier_out.get("geo_location", {}).get("explicit", {}).get("lon"))
        
        # Fallback to row data if everything else fails (but usually verified_coords handles it)
        if not final_lat: final_lat = row.get("Latitude", 0.0)
        if not final_lon: final_lon = row.get("Longitude", 0.0)

        if final_lat and final_lat != 0.0:
            self._update_units_registry(units_detected, row.get("Date"), {'lat': final_lat, 'lon': final_lon})
    
    # 3. Calcolo T.I.E. (Layer 1)
    titan_data = soldier_out.get('titan_assessment', {})
    visual_evidence = soldier_out.get('visual_evidence', False)

    tie_result = self._calculate_tie(titan_data, visual_evidence)

    print(f"      🎯 TIE: {tie_result['value']} [{tie_result['status']}] "
          f"(K:{tie_result['vectors']['k']} T:{tie_result['vectors']['t']} E:{tie_result['vectors']['e']})")

    # Step 3: Calculator (Passiamo la LISTA delle fonti se disponibile, altrimenti stringa singola)
    sources_for_calc = row.get("sources_list_for_bias") or primary_source

    calc_out = self._step_3_the_calculator(
        soldier_out, brain_out, sources_for_calc, combined_text)

    # Step 4: Journalist
    journo_out = self._step_4_the_journalist(
        combined_text, brain_out, soldier_out)

    # 3. FORMAT FINAL OUTPUT
    # Unisce tutte le URL trovate
    all_sources_list = list(set([u for u in urls if u]))
    aggregated_sources_str = " | ".join(all_sources_list)

    # --- CALCOLO TIPO EVENTO ---
    computed_event_type = self._classify_event_type(combined_text)

    # --- FUNZIONE HELPER ANTI-SOVRASCRITTURA ---
    def keep_or_update(key, new_val):
        current = str(row.get(key, '')).strip()
        if current and current not in ['0', '0.0', 'None', '']:
            return row.get(key)
        return new_val

    # --- CORREZIONE VARIABILI ---
    # 1. Recuperiamo il tipo dall'AI (soldier_out ha target_category)
    # Nota: soldier_out viene da Qwen, journo_out da GPT.

    # Usiamo soldier_out per i dati tecnici
    ai_type = soldier_out.get("target_category", "Unknown")

    # Se Qwen fallisce, fallback alla regex
    if ai_type == "Unknown" or ai_type not in self.EVENT_TYPES:
        # Usa combined_text, non raw_text_full
        ai_type = self._classify_event_type(combined_text)

    # 2. Status Finale
    final_verif = "Unconfirmed"
    if event_ctx["status"] == "CONFIRMED_MULTI_SOURCE":
        final_verif = "Confirmed"
    elif event_ctx["status"] == "ORIGINAL_FOUND":
        final_verif = "Verified (Source Linked)"
    elif event_ctx["status"] == "CORROBORATED":
        final_verif = "Corroborated"

    # 3. Costruzione output usando i dizionari corretti (journo_out, soldier_out, calc_out)
    return {
        # Usa journo_out, fallback al titolo originale
        "Title": journo_out.get("title", title),
        "Type": ai_type,
        "Date": row.get("Date"),
        "Location": toponym_raw or row.get("Location"),
        # Use verified_coordinates from Geo-Verifier, fallback to row data
        "Latitude": verified_coords.get('lat') if verified_coords else (
            soldier_out.get("lat") if soldier_out.get("lat") not in [0, 0.0, None] else row.get("Latitude", 0.0)
        ),
        "Longitude": verified_coords.get('lon') if verified_coords else (
            soldier_out.get("lon") if soldier_out.get("lon") not in [0, 0.0, None] else row.get("Longitude", 0.0)
        ),
        "Source": event_ctx.get("best_link") or row.get("Source"),
        "Archived": "No",
        "Verification": final_verif,
        "Description": journo_out.get("description", ""),
        "Notes": f"Strategy: {event_ctx['verification_method']} | Bias: {calc_out['bias_score']}",
        "Video": "Yes" if soldier_out.get("visual_evidence") else "No",
        "Intensity": str(calc_out.get("intensity", 0.0)),
        "Actor": brain_out.get("actor", "Unknown"),  # Preso dal Brain
        "Bias dominante": calc_out.get("dominant_bias", "Neutral"),
        "Location Precision": soldier_out.get("location_precision", "UNKNOWN"),
        "Aggregated Sources": row.get("aggregated_log") or aggregated_sources_str,
        "Reliability": str(calc_out.get("reliability", 0)),
        "Bias Score": str(calc_out.get("bias_score", 0))
    }

    # =========================================================================
    # 🪖 HELPER: UPDATE UNITS REGISTRY
    # =========================================================================
    def _update_units_registry(self, units_list, event_date_str, coords):
        """Updates the units_registry table with fresh location data."""
        if not units_list: return
        
        try:
            # We open a transient connection here to allow portability
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            affected = 0
            for unit in units_list:
                unit_id = unit.get('unit_id')
                status = unit.get('status', 'ACTIVE')
                
                if not unit_id: continue
                
                # Update only if unit exists (Seeder should have populated it)
                cursor.execute("""
                    UPDATE units_registry 
                    SET last_seen_lat = ?, 
                        last_seen_lon = ?, 
                        last_seen_date = ?, 
                        status = ?
                    WHERE unit_id = ?
                """, (coords.get('lat'), coords.get('lon'), event_date_str, status, unit_id))
                
                affected += cursor.rowcount
            
            conn.commit()
            conn.close()
            if affected > 0:
                print(f"      🪖 ORBAT Tracker: Updated positions for {affected} units.")
                
        except Exception as e:
            print(f"      ⚠️ ORBAT Update Error: {e}")

# =============================================================================
# 🚀 NEW MAIN LOOP: SQLITE ENGINE (Fase 4 Ready)
# =============================================================================


DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

# --- 1. IL DIZIONARIO "GAZETTEER" (Whitelist Estesa) ---
# Usiamo questo SOLO per luoghi FUORI dal rettangolo di guerra (UA/RU).
# Tutto ciò che è in Ucraina o Russia viene gestito dinamicamente dall'API (Geocoding).
KNOWN_LOCATIONS = {
    # --- HUB LOGISTICI E MILITARI (I più importanti per la guerra) ---
    "rzeszow": (50.0412, 21.9991),     # Hub principale aiuti (Polonia)
    "jasionka": (50.1120, 22.0180),    # Aeroporto Rzeszow
    "przemysl": (49.7818, 22.7675),    # Confine ferroviario
    "lublin": (51.2465, 22.5684),      # Polonia
    "constanta": (44.1792, 28.6383),   # Porto Romania (Grano/Aiuti)
    "suceava": (47.6514, 26.2555),     # Hub Romania Nord
    "tulcea": (45.1768, 28.8023),      # Romania (confine Danubio)
    "galati": (45.4353, 28.0080),      # Romania
    "satu mare": (47.7900, 22.8900),   # Romania
    "kosice": (48.7164, 21.2611),      # Slovacchia (Riparazioni)
    "michalovce": (48.7547, 21.9195),  # Slovacchia
    "ramstein": (49.4447, 7.6033),     # Base USA Germania (Ramstein Format)
    "wiesbaden": (50.0782, 8.2397),    # HQ US Army Europe

    # --- ZONE "IBRIDE" / CONFINE ESTERNO ---
    "transnistria": (46.8403, 29.6293),  # Moldova (Separatisti)
    "tiraspol": (46.8361, 29.6105),
    "kaliningrad": (54.7104, 20.4522),  # Exclave Russa (Strategica)
    "baltiysk": (54.6558, 19.9126),     # Flotta Baltico
    "suwalki gap": (54.1100, 23.3500),  # Corridoio Suwalki
    "narva": (59.3797, 28.1791),        # Confine Estonia/Russia

    # --- BIELORUSSIA (Spesso base di lancio, ma fuori dal 'recinto' stretto) ---
    "belarus": (53.7098, 27.9534),
    "minsk": (53.9006, 27.5590),
    "gomel": (52.4345, 30.9754),       # Hub sud
    "homel": (52.4345, 30.9754),
    "brest": (52.0976, 23.7341),       # Confine Polonia
    "luninets": (52.2475, 26.7972),    # Base aerea
    "machulishchi": (53.7766, 27.5794),  # Base A-50
    "zyabrovka": (52.3025, 31.1633),    # Base aerea

    # --- CAPITALI ALLEATI (Decisioni Politiche/Sanzioni) ---
    "washington": (38.8951, -77.0364),
    "washington dc": (38.8951, -77.0364),
    "dc": (38.8951, -77.0364),
    "london": (51.5074, -0.1278),
    "brussels": (50.8503, 4.3517),     # EU / NATO HQ
    "paris": (48.8566, 2.3522),
    "berlin": (52.5200, 13.4050),
    "warsaw": (52.2297, 21.0122),
    "warszawa": (52.2297, 21.0122),
    "vilnius": (54.6872, 25.2797),
    "riga": (56.9496, 24.1052),
    "tallinn": (59.4370, 24.7536),
    "helsinki": (60.1699, 24.9384),
    "stockholm": (59.3293, 18.0686),
    "oslo": (59.9139, 10.7522),
    "copenhagen": (55.6761, 12.5683),
    "prague": (50.0755, 14.4378),
    "bratislava": (48.1486, 17.1077),
    "budapest": (47.4979, 19.0402),
    "bucharest": (44.4268, 26.1025),
    "sofia": (42.6977, 23.3219),
    "rome": (41.9028, 12.4964),
    "madrid": (40.4168, -3.7038),
    "the hague": (52.0705, 4.3007),    # CPI / Tribunali

    # --- ASSE AVVERSARIO (Fornitori armi) ---
    "tehran": (35.6892, 51.3890),      # Iran (Shahed)
    "pyongyang": (39.0392, 125.7625),  # Nord Corea (Munizioni)
    "beijing": (39.9042, 116.4074),    # Cina
    "ankara": (39.9334, 32.8597),      # Turchia (Mediatore)
    "istanbul": (41.0082, 28.9784),    # Accordi Grano

    # --- MARI E STRETTI (Guerra Navale/Ibrida) ---
    "black sea": (43.5, 34.0),           # Centro Mar Nero (Generico)
    "mar nero": (43.5, 34.0),
    "international waters": (43.5, 34.0),  # Spesso nel Mar Nero
    # Centro Baltico (Sabotaggi Nord Stream)
    "baltic sea": (56.5, 19.0),
    "mar baltico": (56.5, 19.0),
    "caspian sea": (42.0, 51.0),         # Lancio missili russi
    "mar caspio": (42.0, 51.0),
    "bosphorus": (41.1, 29.1),           # Stretto
    "dardanelles": (40.2, 26.4),
    "snake island": (45.2551, 30.2037),  # Isola dei Serpenti
    "zmiinyi": (45.2551, 30.2037)
}
# --- FUNZIONE GEOCODING SICURO ---


def safe_geocode(geolocator, query):
    """
    1. Cerca nella Whitelist (Hardcoded).
    2. Se non trova, cerca via API ma SOLO nel rettangolo di guerra.
    """
    if not query:
        return None, None

    # Normalizza la query (minuscolo e pulizia spazi)
    clean_query = str(query).lower().strip()

    # --- STEP 1: CONTROLLO WHITELIST (Hardcoded) ---
    # Controlla match esatto
    if clean_query in KNOWN_LOCATIONS:
        print(f"      📍 Whitelist Hit: '{query}' -> Hardcoded.")
        return KNOWN_LOCATIONS[clean_query]

    # --- STEP 2: GEOCODING CON RECINTO (API) ---
    # Recinto: Ucraina + Russia Occidentale
    MIN_LAT, MAX_LAT = 44.0, 60.0
    MIN_LON, MAX_LON = 22.0, 55.0

    try:
        # Priorità a UA/RU
        location = geolocator.geocode(
            query, country_codes=['ua', 'ru'], timeout=5)

        # Fallback globale (se il geocoder locale fallisce)
        if not location:
            location = geolocator.geocode(query, timeout=5)

        if location:
            lat, lon = location.latitude, location.longitude

            # CHECK DEL RECINTO
            if MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON:
                return lat, lon
            else:
                print(
                    f"      ⚠️ COORDINATE FUORI ZONA SCARTATE: {query} ({lat},{lon})")
                return None, None

        return None, None

    except Exception as e:
        print(f"      ❌ Geocoding Error: {e}")
        return None, None


def main():
    print("🤖 STARTING SUPER SQUAD AGENT (SQLite Mode)...")

    # 1. Connessione al DB
    if not os.path.exists(DB_PATH):
        print(f"❌ Database non trovato: {DB_PATH}")
        print("   Esegui prima 'scripts/refiner.py' per popolare il DB!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Inizializza Client OpenAI Standard (per GPT-4o-mini)
    client_openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Inizializza Client OpenRouter (per DeepSeek / The Strategist)
    client_or = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=os.getenv("OPENROUTER_API_KEY"),
    )
    # [MODIFICA 1] Controllo/Creazione colonna per salvare il report AI
    try:
        cursor.execute("ALTER TABLE events ADD COLUMN ai_report_json TEXT")
        conn.commit()
        print("✅ Colonna 'ai_report_json' aggiunta al DB.")
    except sqlite3.OperationalError:
        pass  # La colonna esiste già, tutto ok.

    agent = SuperSquadAgent()

    # 2. Recupero Cluster Pendenti
    print("🔍 Lettura cluster pendenti da SQLite...")

    try:
        print("   📥 Recupero eventi in attesa (Priorità ai Super-Cluster)...")

        # Questa query usa un CASE statement per dare priorità 0 (massima) agli eventi fusi
        cursor.execute("""
            SELECT * FROM unique_events
            WHERE ai_analysis_status = 'PENDING'
            ORDER BY
                CASE
                    WHEN full_text_dossier LIKE '%[MERGED%' THEN 0
                    ELSE 1
                END ASC,
                last_seen_date DESC
            LIMIT 2000
        """)

        clusters_to_process = cursor.fetchall()

    except Exception as e:
        print(f"⚠️ Errore SQL: {e}")
        return

    if not clusters_to_process:
        print("✅ Nessun nuovo cluster da processare (Tutto aggiornato).")
        conn.close()
        return

    print(f"🚀 Trovati {len(clusters_to_process)} cluster da analizzare.")

    # 3. Elaborazione Sequenziale (Architecture: Soldier First -> Brain Verify)
    for row in clusters_to_process:
        cluster_id = row['event_id']
        ref_date = row['last_seen_date']

        text_content = row['full_text_dossier'] if row['full_text_dossier'] else ""

        all_msgs_raw = text_content.split(' ||| ')

        # ==============================================================================
        #  JUNK FILTER: scarta spam e notizie civili irrilevanti
        # ==============================================================================
        # Lista di parole che indicano al 100% che NON è un evento di guerra
        junk_keywords = [
            # Spam & Crypto
            "bitcoin", "crypto", "ethereum", "nft ",
            "casino", "slot ", "betting",

            # Pubblicità
            # Attenzione agli spazi per evitare falsi positivi
            "sconto", "promo ", "offert", "liquidazione",

            # Civile / Immobiliare (Il tuo caso specifico)
            "immobiliare", "affitto", "vendesi", "agenzia immobiliare",
            "рієлтор",  # 'Realtor' in ucraino
            "kvartira",  # 'Appartamento' traslitterato spesso usato negli url

            # Intrattenimento
            "oroscopo", "serie a", "champions league", "calciomercato"
        ]

        # Convertiamo in minuscolo per il controllo
        text_lower = text_content.lower()

        is_junk = False
        for word in junk_keywords:
            if word in text_lower:
                print(
                    f"🗑️ SKIP: Evento scartato per keyword spazzatura: '{word}'")
                is_junk = True
                break

        if is_junk:
            # Marcalo come SKIPPED così non lo ripeschiamo, ma non lo cancelliamo
            cursor.execute(
                "UPDATE unique_events SET ai_analysis_status='SKIPPED_JUNK' WHERE event_id=?", (cluster_id,))
            conn.commit()
            continue
        # =========================================================
        # 🛡️ STEP 0: THE BOUNCER (AI SPAM FILTER - Qwen 32B)
        # =========================================================
        # Filtro intelligente per quello che è sfuggito alle keyword
        bouncer_result = agent._step_0_the_bouncer(text_content)

        if bouncer_result.get('is_relevant') is False:
            print(
                f"      🗑️ REJECTED by Bouncer: {bouncer_result.get('reason')}")

            # Salvataggio nel DB come REJECTED
            cursor.execute(
                "UPDATE unique_events SET ai_analysis_status = 'REJECTED' WHERE event_id = ?", (cluster_id,))
            conn.commit()

            continue

        # --- ✂️ HYBRID SMART SLICER (Telegram + GDELT) ---
        # Ottimizza costi e attenzione: taglia articoli lunghi, rimuove duplicati, limita quantità.

        selected_msgs = []
        current_total_chars = 0

        # PARAMETRI DI SICUREZZA
        MAX_ITEMS = 12              # Max numero di fonti (Telegram o Articoli)
        # Tronca singolo articolo GDELT (prendiamo solo l'inizio)
        MAX_CHARS_PER_ITEM = 3000
        MAX_TOTAL_CHARS = 25000     # Tetto massimo totale input (~6000 token)

        seen_hashes = set()  # Per deduplicazione esatta

        for msg in all_msgs_raw:
            msg = msg.strip()
            if not msg:
                continue

            # Deduplicazione (evita di pagare 2 volte per lo stesso testo identico)
            msg_hash = hash(msg)
            if msg_hash in seen_hashes:
                continue
            seen_hashes.add(msg_hash)

            # Troncamento Intelligente (Per articoli GDELT infiniti)
            # Se supera il limite, prendiamo solo la testa (dove c'è la notizia)
            if len(msg) > MAX_CHARS_PER_ITEM:
                msg = msg[:MAX_CHARS_PER_ITEM] + "... [TRUNCATED]"

            # Controllo Budget Totale (Se il secchio è pieno, ci fermiamo)
            if current_total_chars + len(msg) > MAX_TOTAL_CHARS:
                break

            selected_msgs.append(msg)
            current_total_chars += len(msg)

            # Stop se abbiamo raggiunto il numero massimo di fonti
            if len(selected_msgs) >= MAX_ITEMS:
                break

        # Aggiorniamo la variabile per il log
        raw_msgs = selected_msgs

        print(f"\n⚡ Processing Cluster ID: {cluster_id}")
        print(
            f"   📉 Optimization: {len(all_msgs_raw)} sources -> {len(raw_msgs)} selected (Len: {current_total_chars} chars)")

        # Creazione testo unico per l'AI
        combined_text = "\n".join(raw_msgs)

        # --- STEP 1: THE SOLDIER (Extraction) ---
        cluster_data = {
            "reference_timestamp": ref_date,
            "raw_messages": raw_msgs
        }

        soldier_result = None
        try:
            # Usiamo soldier_result coerentemente
            soldier_result = agent._step_2_the_soldier(cluster_data)
        except Exception as e:
            print(f"   ⚠️ Soldier Stumbled: {e}")

        # LOGICA FALLBACK (SENZA CONTINUE):
        # Se fallisce, creiamo un oggetto vuoto così il codice dopo non si rompe,
        # ma NON fermiamo il ciclo. Andiamo avanti verso il Brain.
        if not soldier_result:
            print("   ⏩ Soldier empty/failed. Escalating to Brain for recovery...")
            soldier_result = {"status": "FAILED_EXTRACTION"}

        # =================================================================
        # 🟢 LAYER 1: T.I.E. CALCULATOR (SAFETY VERSION)
        # =================================================================
        # 1. Estrazione sicura (Se soldier_result è il fallback, userà i default)
        titan_data = soldier_result.get('titan_assessment') or {}
        visual_evidence = soldier_result.get('visual_evidence', False)

        # --- 🛡️ METRIC BOOSTER (FALLBACK) ---
        # Se mancano i dati Titan (K/T/E sono 0 o mancanti), chiamiamo il Fine-Tuning
        k_score = titan_data.get('kinetic_score', 0)
        if not k_score or int(k_score) == 0:
             print("      ⚠️ Missing Metrics (K=0). Activating Titan Fallback...")
             try:
                 # Usa il classificatore dedicato
                 titan_fallback = agent._step_titan_classifier(combined_text)
                 
                 # Merge intelligente: sovrascrive solo se mancano
                 if 'kinetic_score' in titan_fallback: titan_data['kinetic_score'] = titan_fallback['kinetic_score']
                 if 'target_score' in titan_fallback: titan_data['target_score'] = titan_fallback['target_score']
                 if 'effect_score' in titan_fallback: titan_data['effect_score'] = titan_fallback['effect_score']
                 
                 print(f"      ✅ Fallback Metrics Applied: K={titan_data.get('kinetic_score')}")
             except Exception as e:
                 print(f"      ❌ Fallback Failed: {e}")

        # 2. Calcolo T.I.E.
        tie_result = agent._calculate_tie(titan_data, visual_evidence)

        print(f"      🎯 TIE: {tie_result['value']} [{tie_result['status']}]")

        # --- STEP 2: THE BRAIN (Verification & Recovery) ---
        metadata_for_judge = {
            "Target Date": ref_date,
            "Soldier_Extraction": soldier_result
        }

        try:
            brain_review = agent._step_1_the_brain(
                combined_text, metadata_for_judge)

            # IL FILTRO VERO: Se il Brain dice che è spam/irrelevante, buttiamo tutto.
            if not brain_review.get('verification_status', False):
                reason = brain_review.get('rejection_reason', 'Unknown')
                print(f"   ⛔ Brain Invalidated Event: {reason}")
                # TODO: Segnare come processato (scartato)
                # cursor.execute("UPDATE events SET processed = -1 WHERE cluster_id = ?", (cluster_id,))
                # conn.commit()
                continue

            print(
                f"   🧠 Brain Verified. Signal: {brain_review['verified_data'].get('implicit_signal')}")
            if brain_review.get("correction_notes"):
                print(
                    f"   🔧 Correction Applied: {brain_review.get('correction_notes')}")

            # Recupere liste fonti dal DB
            db_urls = row['urls_list']
            db_sources = row['sources_list']
            
            # Parsing sicuro delle liste (potrebbero essere stringhe "foo | bar" o JSON)
            def safe_parse_list(val):
                if not val: return []
                if isinstance(val, list): return val
                
                # Prova JSON
                val_str = str(val).strip()
                if val_str.startswith('[') and val_str.endswith(']'):
                    try:
                        parsed = json.loads(val_str)
                        if isinstance(parsed, list):
                            return parsed
                    except:
                        pass
                
                # Prova separator
                if ' ||| ' in val_str: return [x.strip() for x in val_str.split(' ||| ') if x.strip()]
                if ' | ' in val_str: return [x.strip() for x in val_str.split(' | ') if x.strip()]
                
                # Fallback singolo item
                return [val_str]

            actual_sources_list = safe_parse_list(db_sources)
            actual_urls_list = safe_parse_list(db_urls)
            
            # Uniamo tutto per il Calculator (che vuole nomi di dominio o fonti)
            sources_for_calc = actual_sources_list + actual_urls_list

            # --- STEP 3: CALCULATOR ---
            # Usiamo i dati validati. Se il soldato aveva fallito, usiamo soldier_result (che è dummy)
            # ma il Calculator lavora sul testo, quindi funzionerà comunque per il bias/intensity.
            calc_result = agent._step_3_the_calculator(
                soldier_data=soldier_result if soldier_result.get(
                    "status") != "FAILED_EXTRACTION" else {},
                brain_data=brain_review['verified_data'],
                source_name=sources_for_calc if sources_for_calc else ["Cluster Aggregated"],
                text=combined_text
            )

            # --- STEP 4: JOURNALIST ---
            journo_result = agent._step_4_the_journalist(
                text=combined_text,
                brain_data=brain_review['verified_data'],
                soldier_data=soldier_result
            )

            # --- E. GOLDEN RECORD ---
            final_report = {
                "cluster_id": cluster_id,
                "timestamp_generated": datetime.now().isoformat(),
                "status": "VERIFIED",
                # Contiene i dati corretti dal Brain
                "strategy": brain_review['verified_data'],
                # Dati grezzi del soldato (potrebbero essere null o errati)
                "tactics": soldier_result,
                "scores": calc_result,
                "editorial": journo_result,
                "tie_score": tie_result['value'],
                "tie_status": tie_result['status'],
                "titan_metrics": titan_data,
                "Aggregated Sources": actual_urls_list  # [FIX] Esportazione esplicita per generate_output.py
            }

            print("   ✅ Intelligence Extracted & Verified:")

            # ==============================================================================
            # [NUOVO] STEP 5: THE STRATEGIST (Tactical Insight)
            # ==============================================================================
            # 1. Aggiungi 'self.' davanti alla funzione
            # 2. Usa 'self.client' (o il nome corretto del tuo client) al posto di 'client_or'
            tactical_insight = _step_5_the_strategist(
                client_or, final_report)

            # 3. Allinea questo print ESATTAMENTE sotto la riga sopra
            print("   🧩 Tactical Insight Generated.")

            # 4. Allinea anche questo
            final_report['ai_summary'] = tactical_insight

            # ==================================================================================
            # GEO-FIXER BLOCK (VERSIONE SICURA CON RECINTO)
            # ==================================================================================
            try:
                # 1. Recupera i dati attuali
                tactics = final_report.get('tactics', {})
                geo = tactics.get('geo_location', {})

                # Assicura che 'explicit' esista
                if geo.get('explicit') is None:
                    geo['explicit'] = {}

                lat = geo['explicit'].get('lat')
                location_name = geo.get('inferred', {}).get('toponym_raw')

                # 2. Se mancano coordinate e abbiamo un nome, CERCA!
                if (not lat or lat == 0) and location_name and location_name != "Unknown":

                    # Lista nera di parole generiche
                    banned_locations = ["Ukraine", "Russia", "Europe",
                                        "NATO", "EU", "Border", "Frontline", "Front", "Zone"]

                    if location_name.strip() in banned_locations:
                        print(
                            f"      ⚠️ Skipped Geocoding for generic location: '{location_name}'")
                    else:
                        print(
                            f"      🌍 Geocoding forzato per '{location_name}'...")

                        # --- CHIAMATA ALLA FUNZIONE SICURA ---
                        # Assicurati di aver definito 'geolocator' all'inizio del main
                        # geolocator = Nominatim(user_agent="trident_tracker")
                        new_lat, new_lon = safe_geocode(
                            geolocator, location_name)

                        if new_lat and new_lon:
                            final_report['tactics']['geo_location']['explicit']['lat'] = new_lat
                            final_report['tactics']['geo_location']['explicit']['lon'] = new_lon
                            print(
                                f"       ✅ Trovato e Inserito (Zona Sicura): {new_lat}, {new_lon}")
                            import time
                            time.sleep(1)
                        else:
                            print(
                                "       ⚠️ Luogo non trovato o fuori dalla zona di guerra.")

            except Exception as e:
                print(f"   ⚠️ Warning Geo-Fixer: {e}")
            # ==================================================================================
            # FINE BLOCCO GEO-FIXER
            # ==================================================================================

            # Commentiamo per pulire il log
            print(json.dumps(final_report, indent=2, ensure_ascii=False))

            # [MODIFICA 2] SALVATAGGIO PERSISTENTE
            try:
                # Serializziamo il dizionario in una stringa JSON
                report_text = json.dumps(final_report, ensure_ascii=False)
                
                # Serializziamo anche le metriche Titan separatamente
                titan_metrics_json = json.dumps(titan_data, ensure_ascii=False) if titan_data else None

                # Scriviamo nel DB - Salviamo TUTTI i campi in colonne dedicate
                cursor.execute("""
                    UPDATE unique_events
                    SET ai_report_json = ?,
                        ai_analysis_status = 'COMPLETED',
                        tie_score = ?,
                        tie_status = ?,
                        titan_metrics = ?,
                        kinetic_score = ?,
                        target_score = ?,
                        effect_score = ?,
                        reliability = ?,
                        bias_score = ?,
                        ai_summary = ?,
                        has_video = ?,
                        title = ?,
                        description = ?,
                        urls_list = ?
                    WHERE event_id = ?
                    """, (
                        json.dumps(final_report, ensure_ascii=False),
                        tie_result['value'],
                        tie_result['status'],
                        titan_metrics_json,
                        titan_data.get('kinetic_score', 0),
                        titan_data.get('target_score', 0),
                        titan_data.get('effect_score', 0),
                        calc_result.get('reliability', 0),
                        calc_result.get('bias_score', 0),
                        final_report.get('ai_summary', ''),
                        1 if soldier_result.get('visual_evidence') else 0,
                        journo_result.get('title_en', ''),
                        journo_result.get('description_en', ''),
                        ' | '.join(actual_urls_list) if actual_urls_list else '',
                        cluster_id
                    ))

                conn.commit()
                print("   💾 Salvataggio nel DB completato (Golden Record Saved).")

            except Exception as save_err:
                print(f"   ❌ ERRORE SALVATAGGIO DB: {save_err}")

        except Exception as e:
            print(f"   ⚠️ Error processing cluster {cluster_id}: {e}")

    conn.close()
    print("\n🏁 Sessione conclusa.")


if __name__ == "__main__":
    main()
