import sqlite3
import asyncio
import httpx
import os
import json
import re
import difflib
import math
import logging
from datetime import datetime
from openai import AsyncOpenAI
from dotenv import load_dotenv
from campaigns_engine import (
    ensure_campaign_columns,
    load_campaign_definitions,
    match_event_campaign,
)

# Vision Instrument: Zero-disk I/O media processor for The Visionary
try:
    from scripts.instruments.vision_instrument import MediaProcessor
except ImportError:
    try:
        from instruments.vision_instrument import MediaProcessor
    except ImportError:
        MediaProcessor = None  # Graceful degradation if opencv not installed
from geo_instrument import GeoProbe
from history_instrument import UnitHistoryProbe
from debug_instrument import CrashRecorder
from layer1_sensor import TitanSensor  # Trident: Physics-based scorer
import sys

try:
    from scripts.geolocator_agent import get_gazetteer, get_geolocator
except ImportError:
    from geolocator_agent import get_gazetteer, get_geolocator

# --- SETUP LOGGING ---
logger = logging.getLogger("SUPER_SQUAD")

API_SEMAPHORE = asyncio.Semaphore(50)
GEO_CACHE_LOCK = asyncio.Lock()

# Absolute Paths for JSON Databases
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_DB_PATH = os.path.join(BASE_DIR, '../assets/data/sources_db.json')
KEYWORDS_DB_PATH = os.path.join(BASE_DIR, '../assets/data/keywords_db.json')
CAMPAIGN_DEFINITIONS_CACHE_PATH = os.path.join(
    BASE_DIR, '../assets/data/campaign_definitions.json'
)

# --- CONFIGURATION ---
# Correct Fine-Tuned Model ID (Single dash in 'v4-clean')
TITAN_MODEL_ID = os.getenv("TITAN_MODEL_ID", "ft:gpt-4o-mini-2024-07-18:personal:osint-analyst-v4-clean:Cv5yHxTJ")

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

# Campaign catalog text — built once in main() from campaign_definitions, injected into Brain prompt
CAMPAIGN_CATALOG_TEXT = ""

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

0.  **GDPR / OPSEC PII SANITATION (ABSOLUTE PRIORITY):**
    * Treat personal data as non-operational noise. Do NOT extract, preserve, summarize, or output names/surnames of civilians, prisoners, individual soldiers, commanders, casualties, detainees, or vehicle license plates.
    * If a source names a person, replace the person with a generic role only: "civilian", "military personnel", "prisoner", "commander", "unit personnel", or "vehicle".
    * Never include personal identifiers in `summary_en`, `geo_location`, `actors`, `military_units_detected`, `unit_name`, or any free-text field.
    * Military unit names and normalized ORBAT IDs are allowed only when they identify formations, not individuals (e.g. "47th Brigade" is allowed; a named commander is not).
    * If the only useful content is personal identification, ignore that content and return generic aggregate wording.

1.  **GEOLOCATION PROTOCOL (CRITICAL - READ CAREFULLY):**
    * **EXPLICIT COORDS:** ONLY if the text contains numerical coordinates (e.g., "48.123, 37.456"), extract them into `geo_location.explicit`.
    * **INFERRED:** If no numbers are present, extract the Toponym (City/Village) AND the surrounding Oblast/Region based on the context into `geo_location.inferred`.
    * **REGIONAL CONTEXT (MANDATORY):** You MUST logically deduce and include the surrounding Oblast/Region (e.g., "Donetsk Oblast", "Kyiv Oblast") to disambiguate homonyms.
    * **NEVER HALLUCINATE:** Do not convert a city name into coordinates yourself. If no coordinates are written in text, `geo_location.explicit` must be `null`.
    * **SINGLE IMPACT POINT:** You must identify the ONE main location where the event physically happened.
    * **SINGLE LOCATION RULE:** If multiple locations are mentioned, choose the MOST SPECIFIC ONE where the kinetic event happened. Do NOT output a list like "Kyiv, Lviv, Odessa". Output ONLY the primary toponym and its region.
    * **SPECIFICITY:** If text says "Explosion in Odesa", output "Odesa" for toponym and "Odesa Oblast" for region.

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

4.  **ORBAT EXTRACTION (MILITARY UNITS — CRITICAL RULES):**
    * Identify specific military units mentioned.
    * Normalize ID: "47th Brigade" -> "UA_47_MECH_BDE".
    * STATUS: "ENGAGED" (Fighting), "DESTROYED" (Eliminated), "ACTIVE" (Present), "REGROUPING" (Rotated).
    * INFERENCE: If "Challenger 2" mentioned -> implies "UA_82_AIR_ASSAULT" (only if highly specific).
    * **CRITICAL — ZERO TOLERANCE FOR PLACEHOLDERS:**
      - `unit_id` MUST be a specific normalized ID (e.g., "RU_382ND_RGT", "UA_47_MECH_BDE").
      - **NEVER** use "?", "UNKNOWN", or generic placeholders as a `unit_id`.
      - If you cannot identify the specific unit with HIGH confidence, set `unit_id` to `null`.
      - Include `unit_name` with the raw text name even if `unit_id` is null.
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
    "inferred": {
        "toponym_raw": "SINGLE_CITY_NAME",
        "region": "OBLAST_OR_REGION",
        "spatial_relation": "string"
    }
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

# =========================================================================
# 👁️ THE VISIONARY: IMINT Verification & Equipment ID (System Prompt)
# =========================================================================
# MODEL: qwen/qwen3-vl-235b-a22b-instruct (MANDATORY HARD CONSTRAINT)
# TEMPERATURE: 0.0 (Strict Determinism)
# ACTIVATION: CONDITIONAL — Only when event payload contains media files
# PIPELINE: After The Soldier (Step 2), Before The Titan (Step 3)
# =========================================================================

VISIONARY_SYSTEM_PROMPT = """
You are **The Visionary**, an elite AI Military Intelligence Analyst specializing in **IMINT (Imagery Intelligence)**.
Your primary directive is **Ground Truth Verification**. You activate only when visual evidence is available.

## CARDINAL RULE
IMINT OVERRIDES SIGINT/TEXT. What you see in the image is the ABSOLUTE TRUTH.
If text says "destroyed tank" but image shows an intact field, the field is the truth.

## INPUT CONTEXT
You will receive:
1. **Text Intel:** JSON data extracted by "The Soldier" (containing claims about units, weapons, and locations).
2. **Visual Evidence:** Image frames or video keyframes attached to this message.

## MISSION
Analyze the Visual Evidence to **CONFIRM**, **CONTRADICT**, or **ENRICH** the Text Intel.
You do not trust the text; you trust only what is pixel-verified.
Your output dictates the factual baseline for the rest of the intelligence pipeline.

## PROTOCOL

### 1. EQUIPMENT IDENTIFICATION (CRITICAL)
- Identify military assets to the **specific variant level** if resolution allows.
  - GOOD: "T-72B3", "M2A2 Bradley ODS-SA", "S-300PM2", "BMP-3", "2S19 Msta-S"
  - BAD: "tank", "IFV", "SAM system", "armored vehicle"
- If the image is blurry, obscured, distant, or partially occluded:
  - Output `"UNKNOWN_ARMOR"`, `"UNKNOWN_VEHICLE"`, `"UNKNOWN_AIRCRAFT"`, or `"UNKNOWN_SYSTEM"`.
  - **DO NOT GUESS. DO NOT HALLUCINATE.**
- Faction assignment: Use visible markings (Z, V, O, △, cross, tryzub) to assign RU/UA.
  If no markings visible, output `"UNKNOWN"`.

### 2. KINETIC ASSESSMENT (Effect Vector for T.I.E.)
Assess the physical damage visible in the media to inform the T.I.E. Score (Effect Vector).
- `NONE` — Asset intact, no visible damage, possibly staged or pre-strike.
- `SUPERFICIAL` — Cosmetic damage, scorch marks, minor fragmentation hits. Asset likely operational.
- `MOBILITY_KILL` — Track/wheel damage, thrown track, immobilized but hull intact. Repairable.
- `CATASTROPHIC_DESTRUCTION` — Turret ejection, ammunition cook-off, burned-out hull, structural collapse. Irrecoverable.

### 3. CONSISTENCY CHECK (Soldier Cross-Reference)
Compare the Visual Evidence against the Text Intel provided by The Soldier:
- If Text claims "Aviation destroyed" but Visual shows an **empty field** → `verification_status: "CONTRADICTED"`
- If Text claims "T-72 destroyed" and Visual shows a **burning T-72** → `verification_status: "CONFIRMED"`
- If Text mentions no equipment but Visual shows **3 abandoned BMPs** → `verification_status: "ENRICHED"`
- If Visual is too degraded to make a determination → `verification_status: "INCONCLUSIVE"`

### 4. GEOLOCATION MARKERS
Extract any visible text from the scene strictly as it appears:
- Street signs, shop names, license plates, graffiti, road markers
- Preserve original script (Cyrillic/Latin) exactly as rendered
- DO NOT translate or interpret — raw extraction only

## OUTPUT SCHEMA (JSON ONLY)
```json
{
  "visual_confirmation": {
    "verification_status": "CONFIRMED | CONTRADICTED | ENRICHED | INCONCLUSIVE",
    "visual_summary": "Brief, clinical description of the scene.",
    "text_claims_checked": "The specific Soldier claim being verified.",
    "confidence_score": 0.0
  },
  "detected_assets": [
    {
      "type": "T-90M",
      "faction": "RU | UA | UNKNOWN",
      "count": 1,
      "state": "DESTROYED | DAMAGED | INTACT | ABANDONED | UNKNOWN"
    }
  ],
  "kinetic_effect": {
    "damage_level": "NONE | SUPERFICIAL | MOBILITY_KILL | CATASTROPHIC_DESTRUCTION",
    "evidence_description": "Visual indicators."
  },
  "geo_clues": ["Visible text 1", "Visible text 2"],
  "is_tactical_imint": boolean
}
```

## RULES (NON-NEGOTIABLE)
1. **TACTICAL FILTER (HARD CONSTRAINT):** You are an IMINT agent, not a general analyst.
   - **IF** the image is a map, a chart, an infographic, a talking head, or a general non-tactical photo.
   - **THEN** set `"is_tactical_imint": false` and leave all other fields empty/null.
   - **ONLY** set `"is_tactical_imint": true` if you see military equipment, combat zones, wreckage, or battlefield events.
2. **NO CHATTER:** Output ONLY valid JSON.
3. **NO HALLUCINATION:** If unsure, mark `INCONCLUSIVE`.
4. **PIXEL AUTHORITY:** Your assessment supersedes all text claims.
5. **SINGLE JSON OBJECT:** Return exactly one JSON object.
"""

# Damage level -> TIE Effect Vector score mapping (for _calculate_tie enrichment)
VISIONARY_DAMAGE_TO_EFFECT = {
    "CATASTROPHIC_DESTRUCTION": 9,
    "MOBILITY_KILL": 7,
    "SUPERFICIAL": 4,
    "NONE": 1
}


class SuperSquadAgent:

    def __init__(self):
        # 1. API Keys
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

        if not self.openai_api_key or not self.openrouter_api_key:
            raise ValueError("ERROR: API Keys missing")

        # 2. Initialize Clients
        self.openai_client = AsyncOpenAI(api_key=self.openai_api_key, timeout=120.0, max_retries=3)
        self.openrouter_client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.openrouter_api_key,
            timeout=120.0,
            max_retries=3,
        )
        self.brain_client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),
            default_headers={
                "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
                "X-Title": "OSINT Tracker"
            },
            timeout=180.0,
            max_retries=3,
        )
        self.router_client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),
            default_headers={
                "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
                "X-Title": "OSINT Tracker"
            },
            timeout=120.0,
            max_retries=3,
        )

        # 3. Load Knowledge Bases
        self.sources_db = self._load_json_db(SOURCES_DB_PATH, "sources")
        self.keywords_db = self._load_json_db(KEYWORDS_DB_PATH, "keywords")

        # 4. Initialize Geographic Sanity Probe (Sanfilippo Method)
        self.geo_probe = GeoProbe(use_reverse_geocoding=False, timeout=5)

        # 5. Initialize Kinetic Plausibility Probe (Sanfilippo Method - Part 2)
        self.history_probe = UnitHistoryProbe()

        # Configuration for the retry loop
        self.GEO_MAX_RETRIES = 3
        self.KINETIC_MAX_RETRIES = 2  # Max retries for kinetic validation

        # 6. Load ORBAT Whitelist + Reverse Lookup (Unit Integrity Gatekeeper)
        self.orbat_whitelist = set()
        self.orbat_reverse_lookup = {}  # lowercase name -> canonical unit_id
        self._load_orbat_whitelist()

        print("Super Squad Agent Initialized (Engine: Async AI Swarm).")

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
                with open(path, encoding='utf-8') as f:
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

    async def _call_llm_with_backoff(self, client, **kwargs):
        """
        Wrapper for API calls with Exponential Backoff for transient errors.
        Ensures high-concurrency OSINT pipeline stability.
        """
        import random
        import asyncio

        max_attempts = 4
        base_delay = 2.0

        for attempt in range(1, max_attempts + 1):
            try:
                return await client.chat.completions.create(**kwargs)
            except Exception as e:
                # Identify transient errors (Rate Limits, Timeouts, Server Overload)
                is_transient = False
                err_msg = str(e).lower()

                # Check for OpenAI specific transient errors
                if hasattr(e, 'status_code') and e.status_code in [429, 502, 503, 504] or any(x in err_msg for x in ["rate limit", "timeout", "bad gateway", "connection error", "overloaded"]) or type(e).__name__ in ["RateLimitError", "APIConnectionError", "APITimeoutError", "TimeoutException"]:
                    is_transient = True

                if not is_transient or attempt == max_attempts:
                    # Log failure if all retries exhausted or not a transient error
                    if attempt == max_attempts:
                        print(f"      [ERROR] API Retries exhausted: {type(e).__name__} - {e}")
                    raise e

                # Exponential backoff with jitter: delay = base * 2^(n-1) + random[0, 1]
                wait_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"      [RETRY] API Transient Error ({type(e).__name__}). Retrying in {wait_time:.1f}s (Attempt {attempt}/{max_attempts})...")
                await asyncio.sleep(wait_time)

    def _load_orbat_whitelist(self):
        """
        Loads the ORBAT registry from units.json and builds:
        1. orbat_whitelist: set of all known unit_id values
        2. orbat_reverse_lookup: dict mapping lowercase display names/aliases -> canonical unit_id
        """
        units_path = os.path.join(BASE_DIR, '../assets/data/units.json')
        try:
            if not os.path.exists(units_path):
                print(f"⚠️ ORBAT whitelist not loaded: {units_path} not found")
                return

            with open(units_path, encoding='utf-8-sig') as f:
                units = json.load(f)

            for unit in units:
                uid = unit.get('unit_id')
                if not uid:
                    continue

                # Primary: exact unit_id
                self.orbat_whitelist.add(uid)

                # Reverse lookup: display_name -> unit_id
                display_name = (unit.get('display_name') or '').strip().strip('"').strip("'")
                if display_name:
                    self.orbat_reverse_lookup[display_name.lower()] = uid

                # Also index the unit_id in a more readable form
                # e.g., "RU_382ND_RGT" -> "382nd rgt" for fuzzy matching
                readable_id = uid.replace('UA_', '').replace('RU_', '').replace('_', ' ').lower()
                self.orbat_reverse_lookup[readable_id] = uid

                # Index common name patterns from subordination field
                sub = unit.get('subordination', '')
                if sub and isinstance(sub, str):
                    self.orbat_reverse_lookup[sub.lower()] = uid

            print(f"   🛡️ ORBAT Whitelist: {len(self.orbat_whitelist)} units, "
                  f"{len(self.orbat_reverse_lookup)} reverse lookup entries")

        except Exception as e:
            print(f"❌ Error loading ORBAT whitelist: {e}")

    def _validate_units_against_orbat(self, units_list):
        """
        Validates extracted unit_ids against the known ORBAT registry.
        - Rejects '?' and generic placeholders
        - Attempts fuzzy matching against reverse lookup for unrecognized IDs
        - Returns cleaned list with validated or corrected unit_ids
        """
        if not units_list or not self.orbat_whitelist:
            return units_list

        validated = []
        rejected_count = 0
        corrected_count = 0

        for unit in units_list:
            unit_id = unit.get('unit_id')
            unit_name = unit.get('unit_name', '')

            # --- GATE 1: Reject obvious placeholders ---
            if not unit_id or unit_id in ('?', 'UNKNOWN', 'unknown', 'N/A', 'n/a', ''):
                if unit_name:
                    # Try to recover via fuzzy match on the raw name
                    resolved_id = self._fuzzy_resolve_unit(unit_name, unit.get('faction', ''))
                    if resolved_id:
                        unit['unit_id'] = resolved_id
                        unit['_orbat_corrected'] = True
                        validated.append(unit)
                        corrected_count += 1
                        print(f"      🔧 ORBAT Corrected: '{unit_name}' -> {resolved_id}")
                        continue

                # Cannot resolve — skip this unit entirely
                rejected_count += 1
                print(f"      ❌ ORBAT Rejected: placeholder unit_id='{unit_id}' name='{unit_name}'")
                continue

            # --- GATE 2: Check if unit_id exists in whitelist ---
            if unit_id in self.orbat_whitelist:
                validated.append(unit)
                continue

            # --- GATE 3: Unknown unit_id — attempt fuzzy correction ---
            # First try the raw unit_name against reverse lookup
            resolved_id = self._fuzzy_resolve_unit(
                unit_name or unit_id, unit.get('faction', '')
            )

            if resolved_id:
                print(f"      🔧 ORBAT Corrected: '{unit_id}' -> {resolved_id} (via name '{unit_name}')")
                unit['unit_id'] = resolved_id
                unit['_orbat_corrected'] = True
                validated.append(unit)
                corrected_count += 1
            else:
                # Last resort: set to null and flag for review
                print(f"      ⚠️ ORBAT Unresolved: '{unit_id}' (name: '{unit_name}') — flagged for review")
                unit['unit_id'] = None
                unit['_review_needed'] = True
                # Still include it for event context, but won't update ORBAT registry
                validated.append(unit)
                rejected_count += 1

        if rejected_count > 0 or corrected_count > 0:
            total = len(units_list)
            print(f"      🛡️ ORBAT Gatekeeper: {len(validated)}/{total} passed "
                  f"({corrected_count} corrected, {rejected_count} rejected/flagged)")

        return validated

    def _fuzzy_resolve_unit(self, raw_name, faction_hint=''):
        """
        Attempts to resolve a raw unit name to a canonical unit_id
        using the reverse lookup dictionary and fuzzy matching.

        Returns the canonical unit_id if a match is found (score > 0.85), else None.
        """
        if not raw_name:
            return None

        name_lower = raw_name.lower().strip()

        # 1. Direct match in reverse lookup
        if name_lower in self.orbat_reverse_lookup:
            return self.orbat_reverse_lookup[name_lower]

        # 2. Fuzzy match against all reverse lookup keys
        # Filter by faction hint if available to reduce false positives
        candidates = {}
        faction_prefix = faction_hint.upper() + '_' if faction_hint else ''

        for key, uid in self.orbat_reverse_lookup.items():
            # If we know the faction, only consider matching faction units
            if faction_prefix and not uid.startswith(faction_prefix):
                continue
            candidates[key] = uid

        if not candidates:
            # Fallback: search all units if faction filter yielded nothing
            candidates = self.orbat_reverse_lookup

        # Use difflib to find the best match
        matches = difflib.get_close_matches(name_lower, candidates.keys(), n=1, cutoff=0.85)

        if matches:
            return candidates[matches[0]]

        # 3. Try matching against the unit_id itself (in case LLM produced a close variant)
        id_candidates = list(self.orbat_whitelist)
        if faction_prefix:
            id_candidates = [uid for uid in id_candidates if uid.startswith(faction_prefix)]

        id_matches = difflib.get_close_matches(
            raw_name.upper().replace(' ', '_'),
            id_candidates, n=1, cutoff=0.85
        )

        if id_matches:
            return id_matches[0]

        return None
    async def _normalize_units_ai(self, raw_units, context_text):
        """
        Two-Step Hybrid Normalization:
        1. Local candidate search (Regex/Fuzzy)
        2. AI Disambiguation via minimax-m2.5:free
        """
        if not raw_units:
            return []

        import re
        import difflib
        normalized_results = []

        for unit in raw_units:
            raw_name = unit.get('raw_name', '')
            faction = unit.get('faction', 'UNK')
            if not raw_name: continue

            # 1. Local Candidate Search
            numbers = re.findall(r'\d+', raw_name)
            candidates = []
            faction_prefix = faction.upper() + '_' if faction in ['UKR', 'RUS'] else ''

            # Search by numbers
            if numbers:
                for num in numbers:
                    for key, uid in self.orbat_reverse_lookup.items():
                        if num in key and (not faction_prefix or uid.startswith(faction_prefix)):
                            candidates.append({"name": key, "id": uid})

            # If no number match or too many, try fuzzy as well
            keys = [k for k, v in self.orbat_reverse_lookup.items() if not faction_prefix or v.startswith(faction_prefix)]
            fuzzy_matches = difflib.get_close_matches(raw_name.lower(), keys, n=5, cutoff=0.6)
            for m in fuzzy_matches:
                candidates.append({"name": m, "id": self.orbat_reverse_lookup[m]})

            # De-duplicate candidates
            unique_candidates = []
            seen_ids = set()
            for c in candidates:
                if c['id'] not in seen_ids:
                    unique_candidates.append(c)
                    seen_ids.add(c['id'])

            unique_candidates = unique_candidates[:10] # Token safety

            # 2. AI Disambiguation
            if not unique_candidates:
                normalized_results.append({
                    "unit_id": None,
                    "unit_name": raw_name,
                    "faction": faction,
                    "status": "ENGAGED"
                })
                continue

            try:
                candidate_list_str = "\n".join([f"- {c['name']} (ID: {c['id']})" for c in unique_candidates])
                prompt = f"""
                You are a Military Intelligence Analyst.
                RAW TEXT CONTEXT: "{context_text[:2000]}"
                EXTRACTED UNIT NAME: "{raw_name}"

                POSSIBLE ORBAT MATCHES:
                {candidate_list_str}

                TASK:
                Pick the EXACT match from the ORBAT list that corresponds to the extracted unit based on context.
                If none match perfectly, pick the closest logical one or return 'NULL'.
                Return ONLY the unit ID.
                """

                async with API_SEMAPHORE:
                    response = await self._call_llm_with_backoff(self.brain_client,
                        model="minimax/minimax-m2.5:free",
                        messages=[
                            {"role": "system", "content": "Return ONLY the canonical unit ID or 'NULL'."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.0
                    )

                final_id = response.choices[0].message.content.strip().upper()
                if "NULL" in final_id or final_id not in self.orbat_whitelist:
                    final_id = None

                matched_name = raw_name
                if final_id:
                    for c in unique_candidates:
                        if c['id'] == final_id:
                            matched_name = c['name']
                            break

                normalized_results.append({
                    "unit_id": final_id,
                    "unit_name": matched_name.upper(),
                    "faction": faction,
                    "status": "ENGAGED"
                })
                print(f"      🔗 AI Normalized: '{raw_name}' -> {final_id or 'UNKNOWN'}")

            except Exception as e:
                print(f"   ⚠️ Normalization AI Error: {e}")
                normalized_results.append({"unit_id": None, "unit_name": raw_name, "faction": faction, "status": "ENGAGED"})

        return normalized_results

    # =========================================================================
    # STEP 0: THE BOUNCER v2.0 (Hybrid: Regex + AI)
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

    async def _step_0_the_bouncer(self, text):
        print("   Step 0: The Bouncer v2.0 analyzing...")

        # --- FASE 1: FILTRO MECCANICO (Gratis) ---
        is_junk, reason = self._is_obvious_junk(text)
        if is_junk:
            print(f"      REJECTED by Regex Sentry: {reason}")
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

            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.router_client,
                model="deepseek/deepseek-v4-flash",
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
            print(f"      BOUNCER EXCEPTION: {e}")
            # In caso di dubbio (errore API), lasciamo passare per non perdere dati
            return {"is_relevant": True, "reason": "Error Fallback"}

    async def _step_titan_classifier(self, text):
        """
        Chiama il modello Fine-Tuned per ottenere la classificazione precisa.
        """
        print("   Step 1.5: Titan Fine-Tuned is classifying...")

        # System Prompt Rinforzato (Quello validato prima)
        system_prompt = """You are a military intelligence analyst. Output strict JSON.
CRITICAL CLASSIFICATION RULES:
1. NOISE FILTER: If the text is a summary, historical analysis, political opinion, or static map, classify as NULL.
2. MANOUVRE PRIORITY: If text mentions territorial change (captured, retreated, entered), classify as MANOUVRE.
3. SHAPING PRIORITY: Strikes on deep rear targets, capitals, infrastructure, logistics -> SHAPING (OFFENSIVE/COERCIVE).
4. ATTRITION: Only for static fighting/shelling.

TASK 2: ESTIMATE METRICS (TITAN-10 PROTOCOL)
If classification is NOT NULL, you MUST estimate:
- kinetic_score (1-10): 1=Small Arms, 5=Tank/Grad, 7=Missile, 10=Nuke.
- target_score (1-10): 1=Field, 5=Tank, 8=AirDefense, 10=Command/Capital.
- effect_score (1-10): 1=Fail/Unknown, 5=Moderate Damage, 7=Destroyed.

OUTPUT FORMAT:
{
  "classification": "STRING",
  "kinetic_score": INTEGER,
  "target_score": INTEGER,
  "effect_score": INTEGER
}"""

        try:
            # Uses the constant with fallback to env var
            model_id = TITAN_MODEL_ID
            print(f"      🧬 Model: {model_id}")

            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.openai_client,
                model=model_id,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text[:25000]}
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
    # STEP 1: THE BRAIN (DeepSeek V3 via OpenRouter)
    # =========================================================================
    async def _step_1_the_brain(self, text, metadata):
        """
        Role: Strategy & Context.
        Analysis of raw clusters to determine relevance, actors, and bias.
        """
        print("   Step 1: The Brain (DeepSeek V3) analyzing strategy...")

        # Inject cached campaign catalog into prompt (built once at startup)
        campaign_catalog = globals().get("CAMPAIGN_CATALOG_TEXT", "No campaigns loaded.")

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

**PROTOCOL 5: UNIT EXTRACTION (CRITICAL FALLBACK)**
    - If the raw text mentions specific military units (e.g. "214th Assault Battalion", "82nd Airborne", "Kraken") that were NOT properly captured by The Soldier, YOU MUST extract them.
    - Output them as an array of objects containing `raw_name` and `faction` (UKR/RUS/UNK).

**PROTOCOL 6: CAMPAIGN INTELLIGENCE EXTRACTION (MANDATORY)**
    You MUST classify this event into one of the active strategic campaigns listed below.
    ONLY use these exact campaign_ids. Do NOT invent new campaign identifiers.

{campaign_catalog}

    Campaign Assignment Rules:
    1. Choose the SINGLE best-fit campaign_id based on STRATEGIC INTENT, not keyword coincidence.
    2. If no campaign fits or the event is non-kinetic noise, set campaign_id to null.
    3. confidence must be 0.0-1.0. Only campaign_ids with confidence >= 0.70 are accepted.
    4. destroyed_assets: ONLY assets EXPLICITLY mentioned as destroyed/damaged/captured in the raw text.
       - NEVER infer equipment type from unit names alone.
       - faction MUST have textual evidence (context, markings, unit attribution). If unknown -> "UNK".
       - count defaults to 1 unless the text explicitly states a different number.
       - state must be one of: DESTROYED, DAMAGED, CAPTURED, ABANDONED.
    5. If no assets are explicitly mentioned, return an empty destroyed_assets array.

**OUTPUT SCHEMA (JSON ONLY)**
{{
    "verification_status": boolean,
    "rejection_reason": "null or string (e.g. 'Fundraising')",
    "correction_notes": "String explaining corrections (e.g. 'Fixed wrong Actor from UKR to RUS')",
    "verified_units": [
        {{ "raw_name": "String (Exactly as written in text)", "faction": "UKR | RUS | UNK" }}
    ],
    "verified_data": {{
        "actor": "RUS | UKR | UNK",
        "reliability_score": int (0-100, based on calculation),
        "reliability_reasoning": "string (Explain the math: 'Base 30 + 20 Visual + 10 Specificity')",
        "is_hallucination": boolean,
        "correction_notes": "string",
        "ai_bias_estimate": int (-10 to 10),
        "location_precision_category": "string (EXACT_COORDINATES, CITY_LEVEL, REGION_LEVEL)",
        "strategic_value_assessment": "string",
        "event_category": "string",
        "implicit_signal": "Tactical summary",
        "corrected_coordinates": {{ "lat": float, "lon": float }},
        "verified_campaign": {{
            "campaign_id": "string (exact ID from campaign list) or null",
            "confidence": float (0.0-1.0),
            "reasoning": "string (max 80 words, strategic justification)",
            "destroyed_assets": [
                {{ "asset": "string", "faction": "RU | UA | UNK", "count": int, "state": "DESTROYED | DAMAGED | CAPTURED | ABANDONED" }}
            ]
        }}
    }}
}}
"""

        try:
            # Brain Reasoner (DeepSeek V4 Flash)
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.brain_client,
                model="deepseek/deepseek-v4-flash",

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
                response_format={"type": "json_object"},
                stream=True
                )

            full_content = ""
            full_reasoning = []
            reasoning_tokens = 0

            async for chunk in response:
                delta = chunk.choices[0].delta if chunk.choices else None
                if delta:
                    if getattr(delta, "content", None):
                        full_content += delta.content
                        print(delta.content, end="", flush=True)

                    reasoning_piece = getattr(delta, "reasoning", None)
                    if not reasoning_piece and hasattr(delta, "model_extra") and delta.model_extra:
                        reasoning_piece = delta.model_extra.get("reasoning")
                    if reasoning_piece:
                        full_reasoning.append(reasoning_piece)

                if hasattr(chunk, "usage") and chunk.usage:
                    reasoning_tokens = getattr(chunk.usage, "reasoningTokens", 0)

            print("\n")

            # Costruzione oggetto messaggio per chat history (con reasoning incluso)
            assistant_message = {
                "role": "assistant",
                "content": full_content,
                "reasoning_details": "".join(full_reasoning)
            }

            # Parsing della risposta
            content = full_content.strip()

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
                reasoning_trace = "".join(full_reasoning)
                if reasoning_trace:
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
    async def _repair_json_with_ai(self, broken_text, error_context):
        """
        Calls a fast model (GPT-4o-mini) to fix JSON syntax errors.

        INSTRUMENTED with CrashRecorder:
        - Logs successful repairs with before/after
        - Logs failures with full context for post-mortem
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
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.openai_client,
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": repair_prompt}],
                    temperature=0.0
                )
            fixed_text = response.choices[0].message.content.strip()

            # Remove residual backticks
            if "```" in fixed_text:
                fixed_text = fixed_text.replace(
                    "```json", "").replace("```", "").strip()

            # Attempt to parse the repaired JSON
            repaired_data = json.loads(fixed_text)

            # SUCCESS: Log the repair for analysis (helps understand common errors)
            print(f"   ✅ JSON Mechanic succeeded! Repaired {len(broken_text)} chars -> {len(fixed_text)} chars")

            return repaired_data

        except json.JSONDecodeError as repair_parse_error:
            # AI repair produced invalid JSON - log for forensics
            print(f"   ❌ JSON Mechanic repair produced invalid JSON: {repair_parse_error}")
            CrashRecorder.dump_state(
                context_name="_repair_json_with_ai.post_repair_failure",
                raw_input=broken_text,
                error=repair_parse_error,
                partial_data={
                    "original_error": error_context,
                    "ai_output": fixed_text[:1000] if 'fixed_text' in dir() else None
                }
            )
            return None

        except Exception as e:
            # API error or other failure - log with original context
            print(f"   ❌ JSON Mechanic Failed: {e}")
            CrashRecorder.dump_state(
                context_name="_repair_json_with_ai.api_failure",
                raw_input=broken_text,
                error=e,
                partial_data={"original_error": error_context}
            )
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
        except (TypeError, ValueError):
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

    async def _step_2_the_soldier(self, cluster_data):
        """
        Role: Strict Extraction from Cluster with Fallback Repair.

        UPGRADED with Geographic Sanity Loop:
        - Validates extracted coordinates against theatre of operations
        - Retries up to 3 times if coordinates are hallucinated
        - Falls back to null coordinates after exhausting retries
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

        # =====================================================================
        # GEOGRAPHIC SANITY LOOP
        # =====================================================================
        attempt = 0
        last_probe_result = None
        parsed_data = None

        while attempt < self.GEO_MAX_RETRIES:
            attempt += 1
            raw_response_text = ""

            try:
                # Build the prompt - add feedback if this is a retry
                if attempt == 1:
                    # First attempt - standard prompt
                    current_user_content = user_content
                else:
                    # Retry attempt - include feedback from GeoProbe
                    print(f"   🔄 Geographic Correction Attempt {attempt}/{self.GEO_MAX_RETRIES}...")
                    feedback_prompt = self.geo_probe.format_feedback_prompt(
                        combined_text, parsed_data, last_probe_result, attempt
                    )
                    current_user_content = feedback_prompt

                # LLM Call
                async with API_SEMAPHORE:
                    response = await self._call_llm_with_backoff(self.openrouter_client,
                    model="deepseek/deepseek-v4-flash",
                    messages=[
                        {"role": "system", "content": SOLDIER_SYSTEM_PROMPT},
                        {"role": "user", "content": current_user_content}
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"}
                    )
                raw_response_text = response.choices[0].message.content

                # Parse JSON
                parsed_data = self._clean_and_parse_json(raw_response_text)

                # AUTO-REPAIR if JSON is broken
                if not parsed_data:
                    print("   ⚠️ JSON Syntax Error detected. Calling Mechanic...")
                    parsed_data = await self._repair_json_with_ai(
                        raw_response_text, "Invalid JSON format")

                if not parsed_data:
                    print("   ❌ Soldier Failed (Unfixable JSON).")
                    return None

                # =============================================================
                # INSTRUMENTATION STEP: Validate Coordinates with GeoProbe
                # =============================================================
                # Guard against None parsed_data (can happen if repair failed)
                if not parsed_data:
                    print("   ⚠️ Parsed data is None after parse attempt, retrying...")
                    if attempt >= self.GEO_MAX_RETRIES:
                        return None
                    continue

                geo = parsed_data.get("geo_location", {}) or {}
                explicit = geo.get("explicit")

                # Check if we have explicit coordinates to validate
                if explicit:
                    lat = explicit.get('lat')
                    lon = explicit.get('lon')

                    # Skip validation for null/zero coordinates
                    is_invalid_lat = lat in [0, 0.0, "0", None, "null", ""]
                    is_invalid_lon = lon in [0, 0.0, "0", None, "null", ""]

                    if is_invalid_lat or is_invalid_lon:
                        # Clear invalid coordinates - no need to retry, toponym will be geocoded later
                        parsed_data["geo_location"]["explicit"] = None
                        print("   📍 No explicit coordinates extracted (null/zero). Will geocode toponym instead.")
                        break  # Exit loop - _step_geo_verifier will handle toponym geocoding
                    else:
                        # PROBE THE COORDINATES
                        probe_result = self.geo_probe.probe_coordinates(lat, lon)
                        last_probe_result = probe_result

                        if probe_result['is_valid']:
                            # SUCCESS - Coordinates are valid
                            print(f"   ✅ GeoProbe PASS: {probe_result['region']} ({probe_result['country_code'].upper()})")
                            parsed_data["geo_location"]["suspicious"] = probe_result.get('suspicious', False)
                            # Continue to sanity checks below and return
                            break
                        else:
                            # FAILURE - Coordinates are outside theatre
                            print(f"   ❌ GeoProbe FAIL: {probe_result['error_msg'][:100]}...")
                            if attempt < self.GEO_MAX_RETRIES:
                                # Loop will retry with feedback
                                continue
                            else:
                                # Exhausted retries - fallback to null
                                print(f"   ⚠️ Exhausted {self.GEO_MAX_RETRIES} attempts. Setting coordinates to null.")
                                parsed_data["geo_location"]["explicit"] = None
                                parsed_data["geo_location"]["_geo_validation_failed"] = True
                                parsed_data["geo_location"]["_last_error"] = probe_result['error_msg']
                                break
                else:
                    # No explicit coordinates - nothing to validate
                    print("   📍 No explicit coordinates in response.")
                    break

            except Exception as e:
                print(f"   ⚠️ Soldier Exception on attempt {attempt}: {e}")
                if raw_response_text:
                    print("   🔧 Attempting emergency repair on raw text...")
                    parsed_data = await self._repair_json_with_ai(raw_response_text, str(e))
                    if parsed_data:
                        break
                if attempt >= self.GEO_MAX_RETRIES:
                    return None

        # =================================================================
        # POST-LOOP CLEANUP: Sanity checks on toponym names
        # =================================================================
        if parsed_data:
            try:
                raw_loc = parsed_data.get("geo_location", {}).get(
                    "inferred", {}).get("toponym_raw")
                if raw_loc and isinstance(raw_loc, str):
                    # Clean lists (take first element)
                    if "," in raw_loc:
                        clean_loc = raw_loc.split(",")[0].strip()
                        parsed_data["geo_location"]["inferred"]["toponym_raw"] = clean_loc

                    # Clean lists with 'and' (e.g., "Kyiv and Lviv")
                    if " and " in raw_loc.lower():
                        clean_loc = raw_loc.lower().split(
                            " and ")[0].strip().title()
                        parsed_data["geo_location"]["inferred"]["toponym_raw"] = clean_loc
            except (KeyError, AttributeError, TypeError):
                pass

        return parsed_data

    # =========================================================================
    # 👁️ STEP VISIONARY: Conditional IMINT Verification & Equipment ID
    # =========================================================================
    # PIPELINE POSITION: After The Soldier → Before The Titan
    # ACTIVATION: CONDITIONAL — Only if event payload contains media files
    # MODEL: qwen/qwen3-vl-235b-a22b-instruct (MANDATORY HARD CONSTRAINT)
    # =========================================================================

    async def _step_visionary(self, soldier_data: dict, frame_dicts: list, audio_transcript: str = "") -> dict | None:
        """
        Role: Surgical IMINT Verification & Equipment ID with per-frame analysis.

        Activates ONLY when Base64-encoded keyframes are available.
        Cross-references The Soldier's text extraction against visual evidence.
        Identifies military hardware variants and assesses kinetic damage.
        Produces per-frame analysis for the IMINT Evidence Feed.

        Args:
            soldier_data (dict): Output from The Soldier (text-extracted intel).
            frame_dicts (list): List of enriched frame dicts from MediaProcessor,
                                each containing: base64_data, delta_score,
                                frame_index, selection_reason.

        Returns:
            dict: Structured IMINT report (with `analyzed_frames` array) or None on failure.
        """
        if not frame_dicts:
            return None

        # Build text context from Soldier's extraction (The Dossier)
        dossier_context = json.dumps(soldier_data, indent=2, default=str)[:8000]

        # Construct multimodal message content (text + Base64 images)
        # Format: array of content items per OpenRouter VLM spec
        content_items = [
            {
                "type": "text",
                "text": f"""You are the agent **The Visionary**. You are analyzing the 4 key frames (10%, 40%, 70%, 90%) of a video related to a specific tactical event.

TEXTUAL DOSSIER CONTEXT:
{dossier_context}

AUDIO TRANSCRIPTION:
{audio_transcript or 'No audio detected'}

MISSION: Cross-reference visual data with audio and textual context. Confirm or refute the dossier's claims and issue your objective IMINT assessment on weapons, vehicles, and battle damage (BDA).
SCRIVI TUTTO IN INGLESE, NO ITALIANO.

IMPORTANT: In addition to your standard analysis, add a "per_frame_analysis" array to your JSON output.
For EACH frame, provide:
{{
  "per_frame_analysis": [
    {{
      "frame_id": 1,
      "confidence": 0.0-1.0,
      "explanation": "What you see in THIS specific frame (1-2 sentences max)"
    }}
  ]
}}
Output ONLY valid JSON per your instructions."""
            }
        ]

        # Attach each Base64 frame as an image_url content item
        # Cap at 4 frames to avoid token overflow on the VLM
        for fd in frame_dicts[:4]:
            b64_url = fd.get('base64_data', '') if isinstance(fd, dict) else str(fd)
            content_items.append({
                "type": "image_url",
                "image_url": {"url": str(b64_url)}
            })

        try:
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.openrouter_client,
                model="qwen/qwen3-vl-235b-a22b-instruct",  # MANDATORY HARD CONSTRAINT
                messages=[
                    {"role": "system", "content": VISIONARY_SYSTEM_PROMPT},
                    {"role": "user", "content": content_items}
                ],
                temperature=0.0,  # Strict Determinism
                max_tokens=2048
                )

            raw_response = response.choices[0].message.content
            parsed = self._clean_and_parse_json(raw_response)

            if not parsed:
                print("   ⚠️ Visionary JSON parse failed. Attempting repair...")
                parsed = await self._repair_json_with_ai(raw_response, "Visionary output malformed")

            # Post-process: Map LLM analysis back to Base64 frames
            if parsed:
                llm_frames = parsed.get("analyzed_frames") or parsed.get("per_frame_analysis") or []
                if llm_frames:
                    for i, af in enumerate(llm_frames):
                        if i < len(frame_dicts):
                            af["base64_data"] = frame_dicts[i].get("base64_data")
                    parsed["analyzed_frames"] = llm_frames
                    if "per_frame_analysis" in parsed: del parsed["per_frame_analysis"]

            if parsed:
                # 0. TACTICAL FILTER PATCH: Discard non-military IMINT (maps, charts, talking heads)
                if not parsed.get('is_tactical_imint', True):
                    print("      ⚖️ Visionary Filter: Non-tactical media detected (map/chart/other). Discarding IMINT report.")
                    return None

                # 1. Log key findings
                v_status = parsed.get('visual_confirmation', {}).get('verification_status', 'UNKNOWN')
                v_conf = parsed.get('visual_confirmation', {}).get('confidence_score', 0)
                v_damage = parsed.get('kinetic_effect', {}).get('damage_level', 'UNKNOWN')
                assets = parsed.get('detected_assets', [])

                print(f"      \U0001f441\ufe0f VISIONARY VERDICT: {v_status} (Conf: {v_conf})")
                print(f"      \U0001f4a5 Kinetic Effect: {v_damage}")
                if assets:
                    for a in assets[:3]:
                        print(f"      \U0001f3af Detected: {a.get('type', '?')} [{a.get('faction', '?')}] x{a.get('count', '?')} \u2192 {a.get('state', '?')}")

                geo_clues = parsed.get('geo_clues', [])
                if geo_clues:
                    print(f"      \U0001f4cd Geo Clues: {geo_clues[:5]}")

                # --- BUILD ANALYZED_FRAMES: Merge VLM per-frame analysis with MediaProcessor metadata ---
                vlm_per_frame = parsed.pop('per_frame_analysis', [])  # Extract and remove from parsed
                analyzed_frames = []
                for i, fd in enumerate(frame_dicts[:4]):
                    frame_meta = fd if isinstance(fd, dict) else {"base64_data": str(fd)}
                    # Find matching VLM analysis for this frame (by frame_id = i+1)
                    vlm_match = next((pf for pf in vlm_per_frame if pf.get('frame_id') == i + 1), {})

                    analyzed_frames.append({
                        "frame_id": i + 1,
                        "confidence": vlm_match.get('confidence', v_conf),  # Fallback to aggregate confidence
                        "selection_reason": frame_meta.get('selection_reason', 'Keyframe'),
                        "explanation": vlm_match.get('explanation', parsed.get('visual_confirmation', {}).get('visual_summary', '')),
                        "base64_data": frame_meta.get('base64_data', ''),
                        "delta_score": frame_meta.get('delta_score', 0.0)
                    })

                parsed['analyzed_frames'] = analyzed_frames
                print(f"      📋 IMINT Feed: {len(analyzed_frames)} frames with per-frame analysis.")

                return parsed
            else:
                print("   \u274c Visionary Failed (Unfixable Response).")
                return None

        except Exception as e:
            print(f"   \u26a0\ufe0f Visionary Exception: {e}")
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

    async def _step_geo_verifier(self, location_name: str, context_text: str):
        """
        🌍 GEO-VERIFIER: Validates and corrects geolocation extracted by Soldier.

        Protects against:
        1. Metonymy Errors ("Moscow says" != "Strike on Moscow")
        2. Typos / OCR Errors
        3. Ambiguous Places (Multiple cities with same name)

        Returns: dict with {'lat': float, 'lon': float} or None
        """
        if not location_name or not isinstance(location_name, str):
            return None

        location_name = location_name.strip()
        print(f"      🌍 Geo-Verifier: Validating '{location_name}'...")

        # =====================================================================
        # STEP 1: SANITY CHECK (Local Python Logic - Zero API Cost)
        # =====================================================================
        clean_loc_lower = location_name.lower()
        is_suspicious = any(cap.lower() in clean_loc_lower for cap in self.SUSPICIOUS_CAPITALS)

        # Block generic country names from snapping to capital centroid
        GENERIC_COUNTRIES = ["ukraine", "russia", "romania", "poland", "belarus", "moldova", "usa", "us", "uk", "nato"]
        if clean_loc_lower in GENERIC_COUNTRIES:
            print(f"      ⚠️ COUNTRY-LEVEL EXTRACT DETECTED: '{location_name}'. Rejecting to prevent capital centroid snap.")
            return None

        cached_coords = await geo_cache_lookup(location_name)
        if cached_coords:
            print(f"      📍 GeoCache: Zero-latency match for '{location_name}'")
            return cached_coords

        if is_suspicious:
            # Check if context implies frontline combat (metonymy detection)
            context_lower = context_text.lower()
            is_frontline_event = any(kw in context_lower for kw in self.FRONTLINE_KEYWORDS)

            if is_frontline_event:
                print(f"      ⚠️ METONYMY DETECTED: '{location_name}' mentioned but context is frontline combat.")
                print("         → Skipping capital city. Triggering AI correction...")

                # Trigger AI correction to find the REAL target
                corrected_location = await self._ai_correct_location(location_name, context_text)
                if corrected_location:
                    location_name = corrected_location
                    print(f"      ✅ AI Corrected Location: '{corrected_location}'")
                else:
                    print("      ❌ AI could not determine real location. Returning None.")
                    return None

        # =====================================================================
        # STEP 1.5: LOCAL GAZETTEER FALLBACK (Zero API Cost)
        # =====================================================================
        local_coords = self.geo_probe.gazetteer_lookup(location_name)
        if local_coords:
            print(f"      📍 Local Gazetteer: Zero-latency match for '{location_name}'")
            await geo_cache_store(location_name, local_coords.get("lat"), local_coords.get("lon"))
            return local_coords

        # =====================================================================
        # STEP 2: PHOTON LOOKUP (Get Candidates)
        # =====================================================================
        # Photon lookup for zero-cost, high rate-limit fuzzy matching.

        candidates_list = []
        try:
            # Search Photon API (Prioritize UA by adding it to query if not present, though Photon handles it implicitly well)
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    "https://photon.komoot.io/api/",
                    params={"q": location_name, "limit": 5},
                )
                resp.raise_for_status()
                data = resp.json()

            if data and "features" in data:
                for i, f in enumerate(data["features"]):
                    props = f.get("properties", {})
                    coords = f.get("geometry", {}).get("coordinates", [])
                    if len(coords) == 2:
                        # Construct a display name similar to Geopy's address
                        address_parts = [props.get("name"), props.get("county"), props.get("state"), props.get("country")]
                        display_name = ", ".join([p for p in address_parts if p])

                        candidates_list.append({
                            'id': i,
                            'display_name': display_name,
                            'lat': coords[1],  # Photon returns [lon, lat]
                            'lon': coords[0]
                        })

            if not candidates_list:
                print(f"      ⚠️ Photon: No results for '{location_name}'")
                return None

            print(f"      📍 Photon: Found {len(candidates_list)} candidates")

        except Exception as e:
            print(f"      ❌ Photon Error: {e}")
            return None

        # =====================================================================
        # STEP 3: AI RERANKING & VALIDATION (DeepSeek Call)
        # =====================================================================
        if len(candidates_list) == 1:
            # Single result - verify it's within war zone
            result = candidates_list[0]
            if self._is_in_war_zone(result['lat'], result['lon']):
                await geo_cache_store(location_name, result['lat'], result['lon'])
                return {'lat': result['lat'], 'lon': result['lon']}
            else:
                print("      ⚠️ Single result outside war zone. Rejecting.")
                return None

        # Multiple candidates - use AI to pick the best one
        try:
            rerank_result = await self._ai_rerank_geo_candidates(
                location_name, context_text, candidates_list
            )

            if not rerank_result:
                # Fallback: use first result in war zone
                for c in candidates_list:
                    if self._is_in_war_zone(c['lat'], c['lon']):
                        print("      📍 Fallback: Using first war-zone candidate")
                        await geo_cache_store(location_name, c['lat'], c['lon'])
                        return {'lat': c['lat'], 'lon': c['lon']}
                return None

            # Handle WRONG_EXTRACTION response
            if rerank_result.get('status') == 'WRONG_EXTRACTION':
                corrected_name = rerank_result.get('correct_name')
                if corrected_name and corrected_name != location_name:
                    print(f"      🔄 AI says wrong target. Re-geocoding: '{corrected_name}'")
                    return await self._step_geo_verifier(corrected_name, context_text)
                return None

            # Handle selected_id response
            selected_id = rerank_result.get('selected_id')
            if selected_id is not None and 0 <= selected_id < len(candidates_list):
                chosen = candidates_list[selected_id]
                print(f"      ✅ AI Selected: {chosen['display_name'][:50]}...")
                await geo_cache_store(location_name, chosen['lat'], chosen['lon'])
                return {'lat': chosen['lat'], 'lon': chosen['lon']}

        except Exception as e:
            print(f"      ⚠️ AI Reranking Error: {e}")

        # Ultimate fallback
        for c in candidates_list:
            if self._is_in_war_zone(c['lat'], c['lon']):
                await geo_cache_store(location_name, c['lat'], c['lon'])
                return {'lat': c['lat'], 'lon': c['lon']}

        return None

    def _is_in_war_zone(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within the Ukraine/Russia war zone bounding box."""
        MIN_LAT, MAX_LAT = 44.0, 60.0
        MIN_LON, MAX_LON = 22.0, 55.0
        return MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON

    async def _ai_correct_location(self, wrong_location: str, context_text: str) -> str:
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
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.brain_client,
                    model="deepseek/deepseek-v4-flash",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    stream=True
                )

            full_content = ""
            full_reasoning = []

            async for chunk in response:
                delta = chunk.choices[0].delta if chunk.choices else None
                if delta:
                    if getattr(delta, "content", None):
                        full_content += delta.content

                    reasoning_piece = getattr(delta, "reasoning", None)
                    if not reasoning_piece and hasattr(delta, "model_extra") and delta.model_extra:
                        reasoning_piece = delta.model_extra.get("reasoning")
                    if reasoning_piece:
                        full_reasoning.append(reasoning_piece)

            assistant_message = {
                "role": "assistant",
                "content": full_content,
                "reasoning_details": "".join(full_reasoning)
            }

            result = json.loads(full_content.strip())
            return result.get('correct_location')
        except Exception as e:
            print(f"      ❌ AI Correction Error: {e}")
            return None

    async def _ai_rerank_geo_candidates(self, location_name: str, context_text: str,
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
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.brain_client,
                    model="deepseek/deepseek-v4-flash",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                    stream=True
                )

            full_content = ""
            full_reasoning = []

            async for chunk in response:
                delta = chunk.choices[0].delta if chunk.choices else None
                if delta:
                    if getattr(delta, "content", None):
                        full_content += delta.content

                    reasoning_piece = getattr(delta, "reasoning", None)
                    if not reasoning_piece and hasattr(delta, "model_extra") and delta.model_extra:
                        reasoning_piece = delta.model_extra.get("reasoning")
                    if reasoning_piece:
                        full_reasoning.append(reasoning_piece)

            assistant_message = {
                "role": "assistant",
                "content": full_content,
                "reasoning_details": "".join(full_reasoning)
            }

            content = full_content.strip()
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()

            return json.loads(content)
        except Exception as e:
            print(f"      ❌ AI Rerank Error: {e}")
            return None

    # 🧮 STEP 3: THE CALCULATOR (Python Deterministic Engine)

    def _clean_and_parse_json(self, raw_text):
        """
        SAFE PARSE PATTERN (Sanfilippo Method - Part 3)

        Implements 3-level fallback for JSON parsing with forensic logging:
        - Level 1: Standard json.loads()
        - Level 2: Heuristic cleaning (markdown, trailing commas)
        - Level 3: CrashRecorder dump for post-mortem analysis

        Returns:
            dict: Parsed JSON object, or None if all levels fail
        """
        # Store original for crash dump
        original_raw_text = raw_text

        # =====================================================================
        # LEVEL 1: Direct Parse Attempt
        # =====================================================================
        try:
            return json.loads(raw_text.strip())
        except json.JSONDecodeError:
            pass  # Expected for LLM output with markdown wrappers

        # =====================================================================
        # LEVEL 2: Heuristic Cleaning
        # =====================================================================
        print("   🧹 Level 2: Cleaning attempt triggered...")

        try:
            text = raw_text.strip()

            # 2a. Remove markdown backticks (e.g., ```json ... ```)
            if "```" in text:
                # Handle ```json\n...``` pattern
                parts = text.split("```")
                if len(parts) >= 2:
                    text = parts[1]
                    if text.startswith("json") or text.startswith("JSON"):
                        text = text[4:]
                    text = text.strip()

            # 2b. Surgical extraction: Find first '{' and last '}'
            start = text.find('{')
            end = text.rfind('}')

            if start != -1 and end != -1 and start < end:
                text = text[start:end + 1]

            # 2c. Fix trailing commas before closing braces/brackets
            # Common LLM error: {"key": "value",}
            text = re.sub(r',\s*}', '}', text)
            text = re.sub(r',\s*]', ']', text)

            # 2d. Attempt parse on cleaned text
            parsed = json.loads(text)
            print("   ✅ Level 2: Cleaning succeeded.")
            return parsed

        except json.JSONDecodeError as level_2_error:
            # =====================================================================
            # LEVEL 3: CRASH DUMP (Flight Data Recorder)
            # =====================================================================
            print("   🔴 Level 3: Parsing failed after cleaning. Recording crash dump...")

            # Trigger the CrashRecorder
            CrashRecorder.dump_state(
                context_name="_clean_and_parse_json",
                raw_input=original_raw_text,
                error=level_2_error,
                partial_data={"cleaned_length": len(text) if text else 0}
            )

            # Return None - caller should try _repair_json_with_ai as fallback
            return None

        except Exception as unexpected_error:
            # Catch-all for non-JSON errors (shouldn't happen, but safety first)
            print(f"   🔴 Unexpected error during parsing: {unexpected_error}")
            CrashRecorder.dump_state(
                context_name="_clean_and_parse_json.unexpected",
                raw_input=original_raw_text,
                error=unexpected_error,
                partial_data=None
            )
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
            if not src:
                continue
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
    def _get_error_journalist_response(self):
        """Fallback response when the Journalist step fails."""
        return {
            "title_en": "Event Processing Error",
            "description_en": "Data could not be summarized neutrally."
        }

    async def _step_4_the_journalist(self, text, brain_data, soldier_data):
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

        GDPR / OPSEC PII RULES (HIGHEST PRIORITY):
        - Do NOT output names/surnames of civilians, prisoners, individual soldiers, commanders, casualties, detainees, or license plates, UNLESS of famous RU or UA generals (i.e. Putin, Zelenskyi, Shoigu, Gerasimov, Zaluzhnyi, Syrskyi) or Leaders. APPLY this restriction only to PII from non-high ranking soldiers or civilians.
        - Replace personal identifiers with aggregate roles only: "civilian", "military personnel", "commander", "unit personnel", "vehicle", or "civilian vehicle".
        - Military unit names are allowed only when they identify formations, not individual people, UNLESS high-ranking military personnell or Leaders.
        - If raw text contains personal details, omit them completely from title and description, UNLESS high-ranking military personnell or Leaders.
        - Deterministic downstream redaction is only a fallback; your output must already be sanitized.

        INPUT CONTEXT (Verified AI Dossier):
        BRAIN_VERIFIED_DATA:
        {json.dumps(brain_data, ensure_ascii=False)[:6000]}

        SOLDIER_EXTRACTION:
        {json.dumps(soldier_data, ensure_ascii=False)[:8000]}

        DETECTED ACTORS:
        - Aggressor Side: {aggressor}
        - Target Side: {target}

        "DE-BIASING" RULES (STRICT):
        1. **SOURCE BIAS REMOVAL:** The source text is BIASED (e.g., Ukrainian sources call Russians "The Enemy", "Orcs", "Occupiers").
           - YOU MUST REPLACE "The Enemy" with the specific army name (e.g., "Russian Forces").
           - YOU MUST REPLACE "Our troops" with "Ukrainian Forces".

        2. **FORBIDDEN WORDS (Blacklist):**
           - NEVER use: "Enemy", "Foe", "Hero", "Terrorist", "Liberated", "Glorious", "Horde", "Criminals".
           - USE INSTEAD: "Adversary forces", "Personnel", "Retook control", "Advanced", "Group", "Units".

        3. **TONE:** Cold, clinical, robotic. No adjectives like "Brutal", "Massive", "Cynical". Just numbers and facts.

        OUTPUT REQUIREMENTS:
        1. **Title (EN):** [Who] [Action] [Where]. (e.g. "Russian Infantry Attack Repelled near Sotnytskyi Kozachok").
        2. **Description (EN):** Max 80 words. Focus on kinetics: movements, clashes, casualties.

        OUTPUT JSON:
        {{
            "title_en": "String",
            "description_en": "String"
        }}
        """

        try:
            # 1. Chiamata API (Temperatura 0 = Robotico)
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(self.openai_client,
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system",
                            "content": "You are a neutral database engine. JSON only. Never output personal data, names of individuals, or vehicle license plates; use aggregate roles only."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0
                )

            # 2. Parsing
            result_text = response.choices[0].message.content
            parsed_data = self._clean_and_parse_json(result_text)

            if not parsed_data:
                return self._get_error_journalist_response()

            # No Italian translation logic anymore
            return parsed_data

        except Exception as e:
            print(f"   ❌ Journalist Critical Error: {e}")
            return self._get_error_journalist_response()



    # =========================================================================
    # ♟️ STEP 5: THE STRATEGIST (Strategic Assessment)
    # =========================================================================

    async def _step_5_the_strategist(self, client_or, final_report):
        """
        THE STRATEGIST (DeepSeek-V4 via OpenRouter)
        Generates high-level strategic insight.
        """
        print("   ♟️  Step 5: The Strategist is assessing impact (Dual Lang)...")

        # 1. Prepare Data
        editorial = final_report.get('editorial', {})
        metrics = final_report.get('titan_metrics', {})

        # Tactical Dossier
        dossier = f"""
        EVENT: {editorial.get('title_en')}
        CONTEXT: {editorial.get('description_en')}

        === TACTICAL METRICS ===
        CATEGORY: {metrics.get('target_type_category', 'UNKNOWN')}
        KINETIC: {metrics.get('kinetic_score', 0)}
        TARGET: {metrics.get('target_score', 0)}
        EFFECT: {metrics.get('effect_score', 0)}
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
        5. BREVITY. Maximum 3 sentences. Tone: Cold, Professional, Direct.

        OUTPUT FORMAT (Strictly follow this):
        <Insight in English>
        """

        try:
            async with API_SEMAPHORE:
                response = await self._call_llm_with_backoff(client_or,
                    model="deepseek/deepseek-v4-flash",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": dossier}
                    ],
                    temperature=0.0,
                    stream=True
                )

            full_content = ""
            full_reasoning = []

            async for chunk in response:
                delta = chunk.choices[0].delta if chunk.choices else None
                if delta:
                    if getattr(delta, "content", None):
                        full_content += delta.content

                    reasoning_piece = getattr(delta, "reasoning", None)
                    if not reasoning_piece and hasattr(delta, "model_extra") and delta.model_extra:
                        reasoning_piece = delta.model_extra.get("reasoning")
                    if reasoning_piece:
                        full_reasoning.append(reasoning_piece)

            assistant_message = {
                "role": "assistant",
                "content": full_content,
                "reasoning_details": "".join(full_reasoning)
            }

            insight_raw = full_content.strip()
            print(f"      🧠 Strategist Output:\n{insight_raw}")
            return insight_raw

        except Exception as e:
            print(f"      ⚠️ Strategist Error: {e}")
            return "Analysis unavailable."

    # =========================================================================
    # 🔄 MAIN PROCESS FLOW
    # =========================================================================


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



def _normalize_geo_cache_key(query):
    return str(query or "").strip().lower()


def _geo_cache_lookup_sync(query):
    cache_key = _normalize_geo_cache_key(query)
    if not cache_key:
        return None
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS geo_cache (
                location_name TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        row = conn.execute(
            "SELECT lat, lon FROM geo_cache WHERE location_name = ?",
            (cache_key,),
        ).fetchone()
        if row:
            return {"lat": row[0], "lon": row[1]}
        return None
    finally:
        conn.close()


def _geo_cache_store_sync(query, lat, lon):
    cache_key = _normalize_geo_cache_key(query)
    if not cache_key or lat is None or lon is None:
        return
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS geo_cache (
                location_name TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute(
            """
            INSERT INTO geo_cache (location_name, lat, lon, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(location_name) DO UPDATE SET
                lat = excluded.lat,
                lon = excluded.lon,
                updated_at = CURRENT_TIMESTAMP
            """,
            (cache_key, float(lat), float(lon)),
        )
        conn.commit()
    finally:
        conn.close()


async def geo_cache_lookup(query):
    async with GEO_CACHE_LOCK:
        return await asyncio.to_thread(_geo_cache_lookup_sync, query)


async def geo_cache_store(query, lat, lon):
    async with GEO_CACHE_LOCK:
        await asyncio.to_thread(_geo_cache_store_sync, query, lat, lon)


async def safe_geocode(query, region=""):
    """
    Geocoding wrapper that uses the new GazetteerCache (SQLite)
    before falling back to external APIs.
    """
    if not query:
        return None, None

    gazetteer = get_gazetteer()
    if gazetteer is not None:
        try:
            lat, lon, canonical = await gazetteer.get_coordinates(query, region)
            if lat and lon:
                print(f"      [GEO] Gazetteer hit: '{query}' -> ({lat}, {lon}) [{canonical}]")
                return lat, lon
        except Exception as e:
            print(f"      [GEO] Gazetteer error: {e}")

    # Fallback to old simple cache/photon logic if gazetteer fails or is missing
    clean_query = str(query).lower().strip()
    cached = await geo_cache_lookup(clean_query)
    if cached:
        return cached["lat"], cached["lon"]

    # ... remaining legacy photon logic ...
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://photon.komoot.io/api/",
                params={"q": f"{query}, {region}, Ukraine", "limit": 1},
            )
            resp.raise_for_status()
            data = resp.json()

        for feature in data.get("features", []):
            coords = feature.get("geometry", {}).get("coordinates", [])
            if len(coords) == 2:
                lon, lat = float(coords[0]), float(coords[1])
                await geo_cache_store(clean_query, lat, lon)
                return lat, lon
    except (httpx.HTTPError, KeyError, TypeError, ValueError):
        pass

    return None, None


def safe_parse_list(val):
    if not val:
        return []
    if isinstance(val, list):
        return [str(x) for x in val if x]
    val_str = str(val).strip()
    if val_str.startswith('[') and val_str.endswith(']'):
        try:
            parsed = json.loads(val_str)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x]
        except Exception:
            pass
    if ' ||| ' in val_str:
        return [x.strip() for x in val_str.split(' ||| ') if x.strip()]
    if ' | ' in val_str:
        return [x.strip() for x in val_str.split(' | ') if x.strip()]
    return [val_str]


async def titan_sensor_scores(text):
    try:
        return await asyncio.to_thread(TitanSensor().analyze_text, text)
    except Exception as e:
        print(f"   [WARN] Trident sensor failed: {e}")
        return {}


def _mark_event_status_sync(event_id, status):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute(
            "UPDATE unique_events SET ai_analysis_status = ? WHERE event_id = ?",
            (status, event_id),
        )
        conn.commit()
    finally:
        conn.close()


def _write_final_report_sync(cluster_id, final_report, tie_result, titan_data, calc_result,
                             soldier_result, visionary_out, journo_result, actual_urls_list,
                             campaign_id, campaign_match_meta_json, campaign_tagged_at):
    report_text = json.dumps(final_report, ensure_ascii=False)
    titan_metrics_json = json.dumps(titan_data, ensure_ascii=False) if titan_data else None

    operational_sector = 'UNKNOWN_SECTOR'
    persist_lat = None
    persist_lon = None
    try:
        geo_data = (final_report.get('tactics') or {}).get('geo_location', {})
        explicit = (geo_data.get('explicit') or {}) if isinstance(geo_data, dict) else {}
        verified = (geo_data.get('verified') or {}) if isinstance(geo_data, dict) else {}
        inferred = (geo_data.get('inferred') or {}) if isinstance(geo_data, dict) else {}

        def _pick_pair(candidate):
            if not isinstance(candidate, dict):
                return None
            lat_raw = candidate.get('lat')
            lon_raw = candidate.get('lon')
            invalid_tokens = {None, "", "0", "0.0", "null", "none", "unknown", "n/a"}
            if str(lat_raw).strip().lower() in invalid_tokens or str(lon_raw).strip().lower() in invalid_tokens:
                return None
            try:
                lat_f = float(lat_raw)
                lon_f = float(lon_raw)
            except (TypeError, ValueError):
                return None
            if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
                return None
            return lat_f, lon_f

        for candidate in (explicit, verified, inferred):
            pair = _pick_pair(candidate)
            if pair:
                persist_lat, persist_lon = pair
                break

        sector_agent = get_geolocator()
        if sector_agent is not None and persist_lat is not None and persist_lon is not None:
            operational_sector = sector_agent.assign_sector(float(persist_lon), float(persist_lat))
    except Exception:
        operational_sector = 'UNKNOWN_SECTOR'

    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        cursor = conn.cursor()
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
                urls_list = ?,
                campaign_id = ?,
                campaign_match_meta = ?,
                campaign_tagged_at = ?,
                operational_sector = ?,
                lat = COALESCE(?, lat),
                lon = COALESCE(?, lon)
            WHERE event_id = ?
            """, (
                report_text,
                tie_result['value'],
                tie_result['status'],
                titan_metrics_json,
                titan_data.get('kinetic_score', 0),
                titan_data.get('target_score', 0),
                titan_data.get('effect_score', 0),
                calc_result.get('reliability', 0),
                calc_result.get('bias_score', 0),
                final_report.get('ai_summary', ''),
                1 if (soldier_result.get('visual_evidence') or visionary_out) else 0,
                journo_result.get('title_en', ''),
                journo_result.get('description_en', ''),
                ' | '.join(actual_urls_list) if actual_urls_list else '',
                campaign_id,
                campaign_match_meta_json,
                campaign_tagged_at,
                operational_sector,
                persist_lat,
                persist_lon,
                cluster_id
            ))
        conn.commit()
    finally:
        conn.close()


async def process_cluster_async(agent, row, campaign_definitions, db_write_lock):
    cluster_id = row['event_id']
    try:
        ref_date = row['last_seen_date']
        text_content = row['full_text_dossier'] if row.get('full_text_dossier') else ""
        all_msgs_raw = text_content.split(' ||| ')

        junk_keywords = [
            "bitcoin", "crypto", "ethereum", "nft ", "casino", "slot ", "betting",
            "sconto", "promo ", "offert", "liquidazione", "immobiliare", "affitto",
            "vendesi", "agenzia immobiliare", "kvartira",
            "oroscopo", "serie a", "champions league", "calciomercato"
        ]
        text_lower = text_content.lower()
        for word in junk_keywords:
            if word in text_lower:
                print(f"[SKIP] Evento scartato per keyword spazzatura: '{word}'")
                async with db_write_lock:
                    await asyncio.to_thread(_mark_event_status_sync, cluster_id, 'SKIPPED_JUNK')
                return

        bouncer_result = await agent._step_0_the_bouncer(text_content)
        if bouncer_result.get('is_relevant') is False:
            print(f"      [REJECTED] Bouncer: {bouncer_result.get('reason')}")
            async with db_write_lock:
                await asyncio.to_thread(_mark_event_status_sync, cluster_id, 'REJECTED')
            return

        selected_msgs = []
        current_total_chars = 0
        seen_hashes = set()
        for msg in all_msgs_raw:
            msg = msg.strip()
            if not msg:
                continue
            msg_hash = hash(msg)
            if msg_hash in seen_hashes:
                continue
            seen_hashes.add(msg_hash)
            if len(msg) > 3000:
                msg = msg[:3000] + "... [TRUNCATED]"
            if current_total_chars + len(msg) > 25000:
                break
            selected_msgs.append(msg)
            current_total_chars += len(msg)
            if len(selected_msgs) >= 12:
                break

        raw_msgs = selected_msgs
        print(f"\n[CLUSTER] Processing Cluster ID: {cluster_id}")
        print(f"   [OPT] {len(all_msgs_raw)} sources -> {len(raw_msgs)} selected (Len: {current_total_chars} chars)")
        combined_text = "\n".join(raw_msgs)
        cluster_data = {"reference_timestamp": ref_date, "raw_messages": raw_msgs}

        titan_task = asyncio.create_task(agent._step_titan_classifier(combined_text))
        soldier_task = asyncio.create_task(agent._step_2_the_soldier(cluster_data))
        sensor_task = asyncio.create_task(titan_sensor_scores(combined_text))
        titan_result, soldier_result, sensor_result = await asyncio.gather(
            titan_task,
            soldier_task,
            sensor_task,
        )

        trident_base_scores = {
            'kinetic_score': max(1, int(sensor_result.get('k_metric', 0.1) * 10)),
            'target_score': max(1, int(sensor_result.get('t_metric', 0.1) * 10)),
            'effect_score': max(1, int(sensor_result.get('e_metric', 0.1) * 10))
        }
        trident_classification = titan_result.get('classification', 'UNKNOWN')
        print(f"   [TITAN] Base: {trident_classification} | K={trident_base_scores['kinetic_score']}, T={trident_base_scores['target_score']}, E={trident_base_scores['effect_score']}")

        if not soldier_result:
            print("   [WARN] Soldier empty/failed. Escalating to Brain for recovery...")
            soldier_result = {"status": "FAILED_EXTRACTION"}

        visionary_out = None
        media_urls_raw = row.get('media_urls')
        if media_urls_raw:
            try:
                if isinstance(media_urls_raw, str):
                    media_urls_list = json.loads(media_urls_raw) if media_urls_raw.strip() else []
                elif isinstance(media_urls_raw, list):
                    media_urls_list = media_urls_raw
                else:
                    media_urls_list = []
            except (json.JSONDecodeError, ValueError):
                media_urls_list = []

            if media_urls_list:
                all_frame_dicts = []
                audio_transcript = ""
                if MediaProcessor is not None:
                    try:
                        media_proc = MediaProcessor()
                        for m_url in media_urls_list:
                            m_url_str = str(m_url)
                            # Extract geometric frames
                            frames = await asyncio.to_thread(media_proc.extract_keyframes, m_url_str)
                            all_frame_dicts.extend(frames)

                            # [NEW] Extract audio transcript (Whisper)
                            if not audio_transcript:
                                audio_transcript = await media_proc.extract_audio_transcript(m_url_str)

                            if len(all_frame_dicts) >= 4:
                                all_frame_dicts = all_frame_dicts[:4]
                                break
                    except Exception as mp_err:
                        print(f"      [WARN] MediaProcessor error: {mp_err}")
                if all_frame_dicts:
                    visionary_out = await agent._step_visionary(soldier_result, all_frame_dicts, audio_transcript)
                if visionary_out:
                    soldier_result['visual_evidence'] = True
                    imint_damage = visionary_out.get('kinetic_effect', {}).get('damage_level')
                    imint_confidence = visionary_out.get('visual_confirmation', {}).get('confidence_score', 0)
                    if imint_damage and imint_damage in VISIONARY_DAMAGE_TO_EFFECT and imint_confidence >= 0.5:
                        imint_effect = VISIONARY_DAMAGE_TO_EFFECT[imint_damage]
                        if 'titan_assessment' not in soldier_result:
                            soldier_result['titan_assessment'] = {}
                        soldier_result['titan_assessment']['effect_score'] = imint_effect
                        soldier_result['titan_assessment']['effect_source'] = 'VISIONARY_IMINT'
                    soldier_result['visionary_report'] = visionary_out

        titan_data = soldier_result.get('titan_assessment') or {}
        visual_evidence = soldier_result.get('visual_evidence', False)
        k_score = titan_data.get('kinetic_score', 0)
        if not k_score or int(k_score) == 0:
            titan_data['kinetic_score'] = trident_base_scores.get('kinetic_score', 1)
            titan_data['target_score'] = trident_base_scores.get('target_score', 1)
            titan_data['effect_score'] = trident_base_scores.get('effect_score', 1)
            titan_data['classification'] = trident_classification

        tie_result = agent._calculate_tie(titan_data, visual_evidence)
        print(f"      [TIE] {tie_result['value']} [{tie_result['status']}]")

        metadata_for_judge = {"Target Date": ref_date, "Soldier_Extraction": soldier_result}
        brain_review = await agent._step_1_the_brain(combined_text, metadata_for_judge)
        if not brain_review.get('verification_status', False):
            print(f"   [REJECTED] Brain invalidated event: {brain_review.get('rejection_reason', 'Unknown')}")
            async with db_write_lock:
                await asyncio.to_thread(_mark_event_status_sync, cluster_id, 'REJECTED')
            return
        if not isinstance(brain_review.get('verified_data'), dict):
            async with db_write_lock:
                await asyncio.to_thread(_mark_event_status_sync, cluster_id, 'REJECTED')
            return

        raw_units_to_normalize = []
        for u in soldier_result.get('military_units_detected', []):
            raw_units_to_normalize.append({"raw_name": u.get('unit_name') or u.get('unit_id'), "faction": u.get('faction', 'UNK')})
        for u in brain_review.get('verified_units', []):
            raw_units_to_normalize.append(u)
        if raw_units_to_normalize:
            normalized_units = await agent._normalize_units_ai(raw_units_to_normalize, combined_text)
            final_units = []
            seen_ids = set()
            for nu in normalized_units:
                uid = nu.get('unit_id')
                if uid:
                    if uid not in seen_ids:
                        final_units.append(nu)
                        seen_ids.add(uid)
                else:
                    final_units.append(nu)
            soldier_result['military_units_detected'] = final_units

        actual_sources_list = safe_parse_list(row.get('sources_list'))
        actual_urls_list = safe_parse_list(row.get('urls_list'))
        sources_for_calc = actual_sources_list + actual_urls_list
        calc_result = agent._step_3_the_calculator(
            soldier_data=soldier_result if soldier_result.get("status") != "FAILED_EXTRACTION" else {},
            brain_data=brain_review['verified_data'],
            source_name=sources_for_calc if sources_for_calc else ["Cluster Aggregated"],
            text=combined_text
        )
        journo_result = await agent._step_4_the_journalist(
            text="",
            brain_data=brain_review['verified_data'],
            soldier_data=soldier_result
        )

        final_report = {
            "cluster_id": cluster_id,
            "timestamp_generated": datetime.now().isoformat(),
            "status": "VERIFIED",
            "strategy": brain_review['verified_data'],
            "tactics": soldier_result,
            "scores": calc_result,
            "editorial": journo_result,
            "tie_score": tie_result['value'],
            "tie_status": tie_result['status'],
            "titan_metrics": titan_data,
            "Aggregated Sources": actual_urls_list,
            "visionary_report": visionary_out
        }
        final_report['ai_summary'] = await agent._step_5_the_strategist(agent.brain_client, final_report)

        # --- CAMPAIGN TAGGING (AI-First + Keyword Fallback) ---
        campaign_id = None
        campaign_match_meta_json = None
        campaign_tagged_at = None
        try:
            valid_campaign_ids = {c["campaign_id"] for c in campaign_definitions}

            # 1. AI-First: Use The Brain's verified_campaign extraction
            brain_campaign = (brain_review.get("verified_data") or {}).get("verified_campaign") or {}
            ai_campaign_id = brain_campaign.get("campaign_id")
            ai_confidence = 0.0
            try:
                ai_confidence = float(brain_campaign.get("confidence", 0))
            except (TypeError, ValueError):
                ai_confidence = 0.0

            if (ai_campaign_id
                    and str(ai_campaign_id).strip().lower() not in ("", "null", "none")
                    and str(ai_campaign_id).strip().lower() in valid_campaign_ids
                    and ai_confidence >= 0.70):
                campaign_id = str(ai_campaign_id).strip().lower()
                campaign_tagged_at = datetime.utcnow().isoformat() + "Z"
                campaign_match_meta_json = json.dumps({
                    "pipeline": "brain_inline_v1",
                    "campaign_id": campaign_id,
                    "confidence": ai_confidence,
                    "reasoning": str(brain_campaign.get("reasoning", "")).strip(),
                    "destroyed_assets": brain_campaign.get("destroyed_assets") or [],
                    "tagged_at": campaign_tagged_at,
                }, ensure_ascii=False)
                print(f"      [CAMPAIGN] AI assigned -> {campaign_id} (conf={ai_confidence:.2f})")
            else:
                # 2. Keyword Fallback: existing match_event_campaign
                if ai_campaign_id:
                    print(f"      [CAMPAIGN] AI suggested '{ai_campaign_id}' but rejected (conf={ai_confidence:.2f}, valid={str(ai_campaign_id).strip().lower() in valid_campaign_ids})")
                target_type_value = (
                    (titan_data or {}).get("target_type_category")
                    or (soldier_result.get("titan_assessment", {}) or {}).get("target_type_category")
                    or (brain_review.get("verified_data", {}) or {}).get("target_type")
                    or "unknown"
                )
                campaign_event_text = " ".join([
                    str(journo_result.get("title_en", "")),
                    str(journo_result.get("description_en", "")),
                    str(combined_text),
                ])
                campaign_match = match_event_campaign(
                    campaigns=campaign_definitions,
                    target_type=target_type_value,
                    event_text=campaign_event_text,
                )
                if campaign_match:
                    campaign_id = campaign_match.get("campaign_id")
                    campaign_tagged_at = datetime.utcnow().isoformat() + "Z"
                    campaign_match_meta_json = json.dumps({
                        "pipeline": "keyword_fallback_v1",
                        **campaign_match.get("match_meta", {}),
                        "destroyed_assets": [],
                        "tagged_at": campaign_tagged_at,
                    }, ensure_ascii=False)
                    print(f"      [CAMPAIGN] Keyword fallback -> {campaign_id}")

            # Write campaign data into final_report for downstream consumers
            final_report.setdefault("strategy", {})
            if campaign_id:
                campaign_def = next((c for c in campaign_definitions if c["campaign_id"] == campaign_id), None)
                final_report["strategy"]["campaign"] = {
                    "campaign_id": campaign_id,
                    "name": campaign_def.get("name") if campaign_def else campaign_id,
                    "color": campaign_def.get("color") if campaign_def else "#f59e0b",
                    "match_meta": json.loads(campaign_match_meta_json) if campaign_match_meta_json else {},
                    "tagged_at": campaign_tagged_at,
                }
            else:
                final_report["strategy"]["campaign"] = None
        except Exception as campaign_err:
            print(f"   [WARN] Campaign tagging error: {campaign_err}")

        try:
            tactics = final_report.get('tactics', {})
            geo = tactics.get('geo_location', {})
            if isinstance(geo, dict):
                if geo.get('explicit') is None:
                    geo['explicit'] = {}
                lat = geo['explicit'].get('lat')
                location_name = geo.get('inferred', {}).get('toponym_raw')
                region_name = geo.get('inferred', {}).get('region')
                if (not lat or lat == 0) and location_name and location_name != "Unknown":
                    banned_locations = ["Ukraine", "Russia", "Europe", "NATO", "EU", "Border", "Frontline", "Front", "Zone"]
                    if location_name.strip() not in banned_locations:
                        new_lat, new_lon = await safe_geocode(location_name, region_name)
                        if new_lat and new_lon:
                            final_report['tactics']['geo_location']['explicit']['lat'] = new_lat
                            final_report['tactics']['geo_location']['explicit']['lon'] = new_lon
        except Exception as e:
            print(f"   [WARN] Geo-Fixer: {e}")

        print(json.dumps(final_report, indent=2, ensure_ascii=False))
        async with db_write_lock:
            await asyncio.to_thread(_write_final_report_sync, cluster_id, final_report, tie_result, titan_data,
                                    calc_result, soldier_result, visionary_out, journo_result, actual_urls_list,
                                    campaign_id, campaign_match_meta_json, campaign_tagged_at)
        print("   [DB] Salvataggio completato (Golden Record Saved).")
    except Exception as e:
        print(f"   [ERROR] Error processing cluster {cluster_id}: {e}")


async def main():
    print("[START] STARTING SUPER SQUAD AGENT (SQLite Mode)...")

    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Database non trovato: {DB_PATH}")
        print("   Esegui prima 'scripts/refiner.py' per popolare il DB!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_campaign_columns(conn)

    migrations = [
        "ALTER TABLE events ADD COLUMN ai_report_json TEXT",
        "ALTER TABLE unique_events ADD COLUMN operational_sector TEXT",
        "ALTER TABLE unique_events ADD COLUMN image_phash TEXT",
        "ALTER TABLE unique_events ADD COLUMN source_reputation_score REAL",
        "ALTER TABLE unique_events ADD COLUMN lat REAL",
        "ALTER TABLE unique_events ADD COLUMN lon REAL",
        """CREATE TABLE IF NOT EXISTS geo_cache (
            location_name TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    ]
    for ddl in migrations:
        try:
            cursor.execute(ddl)
            conn.commit()
        except sqlite3.OperationalError:
            pass

    campaign_definitions = load_campaign_definitions(
        sheet_url=os.getenv("SHEET_CSV_URL", ""),
        cache_path=CAMPAIGN_DEFINITIONS_CACHE_PATH,
        tab_name="campaign_definitions",
    )
    print(f"[CAMPAIGNS] Loaded {len(campaign_definitions)} campaign definitions (sheet/cache).")

    # Build compact campaign catalog for Brain prompt injection (token-efficient)
    catalog_lines = ["ACTIVE STRATEGIC CAMPAIGNS (use ONLY these exact IDs):"]
    for cdef in campaign_definitions:
        kw_sample = ", ".join((cdef.get("keywords") or [])[:6])
        catalog_lines.append(
            f"- {cdef['campaign_id']}: {cdef.get('name', '')} (keywords: {kw_sample})"
        )
    campaign_catalog_text = "\n".join(catalog_lines)
    # Store globally so process_cluster_async can access it
    global CAMPAIGN_CATALOG_TEXT
    CAMPAIGN_CATALOG_TEXT = campaign_catalog_text
    print(f"[CAMPAIGNS] Campaign catalog built ({len(campaign_catalog_text)} chars, ~{len(campaign_catalog_text)//4} tokens).")

    agent = SuperSquadAgent()
    print("[DB] Reading pending clusters from SQLite...")

    try:
        cursor.execute("""
            SELECT * FROM unique_events
            WHERE ai_analysis_status = 'PENDING'
            ORDER BY last_seen_date DESC
            LIMIT 2000
        """)
        clusters_to_process = [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        print(f"[ERROR] Errore SQL: {e}")
        conn.close()
        return

    if not clusters_to_process:
        print("[OK] Nessun nuovo cluster da processare (Tutto aggiornato).")
        conn.close()
        return

    print(f"[QUEUE] Trovati {len(clusters_to_process)} cluster da analizzare.")
    conn.close()

    db_write_lock = asyncio.Lock()

    # --- SEQUENTIAL BATCH PROCESSING (Chunks of 50) ---
    # This prevents the terminal from being flooded with 2000+ bouncer messages at once.
    batch_size = 50
    total_events = len(clusters_to_process)

    for i in range(0, total_events, batch_size):
        batch = clusters_to_process[i:i + batch_size]
        current_batch_num = (i // batch_size) + 1
        total_batches = math.ceil(total_events / batch_size)

        print("\n" + "█"*80)
        print(f"📦 BATCH {current_batch_num}/{total_batches} | Processing {len(batch)} events concurrently")
        print("█"*80 + "\n")

        await asyncio.gather(*[
            process_cluster_async(agent, row, campaign_definitions, db_write_lock)
            for row in batch
        ])

        print(f"\n✅ BATCH {current_batch_num}/{total_batches} COMPLETED.")
        if i + batch_size < total_events:
            print("⏳ Waiting 2 seconds before next batch to stabilize output...")
            await asyncio.sleep(2)

    print("\n[DONE] Sessione conclusa.")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    load_dotenv()
    asyncio.run(main())
