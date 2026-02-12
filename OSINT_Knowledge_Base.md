
---
doc_id: OSINT_RUSSIA_UKRAINE_ARCH_V1
doc_type: TECHNICAL_SPECIFICATION
target_audience: AI_AGENT_SCRAPER
context: MILITARY_INTELLIGENCE_AUTOMATION
status: PRODUCTION_READY
language: EN
---

# SYSTEM INSTRUCTION: OSINT DATA ARCHITECTURE
**Objective:** Utilize the following structural analysis to build automated ingestion pipelines for the Russia-Ukraine conflict.
**Constraint:** Prioritize STRUCTURED data (APIs, JSON) over UNSTRUCTURED data.
**Goal:** Construct a "Composite Architecture" merging Kinetic, Organizational, Geospatial, and Logistics truths.

---

## 1. EXECUTIVE SUMMARY & STRATEGY
**Context:** Transition from low-volume intelligence to high-volume data management.
**Core Task:** Extract, normalize, and synthesize disjointed datasets.
**Target Output:** Order of Battle (ORBAT), TO&E reconstruction, dynamic frontline mapping.

### KEY ARCHITECTURAL PRINCIPLES
1.  **Composite Architecture:** No single source is sufficient.
    * *Kinetic Attrition:* WarSpotting
    * *Force Structure:* MilitaryLand
    * *Geospatial:* DeepStateMap
    * *Personnel:* Mediazona/UALosses
2.  **Data Hierarchy:**
    * `PRIORITY_1`: APIs / JSON / GeoJSON
    * `PRIORITY_2`: Consistent HTML Tables / Lists
    * `PRIORITY_3`: Static Files (CSV/Excel)
3.  **Integration Challenges:** Multi-language (UA/RU/EN), Transliteration, Entity Resolution.

---

## 2. KINETIC INTELLIGENCE (Equipment Losses)
*Definition: Empirically verifiable metrics of combat intensity.*

### SOURCE A: WarSpotting.net (The API Standard)
* **Type:** `API_ENDPOINT`
* **Access:** Public
* **Method:** REST Polling

#### Endpoints
* **Full Dump:** `/api/releases/all`
* **Date Specific:** `/api/losses/russia/{YYYY-MM-DD}`

#### Data Schema (JSON)
```json
{
  "id": "Integer (Immutable Unique ID, e.g., 42695)",
  "model": "String (Nomenclature, e.g., 'T-72B3')",
  "status": "Enum ['Destroyed', 'Damaged', 'Abandoned', 'Captured']",
  "geo": {
    "lat": "Float",
    "lng": "Float"
  },
  "location": {
    "oblast": "String",
    "raion": "String",
    "settlement": "String"
  },
  "unit": "String (CRITICAL for ORBAT linking, e.g., '1st Tank Army')",
  "tags": ["Array of Strings", "e.g., 'Turret toss', 'Cope cage'"]
}

```

* **Agent Action:** Use `tags` for survivability analysis (feature flags). Use `geo` for native mapping.

### SOURCE B: Oryx (Verification Baseline)

* **Type:** `STATIC_HTML` / `CSV_REPO`
* **Access:** Public
* **Method:** Do not scrape Blogspot directly. Use maintained GitHub repositories.
* **Recommended Repo:** `PetroIvaniuk/2022-Ukraine-Russia-War-Dataset`
* **Key Value:** Distinguishes sub-variants (e.g., `T-64BV` vs `T-64BM Bulat`) for vintage analysis.

### SOURCE C: LostArmour.info (The "Red" Stream)

* **Type:** `HTML_PARSING`
* **Access:** Public (Pro-Russian bias, high technical fidelity)
* **Method:** Scrape specific tag pages.

#### Target Datasets

1. **Lancet Strikes:** `lostarmour.info/tags/lancet` (Target Type, Outcome, Tags like 'Thermal').
2. **FPV Strikes:** `lostarmour.info/tags/fpv` (Drone Model, Target Class).
3. **Inokhodets UAV:** Specific rear-area targets.

* **Unique Feature:** Unit Attribution (e.g., "328th Guards Airborne Regiment"). Use to map RU unit locations.

---

## 3. ORBAT & HIERARCHY (Organizational Structure)

*Definition: The skeletal structure defining WHO is fighting.*

### SOURCE A: MilitaryLand.net (Hierarchical Backbone)

* **Type:** `HTML_TREE`
* **Method:** Recursive Crawler (Tree Traversal).
* **Root Node:** `/ukraine/armed-forces/`

#### Traversal Logic

1. **Level 1 (Brigade):** Identify maneuver units (e.g., "1st Tank Brigade").
2. **Level 2 (Battalion):** Extract subordinate components.
3. **Level 3 (TO&E):** Parse `<ul>` lists for equipment (e.g., "18x Howitzer").
4. **Level 4 (History):** Text analysis of "History" section for lineage tracking (Reformation/Demobilization).

### SOURCE B: Russian ORBAT Context

* **Milkavkaz:** Archival baseline for Pre-war TO&E templates.
* **TopWar.ru:** Technical reforms (e.g., "Repair and Evacuation Regiments"). Requires NLP for keywords: `formed`, `regiment`, `equipped with`.
* **LostArmour Summaries:** Maps units to "Groups of Forces" (Sever, Zapad, Tsentr, Vostok).

---

## 4. GEOSPATIAL INTELLIGENCE (Frontline Tracking)

*Definition: Vector data sources for territorial control.*

### SOURCE A: DeepStateMap (Vector Gold Standard)

* **Type:** `API_ENDPOINT`
* **Format:** `GeoJSON`
* **Endpoint:** `/api/history/{timestamp}/geojson`

#### Layer Architecture

* `Occupied Territory`: Polygon (RU control).
* `Liberated Territory`: Polygon (UA counter-offensive).
* `Grey Zone`: Polygon (Contested).
* `Unit Icons`: Points (HQs, Assets).

### SOURCE B: NASA FIRMS (Remote Sensing)

* **Type:** `API`
* **Data:** Thermal Anomalies.
* **Logic:** Overlay FIRMS points on DeepState `Grey Zone`.
* *High Thermal Density* = Kinetic Engagement.
* *No Thermal* = Static Front.



### SOURCE C: Liveuamap (Event Driven)

* **Data:** Discrete points (Shelling, Airstrikes).
* **Logic:** Hostility Index. Clusters of shelling presage polygon changes.

---

## 5. PERSONNEL & COMMAND

*Definition: Statistical modeling and officer tracking.*

### SOURCE A: Mediazona & BBC Russian

* **Method:** Hidden JSON in infographic pages.
* **Metric:** Excess Male Mortality (Probate Registry Method).
* **Key Data:** Verified Named List (~44k+), Officer deaths (Generals/Colonels).

### SOURCE B: UALosses.org

* **Format:** Structured Database.
* **Metadata Fields:** `Rank`, `Unit`, `Region`, `Dates`.
* **Analysis:** Calculate attrition burden per Unit ID.

---

## 6. LOGISTICS & AID

*Definition: Material balance and replenishment rates.*

### SOURCE A: Kiel Institute Support Tracker

* **Format:** `Excel` / `CSV`
* **Metric:** In-Kind transfers (e.g., "14 Leopard 2").
* **Equation:** `Current_Strength = (Initial + Captured + Kiel_Aid) - Verified_Losses`.

---

## 7. TECHNICAL IMPLEMENTATION PIPELINE

### STRATEGY 1: SCRAPING ARCHITECTURES

1. **API Polling (Clean Path):**
* Target: WarSpotting, DeepState.
* Logic: Incremental fetching (store `last_fetched_id`). Use Time-Series DB.


2. **Iterative Parsing (Hard Path):**
* Target: LostArmour (ID Enumeration), MilitaryLand (Tree Traversal).
* Logic: Probe ID counters until `404 Error`.



### STRATEGY 2: ENTITY RESOLUTION

* **Problem:** Naming Inconsistency ("93rd Mech" vs "93 OMBr").
* **Solution:** **Master Entity Dictionary**.
* Map `UUID` <-> `[Alias_List]`.
* Implement Transliteration Library (Cyrillic to Latin).



---

## APPENDIX: MASTER SOURCE REGISTRY (JSON REPRESENTATION)

```json
[
  {
    "source": "WarSpotting",
    "domain": "Equipment Losses",
    "format": "JSON/API",
    "priority": "CRITICAL",
    "action": "POLL_API"
  },
  {
    "source": "DeepStateMap",
    "domain": "Geospatial",
    "format": "GeoJSON",
    "priority": "CRITICAL",
    "action": "FETCH_HISTORY"
  },
  {
    "source": "MilitaryLand",
    "domain": "ORBAT",
    "format": "HTML",
    "priority": "HIGH",
    "action": "CRAWL_TREE"
  },
  {
    "source": "LostArmour",
    "domain": "Weapon Application",
    "format": "HTML",
    "priority": "HIGH",
    "action": "PARSE_TABLES"
  },
  {
    "source": "Kiel Institute",
    "domain": "Logistics",
    "format": "XLSX",
    "priority": "MEDIUM",
    "action": "DOWNLOAD_FILE"
  },
  {
    "source": "Mediazona",
    "domain": "Casualties",
    "format": "Hidden JSON",
    "priority": "MEDIUM",
    "action": "EXTRACT_PAYLOAD"
  }
]

```

```

```