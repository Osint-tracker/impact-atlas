# 🗺️ OSINT Tracker / Impact Atlas

**AI-powered military intelligence platform for monitoring the Russia-Ukraine conflict.**

Impact Atlas transforms unstructured OSINT data (Telegram, GDELT, news) into actionable intelligence through a multi-agent AI pipeline, displaying events on an interactive tactical map with real-time analysis.

![Status](https://img.shields.io/badge/Status-Production-green)
![Python](https://img.shields.io/badge/Python-3.12+-blue)
![License](https://img.shields.io/badge/License-Private-red)

---

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| **Super Squad AI Pipeline** | 6-agent chain: Bouncer → Brain → Soldier → Titan → Calculator → Journalist |
| **T.I.E. Scoring System** | Target-Kinetic-Effect vectors (1-10) for event intensity measurement |
| **Smart Fusion Engine** | Entity resolution & deduplication using vector embeddings |
| **ORBAT Tracker** | Real-time military unit tracking (UA/RU forces) |
| **Multi-Source Frontlines** | DeepState, ISW switchable conflict maps |
| **Equipment Losses Feed** | Live ticker aggregating WarSpotting, Oryx data |
| **Geographic Sanity Loop** | Self-healing coordinate validation (GeoProbe) |
| **Kinetic Plausibility Check** | Unit movement physics validation (HistoryProbe) |

---

## 🛠️ Tech Stack

| Layer | Technologies |
|-------|--------------|
| **Backend** | Python 3.12+, SQLite (WAL mode), OpenAI & OpenRouter APIs |
| **AI Models** | Fine-tuned GPT-4o-mini (Titan), DeepSeek V3, Qwen 2.5 |
| **Frontend** | Vanilla JS, Leaflet.js, Mapbox GL, Chart.js |
| **Data Sources** | Telegram (Telethon), GDELT, ACLED |
| **Hosting** | GitHub Pages (GitOps deployment) |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- API Keys: OpenAI, OpenRouter, Serper (optional)

### Installation
```bash
# Clone repository
git clone https://github.com/Osint-tracker/impact-atlas.git
cd osint-tracker

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys
```

### Run Pipeline
```bash
# 1. Ingest data (Telegram + GDELT)
python scripts/run_daily.py

# 2. Run AI analysis
python scripts/ai_agent.py

# 3. Export to GeoJSON
python scripts/generate_output.py

# 4. View locally
# Open index.html in browser
```

---

## 📁 Project Structure

```
osint-tracker/
├── assets/
│   ├── data/           # GeoJSON, JSON exports
│   ├── js/             # Frontend modules (map.js, charts.js, dashboard.js)
│   └── css/            # Stylesheets
├── scripts/            # Core production scripts
│   ├── ai_agent.py     # Main AI pipeline (Super Squad)
│   ├── generate_output.py  # GeoJSON export
│   ├── smart_fusion.py # Entity resolution
│   ├── geo_instrument.py   # GeoProbe validation
│   └── history_instrument.py  # HistoryProbe validation
├── ingestion/          # Data scrapers
├── war_tracker_v2/     # Event processing
│   ├── data/           # SQLite database
│   └── scripts/        # Refiner, event builder
├── training_finetuning/  # Model training datasets
├── scripts_una_tantum/ # One-time utility scripts
├── index.html          # Impact Atlas frontend
├── technical-spec_v1.3.md  # Full technical specification
└── GEOJSON_STRUCTURE.md    # Data schema documentation
```

---

## 📊 UI Views

| View | Purpose |
|------|---------|
| **TACTICAL** | Operational tempo gauge, intensity heatmap, equipment losses |
| **WAR ROOM** | Kanban board (Ground Ops, Air/Strike, Strategic) |
| **INTEL FEED** | Chronological event list with full dossier details |

---

## 📖 Documentation

- **[Technical Specification](technical-spec_v1.3.md)** - Full architecture, AI agents, schemas
- **[GeoJSON Structure](GEOJSON_STRUCTURE.md)** - Output data format reference

---

## 🧪 T.I.E. Score System

Events are scored on three 1-10 vectors:

| Vector | Measures | Example |
|--------|----------|---------|
| **K (Kinetic)** | Weapon magnitude | 1=Rifle, 5=Grad, 10=WMD |
| **T (Target)** | Target value | 1=Field, 5=Tank, 10=Capital |
| **E (Effect)** | Damage outcome | 1=Miss, 5=Damaged, 10=Destroyed |

**TIE Total** = K × T × E ÷ 10 (normalized 0-100)

---

## 📜 License

Private repository. All rights reserved.

---

*Maintained by Osint tracker | Last Updated: January 2026*