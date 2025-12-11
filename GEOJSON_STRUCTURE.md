# Struttura Events.GeoJSON

## Campi Obbligatori per ogni Feature

Il file `assets/data/events.geojson` deve contenere le seguenti proprietà per ogni feature:

### Struttura Base

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [longitude, latitude]
      },
      "properties": {
        "title": "string",
        "date": "string",
        "type": "string",
        "description": "string",
        "intensity": number,
        "link": "string (opzionale)",
        "location": "string",
        "verification": "string"
      }
    }
  ]
}
```

### Campi Dettagliati

#### 1. **title** (obbligatorio)
- **Tipo**: String
- **Descrizione**: Titolo dell'evento
- **Esempio**: `"Raffineria di Novokuibyshevsk"`

#### 2. **date** (obbligatorio)
- **Tipo**: String
- **Formato**: `DD/MM/YY` o `YYYY-MM-DD`
- **Esempio**: `"02/08/2025"` o `"2025-08-02"`

#### 3. **type** (obbligatorio)
- **Tipo**: String
- **Descrizione**: Tipo di attacco
- **Esempio**: `"Drones"`, `"Missiles"`, `"Artillery"`

#### 4. **description** (obbligatorio)
- **Tipo**: String
- **Descrizione**: Descrizione dettagliata dell'evento
- **Esempio**: `"Attacco con droni alla raffineria causando danni significativi"`
- **Nota**: Se non disponibile, usare stringa vuota `""` invece di `NaN`

#### 5. **intensity** (obbligatorio per heatmap)
- **Tipo**: Number
- **Range**: 0.0 - 1.0
- **Descrizione**: Intensità dell'evento per la visualizzazione heatmap
- **Valori suggeriti**:
  - `1.0` - Critico
  - `0.7` - Alto
  - `0.4` - Medio
  - `0.2` - Basso
- **Esempio**: `0.7`

#### 6. **link** (opzionale)
- **Tipo**: String (URL)
- **Descrizione**: Link alla fonte esterna
- **Esempio**: `"https://www.reuters.com/..."`
- **Nota**: Se non disponibile, usare `null` o stringa vuota invece di `NaN`

#### 7. **location** (consigliato)
- **Tipo**: String
- **Descrizione**: Descrizione testuale della località
- **Esempio**: `"Novokuibyshevsk, Oblast di Samara"`

#### 8. **verification** (consigliato)
- **Tipo**: String
- **Valori**: `"verified"` o `"not verified"`
- **Esempio**: `"verified"`

### Esempio Completo di Feature

```json
{
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [49.876177, 53.067561]
  },
  "properties": {
    "title": "Raffineria di Novokuibyshevsk",
    "date": "02/08/2025",
    "type": "Drones",
    "description": "Attacco con droni alla raffineria, operazioni sospese",
    "intensity": 0.7,
    "link": "https://www.reuters.com/business/energy/...",
    "location": "Novokuibyshevsk, Oblast di Samara",
    "verification": "verified"
  }
}
```

### Note Importanti

1. **Evitare valori `NaN`**: Usare `null`, `""`, o `0` invece di `NaN` per campi mancanti
2. **Coordinate**: Formato `[longitude, latitude]` (non invertire!)
3. **Intensity**: Se non specificato, il sistema calcolerà automaticamente in base a `verification` e `title`
4. **Date**: Supporta sia formato italiano `DD/MM/YY` che ISO `YYYY-MM-DD`

### Campi Attualmente nel File

Il file `events.geojson` esistente contiene:
- ✅ `title`
- ✅ `date`
- ✅ `type`
- ✅ `location`
- ⚠️ `source` (dovrebbe essere `link`)
- ⚠️ `notes` (dovrebbe essere `description`)
- ⚠️ `verification` (presente)
- ❌ `intensity` (MANCANTE - necessario per heatmap)

### Migrazione Consigliata

Per rendere il file completamente compatibile:

1. Rinominare `source` → `link`
2. Rinominare `notes` → `description`
3. Aggiungere campo `intensity` per ogni feature
4. Sostituire tutti i valori `NaN` con `null` o `""`

### Script di Conversione Suggerito

```python
import json

# Carica GeoJSON
with open('events.geojson', 'r') as f:
    data = json.load(f)

# Converti ogni feature
for feature in data['features']:
    props = feature['properties']
    
    # Rinomina campi
    if 'source' in props:
        props['link'] = props.pop('source')
    if 'notes' in props:
        props['description'] = props.pop('notes')
    
    # Aggiungi intensity se mancante
    if 'intensity' not in props:
        # Calcola in base a verification
        if props.get('verification') == 'verified':
            props['intensity'] = 0.7
        else:
            props['intensity'] = 0.2
    
    # Pulisci NaN
    for key in props:
        if props[key] == 'NaN' or str(props[key]) == 'nan':
            props[key] = None if key == 'link' else ''

# Salva
with open('events_fixed.geojson', 'w') as f:
    json.dump(data, f, indent=2)
```
