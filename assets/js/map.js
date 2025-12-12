// ============================================
// MAP.JS - RESTRUCTURED WITH PROPER INITIALIZATION
// ============================================

(function () {
  'use strict';

  // ============================================
  // 1. GLOBAL STATE (Declared First)
  // ============================================
  let map;
  let eventsLayer;
  let heatLayer = null;
  let isHeatmapMode = false;
  let currentFrontlineLayer = null;
  let historicalFrontlineLayer = null;
  let firmsLayer = null;

  window.allEventsData = [];
  window.globalEvents = [];
  window.currentFilteredEvents = [];

  let mapDates = []; // Historical dates index

  // Helper centrale per definire cosa è civile
  function isCivilianEvent(e) {
    // Unisce tutti i campi di testo per cercare parole chiave
    const fullText = (e.category + ' ' + e.type + ' ' + e.location_precision + ' ' + e.filters).toUpperCase();

    // Parole che identificano un evento NON strettamente militare/cinetico
    const civKeywords = ['CIVIL', 'POLITIC', 'ECONOM', 'HUMANITAR', 'DIPLOMA', 'ACCIDENT', 'STATEMENT'];

    // Se trova una di queste parole, è civile
    if (civKeywords.some(k => fullText.includes(k))) return true;

    // Opzionale: esclude tutto ciò che non è in UA/RU (Geofencing grezzo)
    // if (e.lat < 44 || e.lat > 57 || e.lon < 22 || e.lon > 50) return true; 

    return false;
  }

  // ============================================
  // 2. CONFIGURATION
  // ============================================
  const impactColors = {
    'critical': '#ef4444',
    'high': '#f97316',
    'medium': '#eab308',
    'low': '#64748b'
  };

  const typeIcons = {
    'drone': 'fa-plane-up',
    'missile': 'fa-rocket',
    'artillery': 'fa-bomb',
    'energy': 'fa-bolt',
    'fire': 'fa-fire',
    'naval': 'fa-anchor',
    'cultural': 'fa-landmark',
    'eco': 'fa-leaf',
    'default': 'fa-crosshairs'
  };

  // ============================================
  // 3. HELPER FUNCTIONS (Define Before Use)
  // ============================================

  function getColor(val) {
    const v = val || 0.2;
    if (v >= 0.8) return impactColors.critical;
    if (v >= 0.6) return impactColors.high;
    if (v >= 0.4) return impactColors.medium;
    return impactColors.low;
  }

  function getIconClass(type) {
    if (!type) return typeIcons.default;
    const t = type.toLowerCase();
    for (const [key, icon] of Object.entries(typeIcons)) {
      if (t.includes(key)) return icon;
    }
    return typeIcons.default;
  }

  function createMarker(e) {
    const color = getColor(e.intensity);
    const iconClass = getIconClass(e.type);
    const size = (e.intensity || 0.2) >= 0.8 ? 34 : 26;
    const iconSize = Math.floor(size / 1.8);

    const marker = L.marker([e.lat, e.lon], {
      icon: L.divIcon({
        className: 'custom-icon-marker',
        html: `<div style="background-color: ${color}; width: ${size}px; height: ${size}px; border-radius: 50%; border: 2px solid #1e293b; box-shadow: 0 0 10px ${color}66; display: flex; align-items: center; justify-content: center; color: #1e293b;"><i class="fa-solid ${iconClass}" style="font-size:${iconSize}px;"></i></div>`,
        iconSize: [size, size]
      })
    });

    marker.bindPopup(createPopupContent(e));
    return marker;
  }

  function createPopupContent(e) {
    // 1. Recupera l'ID in modo sicuro (fondamentale per il fix del click)
    const safeId = e.event_id || e.properties?.event_id;

    // 2. Colore per la barra laterale
    const color = getColor(e.intensity);

    // 3. Gestione Footer Fonte (con nuovo stile)
    let sourceFooter = '';
    if (e.source && e.source !== 'Unknown Source') {
      const url = e.source.startsWith('http') ? e.source : '#';
      let domain = "Fonte Originale";
      try {
        if (url !== '#') domain = new URL(url).hostname.replace('www.', '');
      } catch (err) { }

      sourceFooter = `
            <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #334155; display: flex; align-items: center; justify-content: space-between;">
              <span style="font-size: 0.7rem; color: #64748b;">Fonte:</span>
              <a href="${url}" target="_blank" class="source-link-styled">
                 <i class="fa-solid fa-link"></i> ${domain}
              </a>
            </div>`;
    }
    const id = e.event_id || e.id;
    // 4. Costruzione HTML Popup
    return `
    <div class="acled-popup" style="color:#e2e8f0; font-family: 'Inter', sans-serif; min-width: 260px;">
      
      <div style="border-left: 4px solid ${color}; padding-left: 12px; margin-bottom: 12px;">
        <h5 style="margin:0; font-weight:700; font-size:1rem; line-height:1.3; color:#f8fafc;">${e.title}</h5>
        
        <div style="margin-top:8px; display:flex; gap:8px;">
          <span class="popup-meta-tag"><i class="fa-regular fa-calendar"></i> ${e.date}</span>
          <span class="popup-meta-tag"><i class="fa-solid fa-tag"></i> ${e.type}</span>
        </div>
      </div>

      <div style="font-size:0.85rem; line-height:1.6; color:#cbd5e1; margin-bottom:15px;">
        ${e.description ? (e.description.length > 120 ? e.description.substring(0, 120) + '...' : e.description) : 'Nessuna descrizione.'}
      </div>
      
      <div class="popup-actions">
        <button 
          class="btn-tactical" 
          onclick="openModal('${id}')"> 
          <i class="fas fa-folder-open"></i> APRI DOSSIER
        </button>
      </div>
      
      ${sourceFooter}
    </div>`;
  }

  // ============================================
  // 4. RENDERING FUNCTIONS
  // ============================================

  function renderInternal(eventsToDraw) {
    eventsLayer.clearLayers();
    if (heatLayer) map.removeLayer(heatLayer);

    if (isHeatmapMode) {
      if (typeof L.heatLayer === 'undefined') return;
      const heatPoints = eventsToDraw.map(e => [e.lat, e.lon, (e.intensity || 0.5) * 2]);
      heatLayer = L.heatLayer(heatPoints, {
        radius: 25,
        blur: 15,
        maxZoom: 10,
        gradient: { 0.4: 'blue', 0.6: '#00ff00', 0.8: 'yellow', 1.0: 'red' }
      }).addTo(map);
    } else {
      const markers = eventsToDraw.map(e => createMarker(e));
      eventsLayer.addLayers(markers);
      map.addLayer(eventsLayer);
    }

    if (document.getElementById('eventCount')) {
      document.getElementById('eventCount').innerText = eventsToDraw.length;
    }

    console.log(`✅ Rendered ${eventsToDraw.length} events on map`);
  }

  // ============================================
  // 5. TIME TRAVEL FUNCTIONS
  // ============================================

  function loadHistoricalMap(dateString) {
    const url = `assets/data/history/frontline_${dateString}.geojson`;

    fetch(url)
      .then(response => {
        if (!response.ok) return null;
        return response.json();
      })
      .then(data => {
        if (!data) return;

        // Remove old historical layer
        if (historicalFrontlineLayer) {
          map.removeLayer(historicalFrontlineLayer);
        }

        // Remove current frontline layer to show historical
        if (currentFrontlineLayer && map.hasLayer(currentFrontlineLayer)) {
          map.removeLayer(currentFrontlineLayer);
        }

        // Add historical layer
        historicalFrontlineLayer = L.geoJSON(data, {
          style: {
            color: "#d32f2f",
            weight: 3,
            opacity: 0.8,
            fillOpacity: 0.35,
            className: 'historical-line'
          },
          onEachFeature: function (feature, layer) {
            layer.bindPopup(`<b>Situazione al:</b> ${dateString}<br>Territorio occupato`);
          }
        }).addTo(map);

        console.log(`🗺️ Historical map loaded: ${dateString}`);
      })
      .catch(err => console.error("Error loading historical map:", err));
  }

  function filterEventsByDate(dateString) {
    const targetDate = moment(dateString, 'YYYY-MM-DD');
    if (!targetDate.isValid()) {
      console.error("Invalid date:", dateString);
      return;
    }

    const targetTimestamp = targetDate.valueOf();
    const filtered = window.globalEvents.filter(e => e.timestamp <= targetTimestamp);

    window.currentFilteredEvents = filtered;
    renderInternal(filtered);

    console.log(`📅 Filtered to ${filtered.length} events up to ${dateString}`);
  }

  function loadFrontlineLayer(url, color) {
    if (currentFrontlineLayer) {
      map.removeLayer(currentFrontlineLayer);
    }

    // Remove historical layer when loading current
    if (historicalFrontlineLayer) {
      map.removeLayer(historicalFrontlineLayer);
      historicalFrontlineLayer = null;
    }

    fetch(url)
      .then(response => {
        if (!response.ok) throw new Error("Map file not found: " + url);
        return response.json();
      })
      .then(data => {
        currentFrontlineLayer = L.geoJSON(data, {
          style: function () {
            return {
              color: color,
              weight: 2,
              opacity: 0.8,
              fillOpacity: 0.1
            };
          },
          onEachFeature: function (feature, layer) {
            if (feature.properties && feature.properties.name) {
              layer.bindPopup(feature.properties.name);
            }
          }
        }).addTo(map);
        console.log("✅ Frontline loaded:", url);
      })
      .catch(err => console.error("❌ Error loading frontline:", err));
  }

  // ============================================
  // 6. SLIDER SETUP
  // ============================================

  function setupTimeSlider(allData) {
    const slider = document.getElementById('timeSlider');
    const startLabel = document.getElementById('sliderStartDate');
    const display = document.getElementById('sliderCurrentDate');

    if (!allData.length || !slider) return;

    const timestamps = allData.map(d => d.timestamp).filter(t => t > 0);
    const minTime = Math.min(...timestamps);
    const maxTime = Math.max(...timestamps);

    slider.min = minTime;
    slider.max = maxTime;
    slider.value = maxTime;
    slider.disabled = false;

    startLabel.innerText = moment(minTime).format('DD/MM/YYYY');
    display.innerText = "LIVE";

    // Load historical dates index
    fetch('assets/data/map_dates.json')
      .then(res => res.json())
      .then(dates => {
        mapDates = dates;
        console.log("📅 Historical dates loaded:", mapDates.length);

        const dateSlider = document.getElementById('date-slider');
        const dateLabel = document.getElementById('date-label');

        if (dateSlider && mapDates.length > 0) {
          dateSlider.max = mapDates.length - 1;
          dateSlider.value = mapDates.length - 1;

          const latestDate = mapDates[mapDates.length - 1];
          if (dateLabel) dateLabel.innerText = latestDate;

          loadHistoricalMap(latestDate);
          filterEventsByDate(latestDate);

          dateSlider.addEventListener('input', function (e) {
            const index = parseInt(e.target.value);
            const selectedDate = mapDates[index];

            if (dateLabel) dateLabel.innerText = selectedDate;
            loadHistoricalMap(selectedDate);
            filterEventsByDate(selectedDate);
          });
        }
      })
      .catch(err => console.error("Failed to load map dates:", err));

    // Standard time slider
    slider.addEventListener('input', function (e) {
      const currentSliderVal = parseInt(e.target.value);
      const timeFiltered = window.currentFilteredEvents.filter(ev => ev.timestamp <= currentSliderVal);
      renderInternal(timeFiltered);

      const dateStr = moment(currentSliderVal).format('DD/MM/YYYY');
      display.innerText = currentSliderVal >= maxTime ? "LIVE" : dateStr;
    });
  }

  // ============================================
  // 7. MAP INITIALIZATION
  // ============================================

  function initMap() {
    map = L.map('map', {
      zoomControl: false,
      preferCanvas: true,
      wheelPxPerZoomLevel: 120
    }).setView([48.5, 32.0], 6);

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19,
      attribution: '&copy; IMPACT ATLAS'
    }).addTo(map);

    eventsLayer = L.markerClusterGroup({
      chunkedLoading: true,
      maxClusterRadius: 45,
      spiderfyOnMaxZoom: true,
      iconCreateFunction: function (cluster) {
        const count = cluster.getChildCount();
        const size = count < 10 ? 'small' : (count < 100 ? 'medium' : 'large');
        return new L.DivIcon({
          html: `<div><span>${count}</span></div>`,
          className: `marker-cluster marker-cluster-${size}`,
          iconSize: new L.Point(40, 40)
        });
      }
    });
    map.addLayer(eventsLayer);

    // Load default frontline
    loadFrontlineLayer('assets/data/frontline.geojson', '#f59e0b');

    console.log("✅ Map initialized");
  }

  // ============================================
  // 8. DATA LOADING (Critical - Runs After Map Init)
  // ============================================

  function loadEventsData() {
    console.log("📥 Starting event download...");

    fetch('assets/data/events.geojson')
      .then(response => response.json())
      .then(data => {
        // 1. Dati Grezzi
        window.allEventsData = data.features || data;
        console.log(`💾 Data downloaded: ${window.allEventsData.length} raw events`);

        if (window.allEventsData.length === 0) {
          console.warn("⚠️ No events found in GeoJSON");
          return;
        }

        // 2. PROCESSAMENTO (Unico ciclo map corretto)
        window.globalEvents = window.allEventsData.map(f => {
          // Logica Moment.js
          const props = f.properties || f;

          // Tentativo con formati espliciti
          let m = moment(props.date, ["DD/MM/YY", "DD/MM/YYYY", "YYYY-MM-DD", "DD-MM-YYYY"]);

          // Fallback se non valido
          if (!m.isValid()) {
            m = moment(props.date);
          }

          const ts = m.isValid() ? m.valueOf() : moment().valueOf();

          return {
            ...props,
            lat: f.geometry ? f.geometry.coordinates[1] : props.lat,
            lon: f.geometry ? f.geometry.coordinates[0] : props.lon,
            timestamp: ts,
            // Normalizziamo la data per la visualizzazione
            date: m.isValid() ? m.format("DD/MM/YYYY") : props.date
          };
        }).sort((a, b) => b.timestamp - a.timestamp); // Ordine decrescente

        console.log(`✅ Events processed: ${window.globalEvents.length}`);

        // 3. DEFINIZIONE FILTRI (Dentro il then)
        window.applyMapFilters = function () {
          // Controllo difensivo se l'elemento esiste
          const toggle = document.getElementById('civilianToggle');
          const showCivilian = toggle ? toggle.checked : true;

          const filtered = window.globalEvents.filter(e => {
            if (typeof isCivilianEvent !== 'function') return true; // Fallback se manca la funzione helper
            const isCivil = isCivilianEvent(e);
            if (isCivil && !showCivilian) return false;
            return true;
          });

          window.currentFilteredEvents = filtered;
          renderInternal(filtered);
        };

        // UI Updates
        window.currentFilteredEvents = [...window.globalEvents];

        if (document.getElementById('eventCount')) {
          document.getElementById('eventCount').innerText = window.globalEvents.length;
          document.getElementById('lastUpdate').innerText = new Date().toLocaleDateString();
        }

        // Charts Init
        try {
          if (typeof window.initCharts === 'function') window.initCharts(window.globalEvents);
        } catch (e) { console.log("Charts error:", e); }

        console.log(`✅ Events processed: ${window.globalEvents.length} ready for map`);

        // Slider Init
        if (typeof setupTimeSlider === 'function') setupTimeSlider(window.globalEvents);

        // 4. AVVIO MAPPA E RENDERING
        if (typeof window.applyMapFilters === 'function') {
          window.applyMapFilters();
        } else {
          renderInternal(window.globalEvents);
        }

        // Inizializza Cluster/Map
        initMap(window.globalEvents);

      }) // <--- QUESTA CHIUDE IL .THEN (Il punto critico degli errori precedenti)
      .catch(err => {
        console.error("❌ CRITICAL: Failed to load events:", err);
      });
  }

  // ============================================
  // 9. PUBLIC API (Expose to Window)
  // ============================================



  window.updateMap = function (events) {
    window.currentFilteredEvents = events;
    renderInternal(events);
  }

  window.loadHistoricalMap = loadHistoricalMap;
  window.filterEventsByDate = filterEventsByDate;

  window.toggleVisualMode = function () {
    isHeatmapMode = !isHeatmapMode;
    const btn = document.getElementById('heatmapToggle');

    if (isHeatmapMode) {
      btn.classList.add('active');
      btn.innerHTML = '<i class="fa-solid fa-circle-nodes"></i> Cluster';
    } else {
      btn.classList.remove('active');
      btn.innerHTML = '<i class="fa-solid fa-layer-group"></i> Heatmap';
    }

    renderInternal(window.currentFilteredEvents);
  };

  window.selectMapSource = function (card, sourceName) {
    document.querySelectorAll('.map-layer-card').forEach(c => {
      c.classList.remove('active');
      const icon = c.querySelector('.status-dot');
      if (icon) {
        icon.classList.remove('fa-circle-dot', 'fa-solid');
        icon.classList.add('fa-circle', 'fa-regular');
      }
    });

    if (card) {
      card.classList.add('active');
      const activeIcon = card.querySelector('.status-dot');
      if (activeIcon) {
        activeIcon.classList.remove('fa-circle', 'fa-regular');
        activeIcon.classList.add('fa-circle-dot', 'fa-solid');
      }
    }

    console.log(`🔄 Switching map source: ${sourceName}`);

    let dataUrl = '';
    let colorStyle = '#ff3838';

    if (sourceName === 'deepstate') {
      dataUrl = 'assets/data/frontline.geojson';
      colorStyle = '#f59e0b';
    } else if (sourceName === 'isw') {
      dataUrl = 'assets/data/frontline_isw.geojson';
      colorStyle = '#38bdf8';
    }

    loadFrontlineLayer(dataUrl, colorStyle);
  };

  window.toggleTechLayer = function (layerName, checkbox) {
    const isChecked = checkbox.checked;
    console.log(`Toggle ${layerName}: ${isChecked}`);

    if (layerName === 'firms') {
      if (isChecked) {
        firmsLayer = L.tileLayer('https://map1.vis.earthdata.nasa.gov/wmts-webmerc/VIIRS_SNPP_Fires_375m_Day_Night/default/{time}/GoogleMapsCompatible_Level8/{z}/{y}/{x}.png', {
          attribution: 'NASA FIRMS',
          maxZoom: 12,
          minZoom: 6,
          time: moment().format('YYYY-MM-DD'),
          opacity: 0.7,
          bounds: [[44.0, 22.0], [53.0, 40.0]]
        }).addTo(map);
      } else {
        if (firmsLayer) map.removeLayer(firmsLayer);
      }
    }
  };

  // Funzione filtro globale
  window.applyMapFilters = function () {
    const showCivilian = document.getElementById('civilianToggle').checked;

    const filtered = window.globalEvents.filter(e => {
      // Logica per identificare eventi civili
      // Cerca "CIVIL" nella categoria o "CIVILIAN" nella precisione o nel tipo
      const isCivil = (e.category || '').toUpperCase().includes('CIVIL') ||
        (e.location_precision || '').toUpperCase().includes('CIVILIAN') ||
        (e.type || '').toUpperCase().includes('CIVIL');

      // Se l'evento è civile e il toggle è spento, nascondilo
      if (isCivil && !showCivilian) return false;

      return true;
    });

    window.currentFilteredEvents = filtered;
    // Aggiorna mappa e contatori
    renderInternal(filtered);
  };

  // ============================================
  // 10. MODAL FUNCTIONS (Preserve Visual Features)
  // ============================================

  // --- FUNZIONE AGGIORNATA PER USARE ID ---
  window.openModal = function (eventIdOrObj) {
    let e;
    const eventData = eventsData.find(e => e.event_id === id); // O la tua logica di lookup
    if (!eventData) return console.error("Evento non trovato:", id);

    // 2. DEFINISCI SCORE (Fix per l'errore riga 763)
    const score = parseInt(eventData.reliability) || 0;

    // FIX LOGICA IBRIDA:
    // 1. Se è una stringa che inizia con %7B (codifica di '{') è un vecchio oggetto JSON (War Room)
    if (typeof eventIdOrObj === 'string' && (eventIdOrObj.startsWith('%7B') || eventIdOrObj.startsWith('{'))) {
      try { e = JSON.parse(decodeURIComponent(eventIdOrObj)); } catch (err) { console.error(err); return; }
    }
    // 2. Se è una stringa normale, è un ID (Mappa)
    else if (typeof eventIdOrObj === 'string') {
      e = window.globalEvents.find(evt => (evt.event_id || evt.properties?.event_id) === eventIdOrObj);
      if (!e) { console.error("Evento non trovato per ID:", eventIdOrObj); return; }
    }
    // 3. Se è già un oggetto
    else { e = eventIdOrObj; }

    // --- DATI BASE ---
    document.getElementById('modalTitle').innerText = e.title || "Titolo non disponibile";
    document.getElementById('modalDesc').innerText = e.description || "Nessun dettaglio disponibile.";
    document.getElementById('modalType').innerText = e.type || "N/A";
    document.getElementById('modalDate').innerText = e.date || "";

    // --- DATI INTELLIGENCE AGGIUNTIVI ---
    // --- 1. DOMINANT BIAS (Badge & Tooltip Style) ---
    const biasEl = document.getElementById('modal-dominant-bias');
    if (biasEl) {
      const rawBias = (e.dominant_bias || "UNKNOWN").toUpperCase().replace('_', ' ');

      let color = "#94a3b8"; // Grigio neutro di base
      let desc = "Orientamento della fonte non determinato.";

      if (rawBias.includes('RUSSIA') || rawBias.includes('RU')) {
        color = "#ef4444"; // Rosso
        desc = "Fonte con narrazione filo-russa.";
      } else if (rawBias.includes('UKRAINE') || rawBias.includes('UA')) {
        color = "#3b82f6"; // Blu
        desc = "Fonte con narrazione filo-ucraina.";
      } else if (rawBias.includes('NEUTRAL') || rawBias.includes('WESTERN')) {
        color = "#22c55e"; // Verde
        desc = "Fonte neutrale, verificata o media internazionale.";
      }

      biasEl.innerHTML = `
            <div class="intensity-badge-wrapper">
                <span style="color:${color}; font-weight:800;">${rawBias}</span>
                <div class="info-icon" style="color:${color}; border-color:${color}; transform: scale(0.8);">i</div>
                <div class="intensity-tooltip">
                    <strong style="color:${color}">SOURCE BIAS</strong><br>
                    ${desc}
                </div>
            </div>`;
    }

    // --- GESTIONE PRECISIONE LOCATION (Colori + Link + Tooltip) ---
    const locEl = document.getElementById('modal-location-precision');
    if (locEl) {
      // Normalizza il testo (es. "electrical_substation" -> "ELECTRICAL SUBSTATION")
      const rawType = (e.location_precision || "UNK").toUpperCase();
      const prettyType = rawType.replace(/_/g, ' ');

      // Configurazione Categorie: Colore + Descrizione Legenda
      let color = "#94a3b8"; // Default grigio
      let desc = "Precisione della posizione non specificata.";

      if (rawType.includes('REFINERY')) {
        color = "#ef4444"; // Rosso (Target Energetico Critico)
        desc = "Obiettivo Strategico: Raffineria o deposito carburanti.";
      } else if (rawType.includes('ELECTRICAL') || rawType.includes('SUBSTATION')) {
        color = "#f59e0b"; // Ambra (Target Energetico)
        desc = "Infrastruttura Elettrica: Sottostazione o nodo di rete.";
      } else if (rawType.includes('MILITARY')) {
        color = "#dc2626"; // Rosso Scuro (Target Militare)
        desc = "Obiettivo Militare: Base, aeroporto o deposito munizioni.";
      } else if (rawType.includes('INFRASTRUCTURE')) {
        color = "#facc15"; // Giallo (Logistica)
        desc = "Logistica: Ponti, ferrovie, porti o dighe.";
      } else if (rawType.includes('CIVILIAN')) {
        color = "#38bdf8"; // Azzurro (Civile)
        desc = "Struttura Civile: Edifici specifici non militari (hotel, scuole, uffici).";
      } else if (rawType.includes('CITY') || rawType.includes('REGION')) {
        color = "#cbd5e1"; // Grigio Chiaro (Generico)
        desc = "Area Geografica: Posizione indicativa (centro città o regione).";
      }

      // Genera Link Google Maps (se coordinate valide)
      let linkHtml = prettyType;
      if (e.lat && e.lon && parseFloat(e.lat) !== 0) {
        linkHtml = `<a href="https://www.google.com/maps/search/?api=1&query=${e.lat},${e.lon}" 
                               target="_blank" 
                               style="color: ${color}; text-decoration: none; border-bottom: 1px dashed ${color};"
                               title="Vedi su Google Maps">
                               <i class="fa-solid fa-location-crosshairs"></i> ${prettyType}
                            </a>`;
      }

      // Renderizza HTML con Wrapper per Tooltip (usa le stesse classi dell'intensità)
      locEl.innerHTML = `
                <div class="intensity-badge-wrapper">
                    <span style="color:${color}; font-weight:700;">${linkHtml}</span>
                    <div class="info-icon" style="border-color:${color}; color:${color}; transform: scale(0.8);">i</div>
                    <div class="intensity-tooltip">
                        <strong style="color:${color}">TARGET INTEL</strong><br>
                        ${desc}
                    </div>
                </div>
            `;
    }

    // --- 3. IMPATTO STRATEGICO (Intensità) ---
    const intEl = document.getElementById('modal-intensity');
    if (intEl) {
      // Usa lo stesso helper del filtro per coerenza totale
      if (isCivilianEvent(e)) {
        // VISUALIZZAZIONE PER CIVILI
        intEl.innerHTML = `
                    <div class="intensity-badge-wrapper" style="opacity:0.7;">
                        <span style="color:#94a3b8; font-weight:700; font-size:1rem;">N/D <small style="font-size:0.65rem;">(NON-MILITARY)</small></span>
                    </div>`;
      } else {
        // VISUALIZZAZIONE PER MILITARI (La tua logica colori corretta)
        const val = parseFloat(e.intensity || 0);
        let label = "UNKNOWN"; let colorClass = "#64748b"; let desc = "Dati insufficienti.";

        if (val <= 0.3) { label = "TACTICAL"; colorClass = "#22c55e"; desc = "Schermaglie, droni intercettati, danni lievi."; }
        else if (val <= 0.6) { label = "OPERATIONAL"; colorClass = "#f97316"; desc = "Danni infrastrutture, vittime civili limitate."; }
        else if (val <= 0.8) { label = "STRATEGIC"; colorClass = "#ef4444"; desc = "Colpi a centrali, città o pesanti perdite."; }
        else { label = "CRITICAL"; colorClass = "#000000"; desc = "Evento di portata storica o catastrofica."; }

        const style = `color: ${colorClass}; font-weight: 800; font-size: 1.1rem; text-shadow: 0 0 15px ${colorClass}44;`;

        intEl.innerHTML = `
                    <div class="intensity-badge-wrapper">
                        <span style="${style}">${label} (${(val * 10).toFixed(1)})</span>
                        <div class="info-icon" style="color:${colorClass}; border-color:${colorClass}">i</div>
                        <div class="intensity-tooltip">
                            <strong style="color:${colorClass}">${label} IMPACT</strong><br>${desc}
                        </div>
                    </div>`;
      }
    }

    // --- GESTIONE VIDEO ---
    const vidCont = document.getElementById('modalVideoContainer');
    vidCont.innerHTML = '';

    if (e.video && e.video !== 'null') {
      if (e.video.includes('youtu')) {
        const embed = e.video.replace('watch?v=', 'embed/').split('&')[0];
        vidCont.innerHTML = `<iframe src="${embed}" frameborder="0" allowfullscreen style="width:100%; height:400px; border-radius:8px;"></iframe>`;
      } else {
        vidCont.innerHTML = `<a href="${e.video}" target="_blank" class="btn-primary">Media Esterno</a>`;
      }
    }

    // --- GESTIONE JUXTAPOSE (PRIMA/DOPO) ---
    const sliderCont = document.getElementById('modalJuxtapose');
    sliderCont.innerHTML = '';
    if (e.before_img && e.after_img) {
      sliderCont.innerHTML = `
        <h4 style="color:white; margin:20px 0 10px;">Battle Damage Assessment</h4>
        <div class="juxtapose-wrapper" onmousemove="updateSlider(event, this)">
          <div class="juxtapose-img" style="background-image:url('${e.before_img}')"></div>
          <div class="juxtapose-img after" style="background-image:url('${e.after_img}'); width:50%;"></div>
          <div class="juxtapose-handle" style="left:50%"><div class="juxtapose-button"><i class="fa-solid fa-arrows-left-right"></i></div></div>
        </div>`;
    }

    // --- CHART CONFIDENCE ---
    const conf = e.reliability || e.Reliability || e.confidence || 50;
    // Definisci Tier, Colore e Descrizione breve
    let relData = { label: "NON VERIFICATO", color: "#94a3b8", desc: "Dati insufficienti." };

    if (score >= 90) {
      relData = { label: "CONFERMATO", color: "#3b82f6", desc: "Info verificata con prove visive." }; // Blu
    } else if (score >= 70) {
      relData = { label: "PROBABILE", color: "#22c55e", desc: "Fonti multiple affidabili." }; // Verde
    } else if (score >= 40) {
      relData = { label: "POSSIBILE", color: "#f59e0b", desc: "Fonte singola o parziale." }; // Arancio
    } else {
      relData = { label: "RUMOR", color: "#ef4444", desc: "Voce non verificata / Propaganda." }; // Rosso
    }

    // 1. Aggiorna il Grafico con il colore giusto
    renderConfidenceChart(score, relData.color);

    // 2. Genera il Badge con Tooltip (sostituisce quello statico)
    const relContainer = document.getElementById('modal-reliability-badge');
    if (relContainer) {
      relContainer.innerHTML = `
            <div class="intensity-badge-wrapper" style="font-size:0.7rem; color:${relData.color}; font-weight:700; letter-spacing:1px; cursor:help;">
                ${relData.label}
                <div class="info-icon" style="width:12px; height:12px; font-size:0.6rem; border-color:${relData.color}; color:${relData.color};">i</div>
                
                <div class="intensity-tooltip" style="width:260px; right:-20px; left:auto; transform:none; text-align:left; font-weight:400; color:#cbd5e1;">
                    <strong style="color:${relData.color}; display:block; border-bottom:1px solid #334155; padding-bottom:5px; margin-bottom:5px;">
                        SCORE: ${score}% (${relData.label})
                    </strong>
                    ${relData.desc}<br><br>
                    <em style="font-size:0.65rem; opacity:0.7;">
                        Algoritmo basato su: Credibilità Fonte, Prove Visive, Corroborazione e Semantica.
                    </em>
                </div>
            </div>`;
    }

    // Aggiorna il contatore delle fonti nel box metadati
    const sourceCountEl = document.getElementById('modal-source-count');
    if (sourceCountEl) {
      sourceCountEl.innerText = (e.references && e.references.length) ? e.references.length : "0";
    }

    // --- RENDER BIBLIOGRAFIA (FONTI) ---
    // Richiama la funzione helper (definita sotto)
    renderBibliography(e.references || []);

    // MOSTRA IL MODAL
    document.getElementById('videoModal').style.display = 'flex';
  };

  // Funzione per disegnare le fonti (Aggiornata per liste URL)
  function renderBibliography(references) {
    const container = document.getElementById('modal-bibliography');
    if (!container) return;

    container.innerHTML = '';

    // Se non ci sono reference o è una lista vuota
    if (!references || references.length === 0) {
      container.innerHTML = '<div style="padding:10px; background:rgba(255,255,255,0.02); border-radius:4px; color:#64748b; font-style:italic; font-size:0.85rem; text-align:center;">Nessuna fonte aggregata disponibile per questo evento.</div>';
      return;
    }

    let html = `<h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin-bottom:15px; border-bottom:1px solid #334155; padding-bottom:5px; display:flex; align-items:center; gap:8px;"><i class="fa-solid fa-link"></i> Fonti Correlate & Intelligence</h5>`;

    references.forEach((ref, idx) => {
      // Gestione robusta: supporta sia stringhe (URL) che oggetti vecchi
      let url = (typeof ref === 'object' && ref.url) ? ref.url : ref;

      // Se non è un link valido, lo mostriamo come testo, altrimenti creiamo il link
      let isLink = typeof url === 'string' && (url.startsWith('http') || url.startsWith('www'));

      // Estetica: Estrae il dominio per non mostrare URL chilometrici (es. "twitter.com")
      let displayName = "Fonte Esterna";
      if (isLink) {
        try {
          const urlObj = new URL(url.startsWith('http') ? url : 'https://' + url);
          displayName = urlObj.hostname.replace('www.', '');
        } catch (e) { displayName = url; }
      } else {
        displayName = "Riferimento d'archivio";
      }

      html += `
            <div style="margin-bottom:8px; display:flex; align-items:center; background:rgba(15, 23, 42, 0.6); padding:8px 12px; border-radius:6px; border:1px solid #334155;">
                <span style="color:#64748b; font-family:'JetBrains Mono', monospace; font-size:0.8rem; margin-right:10px; min-width:20px;">${idx + 1}.</span>
                
                ${isLink ?
          `<a href="${url}" target="_blank" style="color:#38bdf8; text-decoration:none; font-size:0.9rem; font-weight:500; display:flex; align-items:center; gap:6px; flex-grow:1; transition: color 0.2s;">
                        <i class="fa-solid fa-earth-europe" style="font-size:0.8em; opacity:0.7;"></i> ${displayName} 
                        <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.7em; margin-left:auto; opacity:0.5;"></i>
                    </a>`
          : `<span style="color:#cbd5e1; font-size:0.9rem;">${ref}</span>`
        }
            </div>`;
    });

    container.innerHTML = html;
  }

  window.updateSlider = function (e, wrapper) {
    const rect = wrapper.getBoundingClientRect();
    let pos = ((e.clientX - rect.left) / rect.width) * 100;
    pos = Math.max(0, Math.min(100, pos));
    wrapper.querySelector('.after').style.width = `${pos}%`;
    wrapper.querySelector('.juxtapose-handle').style.left = `${pos}%`;
  };

  // Funzione Grafico Aggiornata con Colore Dinamico
  let confChart = null;
  function renderConfidenceChart(score, color = '#f59e0b') { // <--- Aggiunto parametro color
    const ctxEl = document.getElementById('confidenceChart');
    if (!ctxEl) return;

    const ctx = ctxEl.getContext('2d');
    if (confChart) confChart.destroy();

    confChart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        datasets: [{
          data: [score, 100 - score],
          backgroundColor: [color, '#1e293b'], // <--- Usa il colore dinamico qui
          borderWidth: 0,
          borderRadius: 20
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '75%',
        animation: false,
        plugins: { tooltip: { enabled: false } }
      },
      plugins: [{
        id: 'text',
        beforeDraw: function (chart) {
          const width = chart.width, height = chart.height, ctx = chart.ctx;
          ctx.restore();
          const fontSize = (height / 100).toFixed(2);
          ctx.font = "bold " + fontSize + "em Inter";
          ctx.textBaseline = "middle";
          ctx.fillStyle = color; // <--- E anche qui per il testo centrale
          const text = score + "%";
          const textX = Math.round((width - ctx.measureText(text).width) / 2);
          const textY = height / 2;
          ctx.fillText(text, textX, textY);
          ctx.save();
        }
      }]
    });
  }

  // ============================================
  // 11. VISUAL GRID RENDERER
  // ============================================

  window.renderVisualGrid = function (events) {
    const grid = document.getElementById('visual-grid-content');
    if (!grid) return;

    grid.innerHTML = '';
    const visualEvents = events.filter(e => e.image || (e.video && e.video !== 'null'));

    visualEvents.forEach(e => {
      const item = document.createElement('div');
      item.className = 'visual-item';
      item.style.cssText = "background:#1e293b; border-radius:8px; overflow:hidden; position:relative; aspect-ratio: 16/9; cursor:pointer; border:1px solid #334155;";

      let bgUrl = e.image;
      if (!bgUrl && e.video && e.video.includes('youtu')) {
        try {
          let vidId = null;
          if (e.video.includes('v=')) vidId = e.video.split('v=')[1]?.split('&')[0];
          else if (e.video.includes('youtu.be/')) vidId = e.video.split('youtu.be/')[1]?.split('?')[0];
          if (vidId) bgUrl = `https://img.youtube.com/vi/${vidId}/mqdefault.jpg`;
        } catch (err) { }
      }

      if (bgUrl) {
        item.innerHTML = `
          <div style="background-image:url('${bgUrl}'); width:100%; height:100%; background-size:cover; background-position:center;"></div>
          <div style="position:absolute; bottom:0; left:0; width:100%; background:linear-gradient(to top, rgba(0,0,0,0.9), transparent); padding:10px; color:white;">
            <div style="font-size:0.85rem; font-weight:bold; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; text-shadow: 0 1px 2px black;">
              ${e.type === 'video' ? '<i class="fa-solid fa-play-circle"></i> ' : ''} ${e.title}
            </div>
            <div style="font-size:0.75rem; color:#cbd5e1; display:flex; justify-content:space-between;">
              <span>${e.date}</span>
              <span style="color:#f59e0b;">${e.intensity > 0.7 ? 'CRITICAL' : ''}</span>
            </div>
          </div>`;

        const eventData = encodeURIComponent(JSON.stringify(e));
        item.onclick = () => window.openModal(eventData);
        grid.appendChild(item);
      }
    });

    if (visualEvents.length === 0) {
      grid.innerHTML = `<div style="grid-column: 1 / -1; text-align:center; color:#64748b; padding:40px;"><i class="fa-solid fa-camera-retro" style="font-size:2rem; margin-bottom:10px; opacity:0.5;"></i><br>Nessun media visivo trovato.</div>`;
    }
  };

  // ============================================
  // 12. APPLICATION START (Sequential Execution)
  // ============================================

  // Wait for DOM before initializing
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startApp);
  } else {
    startApp();
  }

  function startApp() {
    console.log("🚀 Starting Impact Atlas...");
    initMap();
    loadEventsData();
  }

  // --- INTEGRAZIONE DOSSIER NEL VECCHIO MAP.JS ---

  // Funzione globale per aprire il Dossier
  window.openIntelDossier = function (eventData) {
    console.log("📂 Apertura Dossier per:", eventData.title);

    // 1. Popola i dati base
    document.getElementById('modalTitle').innerText = eventData.title || "Titolo non disponibile";
    document.getElementById('modalDesc').innerText = eventData.description || "Nessuna descrizione.";
    document.getElementById('modalDate').innerText = eventData.date || "";

    // 2. Popola i dati Intelligence (con controlli se mancano nel vecchio json)
    const bias = eventData.dominant_bias || "UNKNOWN";
    document.getElementById('modal-dominant-bias').innerText = bias.replace('_', ' ');

    document.getElementById('modal-location-precision').innerText = (eventData.location_precision || "UNK").toUpperCase();

    // --- GESTIONE INTENSITÀ & TOOLTIP ---
    const intEl = document.getElementById('modal-intensity');
    if (intEl) {
      const val = parseFloat(e.intensity || 0);

      // Definisci Label e Colore in base al valore (Scala concordata)
      let label = "UNKNOWN";
      let colorClass = "#64748b"; // Grigio default
      let desc = "Dati non sufficienti per valutare l'impatto.";

      if (val <= 0.3) {
        label = "TACTICAL";
        colorClass = "#22c55e"; // Verde
        desc = "Impatto limitato. Schermaglie locali, droni intercettati o danni lievi alle infrastrutture.";
      } else if (val <= 0.6) {
        label = "OPERATIONAL";
        colorClass = "#f97316"; // Arancio
        desc = "Impatto operativo. Danni a infrastrutture, vittime civili o conquista di posizioni minori.";
      } else if (val <= 0.8) {
        label = "STRATEGIC";
        colorClass = "#ef4444"; // Rosso
        desc = "Alto impatto strategico. Colpi a centrali elettriche, grandi città o pesanti perdite.";
      } else {
        label = "CRITICAL";
        colorClass = "#000000"; // Nero (con bordo magari)
        desc = "Evento di portata storica. Rischio nucleare, catastrofe ambientale o svolta nel conflitto.";
      }

      // Costruisci l'HTML con il Tooltip
      // Nota: text-shadow per rendere leggibile il nero, oppure background
      const style = `color: ${colorClass}; font-weight: 800; font-size: 1.1rem; text-shadow: 0 0 15px ${colorClass}44;`;

      intEl.innerHTML = `
                <div class="intensity-badge-wrapper">
                    <span style="${style}">${label} (${(val * 10).toFixed(1)})</span>
                    <div class="info-icon">i</div>
                    <div class="intensity-tooltip">
                        <strong style="color:${colorClass}">${label} IMPACT</strong><br>
                        ${desc}
                    </div>
                </div>
            `;
    }

    // 3. Renderizza Bibliografia
    // Nota: Se il vecchio JSON non ha "references", questa parte resterà vuota ma non crasherà
    renderBibliography(eventData.references || []);

    // 4. Mostra il Modal
    document.getElementById('videoModal').style.display = 'flex';
  };

  // Funzione per disegnare le fonti (Aggiornata per liste URL)
  function renderBibliography(references) {
    const container = document.getElementById('modal-bibliography');
    if (!container) return;

    container.innerHTML = '';

    // Se non ci sono reference o è una lista vuota
    if (!references || references.length === 0) {
      container.innerHTML = '<div style="padding:10px; background:rgba(255,255,255,0.02); border-radius:4px; color:#64748b; font-style:italic; font-size:0.85rem; text-align:center;">Nessuna fonte aggregata disponibile per questo evento.</div>';
      return;
    }

    let html = `<h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin-bottom:15px; border-bottom:1px solid #334155; padding-bottom:5px; display:flex; align-items:center; gap:8px;"><i class="fa-solid fa-link"></i> Fonti Correlate & Intelligence</h5>`;

    references.forEach((ref, idx) => {
      // Gestione robusta: supporta sia stringhe (URL) che oggetti vecchi
      let url = (typeof ref === 'object' && ref.url) ? ref.url : ref;

      // Se non è un link valido, lo mostriamo come testo, altrimenti creiamo il link
      let isLink = typeof url === 'string' && (url.startsWith('http') || url.startsWith('www'));

      // Estetica: Estrae il dominio per non mostrare URL chilometrici (es. "twitter.com")
      let displayName = "Fonte Esterna";
      if (isLink) {
        try {
          const urlObj = new URL(url.startsWith('http') ? url : 'https://' + url);
          displayName = urlObj.hostname.replace('www.', '');
        } catch (e) { displayName = url; }
      } else {
        displayName = "Riferimento d'archivio";
      }

      html += `
            <div style="margin-bottom:8px; display:flex; align-items:center; background:rgba(15, 23, 42, 0.6); padding:8px 12px; border-radius:6px; border:1px solid #334155;">
                <span style="color:#64748b; font-family:'JetBrains Mono', monospace; font-size:0.8rem; margin-right:10px; min-width:20px;">${idx + 1}.</span>
                
                ${isLink ?
          `<a href="${url}" target="_blank" style="color:#38bdf8; text-decoration:none; font-size:0.9rem; font-weight:500; display:flex; align-items:center; gap:6px; flex-grow:1; transition: color 0.2s;">
                        <i class="fa-solid fa-earth-europe" style="font-size:0.8em; opacity:0.7;"></i> ${displayName} 
                        <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.7em; margin-left:auto; opacity:0.5;"></i>
                    </a>`
          : `<span style="color:#cbd5e1; font-size:0.9rem;">${ref}</span>`
        }
            </div>`;
    });

    container.innerHTML = html;
  }

  // Funzione chiusura
  window.closeModal = function (e) {
    if (!e || e.target.id === 'videoModal' || e.target.classList.contains('close-modal')) {
      document.getElementById('videoModal').style.display = 'none';
    }
  };

})();
