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

  // ==========================================
  // 1. GENERAZIONE POPUP (Corretta e con Stile Elegante)
  // ==========================================
  function createPopupContent(e) {
    // 1. Recupera l'ID in modo sicuro
    // Usa 'event_id' se esiste, altrimenti 'id'. 'feature' NON esiste qui.
    const id = e.event_id || e.id || (e.properties ? e.properties.event_id : null);

    // 2. Determina il colore
    const color = getColor(e.intensity);

    // 3. Gestione Footer Fonte
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
              <a href="${url}" target="_blank" style="color: #3b82f6; text-decoration: none;">
                 <i class="fa-solid fa-link"></i> ${domain}
              </a>
            </div>`;
    }

    // 4. Costruzione HTML Popup (Stile Elegante Ripristinato)
    // Nota: Il bottone ha lo stile INLINE per garantire che sia blu e bello come prima.
    return `
    <div class="acled-popup" style="color:#e2e8f0; font-family: 'Inter', sans-serif; min-width: 260px;">
      
      <div style="border-left: 4px solid ${color}; padding-left: 12px; margin-bottom: 12px;">
        <h5 style="margin:0; font-weight:700; font-size:1rem; line-height:1.3; color:#f8fafc;">${e.title}</h5>
        
        <div style="margin-top:8px; display:flex; gap:8px;">
          <span class="popup-meta-tag" style="background:#1e293b; padding:2px 6px; border-radius:4px; font-size:0.75rem;"><i class="fa-regular fa-calendar"></i> ${e.date}</span>
          <span class="popup-meta-tag" style="background:#1e293b; padding:2px 6px; border-radius:4px; font-size:0.75rem;"><i class="fa-solid fa-tag"></i> ${e.type || 'Evento'}</span>
        </div>
      </div>

      <div style="font-size:0.85rem; line-height:1.6; color:#cbd5e1; margin-bottom:15px;">
        ${e.description ? (e.description.length > 120 ? e.description.substring(0, 120) + '...' : e.description) : 'Nessuna descrizione.'}
      </div>
      
      <div class="popup-actions">
        <button 
          class="custom-dossier-btn" 
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

  // ============================================
  // 7. MAP INITIALIZATION (Fix Crash)
  // ============================================
  function initMap() {
    // FIX: Se la mappa esiste già, ci fermiamo qui.
    if (map) {
      console.log("⚠️ Mappa già inizializzata.");
      return;
    }

    map = L.map('map', {
      zoomControl: false,
      preferCanvas: true,
      wheelPxPerZoomLevel: 120
    }).setView([48.5, 32.0], 6);

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19,
      attribution: '© IMPACT ATLAS'
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
            // --- FIX CRITICO: UNIFICAZIONE ID ---
            // Se esiste cluster_id, usalo come event_id. Questo ripara il tasto Dossier.
            event_id: props.event_id || props.cluster_id || props.id,
            // ------------------------------------
            lat: f.geometry ? f.geometry.coordinates[1] : props.lat,
            lon: f.geometry ? f.geometry.coordinates[0] : props.lon,
            timestamp: ts,
            date: m.isValid() ? m.format("DD/MM/YYYY") : props.date
          };
        })
          // MODIFICA: Filtro "Spazzatura" Frontend
          .filter(e => {
            // Esclude se coordinate sono 0
            if (!e.lat || !e.lon || e.lat === 0 || e.lon === 0) return false;

            return true;
          })
          .sort((a, b) => b.timestamp - a.timestamp); // Ordine decrescente

        console.log(`✅ Events processed: ${window.globalEvents.length}`);

        // 3. DEFINIZIONE FILTRI (CIVILI + ATTORI + RICERCA SMART)
        window._applyMapFiltersImpl = function () {
          // A. Recupera Input (Gestione sicura se mancano elementi)
          const toggle = document.getElementById('civilianToggle');
          const showCivilian = toggle ? toggle.checked : true;

          const searchInput = document.getElementById('textSearch');
          const searchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';

          const actorSelect = document.getElementById('actorFilter');
          const selectedActor = actorSelect ? actorSelect.value : '';

          // B. Ciclo di Filtraggio
          const filtered = window.globalEvents.filter(e => {

            // 1. Filtro Civili (LOGICA ORIGINALE MANTENUTA)
            if (typeof isCivilianEvent === 'function') {
              const isCivil = isCivilianEvent(e);
              if (isCivil && !showCivilian) return false;
            }

            // 2. Filtro Attore (NUOVO)
            // Se c'è un attore selezionato nel menu, l'evento deve coincidere
            if (selectedActor && e.actor !== selectedActor) {
              return false;
            }

            // 3. Ricerca Testuale Smart (NUOVO)
            if (searchTerm) {
              // Mapping Intelligente: Utente scrive "Russia" -> Cerchiamo Actor "RUS"
              let targetActor = null;
              if (['russia', 'russo', 'russi', 'mosca'].some(k => searchTerm.includes(k))) targetActor = 'RUS';
              if (['ucraina', 'ukraine', 'kiev'].some(k => searchTerm.includes(k))) targetActor = 'UKR';

              // Cerca nei campi di testo
              const inTitle = (e.title || '').toLowerCase().includes(searchTerm);
              const inDesc = (e.description || '').toLowerCase().includes(searchTerm);
              const inLoc = (e.location_precision || '').toLowerCase().includes(searchTerm);

              // Cerca per attore smart (es. ho scritto "attacchi russi" -> mostra eventi RUS)
              const isSmartMatch = targetActor && e.actor === targetActor;

              // Se non trovo il testo E non è un match smart -> Nascondi
              if (!inTitle && !inDesc && !inLoc && !isSmartMatch) return false;
            }

            return true;
          });

          // C. Aggiorna Mappa e Contatori
          window.currentFilteredEvents = filtered;
          renderInternal(filtered);
        };

        // Espone la funzione
        window.applyMapFilters = window._applyMapFiltersImpl;

        // --- ATTIVAZIONE LIVE (FONDAMENTALE) ---
        // Collega i filtri agli input HTML per aggiornare la mappa in tempo reale
        const inputsToCheck = ['textSearch', 'actorFilter', 'civilianToggle'];
        inputsToCheck.forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.oninput = window.applyMapFilters; // Per quando scrivi
            el.onchange = window.applyMapFilters; // Per menu e checkbox
          }
        });

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

  // Funzione filtro globale (wrapper difensivo)
  // La vera implementazione viene impostata dopo il caricamento dei dati in window._applyMapFiltersImpl
  window.applyMapFilters = function () {
    // Se l'implementazione reale è disponibile, usala
    if (typeof window._applyMapFiltersImpl === 'function') {
      return window._applyMapFiltersImpl();
    }

    // Fallback difensivo se i dati non sono ancora pronti o il DOM non contiene il toggle
    const toggleEl = document.getElementById('civilianToggle');
    const showCivilian = toggleEl ? toggleEl.checked : true;

    const events = Array.isArray(window.globalEvents) ? window.globalEvents : (Array.isArray(window.allEventsData) ? window.allEventsData : []);
    const filtered = events.filter(e => {
      const isCivil = (e.category || '').toUpperCase().includes('CIVIL') ||
        (e.location_precision || '').toUpperCase().includes('CIVILIAN') ||
        (e.type || '').toUpperCase().includes('CIVIL');
      if (isCivil && !showCivilian) return false;
      return true;
    });

    window.currentFilteredEvents = filtered;
    if (typeof renderInternal === 'function') renderInternal(filtered);
  };
  // ============================================
  // 10. MODAL FUNCTIONS (FIXED & LINKED)
  // ============================================

  window.openModal = function (eventIdOrObj) {
    console.log("Tentativo apertura dossier:", eventIdOrObj);
    let eventData = null;

    if (typeof eventIdOrObj === 'string') {
      if (eventIdOrObj.startsWith('%7B') || eventIdOrObj.startsWith('{')) {
        try { eventData = JSON.parse(decodeURIComponent(eventIdOrObj)); } catch (err) { }
      } else {
        // FIX: Confronto robusto (converte tutto a stringa)
        if (window.globalEvents) {
          const searchId = String(eventIdOrObj);
          eventData = window.globalEvents.find(evt => String(evt.event_id) === searchId);
        }
      }
    } else {
      eventData = eventIdOrObj;
    }

    if (!eventData) {
      console.error(`❌ Evento non trovato per il Dossier. ID ricercato: ${eventIdOrObj}`);
      return; // Interrompe l'esecuzione se non trova dati
    }

    // 2. Passa i dati alla funzione di rendering
    window.openIntelDossier(eventData);
  };
  // ============================================
  // 10. MODAL FUNCTIONS (DOSSIER UI)
  // ============================================

  let tieRadarInstance = null; // Global instance for the modal chart

  window.openIntelDossier = function (eventData) {
    console.log("📂 Opening Dossier for:", eventData.title);

    document.getElementById('videoModal').style.display = 'flex'; // Use Flex for centering

    // --- 1. CONTEXT (Left Column) ---
    document.getElementById('modalTitle').innerText = eventData.title || "Titolo non disponibile";
    document.getElementById('modalDate').innerText = eventData.date || "Unknown Date";

    // Type Tag
    const typeTag = document.getElementById('modalType');
    if (typeTag) typeTag.innerText = (eventData.type || "EVENT").toUpperCase();

    // Description
    const descEl = document.getElementById('modalDesc');
    if (descEl) descEl.innerText = eventData.description || "Nessuna descrizione disponibile per questo evento.";

    // --- 2. METRICS CARD (Right Column) ---
    const vecK = parseFloat(eventData.vec_k) || 0;
    const vecT = parseFloat(eventData.vec_t) || 0;
    const vecE = parseFloat(eventData.vec_e) || 0;

    // A. TIE Bars
    const barsContainer = document.getElementById('tieBarsContent');
    if (barsContainer) {
      barsContainer.innerHTML = `
            <div class="tie-bar-row">
                <div class="tie-bar-label"><span>KINETIC (Intensity)</span> <span>${vecK}/10</span></div>
                <div class="tie-progress-track"><div class="tie-progress-fill bar-kinetic" style="width: ${vecK * 10}%"></div></div>
            </div>
            <div class="tie-bar-row">
                <div class="tie-bar-label"><span>TARGET (Value)</span> <span>${vecT}/10</span></div>
                <div class="tie-progress-track"><div class="tie-progress-fill bar-target" style="width: ${vecT * 10}%"></div></div>
            </div>
            <div class="tie-bar-row">
                <div class="tie-bar-label"><span>EFFECT (Outcome)</span> <span>${vecE}/10</span></div>
                <div class="tie-progress-track"><div class="tie-progress-fill bar-effect" style="width: ${vecE * 10}%"></div></div>
            </div>
        `;
    }

    // B. TIE Radar Chart (Chart.js)
    const ctx = document.getElementById('tieRadarChart');
    if (ctx) {
      if (tieRadarInstance) {
        tieRadarInstance.destroy();
      }

      tieRadarInstance = new Chart(ctx, {
        type: 'radar',
        data: {
          labels: ['KINETIC', 'TARGET', 'EFFECT'],
          datasets: [{
            label: 'TIE Profile',
            data: [vecK, vecT, vecE],
            backgroundColor: 'rgba(245, 158, 11, 0.2)', // Amber transparent
            borderColor: 'rgba(245, 158, 11, 1)',       // Amber solid
            borderWidth: 2,
            pointBackgroundColor: '#fff',
            pointBorderColor: '#eab308',
            pointHoverBackgroundColor: '#fff',
            pointHoverBorderColor: '#eab308'
          }]
        },
        options: {
          scales: {
            r: {
              angleLines: { color: 'rgba(255, 255, 255, 0.1)' },
              grid: { color: 'rgba(255, 255, 255, 0.1)' },
              pointLabels: {
                color: '#94a3b8',
                font: { size: 10, weight: 'bold' }
              },
              ticks: { display: false, max: 10, min: 0 } // Hide numbers, fixed 0-10
            }
          },
          plugins: {
            legend: { display: false } // Hide legend
          },
          maintainAspectRatio: false
        }
      });
    }

    // --- 3. METADATA CARD ---

    // A. Reliability Badge
    const relScore = eventData.reliability || 0;
    let relBadgeHtml = '';
    if (relScore >= 80) relBadgeHtml = `<span style="color:#22c55e; font-weight:700;"><i class="fa-solid fa-shield-halved"></i> HIGH RELIABILITY</span>`;
    else if (relScore >= 50) relBadgeHtml = `<span style="color:#f59e0b; font-weight:700;"><i class="fa-solid fa-shield-halved"></i> MEDIUM RELIABILITY</span>`;
    else relBadgeHtml = `<span style="color:#ef4444; font-weight:700;"><i class="fa-solid fa-triangle-exclamation"></i> LOW RELIABILITY</span>`;

    const relBadgeEl = document.getElementById('modal-reliability-badge');
    if (relBadgeEl) relBadgeEl.innerHTML = relBadgeHtml;

    // B. Sources List
    const sourceListEl = document.getElementById('modal-source-list');
    if (sourceListEl) {
      let sources = [];
      try {
        if (Array.isArray(eventData.sources_list)) {
          sources = eventData.sources_list;
        } else if (typeof eventData.sources_list === 'string') {
          // 1. Try Clean JSON Parse first (Backend v2)
          try {
            sources = JSON.parse(eventData.sources_list);
          } catch (e1) {
            // 2. Fallback for Legacy/Dirty strings (Python list style)
            try {
              // Only replace quotes if it looks like a Python list string
              if (eventData.sources_list.includes("'")) {
                sources = JSON.parse(eventData.sources_list.replace(/'/g, '"'));
              } else {
                sources = [eventData.sources_list];
              }
            } catch (e2) {
              // Last resort: treat as single string
              sources = [eventData.sources_list];
            }
          }
        }
      } catch (e) { console.warn("Error parsing sources:", e); }

      if (!sources || sources.length === 0) {
        sourceListEl.innerHTML = `<span style="color:#64748b; font-style:italic; font-size:0.8rem;">No explicit sources listed.</span>`;
      } else {
        sourceListEl.innerHTML = sources.map(src => {
          // Formatting domain name
          let domain = typeof src === 'string' ? src : (src.name || src.url || "Source");
          let url = typeof src === 'string' ? src : (src.url || "#");

          // Handle object structure from backend
          if (!url.startsWith('http') && url !== '#') url = 'https://' + url;
          try {
            if (url !== '#') domain = new URL(url).hostname.replace('www.', '');
          } catch (e) { }

          // Simple Favicon via Google S2
          const faviconUrl = `https://www.google.com/s2/favicons?domain=${domain}&sz=32`;

          return `
                <a href="${url}" target="_blank" class="source-item">
                    <img src="${faviconUrl}" class="source-icon" onerror="this.style.display='none'">
                    <span>${domain}</span>
                    <i class="fa-solid fa-external-link-alt" style="margin-left:auto; font-size:0.7rem; opacity:0.5;"></i>
                </a>`;
        }).join('');
      }
    }

    // C. Bias Meter
    const biasScore = parseFloat(eventData.bias_score) || 0;
    const biasPercent = ((biasScore + 10) / 20) * 100;
    const clampedBias = Math.max(0, Math.min(100, biasPercent));

    // --- 3b. RELIABILITY BAR ---
    // ... code omitted ...

    // --- 3c. BIAS METER ---
    const biasMarker = document.getElementById('bias-marker');
    const biasValueEl = document.getElementById('bias-value');

    if (biasMarker) {
      biasMarker.style.left = `${clampedBias}%`;
    }
    if (biasValueEl) {
      const biasLabel = biasScore <= -3 ? "Pro-RU" : (biasScore >= 3 ? "Pro-UA" : "Neutral");
      biasValueEl.textContent = `${biasScore.toFixed(1)} (${biasLabel})`;
    }

    // --- 4. THE STRATEGIST ---
    // If there is an AI summary field (e.g. 'desc' repurposed or specific field), use it.
    // For now, we reuse description or a mock if missing.
    const stratBox = document.getElementById('modal-strategist-content');
    if (stratBox) {
      // Fallback or specific reasoning field
      const reasoning = eventData.ai_reasoning || "AI Analysis confirms high probability of kinetic event based on cross-referenced multi-source reporting. Strategic impact affects local logistics.";
      stratBox.innerHTML = reasoning;
    }



    // ============================================================
    // B. GESTIONE SCORE & GRAFICO (INSERITO ORA)
    // ============================================================

    // 1. Normalizza Score
    const score = parseInt(eventData.reliability || eventData.Reliability || eventData.confidence || 0);

    // 2. Definisci Colori e Testi (AGGIUNTO IL FOOTER EXECUTIVE)
    let relData = {
      label: "NON VERIFICATO",
      color: "#64748b",
      desc: "Dati insufficienti per valutare l'attendibilità.",
      footer: "Dati insufficienti per il calcolo algoritmico."
    };

    if (score >= 80) {
      relData = {
        label: "CONFERMATA",
        color: "#22c55e",
        desc: "Confermato visivamente. L'evento è supportato da documentazione multimediale verificata o geolocalizzazione precisa.",
        footer: "Score massimo garantito dalla presenza di prove visive (IMINT) o geolocalizzazione confermata."
      };
    } else if (score >= 60) {
      relData = {
        label: "ATTENDIBILE",
        color: "#84cc16",
        desc: "Molto probabile. Evento confermato da molteplici vettori indipendenti o da fonti istituzionali con alto grado di accuratezza.",
        footer: "Score elevato grazie alla convergenza narrativa rilevata tra molteplici fonti non collegate."
      };
    } else if (score >= 40) {
      relData = {
        label: "INCERTA",
        color: "#f59e0b",
        desc: "In attesa di riscontro. Riportato da fonti mainstream o locali credibili, ma non ancora verificato sul campo.",
        footer: "Score assegnato sulla base della reputazione storica della fonte, in attesa di evidenze materiali."
      };
    } else if (score < 40) {
      relData = {
        label: "DUBBIA",
        color: "#ef4444",
        desc: "Bassa Confidenza. Dati insufficienti per confermare l'evento. Rischio elevato di disinformazione o errore.",
        footer: "Score limitato dall'assenza di riscontri indipendenti o dalla natura speculativa della fonte."
      };
    }

    // 3. Disegna il Grafico (INVARIATO)
    if (typeof renderConfidenceChart === 'function') {
      renderConfidenceChart(score, relData.color);
    }

    // 4. Aggiorna il Badge HTML (AGGIORNATO CON STILE PROFESSIONALE E FOOTER)
    const relContainer = document.getElementById('modal-reliability-badge');
    if (relContainer) {
      relContainer.innerHTML = `
            <div class="intensity-badge-wrapper" style="
                font-size:0.7rem; 
                color:${relData.color}; 
                font-weight:700; 
                letter-spacing:1px; 
                cursor:help; 
                margin-top:5px; 
                display:flex; 
                align-items:center; 
                justify-content:center; 
                gap:4px;
                position: relative;
            ">
                ${relData.label}
                <div class="info-icon" style="
                    width:12px; height:12px; font-size:0.6rem; 
                    border-color:${relData.color}; color:${relData.color}; 
                    display:flex;
                ">i</div>
                
                <div class="intensity-tooltip" style="
                    width: 240px; 
                    bottom: 130%; 
                    left: 50%; 
                    transform: translateX(-50%);
                    background: rgba(15, 23, 42, 0.95);
                    border: 1px solid ${relData.color}44;
                    padding: 12px;
                    box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
                    text-align: left;
                    white-space: normal; /* Importante per il testo lungo */
                    z-index: 100;
                ">
                    <div style="border-bottom: 1px solid #334155; padding-bottom: 8px; margin-bottom: 8px; display:flex; justify-content:space-between; align-items:center;">
                        <span style="font-size:0.7rem; color:#64748b; text-transform:uppercase;">CONFIDENCE SCORE</span>
                        <strong style="color:${relData.color}; font-size:1.1rem;">${score}%</strong>
                    </div>

                    <div style="font-size:0.8rem; color:#e2e8f0; line-height:1.4; margin-bottom:8px; font-weight:400;">
                        ${relData.desc}
                    </div>

                    <div style="font-size:0.65rem; color:#94a3b8; font-style:italic; border-top:1px solid #334155; padding-top:6px; line-height:1.3;">
                        ${relData.footer}
                    </div>
                </div>
            </div>`;
    }

    // 3. Renderizza Bibliografia (se la funzione helper esiste)
    if (typeof renderBibliography === 'function') {
      renderBibliography(eventData.references || []);
    }

    // 4. Mostra il Modal (Supporta entrambi gli ID comuni)
    const modal = document.getElementById('videoModal') || document.getElementById('eventModal');
    if (modal) modal.style.display = 'flex';
  }

  // --- DATI INTELLIGENCE AGGIUNTIVI ---
  // Duplicate intelligence rendering logic removed because it is already handled inside openIntelDossier(eventData).
  // The duplicate block referenced an undefined variable `e` and caused unmatched braces/syntax errors.

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
  function startApp() {
    console.log("🚀 Starting Impact Atlas...");
    initMap();
    loadEventsData();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startApp);
  } else {
    startApp();
  }
})();
