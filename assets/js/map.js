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
  // 10. MODAL FUNCTIONS (TRIDENT INTEL CARD)
  // ============================================

  // Variabile globale per il grafico (per distruggerlo prima di ricrearlo)
  let tieRadarInstance = null;

  // Funzione Entry Point (chiamata dal bottone nel popup o dalla griglia)
  window.openModal = function (eventIdOrObj) {
    console.log("Tentativo apertura dossier:", eventIdOrObj);
    let eventData = null;

    if (typeof eventIdOrObj === 'string') {
      // Caso 1: Stringa JSON (spesso dalla griglia visuale)
      if (eventIdOrObj.startsWith('%7B') || eventIdOrObj.startsWith('{')) {
        try { 
            eventData = JSON.parse(decodeURIComponent(eventIdOrObj)); 
        } catch (err) { console.error("Errore parsing JSON modale", err); }
      } else {
        // Caso 2: ID Evento (dal marker sulla mappa)
        if (window.globalEvents) {
          const searchId = String(eventIdOrObj);
          // Cerca per event_id, cluster_id o id
          eventData = window.globalEvents.find(evt => 
            String(evt.event_id) === searchId || 
            String(evt.id) === searchId || 
            String(evt.cluster_id) === searchId
          );
        }
      }
    } else {
      // Caso 3: Oggetto già passato
      eventData = eventIdOrObj;
    }

    if (!eventData) {
      console.error(`❌ Evento non trovato per il Dossier. ID:`, eventIdOrObj);
      return; 
    }

    // Lancia la Intel Card
    window.openIntelCard(eventData);
  };

  // Funzione che popola e apre la Card
  window.openIntelCard = function(event) {
    console.log("📂 Opening Intel Card for:", event.title);

    // 1. Popola Header
    document.getElementById('intelTitle').innerText = event.title || "Titolo non disponibile";
    
    // Gestione Categoria
    const cat = event.category || event.event_category || 'EVENT';
    const catEl = document.getElementById('intelCategory');
    if(catEl) catEl.innerText = cat.toUpperCase().replace(/_/g, ' ');
    
    document.getElementById('intelDate').innerText = event.date || "";
    document.getElementById('intelLocation').innerText = event.location || event.location_precision || 'Unknown Location';

    // 2. KPI Badge (Reliability & Bias)
    const relEl = document.getElementById('intelReliability');
    if(relEl) relEl.innerText = (event.reliability || 0) + '%';
    
    const biasEl = document.getElementById('intelBias');
    if(biasEl) biasEl.innerText = event.bias_score || '0.0';

    // 3. T.I.E. Score Big Number
    const tieTotal = event.tie_total || 0;
    const tieEl = document.getElementById('intelTieScore');
    if(tieEl) tieEl.innerText = tieTotal;

    // 4. Tactical Bars (Recupero sicuro dei valori)
    const k = parseFloat(event.vec_k || event.kinetic_score || 0);
    const t = parseFloat(event.vec_t || event.target_score || 0);
    const e = parseFloat(event.vec_e || event.effect_score || 0);

    // Aggiorna testi e larghezze barre
    const elValK = document.getElementById('valK'); if(elValK) elValK.innerText = k.toFixed(1);
    const elBarK = document.getElementById('barK'); if(elBarK) elBarK.style.width = (k * 10) + '%';
    
    const elValT = document.getElementById('valT'); if(elValT) elValT.innerText = t.toFixed(1);
    const elBarT = document.getElementById('barT'); if(elBarT) elBarT.style.width = (t * 10) + '%';
    
    const elValE = document.getElementById('valE'); if(elValE) elValE.innerText = e.toFixed(1);
    const elBarE = document.getElementById('barE'); if(elBarE) elBarE.style.width = (e * 10) + '%';

    // 5. Radar Chart (Chart.js)
    const canvas = document.getElementById('tieRadarChart');
    if (canvas && typeof Chart !== 'undefined') {
        const ctx = canvas.getContext('2d');
        
        if (tieRadarInstance) {
            tieRadarInstance.destroy();
        }
        
        tieRadarInstance = new Chart(ctx, {
            type: 'radar',
            data: {
                labels: ['KINETIC', 'TARGET', 'EFFECT'],
                datasets: [{
                    label: 'Event Profile',
                    data: [k, t, e],
                    backgroundColor: 'rgba(245, 158, 11, 0.2)', // var(--primary-dim)
                    borderColor: '#f59e0b', // var(--primary)
                    borderWidth: 2,
                    pointBackgroundColor: '#1e293b', // var(--bg-surface)
                    pointBorderColor: '#f59e0b',
                    pointHoverBackgroundColor: '#fff',
                    pointHoverBorderColor: '#f59e0b'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    r: {
                        angleLines: { color: '#334155' }, 
                        grid: { color: '#334155' },
                        pointLabels: { 
                            color: '#94a3b8', 
                            font: { family: 'JetBrains Mono', size: 10, weight: '700' } 
                        },
                        ticks: { display: false, max: 10, min: 0 }
                    }
                },
                plugins: { legend: { display: false } }
            }
        });
    } else {
        console.warn("Chart.js non caricato o canvas non trovato.");
    }

    // 6. Testo Descrizione
    const descEl = document.getElementById('intelDescription');
    if(descEl) descEl.innerText = event.desc || event.description || "Nessuna descrizione disponibile.";
    
    // 7. AI Summary
    const summary = event.ai_summary || "Analisi strategica dettagliata non disponibile per questo evento.";
    const sumEl = document.getElementById('intelAiSummary');
    if(sumEl) sumEl.innerText = summary;

    // 8. Fonti (Lista)
    const list = document.getElementById('intelSourcesList');
    if(list) {
        list.innerHTML = ''; // Pulisci
        
        // Adatta in base a come arrivano le fonti
        let sources = event.sources_list || event.references || [];
        if (typeof sources === 'string') {
            try { sources = JSON.parse(sources); } catch(e) { sources = [sources]; }
        }
        
        if (sources.length > 0) {
            sources.forEach(src => {
                // Se src è un oggetto {url: ...}, prendi url, altrimenti è stringa
                const url = src.url || src;
                let domain = "Fonte";
                try { domain = new URL(url).hostname.replace('www.',''); } catch(e){ domain=url; }

                const div = document.createElement('a');
                div.className = 'source-item';
                div.href = url.startsWith('http') ? url : '#';
                div.target = "_blank";
                div.innerHTML = `<span class="source-name">${domain}</span> <i class="fa-solid fa-external-link-alt" style="font-size:0.7rem"></i>`;
                list.appendChild(div);
            });
        } else {
            list.innerHTML = '<div style="font-size:0.8rem; color:#64748b; font-style:italic; padding:10px;">Fonti riservate o non pubbliche.</div>';
        }
    }

    // 9. Mostra la Modale
    const modal = document.getElementById('intelModal');
    if(modal) {
        modal.style.display = 'flex';
        modal.classList.add('active'); // Per animazioni CSS se presenti
    }
  };

  // Funzione chiusura
  window.closeIntelCard = function(e) {
    // Chiudi se clicchi sulla X o fuori dalla card
    if (!e || e.target.id === 'intelModal' || e.target.classList.contains('close-modal')) {
        const modal = document.getElementById('intelModal');
        if(modal) modal.style.display = 'none';
    }
  };

    // ============================================================
    // A. GESTIONE INTENSITÀ (CIVILE vs MILITARE)
    // ============================================================
    const intEl = document.getElementById('modal-intensity');
    if (intEl) {
      // 1. Caso Evento Civile/Politico (Mostra N/D Grigio)
      if (typeof isCivilianEvent === 'function' && isCivilianEvent(eventData)) {
        intEl.innerHTML = `
            <div class="intensity-badge-wrapper" style="opacity:0.8; cursor:default;">
                <span style="color:#94a3b8; font-weight:700; font-size:1rem;">N/D <small style="font-size:0.65rem;">(NON-MILITARY)</small></span>
                <div class="info-icon" style="color:#94a3b8; border-color:#94a3b8; transform: scale(0.8);">i</div>
                <div class="intensity-tooltip" style="bottom:120%;">
                    <strong style="color:#94a3b8">IMPATTO NON VALUTABILE</strong><br>
                    L'evento è di natura politica, civile o diplomatica.
                </div>
            </div>`;
      }
      // 2. Caso Evento Militare (Calcolo Standard)
      else {
        const val = parseFloat(eventData.intensity || 0);
        let label = "UNKNOWN"; let colorClass = "#64748b"; let desc = "Dati insufficienti.";

        if (val <= 0.3) { label = "TACTICAL"; colorClass = "#22c55e"; desc = "Impatto limitato. Schermaglie o danni lievi."; }
        else if (val <= 0.6) { label = "OPERATIONAL"; colorClass = "#f97316"; desc = "Impatto operativo. Danni infrastrutture."; }
        else if (val <= 0.8) { label = "STRATEGIC"; colorClass = "#ef4444"; desc = "Alto impatto strategico."; }
        else { label = "CRITICAL"; colorClass = "#000000"; desc = "Evento di portata storica."; }

        const textShadow = colorClass === '#000000' ? 'text-shadow: 0 0 10px rgba(255,255,255,0.5);' : '';
        const style = `color: ${colorClass}; font-weight: 800; font-size: 1.1rem; ${textShadow}`;

        intEl.innerHTML = `
            <div class="intensity-badge-wrapper">
                <span style="${style}">${label} (${(val * 10).toFixed(1)})</span>
                <div class="info-icon">i</div>
                <div class="intensity-tooltip">
                    <strong style="color:${colorClass}">${label} IMPACT</strong><br>${desc}
                </div>
            </div>`;
      }
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
  };

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
