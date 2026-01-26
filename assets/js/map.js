// ============================================
// MAP.JS - RESTRUCTURED WITH PROPER INITIALIZATION
// ============================================

(function () {
  'use strict';
  console.log("🚀 MAP.JS UPDATED VERSION LOADED 🚀");

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
  let unitsLayer = null;

  window.allEventsData = [];
  window.globalEvents = [];
  window.currentFilteredEvents = [];

  let mapDates = []; // Historical dates index

  // Central helper to define what is civilian
  function isCivilianEvent(e) {
    // Joins all text fields to search for keywords
    const fullText = (e.category + ' ' + e.type + ' ' + e.location_precision + ' ' + e.filters).toUpperCase();

    // Words identifying a NON strictly military/kinetic event
    const civKeywords = ['CIVIL', 'POLITIC', 'ECONOM', 'HUMANITAR', 'DIPLOMA', 'ACCIDENT', 'STATEMENT'];

    // If one of these words is found, it is civilian
    if (civKeywords.some(k => fullText.includes(k))) return true;

    // Optional: excludes everything not in UA/RU (Rough Geofencing)
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
  // 1. POPUP GENERATION (Correct and with Elegant Style)
  // ==========================================
  function createPopupContent(e) {
    // 1. Safely retrieve ID
    // Use 'event_id' if exists, otherwise 'id'. 'feature' does NOT exist here.
    const id = e.event_id || e.id || (e.properties ? e.properties.event_id : null);

    // 2. Determine color
    const color = getColor(e.intensity);

    // 3. Source Footer Management
    let sourceFooter = '';
    if (e.source && e.source !== 'Unknown Source') {
      const url = e.source.startsWith('http') ? e.source : '#';
      let domain = "Original Source";
      try {
        if (url !== '#') domain = new URL(url).hostname.replace('www.', '');
      } catch (err) { }

      sourceFooter = `
            <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #334155; display: flex; align-items: center; justify-content: space-between;">
              <span style="font-size: 0.7rem; color: #64748b;">Source:</span>
              <a href="${url}" target="_blank" style="color: #3b82f6; text-decoration: none;">
                 <i class="fa-solid fa-link"></i> ${domain}
              </a>
            </div>`;
    }

    // 4. Popup HTML Construction (Elegant Style Restored)
    // Note: Button has INLINE style to ensure it is blue and beautiful as before.
    return `
    <div class="acled-popup" style="color:#e2e8f0; font-family: 'Inter', sans-serif; min-width: 260px;">
      
      <div style="border-left: 4px solid ${color}; padding-left: 12px; margin-bottom: 12px;">
        <h5 style="margin:0; font-weight:700; font-size:1rem; line-height:1.3; color:#f8fafc;">${e.title}</h5>
        
        <div style="margin-top:8px; display:flex; gap:8px;">
          <span class="popup-meta-tag" style="background:#1e293b; padding:2px 6px; border-radius:4px; font-size:0.75rem;"><i class="fa-regular fa-calendar"></i> ${e.date}</span>
          <span class="popup-meta-tag" style="background:#1e293b; padding:2px 6px; border-radius:4px; font-size:0.75rem;"><i class="fa-solid fa-tag"></i> ${e.type || 'Event'}</span>
        </div>
      </div>

      <div style="font-size:0.85rem; line-height:1.6; color:#cbd5e1; margin-bottom:15px;">
        ${e.description ? (e.description.length > 120 ? e.description.substring(0, 120) + '...' : e.description) : 'No description.'}
      </div>
      
      <div class="popup-actions">
        <button 
          class="custom-dossier-btn" 
          onclick="openModal('${id}')"> 
          <i class="fas fa-folder-open"></i> OPEN DOSSIER
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
            layer.bindPopup(`<b>Situation as of:</b> ${dateString}<br>Occupied territory`);
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
    // FIX: Check both internal state string and DOM element
    const container = L.DomUtil.get('map');
    if (map || (container && container._leaflet_id)) {
      console.log("⚠️ Map container already initialized. Skipping init.");
      return;
    }

    map = L.map('map', {
      zoomControl: false,
      preferCanvas: true, // Performance boost
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

    // DEBUG: Log all clicks
    map.on('click', function (e) {
      console.log(`🖱️ MAP CLICK AT: ${e.latlng.lat}, ${e.latlng.lng}`);
      // alert(`DEBUG: Map clicked at ${e.latlng}`);
    });

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
        // 1. Raw Data
        window.allEventsData = data.features || data;
        console.log(`💾 Data downloaded: ${window.allEventsData.length} raw events`);

        if (window.allEventsData.length === 0) {
          console.warn("⚠️ No events found in GeoJSON");
          return;
        }

        // 2. PROCESSING (Unique correct map cycle)
        window.globalEvents = window.allEventsData.map(f => {
          // Moment.js Logic
          const props = f.properties || f;

          // Attempt with explicit formats
          let m = moment(props.date, ["DD/MM/YY", "DD/MM/YYYY", "YYYY-MM-DD", "DD-MM-YYYY"]);

          // Fallback if invalid
          if (!m.isValid()) {
            m = moment(props.date);
          }

          const ts = m.isValid() ? m.valueOf() : moment().valueOf();

          return {
            ...props,
            // --- CRITICAL FIX: ID UNIFICATION ---
            // If cluster_id exists, use it as event_id. This fixes the Dossier button.
            event_id: props.event_id || props.cluster_id || props.id,
            // ------------------------------------
            lat: f.geometry ? f.geometry.coordinates[1] : props.lat,
            lon: f.geometry ? f.geometry.coordinates[0] : props.lon,
            timestamp: ts,
            date: m.isValid() ? m.format("DD/MM/YYYY") : props.date
          };
        })
          // MODIFICATION: Frontend "Junk" Filter
          .filter(e => {
            // Excludes if coordinates are 0
            if (!e.lat || !e.lon || e.lat === 0 || e.lon === 0) return false;

            return true;
          })
          .sort((a, b) => b.timestamp - a.timestamp); // Descending order

        console.log(`✅ Events processed: ${window.globalEvents.length}`);

        // 3. FILTER DEFINITION (CIVILIAN + ACTORS + SMART SEARCH)
        window._applyMapFiltersImpl = function () {
          // A. Retrieve Input (Safe handling if elements missing)
          const toggle = document.getElementById('civilianToggle');
          const showCivilian = toggle ? toggle.checked : true;

          const searchInput = document.getElementById('textSearch');
          const searchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';

          const actorSelect = document.getElementById('actorFilter');
          const selectedActor = actorSelect ? actorSelect.value : '';

          // B. Filtering Cycle
          const filtered = window.globalEvents.filter(e => {

            // 1. Civilian Filter (ORIGINAL LOGIC KEPT)
            if (typeof isCivilianEvent === 'function') {
              const isCivil = isCivilianEvent(e);
              if (isCivil && !showCivilian) return false;
            }

            // 2. Actor Filter (NEW)
            // If an actor is selected in the menu, the event must match
            if (selectedActor && e.actor !== selectedActor) {
              return false;
            }

            // 3. Smart Text Search (NEW)
            if (searchTerm) {
              // Smart Mapping: User types "Russia" -> We search Actor "RUS"
              let targetActor = null;
              if (['russia', 'russo', 'russi', 'mosca'].some(k => searchTerm.includes(k))) targetActor = 'RUS';
              if (['ucraina', 'ukraine', 'kiev'].some(k => searchTerm.includes(k))) targetActor = 'UKR';

              // Search in text fields
              const inTitle = (e.title || '').toLowerCase().includes(searchTerm);
              const inDesc = (e.description || '').toLowerCase().includes(searchTerm);
              const inLoc = (e.location_precision || '').toLowerCase().includes(searchTerm);

              // Search by smart actor (e.g. wrote "russian attacks" -> show RUS events)
              const isSmartMatch = targetActor && e.actor === targetActor;

              // If text not found AND not a smart match -> Hide
              if (!inTitle && !inDesc && !inLoc && !isSmartMatch) return false;
            }

            return true;
          });

          // C. Update Map and Counters
          window.currentFilteredEvents = filtered;
          renderInternal(filtered);

          // NEW: Dashboard Update
          if (window.Dashboard) window.Dashboard.update(filtered);
        };

        // Exposes the function
        window.applyMapFilters = window._applyMapFiltersImpl;

        // --- LIVE ACTIVATION (FUNDAMENTAL) ---
        // Connects filters to HTML inputs to update map in real time
        const inputsToCheck = ['textSearch', 'actorFilter', 'civilianToggle'];
        inputsToCheck.forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.oninput = window.applyMapFilters; // For when writing
            el.onchange = window.applyMapFilters; // For menu and checkbox
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
          // NEW: Dashboard Init
          if (window.Dashboard) window.Dashboard.init();
        } catch (e) { console.log("Charts/Dashboard error:", e); }

        console.log(`✅ Events processed: ${window.globalEvents.length} ready for map`);

        // Slider Init
        if (typeof setupTimeSlider === 'function') setupTimeSlider(window.globalEvents);

        // 4. START MAP AND RENDERING
        if (typeof window.applyMapFilters === 'function') {
          window.applyMapFilters();
        } else {
          renderInternal(window.globalEvents);
        }

        // Initialize Cluster/Map
        initMap(window.globalEvents);

      }) // <--- THIS CLOSES THE .THEN (The critical point of previous errors)
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
    // Handle both old and new toggle buttons if they exist
    const btn = document.getElementById('heatmapToggle');
    const checkbox = document.getElementById('dashboardHeatmapToggle');

    if (isHeatmapMode) {
      if (btn) {
        btn.classList.add('active');
        btn.innerHTML = '<i class="fa-solid fa-circle-nodes"></i> Cluster';
      }
      if (checkbox) checkbox.checked = true;
    } else {
      if (btn) {
        btn.classList.remove('active');
        btn.innerHTML = '<i class="fa-solid fa-layer-group"></i> Heatmap';
      }
      if (checkbox) checkbox.checked = false;
    }

    renderInternal(window.currentFilteredEvents);
  };

  window.flyToUnit = function (lat, lon, unitName) {
    if (!map) return;
    if (!lat || !lon) {
      console.warn("Unit has no coordinates");
      return;
    }

    map.flyTo([lat, lon], 10, { animate: true, duration: 1.5 });

    // Create a temporary pulse marker
    const popup = L.popup()
      .setLatLng([lat, lon])
      .setContent(`<div style="text-align:center"><b>${unitName}</b><br>Last Known Position</div>`)
      .openOn(map);
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
        // Load FIRMS data from local GeoJSON (more reliable than WMTS tiles)
        fetch('assets/data/thermal_firms.geojson')
          .then(response => response.json())
          .then(data => {
            if (!data.features || data.features.length === 0) {
              console.warn("⚠️ No FIRMS data available");
              return;
            }

            // Create layer group for thermal hotspots
            firmsLayer = L.layerGroup();

            data.features.forEach(f => {
              const coords = f.geometry.coordinates;
              const props = f.properties;
              const brightness = props.brightness || 300;

              // Color based on brightness (hotter = more red)
              let color = '#ff6b35'; // Default orange
              if (brightness >= 350) color = '#ff0000'; // Red hot
              else if (brightness >= 330) color = '#ff4500'; // Orange-red
              else if (brightness >= 310) color = '#ff6b35'; // Orange
              else color = '#ffa500'; // Yellow-orange

              const marker = L.circleMarker([coords[1], coords[0]], {
                radius: 6,
                fillColor: color,
                color: '#000',
                weight: 1,
                opacity: 0.8,
                fillOpacity: 0.7
              });

              // Popup with dossier-matching styling
              const confidenceLabel = props.confidence === 'h' ? 'HIGH' : props.confidence === 'l' ? 'LOW' : 'NOMINAL';
              const confidenceColor = props.confidence === 'h' ? '#22c55e' : props.confidence === 'l' ? '#ef4444' : '#f59e0b';
              const intensityPercent = Math.min(100, ((brightness - 280) / 120) * 100);
              const frpValue = props.frp || 0;
              const timeFormatted = props.acq_time ? `${String(props.acq_time).padStart(4, '0').slice(0, 2)}:${String(props.acq_time).padStart(4, '0').slice(2)} UTC` : 'N/A';

              marker.bindPopup(`
                <div class="firms-popup-content" style="
                  min-width: 260px;
                  max-width: 320px;
                  font-family: 'Inter', sans-serif;
                  background: #0f172a;
                  border-radius: 8px;
                  overflow: visible;
                  border: 1px solid #334155;
                ">
                  <!-- Header matching dossier style -->
                  <div style="
                    background: linear-gradient(90deg, ${color}, #d97706);
                    padding: 16px;
                    border-bottom: 1px solid rgba(0,0,0,0.3);
                  ">
                    <div style="
                      font-size: 1.1rem;
                      font-weight: 700;
                      line-height: 1.3;
                      color: #fff;
                    ">Thermal Anomaly Detected</div>
                    <div style="
                      display: flex;
                      align-items: center;
                      gap: 8px;
                      margin-top: 6px;
                      font-size: 0.8rem;
                      color: rgba(255,255,255,0.8);
                    ">
                      <span>${props.acq_date || 'N/A'}</span>
                      <span style="color: rgba(255,255,255,0.4);">|</span>
                      <span style="
                        background: rgba(0,0,0,0.25);
                        padding: 2px 8px;
                        border-radius: 4px;
                        font-weight: 600;
                        font-size: 0.7rem;
                      ">NASA FIRMS</span>
                    </div>
                  </div>
                  
                  <!-- Body -->
                  <div style="padding: 16px;">
                    <!-- Brightness Meter -->
                    <div style="margin-bottom: 16px;">
                      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                        <div style="display: flex; align-items: center; gap: 4px;">
                          <span style="font-size: 0.65rem; color: #94a3b8; text-transform: uppercase; font-weight: 700;">Brightness Temperature</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Brightness Temperature</div>
                              <div class="tooltip-body">Temperature measured by satellite infrared sensor. Higher values indicate more intense thermal radiation from fires or explosions.</div>
                              <div class="tooltip-footer">Scale: 280K (cool) to 400K+ (intense)</div>
                            </div>
                          </div>
                        </div>
                        <span style="
                          font-family: 'JetBrains Mono', monospace;
                          color: ${color};
                          font-weight: 700;
                          font-size: 1.1rem;
                        ">${brightness.toFixed(0)} K</span>
                      </div>
                      <div style="background: #1e293b; border-radius: 4px; height: 6px; overflow: hidden;">
                        <div style="
                          width: ${intensityPercent}%;
                          height: 100%;
                          background: linear-gradient(90deg, #ffa500, ${color});
                          border-radius: 4px;
                        "></div>
                      </div>
                    </div>
                    
                    <!-- Stats Grid -->
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">Satellite</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Satellite Source</div>
                              <div class="tooltip-body">NASA satellite that captured this detection. VIIRS sensors provide 375m spatial resolution.</div>
                              <div class="tooltip-footer">Updated every 12 hours per satellite.</div>
                            </div>
                          </div>
                        </div>
                        <div style="color: #f8fafc; font-weight: 600; font-size: 0.85rem;">${props.satellite || 'VIIRS'}</div>
                      </div>
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">FRP</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Fire Radiative Power</div>
                              <div class="tooltip-body">Rate of radiant energy released, measured in Megawatts. Higher FRP indicates larger or more intense fires.</div>
                              <div class="tooltip-footer">Typical range: 1-50 MW for fires.</div>
                            </div>
                          </div>
                        </div>
                        <div style="color: #f8fafc; font-weight: 600; font-size: 0.85rem; font-family: 'JetBrains Mono', monospace;">${frpValue.toFixed(1)} MW</div>
                      </div>
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">Detection Time</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Acquisition Time</div>
                              <div class="tooltip-body">Time when the satellite sensor detected this thermal anomaly, in Coordinated Universal Time.</div>
                              <div class="tooltip-footer">Detection window is ~5 minutes.</div>
                            </div>
                          </div>
                        </div>
                        <div style="color: #f8fafc; font-weight: 600; font-size: 0.85rem; font-family: 'JetBrains Mono', monospace;">${timeFormatted}</div>
                      </div>
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">Confidence</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Detection Confidence</div>
                              <div class="tooltip-body">Algorithm confidence level. High = strong thermal signature. Low = possible false positive from industrial activity.</div>
                              <div class="tooltip-footer">Based on NASA FIRMS algorithm.</div>
                            </div>
                          </div>
                        </div>
                        <div style="
                          color: ${confidenceColor};
                          font-weight: 700;
                          font-size: 0.85rem;
                        ">${confidenceLabel}</div>
                      </div>
                    </div>
                    
                    <!-- Coordinates Footer -->
                    <div style="margin-top: 12px; padding-top: 10px; border-top: 1px solid #334155; text-align: center;">
                      <span style="color: #64748b; font-size: 0.7rem; font-family: 'JetBrains Mono', monospace;">
                        ${coords[1].toFixed(5)}°N, ${coords[0].toFixed(5)}°E
                      </span>
                    </div>
                  </div>
                </div>
              `, {
                className: 'firms-popup',
                maxWidth: 340,
                minWidth: 260
              });

              firmsLayer.addLayer(marker);
            });

            firmsLayer.addTo(map);
            console.log(`✅ FIRMS layer loaded: ${data.features.length} hotspots`);

            // Show metadata info
            if (data.metadata) {
              console.log(`   Source: ${data.metadata.source}`);
              console.log(`   Generated: ${data.metadata.generated}`);
            }
          })
          .catch(err => {
            console.error("❌ Failed to load FIRMS data:", err);
          });
      } else {
        if (firmsLayer) {
          map.removeLayer(firmsLayer);
          firmsLayer = null;
        }
      }
    } else if (layerName === 'units') {
      // ORBAT Units Layer - Optimized with marker clustering
      if (isChecked) {
        console.log("🚀 STARTING UNITS FETCH...");
        // alert("DEBUG: Starting Units Fetch...");

        // Cache busting
        fetch(`assets/data/units.json?v=${new Date().getTime()}`)
          .then(response => {
            if (!response.ok) {
              alert("❌ ERROR: units.json not found (404)");
              throw new Error("HTTP 404");
            }
            return response.json();
          })
          .then(data => {
            if (!data || data.length === 0) {
              alert("⚠️ WARNING: units.json is empty!");
              console.warn("⚠️ No units data available");
              return;
            }
            // alert(`✅ LOADED ${data.length} UNITS. Rendering now...`);
            console.log(`✅ Loaded ${data.length} units.`);

            // Use marker cluster for performance
            unitsLayer = L.markerClusterGroup({
              maxClusterRadius: 50,
              spiderfyOnMaxZoom: true,
              showCoverageOnHover: false,
              zoomToBoundsOnClick: true,
              iconCreateFunction: function (cluster) {
                const markers = cluster.getAllChildMarkers();
                let uaCount = 0, ruCount = 0;
                markers.forEach(m => {
                  if (m.options.faction === 'UA') uaCount++;
                  else ruCount++;
                });
                const total = markers.length;
                const color = uaCount > ruCount ? '#3b82f6' : '#ef4444';
                return L.divIcon({
                  html: `<div style="
                    background: ${color};
                    color: white;
                    border-radius: 50%;
                    width: 36px;
                    height: 36px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-weight: bold;
                    font-size: 12px;
                    border: 2px solid white;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.4);
                  ">${total}</div>`,
                  className: 'unit-cluster-icon',
                  iconSize: [36, 36]
                });
              }
            });

            let count = 0;
            data.forEach(unit => {
              const lat = unit.last_seen_lat;
              const lon = unit.last_seen_lon;
              if (!lat || !lon) return;

              // Determine faction
              const isUA = unit.faction === 'UA';
              const isRU = unit.faction === 'RU' || unit.faction === 'RU_PROXY' || unit.faction === 'RU_PMC';
              const color = isUA ? '#3b82f6' : (isRU ? '#ef4444' : '#64748b');
              const factionLabel = isUA ? 'Ukraine' : (isRU ? 'Russia' : 'Unknown');

              // SVG Flag icons (inline for performance)
              const uaFlag = `<svg width="20" height="14" viewBox="0 0 20 14"><rect width="20" height="7" fill="#005BBB"/><rect y="7" width="20" height="7" fill="#FFD500"/></svg>`;
              const ruFlag = `<svg width="20" height="14" viewBox="0 0 20 14"><rect width="20" height="4.67" fill="#fff"/><rect y="4.67" width="20" height="4.67" fill="#0039A6"/><rect y="9.33" width="20" height="4.67" fill="#D52B1E"/></svg>`;
              const unknownFlag = `<svg width="20" height="14" viewBox="0 0 20 14"><rect width="20" height="14" fill="#64748b"/></svg>`;

              const flagSvg = isUA ? uaFlag : (isRU ? ruFlag : unknownFlag);

              const icon = L.divIcon({
                html: `<div style="
                  filter: drop-shadow(0 1px 2px rgba(0,0,0,0.5));
                  border-radius: 2px;
                  overflow: hidden;
                ">${flagSvg}</div>`,
                className: 'unit-flag-marker',
                iconSize: [20, 14],
                iconAnchor: [10, 7]
              });

              const marker = L.marker([lat, lon], {
                icon: icon,
                faction: unit.faction, // Store for cluster icon
                unitData: unit,        // Store data for click event
                zIndexOffset: 1000     // Force ON TOP of everything
              });

              // REMOVED individual click listener here to use Group listener below
              // marker.on('click', () => { ... });

              unitsLayer.addLayer(marker);
              count++;
            });

            // CENTRALIZED CLICK LISTENER (Robust)
            unitsLayer.on('click', function (a) {
              console.log("🎯 UNIT CLICKED via Layer:", a.layer.options.unitData.display_name);
              // alert("DEBUG: Unit Clicked!");
              const unit = a.layer.options.unitData;
              if (unit && typeof window.openUnitModal === 'function') {
                window.openUnitModal(unit);
              } else {
                console.error("Window.openUnitModal missing or unit data invalid");
              }
            });

            unitsLayer.addTo(map);
            // unitsLayer.bringToFront(); // Not always available on ClusterGroup, avoiding error

            console.log(`✅ Units layer loaded: ${count} markers`);
          })
          .catch(err => {
            console.error("❌ Failed to load units data:", err);
            // alert("DEBUG: Fetch Error: " + err.message);
          });
      } else {
        if (unitsLayer) {
          map.removeLayer(unitsLayer);
          unitsLayer = null;
        }
      }
    }
  };

  // Global filter function (defensive wrapper)
  // The real implementation is set after data loading in window._applyMapFiltersImpl
  window.applyMapFilters = function () {
    // If real implementation is available, use it
    if (typeof window._applyMapFiltersImpl === 'function') {
      return window._applyMapFiltersImpl();
    }

    // Defensive fallback if data not yet ready or DOM misses toggle
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

  window.closeAllModals = function () {
    ['videoModal', 'unitModal', 'reportModal'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = 'none';
    });
  };

  window.openModal = function (eventIdOrObj) {
    // Ensure clean state
    window.closeAllModals();

    console.log("Dossier opening attempt:", eventIdOrObj);
    let eventData = null;

    if (typeof eventIdOrObj === 'string') {
      if (eventIdOrObj.startsWith('%7B') || eventIdOrObj.startsWith('{')) {
        try { eventData = JSON.parse(decodeURIComponent(eventIdOrObj)); } catch (err) { }
      } else {
        // FIX: Robust comparison (converts all to string)
        if (window.globalEvents) {
          const searchId = String(eventIdOrObj);
          eventData = window.globalEvents.find(evt => String(evt.event_id) === searchId);
        }
      }
    } else {
      eventData = eventIdOrObj;
    }

    if (!eventData) {
      console.error(`❌ Event not found for Dossier. Searched ID: ${eventIdOrObj}`);
      return; // Interrupts execution if no data found
    }

    // 2. Pass data to rendering function
    window.openIntelDossier(eventData);
  };
  // ============================================
  // 10. MODAL FUNCTIONS (DOSSIER UI)
  // ============================================

  window.closeUnitModal = function (e) {
    // If e is provided, it's an event. Close only if clicking overlay.
    if (e && e.target.id !== 'unitModal' && !e.target.classList.contains('close-modal')) {
      return;
    }
    const modal = document.getElementById('unitModal');
    if (modal) modal.style.display = 'none';
  };

  window.openUnitModal = function (unit) {
    // Ensure clean state
    window.closeAllModals();

    console.log("Opening Unit Modal for:", unit.display_name);
    const modal = document.getElementById('unitModal');
    if (!modal) return;

    modal.style.display = 'flex';

    // Header
    const safeText = (txt) => txt || 'N/A';
    document.getElementById('udTitle').innerText = unit.display_name || unit.unit_name || unit.unit_id;
    document.getElementById('udFaction').innerText = unit.faction === 'UA' ? 'Ukraine' : (unit.faction === 'RU' ? 'Russia' : unit.faction);
    document.getElementById('udEchelon').innerText = safeText(unit.echelon);
    document.getElementById('udTypeBadge').innerText = safeText(unit.type);

    // Flag & Header Style
    const isUA = unit.faction === 'UA';
    const isRU = unit.faction === 'RU';
    const color = isUA ? '#3b82f6' : (isRU ? '#ef4444' : '#64748b');

    // Injected Flag
    const flagSvg = isUA
      ? `<svg class="ud-flag-large" viewBox="0 0 20 14"><rect width="20" height="7" fill="#005BBB"/><rect y="7" width="20" height="7" fill="#FFD500"/></svg>`
      : (isRU
        ? `<svg class="ud-flag-large" viewBox="0 0 20 14"><rect width="20" height="4.67" fill="#fff"/><rect y="4.67" width="20" height="4.67" fill="#0039A6"/><rect y="9.33" width="20" height="4.67" fill="#D52B1E"/></svg>`
        : `<svg class="ud-flag-large" viewBox="0 0 20 14"><rect width="20" height="14" fill="#64748b"/></svg>`);

    document.getElementById('udFlagContainer').innerHTML = flagSvg;
    document.getElementById('udHeader').style.borderBottomColor = color;

    // Left Stats
    const format = (v) => v || '--';
    document.getElementById('udBranch').innerText = safeText(unit.branch);
    document.getElementById('udGarrison').innerText = safeText(unit.garrison).replace(/<[^>]*>?/gm, '');
    document.getElementById('udStatus').innerText = unit.status || 'ACTIVE';
    document.getElementById('udStatus').style.color = (unit.status === 'destroyed') ? '#ef4444' : '#22c55e';

    // New Fields
    document.getElementById('udCommander').innerText = format(unit.commander);
    document.getElementById('udSuperior').innerText = format(unit.superior);
    document.getElementById('udDistrict').innerText = format(unit.district);

    // Calculations: Find related events
    const allEvents = Array.isArray(window.globalEvents) ? window.globalEvents : [];

    // Normalize unit name for search
    const unitName = (unit.unit_name || unit.unit_id || '').toLowerCase();

    // Filter events that mention this unit
    // Note: event.units is a string or array.
    const relatedEvents = allEvents.filter(e => {
      if (!e.units) return false;
      // e.units might be JSON string
      let uList = [];
      try {
        uList = typeof e.units === 'string' ? JSON.parse(e.units) : e.units;
      } catch (e) { }

      // Check fuzzy match in list
      return uList.some(u => {
        const n = (u.unit_name || u.unit_id || '').toLowerCase();
        const oid = (u.orbat_id || '');
        return n.includes(unitName) || (unit.orbat_id && oid === unit.orbat_id);
      });
    });

    // Update Metrics
    document.getElementById('udMentions').innerText = relatedEvents.length;
    document.getElementById('udAvgTie').innerText = unit.avg_tie || '--';
    document.getElementById('udTactic').innerText = unit.primary_tactic || '--';

    // Engagement Freq (events in last 30 days)
    // Assuming simple count for now
    document.getElementById('udEngFreq').innerText = relatedEvents.length > 0
      ? (relatedEvents.length / 30).toFixed(1) + "/day"
      : "Low";

    // Related Events List
    const listEl = document.getElementById('udEventsList');
    listEl.innerHTML = '';

    if (relatedEvents.length === 0) {
      listEl.innerHTML = '<div class="ud-event-item" style="cursor:default; color:#64748b; border:none;">No recent activity linked.</div>';
    } else {
      // Sort by date desc
      relatedEvents.sort((a, b) => new Date(b.date) - new Date(a.date));

      relatedEvents.slice(0, 50).forEach(e => {
        const el = document.createElement('div');
        el.className = 'ud-event-item';
        el.onclick = () => {
          // Open event dossier from unit dossier
          window.openModal(e);
        };
        el.innerHTML = `
             <div style="font-size:0.75rem; color:#f59e0b; margin-bottom:2px;">${e.date}</div>
             <div style="font-size:0.85rem; font-weight:600; color:#e2e8f0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${e.title}</div>
           `;
        listEl.appendChild(el);
      });
    }

    // Sparkline (Activity over time)
    renderSparkline(relatedEvents);
  };

  function renderSparkline(events) {
    const container = document.getElementById('udSparkline');
    container.innerHTML = '';
    if (!events.length) return;

    // Bucket by date
    const buckets = {};
    events.forEach(e => {
      const d = e.date;
      buckets[d] = (buckets[d] || 0) + 1;
    });

    const dates = Object.keys(buckets).sort();
    if (dates.length < 2) return;

    const maxVal = Math.max(...Object.values(buckets));

    dates.forEach(d => {
      const val = buckets[d];
      const h = Math.max(10, (val / maxVal) * 100);
      const bar = document.createElement('div');
      bar.style.width = '100%';
      bar.style.height = h + '%';
      bar.style.background = '#3b82f6';
      bar.style.opacity = '0.7';
      bar.title = `${d}: ${val} events`;
      container.appendChild(bar);
    });
  }

  let tieRadarInstance = null; // Global instance for the modal chart

  window.openIntelDossier = function (eventData) {
    console.log("📂 Opening Dossier for:", eventData.title);

    document.getElementById('videoModal').style.display = 'flex'; // Use Flex for centering

    // --- 1. CONTEXT (Left Column) ---
    document.getElementById('modalTitle').innerText = eventData.title || "Title not available";
    document.getElementById('modalDate').innerText = eventData.date || "Unknown Date";

    // Type Tag
    const typeTag = document.getElementById('modalType');
    if (typeTag) typeTag.innerText = (eventData.type || "EVENT").toUpperCase();

    // Description
    const descEl = document.getElementById('modalDesc');
    if (descEl) descEl.innerText = eventData.description || "No description available for this event.";

    // --- 1.5 UNITS INVOLVED ---
    const unitsContainer = document.getElementById('modalUnits');
    const unitsList = document.getElementById('modalUnitsList');

    if (unitsContainer && unitsList) {
      let units = [];
      try {
        if (eventData.units) {
          units = typeof eventData.units === 'string' ? JSON.parse(eventData.units) : eventData.units;
        }
      } catch (e) {
        console.warn("Failed to parse units:", e);
      }

      if (units && units.length > 0) {
        unitsContainer.style.display = 'block';
        unitsList.innerHTML = units.map(u => {
          const isUA = u.faction === 'UA';
          const isRU = u.faction === 'RU' || u.faction === 'RU_PROXY' || u.faction === 'RU_PMC';
          const bgColor = isUA ? 'rgba(59, 130, 246, 0.2)' : (isRU ? 'rgba(239, 68, 68, 0.2)' : 'rgba(100, 116, 139, 0.2)');
          const borderColor = isUA ? '#3b82f6' : (isRU ? '#ef4444' : '#64748b');
          const flag = isUA ? '🇺🇦' : (isRU ? '🇷🇺' : '🏳️');

          return `
            <div style="
              display: inline-flex;
              align-items: center;
              gap: 6px;
              background: ${bgColor};
              border: 1px solid ${borderColor};
              padding: 4px 8px;
              border-radius: 4px;
              font-size: 0.75rem;
              color: #f1f5f9;
              font-family: 'JetBrains Mono', monospace;
            ">
              <span style="font-size: 0.9rem;">${flag}</span>
              <span style="font-weight: 600;">${u.display_name || u.unit_name || u.unit_id}</span>
              <span style="opacity: 0.6; font-size: 0.7rem;">${u.status || 'ACTIVE'}</span>
          `;
        }).join('');
      } else {
        // Show section with "No units detected" placeholder
        unitsContainer.style.display = 'block';
        unitsList.innerHTML = '<span style="opacity: 0.5; font-style: italic;">No units detected for this event</span>';
      }
    }

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
          let domain = typeof src === 'string' ? src : (src.name || src.source || src.url || "Source");

          // Robust URL extraction
          let url = "#";
          if (typeof src === 'string') {
            url = src;
          } else if (typeof src === 'object') {
            url = src.url || src.link || src.source_url || src.uri || "#";
          }

          // Ensure protocol
          if (url !== '#' && !url.startsWith('http')) {
            url = 'https://' + url;
          }
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
    // --- HOST FUNCTION: Language Parser ---
    function getLocalizedText(text) {
      if (!text) return "";
      // 1. PRIORITIZE ENGLISH ([EN])
      if (text.includes('[EN]')) {
        const parts = text.split('[EN]');
        if (parts.length > 1) {
          // Return text after [EN] until end or next tag
          return parts[1].split('[')[0].trim();
        }
      }
      // 2. Fallback to Italian ([IT]) or raw text
      if (text.includes('[IT]')) {
        const parts = text.split('[IT]');
        if (parts.length > 1) {
          return parts[1].split('[')[0].trim();
        }
      }
      return text;
    }

    // --- 4. THE STRATEGIST ---
    const stratBox = document.getElementById('modal-strategist-content');
    if (stratBox) {
      // Use language parser
      let rawReasoning = eventData.ai_reasoning || "AI Analysis confirms high probability of kinetic event based on cross-referenced multi-source reporting. Strategic impact affects local logistics.";
      const reasoning = getLocalizedText(rawReasoning);
      stratBox.innerHTML = `
        <div style="display:flex; align-items:center; gap:8px; margin-bottom:10px;">
            <h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin:0;">AI STRATEGIST ANALYSIS</h5>
            <div class="info-icon-wrapper">
                <div class="info-icon">i</div>
                <div class="tooltip-card">
                    <div class="tooltip-header">AI Strategist Analysis</div>
                    <div class="tooltip-body">This section provides an AI-generated analysis of the event's strategic implications, based on available intelligence.</div>
                    <div class="tooltip-footer">Powered by advanced language models.</div>
                </div>
            </div>
        </div>
        <p style="font-size:0.9rem; line-height:1.5; color:#cbd5e1;">${reasoning}</p>
      `;
    }

    // ============================================================
    // B. SCORE & CHART MANAGEMENT 
    // ============================================================
    const score = parseInt(eventData.reliability || eventData.Reliability || eventData.confidence || 0);

    // Define Colors and Text (ENGLISH DEFAULT)
    let relData = {
      label: "UNVERIFIED",
      color: "#64748b",
      desc: "Insufficient data to evaluate reliability.",
      footer: "Score withheld due to lack of sources."
    };

    if (score >= 80) {
      relData = {
        label: "CONFIRMED",
        color: "#22c55e",
        desc: "Visually confirmed. Event supported by verified footage (IMINT) or precise geolocation.",
        footer: "Max score guaranteed by visual proof."
      };
    } else if (score >= 60) {
      relData = {
        label: "RELIABLE",
        color: "#84cc16",
        desc: "Highly probable. Confirmed by multiple independent vectors or reputable institutional sources.",
        footer: "High score due to narrative convergence."
      };
    } else if (score >= 40) {
      relData = {
        label: "UNCERTAIN",
        color: "#f59e0b",
        desc: "Pending verification. Reported by mainstream or credible local sources, but not field-verified.",
        footer: "Score based on historical source reputation."
      };
    } else if (score < 40) {
      relData = {
        label: "DUBIOUS",
        color: "#ef4444",
        desc: "Low Confidence. High risk of circular reporting or disinformation.",
        footer: "Limited score due to lack of independent corroboration."
      };
    }

    if (typeof renderConfidenceChart === 'function') {
      renderConfidenceChart(score, relData.color);
    }

    // --- RELIABILITY GRADIENT BAR (NEW VISUAL - CLEAN) ---
    const relContainer = document.getElementById('modal-reliability-badge');
    if (relContainer) {
      // Calculate marker position (0-100%)
      const markerPos = Math.max(0, Math.min(100, score));

      relContainer.innerHTML = `
            <div style="margin-top:10px;">
                <div class="reliability-label">
                    <span>Reliability Score</span> <!-- Title Case -->
                    <strong style="color:${relData.color}">${score}%</strong>
                </div>
                
                <!-- GRADIENT BAR -->
                <div class="reliability-bar-track" style="margin-bottom:8px;">
                    <div class="reliability-bar-fill" style="width: 100%;"></div> 
                    <div class="reliability-marker" style="left: ${markerPos}%"></div>
                </div>

                <!-- INFO ROW (LABEL + TOOLTIP) -->
                <div style="display:flex; align-items:center;">
                     <!-- REMOVED: Static Labels below bar -->
                     
                     <span style="font-size:0.7rem; color:${relData.color}; font-weight:700; letter-spacing:1px; margin-right:5px;">${relData.label}</span>
                     
                     <!-- TOOLTIP WRAPPER (Mini-Card) -->
                     <div class="info-icon-wrapper">
                        <div class="info-icon">i</div>
                        <div class="tooltip-card">
                            <div class="tooltip-header">${relData.label} (${score}%)</div> <!-- Keep classification uppercase/bold as user liked that part -->
                            <div class="tooltip-body">${relData.desc}</div>
                            <div class="tooltip-footer">${relData.footer}</div>
                        </div>
                     </div>
                </div>
            </div>`;
    }

    if (typeof renderBibliography === 'function') {
      renderBibliography(eventData.references || []);
    }

    const modal = document.getElementById('videoModal') || document.getElementById('eventModal');
    if (modal) modal.style.display = 'flex';
  }

  // --- ADDITIONAL INTELLIGENCE DATA ---
  // Duplicate intelligence rendering logic removed because it is already handled inside openIntelDossier(eventData).
  // The duplicate block referenced an undefined variable `e` and caused unmatched braces/syntax errors.

  // Function to draw sources (Updated for URL lists)
  function renderBibliography(references) {
    const container = document.getElementById('modal-bibliography');
    if (!container) return;

    container.innerHTML = '';

    // If no references or empty list
    if (!references || references.length === 0) {
      container.innerHTML = '<div style="padding:10px; background:rgba(255,255,255,0.02); border-radius:4px; color:#64748b; font-style:italic; font-size:0.85rem; text-align:center;">No aggregated sources available for this event.</div>';
      return;
    }

    let html = `<h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin-bottom:15px; border-bottom:1px solid #334155; padding-bottom:5px; display:flex; align-items:center; gap:8px;"><i class="fa-solid fa-link"></i> Related Sources & Intelligence</h5>`;

    references.forEach((ref, idx) => {
      // Robust management: supports both strings (URs) and old objects
      let url = (typeof ref === 'object' && ref.url) ? ref.url : ref;

      // If not valid link, show as text, otherwise create link
      let isLink = typeof url === 'string' && (url.startsWith('http') || url.startsWith('www'));

      // Aesthetics: Extracts domain to not show kilometer-long URLs (e.g. "twitter.com")
      let displayName = "External Source";
      if (isLink) {
        try {
          const urlObj = new URL(url.startsWith('http') ? url : 'https://' + url);
          displayName = urlObj.hostname.replace('www.', '');
        } catch (e) { displayName = url; }
      } else {
        displayName = "Archive Reference";
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

  // Updated Chart Function with Dynamic Color
  let confChart = null;
  function renderConfidenceChart(score, color = '#f59e0b') { // <--- Added color parameter
    const ctxEl = document.getElementById('confidenceChart');
    if (!ctxEl) return;

    const ctx = ctxEl.getContext('2d');
    if (confChart) confChart.destroy();

    confChart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        datasets: [{
          data: [score, 100 - score],
          backgroundColor: [color, '#1e293b'], // <--- Use dynamic color here
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
          ctx.fillStyle = color; // <--- And also here for central text
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
      grid.innerHTML = `<div style="grid-column: 1 / -1; text-align:center; color:#64748b; padding:40px;"><i class="fa-solid fa-camera-retro" style="font-size:2rem; margin-bottom:10px; opacity:0.5;"></i><br>No visual media found.</div>`;
    }
  };

  // ============================================
  // 11. REPORT GENERATION MODAL
  // ============================================

  window.openReportModal = function () {
    console.log("Opening Report Modal...");
    const modal = document.getElementById('reportModal');
    if (!modal) return;

    // Set defaults: Start = 30 days ago, End = Today
    const today = new Date().toISOString().split('T')[0];
    const past = new Date();
    past.setDate(past.getDate() - 30);
    const start = past.toISOString().split('T')[0];

    document.getElementById('reportStartDate').value = start;
    document.getElementById('reportEndDate').value = today;
    document.getElementById('reportLiveToggle').checked = true;
    window.toggleReportLiveDate();

    modal.style.display = 'flex';
  };

  window.closeReportModal = function (e) {
    if (e && e.target.id !== 'reportModal' && !e.target.classList.contains('close-modal')) {
      return;
    }
    const modal = document.getElementById('reportModal');
    if (modal) modal.style.display = 'none';
  };

  window.toggleReportLiveDate = function () {
    const isLive = document.getElementById('reportLiveToggle').checked;
    const endInput = document.getElementById('reportEndDate');
    if (isLive) {
      endInput.disabled = true;
      endInput.style.opacity = '0.5';
      endInput.value = new Date().toISOString().split('T')[0];
    } else {
      endInput.disabled = false;
      endInput.style.opacity = '1';
    }
  };

  window.generateReport = function () {
    const start = document.getElementById('reportStartDate').value;
    let end = document.getElementById('reportEndDate').value;
    const isLive = document.getElementById('reportLiveToggle').checked;

    if (isLive) {
      end = 'LIVE';
    }

    if (!start) {
      alert("Please select a start date.");
      return;
    }

    // Redirect to report.html with params
    const url = `report.html?start=${start}&end=${end}`;
    window.open(url, '_blank');

    window.closeReportModal();
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
