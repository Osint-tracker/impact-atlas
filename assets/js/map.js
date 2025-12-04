// ============================================
// MAP.JS - FINAL FULL EDITION (Moment.js + Modal Fix + Layer Switcher)
// ============================================

// --- CONFIGURAZIONE & VARIABILI GLOBALI ---
let firmsLayer = null; // Layer NASA FIRMS
let map;
let eventsLayer;
let heatLayer = null;
let isHeatmapMode = false;

// --- NUOVE VARIABILI PER LAYER MAPPE ---
let currentFrontlineLayer = null; // Il layer della mappa tattica (DeepState/ISW)

// Dati Globali
window.globalEvents = [];
window.currentFilteredEvents = [];

const impactColors = { 'critical': '#ef4444', 'high': '#f97316', 'medium': '#eab308', 'low': '#64748b' };
const typeIcons = {
  'drone': 'fa-plane-up', 'missile': 'fa-rocket', 'artillery': 'fa-bomb',
  'energy': 'fa-bolt', 'fire': 'fa-fire', 'naval': 'fa-anchor',
  'cultural': 'fa-landmark', 'eco': 'fa-leaf', 'default': 'fa-crosshairs'
};

// --- INIZIALIZZAZIONE MAPPA ---
let initMap = function () {
  map = L.map('map', {
    zoomControl: false, preferCanvas: true, wheelPxPerZoomLevel: 120
  }).setView([48.5, 32.0], 6);

  L.control.zoom({ position: 'bottomright' }).addTo(map);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19, attribution: '&copy; IMPACT ATLAS'
  }).addTo(map);

  // --- CARICAMENTO LAYER DI DEFAULT (DEEPSTATE) ---
  loadFrontlineLayer('assets/data/frontline.geojson', '#f59e0b');

  eventsLayer = L.markerClusterGroup({
    chunkedLoading: true, maxClusterRadius: 45, spiderfyOnMaxZoom: true,
    iconCreateFunction: function (cluster) {
      var count = cluster.getChildCount();
      var size = count < 10 ? 'small' : (count < 100 ? 'medium' : 'large');
      return new L.DivIcon({
        html: `<div><span>${count}</span></div>`,
        className: `marker-cluster marker-cluster-${size}`,
        iconSize: new L.Point(40, 40)
      });
    }
  });
  map.addLayer(eventsLayer);
};

// --- CARICAMENTO DATI ---
async function loadEventsData() {
  try {
    const res = await fetch('assets/data/events.geojson');
    if (!res.ok) throw new Error("Errore fetch GeoJSON");
    const data = await res.json();

    // MAPPING CON MOMENT.JS
    window.globalEvents = data.features.map(f => {
      let m = moment(f.properties.date);
      if (!m.isValid()) {
        m = moment(f.properties.date, ["DD/MM/YYYY", "DD-MM-YYYY", "DD.MM.YYYY"]);
      }

      const ts = m.isValid() ? m.valueOf() : moment().valueOf();

      return {
        ...f.properties,
        lat: f.geometry.coordinates[1],
        lon: f.geometry.coordinates[0],
        timestamp: ts
      };
    }).sort((a, b) => a.timestamp - b.timestamp);

    console.log("Totale eventi pronti:", window.globalEvents.length);

    window.currentFilteredEvents = [...window.globalEvents];

    setupTimeSlider(window.globalEvents);
    window.updateMap(window.globalEvents);

    if (document.getElementById('eventCount')) {
      document.getElementById('eventCount').innerText = window.globalEvents.length;
      document.getElementById('lastUpdate').innerText = new Date().toLocaleDateString();
    }

    if (typeof window.initCharts === 'function') window.initCharts(window.globalEvents);

  } catch (e) { console.error("Errore sistema:", e); }
}

// --- LOGICA RENDERING ---
window.updateMap = function (events) {
  window.currentFilteredEvents = events;
  resetSliderToMax();
  renderInternal(window.currentFilteredEvents);
};

function renderInternal(eventsToDraw) {
  eventsLayer.clearLayers();
  if (heatLayer) map.removeLayer(heatLayer);

  if (isHeatmapMode) {
    if (typeof L.heatLayer === 'undefined') return;
    const heatPoints = eventsToDraw.map(e => [e.lat, e.lon, (e.intensity || 0.5) * 2]);
    heatLayer = L.heatLayer(heatPoints, {
      radius: 25, blur: 15, maxZoom: 10,
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
}

// --- SLIDER ---
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

  slider.addEventListener('input', (e) => {
    const selectedVal = parseInt(e.target.value);
    if (selectedVal >= maxTime) display.innerText = "LIVE";
    else display.innerText = moment(selectedVal).format('DD/MM/YYYY');

    const timeFiltered = window.currentFilteredEvents.filter(ev => ev.timestamp <= selectedVal);
    renderInternal(timeFiltered);
  });
}

function resetSliderToMax() {
  const slider = document.getElementById('timeSlider');
  if (slider && window.currentFilteredEvents.length > 0) {
    slider.value = slider.max;
    document.getElementById('sliderCurrentDate').innerText = "LIVE";
  }
}

window.toggleVisualMode = function () {
  isHeatmapMode = !isHeatmapMode;
  const btn = document.getElementById('heatmapToggle');
  const slider = document.getElementById('timeSlider');

  if (isHeatmapMode) {
    btn.classList.add('active');
    btn.innerHTML = '<i class="fa-solid fa-circle-nodes"></i> Cluster';
  } else {
    btn.classList.remove('active');
    btn.innerHTML = '<i class="fa-solid fa-layer-group"></i> Heatmap';
  }

  const currentSliderVal = parseInt(slider.value);
  const timeFiltered = window.currentFilteredEvents.filter(ev => ev.timestamp <= currentSliderVal);
  renderInternal(timeFiltered);
};

// --- HELPERS MARKER ---
function getColor(val) { const v = val || 0.2; if (v >= 0.8) return impactColors.critical; if (v >= 0.6) return impactColors.high; if (v >= 0.4) return impactColors.medium; return impactColors.low; }

function getIconClass(type) { if (!type) return typeIcons.default; const t = type.toLowerCase(); for (const [key, icon] of Object.entries(typeIcons)) { if (t.includes(key)) return icon; } return typeIcons.default; }

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
  // Codifica i dati per il modale
  const eventData = encodeURIComponent(JSON.stringify(e));
  const color = getColor(e.intensity);

  // --- GESTIONE FONTE (DESIGN PROFESSIONALE) ---
  let sourceFooter = '';

  if (e.source && e.source !== 'Unknown Source') {
    const url = e.source.startsWith('http') ? e.source : '#';
    // Se l'URL è lungo, mostriamo solo il dominio o "Fonte Originale"
    let domain = "Fonte Originale";
    try {
      if (url !== '#') domain = new URL(url).hostname.replace('www.', '');
    } catch (err) { }

    // Footer con stile "scheda" leggermente diverso
    sourceFooter = `
        <div style="
            margin-top: 10px;
            padding-top: 8px;
            border-top: 1px solid #e2e8f0;
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 0.7rem;
            color: #94a3b8;
        ">
            <span style="display:flex; align-items:center; gap:5px;">
                <i class="fa-solid fa-link"></i> Fonte:
            </span>
            <a href="${url}" target="_blank" style="
                color: #3b82f6; 
                text-decoration: none; 
                font-weight: 600; 
                background: rgba(59, 130, 246, 0.1); 
                padding: 2px 6px; 
                border-radius: 4px;
                transition: background 0.2s;
            ">
                ${domain} <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.6rem;"></i>
            </a>
        </div>
      `;
  }

  // --- STRUTTURA POPUP ---
  return `
    <div class="acled-popup" style="color:#334155; font-family: 'Inter', sans-serif; min-width: 200px;">
        
        <div style="border-left: 4px solid ${color}; padding-left: 12px; margin-bottom: 12px;">
            <h5 style="margin:0; font-weight:700; font-size:0.95rem; line-height:1.2;">
                ${e.title}
            </h5>
            <div style="color:#64748b; font-size:0.75rem; margin-top:4px; display:flex; gap:10px;">
                <span><i class="fa-regular fa-calendar"></i> ${e.date}</span>
                <span><i class="fa-solid fa-tag"></i> ${e.type}</span>
            </div>
        </div>

        <div style="font-size:0.85rem; line-height:1.5; color:#475569; margin-bottom:12px;">
            ${e.description ? (e.description.length > 100 ? e.description.substring(0, 100) + '...' : e.description) : 'Nessuna descrizione disponibile.'}
        </div>

        <button onclick="openModal('${eventData}')" class="btn-primary" style="
            width:100%; 
            padding: 8px; 
            font-size:0.8rem; 
            background: #1e293b; 
            color: white; 
            border: none; 
            border-radius: 6px; 
            cursor: pointer; 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            gap: 6px;
        ">
            <i class="fa-solid fa-expand"></i> Apri Dossier Completo
        </button>

        ${sourceFooter}

    </div>`;
}

// --- LOGICA MODALE COMPLETA (RIPRISTINATA) ---

window.openModal = function (eventJson) {
  const e = JSON.parse(decodeURIComponent(eventJson));

  document.getElementById('modalTitle').innerText = e.title;
  document.getElementById('modalDesc').innerText = e.description || "Nessun dettaglio.";
  document.getElementById('modalType').innerText = e.type;
  document.getElementById('modalDate').innerText = e.date;

  const vidCont = document.getElementById('modalVideoContainer');
  vidCont.innerHTML = '';

  // Gestione Video
  if (e.video && e.video !== 'null') {
    if (e.video.includes('youtu')) {
      const embed = e.video.replace('watch?v=', 'embed/').split('&')[0];
      vidCont.innerHTML = `<iframe src="${embed}" frameborder="0" allowfullscreen style="width:100%; height:400px; border-radius:8px;"></iframe>`;
    } else {
      vidCont.innerHTML = `<a href="${e.video}" target="_blank" class="btn-primary">Media Esterno</a>`;
    }
  }

  // Gestione Juxtapose (Before/After)
  const sliderCont = document.getElementById('modalJuxtapose');
  sliderCont.innerHTML = '';
  if (e.before_img && e.after_img) {
    sliderCont.innerHTML = `
       <h4 style="color:white; margin:20px 0 10px;">Battle Damage Assessment</h4>
       <div class="juxtapose-wrapper" onmousemove="updateSlider(event, this)">
         <div class="juxtapose-img" style="background-image:url('${e.before_img}')"></div>
         <div class="juxtapose-img after" style="background-image:url('${e.after_img}'); width:50%;"></div>
         <div class="juxtapose-handle" style="left:50%"><div class="juxtapose-button"><i class="fa-solid fa-arrows-left-right"></i></div></div>
       </div>
     `;
  }

  // Grafico Affidabilità
  const conf = e.confidence || 85;
  renderConfidenceChart(conf);

  document.getElementById('videoModal').style.display = 'flex';
};

window.updateSlider = function (e, wrapper) {
  const rect = wrapper.getBoundingClientRect();
  let pos = ((e.clientX - rect.left) / rect.width) * 100;
  pos = Math.max(0, Math.min(100, pos));
  wrapper.querySelector('.after').style.width = `${pos}%`;
  wrapper.querySelector('.juxtapose-handle').style.left = `${pos}%`;
};

let confChart = null;
function renderConfidenceChart(score) {
  const ctxEl = document.getElementById('confidenceChart');
  if (!ctxEl) return;

  const ctx = ctxEl.getContext('2d');
  if (confChart) confChart.destroy();

  confChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      datasets: [{
        data: [score, 100 - score],
        backgroundColor: ['#f59e0b', '#334155'],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      cutout: '75%',
      animation: false,
      plugins: { tooltip: { enabled: false } }
    },
    plugins: [{
      id: 'text',
      beforeDraw: function (chart) {
        var width = chart.width, height = chart.height, ctx = chart.ctx;
        ctx.restore();
        var fontSize = (height / 100).toFixed(2);
        ctx.font = "bold " + fontSize + "em Inter";
        ctx.textBaseline = "middle";
        ctx.fillStyle = "#f59e0b";
        var text = score + "%",
          textX = Math.round((width - ctx.measureText(text).width) / 2),
          textY = height / 2;
        ctx.fillText(text, textX, textY);
        ctx.save();
      }
    }]
  });
}

// ==========================================
// 🗺️ LOGICA GESTIONE MAPPE (LAYER SWITCHER)
// ==========================================

window.selectMapSource = function (card, sourceName) {
  // 1. Reset Grafico (UI)
  document.querySelectorAll('.map-layer-card').forEach(c => {
    c.classList.remove('active');
    const icon = c.querySelector('.status-dot');
    if (icon) {
      icon.classList.remove('fa-circle-dot', 'fa-solid');
      icon.classList.add('fa-circle', 'fa-regular');
    }
  });

  // 2. Attiva Card Cliccata
  if (card) {
    card.classList.add('active');
    const activeIcon = card.querySelector('.status-dot');
    if (activeIcon) {
      activeIcon.classList.remove('fa-circle', 'fa-regular');
      activeIcon.classList.add('fa-circle-dot', 'fa-solid');
    }
  }

  // 3. Logica Caricamento Dati
  console.log(`🔄 Cambio fonte mappa: ${sourceName}`);

  let dataUrl = '';
  let colorStyle = '#ff3838'; // Default Rosso

  if (sourceName === 'deepstate') {
    dataUrl = 'assets/data/frontline.geojson';
    colorStyle = '#f59e0b'; // Amber
  } else if (sourceName === 'isw') {
    dataUrl = 'assets/data/frontline_isw.geojson';
    colorStyle = '#38bdf8'; // Azzurro ISW
  }

  loadFrontlineLayer(dataUrl, colorStyle);
};

function loadFrontlineLayer(url, color) {
  // Rimuovi il vecchio layer se esiste per evitare sovrapposizioni
  if (currentFrontlineLayer) {
    map.removeLayer(currentFrontlineLayer);
  }

  fetch(url)
    .then(response => {
      if (!response.ok) throw new Error("File mappa non trovato: " + url);
      return response.json();
    })
    .then(data => {
      // Crea il nuovo layer GeoJSON
      currentFrontlineLayer = L.geoJSON(data, {
        style: function (feature) {
          return {
            color: color,
            weight: 2,
            opacity: 0.8,
            fillOpacity: 0.1 // Leggero riempimento per le aree occupate
          };
        },
        // Opzionale: Popup se clicchi sulla linea del fronte
        onEachFeature: function (feature, layer) {
          if (feature.properties && feature.properties.name) {
            layer.bindPopup(feature.properties.name);
          }
        }
      }).addTo(map);
      console.log("✅ Mappa caricata: " + url);
    })
    .catch(err => {
      console.error("❌ Errore caricamento mappa:", err);
      // Non mostrare alert invasivi, magari logga solo in console
    });
}
// --- NASA FIRMS INTEGRATION ---
window.toggleTechLayer = function (layerName, checkbox) {
  const isChecked = checkbox.checked;
  console.log(`Toggle ${layerName}: ${isChecked}`);

  if (layerName === 'firms') {
    if (isChecked) {
      // Aggiungi layer NASA VIIRS (Rilevamento Termico)
      // Usa l'API GIBS della NASA (Gratuita e open)
      firmsLayer = L.tileLayer('https://map1.vis.earthdata.nasa.gov/wmts-webmerc/VIIRS_SNPP_Fires_375m_Day_Night/default/{time}/GoogleMapsCompatible_Level8/{z}/{y}/{x}.png', {
        attribution: 'NASA FIRMS',
        maxZoom: 12,
        minZoom: 6,
        time: moment().format('YYYY-MM-DD'), // Data di oggi
        opacity: 0.7, // Un po' trasparente per vedere sotto
        bounds: [[44.0, 22.0], [53.0, 40.0]] // LIMITA IL CARICAMENTO ALL'UCRAINA (Lat/Lon box approssimativo)
      }).addTo(map);
    } else {
      if (firmsLayer) map.removeLayer(firmsLayer);
    }
  }
  // Qui puoi aggiungere altri casi per meteo, etc.
};

// ... altre funzioni ...

// --- QUESTA È LA FUNZIONE CHE AGGIORNA TUTTO ---
window.updateMap = function (events) {
  window.currentFilteredEvents = events;
  resetSliderToMax();

  // 1. Disegna i punti sulla mappa
  renderInternal(window.currentFilteredEvents);

  // 2. AGGIUNTA FONDAMENTALE: Aggiorna anche la griglia visuale!
  renderVisualGrid(window.currentFilteredEvents);
};

// ... altre funzioni ...

// --- FUNZIONE VISUAL TRACKER (Incollala in fondo al file) ---
function renderVisualGrid(events) {
  const grid = document.getElementById('visual-grid-content');
  if (!grid) return; // Se non siamo nella tab visuale, esci

  grid.innerHTML = '';

  // Filtra: solo eventi con Immagini O Video Youtube/Esterni
  const visualEvents = events.filter(e => e.image || (e.video && e.video !== 'null'));

  visualEvents.forEach(e => {
    const item = document.createElement('div');
    item.className = 'visual-item';
    // Stile base card
    item.style.cssText = "background:#1e293b; border-radius:8px; overflow:hidden; position:relative; aspect-ratio: 16/9; cursor:pointer; border:1px solid #334155;";

    // Logica Thumbnail
    let bgUrl = e.image;

    // Se non c'è immagine ma c'è video YouTube, ruba la copertina
    if (!bgUrl && e.video && e.video.includes('youtu')) {
      try {
        let vidId = null;
        if (e.video.includes('v=')) vidId = e.video.split('v=')[1]?.split('&')[0];
        else if (e.video.includes('youtu.be/')) vidId = e.video.split('youtu.be/')[1]?.split('?')[0];

        if (vidId) bgUrl = `https://img.youtube.com/vi/${vidId}/mqdefault.jpg`;
      } catch (err) { console.error("Errore parsing YouTube ID", err); }
    }

    // Se abbiamo trovato qualcosa da mostrare
    if (bgUrl) {
      item.innerHTML = `
            <div style="background-image:url('${bgUrl}'); width:100%; height:100%; background-size:cover; background-position:center; transition:transform 0.3s;"></div>
            <div style="position:absolute; bottom:0; left:0; width:100%; background:linear-gradient(to top, rgba(0,0,0,0.9), transparent); padding:10px; color:white;">
                <div style="font-size:0.85rem; font-weight:bold; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; text-shadow: 0 1px 2px black;">
                   ${e.type === 'video' ? '<i class="fa-solid fa-play-circle"></i> ' : ''} ${e.title}
                </div>
                <div style="font-size:0.75rem; color:#cbd5e1; display:flex; justify-content:space-between;">
                    <span>${e.date}</span>
                    <span style="color:#f59e0b;">${e.intensity > 0.7 ? 'CRITICAL' : ''}</span>
                </div>
            </div>
            `;

      // Cliccando apre il modale esistente
      const eventData = encodeURIComponent(JSON.stringify(e));
      item.onclick = () => window.openModal(eventData); // Assicurati che openModal sia globale
      grid.appendChild(item);
    }
  });

  if (visualEvents.length === 0) {
    grid.innerHTML = `
            <div style="grid-column: 1 / -1; text-align:center; color:#64748b; padding:40px;">
                <i class="fa-solid fa-camera-retro" style="font-size:2rem; margin-bottom:10px; opacity:0.5;"></i><br>
                Nessun media visivo trovato per i filtri correnti.
            </div>`;
  }
}

// Start App
initMap();
loadEventsData();