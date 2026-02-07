// ============================================
// TIMELINE-MANAGER.JS - INTEGRATO CON SIDEBAR
// ============================================

let tlInstance = null;
let rawTimelineData = null;

// Configurazione Estetica Timeline
const timelineOptions = {
  language: 'it',
  start_at_end: true,
  default_bg_color: { r: 255, g: 255, b: 255 },
  timenav_height_percentage: 25,
  marker_height_min: 30,
  scale_factor: 2,
  timenav_position: 'top', // Navigazione in alto stile professionale
  use_bc: false
};

// Inizializzazione
async function initTimeline() {
  try {
    const res = await fetch('assets/data/events_timeline.json');
    if(!res.ok) throw new Error("File timeline non trovato");
    
    const json = await res.json();
    rawTimelineData = json; // Salviamo i dati grezzi
    
    // Renderizza tutto inizialmente
    renderTimeline(rawTimelineData);
    
    // Collega ai filtri della Sidebar (se esiste il bottone)
    const filterBtn = document.getElementById('applyFilters');
    if (filterBtn) {
      filterBtn.addEventListener('click', applySidebarFiltersToTimeline);
    }
    
    // Collega al tasto Reset
    const resetBtn = document.getElementById('resetFilters');
    if (resetBtn) {
      resetBtn.addEventListener('click', () => renderTimeline(rawTimelineData));
    }

  } catch (e) {
    console.error("Errore Timeline:", e);
    const container = document.getElementById('timeline-embed');
    if (container) {
      container.innerHTML = `
        <div style="padding:40px; text-align:center; color:#b71c1c; background:#fff; border:1px solid #e0e0e0; border-radius:8px;">
          <i class="fa-solid fa-triangle-exclamation" style="font-size:24px; margin-bottom:10px;"></i><br>
          <strong>Impossibile caricare la timeline</strong><br>
          <span style="font-size:0.9em; color:#666;">${e.message}</span>
        </div>`;
    }
  }
}

function renderTimeline(dataJson) {
  const container = document.getElementById('timeline-embed');
  if (!container) return;

  // Verifica se ci sono eventi da mostrare
  if (!dataJson.events || dataJson.events.length === 0) {
    container.innerHTML = `
      <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; height:100%; color:#666; background:#f8f9fa; border-radius:8px;">
        <i class="fa-solid fa-filter-circle-xmark" style="font-size:30px; margin-bottom:15px; color:#ccc;"></i>
        <p>Nessun evento trovato con i filtri attuali.</p>
      </div>`;
    return;
  }

  // Pulisci container per evitare sovrapposizioni di TimelineJS
  container.innerHTML = '';
  
  // Istanzia TimelineJS
  try {
    tlInstance = new TL.Timeline('timeline-embed', dataJson, timelineOptions);
  } catch (err) {
    console.error("Errore interno TimelineJS:", err);
  }
}

// Funzione che legge i valori dalla Sidebar laterale
function applySidebarFiltersToTimeline() {
  if (!rawTimelineData) return;

  // 1. Recupera valori dalla Sidebar
  const startDate = document.getElementById('startDate').value;
  const endDate = document.getElementById('endDate').value;
  const typeFilter = document.getElementById('chartTypeFilter').value;
  const actorFilter = document.getElementById('actorFilter').value;
  
  // Recupera checkbox intensità
  const checkedIntensities = Array.from(document.querySelectorAll('.checkbox-group input:checked')).map(cb => cb.value);

  // 2. Filtra gli eventi
  const filteredEvents = rawTimelineData.events.filter(e => {
    const eventDate = e.start_date ? `${e.start_date.year}-${String(e.start_date.month).padStart(2,'0')}-${String(e.start_date.day).padStart(2,'0')}` : null;
    
    // Filtro Data
    if (startDate && eventDate && eventDate < startDate) return false;
    if (endDate && eventDate && eventDate > endDate) return false;
    
    // Filtro Tipo
    if (typeFilter && e.type !== typeFilter && e.group !== typeFilter) return false;
    
    // Filtro Attore (se presente nei dati timeline, altrimenti ignora o implementa logica custom)
    // Nota: events_timeline.json potrebbe non avere il campo 'actor', controlliamo se c'è
    if (actorFilter && e.actor && e.actor !== actorFilter) return false;

    // Filtro Intensità (Mockup logica, poiché intensity spesso non è nel timeline json di base ma nel geojson)
    // Se vogliamo filtrare per intensità qui, dobbiamo assicurarci che il campo esista in events_timeline.json
    // Per ora lo saltiamo per evitare bug se il dato manca.
    
    return true;
  });

  console.log(`📅 Timeline filtrata: ${filteredEvents.length} eventi.`);

  // 3. Aggiorna la timeline con i dati filtrati
  const filteredData = {
    ...rawTimelineData,
    events: filteredEvents
  };

  renderTimeline(filteredData);
}

// Avvia al caricamento
document.addEventListener('DOMContentLoaded', initTimeline);
