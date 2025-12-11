// ============================================
// CHARTS.JS - 3 VIEWS EDITION
// ============================================

let charts = { timeline: null, type: null, radar: null };
let ORIGINAL_DATA = []; 

const THEME = {
  primary: '#f59e0b', secondary: '#0f172a', text: '#94a3b8', grid: '#334155',
  palette: ['#f59e0b', '#ef4444', '#f97316', '#eab308', '#64748b', '#3b82f6']
};

Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.color = THEME.text;
Chart.defaults.scale.grid.color = THEME.grid;

const SYNONYMS = { 'kiev': 'kyiv', 'kiew': 'kyiv', 'kharkov': 'kharkiv', 'odessa': 'odesa', 'nikolaev': 'mykolaiv', 'artemivsk': 'bakhmut', 'dnepropetrovsk': 'dnipro', 'lvov': 'lviv' };

// --- INIT ---
window.initCharts = function(events) {
  if (!events || events.length === 0) return;
  
  ORIGINAL_DATA = events.map(e => {
    let searchParts = [];
    Object.values(e).forEach(val => { if (val) searchParts.push(String(val).toLowerCase()); });
    
    const titleLower = (e.title || '').toLowerCase();
    for (const [key, val] of Object.entries(SYNONYMS)) { 
        if (titleLower.includes(val)) searchParts.push(key); 
    }
    
    let m = moment(e.date);
    if(!m.isValid()) m = moment(e.date, ["DD/MM/YYYY", "DD-MM-YYYY", "DD.MM.YYYY"]);
    const ts = m.isValid() ? m.valueOf() : moment().valueOf();

    return {
      ...e,
      _searchStr: searchParts.join(' '),
      _actorCode: (e.actor_code || 'UNK').toString().toUpperCase(),
      _intensityNorm: parseFloat(e.intensity || 0.2),
      timestamp: ts
    };
  });

  ORIGINAL_DATA.sort((a,b) => b.timestamp - a.timestamp); // Ordine Decrescente per la lista
  
  updateDashboard(ORIGINAL_DATA);
  populateFilters(ORIGINAL_DATA);
  setupChartFilters();
};

// --- FILTRI ---
function setupChartFilters() {
  const btn = document.getElementById('applyFilters');
  if (!btn) return;
  const newBtn = btn.cloneNode(true);
  btn.parentNode.replaceChild(newBtn, btn);
  newBtn.addEventListener('click', executeFilter);

  const searchInput = document.getElementById('textSearch');
  if(searchInput) {
    const newSearch = searchInput.cloneNode(true);
    searchInput.parentNode.replaceChild(newSearch, searchInput);
    newSearch.addEventListener('input', executeFilter); 
    newSearch.addEventListener('keypress', (e) => { if (e.key === 'Enter') executeFilter(); });
  }
}

function executeFilter() {
  const startVal = document.getElementById('startDate').value;
  const endVal = document.getElementById('endDate').value;
  const type = document.getElementById('chartTypeFilter').value;
  const actorCode = document.getElementById('actorFilter').value; 
  const rawSearch = document.getElementById('textSearch').value.trim().toLowerCase();
  const checkedSeverities = Array.from(document.querySelectorAll('.toggle-container input:checked')).map(cb => cb.value);

  const startTs = startVal ? moment(startVal).startOf('day').valueOf() : null;
  const endTs = endVal ? moment(endVal).endOf('day').valueOf() : null;

  let searchTerms = [rawSearch];
  if(rawSearch) {
      if (SYNONYMS[rawSearch]) searchTerms.push(SYNONYMS[rawSearch]);
      for (let key in SYNONYMS) { if (SYNONYMS[key] === rawSearch) searchTerms.push(key); }
  }

  const filtered = ORIGINAL_DATA.filter(e => {
    if (startTs && e.timestamp < startTs) return false;
    if (endTs && e.timestamp > endTs) return false;
    if (type && e.type !== type) return false;
    if (actorCode && e._actorCode !== actorCode) return false;
    if (rawSearch && !searchTerms.some(term => e._searchStr.includes(term))) return false;
    
    let cat = 'low';
    if (e._intensityNorm >= 0.8) cat = 'critical';
    else if (e._intensityNorm >= 0.6) cat = 'high';
    else if (e._intensityNorm >= 0.4) cat = 'medium';
    if (checkedSeverities.length > 0 && !checkedSeverities.includes(cat)) return false;

    return true;
  });

  updateDashboard(filtered);
  if(window.updateMap) window.updateMap(filtered);
}

function updateDashboard(data) {
  renderTimelineChart(data);
  renderTypeChart(data);
  renderRadarChart(data);
  
  // RENDERIZZAZIONE DELLE 3 VISTE
  renderKanban(data);
  renderVisualGallery(data);
  renderIntelFeed(data);

  if(document.getElementById('eventCount')) document.getElementById('eventCount').innerText = data.length;
}

// ===========================================
// NUOVE FUNZIONI DI RENDERIZZAZIONE VISTE
// ===========================================

function renderKanban(data) {
    const cols = {
        ground: document.querySelector('#col-ground .column-content'),
        air: document.querySelector('#col-air .column-content'),
        strat: document.querySelector('#col-strat .column-content')
    };
    
    // Pulisci
    Object.values(cols).forEach(c => { if(c) c.innerHTML = ''; });
    
    // Contatori
    let counts = { ground: 0, air: 0, strat: 0 };

    data.slice(0, 100).forEach(e => { // Limitiamo a 100 per performance
        // Logica semplice di classificazione (può essere migliorata con tag reali)
        let target = 'ground';
        const t = (e.type || '').toLowerCase();
        
        if (t.includes('air') || t.includes('drone') || t.includes('missile') || t.includes('strike')) target = 'air';
        else if (t.includes('civil') || t.includes('infrastr') || t.includes('politic')) target = 'strat';
        
        counts[target]++;
        
        // Determina classe bordo in base a intensità
        let borderClass = 'bd-low';
        if(e._intensityNorm >= 0.8) borderClass = 'bd-critical';
        else if(e._intensityNorm >= 0.6) borderClass = 'bd-high';
        else if(e._intensityNorm >= 0.4) borderClass = 'bd-medium';

        const card = document.createElement('div');
        card.className = `kanban-card ${borderClass}`;
        const encoded = encodeURIComponent(JSON.stringify(e));
        card.onclick = () => window.openModal(encoded);
        
        card.innerHTML = `
            <span class="k-tag">${e.type}</span>
            <span class="k-title">${e.title}</span>
            <div class="k-footer">
                <span>${moment(e.timestamp).fromNow()}</span>
                <span>${e.actor_code}</span>
            </div>
        `;
        if(cols[target]) cols[target].appendChild(card);
    });
    
    // Aggiorna badge
    document.querySelector('#col-ground .count-badge').innerText = counts.ground;
    document.querySelector('#col-air .count-badge').innerText = counts.air;
    document.querySelector('#col-strat .count-badge').innerText = counts.strat;
}

function renderVisualGallery(data) {
    const container = document.getElementById('visual-grid-content');
    if(!container) return;
    container.innerHTML = '';
    
    // Filtra solo eventi con video o immagini (o mostra placeholder carini)
    data.slice(0, 50).forEach(e => {
        const card = document.createElement('div');
        card.className = 'visual-card';
        const encoded = encodeURIComponent(JSON.stringify(e));
        card.onclick = () => window.openModal(encoded);
        
        // Usa immagine reale se c'è, altrimenti icona
        let contentHtml = `<i class="fa-solid fa-layer-group" style="font-size:2rem; opacity:0.3; color:white;"></i>`;
        let bgStyle = '';
        
        if (e.before_img) {
             bgStyle = `background-image: url('${e.before_img}');`;
             contentHtml = '';
        }

        card.innerHTML = `
            <div class="visual-img" style="${bgStyle}">
                ${contentHtml}
            </div>
            <div class="visual-info">
                <div class="v-date">${moment(e.timestamp).format('DD MMM HH:mm')}</div>
                <div class="v-title">${e.title}</div>
            </div>
        `;
        container.appendChild(card);
    });
}

function renderIntelFeed(data) {
    const list = document.getElementById('intel-list-content');
    if(!list) return;
    list.innerHTML = '';
    
    data.slice(0, 100).forEach(e => {
        const item = document.createElement('div');
        item.className = 'feed-item';
        item.innerHTML = `
            <div class="f-meta"><span>${moment(e.timestamp).format('HH:mm')}</span> <span style="color:var(--primary)">${e.actor_code}</span></div>
            <div class="f-title">${e.title}</div>
        `;
        
        item.onclick = () => {
            // Rimuovi active dagli altri
            document.querySelectorAll('.feed-item').forEach(el => el.classList.remove('active'));
            item.classList.add('active');
            showIntelDetail(e);
        };
        list.appendChild(item);
    });
}

function showIntelDetail(e) {
    const container = document.getElementById('intel-detail-content');
    const encoded = encodeURIComponent(JSON.stringify(e)); // Per il tasto "Apri Modale Full"
    
    let mediaHtml = '';
    if(e.video && e.video !== 'null') mediaHtml = `<div style="padding:15px; background:rgba(0,0,0,0.3); border-radius:8px; margin:15px 0; text-align:center;"><i class="fa-solid fa-play"></i> Video disponibile nella modale completa</div>`;

    container.innerHTML = `
        <div class="d-header">
            <span class="d-tag">${e.type}</span>
            <h2 class="d-title">${e.title}</h2>
            <div class="d-meta">
                <span><i class="fa-regular fa-clock"></i> ${moment(e.timestamp).format('DD MMM YYYY, HH:mm')}</span>
                <span><i class="fa-solid fa-map-location-dot"></i> ${e.lat.toFixed(4)}, ${e.lon.toFixed(4)}</span>
            </div>
        </div>
        <div class="d-body">
            <p>${e.description || "Nessuna descrizione dettagliata disponibile."}</p>
            ${mediaHtml}
            <button class="btn-primary" onclick="window.openModal('${encoded}')" style="margin-top:20px; width:100%;">
                <i class="fa-solid fa-expand"></i> Apri Dossier Completo & Media
            </button>
        </div>
    `;
}

// Funzioni Standard Grafici (Timeline, Type, Radar) - Invariate
function renderTimelineChart(data) { const ctx = document.getElementById('timelineChart'); if (!ctx) return; const aggregated = {}; data.forEach(e => { if(!e.timestamp) return; const key = moment(e.timestamp).format('YYYY-MM'); aggregated[key] = (aggregated[key] || 0) + 1; }); const labels = Object.keys(aggregated).sort(); if (charts.timeline) charts.timeline.destroy(); charts.timeline = new Chart(ctx, { type: 'bar', data: { labels: labels, datasets: [{ label: 'Eventi', data: Object.values(aggregated), backgroundColor: THEME.primary, borderRadius: 4 }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false } }, y: { beginAtZero: true, grid: { color: THEME.grid } } } } }); }
function renderTypeChart(data) { const ctx = document.getElementById('typeDistributionChart'); if (!ctx) return; const counts = {}; data.forEach(e => { counts[e.type || 'N/A'] = (counts[e.type || 'N/A'] || 0) + 1; }); if (charts.type) charts.type.destroy(); charts.type = new Chart(ctx, { type: 'doughnut', data: { labels: Object.keys(counts), datasets: [{ data: Object.values(counts), backgroundColor: THEME.palette, borderWidth: 0 }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { color: THEME.text, boxWidth: 12 } } }, cutout: '70%' } }); }
function renderRadarChart(data) { const ctx = document.getElementById('intensityRadarChart'); if (!ctx) return; if(data.length === 0) { if(charts.radar) charts.radar.destroy(); return; } const stats = {}; data.forEach(e => { const t = e.type || 'N/A'; if (!stats[t]) stats[t] = { sum: 0, count: 0 }; stats[t].sum += e._intensityNorm; stats[t].count++; }); const labels = Object.keys(stats); if (charts.radar) charts.radar.destroy(); charts.radar = new Chart(ctx, { type: 'radar', data: { labels: labels, datasets: [{ label: 'Intensità', data: labels.map(k => (stats[k].sum/stats[k].count).toFixed(2)), backgroundColor: 'rgba(245, 158, 11, 0.2)', borderColor: THEME.primary, pointBackgroundColor: THEME.primary }] }, options: { responsive: true, maintainAspectRatio: false, scales: { r: { grid: { color: THEME.grid }, pointLabels: { color: THEME.text, font: { size: 10 } }, ticks: { display: false } } }, plugins: { legend: { display: false } } } }); }
function populateFilters(data) { const select = document.getElementById('chartTypeFilter'); if (!select) return; const currentVal = select.value; select.innerHTML = '<option value="">Tutte le categorie</option>'; const types = [...new Set(data.map(e => e.type))].filter(t=>t).sort(); types.forEach(t => { select.innerHTML += `<option value="${t}">${t}</option>`; }); select.value = currentVal; }
