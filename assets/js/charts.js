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
window.initCharts = function (events) {
    if (!events || events.length === 0) return;

    ORIGINAL_DATA = events.map(e => {
        let searchParts = [];
        Object.values(e).forEach(val => { if (val) searchParts.push(String(val).toLowerCase()); });

        const titleLower = (e.title || '').toLowerCase();
        for (const [key, val] of Object.entries(SYNONYMS)) {
            if (titleLower.includes(val)) searchParts.push(key);
        }

        let m = moment(e.date, ["YYYY-MM-DD", "DD/MM/YYYY", "DD-MM-YYYY", "DD.MM.YYYY", "MM/DD/YYYY"]);
        if (!m.isValid()) m = moment(e.date); // Fallback to heuristic
        const ts = m.isValid() ? m.valueOf() : moment().valueOf();

        return {
            ...e,
            _searchStr: searchParts.join(' '),
            _actorCode: (e.actor_code || 'UNK').toString().toUpperCase(),
            _intensityNorm: parseFloat(e.intensity || 0.2),
            timestamp: ts
        };
    });

    ORIGINAL_DATA.sort((a, b) => b.timestamp - a.timestamp); // Descending order for the list

    updateDashboard(ORIGINAL_DATA);
    populateFilters(ORIGINAL_DATA);
    setupChartFilters();
};

// --- FILTERS ---
function setupChartFilters() {
    const btn = document.getElementById('applyFilters');
    if (!btn) return;
    const newBtn = btn.cloneNode(true);
    btn.parentNode.replaceChild(newBtn, btn);
    newBtn.addEventListener('click', executeFilter);

    const searchInput = document.getElementById('textSearch');
    if (searchInput) {
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
    if (rawSearch) {
        if (SYNONYMS[rawSearch]) searchTerms.push(SYNONYMS[rawSearch]);
        for (let key in SYNONYMS) { if (SYNONYMS[key] === rawSearch) searchTerms.push(key); }
    }

    const filtered = ORIGINAL_DATA.filter(e => {
        const norm = getNormalizedType(e.type);
        if (norm !== type) return false;
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
    if (window.updateMap) window.updateMap(filtered);
}

function updateDashboard(data) {
    renderTimelineChart(data);
    renderTypeChart(data);
    renderRadarChart(data);
    renderSectorTrendChart(); // NEW: Strategic Sector Chart

    // RENDERING OF THE 3 VIEWS
    renderKanban(data);
    renderVisualGallery(data);
    renderIntelFeed(data);

    if (document.getElementById('eventCount')) document.getElementById('eventCount').innerText = data.length;
}

// ===========================================
// STRATEGIC SECTOR TREND CHART (SciPol)
// ===========================================
let sectorChart = null;

function renderSectorTrendChart() {
    const ctx = document.getElementById('sectorTrendChart');
    if (!ctx) return; // Chart element not in DOM yet

    fetch(`assets/data/strategic_trends.json?v=${new Date().getTime()}`)
        .then(response => {
            if (!response.ok) {
                console.warn('⚠️ strategic_trends.json not found');
                return null;
            }
            return response.json();
        })
        .then(data => {
            if (!data || !data.dates || data.dates.length === 0) {
                console.warn('⚠️ No sector trend data available');
                return;
            }

            // Limit to last 30 days
            const maxDays = 30;
            const startIdx = Math.max(0, data.dates.length - maxDays);
            const dates = data.dates.slice(startIdx);

            // Sector colors (SciPol theme)
            const SECTOR_COLORS = {
                ENERGY_COERCION: { border: '#f59e0b', bg: 'rgba(245, 158, 11, 0.15)' },
                DEEP_STRIKES_RU: { border: '#ef4444', bg: 'rgba(239, 68, 68, 0.15)' },
                EASTERN_FRONT: { border: '#3b82f6', bg: 'rgba(59, 130, 246, 0.15)' },
                SOUTHERN_FRONT: { border: '#10b981', bg: 'rgba(16, 185, 129, 0.15)' }
            };

            const SECTOR_LABELS = {
                ENERGY_COERCION: '⚡ Energy Coercion',
                DEEP_STRIKES_RU: '🎯 Deep Strikes (RU)',
                EASTERN_FRONT: '🔵 Eastern Front',
                SOUTHERN_FRONT: '🟢 Southern Front'
            };

            const datasets = Object.keys(data.datasets).map(sector => ({
                label: SECTOR_LABELS[sector] || sector,
                data: data.datasets[sector].slice(startIdx),
                borderColor: SECTOR_COLORS[sector]?.border || '#94a3b8',
                backgroundColor: SECTOR_COLORS[sector]?.bg || 'rgba(148, 163, 184, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 2,
                pointHoverRadius: 6,
                borderWidth: 2
            }));

            if (sectorChart) sectorChart.destroy();

            sectorChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: dates.map(d => {
                        const parts = d.split('-');
                        return `${parts[2]}/${parts[1]}`; // DD/MM format
                    }),
                    datasets: datasets
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'index',
                        intersect: false
                    },
                    plugins: {
                        legend: {
                            position: 'top',
                            labels: {
                                color: '#94a3b8',
                                font: { size: 11 },
                                boxWidth: 12,
                                padding: 15
                            }
                        },
                        tooltip: {
                            backgroundColor: '#1e293b',
                            titleColor: '#f8fafc',
                            bodyColor: '#cbd5e1',
                            borderColor: '#334155',
                            borderWidth: 1,
                            padding: 12,
                            callbacks: {
                                title: function (ctx) {
                                    return `📅 ${ctx[0].label}`;
                                },
                                label: function (ctx) {
                                    return `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)} T.I.E.`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: { color: '#334155', drawBorder: false },
                            ticks: { color: '#64748b', font: { size: 10 } }
                        },
                        y: {
                            beginAtZero: true,
                            grid: { color: '#334155', drawBorder: false },
                            ticks: { color: '#64748b' },
                            title: {
                                display: true,
                                text: 'Aggregate T.I.E. Sum',
                                color: '#64748b',
                                font: { size: 11 }
                            }
                        }
                    }
                }
            });

            console.log('✅ Sector Trend Chart rendered');
        })
        .catch(err => console.error('Failed to load sector trends:', err));
}

// ===========================================
// NEW VIEW RENDERING FUNCTIONS
// ===========================================

function renderKanban(data) {
    const cols = {
        ground: document.querySelector('#col-ground .column-content'),
        air: document.querySelector('#col-air .column-content'),
        strat: document.querySelector('#col-strat .column-content')
    };

    // Clean
    Object.values(cols).forEach(c => { if (c) c.innerHTML = ''; });

    // Counters
    let counts = { ground: 0, air: 0, strat: 0 };

    data.slice(0, 100).forEach(e => { // Limit to 100 for performance
        // Simple classification logic (can be improved with real tags)
        let target = 'ground';
        const t = (e.type || '').toLowerCase();

        if (t.includes('air') || t.includes('drone') || t.includes('missile') || t.includes('strike')) target = 'air';
        else if (t.includes('civil') || t.includes('infrastr') || t.includes('politic')) target = 'strat';

        counts[target]++;

        // Determine border class based on intensity
        let borderClass = 'bd-low';
        if (e._intensityNorm >= 0.8) borderClass = 'bd-critical';
        else if (e._intensityNorm >= 0.6) borderClass = 'bd-high';
        else if (e._intensityNorm >= 0.4) borderClass = 'bd-medium';

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
        if (cols[target]) cols[target].appendChild(card);
    });

    // Update badges
    const badgeGround = document.querySelector('#col-ground .count-badge');
    if (badgeGround) badgeGround.innerText = counts.ground;

    const badgeAir = document.querySelector('#col-air .count-badge');
    if (badgeAir) badgeAir.innerText = counts.air;

    const badgeStrat = document.querySelector('#col-strat .count-badge');
    if (badgeStrat) badgeStrat.innerText = counts.strat;
}

function renderVisualGallery(data) {
    const container = document.getElementById('visual-grid-content');
    if (!container) return;
    container.innerHTML = '';

    // Filter only events with video or images (or show nice placeholders)
    data.slice(0, 50).forEach(e => {
        const card = document.createElement('div');
        card.className = 'visual-card';
        const encoded = encodeURIComponent(JSON.stringify(e));
        card.onclick = () => window.openModal(encoded);

        // Use real image if exists, otherwise icon
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
    if (!list) return;
    list.innerHTML = '';

    data.slice(0, 100).forEach(e => {
        const item = document.createElement('div');
        item.className = 'feed-item';
        item.innerHTML = `
            <div class="f-meta"><span>${moment(e.timestamp).format('HH:mm')}</span> <span style="color:var(--primary)">${e.actor_code}</span></div>
            <div class="f-title">${e.title}</div>
        `;

        item.onclick = () => {
            // Remove active from others
            document.querySelectorAll('.feed-item').forEach(el => el.classList.remove('active'));
            item.classList.add('active');
            showIntelDetail(e);
        };
        list.appendChild(item);
    });
}

function showIntelDetail(e) {
    const container = document.getElementById('intel-detail-content');
    const encoded = encodeURIComponent(JSON.stringify(e)); // For the "Open Full Modal" button

    let mediaHtml = '';
    if (e.video && e.video !== 'null') mediaHtml = `<div style="padding:15px; background:rgba(0,0,0,0.3); border-radius:8px; margin:15px 0; text-align:center;"><i class="fa-solid fa-play"></i> Video available in full dossier</div>`;

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
            <p>${e.description || "No detailed description available."}</p>
            ${mediaHtml}
            <button class="btn-primary" onclick="window.openModal('${encoded}')" style="margin-top:20px; width:100%;">
                <i class="fa-solid fa-expand"></i> Open Full Dossier & Media
            </button>
        </div>
    `;
}

// --- HELPER: CATEGORY NORMALIZATION (Military + Civil) ---
function getNormalizedType(rawType) {
    if (!rawType) return null;
    const t = rawType.toLowerCase();

    // --- PRIORITY 1: KINETIC MILITARY EVENTS ---

    // 1. NAVAL ENGAGEMENT
    if (t.match(/naval|sea|ship|boat|maritime|vessel/)) return "Naval Engagement";
    // 2. DRONE STRIKE
    if (t.match(/drone|uav|loitering|kamikaze|quadcopter|unmanned/)) return "Drone Strike";
    // 3. MISSILE STRIKE
    if (t.match(/missile|rocket|ballistic|cruise|himars|mlrs/)) return "Missile Strike";
    // 4. AIRSTRIKE
    if (t.match(/air|jet|plane|bombing|airstrike|su-/)) return "Airstrike";
    // 5. ARTILLERY SHELLING
    if (t.match(/artillery|shelling|mortar|howitzer|grad|cannon/)) return "Artillery Shelling";
    // 6. IED / EXPLOSION
    if (t.match(/ied|mine|landmine|vbied|explosion|trap/)) return "IED / Explosion";
    // 7. GROUND CLASH (Include 'firefight')
    if (t.match(/clash|firefight|skirmish|ambush|raid|attack|ground|shooting|sniper/)) return "Ground Clash";

    // --- PRIORITY 2: CIVIL AND POLITICAL CONTEXT ---

    // 8. POLITICAL / UNREST (Protests, Politics, Diplomacy)
    if (t.match(/politic|protest|riot|demonstration|diploma|unrest|arrest/)) return "Political / Unrest";

    // 9. CIVIL / ACCIDENT (Accidents, Generic Fires, Infrastructure)
    // Note: Placed last to avoid 'fire' capturing 'firefight'
    if (t.match(/civil|accident|crash|fire|infrastructure|logistics|humanitarian/)) return "Civil / Accident";

    return null; // Discard the rest
}


// Standard Chart Functions (Timeline, Type, Radar) - Unchanged
function renderTimelineChart(data) { const ctx = document.getElementById('timelineChart'); if (!ctx) return; const aggregated = {}; data.forEach(e => { if (!e.timestamp) return; const key = moment(e.timestamp).format('YYYY-MM'); aggregated[key] = (aggregated[key] || 0) + 1; }); const labels = Object.keys(aggregated).sort(); if (charts.timeline) charts.timeline.destroy(); charts.timeline = new Chart(ctx, { type: 'bar', data: { labels: labels, datasets: [{ label: 'Events', data: Object.values(aggregated), backgroundColor: THEME.primary, borderRadius: 4 }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false } }, y: { beginAtZero: true, grid: { color: THEME.grid } } } } }); }

function renderTypeChart(data) {
    const ctx = document.getElementById('typeDistributionChart');
    if (!ctx) return;

    const counts = {};

    // Define what to EXCLUDE from statistical charts
    const EXCLUDED_FROM_CHARTS = ['POLITICAL / UNREST', 'CIVIL / ACCIDENT'];

    data.forEach(e => {
        const cleanType = getNormalizedType(e.type);
        // Count only if valid AND if not in blacklist
        if (cleanType && !EXCLUDED_FROM_CHARTS.includes(cleanType)) {
            counts[cleanType] = (counts[cleanType] || 0) + 1;
        }
    });

    if (charts.type) charts.type.destroy();

    charts.type = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: Object.keys(counts),
            datasets: [{
                data: Object.values(counts),
                backgroundColor: THEME.palette,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { color: THEME.text, boxWidth: 12 }
                }
            },
            cutout: '70%'
        }
    });
}

function renderRadarChart(data) {
    const ctx = document.getElementById('intensityRadarChart');
    if (!ctx) return;

    if (data.length === 0) {
        if (charts.radar) charts.radar.destroy();
        return;
    }

    const stats = {};
    const EXCLUDED_FROM_CHARTS = ['POLITICAL / UNREST', 'CIVIL / ACCIDENT'];

    data.forEach(e => {
        const cleanType = getNormalizedType(e.type);
        // Filter out non-military categories for intensity calculation
        if (cleanType && !EXCLUDED_FROM_CHARTS.includes(cleanType)) {
            if (!stats[cleanType]) stats[cleanType] = { sum: 0, count: 0 };
            stats[cleanType].sum += e._intensityNorm;
            stats[cleanType].count++;
        }
    });

    const labels = Object.keys(stats);

    if (charts.radar) charts.radar.destroy();

    charts.radar = new Chart(ctx, {
        type: 'radar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Average Intensity',
                data: labels.map(k => (stats[k].sum / stats[k].count).toFixed(2)),
                backgroundColor: 'rgba(245, 158, 11, 0.2)',
                borderColor: THEME.primary,
                pointBackgroundColor: THEME.primary
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                r: {
                    grid: { color: THEME.grid },
                    pointLabels: { color: THEME.text, font: { size: 10 } },
                    ticks: { display: false, backdropColor: 'transparent' }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });
}

function populateFilters(data) {
    const select = document.getElementById('chartTypeFilter');
    if (!select) return;

    const currentVal = select.value;
    select.innerHTML = '<option value="">All categories</option>';

    const uniqueTypes = new Set();

    data.forEach(e => {
        const norm = getNormalizedType(e.type);
        // Here we accept EVERYTHING that is normalized (so also Civil and Political)
        // because we want to be able to select them in the filter
        if (norm) {
            uniqueTypes.add(norm);
        }
    });

    // Sort and create options
    const sortedTypes = [...uniqueTypes].sort();
    sortedTypes.forEach(t => {
        select.innerHTML += `<option value="${t}">${t}</option>`;
    });

    select.value = currentVal;
}