// ============================================
// CHARTS.JS - 3 VIEWS EDITION
// ============================================

let charts = { timeline: null, type: null, radar: null };
let ORIGINAL_DATA = [];
let matrixFactionFilter = 'MIXED';

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

    // --- FACTION SEGMENTED CONTROL ---
    const factionBtns = document.querySelectorAll('#matrixFactionControl .mf-btn');
    factionBtns.forEach(btn => {
        btn.addEventListener('click', function () {
            factionBtns.forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            matrixFactionFilter = this.dataset.faction;
            renderImpactMatrix(ORIGINAL_DATA);
        });
    });
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
    // T.I.E. Situational Awareness
    renderImpactMatrix(data);
    renderHVTCarousel(data);

    // RENDERING OF THE 3 VIEWS
    renderKanban(data);
    renderVisualGallery(data);
    renderIntelFeed(data);

    if (document.getElementById('eventCount')) document.getElementById('eventCount').innerText = data.length;
}

// ===========================================
// IMPACT MATRIX (T.I.E. Bubble Chart)
// ===========================================
let impactMatrixChart = null;

function renderImpactMatrix(data) {
    const ctx = document.getElementById('impactMatrixChart');
    if (!ctx) return;

    // Performance: last 7 days or max 100 items
    const now = Date.now();
    const sevenDaysAgo = now - (7 * 24 * 60 * 60 * 1000);
    let filtered = data.filter(e => e.timestamp && e.timestamp >= sevenDaysAgo);
    if (filtered.length > 100) filtered = filtered.slice(0, 100);
    // Fallback: if no events in last 7 days, show most recent 100
    if (filtered.length === 0) filtered = data.slice(0, 100);

    // --- FACTION FILTER (v2.0 — title-based detection) ---
    // Events don't have actor_code/actors fields; derive side from title keywords
    function _deriveSide(e) {
        const txt = ((e.title || '') + ' ' + (e.description || '')).toUpperCase();
        const ruHits = (txt.match(/\bRUSSIA|RUSSIAN FORCES|RUSSIAN STRIKES?|RUSSIAN DRONES?|RU FORCES|MOSCOW|KREMLIN/g) || []).length;
        const uaHits = (txt.match(/\bUKRAIN|UKRAINIAN FORCES|UKRAINIAN STRIKES?|UKRAINIAN DRONES?|UA FORCES|KYIV FORCES|ZSU\b/g) || []).length;
        if (ruHits > uaHits) return 'RU';
        if (uaHits > ruHits) return 'UA';
        return 'UNK';
    }

    if (matrixFactionFilter !== 'MIXED') {
        const side = matrixFactionFilter; // 'RU' or 'UA'
        filtered = filtered.filter(e => _deriveSide(e) === side);
    }

    const bubbles = filtered
        .filter(e => e.vec_t && e.vec_e && e.vec_k)
        .map(e => {
            const tieTotal = e.tie_total || 0;
            const opacity = 0.3 + (Math.min(tieTotal, 100) / 100) * 0.65;

            // --- COLOR PERSISTENCE (v2.0) ---
            const eventSide = _deriveSide(e);
            let bubbleColor, borderCl;
            if (eventSide === 'RU') {
                bubbleColor = `rgba(239, 68, 68, ${opacity})`;    // Red — RU action
                borderCl = tieTotal >= 60 ? '#fca5a5' : 'rgba(239, 68, 68, 0.4)';
            } else if (eventSide === 'UA') {
                bubbleColor = `rgba(59, 130, 246, ${opacity})`;   // Blue — UA action
                borderCl = tieTotal >= 60 ? '#93c5fd' : 'rgba(59, 130, 246, 0.4)';
            } else {
                bubbleColor = `rgba(245, 158, 11, ${opacity})`;   // Amber — unknown
                borderCl = tieTotal >= 60 ? '#fbbf24' : 'rgba(245, 158, 11, 0.4)';
            }

            return {
                x: parseFloat(e.vec_t) || 1,
                y: parseFloat(e.vec_e) || 1,
                r: Math.max(4, (parseFloat(e.vec_k) || 1) * 3.5),
                _title: e.title || 'Unknown',
                _date: e.date || '--',
                _t: e.vec_t,
                _k: e.vec_k,
                _e: e.vec_e,
                _opacity: opacity,
                _tieTotal: tieTotal,
                _aggressorSide: eventSide,
                _bubbleColor: bubbleColor,
                _borderColor: borderCl
            };
        });

    if (impactMatrixChart) impactMatrixChart.destroy();

    impactMatrixChart = new Chart(ctx, {
        type: 'bubble',
        data: {
            datasets: [{
                label: 'Events',
                data: bubbles,
                backgroundColor: bubbles.map(b => b._bubbleColor),
                borderColor: bubbles.map(b => b._borderColor),
                borderWidth: bubbles.map(b => b._tieTotal >= 60 ? 2 : 1),
                hoverBackgroundColor: 'rgba(251, 191, 36, 0.9)',
                hoverBorderColor: '#fbbf24',
                hoverBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#0f172a',
                    titleColor: '#f8fafc',
                    bodyColor: '#cbd5e1',
                    borderColor: '#f59e0b',
                    borderWidth: 1,
                    padding: 14,
                    titleFont: { family: 'Inter, sans-serif', size: 13, weight: 'bold' },
                    bodyFont: { family: 'JetBrains Mono, monospace', size: 11 },
                    cornerRadius: 4,
                    displayColors: false,
                    callbacks: {
                        title: function (ctx) {
                            const d = ctx[0].raw;
                            return d._title.length > 50 ? d._title.substring(0, 50) + '…' : d._title;
                        },
                        label: function (ctx) {
                            const d = ctx.raw;
                            const sideLabel = d._aggressorSide === 'RU' ? '🔴 RU' : d._aggressorSide === 'UA' ? '🔵 UA' : '⚪ UNK';
                            return [
                                `📅 ${d._date}  |  ${sideLabel}`,
                                `T:${d._t} | K:${d._k} | E:${d._e}`,
                                `TIE Total: ${d._tieTotal}`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    min: 0,
                    max: 11,
                    title: {
                        display: true,
                        text: 'STRATEGIC VALUE (T)',
                        color: '#94a3b8',
                        font: { family: 'JetBrains Mono, monospace', size: 11, weight: 'bold' }
                    },
                    grid: { color: '#1e293b', drawBorder: false },
                    ticks: {
                        color: '#64748b',
                        font: { family: 'JetBrains Mono, monospace', size: 10 },
                        stepSize: 1
                    }
                },
                y: {
                    min: 0,
                    max: 11,
                    title: {
                        display: true,
                        text: 'DAMAGE ASSESSMENT (E)',
                        color: '#94a3b8',
                        font: { family: 'JetBrains Mono, monospace', size: 11, weight: 'bold' }
                    },
                    grid: { color: '#1e293b', drawBorder: false },
                    ticks: {
                        color: '#64748b',
                        font: { family: 'JetBrains Mono, monospace', size: 10 },
                        stepSize: 1
                    }
                }
            }
        }
    });

    const filterLabel = matrixFactionFilter === 'MIXED' ? 'ALL' : matrixFactionFilter;
    console.log(`✅ Impact Matrix rendered: ${bubbles.length} events [${filterLabel}]`);
}

// ===========================================
// HVT CAROUSEL (High Value Targets Feed)
// ===========================================
function renderHVTCarousel(data) {
    const container = document.getElementById('hvt-carousel');
    const countEl = document.getElementById('hvtCount');
    if (!container) return;

    // Doctrinal HVT Filter: Strategic Impact, not just loud explosions
    const hvtEvents = data.filter(e => {
        const tieTotal = parseFloat(e.tie_total) || 0;
        const vecT = parseFloat(e.vec_t) || 0;
        const classification = (e.classification || '').toUpperCase();
        return (tieTotal >= 60) || (vecT >= 8) || (['SHAPING_OFFENSIVE', 'MANOEUVRE'].includes(classification));
    }).slice(0, 30); // Cap at 30 cards for performance

    if (countEl) countEl.textContent = hvtEvents.length;

    container.innerHTML = '';

    if (hvtEvents.length === 0) {
        container.innerHTML = '<div style="color:#64748b; padding:20px; font-size:0.85rem;">No high-value targets in current dataset.</div>';
        return;
    }

    // Classification badge colors
    const BADGE_COLORS = {
        'SHAPING_OFFENSIVE': { bg: 'rgba(239, 68, 68, 0.2)', color: '#ef4444', label: 'SHAPING' },
        'MANOEUVRE': { bg: 'rgba(59, 130, 246, 0.2)', color: '#3b82f6', label: 'MANOEUVRE' },
        'ATTRITION': { bg: 'rgba(245, 158, 11, 0.2)', color: '#f59e0b', label: 'ATTRITION' },
        'DEEP_STRIKE': { bg: 'rgba(168, 85, 247, 0.2)', color: '#a855f7', label: 'DEEP STRIKE' },
        'DEFAULT': { bg: 'rgba(148, 163, 184, 0.15)', color: '#94a3b8', label: 'INTEL' }
    };

    hvtEvents.forEach(e => {
        const card = document.createElement('div');
        card.className = 'hvt-card';

        const classification = (e.classification || '').toUpperCase();
        const badge = BADGE_COLORS[classification] || BADGE_COLORS['DEFAULT'];
        const tieTotal = Math.round(parseFloat(e.tie_total) || 0);
        const title = (e.title || 'Unknown Event');
        const truncTitle = title.length > 60 ? title.substring(0, 57) + '…' : title;
        const dateStr = e.date || '--';

        // TIE color intensity
        let tieColor = '#64748b';
        if (tieTotal >= 80) tieColor = '#ef4444';
        else if (tieTotal >= 60) tieColor = '#f59e0b';
        else if (tieTotal >= 40) tieColor = '#eab308';

        card.innerHTML = `
            <div class="hvt-card-header">
                <span class="hvt-date">${dateStr}</span>
                <span class="hvt-badge" style="background:${badge.bg}; color:${badge.color};">${badge.label}</span>
            </div>
            <div class="hvt-card-body">
                <div class="hvt-card-title" title="${title.replace(/"/g, '&quot;')}">${truncTitle}</div>
            </div>
            <div class="hvt-card-footer">
                <span class="hvt-tie-label">T.I.E.</span>
                <span class="hvt-tie-value" style="color:${tieColor};">${tieTotal}</span>
                <span class="hvt-vectors">T:${e.vec_t || '-'} K:${e.vec_k || '-'} E:${e.vec_e || '-'}</span>
            </div>
        `;

        // Click to fly to event on map
        if (e.lat && e.lon) {
            card.style.cursor = 'pointer';
            card.addEventListener('click', () => {
                if (typeof window.flyToUnit === 'function') {
                    window.flyToUnit(e.lat, e.lon, e.title || 'HVT Event');
                } else if (window.map) {
                    window.map.flyTo([e.lat, e.lon], 10, { animate: true, duration: 1.5 });
                }
            });
        }

        container.appendChild(card);
    });

    console.log(`✅ HVT Carousel rendered: ${hvtEvents.length} targets`);
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


// Legacy chart functions removed (Timeline, Type Distribution, Radar, Sector Trend)
// Replaced by: renderImpactMatrix() and renderHVTCarousel() above

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