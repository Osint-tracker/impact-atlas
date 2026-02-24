/**
 * TACTICAL DASHBOARD CONTROLLER v2.0
 * Manages Momentum Gauge, Heatmap Logic, and War Room Equipment Ticker
 */

console.log("🚀 Dashboard Module Loading (v2.0 War Room)...");

// =============================================================================
// STATIC DATA: Equipment Economic Values (USD in millions)
// =============================================================================
const EQUIPMENT_VALUE_DB = {
    // --- TANKS ---
    'T-90M': 4.5, 'T-90A': 4.0, 'T-90': 3.5,
    'T-80BVM': 3.2, 'T-80U': 3.0, 'T-80BV': 2.5, 'T-80': 2.5,
    'T-72B3': 2.0, 'T-72B': 1.5, 'T-72A': 1.2, 'T-72': 1.5,
    'T-64BV': 1.8, 'T-64': 1.5, 'T-62M': 0.5, 'T-62': 0.4,
    'T-54': 0.3, 'T-55': 0.3,
    'Leopard 2A6': 13.0, 'Leopard 2A4': 6.0,
    'Challenger 2': 8.0, 'M1 Abrams': 10.0,

    // --- IFV / APC ---
    'BMP-3': 1.8, 'BMP-2': 0.8, 'BMP-1': 0.5,
    'BTR-82A': 1.0, 'BTR-80': 0.6, 'BTR-70': 0.4,
    'Bradley': 3.5, 'CV90': 4.0, 'Marder': 1.5,

    // --- AIR DEFENSE ---
    'S-400': 300.0, 'S-300': 115.0, 'S-300V': 120.0,
    'Buk-M2': 25.0, 'Buk-M1': 18.0, 'Buk': 18.0,
    'Tor-M2': 30.0, 'Tor-M1': 25.0, 'Pantsir-S1': 15.0,
    'Patriot': 250.0, 'NASAMS': 40.0, 'IRIS-T': 170.0,

    // --- AIRCRAFT ---
    'Su-35S': 85.0, 'Su-34': 50.0, 'Su-30SM': 50.0,
    'Su-25': 11.0, 'Su-24': 25.0,
    'Ka-52': 16.0, 'Mi-28': 18.0, 'Mi-24': 12.0, 'Mi-8': 7.0,
    'MiG-29': 22.0, 'MiG-31': 33.0,

    // --- ARTILLERY ---
    'HIMARS': 5.5, '2S19 Msta': 3.0, '2S1 Gvozdika': 1.0,
    'Caesar': 7.0, 'PzH 2000': 9.0, 'MLRS': 4.5,

    // --- DEFAULT CATEGORY FALLBACKS ---
    'DEFAULT_TANK': 2.0, 'DEFAULT_IFV': 1.5, 'DEFAULT_APC': 0.8,
    'DEFAULT_SAM': 20.0, 'DEFAULT_AIRCRAFT': 30.0, 'DEFAULT_HELICOPTER': 12.0,
    'DEFAULT_ARTILLERY': 2.5, 'DEFAULT_TRUCK': 0.1, 'DEFAULT_OTHER': 0.5, 'DEFAULT_UAV': 0.15
};

// =============================================================================
// SVG SILHOUETTE ICONS
// =============================================================================
const EQUIPMENT_SVG = {
    tank: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M20 12H4l-2 4h2l1-2h14l1 2h2l-2-4zM6.5 9C5.67 9 5 9.67 5 10.5S5.67 12 6.5 12 8 11.33 8 10.5 7.33 9 6.5 9zm11 0c-.83 0-1.5.67-1.5 1.5s.67 1.5 1.5 1.5 1.5-.67 1.5-1.5S18.33 9 17.5 9zM12 7l3 2H9l3-2z"/></svg>`,
    ifv: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M20 11H4L2 15h2l1-2h14l1 2h2l-2-4zM6 10h12V8H6v2zm3-4h6V4H9v2z"/></svg>`,
    heli: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M3 4h18v2H3V4zm6 4h6v1h6v2H3V9h6V8zm3 5c-1.5 0-3 1.5-3 3v3h2v-2h2v2h2v-3c0-1.5-1.5-3-3-3z"/></svg>`,
    jet: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M21 16v-2l-8-5V3.5c0-.83-.67-1.5-1.5-1.5S10 2.67 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5l8 2.5z"/></svg>`,
    artillery: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M4 18h16v2H4v-2zm1-3l2.5-7L12 4l4.5 4 2.5 7H5z"/></svg>`,
    radar: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm0-14c-3.31 0-6 2.69-6 6h2c0-2.21 1.79-4 4-4V6z"/></svg>`,
    truck: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M20 8h-3V4H3c-1.1 0-2 .9-2 2v11h2c0 1.66 1.34 3 3 3s3-1.34 3-3h6c0 1.66 1.34 3 3 3s3-1.34 3-3h2v-5l-3-4zM6 18.5c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm13.5-9l1.96 2.5H17V9.5h2.5zm-1.5 9c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5z"/></svg>`,
    ship: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M20 21c-1.39 0-2.78-.47-4-1.32-2.44 1.71-5.56 1.71-8 0C6.78 20.53 5.39 21 4 21H2v2h2c1.38 0 2.74-.35 4-.99 2.52 1.29 5.48 1.29 8 0 1.26.65 2.62.99 4 .99h2v-2h-2zM3.95 19H4c1.6 0 3.02-.88 4-2 .98 1.12 2.4 2 4 2s3.02-.88 4-2c.98 1.12 2.4 2 4 2h.05l1.89-6.68c.08-.26.06-.54-.06-.78s-.34-.42-.6-.5L20 10.62V6c0-1.1-.9-2-2-2h-3V1H9v3H6c-1.1 0-2 .9-2 2v4.62l-1.29.42c-.26.08-.48.26-.6.5s-.15.52-.06.78L3.95 19z"/></svg>`,
    drone: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M22 3l-1.67 1.67L18.67 3 17 4.67 15.33 3l-1.66 1.67L12 3l-1.67 1.67L8.67 3 7 4.67 5.33 3 3.67 4.67 2 3v19h20V3zM12 18H6v-2h6v2zm4-4H6v-2h10v2zm0-4H6V8h10v2z"/></svg>`,
    fallback: `<svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/></svg>`
};

// =============================================================================
// MOMENTUM GAUGE (Unchanged from v1)
// =============================================================================
class MomentumGauge {
    constructor(canvasId, valueId, statusId) {
        this.valueEl = document.getElementById(valueId);
        this.statusEl = document.getElementById(statusId);
        this.barFill = document.getElementById('tempoBarFill');
        this.marker = document.getElementById('tempoMarker');
    }

    calculateMetrics(events) {
        const cutoff = moment().subtract(48, 'hours');
        const recentEvents = events.filter(e => moment(e.date, "DD/MM/YYYY").isAfter(cutoff));

        let offensiveCount = 0;
        let staticCount = 0;

        recentEvents.forEach(e => {
            const cls = (e.classification || "UNKNOWN").toUpperCase();
            if (cls === 'MANOEUVRE' || cls === 'SHAPING_OFFENSIVE') offensiveCount++;
            else if (cls === 'ATTRITION') staticCount++;
        });

        const total = offensiveCount + staticCount;
        const ratio = total === 0 ? 0.5 : (offensiveCount / total);
        return { ratio, total, offensiveCount };
    }

    render(events) {
        const metrics = this.calculateMetrics(events);
        const percent = Math.round(metrics.ratio * 100);

        if (this.valueEl) this.valueEl.innerText = `${percent}%`;

        let statusText = "STALEMATE";
        let color = "#64748b";
        if (percent > 65) { statusText = "HIGH TEMPO OFFENSIVE"; color = "#ef4444"; }
        else if (percent > 40) { statusText = "ACTIVE CONTEST"; color = "#f59e0b"; }
        else { statusText = "STATIC / ATTRITION"; color = "#3b82f6"; }

        if (this.statusEl) { this.statusEl.innerText = statusText; this.statusEl.style.color = color; }
        if (this.barFill) this.barFill.style.width = `${percent}%`;
        if (this.marker) this.marker.style.left = `${percent}%`;
    }
}

// =============================================================================
// EQUIPMENT TICKER v2.0 — WAR ROOM EDITION
// =============================================================================
class EquipmentTicker {
    constructor(containerId) {
        this.containerId = containerId;
        this.container = document.getElementById(containerId);
        this.content = document.getElementById('ticker-content');
        this.allData = [];
        this.data = [];
        this.netSummary = null;
        this.viewMode = 'gross'; // 'gross' or 'net'
        if (!this.content) {
            console.warn('⚠️ EquipmentTicker: #ticker-content not found at construction, will retry at render');
        }
        console.log('📦 EquipmentTicker v3.0 (War Ledger) constructed', { container: !!this.container, content: !!this.content });
    }

    // --- DATA LOADING ---
    async loadData(internalEvents) {
        console.log('📡 EquipmentTicker.loadData called');

        // Stream A: Internal events
        const internalLosses = (internalEvents || [])
            .filter(e => e.target_type && e.target_type !== 'NULL' && e.target_type !== 'UNKNOWN')
            .map(e => {
                // Derive side for internal events based on title
                const txt = ((e.title || '') + ' ' + (e.description || '')).toUpperCase();
                const ruHits = (txt.match(/\bRUSSIA|RUSSIAN FORCES|RUSSIAN STRIKES?|RUSSIAN DRONES?|RU FORCES|MOSCOW|KREMLIN/g) || []).length;
                const uaHits = (txt.match(/\bUKRAIN|UKRAINIAN FORCES|UKRAINIAN STRIKES?|UKRAINIAN DRONES?|UA FORCES|KYIV FORCES|ZSU\b/g) || []).length;
                let ctry = 'UNKNOWN';
                if (ruHits > uaHits) ctry = 'RU';
                else if (uaHits > ruHits) ctry = 'UA';

                return {
                    source: 'INTERNAL', date: e.date, model: e.target_type,
                    country: ctry, status: 'REPORTED', category: 'other',
                    lat: e.lat || null, lon: e.lon || null,
                    tie_total: e.tie_total || 0, vec_t: e.vec_t || 0,
                    visual_evidence: e.visual_evidence || e.before_img || null
                };
            });

        // Stream B: External JSON (Oryx + LostArmour)
        let externalLosses = [];
        try {
            const res = await fetch('assets/data/external_losses.json');
            if (res.ok) {
                const json = await res.json();
                externalLosses = json.map(item => {
                    const modelLower = (item.model || '').toLowerCase();
                    const typeLower = (item.type || '').toLowerCase();
                    let category = 'other';
                    if (typeLower.includes('tank') || modelLower.match(/^t-\d/)) category = 'tank';
                    else if (typeLower.includes('artiller') || typeLower.includes('rocket') || typeLower.includes('howitzer')) category = 'artillery';
                    else if (typeLower.includes('infantry fighting') || typeLower.includes('ifv') || typeLower.includes('apc')) category = 'ifv';
                    else if (typeLower.includes('aircraft') || typeLower.includes('helicopter') ||
                        modelLower.match(/^(su-|mi-|ka-|mig)/i) ||
                        typeLower.includes('uav') || typeLower.includes('drone')) category = 'air';

                    return {
                        source: 'EXTERNAL', date: item.date, model: item.model,
                        country: item.country || 'RUS', status: item.status || 'CONFIRMED',
                        proof_url: item.proof_url || '', source_tag: item.source_tag || 'Oryx',
                        type: item.type || 'Vehicle', category: category,
                        lat: item.lat || null, lon: item.lon || null,
                        tie_total: 0
                    };
                });
                console.log(`   External: ${externalLosses.length} records`);
            }
        } catch (err) {
            console.warn("Could not load external losses:", err);
        }

        this.allData = [...internalLosses, ...externalLosses]
            .sort((a, b) => new Date(b.date) - new Date(a.date));
        this.data = this.allData;

        // Load Net Loss Summary
        try {
            const netRes = await fetch('assets/data/net_losses_summary.json');
            if (netRes.ok) {
                this.netSummary = await netRes.json();
                console.log('[WarLedger] Net Loss summary loaded');
            }
        } catch (err) {
            console.warn('Could not load net loss summary:', err);
        }

        this._renderAttritionMatrix();
        if (this.viewMode === 'net') {
            this._renderNetBalance();
        } else {
            this.render();
        }
    }

    filterByCategory(category) {
        this.data = category === 'all' ? this.allData : this.allData.filter(d => d.category === category);
        this.render();
    }

    // --- ATTRITION MATRIX (MIRROR CHART) ---
    _renderAttritionMatrix() {
        const container = document.getElementById('attrition-mirror-chart');
        if (!container) return;

        // Calculate losses by side and category
        const stats = {
            tank: { ru: 0, ua: 0 },
            ifv: { ru: 0, ua: 0 },
            artillery: { ru: 0, ua: 0 },
            air: { ru: 0, ua: 0 }
        };

        this.allData.forEach(d => {
            const side = (d.country === 'RUS' || d.country === 'RU') ? 'ru' : (d.country === 'UA' || d.country === 'UKR' ? 'ua' : null);
            if (!side || !stats[d.category]) return;
            stats[d.category][side]++;
        });

        // Generate Mirror Bars
        const generateBar = (label, ru, ua) => {
            const total = ru + ua;
            const ratioVal = ua > 0 ? (ru / ua) : ru;
            const ratioStr = ratioVal.toFixed(1) + ':1';
            const isExtreme = ratioVal > 3;

            // Adjust proportions for visualization (log scale or clamp to prevent UI break)
            const maxPx = 100; // max width of one side
            const totalMax = Math.max(ru, ua) || 1;
            const ruPct = (ru / totalMax) * maxPx;
            const uaPct = (ua / totalMax) * maxPx;

            return `
            <div style="display:flex; align-items:center; margin-bottom:6px; font-family:'JetBrains Mono', monospace; font-size:0.7rem;">
                <div style="width:40px; text-align:right; color:#64748b; padding-right:8px; text-transform:uppercase; font-weight:bold;">${label}</div>
                <!-- UA Side (Left) -->
                <div style="flex:1; display:flex; justify-content:flex-end; padding-right:2px;">
                    <span style="margin-right:4px; color:#64748b;">${ua}</span>
                    <div style="height:12px; background:#3b82f6; width:${uaPct}px; border-radius:2px 0 0 2px;"></div>
                </div>
                <div style="width:1px; height:16px; background:#94a3b8; margin:0 4px;"></div>
                <!-- RU Side (Right) -->
                <div style="flex:1; display:flex; justify-content:flex-start; padding-left:2px;">
                    <div style="height:12px; background:#ef4444; width:${ruPct}px; border-radius:0 2px 2px 0;"></div>
                    <span style="margin-left:4px; color:#64748b;">${ru}</span>
                </div>
                <!-- Ratio -->
                <div style="width:50px; text-align:right; font-weight:bold; color:${isExtreme ? '#f59e0b' : '#94a3b8'};">
                    ${ratioStr}
                </div>
            </div>`;
        };

        container.innerHTML = `
            <div style="background:rgba(15,23,42,0.8); border:1px solid #334155; border-radius:6px; padding:10px; margin-bottom:12px;">
                <div style="display:flex; justify-content:space-between; margin-bottom:8px; font-size:0.7rem; font-weight:bold; color:#f8fafc; font-family:'JetBrains Mono', monospace;">
                    <span><span style="color:#3b82f6;">UA</span> LOSSES</span>
                    <span><span style="color:#ef4444;">RU</span> LOSSES</span>
                    <span style="color:#94a3b8;">RATIO</span>
                </div>
                ${generateBar('MBT', stats.tank.ru, stats.tank.ua)}
                ${generateBar('IFV', stats.ifv.ru, stats.ifv.ua)}
                ${generateBar('SPG', stats.artillery.ru, stats.artillery.ua)}
                ${generateBar('AIR', stats.air.ru, stats.air.ua)}
            </div>
        `;
    }

    // --- ECONOMIC VALUE LOOKUP ---
    _getEconomicValue(model, type) {
        if (!model) return EQUIPMENT_VALUE_DB['DEFAULT_OTHER'];

        // Direct match
        for (const [key, val] of Object.entries(EQUIPMENT_VALUE_DB)) {
            if (key === 'DEFAULT_TANK' || key.startsWith('DEFAULT_')) continue;
            if (model.includes(key) || model.toLowerCase().includes(key.toLowerCase())) return val;
        }

        // Category fallback
        const typeLower = (type || '').toLowerCase();
        const modelLower = model.toLowerCase();
        if (typeLower.includes('tank') || modelLower.match(/^t-\d/)) return EQUIPMENT_VALUE_DB['DEFAULT_TANK'];
        if (typeLower.includes('infantry fighting') || typeLower.includes('ifv') || modelLower.match(/^bmp/)) return EQUIPMENT_VALUE_DB['DEFAULT_IFV'];
        if (typeLower.includes('apc') || modelLower.match(/^btr/)) return EQUIPMENT_VALUE_DB['DEFAULT_APC'];
        if (typeLower.includes('aircraft') || modelLower.match(/^(su-|mig)/i)) return EQUIPMENT_VALUE_DB['DEFAULT_AIRCRAFT'];
        if (typeLower.includes('helicopter') || modelLower.match(/^(mi-|ka-)/i)) return EQUIPMENT_VALUE_DB['DEFAULT_HELICOPTER'];
        if (typeLower.match(/sam|air.def|radar/) || modelLower.match(/^(s-|buk|tor|pantsir)/i)) return EQUIPMENT_VALUE_DB['DEFAULT_SAM'];
        if (typeLower.includes('artiller') || typeLower.includes('rocket')) return EQUIPMENT_VALUE_DB['DEFAULT_ARTILLERY'];
        if (typeLower.includes('uav') || typeLower.includes('drone')) return EQUIPMENT_VALUE_DB['DEFAULT_UAV'];
        if (typeLower.includes('truck') || typeLower.includes('engineering')) return EQUIPMENT_VALUE_DB['DEFAULT_TRUCK'];

        return EQUIPMENT_VALUE_DB['DEFAULT_OTHER'];
    }

    // --- SVG ICON RESOLVER ---
    _getSvgIcon(model, type) {
        const m = (model || '').toLowerCase();
        const t = (type || '').toLowerCase();

        if (t.includes('tank') || m.match(/^t-\d/)) return EQUIPMENT_SVG.tank;
        if (t.includes('infantry fighting') || t.includes('ifv') || m.match(/^bmp/)) return EQUIPMENT_SVG.ifv;
        if (t.includes('helicopter') || m.match(/^(ka-|mi-)/)) return EQUIPMENT_SVG.heli;
        if (t.includes('aircraft') || m.match(/^(su-|mig)/)) return EQUIPMENT_SVG.jet;
        if (t.match(/artiller|rocket|mlrs|howitzer/) || m.match(/himars|msta|gvozdika|grad/)) return EQUIPMENT_SVG.artillery;
        if (t.match(/radar|jammer|air.def|sam/) || m.match(/^(s-\d|buk|pantsir|tor)/)) return EQUIPMENT_SVG.radar;
        if (t.includes('uav') || t.includes('drone') || t.includes('reconnaissance')) return EQUIPMENT_SVG.drone;
        if (t.match(/naval|ship|boat/)) return EQUIPMENT_SVG.ship;
        if (t.match(/truck|engineering|command|communication/)) return EQUIPMENT_SVG.truck;

        return EQUIPMENT_SVG.fallback;
    }

    // --- CLICK TO LOCATE ---
    _flyToLoss(lat, lon, model) {
        if (!lat || !lon || !window.map) return;

        // Fly to location
        window.map.flyTo([lat, lon], 13, { animate: true, duration: 1.5 });

        // Pulse marker
        const pulseMarker = L.circleMarker([lat, lon], {
            radius: 12,
            color: '#f59e0b',
            fillColor: '#f59e0b',
            fillOpacity: 0.3,
            weight: 2,
            className: 'pulse-marker'
        }).addTo(window.map);

        // Popup
        pulseMarker.bindPopup(
            `<div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#f8fafc;background:#0f172a;padding:6px 10px;border-radius:4px;">
                <b style="color:#f59e0b;">⚡ ${model}</b><br>Equipment Loss Location
            </div>`,
            { className: 'pulse-popup', closeButton: false, autoClose: true }
        ).openPopup();

        // Auto-remove after 5s
        setTimeout(() => {
            window.map.removeLayer(pulseMarker);
        }, 5000);
    }

    // --- NET BALANCE VIEW (War Ledger v2) ---
    _renderNetBalance() {
        if (!this.content) {
            this.content = document.getElementById('ticker-content');
        }
        if (!this.content || !this.netSummary) {
            console.warn('[WarLedger] Cannot render net balance — missing content or summary');
            if (this.content) this.content.innerHTML = '<div class="loss-card holo placeholder">Net Loss data unavailable.</div>';
            return;
        }

        // SVG silhouettes for each category
        const CAT_SVG = {
            'Tanks': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><path d="M8 24h48l4-6H56l-2-4h-8V8H18v6h-8l-2 4H4l4 6zm10-14h28v4H18v-4zm-4 6h36l1 2H13l1-2z"/><circle cx="14" cy="24" r="3"/><circle cx="26" cy="24" r="3"/><circle cx="38" cy="24" r="3"/><circle cx="50" cy="24" r="3"/></svg>`,
            'AFVs': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><rect x="8" y="10" width="48" height="14" rx="3"/><rect x="20" y="4" width="20" height="8" rx="2"/><circle cx="14" cy="26" r="3"/><circle cx="26" cy="26" r="3"/><circle cx="38" cy="26" r="3"/><circle cx="50" cy="26" r="3"/></svg>`,
            'IFVs': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><rect x="6" y="10" width="52" height="14" rx="3"/><rect x="24" y="4" width="16" height="8" rx="2"/><line x1="32" y1="4" x2="40" y2="0" stroke="currentColor" stroke-width="2"/><circle cx="14" cy="26" r="3"/><circle cx="26" cy="26" r="3"/><circle cx="38" cy="26" r="3"/><circle cx="50" cy="26" r="3"/></svg>`,
            'APCs': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><rect x="6" y="8" width="52" height="16" rx="4"/><circle cx="14" cy="26" r="3"/><circle cx="50" cy="26" r="3"/><rect x="30" y="4" width="8" height="6" rx="1"/></svg>`,
            'MRAPs': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><path d="M10 12h44c2 0 4 2 4 4v8H6v-8c0-2 2-4 4-4z"/><rect x="14" y="6" width="36" height="8" rx="3"/><circle cx="16" cy="26" r="4"/><circle cx="48" cy="26" r="4"/></svg>`,
            'SP Artillery': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><rect x="8" y="14" width="44" height="12" rx="2"/><circle cx="16" cy="28" r="3"/><circle cx="28" cy="28" r="3"/><circle cx="40" cy="28" r="3"/><line x1="32" y1="16" x2="60" y2="4" stroke="currentColor" stroke-width="3" stroke-linecap="round"/></svg>`,
            'Towed Artillery': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><line x1="20" y1="20" x2="58" y2="6" stroke="currentColor" stroke-width="3" stroke-linecap="round"/><circle cx="20" cy="24" r="5" fill="none" stroke="currentColor" stroke-width="2"/><rect x="10" y="18" width="20" height="8" rx="2"/><line x1="6" y1="28" x2="16" y2="20" stroke="currentColor" stroke-width="2"/></svg>`,
            'MLRS': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><rect x="6" y="16" width="36" height="10" rx="2"/><circle cx="14" cy="28" r="3"/><circle cx="34" cy="28" r="3"/><rect x="24" y="6" width="28" height="12" rx="1" transform="rotate(-15 38 12)"/><line x1="30" y1="10" x2="30" y2="16" stroke="currentColor" stroke-width="1"/><line x1="36" y1="8" x2="36" y2="16" stroke="currentColor" stroke-width="1"/><line x1="42" y1="6" x2="42" y2="16" stroke="currentColor" stroke-width="1"/></svg>`,
            'SAM Systems': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><rect x="10" y="18" width="32" height="8" rx="2"/><circle cx="16" cy="28" r="3"/><circle cx="36" cy="28" r="3"/><rect x="20" y="10" width="24" height="10" rx="2"/><path d="M48 12l8-8v6l-8 4z"/><circle cx="56" cy="6" r="3" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>`,
            'Aircraft': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><path d="M58 16l-16-4V6l-3-2-3 2v6L20 16l-8-3-4 1 6 4-8 2v3l10-2 12 4h6l-4-4 16-2 6 3 4-1-6-5z"/></svg>`,
            'Helicopters': `<svg viewBox="0 0 64 32" fill="currentColor" width="36" height="18"><ellipse cx="24" cy="18" rx="14" ry="8"/><path d="M38 16h16l4 4H38z"/><line x1="4" y1="10" x2="44" y2="10" stroke="currentColor" stroke-width="2"/><line x1="24" y1="10" x2="24" y2="14" stroke="currentColor" stroke-width="2"/><line x1="18" y1="26" x2="14" y2="30" stroke="currentColor" stroke-width="2"/><line x1="30" y1="26" x2="34" y2="30" stroke="currentColor" stroke-width="2"/></svg>`,
        };

        const cats = this.netSummary.categories;
        const globalRU = this.netSummary.global?.RU || {};
        const globalUA = this.netSummary.global?.UA || {};

        const displayCats = ['Tanks', 'AFVs', 'IFVs', 'APCs', 'MRAPs', 'SP Artillery', 'Towed Artillery', 'MLRS', 'SAM Systems'];
        const maxLoss = Math.max(
            ...displayCats.map(c => Math.max(cats[c]?.RU?.total_lost || 0, cats[c]?.UA?.total_lost || 0))
        ) || 1;

        // LEGEND
        let html = `
        <div style="display:flex; gap:10px; padding:6px 10px; margin-bottom:6px; font-family:'JetBrains Mono',monospace; font-size:0.6rem; color:#64748b; align-items:center; flex-wrap:wrap;">
            <span><span style="display:inline-block;width:10px;height:10px;background:#ef4444;border-radius:2px;margin-right:3px;vertical-align:middle;"></span>RU Losses</span>
            <span><span style="display:inline-block;width:10px;height:10px;background:#3b82f6;border-radius:2px;margin-right:3px;vertical-align:middle;"></span>UA Losses</span>
            <span><span style="display:inline-block;width:10px;height:10px;background:#f59e0b;border-radius:2px;margin-right:3px;vertical-align:middle;"></span>Captured from Enemy</span>
        </div>`;

        displayCats.forEach(cat => {
            if (!cats[cat]) return;
            const ru = cats[cat].RU || {};
            const ua = cats[cat].UA || {};

            const ruTotal = ru.total_lost || 0;
            const ruCap = ru.captured_from_enemy || 0;
            const ruNet = ru.net_loss || 0;
            const uaTotal = ua.total_lost || 0;
            const uaCap = ua.captured_from_enemy || 0;
            const uaNet = ua.net_loss || 0;

            const ruPct = Math.round((ruTotal / maxLoss) * 100);
            const uaPct = Math.round((uaTotal / maxLoss) * 100);
            const ruCapPct = ruTotal > 0 ? (ruCap / ruTotal * 100) : 0;
            const uaCapPct = uaTotal > 0 ? (uaCap / uaTotal * 100) : 0;

            const svgIcon = CAT_SVG[cat] || CAT_SVG['AFVs'];
            const ratio = uaTotal > 0 ? (ruTotal / uaTotal).toFixed(1) : '∞';

            html += `
            <div style="display:grid; grid-template-columns:44px 1fr; gap:6px; background:rgba(15,23,42,0.7); border:1px solid #1e293b; border-radius:6px; padding:8px; margin-bottom:5px; transition:all 0.3s ease; align-items:center;"
                 onmouseenter="this.style.borderColor='#334155'" onmouseleave="this.style.borderColor='#1e293b'">
                
                <!-- Icon Column -->
                <div style="display:flex; flex-direction:column; align-items:center; gap:2px;">
                    <div style="color:#94a3b8; opacity:0.7;">${svgIcon}</div>
                    <span style="font-family:'JetBrains Mono',monospace; font-size:0.55rem; color:#64748b; font-weight:700; letter-spacing:0.5px; text-align:center; line-height:1;">${cat.replace(' Artillery', '').replace('Systems', '')}</span>
                </div>

                <!-- Bars Column -->
                <div style="display:flex; flex-direction:column; gap:3px;">
                    <!-- RU -->
                    <div style="display:flex; align-items:center; gap:4px;">
                        <div style="flex:1; height:16px; background:#1e293b; border-radius:3px; position:relative; overflow:hidden;">
                            <div style="position:absolute;left:0;top:0;height:100%;width:${ruPct}%;background:linear-gradient(90deg,#dc2626,#991b1b);border-radius:3px;transition:width 0.8s ease;"></div>
                            ${ruCap > 0 ? `<div style="position:absolute;right:${100 - ruPct}%;top:0;height:100%;width:${Math.max(ruCapPct * ruPct / 100, 2)}%;background:#f59e0b;opacity:0.7;border-radius:0 3px 3px 0;" title="+${ruCap} captured from UA"></div>` : ''}
                            <span style="position:absolute;left:6px;top:50%;transform:translateY(-50%);font-family:'JetBrains Mono',monospace;font-size:0.6rem;color:#fef2f2;font-weight:700;text-shadow:0 1px 2px rgba(0,0,0,0.8);">${ruTotal.toLocaleString()}</span>
                        </div>
                        <span style="min-width:52px;text-align:right;font-family:'JetBrains Mono',monospace;font-size:0.65rem;font-weight:700;color:#ef4444;">NET ${ruNet.toLocaleString()}</span>
                    </div>
                    <!-- UA -->
                    <div style="display:flex; align-items:center; gap:4px;">
                        <div style="flex:1; height:16px; background:#1e293b; border-radius:3px; position:relative; overflow:hidden;">
                            <div style="position:absolute;left:0;top:0;height:100%;width:${uaPct}%;background:linear-gradient(90deg,#2563eb,#1e40af);border-radius:3px;transition:width 0.8s ease;"></div>
                            ${uaCap > 0 ? `<div style="position:absolute;right:${100 - uaPct}%;top:0;height:100%;width:${Math.max(uaCapPct * uaPct / 100, 2)}%;background:#f59e0b;opacity:0.7;border-radius:0 3px 3px 0;" title="+${uaCap} captured from RU"></div>` : ''}
                            <span style="position:absolute;left:6px;top:50%;transform:translateY(-50%);font-family:'JetBrains Mono',monospace;font-size:0.6rem;color:#eff6ff;font-weight:700;text-shadow:0 1px 2px rgba(0,0,0,0.8);">${uaTotal.toLocaleString()}</span>
                        </div>
                        <span style="min-width:52px;text-align:right;font-family:'JetBrains Mono',monospace;font-size:0.65rem;font-weight:700;color:#3b82f6;">NET ${uaNet.toLocaleString()}</span>
                    </div>
                    <!-- Ratio -->
                    <div style="text-align:right;font-family:'JetBrains Mono',monospace;font-size:0.5rem;color:#475569;">RATIO ${ratio}:1</div>
                </div>
            </div>`;
        });

        // GLOBAL TOTALS
        html += `
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:6px; margin-top:8px;">
            <div style="background:rgba(220,38,38,0.08); border:1px solid rgba(220,38,38,0.25); border-radius:6px; padding:8px 10px; text-align:center;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;color:#94a3b8;letter-spacing:1px;">🇷🇺 RU TOTAL</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:1.1rem;font-weight:700;color:#ef4444;">${(globalRU.total_lost || 0).toLocaleString()}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:#f59e0b;">+${(globalRU.captured_from_enemy || 0).toLocaleString()} recaptured</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;font-weight:700;color:#fca5a5;margin-top:2px;">NET: ${(globalRU.net_loss || 0).toLocaleString()}</div>
            </div>
            <div style="background:rgba(37,99,235,0.08); border:1px solid rgba(37,99,235,0.25); border-radius:6px; padding:8px 10px; text-align:center;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;color:#94a3b8;letter-spacing:1px;">🇺🇦 UA TOTAL</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:1.1rem;font-weight:700;color:#3b82f6;">${(globalUA.total_lost || 0).toLocaleString()}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:#f59e0b;">+${(globalUA.captured_from_enemy || 0).toLocaleString()} recaptured</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.75rem;font-weight:700;color:#93c5fd;margin-top:2px;">NET: ${(globalUA.net_loss || 0).toLocaleString()}</div>
            </div>
        </div>`;

        // Data Integrity Footnote
        html += `
        <div style="margin-top:8px; padding:6px 10px; font-size:0.55rem; color:#475569; font-family:'JetBrains Mono',monospace; background:rgba(15,23,42,0.4); border-radius:4px; border:1px solid #1e293b; line-height:1.4;">
            <i class="fa-solid fa-shield-halved" style="color:#f59e0b;margin-right:4px;"></i>
            UA data = visually confirmed minimums · RU data = visual confirmations · Net Loss = Total - Captured from Enemy ·
            Source: <a href="https://www.oryxspioenkop.com" target="_blank" style="color:#f59e0b;text-decoration:none;">oryxspioenkop.com</a>
        </div>`;

        this.content.innerHTML = html;
        console.log('[WarLedger] Net Balance v2 rendered');
    }

    // --- RENDER ---
    render() {
        // Lazy DOM resolution
        if (!this.content) {
            this.content = document.getElementById('ticker-content');
        }
        if (!this.content) {
            console.warn('❌ EquipmentTicker: #ticker-content still not found!');
            return;
        }

        if (this.data.length === 0) {
            this.content.innerHTML = '<div class="loss-card holo placeholder">No recent losses reported.</div>';
            return;
        }

        const displayData = this.data.slice(0, 50);

        const itemsHtml = displayData.map(item => {
            const statusClass = (item.status || 'unknown').toLowerCase().replace(/\s+/g, '-');
            const svgIcon = this._getSvgIcon(item.model, item.type);
            const econValue = this._getEconomicValue(item.model, item.type);
            const econStr = econValue >= 100 ? `$${econValue.toFixed(0)}M` : `$${econValue.toFixed(1)}M`;
            const tieTotal = Math.round(item.tie_total || 0);
            const vecT = parseFloat(item.vec_t || 0);
            const isHVT = tieTotal >= 80 || vecT >= 8;

            // Recency Effect (< 24H)
            const itemDate = new Date(item.date);
            const isRecent = (new Date() - itemDate) <= 86400000;
            const recentClass = isRecent ? 'pulse-recent-loss' : '';

            // Country flag
            const flagIcon = (item.country === 'RUS' || item.country === 'RU')
                ? '🇷🇺' : (item.country === 'UA' || item.country === 'UKR' ? '🇺🇦' : '🏴');

            // Source badge
            const sourceBadge = item.source_tag
                ? `<span class="loss-source-tag">${item.source_tag}</span>` : '';

            // IMINT / Tooltip integration
            let imintHover = '';
            let imintIcon = '';
            if (item.visual_evidence) {
                imintHover = `onmouseover="window.showImintTooltip(this, '${item.visual_evidence}')" onmouseout="window.hideImintTooltip()"`;
                imintIcon = `<i class="fa-solid fa-camera" style="color:#f59e0b; font-size:0.6rem; margin-left:4px;" title="IMINT Available"></i>`;
            }

            // Proof link
            const proofLink = item.proof_url
                ? `<a href="${item.proof_url}" target="_blank" rel="noopener" class="loss-proof-link" title="View proof" onclick="event.stopPropagation()"><i class="fa-solid fa-arrow-up-right-from-square"></i></a>` : '';

            // Clickable attributes
            const clickAttr = (item.lat && item.lon)
                ? `onclick="window.Dashboard.ticker._flyToLoss(${item.lat}, ${item.lon}, '${(item.model || '').replace(/'/g, "\\'")}')" style="cursor:pointer;"`
                : '';

            return `
            <div class="loss-card holo ${statusClass} ${isHVT ? 'hvt-glow hvt-elite-border' : ''} ${recentClass}" ${clickAttr} ${imintHover}>
                <div class="loss-icon-svg">${svgIcon}</div>
                <div class="loss-info">
                    <span class="loss-model">${item.model || 'Unknown'} ${imintIcon}</span>
                    <span class="loss-meta">${item.type || ''} • ${item.date || '--'}</span>
                </div>
                <div class="loss-econ">
                    <span class="loss-econ-value">${econStr}</span>
                    ${tieTotal > 0 ? `<span class="loss-tie-badge">TIE ${tieTotal}</span>` : ''}
                </div>
                <span class="loss-status ${statusClass}">${(item.status || '').split(' ')[0].toUpperCase()}</span>
                <span class="loss-country">${flagIcon}</span>
                ${sourceBadge}
                ${proofLink}
            </div>`;
        }).join('');

        this.content.innerHTML = itemsHtml;
        console.log(`✅ EquipmentTicker v2.0 rendered ${displayData.length} items`);
    }
}

// =============================================================================
// GLOBAL DASHBOARD INSTANCE
// =============================================================================
window.Dashboard = {
    gauge: null,
    ticker: null,

    init: async function () {
        console.log("🛡️ Initializing Tactical Dashboard v2.0...");

        if (!window.globalEvents || window.globalEvents.length === 0) {
            console.warn("Dashboard: Waiting for events...");
            setTimeout(() => window.Dashboard.init(), 1000);
            return;
        }

        window.Dashboard.gauge = new MomentumGauge('momentumCanvas', 'momentumValue', 'momentumStatus');
        window.Dashboard.ticker = new EquipmentTicker('equipment-ticker-wrapper');

        window.Dashboard.gauge.render(window.globalEvents);
        await window.Dashboard.ticker.loadData(window.globalEvents);

        console.log("✅ Dashboard v2.0 Active.");
    },

    update: function (filteredEvents) {
        if (window.Dashboard.gauge) window.Dashboard.gauge.render(filteredEvents);
        if (window.Dashboard.ticker) window.Dashboard.ticker.loadData(filteredEvents);
    }
};

// Filter function for Equipment Losses tabs
window.filterLosses = function (category) {
    document.querySelectorAll('.loss-tab').forEach(btn => btn.classList.remove('active'));
    if (event && event.target) event.target.classList.add('active');

    // If in net view, switch back to gross
    if (window.Dashboard.ticker) window.Dashboard.ticker.viewMode = 'gross';

    if (category === 'hvt') {
        window.Dashboard.ticker.data = window.Dashboard.ticker.allData.filter(d => (d.tie_total >= 80 || d.vec_t >= 8));
        window.Dashboard.ticker.render();
    } else {
        window.Dashboard.ticker.filterByCategory(category);
    }
}

// Toggle between Gross/Net view
window.toggleNetBalance = function () {
    const ticker = window.Dashboard?.ticker;
    if (!ticker) return;

    ticker.viewMode = ticker.viewMode === 'gross' ? 'net' : 'gross';

    // Update toggle button
    const btn = document.getElementById('netBalanceToggle');
    if (btn) {
        btn.classList.toggle('active', ticker.viewMode === 'net');
        btn.innerHTML = ticker.viewMode === 'net'
            ? '<i class="fa-solid fa-scale-balanced"></i> NET'
            : '<i class="fa-solid fa-scale-balanced"></i> NET';
    }

    if (ticker.viewMode === 'net') {
        ticker._renderNetBalance();
    } else {
        ticker.render();
    }
}

// =============================================================================
// GLOBAL PERFORMANCE FILTERS
// =============================================================================
window.activePerformanceFilter = null;

window.togglePerformanceFilter = function (filterType) {
    if (window.activePerformanceFilter === filterType) {
        // Deactivate
        window.activePerformanceFilter = null;
        document.querySelectorAll('.perf-filter-btn').forEach(b => b.classList.remove('active'));
    } else {
        window.activePerformanceFilter = filterType;
        document.querySelectorAll('.perf-filter-btn').forEach(b => {
            b.classList.toggle('active', b.dataset.filter === filterType);
        });
    }

    // Re-trigger the main map filter pipeline
    if (window.applyMapFilters) {
        window.applyMapFilters();
    }
}

// Global IMINT tooltip handlers
window.showImintTooltip = function (el, imgUrl) {
    let tooltip = document.getElementById('imint-hover-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.id = 'imint-hover-tooltip';
        tooltip.style.position = 'absolute';
        tooltip.style.zIndex = '100000';
        tooltip.style.background = '#0f172a';
        tooltip.style.border = '1px solid #f59e0b';
        tooltip.style.borderRadius = '4px';
        tooltip.style.padding = '4px';
        tooltip.style.boxShadow = '0 4px 12px rgba(0,0,0,0.5)';
        tooltip.style.pointerEvents = 'none';
        document.body.appendChild(tooltip);
    }

    // Add image
    tooltip.innerHTML = `<img src="${imgUrl}" style="max-width:200px; max-height:150px; border-radius:2px; display:block;">`;

    // Position
    const rect = el.getBoundingClientRect();
    tooltip.style.left = (rect.left - 210) + 'px'; // Show to the left of the card
    tooltip.style.top = rect.top + 'px';
    tooltip.style.display = 'block';
};

window.hideImintTooltip = function () {
    const tooltip = document.getElementById('imint-hover-tooltip');
    if (tooltip) tooltip.style.display = 'none';
};
