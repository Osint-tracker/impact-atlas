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
        if (!this.content) {
            console.warn('⚠️ EquipmentTicker: #ticker-content not found at construction, will retry at render');
        }
        console.log('📦 EquipmentTicker v2.0 constructed', { container: !!this.container, content: !!this.content });
    }

    // --- DATA LOADING ---
    async loadData(internalEvents) {
        console.log('📡 EquipmentTicker.loadData called');

        // Stream A: Internal events
        const internalLosses = (internalEvents || [])
            .filter(e => e.target_type && e.target_type !== 'NULL' && e.target_type !== 'UNKNOWN')
            .map(e => ({
                source: 'INTERNAL', date: e.date, model: e.target_type,
                country: 'UNKNOWN', status: 'REPORTED', category: 'other',
                lat: e.lat || null, lon: e.lon || null,
                tie_total: e.tie_total || 0
            }));

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
                    if (typeLower.includes('aircraft') || typeLower.includes('helicopter') ||
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
        this._renderBurnRate();
        this.render();
    }

    filterByCategory(category) {
        this.data = category === 'all' ? this.allData : this.allData.filter(d => d.category === category);
        this.render();
    }

    // --- BURN RATE HEADER ---
    _renderBurnRate() {
        const burnEl = document.getElementById('burn-rate-stats');
        if (!burnEl) return;

        const total = this.allData.length;
        const now = new Date();
        const today = now.toISOString().split('T')[0];
        const yesterday = new Date(now - 86400000).toISOString().split('T')[0];

        // Count today + yesterday
        const last24h = this.allData.filter(d => d.date >= yesterday).length;

        // Daily average (using date range of dataset)
        const dates = this.allData.map(d => d.date).filter(Boolean);
        let dailyAvg = 0;
        if (dates.length > 1) {
            const earliest = new Date(dates[dates.length - 1]);
            const latest = new Date(dates[0]);
            const daySpan = Math.max(1, (latest - earliest) / 86400000);
            dailyAvg = (total / daySpan).toFixed(1);
        }

        // Economic total
        let econTotal = 0;
        this.allData.forEach(d => {
            econTotal += this._getEconomicValue(d.model, d.type);
        });
        const econStr = econTotal >= 1000 ? `$${(econTotal / 1000).toFixed(1)}B` : `$${econTotal.toFixed(0)}M`;

        burnEl.innerHTML = `
            <div class="burn-stat">
                <span class="burn-label">24H</span>
                <span class="burn-value">${last24h}</span>
            </div>
            <div class="burn-stat">
                <span class="burn-label">DAILY AVG</span>
                <span class="burn-value">${dailyAvg}</span>
            </div>
            <div class="burn-stat">
                <span class="burn-label">TOTAL</span>
                <span class="burn-value">${total}</span>
            </div>
            <div class="burn-stat econ">
                <span class="burn-label">EST. VALUE</span>
                <span class="burn-value">${econStr}</span>
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
            const isHVT = tieTotal >= 70 || econValue >= 20;

            // Country flag
            const flagIcon = (item.country === 'RUS' || item.country === 'RU')
                ? '🇷🇺' : (item.country === 'UA' ? '🇺🇦' : '🏴');

            // Source badge
            const sourceBadge = item.source_tag
                ? `<span class="loss-source-tag">${item.source_tag}</span>` : '';

            // Proof link
            const proofLink = item.proof_url
                ? `<a href="${item.proof_url}" target="_blank" rel="noopener" class="loss-proof-link" title="View proof" onclick="event.stopPropagation()"><i class="fa-solid fa-arrow-up-right-from-square"></i></a>` : '';

            // Clickable attributes
            const clickAttr = (item.lat && item.lon)
                ? `onclick="window.Dashboard.ticker._flyToLoss(${item.lat}, ${item.lon}, '${(item.model || '').replace(/'/g, "\\'")}')" style="cursor:pointer;"`
                : '';

            return `
            <div class="loss-card holo ${statusClass} ${isHVT ? 'hvt-glow' : ''}" ${clickAttr}>
                <div class="loss-icon-svg">${svgIcon}</div>
                <div class="loss-info">
                    <span class="loss-model">${item.model || 'Unknown'}</span>
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

    if (window.Dashboard.ticker) {
        window.Dashboard.ticker.filterByCategory(category);
    } else {
        console.warn('filterLosses: ticker not yet initialized');
    }
};
