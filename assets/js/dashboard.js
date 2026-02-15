/**
 * TACTICAL DASHBOARD CONTROLLER
 * Manages Momentum Gauge, Heatmap Logic, and Equipment Ticker
 */

console.log("🚀 Dashboard Module Loading...");

class MomentumGauge {
    constructor(canvasId, valueId, statusId) {
        this.valueEl = document.getElementById(valueId);
        this.statusEl = document.getElementById(statusId);
        this.barFill = document.getElementById('tempoBarFill');
        this.marker = document.getElementById('tempoMarker');
    }

    calculateMetrics(events) {
        const now = moment();
        const cutoff = moment().subtract(48, 'hours');
        const recentEvents = events.filter(e => moment(e.date, "DD/MM/YYYY").isAfter(cutoff));

        let offensiveCount = 0;
        let staticCount = 0;

        recentEvents.forEach(e => {
            const cls = (e.classification || "UNKNOWN").toUpperCase();
            if (cls === 'MANOEUVRE' || cls === 'SHAPING_OFFENSIVE') {
                offensiveCount++;
            } else if (cls === 'ATTRITION') {
                staticCount++;
            }
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

        if (percent > 65) {
            statusText = "HIGH TEMPO OFFENSIVE";
            color = "#ef4444";
        } else if (percent > 40) {
            statusText = "ACTIVE CONTEST";
            color = "#f59e0b";
        } else {
            statusText = "STATIC / ATTRITION";
            color = "#3b82f6";
        }

        if (this.statusEl) {
            this.statusEl.innerText = statusText;
            this.statusEl.style.color = color;
        }

        if (this.barFill) this.barFill.style.width = `${percent}%`;
        if (this.marker) this.marker.style.left = `${percent}%`;
    }
}

class EquipmentTicker {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.content = document.getElementById('ticker-content');
        this.allData = [];  // Full dataset for filtering
        this.data = [];     // Currently displayed (filtered) data
        console.log('📦 EquipmentTicker constructed:', {
            container: !!this.container,
            content: !!this.content
        });
    }

    async loadData(internalEvents) {
        console.log('📡 EquipmentTicker.loadData called with', internalEvents?.length, 'internal events');

        // Stream A: Internal Report (Filtered by target_type)
        const internalLosses = (internalEvents || [])
            .filter(e => e.target_type && e.target_type !== 'NULL' && e.target_type !== 'UNKNOWN')
            .map(e => ({
                source: 'INTERNAL',
                date: e.date,
                model: e.target_type,
                country: 'UNKNOWN',
                status: 'REPORTED',
                category: 'other'
            }));

        console.log(`   Internal losses: ${internalLosses.length}`);

        // Stream B: External JSON (Oryx + LostArmour from impact_atlas.db)
        let externalLosses = [];
        try {
            const res = await fetch('assets/data/external_losses.json');
            console.log('   external_losses.json fetch status:', res.status);
            if (res.ok) {
                const json = await res.json();
                console.log(`   External losses loaded: ${json.length} records`);
                externalLosses = json.map(item => {
                    // Classify for filter tabs
                    const modelLower = (item.model || '').toLowerCase();
                    const typeLower = (item.type || '').toLowerCase();
                    let category = 'other';
                    if (typeLower.includes('tank') || modelLower.includes('t-')) category = 'tank';
                    if (typeLower.includes('aircraft') || typeLower.includes('helicopter') ||
                        modelLower.includes('su-') || modelLower.includes('mi-') ||
                        modelLower.includes('ka-') || modelLower.includes('mig') ||
                        typeLower.includes('uav') || typeLower.includes('drone')) category = 'air';

                    return {
                        source: 'EXTERNAL',
                        date: item.date,
                        model: item.model,
                        country: item.country || 'RUS',
                        status: item.status || 'CONFIRMED',
                        proof_url: item.proof_url || '',
                        source_tag: item.source_tag || 'Oryx',
                        type: item.type || 'Vehicle',
                        category: category
                    };
                });
            }
        } catch (err) {
            console.warn("Could not load external losses:", err);
        }

        // Merge and Sort
        this.allData = [...internalLosses, ...externalLosses]
            .sort((a, b) => new Date(b.date) - new Date(a.date));

        this.data = this.allData;

        console.log(`   Total merged losses: ${this.allData.length}`);
        this.render();
    }

    filterByCategory(category) {
        if (category === 'all') {
            this.data = this.allData;
        } else {
            this.data = this.allData.filter(d => d.category === category);
        }
        this.render();
    }

    render() {
        if (!this.content) {
            console.warn('❌ EquipmentTicker: #ticker-content not found!');
            return;
        }

        if (this.data.length === 0) {
            this.content.innerHTML = '<div class="loss-card placeholder">No recent losses reported.</div>';
            return;
        }

        // Show up to 50 most recent
        const displayData = this.data.slice(0, 50);

        const itemsHtml = displayData.map(item => {
            const statusClass = (item.status || 'unknown').toLowerCase().replace(/\s+/g, '-');

            // Icon based on model/type
            let iconClass = 'fa-solid fa-truck-monster';
            const type = (item.model || '').toLowerCase();
            const typeCategory = (item.type || '').toLowerCase();

            if (type.includes('heli') || type.includes('ka-') || type.includes('mi-')) iconClass = 'fa-solid fa-helicopter';
            else if (type.includes('su-') || type.includes('mig') || typeCategory.includes('aircraft')) iconClass = 'fa-solid fa-jet-fighter';
            else if (typeCategory.includes('artiller') || type.includes('howitzer') || typeCategory.includes('rocket')) iconClass = 'fa-solid fa-burst';
            else if (typeCategory.includes('tank') || type.includes('t-')) iconClass = 'fa-solid fa-truck-monster';
            else if (typeCategory.includes('uav') || type.includes('drone') || typeCategory.includes('reconnaissance')) iconClass = 'fa-solid fa-plane';
            else if (typeCategory.includes('radar') || typeCategory.includes('jammer')) iconClass = 'fa-solid fa-satellite-dish';
            else if (typeCategory.includes('naval') || typeCategory.includes('ship')) iconClass = 'fa-solid fa-ship';
            else if (typeCategory.includes('engineering') || typeCategory.includes('truck')) iconClass = 'fa-solid fa-truck';
            else if (typeCategory.includes('command') || typeCategory.includes('communication')) iconClass = 'fa-solid fa-tower-cell';

            // Country flag
            const flagIcon = item.country === 'RUS' || item.country === 'RU'
                ? '🇷🇺'
                : (item.country === 'UA' ? '🇺🇦' : '🏴');

            // Source badge
            const sourceBadge = item.source_tag
                ? `<span class="loss-source-tag">${item.source_tag}</span>`
                : '';

            // Proof link
            const proofLink = item.proof_url
                ? `<a href="${item.proof_url}" target="_blank" rel="noopener" class="loss-proof-link" title="View proof"><i class="fa-solid fa-arrow-up-right-from-square"></i></a>`
                : '';

            return `
            <div class="loss-card ${statusClass}">
                <div class="loss-icon"><i class="${iconClass}"></i></div>
                <div class="loss-info">
                    <span class="loss-model">${item.model}</span>
                    <span class="loss-meta">${item.type || ''} • ${item.date}</span>
                </div>
                <span class="loss-status ${statusClass}">${(item.status || '').split(' ')[0].toUpperCase()}</span>
                <span class="loss-country">${flagIcon}</span>
                ${sourceBadge}
                ${proofLink}
            </div>`;
        }).join('');

        this.content.innerHTML = itemsHtml;
        console.log(`✅ EquipmentTicker rendered ${displayData.length} items`);
    }
}

// Global Dashboard Instance
window.Dashboard = {
    gauge: null,
    ticker: null,

    init: async function () {
        console.log("🛡️ Initializing Tactical Dashboard...");

        // Wait for global events to be loaded by map.js
        if (!window.globalEvents || window.globalEvents.length === 0) {
            console.warn("Dashboard: Waiting for events...");
            // FIX: preserve `this` context with arrow function
            setTimeout(() => window.Dashboard.init(), 1000);
            return;
        }

        window.Dashboard.gauge = new MomentumGauge('momentumCanvas', 'momentumValue', 'momentumStatus');
        window.Dashboard.ticker = new EquipmentTicker('equipment-ticker-wrapper');

        // Render components
        window.Dashboard.gauge.render(window.globalEvents);
        await window.Dashboard.ticker.loadData(window.globalEvents);

        console.log("✅ Dashboard Active.");
    },

    update: function (filteredEvents) {
        if (window.Dashboard.gauge) window.Dashboard.gauge.render(filteredEvents);
        if (window.Dashboard.ticker) window.Dashboard.ticker.loadData(filteredEvents);
    }
};

// Filter function for Equipment Losses tabs
window.filterLosses = function (category) {
    // Update active tab styling
    document.querySelectorAll('.loss-tab').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');

    if (window.Dashboard.ticker) {
        window.Dashboard.ticker.filterByCategory(category);
    } else {
        console.warn('filterLosses: ticker not yet initialized');
    }
};
