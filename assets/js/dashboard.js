/**
 * TACTICAL DASHBOARD CONTROLLER
 * Manages Momentum Gauge, Heatmap Logic, and Equipment Ticker
 */

console.log("🚀 Dashboard Module Loading...");

class MomentumGauge {
    constructor(canvasId, valueId, statusId) {
        // Canvas no longer used, but keep reference for compatibility
        this.valueEl = document.getElementById(valueId);
        this.statusEl = document.getElementById(statusId);
        this.barFill = document.getElementById('tempoBarFill');
        this.marker = document.getElementById('tempoMarker');
    }

    calculateMetrics(events) {
        // Filter last 48 hours
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
        const ratio = total === 0 ? 0.5 : (offensiveCount / total); // 0.0 - 1.0

        return { ratio, total, offensiveCount };
    }

    render(events) {
        const metrics = this.calculateMetrics(events);
        const percent = Math.round(metrics.ratio * 100);

        // Update Text
        if (this.valueEl) this.valueEl.innerText = `${percent}%`;

        let statusText = "STALEMATE";
        let color = "#64748b"; // Grey

        if (percent > 65) {
            statusText = "HIGH TEMPO OFFENSIVE";
            color = "#ef4444"; // Red
        } else if (percent > 40) {
            statusText = "ACTIVE CONTEST";
            color = "#f59e0b"; // Amber
        } else {
            statusText = "STATIC / ATTRITION";
            color = "#3b82f6"; // Blue
        }

        if (this.statusEl) {
            this.statusEl.innerText = statusText;
            this.statusEl.style.color = color;
        }

        // Update horizontal bar and marker
        if (this.barFill) {
            this.barFill.style.width = `${percent}%`;
        }
        if (this.marker) {
            this.marker.style.left = `${percent}%`;
        }
    }
}

class EquipmentTicker {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.content = document.getElementById('ticker-content');
        this.data = [];
    }

    async loadData(internalEvents) {
        // Stream A: Internal Report (Filtered by target_type)
        const internalLosses = internalEvents
            .filter(e => e.target_type && e.target_type !== 'NULL' && e.target_type !== 'UNKNOWN')
            .map(e => ({
                source: 'INTERNAL',
                date: e.date,
                model: e.target_type, // Using target_type as model proxy
                country: 'UNKNOWN', // We might infer this from 'actor' if available
                status: 'REPORTED',
                icon: '<i class="fa-solid fa-triangle-exclamation" style="color:#eab308"></i>'
            }));

        // Stream B: External JSON (Oryx Mock)
        let externalLosses = [];
        try {
            const res = await fetch('assets/data/external_losses.json');
            if (res.ok) {
                const json = await res.json();
                externalLosses = json.map(item => ({
                    source: 'EXTERNAL',
                    date: item.date, // YYYY-MM-DD
                    model: item.model,
                    country: item.country,
                    status: 'CONFIRMED',
                    icon: '<i class="fa-solid fa-circle-check" style="color:#22c55e"></i>'
                }));
            }
        } catch (err) {
            console.warn("Could not load external losses:", err);
        }

        // Merge and Sort
        this.data = [...internalLosses, ...externalLosses]
            .sort((a, b) => new Date(b.date) - new Date(a.date));

        this.render();
    }

    render() {
        if (!this.content) return;

        if (this.data.length === 0) {
            this.content.innerHTML = '<div class="loss-card placeholder">No recent losses reported.</div>';
            return;
        }

        // Limit to 20 most recent for cleaner display
        const displayData = this.data.slice(0, 20);

        // Create HTML cards
        const itemsHtml = displayData.map(item => {
            const statusClass = item.status.toLowerCase().replace(/\s+/g, '-');
            const statusClean = item.status.split(' ')[0].toUpperCase(); // First word

            // Icon based on type
            let iconClass = 'fa-solid fa-truck-monster'; // Default ground
            const type = (item.model || '').toLowerCase();
            if (type.includes('heli') || type.includes('ka-') || type.includes('mi-')) iconClass = 'fa-solid fa-helicopter';
            if (type.includes('jet') || type.includes('su-') || type.includes('mig')) iconClass = 'fa-solid fa-jet-fighter';
            if (type.includes('art') || type.includes('howitzer') || type.includes('m777')) iconClass = 'fa-solid fa-burst';
            if (type.includes('tank') || type.includes('t-')) iconClass = 'fa-solid fa-truck-monster';
            if (type.includes('drone') || type.includes('uav') || type.includes('orlan')) iconClass = 'fa-solid fa-plane';

            return `
            <div class="loss-card ${statusClass}">
                <div class="loss-icon"><i class="${iconClass}"></i></div>
                <div class="loss-info">
                    <span class="loss-model">${item.model}</span>
                    <span class="loss-meta">${item.date}</span>
                </div>
                <span class="loss-status ${statusClass}">${statusClean}</span>
                <span class="loss-country">${item.country}</span>
            </div>`;
        }).join('');

        this.content.innerHTML = itemsHtml;
    }
}

// Global Dashboard Instance
window.Dashboard = {
    init: async function () {
        console.log("🛡️ Initializing Tactical Dashboard...");

        // Wait for global events to be loaded by map.js
        if (!window.globalEvents || window.globalEvents.length === 0) {
            console.warn("Dashboard: Waiting for events...");
            setTimeout(window.Dashboard.init, 1000); // Retry logic could be cleaner but works
            return;
        }

        this.gauge = new MomentumGauge('momentumCanvas', 'momentumValue', 'momentumStatus');
        this.ticker = new EquipmentTicker('equipment-ticker-wrapper');

        // Render components
        this.gauge.render(window.globalEvents);
        await this.ticker.loadData(window.globalEvents);

        console.log("✅ Dashboard Active.");
    },

    // Called when filters change (hooked into map.js filter logic if needed)
    update: function (filteredEvents) {
        if (this.gauge) this.gauge.render(filteredEvents);
        if (this.ticker) this.ticker.loadData(filteredEvents); // Or keep ticker static/global
    }
};

// Hook into existing toggleVisualMode
// We'll Monkey Patch it or ensure map.js calls it.
// For now, let's rely on map.js modifications.
