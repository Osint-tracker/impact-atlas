/**
 * TACTICAL DASHBOARD CONTROLLER
 * Manages Momentum Gauge, Heatmap Logic, and Equipment Ticker
 */

console.log("🚀 Dashboard Module Loading...");

class MomentumGauge {
    constructor(canvasId, valueId, statusId) {
        this.canvas = document.getElementById(canvasId);
        this.valueEl = document.getElementById(valueId);
        this.statusEl = document.getElementById(statusId);
        this.chart = null;
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
        this.valueEl.innerText = `${percent}%`;

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

        this.statusEl.innerText = statusText;
        this.statusEl.style.color = color;

        // Draw Gauge (Simple Arc on Canvas)
        this.drawGauge(metrics.ratio, color);
    }

    drawGauge(value, color) {
        if (!this.canvas) return;
        const ctx = this.canvas.getContext('2d');
        const W = this.canvas.width = this.canvas.parentElement.clientWidth;
        const H = this.canvas.height = 200;
        const CX = W / 2;
        const CY = H - 20;
        const R = Math.min(W, H) / 1.5;

        // Clear
        ctx.clearRect(0, 0, W, H);

        // Background Arc
        ctx.beginPath();
        ctx.arc(CX, CY, R, Math.PI, 2 * Math.PI);
        ctx.lineWidth = 20;
        ctx.strokeStyle = "#1e293b";
        ctx.stroke();

        // Value Arc
        ctx.beginPath();
        const endAngle = Math.PI + (value * Math.PI);
        ctx.arc(CX, CY, R, Math.PI, endAngle);
        ctx.lineWidth = 20;
        ctx.strokeStyle = color;
        ctx.stroke();
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
        if (this.data.length === 0) {
            this.content.innerHTML = '<div class="ticker-item">No recent losses reported.</div>';
            return;
        }

        // Create HTML
        const itemsHtml = this.data.map(item => `
            <div class="ticker-item ${item.status.toLowerCase()}">
                <div class="ticker-icon">${item.icon}</div>
                <div class="ticker-info">
                    <span class="ticker-model">${item.model}</span>
                    <span class="ticker-meta">${item.date} | ${item.country} | ${item.status}</span>
                </div>
            </div>
        `).join('');

        // Duplicate for seamless scroll loop
        this.content.innerHTML = itemsHtml + itemsHtml;
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
