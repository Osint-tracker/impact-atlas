class OrbatTracker {
    constructor() {
        this.units = [];
        this.currentFaction = 'UA'; // 'UA' or 'RU'
        this.container = document.getElementById('orbat-list');
    }

    async init() {
        console.log("🚀 ORBAT TRACKER UPDATED VERSION LOADED 🚀");
        try {
            const response = await fetch(`assets/data/units.json?v=${new Date().getTime()}`);
            this.units = await response.json();
            console.log(`[ORBAT] Loaded ${this.units.length} units.`);
            this.render();
        } catch (e) {
            console.error("[ORBAT] Failed to load units:", e);
            if (this.container) this.container.innerHTML = '<div class="error-msg">Failed to load unit registry.</div>';
        }
    }

    setFaction(faction) {
        this.currentFaction = faction;
        // Update tab styling
        document.querySelectorAll('.s-tab-btn').forEach(btn => {
            if (btn.innerText.includes(faction)) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
        this.render();
    }

    render() {
        if (!this.container) return;
        this.container.innerHTML = '';

        const filtered = this.units.filter(u => u.faction === this.currentFaction);

        // Sort: ENGAGED first, then by date desc
        filtered.sort((a, b) => {
            if (a.status === 'ENGAGED' && b.status !== 'ENGAGED') return -1;
            if (a.status !== 'ENGAGED' && b.status === 'ENGAGED') return 1;
            return new Date(b.last_seen_date || 0) - new Date(a.last_seen_date || 0);
        });

        filtered.forEach(unit => {
            const el = document.createElement('div');
            el.className = 'unit-card';
            // Make the whole card clickable
            el.style.cursor = 'pointer';
            el.onclick = () => {
                console.log(`[ORBAT] Card clicked for: ${unit.display_name}`);
                if (window.openUnitModal) {
                    window.openUnitModal(unit);
                } else {
                    console.error("openUnitModal not defined");
                }
            };

            // Status Logic for CSS class
            let statusClass = 'status-idle';
            if (unit.status === 'ENGAGED') statusClass = 'status-engaged';
            if (unit.status === 'REGROUPING' || unit.status === 'MOVING') statusClass = 'status-regrouping';

            // Strength formatting
            let strengthDisplay = unit.strength ? `${unit.strength}%` : 'UNK';

            // Location formatting
            let locDisplay = unit.location_name || 'CLASSIFIED';

            // Icon mapping (Restored)
            let icon = 'fa-person-rifle';
            if (unit.type && unit.type.includes('ARMORED')) icon = 'fa-truck-monster';
            if (unit.type && unit.type.includes('ARTILLERY')) icon = 'fa-bomb';
            if (unit.type && unit.type.includes('AIRBORNE')) icon = 'fa-parachute-box';
            if (unit.type && unit.type.includes('RECON')) icon = 'fa-binoculars';

            // Inner HTML Structure
            el.innerHTML = `
                <div class="hud-card ${statusClass}">
                    
                    <div class="hud-header">
                        <span class="hud-unit-name">
                             <i class="fa-solid ${icon}" style="margin-right:8px; opacity:0.7; font-size:0.9em;"></i>
                             ${unit.display_name}
                        </span>
                        <span class="hud-status-dot"></span> 
                    </div>

                    <div class="hud-details">
                        <div class="hud-meta-row">
                             <div><span class="hud-label">STR:</span> ${strengthDisplay}</div>
                             <div style="text-align:right;"><span class="hud-label">LOC:</span> ${locDisplay}</div>
                        </div>
                        <div class="hud-meta-row">
                            <div><span class="hud-label">UPD:</span> ${this.formatTimeAgo(unit.last_seen_date)}</div>
                        </div>

                        ${unit.subordination ? `<div class="unit-sub" style="margin-top:4px;">CMD: ${unit.subordination}</div>` : ''}

                        <div class="hud-coords" 
                            title="${(unit.last_seen_lat && unit.last_seen_lon) ? 'Locate on Map' : 'Location Unknown'}"
                            style="${(!unit.last_seen_lat || !unit.last_seen_lon) ? 'opacity:0.5; cursor:not-allowed;' : ''}">
                            <i class="fa-solid fa-crosshairs"></i> 
                            ${(unit.last_seen_lat && unit.last_seen_lon) ? `${unit.last_seen_lat.toFixed(4)}N ${unit.last_seen_lon.toFixed(4)}E` : 'GRID UNKNOWN'}
                        </div>
                    </div>
                </div>
            `;

            // Attach specific listener for "Locate" button to stop propagation
            const locateBtn = el.querySelector('.hud-coords');
            if (locateBtn && unit.last_seen_lat && unit.last_seen_lon) {
                locateBtn.onclick = (e) => {
                    e.stopPropagation(); // Prevents card click (Modal)
                    console.log(`[ORBAT] Locate clicked for: ${unit.display_name}`);
                    if (window.flyToUnit) {
                        window.flyToUnit(unit.last_seen_lat, unit.last_seen_lon, unit.display_name);
                    }
                };
            }

            this.container.appendChild(el);
        });
    }

    formatTimeAgo(dateStr) {
        if (!dateStr || dateStr === 'None') return 'UNK';
        try {
            const date = new Date(dateStr);
            const now = new Date();
            const diffMs = now - date;
            const diffHrs = Math.floor(diffMs / (1000 * 60 * 60));

            if (diffHrs < 1) return 'JUST NOW';
            if (diffHrs < 24) return `${diffHrs}H AGO`;
            return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }).toUpperCase();
        } catch (e) { return 'UNK'; }
    }
}

// Global accessor
window.orbatTracker = new OrbatTracker();

// Expose switch function for HTML onclick
window.switchOrbatTab = function (faction) {
    window.orbatTracker.setFaction(faction);
};

// Auto-init when DOM ready
document.addEventListener('DOMContentLoaded', () => {
    // Check if container exists, if so init
    if (document.getElementById('orbat-list')) {
        window.orbatTracker.init();
    }
});
