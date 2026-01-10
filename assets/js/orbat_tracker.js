class OrbatTracker {
    constructor() {
        this.units = [];
        this.currentFaction = 'UA'; // 'UA' or 'RU'
        this.container = document.getElementById('orbat-list');
    }

    async init() {
        try {
            const response = await fetch('assets/data/units.json');
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

            // Icon mapping
            let icon = 'fa-person-rifle';
            if (unit.type.includes('ARMORED')) icon = 'fa-truck-monster'; // Tank-ish
            if (unit.type.includes('ARTILLERY')) icon = 'fa-bomb';
            if (unit.type.includes('AIRBORNE')) icon = 'fa-parachute-box';

            // Status Color
            let statusColor = '#94a3b8'; // active/grey
            if (unit.status === 'ENGAGED') statusColor = '#ef4444'; // red
            if (unit.status === 'REGROUPING') statusColor = '#f59e0b'; // amber

            el.innerHTML = `
                <div class="unit-header">
                    <span class="unit-type-icon"><i class="fa-solid ${icon}"></i></span>
                    <span class="unit-name">${unit.display_name}</span>
                </div>
                <div class="unit-meta">
                    <span class="unit-badge" style="border: 1px solid ${statusColor}; color: ${statusColor}">
                        ${unit.status}
                    </span>
                    <span class="unit-date">
                        <i class="fa-regular fa-clock"></i> ${this.formatDate(unit.last_seen_date)}
                    </span>
                </div>
                ${unit.subordination ? `<div class="unit-sub">${unit.subordination}</div>` : ''}
                
                <button class="unit-locate-btn" onclick="window.flyToUnit(${unit.last_seen_lat}, ${unit.last_seen_lon}, '${unit.unit_id}')">
                    <i class="fa-solid fa-crosshairs"></i> LOCATE ON MAP
                </button>
            `;
            this.container.appendChild(el);
        });
    }

    formatDate(dateStr) {
        if (!dateStr || dateStr === 'None') return 'Unknown';
        try {
            return new Date(dateStr).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
        } catch (e) { return dateStr; }
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
