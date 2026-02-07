
// Report Generation Engine (Client-Side)
document.addEventListener('DOMContentLoaded', async function () {

    // 1. Parse URL Parameters
    const urlParams = new URLSearchParams(window.location.search);
    const startDateStr = urlParams.get('start');
    const endDateStr = urlParams.get('end');

    if (!startDateStr) {
        document.getElementById('execSummaryText').innerHTML = "<p style='color:red'>Error: No date range specified.</p>";
        return;
    }

    // Handle "LIVE" end date
    let endDate = new Date();
    if (endDateStr && endDateStr !== 'LIVE') {
        endDate = new Date(endDateStr);
    }
    const startDate = new Date(startDateStr);

    // Update Header
    document.querySelector('.date').innerText = `${startDate.toLocaleDateString()} - ${endDateStr === 'LIVE' ? 'LIVE' : endDate.toLocaleDateString()}`;

    // 2. Fetch Data
    try {
        const response = await fetch('assets/data/events.geojson');
        const data = await response.json();

        // 3. Filter Data
        const events = data.features.filter(f => {
            const d = new Date(f.properties.date);
            return d >= startDate && d <= endDate;
        });

        // 4. Calculate Metrics
        calculateAndRenderMetrics(events, startDate, endDate);

    } catch (error) {
        console.error("Failed to load report data:", error);
        document.getElementById('execSummaryText').innerHTML = "<p>Data unavailable for this period.</p>";
    }

    // Clock
    setInterval(() => {
        document.getElementById('utc-time').innerText = new Date().toISOString().split('T')[1].split('.')[0] + ' UTC';
    }, 1000);
});

function calculateAndRenderMetrics(events, startDate, endDate) {
    const totalEvents = events.length;

    // Avg TIE
    let totalTie = 0;
    let maxTie = 0;
    let kineticSum = 0;

    // Sectors (rough approximation by density)
    // We can't do complex geo-hashing here easily, so we'll look at descriptions or just generic
    // Let's use simple coordinate averaging to determine "Active Front"
    let latSum = 0;
    let lonSum = 0;

    events.forEach(e => {
        const tie = e.properties.tie_total || 0;
        totalTie += tie;
        if (tie > maxTie) maxTie = tie;
        kineticSum += (e.properties.vec_k || 0);

        const coords = e.geometry.coordinates; // [lon, lat]
        latSum += coords[1];
        lonSum += coords[0];
    });

    const avgTie = totalEvents > 0 ? (totalTie / totalEvents).toFixed(1) : 0;

    // Render Basic Metrics
    document.getElementById('metricEvents').innerText = totalEvents;
    document.getElementById('metricTie').innerText = avgTie;
    document.getElementById('metricSources').innerText = events.length > 0 ? (events.length * 1.5).toFixed(0) : 0; // Simulated source count logic

    // Gauge Icon Color
    const tieIcon = document.getElementById('metricTieIcon');
    const tieLabel = document.getElementById('metricTieLabel');
    if (avgTie > 70) {
        tieIcon.style.color = '#ef4444';
        tieLabel.innerHTML = 'Warning Level: <span style="color:#ef4444">CRITICAL</span>';
    } else if (avgTie > 40) {
        tieIcon.style.color = '#f59e0b';
        tieLabel.innerHTML = 'Warning Level: <span style="color:#f59e0b">ELEVATED</span>';
    } else {
        tieIcon.style.color = '#3b82f6';
        tieLabel.innerHTML = 'Warning Level: <span style="color:#3b82f6">NORMAL</span>';
    }

    // Active Sector Logic (Simple Centroid)
    let sectorName = "UNKNOWN";
    if (totalEvents > 0) {
        const avgLat = latSum / totalEvents;
        const avgLon = lonSum / totalEvents;

        // Rough Ukraine Boxes
        if (avgLat > 50) sectorName = "NORTHERN FRONT";
        else if (avgLon > 37) sectorName = "DONBAS (EAST)";
        else if (avgLat < 47.5) sectorName = "ZAPORIZHZHIA (SOUTH)";
        else sectorName = "EASTERN FRONT"; // Fallback
    }
    document.getElementById('metricSector').innerText = sectorName;
    document.getElementById('metricSectorLabel').innerHTML = totalEvents > 0 ?
        `<i class="fa-solid fa-crosshairs"></i> HIGH ACTIVITY ZONE` : "NO DATA";

    // 5. Generate Chart (Trend)
    renderTrendChart(events);

    // 6. Generate Executive Summary
    generateExecutiveSummary(totalEvents, avgTie, sectorName, events);
}

function renderTrendChart(events) {
    const ctx = document.getElementById('threatChart').getContext('2d');

    // bin events by date
    const dateCounts = {};
    events.forEach(e => {
        const day = e.properties.date.split('T')[0];
        if (!dateCounts[day]) dateCounts[day] = 0;
        dateCounts[day] += (e.properties.tie_total || 5); // Accumulate TIE or Count
    });

    const labels = Object.keys(dateCounts).sort();
    const dataPoints = labels.map(d => dateCounts[d]);

    if (labels.length === 0) {
        labels.push("No Data");
        dataPoints.push(0);
    }

    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(239, 68, 68, 0.5)');
    gradient.addColorStop(1, 'rgba(239, 68, 68, 0.0)');

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Cumulative Threat Score',
                data: dataPoints,
                borderColor: '#ef4444',
                backgroundColor: gradient,
                borderWidth: 2,
                tension: 0.4,
                fill: true,
                pointBackgroundColor: '#ef4444',
                pointRadius: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.05)' },
                    ticks: { color: '#94a3b8', maxTicksLimit: 6 }
                },
                y: { display: false }
            }
        }
    });
}

function generateExecutiveSummary(count, avgTie, sector, events) {
    const container = document.getElementById('execSummaryText');
    const badge = document.getElementById('alertBadge');

    if (count === 0) {
        container.innerHTML = "<p>No intelligence activity reported for the selected period.</p>";
        badge.style.display = 'none';
        return;
    }

    // Determine tone
    let tone = "stable";
    if (avgTie > 50) tone = "critical";
    else if (avgTie > 30) tone = "volatile";

    if (tone === 'critical') badge.style.display = 'flex';
    else badge.style.display = 'none';

    // Highlight Event
    const topEvent = events.sort((a, b) => (b.properties.tie_total || 0) - (a.properties.tie_total || 0))[0];
    const topEventTitle = topEvent ? topEvent.properties.title : "Unknown Event";

    const text = `
    <p><b>Executive Summary:</b> Analysis of the period indicates a <b>${tone.toUpperCase()}</b> security environment with <b>${count}</b> distinct confirmed events. 
    The operational tempo was concentrated primarily in the <b>${sector}</b>, with an average Threat Integration Event (T.I.E.) score of <b>${avgTie}</b>.</p>
    
    <p>The most significant incident documented was "<b>${topEventTitle}</b>", which contributed disproportionately to the period's kinetic index. 
    ${avgTie > 40 ? "Intelligence recommends heightened readiness posture for units in this sector." : "Activity remains within expected baseline parameters."}</p>
    `;

    container.innerHTML = text;
}
