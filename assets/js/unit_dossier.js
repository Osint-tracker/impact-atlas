/**
 * UNIT DOSSIER CARD — Frontend Rendering Module
 * Renders high-density tactical analytics from units.json dossier fields.
 * Dependencies: Chart.js (v4.4.4+), Leaflet
 */
(function () {
  'use strict';

  let _radarChart = null;
  let _sparkChart = null;

  /**
   * Main entry point: populate the dossier analytics section.
   */
  window.renderUnitDossierAnalytics = function (unit) {
    console.log("[UD] Rendering analytics for:", unit.unit_id);
    
    try {
      _renderAssets(unit.assets_detected || []);
    } catch (e) { console.error("[UD] Assets error:", e); }

    try {
      _renderFrequencyBadge(unit.engagement_freq_label || 'Low');
    } catch (e) { console.error("[UD] Freq badge error:", e); }

    try {
      _renderCharts(unit.avg_tie || {}, unit.engagement_trend_30d || []);
    } catch (e) { console.error("[UD] Charts error:", e); }

    try {
      _renderTacticalTimeline(unit.recent_engagements || []);
    } catch (e) { console.error("[UD] Timeline error:", e); }
  };

  // =========================================================================
  // ASSETS: Pill Badges
  // =========================================================================
  function _renderAssets(assets) {
    const container = document.getElementById('udAssetsContainer');
    if (!container) return;

    container.innerHTML = '';
    if (!assets || !assets.length) {
      container.innerHTML = '<span style="color:#475569; font-size:0.7rem; font-style:italic;">No tactical assets identified</span>';
      return;
    }

    assets.forEach(function (name) {
      const pill = document.createElement('span');
      pill.className = 'ud-asset-pill';
      pill.innerHTML = '<i class="fa-solid fa-crosshairs"></i> ' + _escapeHtml(name);
      container.appendChild(pill);
    });
  }

  // =========================================================================
  // FREQUENCY BADGE
  // =========================================================================
  function _renderFrequencyBadge(label) {
    const el = document.getElementById('udEngFreq');
    if (!el) return;

    const cssClass = label === 'High' ? 'freq-high' : (label === 'Medium' ? 'freq-medium' : 'freq-low');
    el.innerHTML = '<span class="ud-freq-badge ' + cssClass + '">' + label + '</span>';
  }

  // =========================================================================
  // CHART.JS: Radar + Sparkline
  // =========================================================================
  function _renderCharts(avgTie, trend30d) {
    const chartRow = document.getElementById('udChartRow');
    if (!chartRow) return;

    // Ensure canvases exist without wiping the whole row if possible
    if (!document.getElementById('udRadarCanvas') || !document.getElementById('udSparkCanvas')) {
      chartRow.innerHTML =
        '<div class="ud-chart-box">' +
          '<span class="ud-chart-label">T.I.E. RADAR</span>' +
          '<canvas id="udRadarCanvas"></canvas>' +
        '</div>' +
        '<div class="ud-chart-box">' +
          '<span class="ud-chart-label">30-DAY ACTIVITY</span>' +
          '<canvas id="udSparkCanvas"></canvas>' +
        '</div>';
    }

    requestAnimationFrame(function () {
      try { _initRadarChart(avgTie); } catch (e) { console.error("[UD] Radar init fail:", e); }
      try { _initSparkline(trend30d); } catch (e) { console.error("[UD] Spark init fail:", e); }
    });
  }

  function _initRadarChart(avgTie) {
    if (typeof Chart === 'undefined') return;
    const canvas = document.getElementById('udRadarCanvas');
    if (!canvas) return;

    if (_radarChart) { _radarChart.destroy(); _radarChart = null; }

    const k = avgTie.kinetic || 0;
    const t = avgTie.target || 0;
    const e = avgTie.effect || 0;

    _radarChart = new Chart(canvas.getContext('2d'), {
      type: 'radar',
      data: {
        labels: ['Kinetic', 'Target', 'Effect'],
        datasets: [{
          data: [k, t, e],
          backgroundColor: 'rgba(245, 158, 11, 0.15)',
          borderColor: '#f59e0b',
          borderWidth: 2,
          pointBackgroundColor: '#f59e0b',
          pointRadius: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          r: {
            beginAtZero: true,
            max: 10,
            ticks: { display: false, stepSize: 2 },
            grid: { color: 'rgba(71, 85, 105, 0.3)' },
            pointLabels: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 9 } }
          }
        }
      }
    });
  }

  function _initSparkline(trend30d) {
    if (typeof Chart === 'undefined') return;
    const canvas = document.getElementById('udSparkCanvas');
    if (!canvas) return;

    if (_sparkChart) { _sparkChart.destroy(); _sparkChart = null; }

    const ctx = canvas.getContext('2d');
    const labels = Array(trend30d.length || 30).fill('');
    
    // Gradient
    const gradient = ctx.createLinearGradient(0, 0, 0, 80);
    gradient.addColorStop(0, 'rgba(245, 158, 11, 0.3)');
    gradient.addColorStop(1, 'rgba(245, 158, 11, 0.0)');

    _sparkChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          data: trend30d,
          fill: true,
          backgroundColor: gradient,
          borderColor: '#f59e0b',
          borderWidth: 1.5,
          tension: 0.4,
          pointRadius: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { x: { display: false }, y: { display: false, beginAtZero: true } }
      }
    });
  }

  // =========================================================================
  // TACTICAL TIMELINE
  // =========================================================================
  function _renderTacticalTimeline(engagements) {
    const container = document.getElementById('udEventsList');
    if (!container) return;

    container.innerHTML = '';
    container.className = 'ud-tactical-timeline';

    if (!engagements || !engagements.length) {
      container.className = '';
      container.innerHTML =
        '<div class="ud-empty-state">' +
          '<i class="fa-solid fa-satellite-dish"></i>' +
          '<span>No recent engagements recorded</span>' +
        '</div>';
      return;
    }

    engagements.forEach(function (eng) {
      const entry = document.createElement('div');
      entry.className = 'ud-tl-entry';

      const dateStr = (eng.date || 'Unknown').substring(0, 10);
      const title = eng.title || 'Untitled Event';
      const locHtml = eng.location ? `<div class="ud-tl-location"><i class="fa-solid fa-location-dot"></i>${_escapeHtml(eng.location)}</div>` : '';

      let actionsHtml = '<div class="ud-tl-actions">';
      if (eng.lat && eng.lon) {
        actionsHtml += `<button onclick="event.stopPropagation(); window._dossierFlyTo(${eng.lat},${eng.lon})" title="Fly to location"><i class="fa-solid fa-location-crosshairs"></i></button>`;
      }
      if (eng.url) {
        actionsHtml += `<a href="${_escapeHtml(eng.url)}" target="_blank" rel="noopener" title="Source"><i class="fa-solid fa-arrow-up-right-from-square"></i></a>`;
      }
      actionsHtml += '</div>';

      entry.innerHTML = `
        <div class="ud-tl-content">
          <div class="ud-tl-date">${_escapeHtml(dateStr)}</div>
          <div class="ud-tl-title">${_escapeHtml(title)}</div>
          ${locHtml}
        </div>
        ${actionsHtml}
      `;

      container.appendChild(entry);
    });
  }

  // =========================================================================
  // LEAFLET FLYTO
  // =========================================================================
  window._dossierFlyTo = function (lat, lon) {
    const modal = document.getElementById('unitModal');
    if (modal) modal.style.display = 'none';

    const mapRef = window.map || window.leafletMap;
    if (mapRef && typeof mapRef.flyTo === 'function') {
      mapRef.flyTo([lat, lon], 12, { duration: 1.2 });
    }
  };

  function _escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
  }

})();
