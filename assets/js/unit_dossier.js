/**
 * UNIT DOSSIER CARD — Frontend Rendering Module
 * Renders high-density tactical analytics from units.json dossier fields.
 * Dependencies: Chart.js (loaded via CDN), Leaflet (global `map` object)
 */
(function () {
  'use strict';

  // Chart.js instance refs (destroyed on re-render to prevent memory leaks)
  let _radarChart = null;
  let _sparkChart = null;

  /**
   * Main entry point: populate the dossier analytics section inside the unit modal.
   * Called from openUnitModal after the base fields are populated.
   * @param {Object} unit — unit object from units.json (with dossier fields)
   */
  window.renderUnitDossierAnalytics = function (unit) {
    _renderAssets(unit.assets_detected || []);
    _renderFrequencyBadge(unit.engagement_freq_label || 'Low');
    _renderCharts(unit.avg_tie || {}, unit.engagement_trend_30d || []);
    _renderTacticalTimeline(unit.recent_engagements || []);
  };

  // =========================================================================
  // ASSETS: Pill Badges
  // =========================================================================
  function _renderAssets(assets) {
    const container = document.getElementById('udAssetsContainer');
    if (!container) return;

    container.innerHTML = '';
    if (!assets.length) {
      container.innerHTML = '<span style="color:#475569; font-size:0.7rem;">No assets detected</span>';
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
  // CHART.JS: Radar (T.I.E.) + Sparkline (30-day trend)
  // =========================================================================
  function _renderCharts(avgTie, trend30d) {
    // Inject chart containers if not present
    var chartRow = document.getElementById('udChartRow');
    if (!chartRow) {
      // Find the sparkline placeholder and replace it
      var sparkPlaceholder = document.getElementById('udSparkline');
      if (sparkPlaceholder) {
        chartRow = document.createElement('div');
        chartRow.id = 'udChartRow';
        chartRow.className = 'ud-chart-row';
        chartRow.innerHTML =
          '<div class="ud-chart-box">' +
            '<span class="ud-chart-label">T.I.E. RADAR</span>' +
            '<canvas id="udRadarCanvas"></canvas>' +
          '</div>' +
          '<div class="ud-chart-box">' +
            '<span class="ud-chart-label">30-DAY ACTIVITY</span>' +
            '<canvas id="udSparkCanvas"></canvas>' +
          '</div>';
        sparkPlaceholder.parentNode.replaceChild(chartRow, sparkPlaceholder);
      }
    } else {
      // Ensure canvases exist for re-render
      if (!document.getElementById('udRadarCanvas')) {
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
    }

    // Wait for next frame to ensure DOM is ready
    requestAnimationFrame(function () {
      _initRadarChart(avgTie);
      _initSparkline(trend30d);
    });
  }

  function _initRadarChart(avgTie) {
    if (typeof Chart === 'undefined') return;

    var canvas = document.getElementById('udRadarCanvas');
    if (!canvas) return;
    var ctx = canvas.getContext('2d');

    // Destroy previous instance
    if (_radarChart) {
      _radarChart.destroy();
      _radarChart = null;
    }

    var k = avgTie.kinetic || 0;
    var t = avgTie.target || 0;
    var e = avgTie.effect || 0;

    _radarChart = new Chart(ctx, {
      type: 'radar',
      data: {
        labels: ['Kinetic', 'Target', 'Effect'],
        datasets: [{
          data: [k, t, e],
          backgroundColor: 'rgba(245, 158, 11, 0.15)',
          borderColor: '#f59e0b',
          borderWidth: 2,
          pointBackgroundColor: '#f59e0b',
          pointBorderColor: '#0f172a',
          pointBorderWidth: 2,
          pointRadius: 4,
          pointHoverRadius: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0f172a',
            titleColor: '#f59e0b',
            bodyColor: '#e2e8f0',
            borderColor: '#334155',
            borderWidth: 1,
            padding: 8,
            titleFont: { family: 'JetBrains Mono', size: 10 },
            bodyFont: { family: 'JetBrains Mono', size: 10 }
          }
        },
        scales: {
          r: {
            beginAtZero: true,
            max: 10,
            ticks: {
              display: false,
              stepSize: 2
            },
            grid: {
              color: 'rgba(71, 85, 105, 0.3)',
              lineWidth: 1
            },
            angleLines: {
              color: 'rgba(71, 85, 105, 0.2)'
            },
            pointLabels: {
              color: '#94a3b8',
              font: { family: 'JetBrains Mono', size: 9, weight: '600' }
            }
          }
        }
      }
    });
  }

  function _initSparkline(trend30d) {
    if (typeof Chart === 'undefined') return;

    var canvas = document.getElementById('udSparkCanvas');
    if (!canvas) return;
    var ctx = canvas.getContext('2d');

    if (_sparkChart) {
      _sparkChart.destroy();
      _sparkChart = null;
    }

    // Build labels (day indices)
    var labels = [];
    for (var i = 0; i < 30; i++) labels.push('');

    // Gradient fill
    var gradient = ctx.createLinearGradient(0, 0, 0, canvas.parentElement.clientHeight || 100);
    gradient.addColorStop(0, 'rgba(245, 158, 11, 0.35)');
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
          pointRadius: 0,
          pointHitRadius: 6
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: true,
            backgroundColor: '#0f172a',
            titleColor: '#f59e0b',
            bodyColor: '#e2e8f0',
            borderColor: '#334155',
            borderWidth: 1,
            padding: 6,
            bodyFont: { family: 'JetBrains Mono', size: 9 },
            callbacks: {
              title: function () { return ''; },
              label: function (ctx) {
                return ctx.parsed.y + ' events';
              }
            }
          }
        },
        scales: {
          x: { display: false },
          y: {
            display: false,
            beginAtZero: true
          }
        }
      }
    });
  }

  // =========================================================================
  // TACTICAL TIMELINE (vertical, max 5 entries)
  // =========================================================================
  function _renderTacticalTimeline(engagements) {
    var container = document.getElementById('udEventsList');
    if (!container) return;

    container.innerHTML = '';
    container.className = 'ud-tactical-timeline';

    if (!engagements || !engagements.length) {
      container.className = '';
      container.innerHTML =
        '<div class="ud-empty-state">' +
          '<i class="fa-solid fa-satellite-dish"></i>' +
          '<span>No recent engagements linked</span>' +
        '</div>';
      return;
    }

    engagements.forEach(function (eng) {
      var entry = document.createElement('div');
      entry.className = 'ud-tl-entry';

      // Date
      var dateStr = eng.date ? eng.date.substring(0, 10) : 'Unknown';

      // Title (truncate)
      var title = eng.title || 'Untitled Event';

      // Location
      var locHtml = '';
      if (eng.location) {
        locHtml = '<div class="ud-tl-location"><i class="fa-solid fa-location-dot"></i>' + _escapeHtml(eng.location) + '</div>';
      }

      // Action icons
      var actionsHtml = '<div class="ud-tl-actions">';

      // FlyTo icon (only if lat/lon exist)
      if (eng.lat && eng.lon) {
        actionsHtml +=
          '<button onclick="event.stopPropagation(); window._dossierFlyTo(' + eng.lat + ',' + eng.lon + ')" title="Fly to location">' +
            '<i class="fa-solid fa-location-crosshairs"></i>' +
          '</button>';
      }

      // External link icon (only if url exists)
      if (eng.url) {
        actionsHtml +=
          '<a href="' + _escapeHtml(eng.url) + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="Open source">' +
            '<i class="fa-solid fa-arrow-up-right-from-square"></i>' +
          '</a>';
      }

      actionsHtml += '</div>';

      entry.innerHTML =
        '<div class="ud-tl-content">' +
          '<div class="ud-tl-date">' + _escapeHtml(dateStr) + '</div>' +
          '<div class="ud-tl-title">' + _escapeHtml(title) + '</div>' +
          locHtml +
        '</div>' +
        actionsHtml;

      container.appendChild(entry);
    });
  }

  // =========================================================================
  // LEAFLET FLYTO (zoom 12)
  // =========================================================================
  window._dossierFlyTo = function (lat, lon) {
    // Close modal first for unobstructed view
    var modal = document.getElementById('unitModal');
    if (modal) modal.style.display = 'none';

    // Access the global Leaflet map instance
    var mapRef = window.map || window.leafletMap;
    if (mapRef && typeof mapRef.flyTo === 'function') {
      mapRef.flyTo([lat, lon], 12, { duration: 1.2 });
    }
  };

  // =========================================================================
  // UTILS
  // =========================================================================
  function _escapeHtml(str) {
    if (!str) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
  }

})();
