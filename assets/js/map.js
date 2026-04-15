// ============================================
// MAP.JS - RESTRUCTURED WITH PROPER INITIALIZATION
// ============================================

(function () {
  'use strict';
  console.log("Ã°Å¸Å¡â‚¬ MAP.JS UPDATED VERSION LOADED Ã°Å¸Å¡â‚¬");

  // ============================================
  // 1. GLOBAL STATE (Declared First)
  // ============================================
  let map;
  let eventsLayer;
  let heatLayer = null;
  let isHeatmapMode = false;
  let currentFrontlineLayer = null;
  let historicalFrontlineLayer = null;
  let firmsLayer = null;
  let unitsLayer = null;
  let narrativesLayer = null; // Strategic Context layer
  let owlLayer = null; // Project Owl Layer
  let strategicCampaignsMode = false;
  let strategicCanvasLayer = null;
  let strategicHullLayer = null;
  let strategicCampaignDefinitions = [];
  let strategicCampaignReports = [];
  let selectedStrategicCampaignId = null;
  const strategicSparklineCharts = {};
  const strategicCanvasRenderer = L.canvas();

  window.allEventsData = [];
  window.globalEvents = [];
  window.currentFilteredEvents = [];
  window.hideLowReputation = true;
  window.sectorAnomalySet = new Set();
  window.tacticalSectorsIndex = new Map();
  window.axisThermalFeatures = [];
  window.axisThermalMetadata = null;
  window.currentAxisSector = '';

  let mapDates = []; // Historical dates index

  // Tactical Time Command State
  let tacticalTimeWindowHours = 0; // 0 = ALL (no filter)
  let tacticalPersistence = false;  // Default: OFF
  let axisThermalPromise = null;
  let axisPanelMode = 'expanded';
  let axisPanelDismissedSector = '';

  // Central helper to define what is civilian
  function isCivilianEvent(e) {
    // Joins all text fields to search for keywords
    const fullText = (e.category + ' ' + e.type + ' ' + e.location_precision + ' ' + e.filters).toUpperCase();

    // Words identifying a NON strictly military/kinetic event
    const civKeywords = ['CIVIL', 'POLITIC', 'ECONOM', 'HUMANITAR', 'DIPLOMA', 'ACCIDENT', 'STATEMENT'];

    // If one of these words is found, it is civilian
    if (civKeywords.some(k => fullText.includes(k))) return true;

    // Optional: excludes everything not in UA/RU (Rough Geofencing)
    // if (e.lat < 44 || e.lat > 57 || e.lon < 22 || e.lon > 50) return true; 

    return false;
  }

  // ============================================
  // 2. CONFIGURATION
  // ============================================
  const impactColors = {
    'critical': '#ef4444',
    'high': '#f97316',
    'medium': '#eab308',
    'low': '#64748b'
  };

  const typeIcons = {
    'drone': 'fa-plane-up',
    'missile': 'fa-rocket',
    'artillery': 'fa-bomb',
    'energy': 'fa-bolt',
    'fire': 'fa-fire',
    'naval': 'fa-anchor',
    'cultural': 'fa-landmark',
    'eco': 'fa-leaf',
    'default': 'fa-crosshairs'
  };

  const CONFLICT_TERRITORY_WHITELIST = [
    {
      name: 'UA_LAND',
      geometry: {
        type: 'Polygon',
        coordinates: [[
          [22.12, 52.24],
          [23.52, 51.56],
          [24.25, 51.93],
          [26.10, 51.84],
          [28.60, 51.58],
          [30.92, 52.08],
          [32.62, 52.11],
          [34.76, 51.74],
          [36.58, 50.83],
          [38.95, 50.09],
          [39.80, 49.00],
          [39.72, 47.95],
          [38.23, 47.10],
          [37.40, 46.02],
          [36.62, 45.16],
          [34.47, 45.31],
          [33.44, 45.72],
          [31.82, 45.27],
          [30.94, 45.50],
          [29.65, 45.20],
          [29.31, 45.45],
          [28.22, 45.47],
          [28.71, 46.63],
          [29.67, 46.83],
          [29.63, 47.20],
          [29.24, 47.98],
          [28.67, 48.16],
          [27.36, 48.66],
          [26.26, 48.62],
          [24.98, 48.61],
          [23.19, 49.44],
          [22.64, 50.41],
          [22.12, 52.24]
        ]]
      }
    },
    {
      name: 'RU_LAND',
      geometry: {
        type: 'Polygon',
        coordinates: [[
          [30.85, 55.95],
          [32.95, 56.05],
          [36.10, 55.75],
          [39.95, 54.55],
          [40.35, 52.35],
          [40.73, 50.70],
          [40.55, 48.55],
          [40.30, 46.60],
          [39.35, 45.20],
          [37.05, 44.85],
          [36.05, 45.60],
          [36.85, 47.35],
          [38.72, 48.55],
          [39.28, 50.05],
          [38.84, 51.18],
          [37.52, 51.88],
          [35.32, 52.82],
          [33.10, 53.82],
          [31.30, 54.35],
          [30.85, 55.95]
        ]]
      }
    }
  ];

  const AXIS_THERMAL_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
  const AXIS_THERMAL_MATCH_DISTANCE_KM = 3;
  const AXIS_THERMAL_MATCH_WINDOW_MS = 36 * 60 * 60 * 1000;

  // ============================================
  // 3. HELPER FUNCTIONS (Define Before Use)
  // ============================================

  function getColor(val) {
    const v = val || 0.2;
    if (v >= 0.8) return impactColors.critical;
    if (v >= 0.6) return impactColors.high;
    if (v >= 0.4) return impactColors.medium;
    return impactColors.low;
  }

  function getIconClass(type) {
    if (!type) return typeIcons.default;
    const t = type.toLowerCase();
    for (const [key, icon] of Object.entries(typeIcons)) {
      if (t.includes(key)) return icon;
    }
    return typeIcons.default;
  }

  function isFinitePoint(point) {
    return Array.isArray(point)
      && point.length >= 2
      && Number.isFinite(point[0])
      && Number.isFinite(point[1]);
  }

  function isPointOnSegment(point, start, end) {
    const [x, y] = point;
    const [x1, y1] = start;
    const [x2, y2] = end;
    const cross = (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1);
    if (Math.abs(cross) > 1e-10) return false;

    const dot = (x - x1) * (x2 - x1) + (y - y1) * (y2 - y1);
    if (dot < 0) return false;

    const segmentLengthSquared = (x2 - x1) ** 2 + (y2 - y1) ** 2;
    return dot <= segmentLengthSquared;
  }

  function pointInRing(point, ring) {
    if (!Array.isArray(ring) || ring.length < 3) return false;

    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const current = ring[i];
      const previous = ring[j];
      if (!isFinitePoint(current) || !isFinitePoint(previous)) continue;

      if (isPointOnSegment(point, current, previous)) {
        return true;
      }

      const intersects = ((current[1] > point[1]) !== (previous[1] > point[1]))
        && (point[0] < ((previous[0] - current[0]) * (point[1] - current[1])) / (previous[1] - current[1]) + current[0]);
      if (intersects) inside = !inside;
    }

    return inside;
  }

  function pointInPolygonCoordinates(point, polygonCoords) {
    if (!Array.isArray(polygonCoords) || polygonCoords.length === 0) return false;
    if (!pointInRing(point, polygonCoords[0])) return false;

    for (let i = 1; i < polygonCoords.length; i++) {
      if (pointInRing(point, polygonCoords[i])) {
        return false;
      }
    }

    return true;
  }

  function pointInGeometry(point, geometry) {
    if (!isFinitePoint(point) || !geometry || !geometry.type || !geometry.coordinates) return false;

    if (geometry.type === 'Polygon') {
      return pointInPolygonCoordinates(point, geometry.coordinates);
    }

    if (geometry.type === 'MultiPolygon') {
      return geometry.coordinates.some(polygonCoords => pointInPolygonCoordinates(point, polygonCoords));
    }

    return false;
  }

  function isInConflictTerritory(lat, lon) {
    const point = [Number(lon), Number(lat)];
    return CONFLICT_TERRITORY_WHITELIST.some(area => pointInGeometry(point, area.geometry));
  }

  function getSectorEntries(sectorName) {
    if (!sectorName || !(window.tacticalSectorsIndex instanceof Map)) return [];
    return window.tacticalSectorsIndex.get(sectorName) || [];
  }

  function buildSectorBounds(entries) {
    return entries.reduce((combinedBounds, entry) => {
      if (!entry || !entry.bounds) return combinedBounds;
      if (!combinedBounds) {
        return entry.bounds.clone ? entry.bounds.clone() : L.latLngBounds(entry.bounds);
      }
      combinedBounds.extend(entry.bounds);
      return combinedBounds;
    }, null);
  }

  function isEventInsideSector(event, sectorName) {
    if (!sectorName) return true;

    const lat = Number(event && event.lat);
    const lon = Number(event && event.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;

    const sectorEntries = getSectorEntries(sectorName);
    if (sectorEntries.length === 0) {
      return event.operational_sector === sectorName;
    }

    const point = [lon, lat];
    const latLng = L.latLng(lat, lon);
    return sectorEntries.some(entry => entry.bounds.contains(latLng) && pointInGeometry(point, entry.geometry));
  }

  function getNarrativePolygonLatLngs(geometry) {
    if (!geometry || !geometry.type || !geometry.coordinates) return null;

    if (geometry.type === 'Polygon') {
      return geometry.coordinates[0].map(c => [Number(c[1]), Number(c[0])]);
    }

    if (geometry.type === 'MultiPolygon' && geometry.coordinates.length > 0) {
      return geometry.coordinates[0][0].map(c => [Number(c[1]), Number(c[0])]);
    }

    return null;
  }

  function normalizeNarrativeCentroid(centroid, geometry) {
    if (!Array.isArray(centroid) || centroid.length < 2) return null;

    const a = [Number(centroid[0]), Number(centroid[1])]; // [lat, lon]
    const b = [Number(centroid[1]), Number(centroid[0])]; // [lon, lat] -> [lat, lon]
    const polygonLatLngs = getNarrativePolygonLatLngs(geometry);

    const isValidLatLng = candidate => Number.isFinite(candidate[0])
      && Number.isFinite(candidate[1])
      && Math.abs(candidate[0]) <= 90
      && Math.abs(candidate[1]) <= 180;

    if (!polygonLatLngs || polygonLatLngs.length < 3) {
      return isValidLatLng(a) ? a : (isValidLatLng(b) ? b : null);
    }

    const bounds = L.latLngBounds(polygonLatLngs);
    const paddedBounds = bounds.pad(2);
    const aInside = isValidLatLng(a) && paddedBounds.contains(L.latLng(a[0], a[1]));
    const bInside = isValidLatLng(b) && paddedBounds.contains(L.latLng(b[0], b[1]));

    if (aInside && !bInside) return a;
    if (bInside && !aInside) return b;
    if (aInside) return a;
    if (isValidLatLng(a)) return a;
    if (isValidLatLng(b)) return b;

    const center = bounds.getCenter();
    return [center.lat, center.lng];
  }

  function toFiniteNumber(value, fallback = 0) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function normalizeAxisScore(value, fallback = 50) {
    if (value == null || value === '') return fallback;

    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (!normalized) return fallback;
      if (normalized === 'high' || normalized === 'h') return 85;
      if (normalized === 'medium' || normalized === 'med' || normalized === 'm' || normalized === 'nominal' || normalized === 'n') return 60;
      if (normalized === 'low' || normalized === 'l') return 30;
    }

    return clamp(toFiniteNumber(value, fallback), 0, 100);
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function haversineKm(lat1, lon1, lat2, lon2) {
    const r = 6371;
    const toRadians = value => value * (Math.PI / 180);
    const dLat = toRadians(lat2 - lat1);
    const dLon = toRadians(lon2 - lon1);
    const a = Math.sin(dLat / 2) ** 2
      + Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
    return 2 * r * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  const AXIS_DOCTRINE_PATTERNS = {
    manoeuvre: /(MANOEUVRE|MANOUVRE|MANEUVER|ADVANCE|CAPTUR|SEIZ|STORM|ASSAULT|BREACH|RAID|RECAPTUR|FOOTHOLD|CROSSING|ENCIRCLE|OVERWHELM|LIBERAT|WITHDREW|ROUT)/,
    shaping: /(SHAPING|LOGISTICS|SUPPLY|DEPOT|WAREHOUSE|AMMUNITION|AMMO|FUEL|RAIL|TRAIN|BRIDGE|REFINERY|ENERGY|SUBSTATION|POWER|AIR_DEFENSE|RADAR|COMMAND|COMMUNICATION|INDUSTR|PORT|AIRFIELD|REAR AREA|INTERDICTION)/
  };

  const AXIS_MOVEMENT_PATTERN = /(ADVANCE|CAPTUR|SEIZ|STORM|ASSAULT|BREACH|FOOTHOLD|CROSSING|RECAPTUR|WITHDREW|LIBERAT|ROUT|COLLAPSE|CONTROL OF THE POSITION|CONTROL OF POSITION)/;
  const AXIS_LOGISTICS_PATTERN = /(LOGISTICS|SUPPLY|DEPOT|WAREHOUSE|AMMUNITION|AMMO|FUEL|RAIL|TRAIN|CONVOY|BRIDGE|REFINERY|PORT|TERMINAL|SUBSTATION|POWER|ENERGY|PIPELINE|TANKER|WAREHOUSE)/;
  const AXIS_EQUIPMENT_SIGNALS = [
    { label: 'Armor', regex: /\b(TANK|T-\d+|LEOPARD|ABRAMS|BRADLEY|BMP|BTR|IFV|APC|ARMOU?RED VEHICLE|ARMOU?R)\b/, weight: 2.5 },
    { label: 'Artillery', regex: /\b(ARTILLERY|HOWITZER|MLRS|GRAD|MORTAR|SELF-PROPELLED|LAUNCHER)\b/, weight: 2.1 },
    { label: 'Air Defense', regex: /\b(AIR DEFENSE|AIR_DEFENSE|S-300|S-400|BUK|PATRIOT|SAM|RADAR)\b/, weight: 2.4 },
    { label: 'Aviation', regex: /\b(SU-\d+|MIG-\d+|AIRCRAFT|HELICOPTER|KA-\d+|MI-\d+|DRONE)\b/, weight: 2.2 },
    { label: 'Naval', regex: /\b(SHIP|VESSEL|BOAT|FRIGATE|CORVETTE|TANKER|SUBMARINE|NAVAL)\b/, weight: 2.0 }
  ];

  function buildAxisEventText(event) {
    return [
      event.classification,
      event.category,
      event.target_type,
      event.title,
      event.description,
      event.ai_reasoning
    ].filter(Boolean).join(' ').toUpperCase();
  }

  function inferDoctrineBucket(event, eventText) {
    const text = eventText || buildAxisEventText(event);
    if (AXIS_DOCTRINE_PATTERNS.manoeuvre.test(text)) return 'manoeuvre';
    if (AXIS_DOCTRINE_PATTERNS.shaping.test(text)) return 'shaping';
    return 'attrition';
  }

  function isLogisticsEvent(event, eventText) {
    const text = eventText || buildAxisEventText(event);
    return AXIS_LOGISTICS_PATTERN.test(text);
  }

  function extractFrontlineShiftKm(event, doctrine, eventText) {
    if (doctrine !== 'manoeuvre') {
      return { shiftKm: 0, hasEvidence: false };
    }

    const text = eventText || buildAxisEventText(event);
    const distanceMatches = AXIS_MOVEMENT_PATTERN.test(text)
      ? Array.from(text.matchAll(/(\d+(?:\.\d+)?)\s*(?:KM|KILOMETERS?|KILOMETRES?)/g))
        .map(match => parseFloat(match[1]))
        .filter(value => Number.isFinite(value) && value <= 80)
      : [];

    if (distanceMatches.length > 0) {
      return {
        shiftKm: clamp(distanceMatches[0], 0.4, 30),
        hasEvidence: true
      };
    }

    return { shiftKm: 0, hasEvidence: false };
  }

  function analyzeEquipmentSignals(eventText, tieScore) {
    return AXIS_EQUIPMENT_SIGNALS.reduce((acc, signal) => {
      if (signal.regex.test(eventText)) {
        acc.score += signal.weight + (tieScore * 0.18);
        acc.tags[signal.label] = (acc.tags[signal.label] || 0) + 1;
      }
      return acc;
    }, { score: 0, tags: {} });
  }

  function getAxisFogState(fogIndex) {
    if (fogIndex >= 72) return 'critical';
    if (fogIndex >= 48) return 'high';
    if (fogIndex >= 28) return 'mid';
    return 'low';
  }

  function describeAxisVolume(totalEvents) {
    if (totalEvents >= 100) return 'Dense operational burden';
    if (totalEvents >= 40) return 'Sustained contact picture';
    if (totalEvents >= 15) return 'Moderate signal density';
    if (totalEvents > 0) return 'Sparse but active feed';
    return 'No qualifying events in scope';
  }

  function describeFrontlineFriction(friction, frontlineShiftKm) {
    if (frontlineShiftKm < 1) return 'Brutal stalemate with minimal territorial change.';
    if (friction >= 18) return 'Grinding frontage under heavy kinetic load.';
    if (friction >= 9) return 'Contested movement with costly advances.';
    return 'Mobility exceeds kinetic drag in this axis.';
  }

  function parseAxisThermalTimestamp(properties) {
    const dateValue = properties && properties.acq_date ? String(properties.acq_date).trim() : '';
    if (!dateValue) return NaN;

    const timeValue = String(properties && properties.acq_time != null ? properties.acq_time : '')
      .replace(/\D/g, '')
      .padStart(4, '0')
      .slice(-4);
    const hours = timeValue.slice(0, 2) || '00';
    const minutes = timeValue.slice(2, 4) || '00';
    return Date.parse(`${dateValue}T${hours}:${minutes}:00Z`);
  }

  function getVisibleAxisThermalFeatures(referenceTime = Date.now()) {
    const cutoff = referenceTime - AXIS_THERMAL_MAX_AGE_MS;
    return (window.axisThermalFeatures || []).filter(feature => Number.isFinite(feature.timestampMs)
      && feature.timestampMs >= cutoff);
  }

  function formatAxisCompactValue(value, digits = 1) {
    return Number.isFinite(value) ? value.toFixed(digits) : 'N/A';
  }

  function updateAxisPanelUIState(hasSector) {
    const panel = document.getElementById('axis-stats-panel');
    const restoreButton = document.getElementById('axis-panel-restore');
    const restoreLabel = document.getElementById('axisPanelRestoreLabel');
    const minimizeButton = document.getElementById('axisPanelMinimize');
    const minimizeIcon = minimizeButton ? minimizeButton.querySelector('i') : null;

    if (!panel || !restoreButton) return;

    const showPanel = hasSector && axisPanelMode !== 'hidden';
    panel.classList.toggle('is-visible', showPanel);
    panel.classList.toggle('is-collapsed', hasSector && axisPanelMode === 'collapsed');
    panel.setAttribute('aria-hidden', showPanel ? 'false' : 'true');

    const showRestore = hasSector && axisPanelMode === 'hidden';
    restoreButton.hidden = !showRestore;
    restoreButton.classList.toggle('is-visible', showRestore);

    if (restoreLabel) {
      restoreLabel.textContent = window.currentAxisSector
        ? `Restore ${window.currentAxisSector}`
        : 'Restore Axis Analytics';
    }

    if (minimizeButton) {
      const isCollapsed = axisPanelMode === 'collapsed';
      minimizeButton.setAttribute('aria-label', isCollapsed ? 'Expand axis analytics' : 'Minimize axis analytics');
      minimizeButton.title = isCollapsed ? 'Expand axis analytics' : 'Minimize axis analytics';
      if (minimizeIcon) {
        minimizeIcon.className = isCollapsed
          ? 'fa-solid fa-up-right-and-down-left-from-center'
          : 'fa-solid fa-window-minimize';
      }
    }
  }

  function initAxisStatsPanelControls() {
    const minimizeButton = document.getElementById('axisPanelMinimize');
    const closeButton = document.getElementById('axisPanelClose');
    const restoreButton = document.getElementById('axis-panel-restore');

    if (minimizeButton && !minimizeButton.dataset.bound) {
      minimizeButton.dataset.bound = 'true';
      minimizeButton.addEventListener('click', () => {
        if (!window.currentAxisSector) return;
        axisPanelMode = axisPanelMode === 'collapsed' ? 'expanded' : 'collapsed';
        updateAxisPanelUIState(true);
        syncAxisHudOffset();
      });
    }

    if (closeButton && !closeButton.dataset.bound) {
      closeButton.dataset.bound = 'true';
      closeButton.addEventListener('click', () => {
        if (!window.currentAxisSector) return;
        axisPanelDismissedSector = window.currentAxisSector;
        axisPanelMode = 'hidden';
        updateAxisPanelUIState(true);
        syncAxisHudOffset();
      });
    }

    if (restoreButton && !restoreButton.dataset.bound) {
      restoreButton.dataset.bound = 'true';
      restoreButton.addEventListener('click', () => {
        axisPanelDismissedSector = '';
        axisPanelMode = 'expanded';
        updateAxisPanelUIState(Boolean(window.currentAxisSector));
        syncAxisHudOffset();

        if (window.currentAxisSector) {
          renderAxisStatsPanel(computeAxisMetrics(
            window.currentAxisSector,
            Array.isArray(window.currentFilteredEvents) ? window.currentFilteredEvents : window.globalEvents
          ));
        }
      });
    }

    updateAxisPanelUIState(Boolean(window.currentAxisSector));
  }

  function loadAxisThermalFeatures() {
    if (axisThermalPromise) {
      return axisThermalPromise.then(() => getVisibleAxisThermalFeatures());
    }

    axisThermalPromise = fetch('assets/data/thermal_firms.geojson')
      .then(response => response.json())
      .then(data => {
        const features = Array.isArray(data.features) ? data.features.map(feature => {
          const coords = feature.geometry && Array.isArray(feature.geometry.coordinates)
            ? feature.geometry.coordinates
            : [];
          return {
            lat: Number(coords[1]),
            lon: Number(coords[0]),
            properties: feature.properties || {},
            timestampMs: parseAxisThermalTimestamp(feature.properties || {})
          };
        }).filter(feature => Number.isFinite(feature.lat)
          && Number.isFinite(feature.lon)
          && Number.isFinite(feature.timestampMs)
          && isInConflictTerritory(feature.lat, feature.lon)) : [];

        window.axisThermalFeatures = features;
        window.axisThermalMetadata = data.metadata || null;
        return getVisibleAxisThermalFeatures();
      })
      .catch(error => {
        console.warn('Axis thermal support data unavailable:', error);
        window.axisThermalFeatures = [];
        window.axisThermalMetadata = null;
        axisThermalPromise = null;
        return [];
      });

    return axisThermalPromise;
  }

  function syncAxisHudOffset() {
    const wrapper = document.querySelector('.map-container-wrapper');
    const hud = document.getElementById('tacticalHudContainer');
    const topbar = wrapper ? wrapper.querySelector('.map-topbar') : null;
    if (!wrapper || !hud) return;
    wrapper.style.setProperty('--axis-hud-left', '15px');
    wrapper.style.setProperty('--axis-hud-top', `${(topbar ? topbar.offsetHeight : 0) + 15}px`);
    wrapper.style.setProperty('--axis-hud-offset', `${hud.offsetHeight + 16}px`);
  }

  function animateAxisWidth(element, percentage) {
    if (!element) return;
    const safeWidth = clamp(percentage, 0, 100);
    element.style.width = '0%';
    window.requestAnimationFrame(() => {
      element.style.width = `${safeWidth.toFixed(1)}%`;
    });
  }

  function setAxisGauge(element, percentage, state) {
    if (!element) return;

    const safeValue = clamp(percentage, 0, 100);
    const colorMap = {
      low: '#22c55e',
      mid: '#f59e0b',
      high: '#f97316',
      critical: '#ef4444'
    };

    element.style.setProperty('--gauge-angle', `${(safeValue / 100) * 360}deg`);
    element.style.setProperty('--gauge-color', colorMap[state] || '#f59e0b');
  }

  function computeAxisMetrics(sectorName, sourceEvents) {
    if (!sectorName) return null;

    const baseEvents = Array.isArray(sourceEvents) ? sourceEvents : window.globalEvents;
    const sectorEvents = baseEvents.filter(event => isEventInsideSector(event, sectorName));
    const sectorThermals = getVisibleAxisThermalFeatures().filter(point => isEventInsideSector(point, sectorName));
    const recentEventCutoff = Date.now() - AXIS_THERMAL_MAX_AGE_MS;
    const aggregate = sectorEvents.reduce((acc, event) => {
      const eventText = buildAxisEventText(event);
      const doctrine = inferDoctrineBucket(event, eventText);
      const tieScore = clamp(
        event.tie_total != null ? toFiniteNumber(event.tie_total) / 10
          : event.tie_score != null ? toFiniteNumber(event.tie_score) / 10
            : toFiniteNumber(event.intensity_score),
        0,
        10
      );

      acc.totalEvents += 1;
      acc.tieSum += tieScore;
      acc.doctrine[doctrine] += 1;

      const equipment = analyzeEquipmentSignals(eventText, tieScore);
      acc.equipmentScore += equipment.score;
      if (equipment.score > 0) acc.equipmentEvidence += 1;
      Object.entries(equipment.tags).forEach(([label, count]) => {
        acc.equipmentTags[label] = (acc.equipmentTags[label] || 0) + count;
      });

      const reliabilityScore = normalizeAxisScore(
        event.reliability_score != null ? event.reliability_score : event.reliability,
        50
      );
      const confidenceScore = normalizeAxisScore(
        event.confidence != null ? event.confidence : event.source_reputation_score,
        reliabilityScore
      );
      const cohesionScore = (reliabilityScore + confidenceScore) / 2;
      acc.cohesionSum += cohesionScore;
      acc.cohesionSquared += cohesionScore * cohesionScore;

      if (isLogisticsEvent(event, eventText)) acc.logisticsHits += 1;
      if (doctrine === 'shaping') acc.shapingEvents += 1;

      const movementEvidence = extractFrontlineShiftKm(event, doctrine, eventText);
      if (movementEvidence.hasEvidence) {
        acc.frontlineShiftKm += movementEvidence.shiftKm;
        acc.frontlineEvidence += 1;
      }

      if (doctrine === 'attrition' || doctrine === 'shaping') {
        const eventTimestamp = Number.isFinite(Number(event.timestamp)) ? Number(event.timestamp) : NaN;
        const lat = Number(event.lat);
        const lon = Number(event.lon);

        if (Number.isFinite(lat)
          && Number.isFinite(lon)
          && Number.isFinite(eventTimestamp)
          && eventTimestamp >= recentEventCutoff) {
          acc.thermalEligible += 1;
        }

        if (Number.isFinite(lat)
          && Number.isFinite(lon)
          && Number.isFinite(eventTimestamp)
          && eventTimestamp >= recentEventCutoff
          && sectorThermals.some(point => Math.abs(eventTimestamp - point.timestampMs) <= AXIS_THERMAL_MATCH_WINDOW_MS
            && haversineKm(lat, lon, point.lat, point.lon) <= AXIS_THERMAL_MATCH_DISTANCE_KM)) {
          acc.thermalVerified += 1;
        }
      }

      return acc;
    }, {
      totalEvents: 0,
      tieSum: 0,
      doctrine: { manoeuvre: 0, shaping: 0, attrition: 0 },
      equipmentScore: 0,
      equipmentEvidence: 0,
      equipmentTags: {},
      cohesionSum: 0,
      cohesionSquared: 0,
      logisticsHits: 0,
      shapingEvents: 0,
      frontlineShiftKm: 0,
      frontlineEvidence: 0,
      thermalEligible: 0,
      thermalVerified: 0
    });

    const doctrineLabels = {
      manoeuvre: 'Manoeuvre',
      shaping: 'Shaping',
      attrition: 'Attrition'
    };
    const dominantDoctrineKey = aggregate.totalEvents
      ? Object.entries(aggregate.doctrine).sort((left, right) => right[1] - left[1])[0][0]
      : null;
    const averageTie = aggregate.totalEvents ? aggregate.tieSum / aggregate.totalEvents : 0;
    const averageCohesion = aggregate.totalEvents ? aggregate.cohesionSum / aggregate.totalEvents : 0;
    const cohesionVariance = aggregate.totalEvents
      ? Math.max(0, (aggregate.cohesionSquared / aggregate.totalEvents) - (averageCohesion ** 2))
      : 0;
    const fogIndex = aggregate.totalEvents
      ? clamp(((100 - averageCohesion) * 0.72) + (Math.sqrt(cohesionVariance) * 1.15), 0, 100)
      : 0;
    const doctrinePercentages = {
      manoeuvre: aggregate.totalEvents ? (aggregate.doctrine.manoeuvre / aggregate.totalEvents) * 100 : 0,
      shaping: aggregate.totalEvents ? (aggregate.doctrine.shaping / aggregate.totalEvents) * 100 : 0,
      attrition: aggregate.totalEvents ? (aggregate.doctrine.attrition / aggregate.totalEvents) * 100 : 0
    };
    const logisticsRatio = aggregate.shapingEvents ? aggregate.logisticsHits / aggregate.shapingEvents : 0;
    const logisticsShare = aggregate.shapingEvents
      ? clamp((aggregate.logisticsHits / aggregate.shapingEvents) * 100, 0, 100)
      : 0;
    const frontlineShiftKm = Number(aggregate.frontlineShiftKm.toFixed(1));
    const friction = aggregate.frontlineEvidence > 0 && aggregate.frontlineShiftKm > 0
      ? aggregate.tieSum / aggregate.frontlineShiftKm
      : NaN;
    const thermalVerification = aggregate.thermalEligible
      ? (aggregate.thermalVerified / aggregate.thermalEligible) * 100
      : NaN;
    const equipmentTags = Object.entries(aggregate.equipmentTags)
      .sort((left, right) => right[1] - left[1])
      .slice(0, 3)
      .map(([label, count]) => `${label} x${count}`);

    return {
      sectorName,
      totalEvents: aggregate.totalEvents,
      averageTie,
      doctrinePercentages,
      dominantDoctrine: dominantDoctrineKey ? doctrineLabels[dominantDoctrineKey] : 'No dominant profile',
      equipmentEstimate: aggregate.equipmentEvidence > 0 ? Math.round(aggregate.equipmentScore) : NaN,
      equipmentEvidence: aggregate.equipmentEvidence,
      equipmentTags,
      fogIndex,
      fogState: getAxisFogState(fogIndex),
      logisticsHits: aggregate.logisticsHits,
      shapingEvents: aggregate.shapingEvents,
      logisticsRatio,
      logisticsShare,
      frontlineShiftKm,
      friction,
      frontlineEvidence: aggregate.frontlineEvidence,
      thermalEligible: aggregate.thermalEligible,
      thermalVerified: aggregate.thermalVerified,
      thermalVerification,
      sectorThermalCount: sectorThermals.length
    };
  }

  function renderAxisStatsPanel(metrics) {
    const panel = document.getElementById('axis-stats-panel');
    if (!panel) return;

    if (!metrics || !metrics.sectorName) {
      updateAxisPanelUIState(false);
      return;
    }

    const panelBadge = document.getElementById('axisStatsBadge');
    const fogBadge = document.getElementById('axisFogBadge');
    const fogLabels = {
      low: 'Low Fog',
      mid: 'Contested',
      high: 'High Fog',
      critical: 'Black Box'
    };

    document.getElementById('axisStatsTitle').textContent = metrics.sectorName;
    if (panelBadge) {
      panelBadge.textContent = metrics.totalEvents > 0 ? 'Live' : 'Empty';
      panelBadge.dataset.state = metrics.totalEvents > 0 ? 'live' : 'empty';
    }

    document.getElementById('axisStatsSummary').textContent = metrics.totalEvents > 0
      ? `${metrics.totalEvents} filtered events | Avg T.I.E. ${metrics.averageTie.toFixed(1)} | ${metrics.sectorThermalCount} FIRMS points within the last 7 days`
      : 'No qualifying events detected inside this axis under the active filter stack.';

    document.getElementById('axisTotalEvents').textContent = metrics.totalEvents;
    document.getElementById('axisVolumeNote').textContent = describeAxisVolume(metrics.totalEvents);
    document.getElementById('axisTieAverage').textContent = metrics.totalEvents > 0
      ? metrics.averageTie.toFixed(1)
      : 'N/A';

    if (fogBadge) {
      fogBadge.textContent = fogLabels[metrics.fogState] || 'Unknown';
      fogBadge.dataset.state = metrics.fogState;
    }
    document.getElementById('axisFogValue').textContent = metrics.totalEvents > 0
      ? `${Math.round(metrics.fogIndex)}%`
      : 'N/A';
    document.getElementById('axisFogNote').textContent = metrics.totalEvents > 0
      ? `Built from reliability and confidence dispersion across ${metrics.totalEvents} sector events.`
      : 'No signal stack available for coherence scoring.';
    setAxisGauge(document.getElementById('axisFogGauge'), metrics.fogIndex, metrics.fogState);

    document.getElementById('axisEquipmentEstimate').textContent = Number.isFinite(metrics.equipmentEstimate)
      ? metrics.equipmentEstimate
      : 'N/A';
    document.getElementById('axisEquipmentNote').textContent = metrics.equipmentEvidence > 0
      ? `${metrics.equipmentEvidence} sector events contained heavy-platform loss wording.`
      : 'No vehicle or heavy-platform loss wording detected in current sector events.';
    const equipmentTags = document.getElementById('axisEquipmentTags');
    if (equipmentTags) {
      const tags = metrics.equipmentTags.length > 0 ? metrics.equipmentTags : ['No heavy-platform signatures'];
      equipmentTags.innerHTML = tags.map(tag => `<span class="axis-tag">${tag}</span>`).join('');
    }

    document.getElementById('axisDoctrineMeta').textContent = metrics.totalEvents > 0
      ? `${metrics.dominantDoctrine} profile dominant`
      : 'Awaiting doctrinal split.';
    document.getElementById('axisDoctrineManoeuvreValue').textContent = `${metrics.doctrinePercentages.manoeuvre.toFixed(0)}%`;
    document.getElementById('axisDoctrineShapingValue').textContent = `${metrics.doctrinePercentages.shaping.toFixed(0)}%`;
    document.getElementById('axisDoctrineAttritionValue').textContent = `${metrics.doctrinePercentages.attrition.toFixed(0)}%`;
    animateAxisWidth(document.getElementById('axisDoctrineManoeuvreFill'), metrics.doctrinePercentages.manoeuvre);
    animateAxisWidth(document.getElementById('axisDoctrineShapingFill'), metrics.doctrinePercentages.shaping);
    animateAxisWidth(document.getElementById('axisDoctrineAttritionFill'), metrics.doctrinePercentages.attrition);

    document.getElementById('axisLogisticsRatio').textContent = metrics.shapingEvents > 0
      ? `${metrics.logisticsRatio.toFixed(2)}x`
      : 'N/A';
    document.getElementById('axisLogisticsNote').textContent = metrics.shapingEvents > 0
      ? `${metrics.logisticsHits} logistics-linked events inside ${metrics.shapingEvents} shaping events.`
      : 'No shaping cluster available for a suppression ratio.';
    animateAxisWidth(document.getElementById('axisLogisticsHitFill'), metrics.logisticsShare);
    animateAxisWidth(document.getElementById('axisLogisticsPressureFill'), 100 - metrics.logisticsShare);

    document.getElementById('axisFrictionValue').textContent = formatAxisCompactValue(metrics.friction);
    document.getElementById('axisFrictionNote').textContent = Number.isFinite(metrics.friction)
      ? `${describeFrontlineFriction(metrics.friction, metrics.frontlineShiftKm)} Explicit movement evidence: ${metrics.frontlineShiftKm.toFixed(1)} km across ${metrics.frontlineEvidence} manoeuvre reports.`
      : 'Insufficient explicit territorial movement data inside the selected sector. No friction score is shown.';

    document.getElementById('axisThermalValue').textContent = Number.isFinite(metrics.thermalVerification)
      ? `${Math.round(metrics.thermalVerification)}%`
      : 'N/A';
    document.getElementById('axisThermalNote').textContent = metrics.thermalEligible > 0
      ? `${metrics.thermalVerified}/${metrics.thermalEligible} recent shaping or attrition events match FIRMS hotspots within 3 km and 36 hours.`
      : 'No recent shaping or attrition events were eligible for FIRMS cross-checking.';
    animateAxisWidth(document.getElementById('axisThermalFill'), Number.isFinite(metrics.thermalVerification) ? metrics.thermalVerification : 0);
    updateAxisPanelUIState(true);
  }

  function updateAxisStatsPanel(sectorName, sourceEvents) {
    syncAxisHudOffset();
    initAxisStatsPanelControls();
    const previousSector = window.currentAxisSector;
    window.currentAxisSector = sectorName || '';

    if (!sectorName) {
      axisPanelMode = 'expanded';
      axisPanelDismissedSector = '';
      renderAxisStatsPanel(null);
      return;
    }

    if (axisPanelMode === 'hidden' && axisPanelDismissedSector && axisPanelDismissedSector !== sectorName) {
      axisPanelMode = 'expanded';
      axisPanelDismissedSector = '';
    }

    renderAxisStatsPanel(computeAxisMetrics(sectorName, sourceEvents));

    loadAxisThermalFeatures().then(() => {
      const activeSector = document.getElementById('sectorFilter');
      const currentSector = activeSector ? activeSector.value : window.currentAxisSector;
      if (currentSector !== sectorName) return;
      if (previousSector !== sectorName && axisPanelMode === 'hidden' && axisPanelDismissedSector !== sectorName) {
        axisPanelMode = 'expanded';
      }
      renderAxisStatsPanel(computeAxisMetrics(sectorName, Array.isArray(window.currentFilteredEvents) ? window.currentFilteredEvents : sourceEvents));
    });
  }

  function createMarker(e) {
    const color = getColor(e.intensity);
    const iconClass = getIconClass(e.type);
    const size = (e.intensity || 0.2) >= 0.8 ? 34 : 26;
    const iconSize = Math.floor(size / 1.8);

    // Standard marker
    const marker = L.marker([e.lat, e.lon], {
      icon: L.divIcon({
        className: 'custom-icon-marker',
        html: `<div style="background-color: ${color}; width: ${size}px; height: ${size}px; border-radius: 50%; border: 2px solid #1e293b; box-shadow: 0 0 10px ${color}66; display: flex; align-items: center; justify-content: center; color: #1e293b;"><i class="fa-solid ${iconClass}" style="font-size:${iconSize}px;"></i></div>`,
        iconSize: [size, size]
      })
    });

    marker.bindPopup(createPopupContent(e));
    marker.on('popupopen', function () {
      if (window.fetchHistoricalWeatherImpact) window.fetchHistoricalWeatherImpact(e);
    });

    return marker;
  }

  // ==========================================
  // 1. POPUP GENERATION (Correct and with Elegant Style)
  // ==========================================
  function createPopupContent(e) {
    // 1. Safely retrieve ID
    // Use 'event_id' if exists, otherwise 'id'. 'feature' does NOT exist here.
    const id = e.event_id || e.id || (e.properties ? e.properties.event_id : null);

    // 2. Determine color
    const color = getColor(e.intensity);

    // 3. Source Footer Management
    let sourceFooter = '';
    let primaryUrl = null;

    if (e.source && e.source !== 'Unknown Source' && e.source !== 'Source' && e.source !== '#') {
      primaryUrl = e.source.startsWith('http') ? e.source : (e.source.includes('.') ? 'https://' + e.source : null);
    } else if (e.sources_list) {
      try {
        let sList = typeof e.sources_list === 'string' ? JSON.parse(e.sources_list.replace(/'/g, '"')) : e.sources_list;
        if (Array.isArray(sList)) {
          let validUrlObj = sList.find(s => {
            let u = typeof s === 'string' ? s : (s.url || s.link || s.source_url || '#');
            return u && u !== '#' && u.toLowerCase() !== 'source' && u.trim() !== '';
          });
          if (validUrlObj) {
            let extracted = typeof validUrlObj === 'string' ? validUrlObj : (validUrlObj.url || validUrlObj.link);
            primaryUrl = extracted.startsWith('http') ? extracted : 'https://' + extracted;
          }
        }
      } catch (err) { }
    }

    if (primaryUrl && primaryUrl.startsWith('http')) {
      let domain = "Original Source";
      try {
        domain = new URL(primaryUrl).hostname.replace('www.', '');
      } catch (err) { }

      sourceFooter = `
            <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #334155; display: flex; align-items: center; justify-content: space-between;">
              <span style="font-size: 0.7rem; color: #64748b;">Source:</span>
              <a href="${primaryUrl}" target="_blank" style="color: #3b82f6; text-decoration: none;">
                 <i class="fa-solid fa-link"></i> ${domain}
              </a>
            </div>`;
    }

    // 4. Popup HTML Construction (Elegant Style Restored)
    // Note: Button has INLINE style to ensure it is blue and beautiful as before.
    return `
    <div class="acled-popup" style="color:#e2e8f0; font-family: 'Inter', sans-serif; min-width: 260px;">
      
      <div style="border-left: 4px solid ${color}; padding-left: 12px; margin-bottom: 12px;">
        <h5 style="margin:0; font-weight:700; font-size:1rem; line-height:1.3; color:#f8fafc;">${e.title}</h5>
        
        <div style="margin-top:8px; display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
          <span class="popup-meta-tag" style="background:#1e293b; padding:2px 6px; border-radius:4px; font-size:0.75rem;"><i class="fa-regular fa-calendar"></i> ${e.date}</span>
          <span class="popup-meta-tag" style="background:#1e293b; padding:2px 6px; border-radius:4px; font-size:0.75rem;"><i class="fa-solid fa-tag"></i> ${e.type || 'Event'}</span>
          <span id="weather-badge-${id}" style="font-size:0.75rem; display:inline-block;"></span>
        </div>
      </div>

      <div style="font-size:0.85rem; line-height:1.6; color:#cbd5e1; margin-bottom:15px;">
        ${e.description ? (e.description.length > 120 ? e.description.substring(0, 120) + '...' : e.description) : 'No description.'}
      </div>
      
      <div class="popup-actions">
        <button 
          class="custom-dossier-btn" 
          onclick="openModal('${id}')"> 
          <i class="fas fa-folder-open"></i> OPEN DOSSIER
        </button>
      </div>
      
      ${sourceFooter}
    </div>`;
  }

  // ==========================================
  // 1.5 HISTORICAL WEATHER IMPACT GENERATOR
  // ==========================================
  window.fetchHistoricalWeatherImpact = function (e) {
    const id = e.event_id || e.id || (e.properties ? e.properties.event_id : null);
    const badge = document.getElementById(`weather-badge-${id}`);

    if (!badge || badge.dataset.loaded === 'true') return;

    // Extract date (YYYY-MM-DD format required for Open-Meteo)
    let isoDate = null;
    if (e.timestamp) {
      isoDate = new Date(e.timestamp).toISOString().split('T')[0];
    } else {
      const dateMatch = (e.date || '').match(/\d{4}-\d{2}-\d{2}/);
      if (dateMatch) isoDate = dateMatch[0];
    }

    if (!isoDate || !e.lat || !e.lon) {
      badge.style.display = 'none';
      return; // Can't fetch without valid location and date
    }

    badge.innerHTML = `<span style="color:#64748b; font-size:0.65rem;">Fetching weather...</span>`;

    // Fetch from Open-Meteo Archive API
    fetch(`https://archive-api.open-meteo.com/v1/archive?latitude=${e.lat}&longitude=${e.lon}&start_date=${isoDate}&end_date=${isoDate}&daily=temperature_2m_max,precipitation_sum&timezone=auto`)
      .then(r => r.json())
      .then(data => {
        if (data.daily) {
          const tMax = data.daily.temperature_2m_max[0];
          const precip = data.daily.precipitation_sum[0] || 0;

          let impactHtml = '';
          if (tMax <= -2) {
            impactHtml = `<span style="background:#082f49; color:#38bdf8; border:1px solid #0284c7; padding:2px 6px; border-radius:4px;"><i class="fa-solid fa-snowflake"></i> Frozen (${tMax}Ã‚Â°C)</span>`;
          } else if (tMax > 0 && precip > 5) {
            impactHtml = `<span style="background:#422006; color:#fb923c; border:1px solid #ea580c; padding:2px 6px; border-radius:4px;"><i class="fa-solid fa-cloud-showers-heavy"></i> Mud/Rain (${precip}mm)</span>`;
          } else {
            impactHtml = `<span style="background:#1e293b; color:#94a3b8; border:1px solid #334155; padding:2px 6px; border-radius:4px;"><i class="fa-solid fa-cloud"></i> Fair (${tMax}Ã‚Â°C)</span>`;
          }

          badge.innerHTML = impactHtml;
          badge.dataset.loaded = 'true';
        }
      })
      .catch(err => {
        console.warn(`Historical weather fetch failed for event ${id}:`, err);
        badge.style.display = 'none';
      });
  };

  function normalizeCampaignId(value) {
    return String(value || '').trim().toLowerCase();
  }

  function findCampaignMeta(campaignId) {
    const cid = normalizeCampaignId(campaignId);
    if (!cid) return null;
    return strategicCampaignReports.find(c => normalizeCampaignId(c.campaign_id) === cid)
      || strategicCampaignDefinitions.find(c => normalizeCampaignId(c.campaign_id) === cid)
      || null;
  }

  function cleanupStrategicCharts() {
    Object.keys(strategicSparklineCharts).forEach(function (key) {
      try {
        if (strategicSparklineCharts[key]) strategicSparklineCharts[key].destroy();
      } catch (err) { }
      delete strategicSparklineCharts[key];
    });
  }

  function setStrategicDrawerVisible(visible) {
    const drawer = document.getElementById('strategicCampaignsDrawer');
    if (!drawer) return;
    drawer.classList.toggle('active', !!visible);
  }

  function updateStrategicHull(events) {
    if (!map) return;

    if (strategicHullLayer && map.hasLayer(strategicHullLayer)) {
      map.removeLayer(strategicHullLayer);
    }
    strategicHullLayer = null;

    if (!strategicCampaignsMode || !selectedStrategicCampaignId || !window.turf) return;

    const matched = (events || []).filter(function (e) {
      return normalizeCampaignId(e.campaign_id) === normalizeCampaignId(selectedStrategicCampaignId)
        && Number.isFinite(e.lon) && Number.isFinite(e.lat);
    });

    if (matched.length < 3) return;

    try {
      const points = matched.map(function (e) {
        return window.turf.point([e.lon, e.lat]);
      });
      const fc = window.turf.featureCollection(points);
      const hull = window.turf.convex(fc);
      if (!hull) return;

      const meta = findCampaignMeta(selectedStrategicCampaignId) || {};
      const hullColor = meta.color || '#f59e0b';
      strategicHullLayer = L.geoJSON(hull, {
        style: {
          color: hullColor,
          fillColor: hullColor,
          fillOpacity: 0.1,
          opacity: 0.8,
          weight: 2
        }
      }).addTo(map);
    } catch (err) {
      console.warn('Strategic hull generation failed:', err);
    }
  }

  function renderStrategicCampaignLayer(events) {
    if (!map) return;

    if (!strategicCanvasLayer) {
      strategicCanvasLayer = L.layerGroup();
    }
    strategicCanvasLayer.clearLayers();

    const selectedId = normalizeCampaignId(selectedStrategicCampaignId);
    (events || []).forEach(function (e) {
      if (!Number.isFinite(e.lat) || !Number.isFinite(e.lon)) return;

      const eventCampaignId = normalizeCampaignId(e.campaign_id);
      const isMatch = selectedId && eventCampaignId === selectedId;
      const baseColor = e.campaign_color || e.marker_color || '#f59e0b';
      const markerColor = isMatch ? baseColor : (selectedId ? '#64748b' : baseColor);
      const markerOpacity = selectedId ? (isMatch ? 0.95 : 0.2) : 0.85;
      const radius = selectedId ? (isMatch ? Math.max(5, e.marker_radius || 5) : 3.4) : Math.max(4, e.marker_radius || 4);

      const marker = L.circleMarker([e.lat, e.lon], {
        renderer: strategicCanvasRenderer,
        radius: radius,
        color: markerColor,
        weight: isMatch ? 1.7 : 1,
        opacity: markerOpacity,
        fillColor: markerColor,
        fillOpacity: Math.min(0.95, markerOpacity + 0.08)
      });

      marker.bindPopup(createPopupContent(e));
      strategicCanvasLayer.addLayer(marker);
    });

    if (!map.hasLayer(strategicCanvasLayer)) {
      map.addLayer(strategicCanvasLayer);
    }

    updateStrategicHull(events);
  }

  function renderStrategicCampaignCards() {
    const container = document.getElementById('strategicCampaignCards');
    if (!container) return;

    const items = (strategicCampaignReports || []).map(function (item) {
      const campaignId = normalizeCampaignId(item.campaign_id);
      return {
        campaign_id: campaignId,
        name: item.name || campaignId.toUpperCase(),
        color: item.color || '#f59e0b',
        status: String(item.status || 'STANDBY').toUpperCase(),
        sum_vec_e: Number(item.sum_vec_e || 0),
        brief_text: item.brief_text || 'No strategic brief available.',
        sparkline: (item.sparkline_daily_vec_e && Array.isArray(item.sparkline_daily_vec_e.values))
          ? item.sparkline_daily_vec_e.values
          : [],
      };
    });

    if (!items.length) {
      container.innerHTML = '<div class=\"sc-empty\">No campaign reports available.</div>';
      cleanupStrategicCharts();
      return;
    }

    const activeId = normalizeCampaignId(selectedStrategicCampaignId);
    container.innerHTML = items.map(function (item, idx) {
      const sparkId = 'scSpark_' + idx;
      const statusClass = item.status === 'LIVE' ? 'live' : 'standby';
      const isActive = activeId && item.campaign_id === activeId;
      return `
        <article class=\"sc-card ${isActive ? 'active' : ''}\" data-campaign-id=\"${item.campaign_id}\" onclick=\"toggleStrategicCampaignSelection('${item.campaign_id}')\" style=\"border-left:3px solid ${item.color};\">
          <div class=\"sc-row\">
            <div class=\"sc-name\">${item.name}</div>
            <span class=\"sc-badge ${statusClass}\">${item.status}</span>
          </div>
          <div class=\"sc-row\">
            <div>
              <div class=\"sc-metric\">${item.sum_vec_e.toFixed(1)}</div>
              <div class=\"sc-metric-label\">Cumulative E-Vector</div>
            </div>
          </div>
          <div class=\"sc-sparkline-wrap\"><canvas id=\"${sparkId}\" height=\"42\"></canvas></div>
          <div class=\"sc-brief\">${item.brief_text}</div>
        </article>`;
    }).join('');

    cleanupStrategicCharts();

    items.forEach(function (item, idx) {
      const sparkId = 'scSpark_' + idx;
      const canvas = document.getElementById(sparkId);
      if (!canvas || typeof Chart === 'undefined') return;
      const values = (item.sparkline && item.sparkline.length) ? item.sparkline : [0];
      strategicSparklineCharts[sparkId] = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
          labels: values.map(function (_, i) { return i + 1; }),
          datasets: [{
            data: values,
            borderColor: item.color,
            backgroundColor: item.color + '33',
            pointRadius: 0,
            borderWidth: 1.5,
            fill: true,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { enabled: false } },
          scales: { x: { display: false }, y: { display: false } }
        }
      });
    });
  }

  function loadStrategicCampaignData() {
    return Promise.all([
      fetch('assets/data/campaign_reports.json')
        .then(function (r) { return r.ok ? r.json() : { campaigns: [] }; })
        .catch(function () { return { campaigns: [] }; }),
      fetch('assets/data/campaign_definitions.json')
        .then(function (r) { return r.ok ? r.json() : { campaigns: [] }; })
        .catch(function () { return { campaigns: [] }; })
    ]).then(function (result) {
      const reportsPayload = result[0] || {};
      const defsPayload = result[1] || {};
      strategicCampaignReports = Array.isArray(reportsPayload.campaigns) ? reportsPayload.campaigns : [];
      strategicCampaignDefinitions = Array.isArray(defsPayload.campaigns) ? defsPayload.campaigns : [];
      renderStrategicCampaignCards();
    });
  }

  window.toggleStrategicCampaignSelection = function (campaignId) {
    const normalized = normalizeCampaignId(campaignId);
    if (!normalized) return;
    selectedStrategicCampaignId = normalizeCampaignId(selectedStrategicCampaignId) === normalized ? null : normalized;
    renderStrategicCampaignCards();
    if (window.applyMapFilters) window.applyMapFilters();
  };

  window.toggleStrategicCampaignsMode = function (forceState) {
    strategicCampaignsMode = typeof forceState === 'boolean' ? forceState : !strategicCampaignsMode;
    if (!strategicCampaignsMode) {
      selectedStrategicCampaignId = null;
      if (strategicCanvasLayer && map && map.hasLayer(strategicCanvasLayer)) {
        map.removeLayer(strategicCanvasLayer);
      }
      if (strategicHullLayer && map && map.hasLayer(strategicHullLayer)) {
        map.removeLayer(strategicHullLayer);
      }
      cleanupStrategicCharts();
      setStrategicDrawerVisible(false);
      if (window.applyMapFilters) window.applyMapFilters();
      return;
    }

    setStrategicDrawerVisible(true);
    loadStrategicCampaignData().finally(function () {
      if (window.applyMapFilters) window.applyMapFilters();
    });
  };

  // ============================================
  // 4. RENDERING FUNCTIONS
  // ============================================

  function renderInternal(eventsToDraw) {
    eventsLayer.clearLayers();
    if (heatLayer) map.removeLayer(heatLayer);

    let renderable = eventsToDraw || [];
    if (isHeatmapMode && window.sectorAnomalySet && window.sectorAnomalySet.size > 0) {
      renderable = renderable.filter(e => window.sectorAnomalySet.has(e.operational_sector) || e.is_anomaly_sector === true);
    }

    if (isHeatmapMode) {
      if (strategicCanvasLayer && map.hasLayer(strategicCanvasLayer)) {
        map.removeLayer(strategicCanvasLayer);
      }
      if (strategicHullLayer && map.hasLayer(strategicHullLayer)) {
        map.removeLayer(strategicHullLayer);
      }
      if (typeof L.heatLayer === 'undefined') return;
      const heatPoints = renderable.map(e => [e.lat, e.lon, (e.intensity || 0.5) * 2]);
      heatLayer = L.heatLayer(heatPoints, {
        radius: 25,
        blur: 15,
        maxZoom: 10,
        gradient: { 0.4: 'blue', 0.6: '#00ff00', 0.8: 'yellow', 1.0: 'red' }
      }).addTo(map);
    } else if (strategicCampaignsMode) {
      if (map.hasLayer(eventsLayer)) map.removeLayer(eventsLayer);
      renderStrategicCampaignLayer(renderable);
    } else {
      if (strategicCanvasLayer && map.hasLayer(strategicCanvasLayer)) {
        map.removeLayer(strategicCanvasLayer);
      }
      if (strategicHullLayer && map.hasLayer(strategicHullLayer)) {
        map.removeLayer(strategicHullLayer);
      }
      const markers = renderable.map(e => createMarker(e));
      eventsLayer.addLayers(markers);
      map.addLayer(eventsLayer);
    }

    if (document.getElementById('eventCount')) {
      document.getElementById('eventCount').innerText = renderable.length;
    }

    console.log(`Rendered ${renderable.length} events on map`);
  }

  // ============================================
  // 5. TIME TRAVEL FUNCTIONS
  // ============================================

  function loadHistoricalMap(dateString) {
    const url = `assets/data/history/frontline_${dateString}.geojson`;

    fetch(url)
      .then(response => {
        if (!response.ok) return null;
        return response.json();
      })
      .then(data => {
        if (!data) return;

        // Remove old historical layer
        if (historicalFrontlineLayer) {
          map.removeLayer(historicalFrontlineLayer);
        }

        // Remove current frontline layer to show historical
        if (currentFrontlineLayer && map.hasLayer(currentFrontlineLayer)) {
          map.removeLayer(currentFrontlineLayer);
        }

        // Add historical layer
        historicalFrontlineLayer = L.geoJSON(data, {
          style: {
            color: "#d32f2f",
            weight: 3,
            opacity: 0.8,
            fillOpacity: 0.35,
            className: 'historical-line'
          },
          onEachFeature: function (feature, layer) {
            layer.bindPopup(`<b>Situation as of:</b> ${dateString}<br>Occupied territory`);
          }
        }).addTo(map);

        console.log(`Ã°Å¸â€”ÂºÃ¯Â¸Â Historical map loaded: ${dateString}`);
      })
      .catch(err => console.error("Error loading historical map:", err));
  }

  function filterEventsByDate(dateString) {
    const targetDate = moment(dateString, 'YYYY-MM-DD');
    if (!targetDate.isValid()) {
      console.error("Invalid date:", dateString);
      return;
    }

    const targetTimestamp = targetDate.valueOf();
    const filtered = window.globalEvents.filter(e => e.timestamp <= targetTimestamp);

    window.currentFilteredEvents = filtered;
    renderInternal(filtered);

    console.log(`Ã°Å¸â€œâ€¦ Filtered to ${filtered.length} events up to ${dateString}`);
  }

  // Pre-load ORBAT metadata for richness
  fetch('assets/data/orbat_full.json')
    .then(r => r.json())
    .then(data => {
      window.orbatData = data;
      console.log(`Ã¢Å“â€¦ ORBAT Metadata Loaded: ${data.length} units`);
    })
    .catch(e => console.warn("Ã¢Å¡Â Ã¯Â¸Â ORBAT Metadata missing"));

  // ===========================================
  // OWL INTEGRATION: DATA FETCHER (No Auto-Render)
  // ===========================================
  window.owlData = {
    frontline: null,     // LayerGroup (Frontline segments)
    fortifications: [],  // Array of Feature (Lines)
    units: new Map()     // Map<NormalizedName, Feature>
  };

  function fetchOwlData() {
    console.log("Ã°Å¸Â¦â€° Fetching Project Owl Data...");

    return fetch('assets/data/owl_layer.geojson')
      .then(res => res.json())
      .then(data => {
        const fortifications = [];
        const units = new Map();
        const frontlineFeatures = [];

        data.features.forEach(f => {
          const props = f.properties || {};
          const name = (props.name || '').toLowerCase();

          // Skip strange Holding Area rectangles in the Black Sea
          if (name.includes('holding area')) return;

          if (f.geometry.type === 'Point') {
            // UNIT
            // Normalize name key for matching
            // Remove 'brigade', 'regiment', 'separate', etc for key, but keep full prop
            const key = name.replace(/separate|mechanized|brigade|regiment|infantry|marine|assault|airborne|battalion/g, '').trim();
            units.set(key, f);
          } else if (f.geometry.type === 'LineString' || f.geometry.type === 'MultiLineString') {
            if (name.includes('fortification') || name.includes('trench') || name.includes('dragon')) {
              fortifications.push(f);
            } else {
              frontlineFeatures.push(f);
            }
          } else if (f.geometry.type === 'Polygon' || f.geometry.type === 'MultiPolygon') {
            frontlineFeatures.push(f); // Polygons are part of the frontline map
          }
        });

        // Store Logic
        window.owlData.fortifications = fortifications;
        window.owlData.units = units;

        // Pre-build Frontline Layer (Canvas) for 'Project Owl' source
        const canvasRenderer = L.canvas();
        window.owlData.frontline = L.geoJSON({ type: 'FeatureCollection', features: frontlineFeatures }, {
          renderer: canvasRenderer,
          style: function (feature) {
            const side = feature.properties.side || 'NEUTRAL';
            const type = feature.geometry.type;

            // Base styles
            let color = '#d4d4d8';      // Zinc line for borders
            let fillColor = 'transparent';
            let fillOpacity = 0;
            let weight = 2.5;

            if (type.includes('Polygon')) {
              if (side === 'UA') {
                // Ukrainian held (e.g. counterattacks)
                fillColor = 'transparent';
                fillOpacity = 0;
                color = 'transparent';
                weight = 0;
              } else {
                // RU or NEUTRAL Polygons represent Russian-occupied territories (Crimea, LPR, etc)
                color = '#ef4444'; // Red
                fillColor = '#ef4444';
                fillOpacity = 0.3; // Red field for Russian occupation
                weight = 1;        // Thinner border for polygon
              }
            } else {
              // Lines (Frontline, Grey Zone, Fortifications)
              if (side === 'RU') {
                color = '#ef4444'; // Red
              } else if (side === 'UA') {
                color = '#3b82f6'; // Blue
              } else {
                // Neutral / Grey Zone / Unknown lines
                color = '#94a3b8'; // Slate Grey
              }
            }

            return {
              color: color,
              weight: weight,
              opacity: 0.8,
              fillColor: fillColor,
              fillOpacity: fillOpacity,
              lineCap: 'square'
            };
          }
        });

        console.log(`Ã°Å¸Â¦â€° Owl Data Ready: ${units.size} Units, ${fortifications.length} Forts`);
        return window.owlData;
      })
      .catch(err => console.error("Ã¢ÂÅ’ Failed to fetch Owl Data:", err));
  }

  // Alias for backward compatibility if needed, or simply replaced
  window.loadOwlLayer = function () {
    fetchOwlData().then(() => {
      if (window.owlData.frontline) {
        if (currentFrontlineLayer) map.removeLayer(currentFrontlineLayer);
        currentFrontlineLayer = window.owlData.frontline;
        currentFrontlineLayer.addTo(map);
      }
    });
  };

  function loadFrontlineLayer(url, color) {
    // Deprecated wrapper - redirects to Owl if needed, or loads legacy
    if (url.includes('owl')) {
      loadOwlLayer();
      return;
    }

    // Legacy support for basic LineStrings
    if (currentFrontlineLayer) map.removeLayer(currentFrontlineLayer);

    fetch(url)
      .then(response => {
        if (!response.ok) throw new Error("Map file not found: " + url);
        return response.json();
      })
      .then(data => {
        currentFrontlineLayer = L.geoJSON(data, {
          style: function () {
            return {
              color: color,
              weight: 2,
              opacity: 0.8,
              fillOpacity: 0.1
            };
          },
          onEachFeature: function (feature, layer) {
            if (feature.properties && feature.properties.name) {
              layer.bindPopup(feature.properties.name);
            }
          }
        }).addTo(map);
        console.log("Ã¢Å“â€¦ Frontline loaded:", url);
      })
      .catch(err => console.error("Ã¢ÂÅ’ Error loading frontline:", err));
  }

  // ============================================
  // 6. SLIDER SETUP
  // ============================================

  function setupTimeSlider(allData) {
    const slider = document.getElementById('timeSlider');
    const startLabel = document.getElementById('sliderStartDate');
    const display = document.getElementById('sliderCurrentDate');

    if (!allData.length || !slider) return;

    const timestamps = allData.map(d => d.timestamp).filter(t => t > 0);
    const minTime = Math.min(...timestamps);
    const maxTime = Math.max(...timestamps);

    slider.min = minTime;
    slider.max = maxTime;
    slider.value = maxTime;
    slider.disabled = false;

    startLabel.innerText = moment(minTime).format('DD/MM/YYYY');
    display.innerText = "LIVE";

    // Load historical dates index
    fetch('assets/data/map_dates.json')
      .then(res => res.json())
      .then(dates => {
        mapDates = dates;
        console.log("Ã°Å¸â€œâ€¦ Historical dates loaded:", mapDates.length);

        const dateSlider = document.getElementById('date-slider');
        const dateLabel = document.getElementById('date-label');

        if (dateSlider && mapDates.length > 0) {
          dateSlider.max = mapDates.length - 1;
          dateSlider.value = mapDates.length - 1;

          const latestDate = mapDates[mapDates.length - 1];
          if (dateLabel) dateLabel.innerText = latestDate;

          loadHistoricalMap(latestDate);
          filterEventsByDate(latestDate);

          dateSlider.addEventListener('input', function (e) {
            const index = parseInt(e.target.value);
            const selectedDate = mapDates[index];

            if (dateLabel) dateLabel.innerText = selectedDate;
            loadHistoricalMap(selectedDate);
            filterEventsByDate(selectedDate);
          });
        }
      })
      .catch(err => console.error("Failed to load map dates:", err));

    // Standard time slider
    slider.addEventListener('input', function (e) {
      const currentSliderVal = parseInt(e.target.value);
      const timeFiltered = window.currentFilteredEvents.filter(ev => ev.timestamp <= currentSliderVal);
      renderInternal(timeFiltered);

      const dateStr = moment(currentSliderVal).format('DD/MM/YYYY');
      display.innerText = currentSliderVal >= maxTime ? "LIVE" : dateStr;
    });
  }

  // ============================================
  // 7. MAP INITIALIZATION
  // ============================================

  // ============================================
  // 7. MAP INITIALIZATION (Fix Crash)
  // ============================================
  function initMap() {
    // FIX: Check both internal state string and DOM element
    const container = L.DomUtil.get('map');
    if (map || (container && container._leaflet_id)) {
      console.log("Ã¢Å¡Â Ã¯Â¸Â Map container already initialized. Skipping init.");
      return;
    }

    map = L.map('map', {
      zoomControl: false,
      preferCanvas: true, // Performance boost
      wheelPxPerZoomLevel: 120
    }).setView([48.5, 32.0], 6);
    window.map = map;

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19,
      attribution: 'Ã‚Â© IMPACT ATLAS'
    }).addTo(map);

    eventsLayer = L.markerClusterGroup({
      chunkedLoading: true,
      maxClusterRadius: 45,
      spiderfyOnMaxZoom: true,
      iconCreateFunction: function (cluster) {
        const count = cluster.getChildCount();
        const size = count < 10 ? 'small' : (count < 100 ? 'medium' : 'large');
        return new L.DivIcon({
          html: `<div><span>${count}</span></div>`,
          className: `marker-cluster marker-cluster-${size}`,
          iconSize: new L.Point(40, 40)
        });
      }
    });
    map.addLayer(eventsLayer);

    // DEBUG: Log all clicks
    map.on('click', function (e) {
      console.log(`Ã°Å¸â€“Â±Ã¯Â¸Â MAP CLICK AT: ${e.latlng.lat}, ${e.latlng.lng}`);
      // alert(`DEBUG: Map clicked at ${e.latlng}`);
    });

    // Load PROJECT OWL as default
    loadOwlLayer();

    console.log("Ã¢Å“â€¦ Map initialized");
  }

  // ============================================
  // 8. DATA LOADING (Critical - Runs After Map Init)
  // ============================================

  function loadSectorsData() {
    console.log("Ã°Å¸â€œÂ¥ Starting sectors download...");
    fetch('assets/data/operational_sectors.geojson')
      .then(response => response.json())
      .then(data => {
        window.operationalSectorsData = data;
        const sectorSelect = document.getElementById('sectorFilter');
        if (sectorSelect && data.features) {
          // Sort sectors by name
          const sortedFeatures = data.features.sort((a, b) => {
            const nameA = a.properties.operational_sector || a.properties.name || '';
            const nameB = b.properties.operational_sector || b.properties.name || '';
            return nameA.localeCompare(nameB);
          });
          sortedFeatures.forEach(f => {
            const sectorName = f.properties.operational_sector || f.properties.name;
            const opt = document.createElement('option');
            opt.value = sectorName;
            opt.innerText = sectorName;
            sectorSelect.appendChild(opt);
          });
          ['Deep_Strike_RU', 'Rear_Area_UA'].forEach(sectorName => {
            const exists = Array.from(sectorSelect.options).some(o => o.value === sectorName);
            if (!exists) {
              const opt = document.createElement('option');
              opt.value = sectorName;
              opt.innerText = sectorName;
              sectorSelect.appendChild(opt);
            }
          });
        }

        // Add invisible layer for flyToBounds (we don't render it directly, just keep it for geo finding)
        window.tacticalSectorsLayer = L.geoJSON(data, {
          style: function (feature) {
            return { color: '#f59e0b', weight: 2, fillOpacity: 0.1, dashArray: '5, 5', opacity: 0.8 };
          }
        });
        window.tacticalSectorsIndex = new Map();
        window.tacticalSectorsLayer.eachLayer(function (layer) {
          const sectorName = layer.feature.properties.operational_sector || layer.feature.properties.name;
          if (!sectorName) return;

          const entries = window.tacticalSectorsIndex.get(sectorName) || [];
          entries.push({
            layer: layer,
            geometry: layer.feature.geometry,
            bounds: layer.getBounds()
          });
          window.tacticalSectorsIndex.set(sectorName, entries);
        });
      })
      .catch(err => console.error("Ã¢ÂÅ’ Failed to load sectors:", err));
  }

  function loadSectorAnomaliesData() {
    fetch('assets/data/sector_anomalies.json')
      .then(r => r.json())
      .then(data => {
        const anomalies = Array.isArray(data.anomalies) ? data.anomalies : [];
        window.sectorAnomalySet = new Set(anomalies.map(a => a.sector));
      })
      .catch(() => {
        window.sectorAnomalySet = new Set();
      });
  }

  function loadEventsData() {
    console.log("Ã°Å¸â€œÂ¥ Starting event download...");

    fetch('assets/data/events.geojson')
      .then(response => response.json())
      .then(data => {
        // 1. Raw Data
        window.allEventsData = data.features || data;
        console.log(`Ã°Å¸â€™Â¾ Data downloaded: ${window.allEventsData.length} raw events`);

        if (window.allEventsData.length === 0) {
          console.warn("Ã¢Å¡Â Ã¯Â¸Â No events found in GeoJSON");
          return;
        }

        // 2. PROCESSING (Unique correct map cycle)
        window.globalEvents = window.allEventsData.map(f => {
          // Moment.js Logic
          const props = f.properties || f;

          // Attempt with explicit formats
          let m = moment(props.date, ["DD/MM/YY", "DD/MM/YYYY", "YYYY-MM-DD", "DD-MM-YYYY"]);

          // Fallback if invalid
          if (!m.isValid()) {
            m = moment(props.date);
          }

          const explicitTimestamp = Number(props.timestamp);
          const ts = Number.isFinite(explicitTimestamp)
            ? explicitTimestamp
            : m.isValid() ? m.valueOf() : moment().valueOf();

          // --- ENRICHMENT FOR FILTERS ---
          const txt = (props.title + " " + (props.description || "")).toLowerCase();

          // 1. Actor (Heuristic)
          let actor = 'UNK';
          if (txt.includes('russia') || txt.includes('moscow') || txt.includes('kremlin') || txt.includes('putin') || txt.includes('russian')) actor = 'RUS';
          if (txt.includes('ukrain') || txt.includes('kyiv') || txt.includes('zelensky') || txt.includes('afu') || txt.includes('uaf')) actor = 'UKR';
          if (props.actor) actor = props.actor; // Fallback

          // 2. Threat Level (Intensity 1-10 -> Categories)
          const score = props.intensity_score || (props.tie_total ? props.tie_total / 10 : 0) || 0;
          let threat = 'low';
          if (score >= 4) threat = 'medium';
          if (score >= 7) threat = 'high';
          if (score >= 9) threat = 'critical';

          // 3. Category (Heuristic)
          let cat = 'Other';
          if (txt.includes('strike') || txt.includes('attack') || txt.includes('bomb') || txt.includes('fire') || txt.includes('explo') || txt.includes('destroy')) cat = 'Kinetic';
          else if (txt.includes('advance') || txt.includes('captur') || txt.includes('retreat') || txt.includes('storm') || txt.includes('seiz')) cat = 'Maneuver';
          else if (txt.includes('civilian') || txt.includes('casualt') || txt.includes('hous') || txt.includes('school') || txt.includes('hospital')) cat = 'Civilian';
          else if (txt.includes('statement') || txt.includes('meet') || txt.includes('warn') || txt.includes('visit') || txt.includes('politic')) cat = 'Political';
          if (props.category) cat = props.category;

          return {
            ...props,
            event_id: props.event_id || props.cluster_id || props.id,
            lat: f.geometry ? f.geometry.coordinates[1] : props.lat,
            lon: f.geometry ? f.geometry.coordinates[0] : props.lon,
            timestamp: ts,
            date: m.isValid() ? m.format("DD/MM/YYYY") : props.date,
            // Enriched Fields
            actor: actor,
            threat_level: threat,
            category: cat,
            source_reputation_score: parseFloat(props.source_reputation_score || 50),
            hide_by_default: !!props.hide_by_default
          };
        })
          // MODIFICATION: Frontend "Junk" Filter
          .filter(e => {
            // Excludes if coordinates are 0
            if (!e.lat || !e.lon || e.lat === 0 || e.lon === 0) return false;

            return true;
          })
          .sort((a, b) => b.timestamp - a.timestamp); // Descending order

        console.log(`Ã¢Å“â€¦ Events processed: ${window.globalEvents.length}`);

        // 3. FILTER DEFINITION (CIVILIAN + ACTORS + SMART SEARCH + HIGH IMPACT)
        // 3. FILTER DEFINITION (Comprehensive Rewrite)
        window._applyMapFiltersImpl = function () {
          // A. Retrieve Input
          const searchInput = document.getElementById('textSearch');
          const searchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';

          const startDateInput = document.getElementById('startDate');
          const endDateInput = document.getElementById('endDate');
          const startDate = startDateInput && startDateInput.value ? new Date(startDateInput.value).getTime() : 0;
          const endDate = endDateInput && endDateInput.value ? new Date(endDateInput.value).getTime() : 9999999999999;

          const actorSelect = document.getElementById('actorFilter');
          const selectedActor = actorSelect ? actorSelect.value : '';

          const categorySelect = document.getElementById('chartTypeFilter');
          const selectedCategory = categorySelect ? categorySelect.value : '';

          const sectorSelect = document.getElementById('sectorFilter');
          const selectedSector = sectorSelect ? sectorSelect.value : '';

          console.log(`Ã°Å¸â€Â Filtering: Range[${startDate}-${endDate}] Actor[${selectedActor}] Cat[${selectedCategory}] Sector[${selectedSector}] Search[${searchTerm}]`);

          // B. Filtering Cycle
          const filtered = window.globalEvents.filter(e => {
            // 1. Date Range
            if (e.timestamp < startDate || e.timestamp > endDate) return false;

            // 2. Actor
            if (selectedActor && e.actor !== selectedActor) return false;

            // 3. Category
            if (selectedCategory && e.category !== selectedCategory) return false;

            // 4. Sector
            if (selectedSector && !isEventInsideSector(e, selectedSector)) return false;

            // 4.5 Source reputation filter (default ON)
            if (window.hideLowReputation !== false) {
              const rep = parseFloat(e.source_reputation_score || 50);
              if (rep < 50) return false;
            }

            // 5. Smart Text Search
            if (searchTerm) {
              const searchId = String(e.id || e.event_id || '').toLowerCase();
              if (searchId === searchTerm) return true; // Direct exact match for ID

              const inTitle = (e.title || '').toLowerCase().includes(searchTerm);
              const inDesc = (e.description || '').toLowerCase().includes(searchTerm);
              const inLoc = (e.location_precision || '').toLowerCase().includes(searchTerm);

              const inVis = (e.visual_analysis || '').toLowerCase().includes(searchTerm);

              const isDateMatch = (e.date || '').includes(searchTerm);
              const isSmartActor = (searchTerm.includes('russia') && e.actor === 'RUS') || (searchTerm.includes('ukrain') && e.actor === 'UKR');

              if (!inTitle && !inDesc && !inLoc && !inVis && !isDateMatch && !isSmartActor) return false;
            }

            // 6. Tactical Time Window Filter
            if (tacticalTimeWindowHours > 0 && window.globalEvents && window.globalEvents.length > 0) {
              // Use the most recent event as reference point (not Date.now())
              const maxTimestamp = Math.max(...window.globalEvents.map(ev => ev.timestamp || 0));
              const cutoff = maxTimestamp - (tacticalTimeWindowHours * 3600000);
              if (e.timestamp < cutoff) {
                // Event is outside the time window
                if (tacticalPersistence) {
                  // Persistence ON: keep if TIE >= 100 or category is MANOEUVRE/SHAPING_OFFENSIVE
                  const tieScore = e.tie_total || e.tie_score || 0;
                  const eCat = (e.category || '').toUpperCase();
                  const isPersistent = tieScore >= 100 || eCat === 'MANOEUVRE' || eCat === 'MANEUVER' || eCat === 'SHAPING_OFFENSIVE';
                  if (!isPersistent) return false;
                } else {
                  return false; // Persistence OFF: strict cutoff
                }
              }
            }

            // 7. Global Performance Filter (ELITE / ATTRITION)
            if (window.activePerformanceFilter) {
              if (window.activePerformanceFilter === 'elite') {
                const tie = parseFloat(e.tie_total || e.tie_score || 0);
                const vecT = parseFloat(e.vec_t || 0);
                if (tie < 80 && vecT < 8) return false;
              } else if (window.activePerformanceFilter === 'attrition') {
                const vecE = parseFloat(e.vec_e || 0);
                if (vecE < 7) return false;
              }
            }

            return true;
          });

          // C. Update Map and Counters
          window.currentFilteredEvents = filtered;

          if (document.getElementById('eventCount')) {
            document.getElementById('eventCount').innerText = filtered.length;
          }

          renderInternal(filtered);
          updateAxisStatsPanel(selectedSector, filtered);

          if (window.Dashboard) window.Dashboard.update(filtered);
        };

        // Helper to Populate Category Dropdown
        window.populateCategoryFilter = function (events) {
          const catSelect = document.getElementById('chartTypeFilter');
          if (!catSelect) return;

          // Get unique categories
          const categories = [...new Set(events.map(e => e.category))].sort();

          // Keep "All categories" option
          catSelect.innerHTML = '<option value="">All categories</option>';

          categories.forEach(cat => {
            const opt = document.createElement('option');
            opt.value = cat;
            opt.innerText = cat;
            catSelect.appendChild(opt);
          });
        };

        // CALL POPULATION ONCE
        window.populateCategoryFilter(window.globalEvents);

        // Exposes the function
        window.applyMapFilters = window._applyMapFiltersImpl;

        // --- LIVE ACTIVATION (FUNDAMENTAL) ---
        const inputsToCheck = ['textSearch', 'actorFilter', 'chartTypeFilter', 'sectorFilter', 'startDate', 'endDate'];
        inputsToCheck.forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.oninput = window.applyMapFilters;
            el.onchange = window.applyMapFilters;
          }
        });

        // Add specific event listener for sector flyToBounds
        const sectorDropdown = document.getElementById('sectorFilter');
        if (sectorDropdown) {
          sectorDropdown.addEventListener('change', function (e) {
            const val = e.target.value;
            if (val && window.tacticalSectorsLayer && window.map) {
              const sectorEntries = getSectorEntries(val);
              const sectorBounds = buildSectorBounds(sectorEntries);

              if (sectorBounds) {
                window.map.flyToBounds(sectorBounds, { padding: [50, 50], duration: 1.5 });
              }

              sectorEntries.forEach(function (entry) {
                const layer = entry.layer;
                const oldStyle = Object.assign({}, layer.options);
                layer.setStyle({ fillOpacity: 0.2, weight: 3, color: '#f97316' });
                layer.addTo(window.map);
                setTimeout(() => {
                  if (window.map.hasLayer(layer)) window.map.removeLayer(layer);
                  layer.setStyle(oldStyle);
                }, 5000);
              });
            } else if (!val && window.map) {
              window.map.setView([48.5, 32.0], 6); // Reset view to default Zoom
            }

            updateAxisStatsPanel(
              val,
              Array.isArray(window.currentFilteredEvents) ? window.currentFilteredEvents : window.globalEvents
            );
          });
        }

        // UI Updates
        window.currentFilteredEvents = [...window.globalEvents];

        if (document.getElementById('eventCount')) {
          document.getElementById('eventCount').innerText = window.globalEvents.length;
          document.getElementById('lastUpdate').innerText = new Date().toLocaleDateString();
        }

        // Charts Init
        try {
          if (typeof window.initCharts === 'function') window.initCharts(window.globalEvents);
          // NEW: Dashboard Init
          if (window.Dashboard) window.Dashboard.init();
        } catch (e) { console.log("Charts/Dashboard error:", e); }

        console.log(`Ã¢Å“â€¦ Events processed: ${window.globalEvents.length} ready for map`);

        // Slider Init
        if (typeof setupTimeSlider === 'function') setupTimeSlider(window.globalEvents);

        // 4. START MAP AND RENDERING
        if (typeof window.applyMapFilters === 'function') {
          window.applyMapFilters();
        } else {
          renderInternal(window.globalEvents);
        }

        // Initialize Cluster/Map
        initMap(window.globalEvents);

      }) // <--- THIS CLOSES THE .THEN (The critical point of previous errors)
      .catch(err => {
        console.error("Ã¢ÂÅ’ CRITICAL: Failed to load events:", err);
      });
  }

  // ============================================
  // 9. PUBLIC API (Expose to Window)
  // ============================================



  window.updateMap = function (events) {
    window.currentFilteredEvents = events;
    renderInternal(events);
  }

  window.updateAxisStatsPanel = updateAxisStatsPanel;
  window.loadHistoricalMap = loadHistoricalMap;
  window.filterEventsByDate = filterEventsByDate;

  // ============================================
  // TASK 1: TACTICAL TIME COMMAND (Global API)
  // ============================================
  window.setTimeWindow = function (hours) {
    tacticalTimeWindowHours = hours;
    const customInput = document.getElementById('ttbCustomHours');
    const isPreset = [0, 24, 48, 72].includes(hours);

    // Update preset button states
    document.querySelectorAll('.ttb-btn').forEach(btn => {
      btn.classList.toggle('active', isPreset && parseInt(btn.dataset.hours) === hours);
    });

    // Sync custom input: clear if preset was clicked, show value if custom
    if (customInput) {
      if (isPreset) {
        customInput.value = '';
      } else {
        customInput.value = hours;
      }
    }

    // Re-apply filters
    if (window.applyMapFilters) window.applyMapFilters();
    console.log(`\u23F0 Time Window set to: ${hours === 0 ? 'ALL' : hours + 'H'}${isPreset ? '' : ' (custom)'}`);
  };

  window.toggleLowReputationVisibility = function () {
    window.hideLowReputation = !window.hideLowReputation;
    const btn = document.getElementById('lowRepFilterBtn');
    if (btn) btn.classList.toggle('active', !window.hideLowReputation);
    if (window.applyMapFilters) window.applyMapFilters();
  };

  window.toggleTacticalPersistence = function () {
    const checkbox = document.getElementById('ttbPersistence');
    tacticalPersistence = checkbox ? checkbox.checked : !tacticalPersistence;
    if (window.applyMapFilters) window.applyMapFilters();
    console.log(`\uD83D\uDD12 Tactical Persistence: ${tacticalPersistence ? 'ON' : 'OFF'}`);
  };

  // ============================================
  // TASK 4: QUICK SEARCH (flyTo Logic)
  // ============================================
  (function initQuickSearch() {
    const input = document.getElementById('quickSearchInput');
    const resultsBox = document.getElementById('quickSearchResults');
    if (!input || !resultsBox) return;

    let debounceTimer = null;

    input.addEventListener('input', function () {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => performQuickSearch(input.value.trim()), 250);
    });

    // Close results on outside click
    document.addEventListener('click', function (ev) {
      if (!ev.target.closest('.map-quick-search')) {
        resultsBox.classList.remove('visible');
      }
    });

    function performQuickSearch(query) {
      if (query.length < 2) {
        resultsBox.classList.remove('visible');
        return;
      }

      const q = query.toLowerCase();
      const results = [];

      // 1. Search ORBAT units
      if (window.orbatData && Array.isArray(window.orbatData)) {
        window.orbatData.forEach(unit => {
          const name = (unit.display_name || unit.unit_name || unit.unit_id || '').toLowerCase();
          if (name.includes(q)) {
            const lat = unit.last_lat || unit.lat;
            const lon = unit.last_lon || unit.lon;
            if (lat && lon) {
              results.push({
                label: unit.display_name || unit.unit_name || unit.unit_id,
                type: 'UNIT',
                lat: parseFloat(lat),
                lon: parseFloat(lon),
                _unitData: unit
              });
            }
          }
        });
      }

      // 2. Search OWL unit positions
      if (window.owlData && window.owlData.units) {
        window.owlData.units.forEach((feature, key) => {
          const name = (feature.properties?.name || key || '').toLowerCase();
          if (name.includes(q) && feature.geometry?.coordinates) {
            results.push({
              label: feature.properties?.name || key,
              type: 'OWL',
              lat: feature.geometry.coordinates[1],
              lon: feature.geometry.coordinates[0],
              _unitData: Object.assign({}, feature.properties, { 
                  owl_meta: feature.properties,
                  display_name: feature.properties?.name,
                  faction: feature.properties?.side,
                  lat: feature.geometry.coordinates[1],
                  lon: feature.geometry.coordinates[0]
                })
            });
          }
        });
      }



      // 4. Search event titles and descriptions (for cities)
      if (window.globalEvents) {
        window.globalEvents.forEach(evt => {
          const title = (evt.title || '').toLowerCase();
          const desc = (evt.description || '').toLowerCase();
          if (title.includes(q) || desc.includes(q)) {
            results.push({
              label: evt.title || 'Event',
              type: 'EVENT',
              lat: evt.lat,
              lon: evt.lon,
              _eventData: evt
            });
          }
        });
      }

      // Render results (max 8, prioritize: UNIT > CITY > EVENT)
      const sorted = results.sort((a, b) => {
        const order = { UNIT: 0, OWL: 1, CITY: 2, EVENT: 3 };
        return (order[a.type] || 9) - (order[b.type] || 9);
      });
      const limited = sorted.slice(0, 8);
      if (limited.length === 0) {
        resultsBox.classList.remove('visible');
        return;
      }

      // Store reference data for click handlers
      window._qsResults = limited;

      resultsBox.innerHTML = limited.map((r, idx) => `
        <div class="qs-item" data-idx="${idx}" data-lat="${r.lat}" data-lon="${r.lon}">
          <span class="qs-type">${r.type}</span>
          <span>${r.label.length > 45 ? r.label.substring(0, 45) + '...' : r.label}</span>
        </div>
      `).join('');

      resultsBox.classList.add('visible');

      // Bind click handlers
      resultsBox.querySelectorAll('.qs-item').forEach(item => {
        item.addEventListener('click', () => {
          const idx = parseInt(item.dataset.idx);
          const r = window._qsResults[idx];
          const lat = parseFloat(item.dataset.lat);
          const lon = parseFloat(item.dataset.lon);

          // FlyTo in all cases
          if (map && !isNaN(lat) && !isNaN(lon)) {
            map.flyTo([lat, lon], 12, { duration: 1.5 });
          }

          // Open modal based on type
          if (r && r.type === 'EVENT' && r._eventData && window.openModal) {
            setTimeout(() => window.openModal(r._eventData), 800);
          } else if (r && (r.type === 'UNIT' || r.type === 'OWL') && r._unitData && window.openUnitModal) {
            setTimeout(() => window.openUnitModal(r._unitData), 800);
          }

          resultsBox.classList.remove('visible');
          input.value = '';
        });
      });
    }
  })();

  window.toggleVisualMode = function () {
    isHeatmapMode = !isHeatmapMode;
    // Handle both old and new toggle buttons if they exist
    const btn = document.getElementById('heatmapToggle');
    const checkbox = document.getElementById('dashboardHeatmapToggle');

    if (isHeatmapMode) {
      if (btn) {
        btn.classList.add('active');
        btn.innerHTML = '<i class="fa-solid fa-circle-nodes"></i> Cluster';
      }
      if (checkbox) checkbox.checked = true;
    } else {
      if (btn) {
        btn.classList.remove('active');
        btn.innerHTML = '<i class="fa-solid fa-layer-group"></i> Heatmap';
      }
      if (checkbox) checkbox.checked = false;
    }

    renderInternal(window.currentFilteredEvents);
  };

  window.flyToUnit = function (lat, lon, unitName) {
    if (!map) return;
    if (!lat || !lon) {
      console.warn("Unit has no coordinates");
      return;
    }

    map.flyTo([lat, lon], 10, { animate: true, duration: 1.5 });

    // Create a temporary pulse marker
    const popup = L.popup()
      .setLatLng([lat, lon])
      .setContent(`<div style="text-align:center"><b>${unitName}</b><br>Last Known Position</div>`)
      .openOn(map);
  };

  window.selectMapSource = function (card, sourceName) {
    document.querySelectorAll('.map-layer-card').forEach(c => {
      c.classList.remove('active');
      const icon = c.querySelector('.status-dot');
      if (icon) {
        icon.classList.remove('fa-circle-dot', 'fa-solid');
        icon.classList.add('fa-circle', 'fa-regular');
      }
    });

    if (card) {
      card.classList.add('active');
      const activeIcon = card.querySelector('.status-dot');
      if (activeIcon) {
        activeIcon.classList.remove('fa-circle', 'fa-regular');
        activeIcon.classList.add('fa-circle-dot', 'fa-solid');
      }
    }

    console.log(`Ã°Å¸â€â€ž Switching map source: ${sourceName}`);

    let dataUrl = '';
    let colorStyle = '#ff3838';

    if (sourceName === 'deepstate') {
      // NOW MAPPED TO OWL (Primary Source)
      loadOwlLayer();
      colorStyle = '#f59e0b';
    } else if (sourceName === 'isw') {
      // Fallback or secondary
      dataUrl = 'assets/data/frontline_isw.geojson';
      colorStyle = '#38bdf8';
      loadFrontlineLayer(dataUrl, colorStyle);
    } else {
      loadOwlLayer();
    }
  };

  window.toggleTechLayer = function (layerName, checkbox) {
    const isChecked = checkbox ? checkbox.checked : false;
    // FIX: Use closure variable 'map', DO NOT redefine or use window.map unless initialized
    // const map = window.map; <--- REMOVED this bug

    console.log(`Toggling layer: ${layerName} -> ${isChecked}`);

    if (layerName === 'radar') {
      if (isChecked) {
        window.initWeatherRadar();
      } else {
        window.stopWeatherRadar();
      }
    } else if (layerName === 'vfr') {
      if (isChecked) {
        window.showVFR();
      } else {
        window.hideVFR();
      }
    } else if (layerName === 'satellite') {
      if (isChecked) {
        if (!window.satelliteLayer) {
          window.satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: 'Esri'
          }).addTo(map);
        } else {
          map.addLayer(window.satelliteLayer);
        }
      } else {
        if (window.satelliteLayer) map.removeLayer(window.satelliteLayer);
      }
    } else if (layerName === 'events') {
      // Toggle MarkerCluster (Using closure variable 'eventsLayer')
      if (isChecked) {
        if (eventsLayer) map.addLayer(eventsLayer);
      } else {
        if (eventsLayer) map.removeLayer(eventsLayer);
      }
    }

    if (layerName === 'firms') {
      if (isChecked) {
        // Use cached FIRMS support data when available.
        loadAxisThermalFeatures()
          .then(features => {
            if (!features || features.length === 0) {
              console.warn("Ã¢Å¡Â Ã¯Â¸Â No recent FIRMS data available inside Ukraine/Russia land territory");
              return;
            }

            // Create layer group for thermal hotspots
            if (firmsLayer) {
              map.removeLayer(firmsLayer);
            }
            firmsLayer = L.layerGroup();

            let visibleHotspots = 0;
            features.forEach(feature => {
              visibleHotspots += 1;
              const coords = [feature.lon, feature.lat];
              const props = feature.properties || {};
              const brightness = props.brightness || 300;

              // Color based on brightness (hotter = more red)
              let color = '#ff6b35'; // Default orange
              if (brightness >= 350) color = '#ff0000'; // Red hot
              else if (brightness >= 330) color = '#ff4500'; // Orange-red
              else if (brightness >= 310) color = '#ff6b35'; // Orange
              else color = '#ffa500'; // Yellow-orange

              const marker = L.circleMarker([coords[1], coords[0]], {
                radius: 6,
                fillColor: color,
                color: '#000',
                weight: 1,
                opacity: 0.8,
                fillOpacity: 0.7
              });

              // Popup with dossier-matching styling
              const confidenceLabel = props.confidence === 'h' ? 'HIGH' : props.confidence === 'l' ? 'LOW' : 'NOMINAL';
              const confidenceColor = props.confidence === 'h' ? '#22c55e' : props.confidence === 'l' ? '#ef4444' : '#f59e0b';
              const intensityPercent = Math.min(100, ((brightness - 280) / 120) * 100);
              const frpValue = props.frp || 0;
              const timeFormatted = props.acq_time ? `${String(props.acq_time).padStart(4, '0').slice(0, 2)}:${String(props.acq_time).padStart(4, '0').slice(2)} UTC` : 'N/A';

              marker.bindPopup(`
                <div class="firms-popup-content" style="
                  min-width: 260px;
                  max-width: 320px;
                  font-family: 'Inter', sans-serif;
                  background: #0f172a;
                  border-radius: 8px;
                  overflow: visible;
                  border: 1px solid #334155;
                ">
                  <!-- Header matching dossier style -->
                  <div style="
                    background: linear-gradient(90deg, ${color}, #d97706);
                    padding: 16px;
                    border-bottom: 1px solid rgba(0,0,0,0.3);
                  ">
                    <div style="
                      font-size: 1.1rem;
                      font-weight: 700;
                      line-height: 1.3;
                      color: #fff;
                    ">Thermal Anomaly Detected</div>
                    <div style="
                      display: flex;
                      align-items: center;
                      gap: 8px;
                      margin-top: 6px;
                      font-size: 0.8rem;
                      color: rgba(255,255,255,0.8);
                    ">
                      <span>${props.acq_date || 'N/A'}</span>
                      <span style="color: rgba(255,255,255,0.4);">|</span>
                      <span style="
                        background: rgba(0,0,0,0.25);
                        padding: 2px 8px;
                        border-radius: 4px;
                        font-weight: 600;
                        font-size: 0.7rem;
                      ">NASA FIRMS</span>
                    </div>
                  </div>
                  
                  <!-- Body -->
                  <div style="padding: 16px;">
                    <!-- Brightness Meter -->
                    <div style="margin-bottom: 16px;">
                      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                        <div style="display: flex; align-items: center; gap: 4px;">
                          <span style="font-size: 0.65rem; color: #94a3b8; text-transform: uppercase; font-weight: 700;">Brightness Temperature</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Brightness Temperature</div>
                              <div class="tooltip-body">Temperature measured by satellite infrared sensor. Higher values indicate more intense thermal radiation from fires or explosions.</div>
                              <div class="tooltip-footer">Scale: 280K (cool) to 400K+ (intense)</div>
                            </div>
                          </div>
                        </div>
                        <span style="
                          font-family: 'JetBrains Mono', monospace;
                          color: ${color};
                          font-weight: 700;
                          font-size: 1.1rem;
                        ">${brightness.toFixed(0)} K</span>
                      </div>
                      <div style="background: #1e293b; border-radius: 4px; height: 6px; overflow: hidden;">
                        <div style="
                          width: ${intensityPercent}%;
                          height: 100%;
                          background: linear-gradient(90deg, #ffa500, ${color});
                          border-radius: 4px;
                        "></div>
                      </div>
                    </div>
                    
                    <!-- Stats Grid -->
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">Satellite</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Satellite Source</div>
                              <div class="tooltip-body">NASA satellite that captured this detection. VIIRS sensors provide 375m spatial resolution.</div>
                              <div class="tooltip-footer">Updated every 12 hours per satellite.</div>
                            </div>
                          </div>
                        </div>
                        <div style="color: #f8fafc; font-weight: 600; font-size: 0.85rem;">${props.satellite || 'VIIRS'}</div>
                      </div>
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">FRP</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Fire Radiative Power</div>
                              <div class="tooltip-body">Rate of radiant energy released, measured in Megawatts. Higher FRP indicates larger or more intense fires.</div>
                              <div class="tooltip-footer">Typical range: 1-50 MW for fires.</div>
                            </div>
                          </div>
                        </div>
                        <div style="color: #f8fafc; font-weight: 600; font-size: 0.85rem; font-family: 'JetBrains Mono', monospace;">${frpValue.toFixed(1)} MW</div>
                      </div>
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">Detection Time</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Acquisition Time</div>
                              <div class="tooltip-body">Time when the satellite sensor detected this thermal anomaly, in Coordinated Universal Time.</div>
                              <div class="tooltip-footer">Detection window is ~5 minutes.</div>
                            </div>
                          </div>
                        </div>
                        <div style="color: #f8fafc; font-weight: 600; font-size: 0.85rem; font-family: 'JetBrains Mono', monospace;">${timeFormatted}</div>
                      </div>
                      <div style="background: rgba(30,41,59,0.5); padding: 10px; border-radius: 6px; border: 1px solid #334155;">
                        <div style="display: flex; align-items: center; gap: 4px; margin-bottom: 4px;">
                          <span style="color: #94a3b8; font-size: 0.6rem; text-transform: uppercase; font-weight: 700;">Confidence</span>
                          <div class="info-icon-wrapper">
                            <div class="info-icon">i</div>
                            <div class="tooltip-card">
                              <div class="tooltip-header">Detection Confidence</div>
                              <div class="tooltip-body">Algorithm confidence level. High = strong thermal signature. Low = possible false positive from industrial activity.</div>
                              <div class="tooltip-footer">Based on NASA FIRMS algorithm.</div>
                            </div>
                          </div>
                        </div>
                        <div style="
                          color: ${confidenceColor};
                          font-weight: 700;
                          font-size: 0.85rem;
                        ">${confidenceLabel}</div>
                      </div>
                    </div>
                    
                    <!-- Coordinates Footer -->
                    <div style="margin-top: 12px; padding-top: 10px; border-top: 1px solid #334155; text-align: center;">
                      <span style="color: #64748b; font-size: 0.7rem; font-family: 'JetBrains Mono', monospace;">
                        ${coords[1].toFixed(5)}Ã‚Â°N, ${coords[0].toFixed(5)}Ã‚Â°E
                      </span>
                    </div>
                  </div>
                </div>
              `, {
                className: 'firms-popup',
                maxWidth: 340,
                minWidth: 260
              });

              firmsLayer.addLayer(marker);
            });

            firmsLayer.addTo(map);
            console.log(`Ã¢Å“â€¦ FIRMS layer loaded: ${visibleHotspots}/${features.length} recent in-theater hotspots`);

            // Show metadata info
            if (window.axisThermalMetadata) {
              console.log(`   Source: ${window.axisThermalMetadata.source}`);
              console.log(`   Generated: ${window.axisThermalMetadata.generated}`);
            }
          })
          .catch(err => {
            console.error("Ã¢ÂÅ’ Failed to load FIRMS data:", err);
          });
      } else {
        if (firmsLayer) {
          map.removeLayer(firmsLayer);
          firmsLayer = null;
        }
      }
    } else if (layerName === 'fortifications') {
      // PARABELLUM FORTIFICATIONS (Repaired Data)
      if (isChecked) {
        if (!window.fortificationsLayer) {
          console.log("Ã°Å¸â€ºÂ¡Ã¯Â¸Â Loading Fortifications (Dragon's Teeth)...");
          fetch('assets/data/fortifications_parabellum.geojson')
            .then(r => r.json())
            .then(data => {
              // Canvas Renderer for performance (80k features!)
              const canvasRenderer = L.canvas();
              window.fortificationsLayer = L.geoJSON(data, {
                renderer: canvasRenderer,
                style: styleFortifications, // Use new Dragon's Teeth style
                onEachFeature: function (f, l) {
                  l.bindPopup("<b>Fortification Line</b><br>Dragon's Teeth / Trench System");
                }
              }).addTo(map);
              console.log(`Ã¢Å“â€¦ Fortifications loaded: ${data.features.length}`);
            })
            .catch(e => console.error("Fortification load failed:", e));
        } else {
          map.addLayer(window.fortificationsLayer);
        }
      } else {
        if (window.fortificationsLayer) {
          map.removeLayer(window.fortificationsLayer);
          window.fortificationsLayer = null;
        }
      }

    } else if (layerName === 'units') {
      // ORBAT Units Layer - PARABELLUM PRIMARY + OWL ENRICHMENT
      if (isChecked) {
        console.log("Ã°Å¸Å¡â‚¬ STARTING UNITS FETCH (PARABELLUM PRIMARY)...");

        // 1. Fetch Parabellum Data (Primary Ã¢â‚¬â€ accurate WFS positions)
        fetch(`assets/data/orbat_units.json?v=${new Date().getTime()}`)
          .then(res => res.json())
          .then(async userUnits => {
            if (!userUnits) userUnits = [];

            // 2. Fetch Owl Data (Authoritative Live)
            const owl = window.owlData || await fetchOwlData();

            // 3. UPSERT LOGIC
            const mergedUnits = new Map();

            // Helper: Extract unit numeric ID
            const getUnitNum = (str) => { const m = (str||'').match(/\d+/); return m ? m[0] : null; };
            // Helper: Clean string for loose matching
            const cleanStr = (str) => (str || '').toLowerCase().replace(/separate|mechanized|brigade|regiment|infantry|marine|assault|airborne|battalion|group|tactical|naval|forces|motorized|rifle/g, '').replace(/[^a-z0-9]/g, '');

            // A. Load Parabellum Units First (authoritative positions)
            const parabellumKeys = [];
            userUnits.forEach(u => {
              const nameIdx = (u.unit_name || u.full_name_en || u.orbat_id || '').trim().toLowerCase();
              mergedUnits.set(nameIdx, u);
              parabellumKeys.push(nameIdx);
            });

            // B. Enrich from Owl (METADATA ONLY — no coordinate injection)
            if (owl && owl.units) {
              let enrichCount = 0;
              owl.units.forEach((feature, owlKeyOriginal) => {
                const owlName = (feature.properties.name || owlKeyOriginal || '').trim().toLowerCase();
                const owlNum = getUnitNum(owlName);
                const owlClean = cleanStr(owlName);

                let matchedKey = null;

                // Match based on Number + Branch heuristic
                // Strict rule: they MUST share the same numeric ID if numbers exist.
                matchedKey = parabellumKeys.find(pkey => {
                    const pNum = getUnitNum(pkey);
                    // Match numbers
                    if (pNum !== owlNum) return false; 
                    
                    // If both have numbers and they match, check strings
                    if (pNum && owlNum) {
                        const pClean = cleanStr(pkey);
                        if (pClean.length === 0 || owlClean.length === 0) return true; // If only number exists, trust it.
                        if (pClean.length > 2 && owlClean.includes(pClean)) return true;
                        if (owlClean.length > 2 && pClean.includes(owlClean)) return true;
                        return false;
                    }
                    
                    // If neither has numbers, do string match
                    if (!pNum && !owlNum) {
                        if (pkey === owlName) return true;
                        if (pkey.length > 5 && owlName.includes(pkey)) return true;
                        if (owlName.length > 5 && pkey.includes(owlName)) return true;
                    }
                    
                    return false;
                });

                if (matchedKey) {
                  const existing = mergedUnits.get(matchedKey);
                  if (existing) {
                    // ENRICH: Add Owl metadata only.
                    existing.owl_meta = feature.properties;
                    existing.owl_garrison_lat = feature.geometry.coordinates[1];
                    existing.owl_garrison_lon = feature.geometry.coordinates[0];
                    enrichCount++;
                  }
                }
              });
              console.log(`🦉 Owl enriched ${enrichCount} units with metadata.`);
            }
            const finalData = Array.from(mergedUnits.values());
            console.log(`Ã¢Å“â€¦ Final Units: ${finalData.length} (Parabellum: ${userUnits.length})`);

            // 4. RENDER (Cluster)
            unitsLayer = L.markerClusterGroup({
              maxClusterRadius: 50,
              spiderfyOnMaxZoom: true,
              showCoverageOnHover: false,
              zoomToBoundsOnClick: true,
              iconCreateFunction: function (cluster) {
                const markers = cluster.getAllChildMarkers();
                let uaCount = 0, ruCount = 0;
                markers.forEach(m => {
                  if (m.options.faction === 'UA') uaCount++;
                  else ruCount++;
                });
                const total = markers.length;
                const color = uaCount > ruCount ? '#3b82f6' : '#ef4444';
                return L.divIcon({
                  html: `<div style="
                    background: ${color};
                    color: white;
                    border-radius: 50%;
                    width: 36px;
                    height: 36px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-weight: bold;
                    font-size: 12px;
                    border: 2px solid white;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.4);
                  ">${total}</div>`,
                  className: 'unit-cluster-icon',
                  iconSize: [36, 36]
                });
              }
            });

            finalData.forEach(unit => {
              const lat = unit.lat || unit.last_seen_lat;
              const lon = unit.lon || unit.last_seen_lon;
              if (!lat || !lon) return;

              // Determine faction
              const isUA = unit.faction === 'UA';
              const isRU = unit.faction === 'RU' || unit.faction === 'RU_PROXY' || unit.faction === 'RU_PMC';
              const color = isUA ? '#3b82f6' : (isRU ? '#ef4444' : '#64748b');

              const uaFlag = `<svg width="20" height="14" viewBox="0 0 20 14"><rect width="20" height="7" fill="#005BBB"/><rect y="7" width="20" height="7" fill="#FFD500"/></svg>`;
              const ruFlag = `<svg width="20" height="14" viewBox="0 0 20 14"><rect width="20" height="4.67" fill="#fff"/><rect y="4.67" width="20" height="4.67" fill="#0039A6"/><rect y="9.33" width="20" height="4.67" fill="#D52B1E"/></svg>`;
              const unknownFlag = `<svg width="20" height="14" viewBox="0 0 20 14"><rect width="20" height="14" fill="#64748b"/></svg>`;
              const owlBadge = unit.source === 'OWL' ? `<div style="position:absolute; bottom:0; right:0; width:6px; height:6px; background:#f59e0b; border-radius:50%; border:1px solid #fff;"></div>` : '';

              const flagSvg = isUA ? uaFlag : (isRU ? ruFlag : unknownFlag);

              const icon = L.divIcon({
                html: `<div style="
                  filter: drop-shadow(0 1px 2px rgba(0,0,0,0.5));
                  border-radius: 2px;
                  overflow: visible; 
                  position: relative;
                ">${flagSvg}${owlBadge}</div>`, // Added Owl dot for live units
                className: 'unit-flag-marker',
                iconSize: [20, 14],
                iconAnchor: [10, 7]
              });

              const marker = L.marker([lat, lon], {
                icon: icon,
                faction: unit.faction,
                unitData: unit,
                zIndexOffset: 1000
              });

              unitsLayer.addLayer(marker);
            });

            // Click Listener
            unitsLayer.on('click', function (a) {
              const unit = a.layer.options.unitData;
              if (window.openUnitModal) {
                window.openUnitModal(unit);
              }
            });

            unitsLayer.addTo(map);
          })
          .catch(err => {
            console.error("Ã¢ÂÅ’ Failed to load Units:", err);
            alert("Error loading units.");
          });
      } else {
        if (unitsLayer) {
          map.removeLayer(unitsLayer);
          unitsLayer = null;
        }
      }

    } else if (layerName === 'narratives') {
      // ============================================
      // STRATEGIC CONTEXT LAYER (Narrative Polygons)
      // ============================================
      if (isChecked) {
        console.log("Loading Strategic Context layer...");

        if (narrativesLayer) {
          map.removeLayer(narrativesLayer);
          narrativesLayer = null;
        }

        fetch(`assets/data/narratives.json?v=${new Date().getTime()}`)
          .then(response => {
            if (!response.ok) {
              console.warn("narratives.json not found (run semantic_cluster.py first)");
              throw new Error("HTTP 404");
            }
            return response.json();
          })
          .then(data => {
            if (!data.narratives || data.narratives.length === 0) {
              console.warn("No narratives available");
              return;
            }

            console.log(`Loaded ${data.narratives.length} strategic narratives`);
            narrativesLayer = L.layerGroup();

            // Minimum radius in degrees (~5km at Ukraine's latitude)
            const MIN_RADIUS_DEG = 0.05;

            data.narratives.forEach(narrative => {
              const meta = narrative.meta;
              const geometry = narrative.geometry;
              const centroid = narrative.centroid_latlon || narrative.centroid || narrative.centroid_geojson;

              if (!geometry || !geometry.coordinates || !centroid) return;

              const markerCenter = normalizeNarrativeCentroid(centroid, geometry);
              if (!markerCenter) return;

              // Convert GeoJSON coordinates to Leaflet format [lat, lng]
              let coords = getNarrativePolygonLatLngs(geometry);
              if (!coords || coords.length < 3) return;

              // Calculate bounding box to check if polygon is too small
              const lats = coords.map(c => c[0]);
              const lngs = coords.map(c => c[1]);
              const latSpan = Math.max(...lats) - Math.min(...lats);
              const lngSpan = Math.max(...lngs) - Math.min(...lngs);

              // If polygon is too small, create a circle-like polygon around centroid
              if (latSpan < MIN_RADIUS_DEG && lngSpan < MIN_RADIUS_DEG) {
                const centerLat = markerCenter[0];
                const centerLng = markerCenter[1];
                const numPoints = 32;
                coords = [];
                for (let i = 0; i < numPoints; i++) {
                  const angle = (i / numPoints) * 2 * Math.PI;
                  const lat = centerLat + MIN_RADIUS_DEG * Math.sin(angle);
                  const lng = centerLng + MIN_RADIUS_DEG * Math.cos(angle) * 1.5; // Adjust for lat
                  coords.push([lat, lng]);
                }
              }

              // Create styled polygon (Wireframe Zone)
              const polygon = L.polygon(coords, {
                color: meta.tactic_color || '#94a3b8',
                weight: 1,
                opacity: 0.6,
                fillColor: meta.tactic_color || '#94a3b8',
                fillOpacity: 0.05,
                className: 'narrative-polygon', // Dashed line via CSS
                dashArray: '4, 8'
              });

              // Create Tactical Hex Marker at Centroid
              const markerHtml = `
                <div style="width:100%; height:100%; display:flex; align-items:center; justify-content:center; position:relative;">
                  ${meta.intensity >= 7 ? `<div class="pulse-emitter" style="background: ${meta.tactic_color};"></div>` : ''}
                  ${meta.intensity >= 4 ? `<div class="narrative-marker-ring" style="border-color: ${meta.tactic_color}; box-shadow: 0 0 10px ${meta.tactic_color}44;"></div>` : ''}
                  <div class="narrative-marker-hex" style="border: 1px solid ${meta.tactic_color}; color: ${meta.tactic_color}; box-shadow: 0 0 10px ${meta.tactic_color}66;">
                    <div class="hex-icon" style="font-size: 10px;">&#10148;</div>
                    <div class="hex-score">${meta.intensity.toFixed(1)}</div>
                  </div>
                </div>
              `;

              const markerIcon = L.divIcon({
                className: 'narrative-marker-container',
                html: markerHtml,
                iconSize: [50, 50],
                iconAnchor: [25, 25]
              });

              const marker = L.marker(markerCenter, {
                icon: markerIcon,
                zIndexOffset: 1000, // Always on top
                noWrap: true
              });

              // Hover effects
              polygon.on('mouseover', function (e) {
                this.setStyle({ weight: 2, fillOpacity: 0.15 });
              });
              polygon.on('mouseout', function (e) {
                this.setStyle({ weight: 1, fillOpacity: 0.05 });
              });

              // Shared Popup Logic
              const openPopup = (e) => {
                L.DomEvent.stopPropagation(e);

                const intensityClass = meta.intensity >= 7 ? 'CRITICAL' :
                  meta.intensity >= 5 ? 'HIGH' :
                    meta.intensity >= 3 ? 'MODERATE' : 'LOW';
                const intensityColor = meta.intensity >= 7 ? '#ef4444' :
                  meta.intensity >= 5 ? '#f97316' :
                    meta.intensity >= 3 ? '#eab308' : '#64748b';

                // Build tactical kill chain from correlated events
                const eventIds = narrative.event_ids || [];
                let killChainHtml = '';
                if (eventIds.length > 0 && window.globalEvents) {
                  // Find matching events for the kill chain
                  const matchedEvents = [];
                  eventIds.forEach(eid => {
                    const found = window.globalEvents.find(e => e.event_id === eid || e.id === eid);
                    if (found) matchedEvents.push(found);
                  });
                  // Show up to 6 events as kill chain nodes
                  matchedEvents.slice(0, 6).forEach((ev, idx) => {
                    const evTitle = (ev.title || ev.event_title || '').substring(0, 60);
                    const evDate = (ev.date || '').substring(0, 10);
                    const evTie = (parseFloat(ev.tie_total || 0) / 10).toFixed(1);
                    const evCat = (ev.category || ev.classification || 'UNK').toUpperCase();
                    const isActive = parseFloat(ev.tie_total || 0) >= 50;
                    const nodeColor = isActive ? '#f59e0b' : '#94a3b8';
                    const nodeIcon = isActive ? 'fas fa-crosshairs' : 'fas fa-square';
                    const glowStyle = isActive ? 'text-shadow: 0 0 8px #f59e0b, 0 0 16px #f59e0b44;' : '';
                    killChainHtml += `
                      <div style="display:flex; gap:8px; position:relative;">
                        <div style="display:flex; flex-direction:column; align-items:center; width:16px;">
                          <i class="${nodeIcon}" style="color:${nodeColor}; font-size:0.6rem; ${glowStyle} z-index:1;"></i>
                          ${idx < Math.min(matchedEvents.length, 6) - 1 ? '<div style="flex:1; border-left:2px dashed #475569; min-height:30px;"></div>' : ''}
                        </div>
                        <div style="flex:1; padding:3px 0 8px 0;">
                          <div style="display:flex; align-items:center; gap:4px; margin-bottom:2px;">
                            <span style="font-family:JetBrains Mono,monospace; font-size:0.52rem; color:#64748b; text-transform:uppercase;">${evDate}</span>
                            <span style="font-family:JetBrains Mono,monospace; font-size:0.5rem; color:${nodeColor}; font-weight:700;">T.I.E. ${evTie}</span>
                          </div>
                          <div style="font-size:0.62rem; color:#e2e8f0; font-weight:600; line-height:1.3;">${evTitle}</div>
                          <span style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#94a3b8; text-transform:uppercase;">${evCat}</span>
                        </div>
                      </div>`;
                  });
                }

                const durationDays = meta.date_range ? Math.ceil((new Date(meta.date_range[1]) - new Date(meta.date_range[0])) / (1000 * 60 * 60 * 24)) : 0;

                const popupContent = `
                  <div style="font-family:'Inter',sans-serif; background:#0f172a; border:1px solid #334155; border-radius:6px; overflow:hidden; overflow-y:auto; max-height:100%;">
                    <!-- CLOSE BUTTON -->
                    <div style="position:sticky; top:0; z-index:10; display:flex; justify-content:flex-end; padding:4px 6px; background:#0f172a;">
                      <button onclick="navSwitchTab('layers', document.querySelector('[data-panel=layers]'))" style="background:rgba(100,116,139,0.2); border:1px solid #475569; border-radius:4px; color:#94a3b8; cursor:pointer; padding:3px 8px; font-family:JetBrains Mono,monospace; font-size:0.6rem; text-transform:uppercase;">
                        <i class="fas fa-times" style="margin-right:3px;"></i>CLOSE
                      </button>
                    </div>
                    <!-- HERO HEADER (dark forest green) -->
                    <div style="background:#0f291e; padding:10px 12px; border-bottom:1px solid #1a3a28;">
                      <div style="font-family:JetBrains Mono,monospace; font-size:0.5rem; color:#64748b; text-transform:uppercase; letter-spacing:0.15em; margin-bottom:4px;">CLUSTER DOSSIER</div>
                      <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:8px;">
                        <div style="flex:1;">
                          <div style="font-size:0.9rem; font-weight:700; color:#f8fafc; line-height:1.3; margin-bottom:4px;">${meta.title}</div>
                          <div style="font-family:JetBrains Mono,monospace; font-size:0.52rem; color:#94a3b8; text-transform:uppercase;">
                            ${narrative.cluster_id} | ${meta.event_count} EVENTS
                          </div>
                        </div>
                        <!-- TIE SCORE WIDGET -->
                        <div style="border:2px solid #f59e0b; border-radius:6px; padding:6px 10px; text-align:center; min-width:48px; background:rgba(245,158,11,0.08);">
                          <div style="font-family:JetBrains Mono,monospace; font-size:1.1rem; font-weight:800; color:#f59e0b;">${meta.intensity.toFixed(1)}</div>
                          <div style="font-family:JetBrains Mono,monospace; font-size:0.4rem; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em;">T.I.E.</div>
                        </div>
                      </div>
                    </div>

                    <!-- BADGES -->
                    <div style="padding:6px 12px; display:flex; flex-wrap:wrap; gap:4px; border-bottom:1px solid #1e293b;">
                      <span style="background:${meta.tactic_color}20; color:${meta.tactic_color}; padding:2px 8px; border-radius:3px; font-family:JetBrains Mono,monospace; font-size:0.52rem; font-weight:700; text-transform:uppercase;">${meta.primary_tactic}</span>
                      <span style="background:${intensityColor}15; color:${intensityColor}; padding:2px 8px; border-radius:3px; font-family:JetBrains Mono,monospace; font-size:0.52rem; font-weight:700; text-transform:uppercase;">${intensityClass}</span>
                      ${meta.strategic_context && meta.strategic_context !== 'UNKNOWN' ? `<span style="background:#1e293b; color:#94a3b8; padding:2px 8px; border-radius:3px; font-family:JetBrains Mono,monospace; font-size:0.52rem; font-weight:600;">${meta.strategic_context.replace(/_/g, ' ')}</span>` : ''}
                    </div>

                    <!-- 3-COLUMN STATS -->
                    <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:4px; padding:6px 12px; border-bottom:1px solid #1e293b;">
                      <div style="background:#1e293b; padding:6px; border-radius:4px; text-align:center;">
                        <div style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#64748b; text-transform:uppercase; font-weight:700; margin-bottom:2px;">EVENTS</div>
                        <div style="font-family:JetBrains Mono,monospace; font-size:0.85rem; font-weight:700; color:#f8fafc;">${meta.event_count}</div>
                      </div>
                      <div style="background:#1e293b; padding:6px; border-radius:4px; text-align:center;">
                        <div style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#64748b; text-transform:uppercase; font-weight:700; margin-bottom:2px;">DURATION</div>
                        <div style="font-family:JetBrains Mono,monospace; font-size:0.85rem; font-weight:700; color:#f8fafc;">${durationDays}d</div>
                      </div>
                      <div style="background:#1e293b; padding:6px; border-radius:4px; text-align:center;">
                        <div style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#64748b; text-transform:uppercase; font-weight:700; margin-bottom:2px;">CLUSTER</div>
                        <div style="font-family:JetBrains Mono,monospace; font-size:0.65rem; font-weight:600; color:#f8fafc;">${narrative.cluster_id.split('_').pop()}</div>
                      </div>
                    </div>

                    <!-- STRATEGIC ASSESSMENT -->
                    <div style="padding:8px 12px; border-bottom:1px solid #1e293b;">
                      <div style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#64748b; text-transform:uppercase; font-weight:700; letter-spacing:0.1em; margin-bottom:4px;">STRATEGIC ASSESSMENT</div>
                      <div style="color:#cbd5e1; font-size:0.7rem; line-height:1.5;">${meta.summary}</div>
                    </div>

                    <!-- TIME WINDOW -->
                    <div style="padding:6px 12px; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #1e293b;">
                      <span style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#64748b; text-transform:uppercase; font-weight:700;">TIME WINDOW</span>
                      <span style="font-family:JetBrains Mono,monospace; font-size:0.6rem; color:#f8fafc; font-weight:600;">
                        ${meta.date_range ? meta.date_range[0] + ' -- ' + meta.date_range[1] : 'N/A'}
                      </span>
                    </div>

                    <!-- TACTICAL KILL CHAIN -->
                    ${killChainHtml ? `
                    <div style="padding:8px 12px;">
                      <div style="font-family:JetBrains Mono,monospace; font-size:0.48rem; color:#64748b; text-transform:uppercase; font-weight:700; letter-spacing:0.1em; margin-bottom:6px;">
                        <i class="fas fa-link" style="margin-right:4px;"></i>TACTICAL KILL CHAIN (${eventIds.length} CORRELATED)
                      </div>
                      ${killChainHtml}
                    </div>` : ''}
                  </div>
                `;

                // Open in sidebar's active panel (per reference image) instead of map popup
                var sidebarPanel = document.querySelector('.sidebar-nav-panel[style*="display: flex"], .sidebar-nav-panel[style*="display:flex"]');
                // Hide any existing nav panels
                document.querySelectorAll('.sidebar-nav-panel').forEach(function (el) { el.style.display = 'none'; });
                // Hide the dashboard-container and sidebar-tabs
                var dc = document.querySelector('.dashboard-container');
                if (dc) dc.style.display = 'none';
                document.querySelectorAll('.sidebar-tabs').forEach(function (t) { t.style.display = 'none'; });
                document.querySelectorAll('.s-tab-content').forEach(function (c) { c.classList.remove('active'); });

                // Create or reuse a cluster dossier panel
                var clusterPanel = document.getElementById('sidebar-panel-cluster');
                if (!clusterPanel) {
                  clusterPanel = document.createElement('div');
                  clusterPanel.id = 'sidebar-panel-cluster';
                  clusterPanel.className = 'sidebar-nav-panel';
                  var activePanelBucket = document.querySelector('.active-panel') || document.querySelector('.sidebar-content-wrapper');
                  if (activePanelBucket) activePanelBucket.appendChild(clusterPanel);
                }
                clusterPanel.style.display = 'flex';
                clusterPanel.style.flexDirection = 'column';
                clusterPanel.style.height = '100%';
                clusterPanel.innerHTML = popupContent;

                // Ensure sidebar is visible
                var appShell = document.getElementById('appShell');
                if (appShell && appShell.classList.contains('sidebar-collapsed')) {
                  appShell.classList.remove('sidebar-collapsed');
                  setTimeout(function () { if (window.map && typeof window.map.invalidateSize === 'function') window.map.invalidateSize(); }, 400);
                }

                // Also zoom the map to the cluster centroid
                if (e.latlng && window.map) {
                  window.map.flyTo(e.latlng, 10, { animate: true, duration: 1.2 });
                }
              };

              polygon.on('click', openPopup);
              marker.on('click', openPopup);

              narrativesLayer.addLayer(polygon);
              narrativesLayer.addLayer(marker);
            });

            narrativesLayer.addTo(map);
            // Send to back so event markers stay on top
            narrativesLayer.bringToBack();

            console.log(`Strategic Context layer rendered: ${data.narratives.length} polygons`);
          })
          .catch(err => {
            console.error("Failed to load narratives:", err);
          });
      } else {
        if (narrativesLayer) {
          map.removeLayer(narrativesLayer);
          narrativesLayer = null;
        }
      }
    }
  };

  // ============================================
  // 9. WEATHER & FORTIFICATIONS (NEW)
  // ============================================

  // A. Fortifications Styling (Concrete / Defensive Line)
  function styleFortifications(feature) {
    return {
      color: '#52525b', // Zinc-600 (Concrete)
      weight: 3,
      opacity: 0.8,
      dashArray: '4, 8', // Dashed
      lineCap: 'square'
    };
  }

  // B. Weather Radar (Animated Loop)
  let radarInterval = null;
  let radarFrames = [];
  let currentFrameIndex = 0;

  window.initWeatherRadar = function () {
    if (window.radarLayer) return; // Already running

    console.log("Ã°Å¸Å’Â¦Ã¯Â¸Â Starting Weather Radar Loop (Fetching Metadata)...");

    // FETCH VALID TIMESTAMPS FROM RAINVIEWER API
    fetch('https://api.rainviewer.com/public/weather-maps.json')
      .then(response => response.json())
      .then(data => {
        // We use 'past' frames for the loop
        // data.radar.past is array of { time: UNIX_TIMESTAMP, path: ... }
        if (!data.radar || !data.radar.past) {
          console.error("Ã¢ÂÅ’ RainViewer Metadata invalid:", data);
          return;
        }

        const pastFrames = data.radar.past;
        console.log(`Ã¢Å“â€¦ Loaded ${pastFrames.length} radar frames from ${data.host}`);

        radarFrames = pastFrames.map(frame => {
          return L.tileLayer(`${data.host}${frame.path}/256/{z}/{x}/{y}/2/1_1.png`, {
            opacity: 0,
            attribution: 'RainViewer',
            zIndex: 500
          }).addTo(map);
        });

        window.radarLayer = L.layerGroup(radarFrames);

        // Animation Loop
        radarInterval = setInterval(() => {
          radarFrames.forEach(l => l.setOpacity(0)); // Hide all
          if (radarFrames[currentFrameIndex]) {
            radarFrames[currentFrameIndex].setOpacity(0.6); // Show current
          }
          currentFrameIndex = (currentFrameIndex + 1) % radarFrames.length;
        }, 500); // 0.5s per frame
      })
      .catch(e => console.error("Ã¢ÂÅ’ Weather Radar Metadata Fetch Failed:", e));
  };

  window.stopWeatherRadar = function () {
    if (radarInterval) clearInterval(radarInterval);
    if (radarFrames.length > 0) {
      radarFrames.forEach(l => map.removeLayer(l));
      radarFrames = [];
    }
    window.radarLayer = null;
    currentFrameIndex = 0;
    console.log("Ã°Å¸â€ºâ€˜ Weather Radar Stopped");
  };

  // C. Drone Visibility Index (V.F.R.)
  window.vfrLayer = null;

  window.showVFR = function () {
    if (window.vfrLayer) return;
    console.log("Ã°Å¸Å¡Â Fetching Drone V.F.R. Data...");

    // 5 Key frontline sectors
    const sectors = [
      { name: 'Kupiansk', lat: 49.7, lon: 37.6 },
      { name: 'Bakhmut', lat: 48.6, lon: 38.0 },
      { name: 'Donetsk', lat: 48.1, lon: 37.7 },
      { name: 'Zaporizhzhia', lat: 47.5, lon: 35.8 },
      { name: 'Kherson', lat: 46.6, lon: 32.6 }
    ];

    window.vfrLayer = L.layerGroup().addTo(map);

    sectors.forEach(sector => {
      fetch(`https://api.open-meteo.com/v1/forecast?latitude=${sector.lat}&longitude=${sector.lon}&current=cloud_cover,visibility`)
        .then(r => r.json())
        .then(data => {
          if (!data.current) return;
          const clouds = data.current.cloud_cover || 0; // percentage
          const vis = data.current.visibility || 10000; // meters

          // Drone ops degraded if clouds > 70% or visibility < 3000m
          const isDegraded = clouds > 70 || vis < 3000;

          if (isDegraded) {
            // Draw a hashed/solid dark mask over the sector
            const circle = L.circle([sector.lat, sector.lon], {
              color: '#0f172a',
              fillColor: '#1e293b',
              weight: 2,
              dashArray: '5, 10',
              fillOpacity: 0.6,
              radius: 40000 // 40km radius
            }).addTo(window.vfrLayer);

            // Add warning label
            const labelIcon = L.divIcon({
              className: 'vfr-label',
              html: `<div style="background: rgba(15, 23, 42, 0.8); border: 1px solid #ef4444; color: #ef4444; padding: 4px 8px; border-radius: 4px; font-size: 0.7rem; font-family: 'JetBrains Mono', monospace; font-weight: bold; white-space: nowrap; text-align: center;">
                                Ã¢Å¡Â Ã¯Â¸Â V.F.R. DEGRADED<br>
                                <span style="color:#94a3b8; font-size:0.6rem;">Clouds: ${clouds}% | Vis: ${(vis / 1000).toFixed(1)}km</span>
                             </div>`,
              iconSize: [120, 40],
              iconAnchor: [60, 20]
            });
            L.marker([sector.lat, sector.lon], { icon: labelIcon }).addTo(window.vfrLayer);
          }
        })
        .catch(e => console.error("VFR Fetch Error:", e));
    });
  };

  window.hideVFR = function () {
    if (window.vfrLayer) {
      map.removeLayer(window.vfrLayer);
      window.vfrLayer = null;
    }
  };

  // D. Physical Frontline (Open-Meteo) 3-Day Forecast
  window.fetchFrontlineWeather = function () {
    // Center of Frontline (approx Donbas)
    const lat = 48.0, lon = 37.8;

    console.log("Ã°Å¸Å’Â¡Ã¯Â¸Â Fetching Tactical Weather Forecast...");
    fetch(`https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&hourly=soil_moisture_0_1cm,soil_temperature_0cm&forecast_days=3`)
      .then(r => r.json())
      .then(data => {
        if (!data.hourly) return;

        // Helper to get status
        const getStatus = (temp, moisture) => {
          if (temp > 0 && moisture > 0.35) return { label: "MUD", color: "#ef4444" }; // Red
          if (temp <= -2) return { label: "FROZEN", color: "#3b82f6" }; // Blue
          if (moisture < 0.25) return { label: "DRY", color: "#eab308" }; // Yellow
          return { label: "OPTIMAL", color: "#22c55e" }; // Green
        };

        // Sample noon (12:00) for each of the 3 days
        const t1 = data.hourly.soil_temperature_0cm[12] || 0;
        const m1 = data.hourly.soil_moisture_0_1cm[12] || 0.3;
        const d1 = getStatus(t1, m1);

        const t2 = data.hourly.soil_temperature_0cm[36] || t1;
        const m2 = data.hourly.soil_moisture_0_1cm[36] || m1;
        const d2 = getStatus(t2, m2);

        const t3 = data.hourly.soil_temperature_0cm[60] || t2;
        const m3 = data.hourly.soil_moisture_0_1cm[60] || m2;
        const d3 = getStatus(t3, m3);

        const mobIndex = document.getElementById('mobility-index');
        if (mobIndex) {
          mobIndex.innerHTML = `
              <div style="display:flex; justify-content:space-between; margin-top:5px; gap:4px;">
                <div style="flex:1; background:#0f172a; border: 1px solid ${d1.color}55; padding:6px; border-radius:4px; text-align:center;">
                  <div style="font-size:0.5rem; color:#94a3b8; font-weight:bold; letter-spacing:1px; margin-bottom:2px;">TODAY</div>
                  <div style="color:${d1.color}; font-size:0.75rem; font-weight:800; font-family:'JetBrains Mono', monospace;">${d1.label}</div>
                </div>
                <div style="flex:1; background:#0f172a; border: 1px solid ${d2.color}44; padding:6px; border-radius:4px; text-align:center;">
                  <div style="font-size:0.5rem; color:#94a3b8; font-weight:bold; letter-spacing:1px; margin-bottom:2px;">+24H</div>
                  <div style="color:${d2.color}; font-size:0.7rem; font-weight:800; font-family:'JetBrains Mono', monospace;">${d2.label}</div>
                </div>
                <div style="flex:1; background:#0f172a; border: 1px solid ${d3.color}33; padding:6px; border-radius:4px; text-align:center;">
                  <div style="font-size:0.5rem; color:#94a3b8; font-weight:bold; letter-spacing:1px; margin-bottom:2px;">+48H</div>
                  <div style="color:${d3.color}; font-size:0.7rem; font-weight:800; font-family:'JetBrains Mono', monospace;">${d3.label}</div>
                </div>
              </div>
            `;
        }

        // Apply Physical Glow to Frontline (if exists) based on Today's conditions
        if (currentFrontlineLayer) {
          currentFrontlineLayer.eachLayer(layer => {
            let styleColor = '#d4d4d8'; // Default Zinc

            if (t1 > 0 && m1 > 0.35) {
              styleColor = '#854d0e'; // Brown Mud
            } else if (t1 < -2) {
              styleColor = '#cffafe'; // Cyan Frozen
            }

            if (layer.setStyle && (!layer.options.fillColor || layer.options.fillColor === 'transparent')) {
              // Only style lines or neutral polygons, not filled occupation zones
              if (layer.options.color !== 'transparent') {
                layer.setStyle({ color: styleColor, weight: 3 });
              }
            }
          });
        }
      })
      .catch(e => console.error("Forecast fetch failed:", e));
  };

  // Global filter function (defensive wrapper)
  // The real implementation is set after data loading in window._applyMapFiltersImpl
  window.applyMapFilters = function () {
    // If real implementation is available, use it
    if (typeof window._applyMapFiltersImpl === 'function') {
      return window._applyMapFiltersImpl();
    }

    // Defensive fallback if data not yet ready or DOM misses toggle
    const toggleEl = document.getElementById('civilianToggle');
    const showCivilian = toggleEl ? toggleEl.checked : true;

    const events = Array.isArray(window.globalEvents) ? window.globalEvents : (Array.isArray(window.allEventsData) ? window.allEventsData : []);
    const filtered = events.filter(e => {
      const isCivil = (e.category || '').toUpperCase().includes('CIVIL') ||
        (e.location_precision || '').toUpperCase().includes('CIVILIAN') ||
        (e.type || '').toUpperCase().includes('CIVIL');
      if (isCivil && !showCivilian) return false;
      return true;
    });

    window.currentFilteredEvents = filtered;
    if (typeof renderInternal === 'function') renderInternal(filtered);
  };
  // ============================================
  // 10. MODAL FUNCTIONS (FIXED & LINKED)
  // ============================================

  window.closeAllModals = function () {
    ['videoModal', 'unitModal', 'reportModal'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = 'none';
    });
  };

  window.openModal = function (eventIdOrObj) {
    // Ensure clean state
    window.closeAllModals();

    console.log("Dossier opening attempt:", eventIdOrObj);
    let eventData = null;

    if (typeof eventIdOrObj === 'string') {
      if (eventIdOrObj.startsWith('%7B') || eventIdOrObj.startsWith('{')) {
        try { eventData = JSON.parse(decodeURIComponent(eventIdOrObj)); } catch (err) { }
      } else {
        // FIX: Robust comparison (converts all to string)
        if (window.globalEvents) {
          const searchId = String(eventIdOrObj);
          eventData = window.globalEvents.find(evt => String(evt.event_id) === searchId);
        }
      }
    } else {
      eventData = eventIdOrObj;
    }

    if (!eventData) {
      console.error(`Ã¢ÂÅ’ Event not found for Dossier. Searched ID: ${eventIdOrObj}`);
      return; // Interrupts execution if no data found
    }

    // 2. Pass data to rendering function
    window.openIntelDossier(eventData);
  };
  // ============================================
  // 10. MODAL FUNCTIONS (DOSSIER UI)
  // ============================================

  window.closeUnitModal = function (e) {
    // If e is provided, it's an event. Close only if clicking overlay.
    if (e && e.target.id !== 'unitModal' && !e.target.classList.contains('close-modal')) {
      return;
    }
    const modal = document.getElementById('unitModal');
    if (modal) modal.style.display = 'none';
  };

  window.openUnitModal = function (unit) {
    // Ensure clean state
    window.closeAllModals();

    console.log("Ã°Å¸â€œÂ¦ openUnitModal CALLED with:", unit?.display_name || unit?.unit_name || 'unknown');

    // Helper must be function-scoped
    const safeText = (txt) => txt || 'N/A';
    const format = (v) => v || '--';
    const stripHtml = (raw) => {
      if (!raw) return 'N/A';
      const el = document.createElement('div');
      el.innerHTML = String(raw);
      return (el.textContent || el.innerText || 'N/A').trim().replace(/\s+/g, ' ');
    };

    try {
      const modal = document.getElementById('unitModal');
      if (!modal) {
        console.error("Ã¢ÂÅ’ CRITICAL: 'unitModal' element NOT FOUND in DOM!");
        alert("DEBUG: unitModal element not found in DOM!");
        return;
      }

      console.log("Ã¢Å“â€¦ Modal element found. Current display:", modal.style.display);
      console.log("Ã°Å¸â€œÂ Modal computed style:", window.getComputedStyle(modal).display);

      // NUCLEAR STYLE RESET
      modal.setAttribute('style', 'display: flex !important; visibility: visible !important; opacity: 1 !important; z-index: 9999 !important; position: fixed !important; top: 0 !important; left: 0 !important; width: 100% !important; height: 100% !important; background: rgba(15, 23, 42, 0.9) !important; justify-content: center !important; align-items: center !important;');

      console.log("Ã°Å¸â€Â§ AFTER setAttribute - display:", modal.style.display);
      console.log("Ã°Å¸â€œÂ AFTER computed style:", window.getComputedStyle(modal).display);

      // Header
      const elTitle = document.getElementById('udTitle');
      if (elTitle) elTitle.innerText = unit.display_name || unit.unit_name || unit.name || unit.unit_id || 'Unknown Unit';

      // Normalize: OWL uses 'side', units.json uses 'faction'
      const unitFaction = unit.faction || unit.side || 'NEUTRAL';
      const elFaction = document.getElementById('udFaction');
      if (elFaction) elFaction.innerText = unitFaction === 'UA' ? 'Ukraine' : (unitFaction === 'RU' ? 'Russia' : unitFaction);

      const elEchelon = document.getElementById('udEchelon');
      if (elEchelon) elEchelon.innerText = safeText(unit.echelon);

      const elBadge = document.getElementById('udTypeBadge');
      if (elBadge) elBadge.innerText = safeText(unit.type);

      // Flag & Header Style
      const isUA = (unit.faction || unit.side) === 'UA';
      const isRU = (unit.faction || unit.side) === 'RU';
      const color = isUA ? '#3b82f6' : (isRU ? '#ef4444' : '#64748b');

      // Injected Flag
      const flagSvg = isUA
        ? `<svg class="ud-flag-large" viewBox="0 0 20 14"><rect width="20" height="7" fill="#005BBB"/><rect y="7" width="20" height="7" fill="#FFD500"/></svg>`
        : (isRU
          ? `<svg class="ud-flag-large" viewBox="0 0 20 14"><rect width="20" height="4.67" fill="#fff"/><rect y="4.67" width="20" height="4.67" fill="#0039A6"/><rect y="9.33" width="20" height="4.67" fill="#D52B1E"/></svg>`
          : `<svg class="ud-flag-large" viewBox="0 0 20 14"><rect width="20" height="14" fill="#64748b"/></svg>`);

      const elFlag = document.getElementById('udFlagContainer');
      if (elFlag) elFlag.innerHTML = flagSvg;

      const elHeader = document.getElementById('udHeader');
      if (elHeader) elHeader.style.borderBottomColor = color;

      // Left Stats
      const elBranch = document.getElementById('udBranch');
      if (elBranch) elBranch.innerText = safeText(unit.branch);

      const elGarrison = document.getElementById('udGarrison');
      if (elGarrison) elGarrison.innerText = stripHtml(unit.garrison);

      const elStatus = document.getElementById('udStatus');
      if (elStatus) {
        elStatus.innerText = unit.status || 'ACTIVE';
        elStatus.style.color = (unit.status === 'destroyed') ? '#ef4444' : '#22c55e';
      }

      // New Fields (all HTML-stripped)
      const elCmd = document.getElementById('udCommander');
      if (elCmd) elCmd.innerText = stripHtml(unit.commander);

      const elSup = document.getElementById('udSuperior');
      if (elSup) elSup.innerText = stripHtml(unit.superior);

      const elDist = document.getElementById('udDistrict');
      if (elDist) elDist.innerText = stripHtml(unit.district);

      console.log("Ã¢Å“â€¦ Unit Modal Content Population Complete.");

    } catch (e) {
      console.error("Ã¢ÂÅ’ Error generating Unit Modal content:", e);
    }

    // Calculations: Find related events
    const allEvents = Array.isArray(window.globalEvents) ? window.globalEvents : [];

    // Normalize unit name for search
    const unitName = (unit.unit_name || unit.display_name || unit.name || unit.unit_id || '').toLowerCase();

    // Filter events that mention this unit
    // Note: event.units is a string or array.
    const relatedEvents = allEvents.filter(e => {
      if (!e.units) return false;
      let uList = [];
      try {
        uList = typeof e.units === 'string' ? JSON.parse(e.units) : e.units;
      } catch (err) {
        uList = [];
      }
      if (!Array.isArray(uList)) return false;
      return uList.some(u => {
        const n = String(u?.unit_name || u?.unit_id || '').toLowerCase();
        const oid = String(u?.orbat_id || '');
        return n.includes(unitName) || (unit.orbat_id && oid === String(unit.orbat_id));
      });
    });

    // Update Metrics
    document.getElementById('udMentions').innerText = relatedEvents.length;
    document.getElementById('udAvgTie').innerText = unit.avg_tie || '--';
    document.getElementById('udTactic').innerText = unit.primary_tactic || '--';

    // Last Location (Coordinates) Ã¢â‚¬â€ user coords only, NOT Owl garrison coords
    const lat = unit.last_seen_lat || unit.lat;
    const lon = unit.last_seen_lon || unit.lon;
    const elLoc = document.getElementById('udLastLocation');
    if (elLoc) {
      if (lat && lon) {
        elLoc.innerText = `${parseFloat(lat).toFixed(4)}, ${parseFloat(lon).toFixed(4)}`;
      } else {
        elLoc.innerText = '--';
      }
    }

    // Engagement Freq (events in last 30 days)
    document.getElementById('udEngFreq').innerText = relatedEvents.length > 0
      ? (relatedEvents.length / 30).toFixed(1) + "/day"
      : "Low";

    // --- OWL METADATA -> METRICS ---
    const eqRow = document.getElementById('udEquipmentRow');
    const muRow = document.getElementById('udMilUnitRow');
    const baseRow = document.getElementById('udBaseRow');
    const elFlag = document.getElementById('udFlagContainer');
    
    // Reset
    if (eqRow) eqRow.style.display = 'none';
    if (muRow) muRow.style.display = 'none';
    if (baseRow) baseRow.style.display = 'none';

    // IMPORTANT: If unit was clicked from the OWL Map Layer, `unit` itself contains the OWL properties.
    // If it was clicked from the OSINT search/list, `unit.owl_meta` contains them.
    const owl = unit.owl_meta || unit;
    
    const listEl = document.getElementById('udEventsList');
    if (listEl) listEl.innerHTML = '';
    const engagementItems = [];

    const parseTimelineDate = (raw) => {
      if (!raw) return null;
      const s = String(raw).trim();
      if (!s) return null;
      let m = s.match(/\b(20\d{2})-(\d{1,2})-(\d{1,2})\b/);
      if (m) {
        const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
        return Number.isNaN(d.getTime()) ? null : d;
      }
      m = s.match(/\b(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{2,4})\b/);
      if (m) {
        const yyyy = Number(m[3]) < 100 ? (2000 + Number(m[3])) : Number(m[3]);
        const d = new Date(yyyy, Number(m[2]) - 1, Number(m[1]));
        return Number.isNaN(d.getTime()) ? null : d;
      }
      m = s.match(/\b((?:19|20)\d{2})\b/);
      if (m) {
        const d = new Date(Number(m[1]), 0, 1);
        return Number.isNaN(d.getTime()) ? null : d;
      }
      const fallback = new Date(s);
      return Number.isNaN(fallback.getTime()) ? null : fallback;
    };

    const normalizeTimelineLines = (rawValue) => {
      const lines = [];
      const consume = (value) => {
        if (value === null || value === undefined) return;
        if (Array.isArray(value)) {
          value.forEach(consume);
          return;
        }
        if (typeof value === 'object') {
          const datePart = value.date || value.when || value.timestamp || '';
          const textPart = value.description || value.location || value.text || value.name || '';
          const urlPart = value.url || value.source_url || value.link || '';
          const joined = `${datePart} ${textPart} ${urlPart}`.trim();
          if (joined) lines.push(joined);
          return;
        }
        let s = String(value);
        if (!s.trim()) return;
        s = s
          .replace(/<br\s*\/?>/gi, '\n')
          .replace(/<\/li>/gi, '\n')
          .replace(/<li[^>]*>/gi, '');
        s.split(/\r?\n/).forEach(part => {
          const cleaned = stripHtml(part).trim();
          if (cleaned && cleaned !== 'N/A') lines.push(cleaned);
        });
      };
      consume(rawValue);
      return lines;
    };

    if (owl) {
      // 1. Emblem (Media Link)
      if (owl.emblem_url && elFlag) {
        if (owl.emblem_url.includes('hostedimage')) {
            const fbSvg = elFlag.innerHTML.replace(/'/g, "\'").replace(/"/g, "&quot;");
            elFlag.innerHTML = `<img src="${owl.emblem_url}" onerror="this.outerHTML='${fbSvg}'" style="width:100%; height:100%; object-fit:cover; border-radius:4px;">`;
        }
      }

      // 2. Military Unit Number
      if (owl.military_unit_number) {
        if (muRow) muRow.style.display = 'flex';
        document.getElementById('udMilUnit').innerText = stripHtml(owl.military_unit_number);
      }
      
      // 2b. Last Known Location (Inject into Operational Profile)
      if (owl.last_known_location) {
          const elLastLoc = document.getElementById('udLastLocation');
          if (elLastLoc) elLastLoc.innerHTML = stripHtml(owl.last_known_location);
      }

      // 3. Extracted Equipment/Base from old description
      if (owl.description) {
          const rawDesc = owl.description;
          const tmpDiv = document.createElement('div');
          tmpDiv.innerHTML = rawDesc;
          const plain = (tmpDiv.textContent || tmpDiv.innerText || '').trim();
          const equipMatch = plain.match(/(?:equipped|armed|using|operates?)\s+(?:with\s+)?(.+?)(?:\.|,|\n|Military|$)/i);
          const basedMatch = plain.match(/based (?:at|in)\s+(.+?)(?:\.|,|\n|Military|$)/i);
          
          if (equipMatch && equipMatch[1].trim().length > 2) {
            if (eqRow) eqRow.style.display = 'flex';
            document.getElementById('udEquipment').innerText = equipMatch[1].trim();
          }
          if (basedMatch && basedMatch[1].trim().length > 2) {
            if (baseRow) baseRow.style.display = 'flex';
            document.getElementById('udBase').innerText = basedMatch[1].trim();
          }
      }

      // 4. Geolocation Timeline -> merge into Recent Engagements
      const geoLines = [
        ...normalizeTimelineLines(owl.older_geolocations_2),
        ...normalizeTimelineLines(owl.older_geolocations),
      ];
      geoLines.forEach((line, idx) => {
        const urlMatch = line.match(/(https?:\/\/[^\s<>"']+)/i);
        const url = urlMatch ? urlMatch[1] : '';
        let txt = line.replace(/(https?:\/\/[^\s<>"']+)/gi, ' ').trim();
        const dateMatch = txt.match(/\b(20\d{2}-\d{1,2}-\d{1,2}|\d{1,2}[\/.\-]\d{1,2}[\/.\-]\d{2,4}|(?:19|20)\d{2})\b/);
        const dateTxt = dateMatch ? dateMatch[1] : 'Archive';
        if (dateMatch) txt = txt.replace(dateMatch[1], ' ').trim();
        txt = txt.replace(/^AND\s+/i, '').replace(/^[\-\u2022*]+\s*/, '').trim();
        const cleanedVal = stripHtml(txt);
        const cleanTxt = (!cleanedVal || cleanedVal === 'N/A') ? 'Geolocation point' : cleanedVal;
        engagementItems.push({
          source: 'OWL',
          sortDate: parseTimelineDate(dateTxt),
          dateText: dateTxt,
          title: cleanTxt,
          url,
          seq: idx,
        });
      });
    }

    relatedEvents.forEach((e, idx) => {
      engagementItems.push({
        source: 'OSINT',
        sortDate: parseTimelineDate(e.date),
        dateText: e.date || 'Unknown Date',
        title: e.title || 'Untitled Event',
        eventRef: e,
        seq: idx,
      });
    });

    engagementItems.sort((a, b) => {
      const at = a.sortDate ? a.sortDate.getTime() : -1;
      const bt = b.sortDate ? b.sortDate.getTime() : -1;
      if (at !== bt) return bt - at;
      if (a.source !== b.source) return a.source === 'OWL' ? -1 : 1;
      return a.seq - b.seq;
    });

    console.log(`[UNIT_MODAL] engagements merged: owl=${engagementItems.filter(x => x.source === 'OWL').length} osint=${engagementItems.filter(x => x.source === 'OSINT').length} total=${engagementItems.length}`);

    if (listEl) {
      listEl.innerHTML = '';
      if (engagementItems.length === 0) {
        listEl.innerHTML = '<div class="ud-event-item" style="cursor:default; color:#64748b; border:none;">No recent activity linked.</div>';
      } else {
        engagementItems.slice(0, 80).forEach(item => {
          const el = document.createElement('div');
          el.className = 'ud-event-item';
          if (item.source === 'OWL') {
            el.style.borderLeft = '3px solid #f59e0b';
            el.style.background = 'rgba(15, 23, 42, 0.4)';
            el.style.padding = '8px 12px';
            el.style.marginBottom = '8px';
            if (item.url) {
              el.onclick = () => window.open(item.url, '_blank');
              el.title = 'View map source';
              el.style.cursor = 'pointer';
            }
            el.innerHTML = `
              <div style="display:flex; flex-direction:column; gap:6px;">
                <div style="font-size:0.75rem; color:#f59e0b; display:flex; justify-content:space-between; align-items:flex-start;">
                  <span style="display:flex; align-items:center; gap:6px; font-weight:700;"><i class="fa-solid fa-location-crosshairs"></i> ${item.dateText}</span>
                  <span style="font-size:0.65rem; font-weight:700; color:#cbd5e1; background:rgba(245, 158, 11, 0.15); border: 1px solid rgba(245, 158, 11, 0.3); padding:2px 6px; border-radius:4px; letter-spacing:0.5px;">MAP DATA</span>
                </div>
                <div style="font-size:0.85rem; font-weight:500; color:#e2e8f0; white-space:normal; line-height:1.5;">${item.title}</div>
              </div>
            `;
          } else {
            el.style.padding = '8px 12px';
            el.style.marginBottom = '8px';
            el.onclick = () => window.openModal(item.eventRef);
            el.innerHTML = `
              <div style="display:flex; flex-direction:column; gap:4px;">
                <div style="font-size:0.75rem; color:#3b82f6; font-weight:600;"><i class="fa-light fa-satellite-dish" style="margin-right:6px;"></i>${item.dateText}</div>
                <div style="font-size:0.85rem; font-weight:600; color:#f8fafc; white-space:normal; line-height:1.4;">${item.title}</div>
              </div>
            `;
          }
          listEl.appendChild(el);
        });
      }
    }
    // === VERIFIED CASUALTIES (UALosses enrichment) ===
    const casualtiesPanel = document.getElementById('udCasualtiesPanel');
    const casualtiesList = document.getElementById('udCasualtiesList');
    const casualtyBadge = document.getElementById('udCasualtyBadge');

    console.log('Ã°Å¸Â©Â¸ CASUALTY DEBUG:', {
      panelFound: !!casualtiesPanel,
      listFound: !!casualtiesList,
      hasCasualties: !!(unit.verified_casualties),
      casualtyCount: unit.casualty_count || 0,
      casualtiesLength: (unit.verified_casualties || []).length,
      unitId: unit.unit_id,
      unitName: unit.display_name,
      faction: unit.faction
    });

    if (casualtiesPanel && casualtiesList) {
      const casualties = unit.verified_casualties || [];
      const casualtyCount = unit.casualty_count || 0;

      if (casualtyCount > 0 && casualties.length > 0) {
        casualtiesPanel.style.display = 'block';
        casualtyBadge.innerText = casualtyCount;
        casualtiesList.innerHTML = '';

        casualties.forEach(c => {
          const item = document.createElement('div');
          item.style.cssText = 'display:flex; align-items:center; gap:8px; padding:6px 8px; background:rgba(239,68,68,0.06); border-radius:6px; border-left:2px solid #ef444444;';

          const rankBadge = c.rank && c.rank !== 'Unknown'
            ? `<span style="font-size:0.65rem; color:#f59e0b; background:#f59e0b22; padding:1px 5px; border-radius:3px; font-weight:600; white-space:nowrap;">${c.rank}</span>`
            : '';

          const sourceLink = c.source_url
            ? `<a href="${c.source_url}" target="_blank" rel="noopener" style="color:#64748b; font-size:0.7rem; margin-left:auto; white-space:nowrap;" title="Source"><i class="fa-solid fa-arrow-up-right-from-square"></i></a>`
            : '';

          item.innerHTML = `
            <i class="fa-solid fa-skull" style="color:#ef4444; font-size:0.65rem; opacity:0.6;"></i>
            ${rankBadge}
            <span style="font-size:0.8rem; color:#cbd5e1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${c.name || 'Unknown'}</span>
            ${sourceLink}
          `;
          casualtiesList.appendChild(item);
        });
      } else {
        casualtiesPanel.style.display = 'none';
      }
    }

    // Sparkline (Activity over time)
    renderSparkline(relatedEvents);
  };

  function renderSparkline(events) {
    const container = document.getElementById('udSparkline');
    container.innerHTML = '';
    if (!events.length) return;

    // Bucket by date
    const buckets = {};
    events.forEach(e => {
      const d = e.date;
      buckets[d] = (buckets[d] || 0) + 1;
    });

    const dates = Object.keys(buckets).sort();
    if (dates.length < 2) return;

    const maxVal = Math.max(...Object.values(buckets));

    dates.forEach(d => {
      const val = buckets[d];
      const h = Math.max(10, (val / maxVal) * 100);
      const bar = document.createElement('div');
      bar.style.width = '100%';
      bar.style.height = h + '%';
      bar.style.background = '#3b82f6';
      bar.style.opacity = '0.7';
      bar.title = `${d}: ${val} events`;
      container.appendChild(bar);
    });
  }

  let tieRadarInstance = null; // Global instance for the modal chart

  window.openIntelDossier = function (eventData) {
    console.log("Ã°Å¸â€œâ€š Opening Dossier for:", eventData.title);

    document.getElementById('videoModal').style.display = 'flex'; // Use Flex for centering

    // --- 1. CONTEXT (Left Column) ---
    document.getElementById('modalTitle').innerText = eventData.title || "Title not available";
    document.getElementById('modalDate').innerText = eventData.date || "Unknown Date";

    // Type Tag
    const typeTag = document.getElementById('modalType');
    if (typeTag) typeTag.innerText = (eventData.type || "EVENT").toUpperCase();

    // Description
    const descEl = document.getElementById('modalDesc');
    if (descEl) descEl.innerText = eventData.description || "No description available for this event.";

    // --- 1.5 UNITS INVOLVED ---
    const unitsContainer = document.getElementById('modalUnits');
    const unitsList = document.getElementById('modalUnitsList');

    if (unitsContainer && unitsList) {
      let units = [];
      try {
        if (eventData.units) {
          units = typeof eventData.units === 'string' ? JSON.parse(eventData.units) : eventData.units;
        }
      } catch (e) {
        console.warn("Failed to parse units:", e);
      }

      if (units && units.length > 0) {
        unitsContainer.style.display = 'block';
        unitsList.innerHTML = units.map(u => {
          const isUA = u.faction === 'UA';
          const isRU = u.faction === 'RU' || u.faction === 'RU_PROXY' || u.faction === 'RU_PMC';
          const bgColor = isUA ? 'rgba(59, 130, 246, 0.2)' : (isRU ? 'rgba(239, 68, 68, 0.2)' : 'rgba(100, 116, 139, 0.2)');
          const borderColor = isUA ? '#3b82f6' : (isRU ? '#ef4444' : '#64748b');
          const flag = isUA ? 'Ã°Å¸â€¡ÂºÃ°Å¸â€¡Â¦' : (isRU ? 'Ã°Å¸â€¡Â·Ã°Å¸â€¡Âº' : 'Ã°Å¸ÂÂ³Ã¯Â¸Â');

          return `
            <div style="
              display: inline-flex;
              align-items: center;
              gap: 6px;
              background: ${bgColor};
              border: 1px solid ${borderColor};
              padding: 4px 8px;
              border-radius: 4px;
              font-size: 0.75rem;
              color: #f1f5f9;
              font-family: 'JetBrains Mono', monospace;
            ">
              <span style="font-size: 0.9rem;">${flag}</span>
              <span style="font-weight: 600;">${u.display_name || u.unit_name || u.unit_id}</span>
              <span style="opacity: 0.6; font-size: 0.7rem;">${u.status || 'ACTIVE'}</span>
          `;
        }).join('');
      } else {
        // Show section with "No units detected" placeholder
        unitsContainer.style.display = 'block';
        unitsList.innerHTML = '<span style="opacity: 0.5; font-style: italic;">No units detected for this event</span>';
      }
    }

    // --- 1.6 IMINT EVIDENCE FEED ---
    const imintFeed = document.getElementById('imint-evidence-feed');
    const imintFilmstrip = document.getElementById('imintFilmstrip');
    const imintCount = document.getElementById('imintFrameCount');

    if (imintFeed && imintFilmstrip) {
      let frames = [];
      try {
        if (eventData.visual_analysis) {
          frames = typeof eventData.visual_analysis === 'string'
            ? JSON.parse(eventData.visual_analysis)
            : eventData.visual_analysis;
        }
      } catch (e) {
        console.warn("Failed to parse visual_analysis:", e);
      }

      if (frames && frames.length > 0) {
        imintFeed.style.display = 'block';
        imintCount.innerText = frames.length + ' FRAMES';

        // Choose layout mode: filmstrip (>3) or vertical stack (<=3)
        imintFilmstrip.className = frames.length > 3
          ? 'imint-filmstrip'
          : 'imint-filmstrip imint-stack';

        imintFilmstrip.innerHTML = frames.map((f, idx) => {
          const conf = Math.round((f.confidence || 0) * 100);
          const isContradicted = (f.explanation || '').toUpperCase().includes('CONTRADICT')
            || (f.verification_status || '').toUpperCase().includes('CONTRADICT');
          const cardClass = 'imint-frame-card' + (isContradicted ? ' imint-contradicted' : '');
          const explId = `imint-expl-${idx}`;

          return `
            <div class="${cardClass}">
              <img class="imint-thumb"
                   src="${f.base64_data || ''}"
                   alt="Frame ${f.frame_id || idx + 1}"
                   onclick="document.getElementById('imintLightboxImg').src=this.src; document.getElementById('imintLightbox').style.display='flex';"
                   onerror="this.style.display='none'">
              <div class="imint-meta-row">
                <span class="imint-confidence-badge">${conf}%</span>
                <span class="imint-selection-tag" title="${f.selection_reason || ''}">${f.selection_reason || 'Keyframe'}</span>
                <span class="imint-frame-id">F${f.frame_id || idx + 1}</span>
              </div>
              <div class="imint-explanation" id="${explId}">${f.explanation || 'No analysis available.'}</div>
              <span class="imint-read-more" onclick="var el=document.getElementById('${explId}'); el.classList.toggle('expanded'); this.innerText=el.classList.contains('expanded')?'Show Less':'Read More';">Read More</span>
            </div>
          `;
        }).join('');
      } else {
        imintFeed.style.display = 'none';
        imintFilmstrip.innerHTML = '';
      }
    }

    // --- 2. METRICS CARD (Right Column) ---
    const vecK = parseFloat(eventData.vec_k) || 0;
    const vecT = parseFloat(eventData.vec_t) || 0;
    const vecE = parseFloat(eventData.vec_e) || 0;

    // A. TIE Bars
    const barsContainer = document.getElementById('tieBarsContent');
    if (barsContainer) {
      barsContainer.innerHTML = `
            <div class="tie-bar-row">
                <div class="tie-bar-label"><span>KINETIC (Intensity)</span> <span>${vecK}/10</span></div>
                <div class="tie-progress-track"><div class="tie-progress-fill bar-kinetic" style="width: ${vecK * 10}%"></div></div>
            </div>
            <div class="tie-bar-row">
                <div class="tie-bar-label"><span>TARGET (Value)</span> <span>${vecT}/10</span></div>
                <div class="tie-progress-track"><div class="tie-progress-fill bar-target" style="width: ${vecT * 10}%"></div></div>
            </div>
            <div class="tie-bar-row">
                <div class="tie-bar-label"><span>EFFECT (Outcome)</span> <span>${vecE}/10</span></div>
                <div class="tie-progress-track"><div class="tie-progress-fill bar-effect" style="width: ${vecE * 10}%"></div></div>
            </div>
        `;
    }

    // B. TIE Radar Chart (Chart.js)
    const ctx = document.getElementById('tieRadarChart');
    if (ctx) {
      if (tieRadarInstance) {
        tieRadarInstance.destroy();
      }

      tieRadarInstance = new Chart(ctx, {
        type: 'radar',
        data: {
          labels: ['KINETIC', 'TARGET', 'EFFECT'],
          datasets: [{
            label: 'TIE Profile',
            data: [vecK, vecT, vecE],
            backgroundColor: 'rgba(245, 158, 11, 0.2)', // Amber transparent
            borderColor: 'rgba(245, 158, 11, 1)',       // Amber solid
            borderWidth: 2,
            pointBackgroundColor: '#fff',
            pointBorderColor: '#eab308',
            pointHoverBackgroundColor: '#fff',
            pointHoverBorderColor: '#eab308'
          }]
        },
        options: {
          scales: {
            r: {
              angleLines: { color: 'rgba(255, 255, 255, 0.1)' },
              grid: { color: 'rgba(255, 255, 255, 0.1)' },
              pointLabels: {
                color: '#94a3b8',
                font: { size: 10, weight: 'bold' }
              },
              ticks: { display: false, max: 10, min: 0 } // Hide numbers, fixed 0-10
            }
          },
          plugins: {
            legend: { display: false } // Hide legend
          },
          maintainAspectRatio: false
        }
      });
    }

    // --- 3. METADATA CARD ---

    // A. Reliability Badge
    const relScore = eventData.reliability || 0;
    let relBadgeHtml = '';
    if (relScore >= 80) relBadgeHtml = `<span style="color:#22c55e; font-weight:700;"><i class="fa-solid fa-shield-halved"></i> HIGH RELIABILITY</span>`;
    else if (relScore >= 50) relBadgeHtml = `<span style="color:#f59e0b; font-weight:700;"><i class="fa-solid fa-shield-halved"></i> MEDIUM RELIABILITY</span>`;
    else relBadgeHtml = `<span style="color:#ef4444; font-weight:700;"><i class="fa-solid fa-triangle-exclamation"></i> LOW RELIABILITY</span>`;

    const relBadgeEl = document.getElementById('modal-reliability-badge');
    if (relBadgeEl) relBadgeEl.innerHTML = relBadgeHtml;

    // B. Sources List
    const sourceListEl = document.getElementById('modal-source-list');
    if (sourceListEl) {
      let sources = [];
      try {
        if (Array.isArray(eventData.sources_list)) {
          sources = eventData.sources_list;
        } else if (typeof eventData.sources_list === 'string') {
          try {
            sources = JSON.parse(eventData.sources_list);
          } catch (e1) {
            try {
              sources = JSON.parse(eventData.sources_list.replace(/'/g, '"'));
            } catch (e2) {
              sources = [eventData.sources_list];
            }
          }
        }
      } catch (e) { console.warn("Error parsing sources:", e); }

      if (!sources || sources.length === 0) {
        sourceListEl.innerHTML = `<span style="color:#64748b; font-style:italic; font-size:0.8rem;">No explicit sources listed.</span>`;
      } else {
        // Backend now sends [{name, url}] objects
        let telegramUrls = [];
        let validHtml = sources.map(src => {
          let displayName, url;

          if (typeof src === 'object' && src !== null) {
            // New format: {name: "rybar", url: "https://t.me/rybar/76184"}
            displayName = src.name || "Source";
            url = src.url || "#";
          } else if (typeof src === 'string') {
            // Legacy string format
            displayName = src;
            if (src.startsWith('http')) {
              url = src;
              // Extract display name from URL
              try {
                if (src.includes('t.me/')) {
                  displayName = src.split('t.me/')[1].split('/')[0];
                } else {
                  displayName = new URL(src).hostname.replace('www.', '');
                }
              } catch (e) { }
            } else {
              url = '#';
            }
          } else {
            return '';
          }

          // Skip invalid
          if (!url || url === '#' || url === 'None' || url === 'null') {
            if (displayName && displayName !== 'Source') {
              return `
                <div class="source-item" style="cursor:default; opacity:0.8; display:flex; align-items:center;">
                    <i class="fa-solid fa-file-lines" style="margin-right:8px; opacity:0.5;"></i>
                    <span>${displayName}</span>
                </div>`;
            }
            return '';
          }

          // Ensure protocol
          if (!url.startsWith('http')) {
            url = 'https://' + url;
          }

          if (url.includes('t.me/')) {
            telegramUrls.push(url);
          }

          // Favicon domain (use t.me for Telegram, otherwise actual domain)
          let faviconDomain = displayName;
          try {
            faviconDomain = new URL(url).hostname.replace('www.', '');
          } catch (e) { }

          const faviconUrl = `https://www.google.com/s2/favicons?domain=${faviconDomain}&sz=32`;

          return `
                <a href="${url}" target="_blank" rel="noopener noreferrer" class="source-item">
                    <img src="${faviconUrl}" class="source-icon" onerror="this.style.display='none'">
                    <span>${displayName}</span>
                    <i class="fa-solid fa-external-link-alt" style="margin-left:auto; font-size:0.7rem; opacity:0.5;"></i>
                </a>`;
        }).filter(Boolean).join('');

        sourceListEl.innerHTML = validHtml || `<span style="color:#64748b; font-style:italic; font-size:0.8rem;">No explicit sources listed.</span>`;

        // --- INJECT TELEGRAM EMBED IFRAME ---
        const videoContainer = document.getElementById('modalVideoContainer');
        if (videoContainer) {
          videoContainer.innerHTML = '';
          videoContainer.style.display = 'none';
          if (telegramUrls.length > 0) {
            let tgUrl = telegramUrls[0].trim();
            let embedUrl = tgUrl;
            if (!embedUrl.includes('?embed=')) {
              embedUrl = embedUrl.split('?')[0] + '?embed=1&dark=1';
            } else if (!embedUrl.includes('dark=')) {
              embedUrl += '&dark=1';
            }

            videoContainer.innerHTML = `
                    <div style="margin-top:20px; border-top: 1px solid rgba(255,255,255,0.1); padding-top:15px;">
                        <div style="display:flex; align-items:center; gap:8px; margin-bottom:10px;">
                            <i class="fa-brands fa-telegram" style="color:#38bdf8; font-size:1.1rem;"></i>
                            <h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin:0;">PRIMARY SOURCE MEDIA</h5>
                        </div>
                        <iframe src="${embedUrl}" style="width: 100%; height: 500px; border: none; border-radius: 8px; overflow: hidden; background: #fff;" allow="fullscreen"></iframe>
                    </div>
                `;
            videoContainer.style.display = 'block';
          }
        }
      }
    }

    // C. Bias Meter
    const biasScore = parseFloat(eventData.bias_score) || 0;
    const biasPercent = ((biasScore + 10) / 20) * 100;
    const clampedBias = Math.max(0, Math.min(100, biasPercent));

    // --- 3b. RELIABILITY BAR ---
    // ... code omitted ...

    // --- 3c. BIAS METER ---
    const biasMarker = document.getElementById('bias-marker');
    const biasValueEl = document.getElementById('bias-value');

    if (biasMarker) {
      biasMarker.style.left = `${clampedBias}%`;
    }
    if (biasValueEl) {
      const biasLabel = biasScore <= -3 ? "Pro-RU" : (biasScore >= 3 ? "Pro-UA" : "Neutral");
      biasValueEl.textContent = `${biasScore.toFixed(1)} (${biasLabel})`;
    }

    // --- 4. THE STRATEGIST ---
    // If there is an AI summary field (e.g. 'desc' repurposed or specific field), use it.
    // --- HOST FUNCTION: Language Parser ---
    function getLocalizedText(text) {
      if (!text) return "";
      // 1. PRIORITIZE ENGLISH ([EN])
      if (text.includes('[EN]')) {
        const parts = text.split('[EN]');
        if (parts.length > 1) {
          // Return text after [EN] until end or next tag
          return parts[1].split('[')[0].trim();
        }
      }
      // 2. Fallback to Italian ([IT]) or raw text
      if (text.includes('[IT]')) {
        const parts = text.split('[IT]');
        if (parts.length > 1) {
          return parts[1].split('[')[0].trim();
        }
      }
      return text;
    }

    // --- 4. THE STRATEGIST ---
    const stratBox = document.getElementById('modal-strategist-content');
    if (stratBox) {
      // Use language parser
      let rawReasoning = eventData.ai_reasoning || "AI Analysis confirms high probability of kinetic event based on cross-referenced multi-source reporting. Strategic impact affects local logistics.";
      const reasoning = getLocalizedText(rawReasoning);
      stratBox.innerHTML = `
        <div style="display:flex; align-items:center; gap:8px; margin-bottom:10px;">
            <h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin:0;">AI STRATEGIST ANALYSIS</h5>
            <div class="info-icon-wrapper">
                <div class="info-icon">i</div>
                <div class="tooltip-card">
                    <div class="tooltip-header">AI Strategist Analysis</div>
                    <div class="tooltip-body">This section provides an AI-generated analysis of the event's strategic implications, based on available intelligence.</div>
                    <div class="tooltip-footer">Powered by advanced language models.</div>
                </div>
            </div>
        </div>
        <p style="font-size:0.9rem; line-height:1.5; color:#cbd5e1;">${reasoning}</p>
      `;
    }

    // ============================================================
    // B. SCORE & CHART MANAGEMENT 
    // ============================================================
    const score = parseInt(eventData.reliability || eventData.Reliability || eventData.confidence || 0);

    // Define Colors and Text (ENGLISH DEFAULT)
    let relData = {
      label: "UNVERIFIED",
      color: "#64748b",
      desc: "Insufficient data to evaluate reliability.",
      footer: "Score withheld due to lack of sources."
    };

    if (score >= 80) {
      relData = {
        label: "CONFIRMED",
        color: "#22c55e",
        desc: "Visually confirmed. Event supported by verified footage (IMINT) or precise geolocation.",
        footer: "Max score guaranteed by visual proof."
      };
    } else if (score >= 60) {
      relData = {
        label: "RELIABLE",
        color: "#84cc16",
        desc: "Highly probable. Confirmed by multiple independent vectors or reputable institutional sources.",
        footer: "High score due to narrative convergence."
      };
    } else if (score >= 40) {
      relData = {
        label: "UNCERTAIN",
        color: "#f59e0b",
        desc: "Pending verification. Reported by mainstream or credible local sources, but not field-verified.",
        footer: "Score based on historical source reputation."
      };
    } else if (score < 40) {
      relData = {
        label: "DUBIOUS",
        color: "#ef4444",
        desc: "Low Confidence. High risk of circular reporting or disinformation.",
        footer: "Limited score due to lack of independent corroboration."
      };
    }

    if (typeof renderConfidenceChart === 'function') {
      renderConfidenceChart(score, relData.color);
    }

    // --- RELIABILITY GRADIENT BAR (NEW VISUAL - CLEAN) ---
    const relContainer = document.getElementById('modal-reliability-badge');
    if (relContainer) {
      // Calculate marker position (0-100%)
      const markerPos = Math.max(0, Math.min(100, score));

      relContainer.innerHTML = `
            <div style="margin-top:10px;">
                <div class="reliability-label">
                    <span>Reliability Score</span> <!-- Title Case -->
                    <strong style="color:${relData.color}">${score}%</strong>
                </div>
                
                <!-- GRADIENT BAR -->
                <div class="reliability-bar-track" style="margin-bottom:8px;">
                    <div class="reliability-bar-fill" style="width: 100%;"></div> 
                    <div class="reliability-marker" style="left: ${markerPos}%"></div>
                </div>

                <!-- INFO ROW (LABEL + TOOLTIP) -->
                <div style="display:flex; align-items:center;">
                     <!-- REMOVED: Static Labels below bar -->
                     
                     <span style="font-size:0.7rem; color:${relData.color}; font-weight:700; letter-spacing:1px; margin-right:5px;">${relData.label}</span>
                     
                     <!-- TOOLTIP WRAPPER (Mini-Card) -->
                     <div class="info-icon-wrapper">
                        <div class="info-icon">i</div>
                        <div class="tooltip-card">
                            <div class="tooltip-header">${relData.label} (${score}%)</div> <!-- Keep classification uppercase/bold as user liked that part -->
                            <div class="tooltip-body">${relData.desc}</div>
                            <div class="tooltip-footer">${relData.footer}</div>
                        </div>
                     </div>
                </div>
            </div>`;
    }

    if (typeof renderBibliography === 'function') {
      renderBibliography(eventData.references || []);
    }

    const modal = document.getElementById('videoModal') || document.getElementById('eventModal');
    if (modal) modal.style.display = 'flex';
  }

  // --- ADDITIONAL INTELLIGENCE DATA ---
  // Duplicate intelligence rendering logic removed because it is already handled inside openIntelDossier(eventData).
  // The duplicate block referenced an undefined variable `e` and caused unmatched braces/syntax errors.

  // Function to draw sources (Updated for URL lists)
  function renderBibliography(references) {
    const container = document.getElementById('modal-bibliography');
    if (!container) return;

    container.innerHTML = '';

    // If no references or empty list
    if (!references || references.length === 0) {
      container.innerHTML = '<div style="padding:10px; background:rgba(255,255,255,0.02); border-radius:4px; color:#64748b; font-style:italic; font-size:0.85rem; text-align:center;">No aggregated sources available for this event.</div>';
      return;
    }

    let html = `<h5 style="color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin-bottom:15px; border-bottom:1px solid #334155; padding-bottom:5px; display:flex; align-items:center; gap:8px;"><i class="fa-solid fa-link"></i> Related Sources & Intelligence</h5>`;

    references.forEach((ref, idx) => {
      // Robust management: supports both strings (URs) and old objects
      let url = (typeof ref === 'object' && ref.url) ? ref.url : ref;

      // If not valid link, show as text, otherwise create link
      let isLink = typeof url === 'string' && (url.startsWith('http') || url.startsWith('www'));

      // Aesthetics: Extracts domain to not show kilometer-long URLs (e.g. "twitter.com")
      let displayName = "External Source";
      if (isLink) {
        try {
          const urlObj = new URL(url.startsWith('http') ? url : 'https://' + url);
          displayName = urlObj.hostname.replace('www.', '');
        } catch (e) { displayName = url; }
      } else {
        displayName = "Archive Reference";
      }

      html += `
            <div style="margin-bottom:8px; display:flex; align-items:center; background:rgba(15, 23, 42, 0.6); padding:8px 12px; border-radius:6px; border:1px solid #334155;">
                <span style="color:#64748b; font-family:'JetBrains Mono', monospace; font-size:0.8rem; margin-right:10px; min-width:20px;">${idx + 1}.</span>
                
                ${isLink ?
          `<a href="${url}" target="_blank" style="color:#38bdf8; text-decoration:none; font-size:0.9rem; font-weight:500; display:flex; align-items:center; gap:6px; flex-grow:1; transition: color 0.2s;">
                        <i class="fa-solid fa-earth-europe" style="font-size:0.8em; opacity:0.7;"></i> ${displayName} 
                        <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.7em; margin-left:auto; opacity:0.5;"></i>
                    </a>`
          : `<span style="color:#cbd5e1; font-size:0.9rem;">${ref}</span>`
        }
            </div>`;
    });

    container.innerHTML = html;
  }

  window.updateSlider = function (e, wrapper) {
    const rect = wrapper.getBoundingClientRect();
    let pos = ((e.clientX - rect.left) / rect.width) * 100;
    pos = Math.max(0, Math.min(100, pos));
    wrapper.querySelector('.after').style.width = `${pos}%`;
    wrapper.querySelector('.juxtapose-handle').style.left = `${pos}%`;
  };

  // Updated Chart Function with Dynamic Color
  let confChart = null;
  function renderConfidenceChart(score, color = '#f59e0b') { // <--- Added color parameter
    const ctxEl = document.getElementById('confidenceChart');
    if (!ctxEl) return;

    const ctx = ctxEl.getContext('2d');
    if (confChart) confChart.destroy();

    confChart = new Chart(ctx, {
      type: 'doughnut',
      data: {
        datasets: [{
          data: [score, 100 - score],
          backgroundColor: [color, '#1e293b'], // <--- Use dynamic color here
          borderWidth: 0,
          borderRadius: 20
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '75%',
        animation: false,
        plugins: { tooltip: { enabled: false } }
      },
      plugins: [{
        id: 'text',
        beforeDraw: function (chart) {
          const width = chart.width, height = chart.height, ctx = chart.ctx;
          ctx.restore();
          const fontSize = (height / 100).toFixed(2);
          ctx.font = "bold " + fontSize + "em Inter";
          ctx.textBaseline = "middle";
          ctx.fillStyle = color; // <--- And also here for central text
          const text = score + "%";
          const textX = Math.round((width - ctx.measureText(text).width) / 2);
          const textY = height / 2;
          ctx.fillText(text, textX, textY);
          ctx.save();
        }
      }]
    });
  }

  // ============================================
  // 11. VISUAL GRID RENDERER
  // ============================================

  window.renderVisualGrid = function (events) {
    const grid = document.getElementById('visual-grid-content');
    if (!grid) return;

    grid.innerHTML = '';
    const visualEvents = events.filter(e => e.image || (e.video && e.video !== 'null'));

    visualEvents.forEach(e => {
      const item = document.createElement('div');
      item.className = 'visual-item';
      item.style.cssText = "background:#1e293b; border-radius:8px; overflow:hidden; position:relative; aspect-ratio: 16/9; cursor:pointer; border:1px solid #334155;";

      let bgUrl = e.image;
      if (!bgUrl && e.video && e.video.includes('youtu')) {
        try {
          let vidId = null;
          if (e.video.includes('v=')) vidId = e.video.split('v=')[1]?.split('&')[0];
          else if (e.video.includes('youtu.be/')) vidId = e.video.split('youtu.be/')[1]?.split('?')[0];
          if (vidId) bgUrl = `https://img.youtube.com/vi/${vidId}/mqdefault.jpg`;
        } catch (err) { }
      }

      if (bgUrl) {
        item.innerHTML = `
          <div style="background-image:url('${bgUrl}'); width:100%; height:100%; background-size:cover; background-position:center;"></div>
          <div style="position:absolute; bottom:0; left:0; width:100%; background:linear-gradient(to top, rgba(0,0,0,0.9), transparent); padding:10px; color:white;">
            <div style="font-size:0.85rem; font-weight:bold; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; text-shadow: 0 1px 2px black;">
              ${e.type === 'video' ? '<i class="fa-solid fa-play-circle"></i> ' : ''} ${e.title}
            </div>
            <div style="font-size:0.75rem; color:#cbd5e1; display:flex; justify-content:space-between;">
              <span>${e.date}</span>
              <span style="color:#f59e0b;">${e.intensity > 0.7 ? 'CRITICAL' : ''}</span>
            </div>
          </div>`;

        const eventData = encodeURIComponent(JSON.stringify(e));
        item.onclick = () => window.openModal(eventData);
        grid.appendChild(item);
      }
    });

    if (visualEvents.length === 0) {
      grid.innerHTML = `<div style="grid-column: 1 / -1; text-align:center; color:#64748b; padding:40px;"><i class="fa-solid fa-camera-retro" style="font-size:2rem; margin-bottom:10px; opacity:0.5;"></i><br>No visual media found.</div>`;
    }
  };

  // ============================================
  // 11. REPORT GENERATION MODAL
  // ============================================

  window.openReportModal = function () {
    console.log("Opening Report Modal...");
    const modal = document.getElementById('reportModal');
    if (!modal) return;

    // Set defaults: Start = 30 days ago, End = Today
    const today = new Date().toISOString().split('T')[0];
    const past = new Date();
    past.setDate(past.getDate() - 30);
    const start = past.toISOString().split('T')[0];

    document.getElementById('reportStartDate').value = start;
    document.getElementById('reportEndDate').value = today;
    document.getElementById('reportLiveToggle').checked = true;
    window.toggleReportLiveDate();

    modal.style.display = 'flex';
  };

  window.closeReportModal = function (e) {
    if (e && e.target.id !== 'reportModal' && !e.target.classList.contains('close-modal')) {
      return;
    }
    const modal = document.getElementById('reportModal');
    if (modal) modal.style.display = 'none';
  };

  window.toggleReportLiveDate = function () {
    const isLive = document.getElementById('reportLiveToggle').checked;
    const endInput = document.getElementById('reportEndDate');
    if (isLive) {
      endInput.disabled = true;
      endInput.style.opacity = '0.5';
      endInput.value = new Date().toISOString().split('T')[0];
    } else {
      endInput.disabled = false;
      endInput.style.opacity = '1';
    }
  };

  window.generateReport = function () {
    const start = document.getElementById('reportStartDate').value;
    let end = document.getElementById('reportEndDate').value;
    const isLive = document.getElementById('reportLiveToggle').checked;

    if (isLive) {
      end = 'LIVE';
    }

    if (!start) {
      alert("Please select a start date.");
      return;
    }

    // Redirect to report.html with params
    const url = `report.html?start=${start}&end=${end}`;
    window.open(url, '_blank');

    window.closeReportModal();
  };

  // ============================================
  // 12. APPLICATION START (Sequential Execution)
  // ============================================

  // Wait for DOM before initializing
  function startApp() {
    console.log("Ã°Å¸Å¡â‚¬ Starting Impact Atlas...");
    initAxisStatsPanelControls();
    syncAxisHudOffset();
    window.addEventListener('resize', syncAxisHudOffset);
    initMap();
    loadSectorsData();
    loadSectorAnomaliesData();
    loadEventsData();
    loadStrategicCampaignData();
    loadAxisThermalFeatures();

    // Initialize Physical Weather
    if (window.fetchFrontlineWeather) window.fetchFrontlineWeather();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startApp);
  } else {
    startApp();
  }
})();

