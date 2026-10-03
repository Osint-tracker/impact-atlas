/** Key-free background cartography, independent of application data layers. */
(function (global) {
  'use strict';
  var ATTRIBUTION = '<a href="https://openfreemap.org/" target="_blank" rel="noopener noreferrer">OpenFreeMap</a> · ' +
    '<a href="https://openmaptiles.org/" target="_blank" rel="noopener noreferrer">© OpenMapTiles</a> · ' +
    '<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">© OpenStreetMap contributors</a>';

  function showUnavailable(map) {
    var host = map.getContainer();
    var existing = host.querySelector('.basemap-notice');
    if (existing) return;
    var message = document.createElement('div');
    message.className = 'basemap-notice';
    message.setAttribute('role', 'status');
    message.textContent = 'Background map unavailable. Check your connection and WebGL support, then reload.';
    host.appendChild(message);
  }

  /** Add only the background layer; leave application overlays and coordinates intact. */
  function addTo(map) {
    try {
      if (!global.maplibregl || !global.L.maplibreGL) throw new Error('Map renderer unavailable');
      var layer = global.L.maplibreGL({
        style: 'https://tiles.openfreemap.org/styles/dark',
        attribution: ATTRIBUTION,
        interactive: false,
        pane: 'tilePane'
      }).addTo(map);
      var renderer = layer.getMaplibreMap();
      renderer.on('error', function () { showUnavailable(map); });
      renderer.on('idle', function () {
        if (!renderer.isStyleLoaded() || !renderer.areTilesLoaded()) return;
        var message = map.getContainer().querySelector('.basemap-notice');
        if (message) message.remove();
      });
      return layer;
    } catch (error) {
      showUnavailable(map);
      console.warn('Background map could not initialize:', error.name);
      return null;
    }
  }

  global.AtlasBasemap = Object.freeze({ addTo: addTo });
})(window);
