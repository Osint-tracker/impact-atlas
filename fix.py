import re

# Update map.js
with open('assets/js/map.js', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove SEGNALA ERRORE button
content = re.sub(r'<a\s+class="custom-dossier-btn report-error-btn"[^>]*>\s*<i class="fa-solid fa-triangle-exclamation"></i> SEGNALA ERRORE\s*</a>', '', content)

# 2. Add modalReportError logic inside openIntelDossier
target = 'document.getElementById(\'modalTitle\').innerText = eventData.title || "Title not available";'
replacement = """document.getElementById('modalTitle').innerText = eventData.title || "Title not available";

    const reportErrorLink = document.getElementById('modalReportError');
    if (reportErrorLink) {
      let primaryUrl = '';
      try {
        if (eventData.sources_list) {
          let sources = typeof eventData.sources_list === 'string' ? JSON.parse(eventData.sources_list) : eventData.sources_list;
          if (sources.length > 0) {
            primaryUrl = typeof sources[0] === 'object' ? (sources[0].url || sources[0].link || '') : sources[0];
          }
        }
      } catch (e) {}
      reportErrorLink.href = (typeof buildReportIssueUrl === 'function') ? buildReportIssueUrl(eventData.id || eventData.event_id || '', eventData.title || '', primaryUrl) : "https://github.com/Osint-tracker/impact-atlas/issues/new";
    }"""
content = content.replace(target, replacement)

with open('assets/js/map.js', 'w', encoding='utf-8') as f:
    f.write(content)

# Update index.html
with open('index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# AI Disclaimer
content = content.replace('I dati sono aggregati ed elaborati da Intelligenze Artificiali in modo automatizzato. Possono contenere allucinazioni, errori di classificazione o bias legati alle fonti.',
                          'Data is aggregated and processed automatically by AI models. It may contain hallucinations, misclassifications, or source-related bias.')

# Labels
content = content.replace('Ultimo aggiornamento dati: Loading...', 'Last data update: Loading...')
content = content.replace('Carica archivio completo</button>', 'Load full archive</button>')

# Inject modalReportError link
target = '''              <div class="dossier-header-group">
                <h3 id="modalTitle">Event Title</h3>
                <div class="dossier-meta-row">'''
replacement = '''              <div class="dossier-header-group">
                <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                  <h3 id="modalTitle">Event Title</h3>
                  <a id="modalReportError" href="#" target="_blank" rel="noopener noreferrer" style="font-size:0.65rem; color:#ef4444; text-decoration:none; text-transform:uppercase; letter-spacing:0.5px; border:1px solid rgba(239, 68, 68, 0.4); padding:3px 8px; border-radius:4px; opacity:0.8; transition:all 0.2s; background:rgba(239, 68, 68, 0.05);" onmouseover="this.style.opacity='1'; this.style.background='rgba(239, 68, 68, 0.15)'; this.style.borderColor='#ef4444'" onmouseout="this.style.opacity='0.8'; this.style.background='rgba(239, 68, 68, 0.05)'; this.style.borderColor='rgba(239, 68, 68, 0.4)'">Report Error</a>
                </div>
                <div class="dossier-meta-row">'''
content = content.replace(target, replacement)

# Tutorial texts that I previously forgot
content = content.replace('<h2 id="tutorialTitle">Guida rapida</h2>', '<h2 id="tutorialTitle">Quick Start Guide</h2>')
content = content.replace('<h3>Mappa</h3>', '<h3>Map View</h3>')
content = content.replace('<p>Visualizza eventi aggregati, frontline e layer OSINT. Apri un marker per dossier, fonte e segnalazione errore.</p>', '<p>Visualize aggregated events, frontlines, and OSINT layers. Open a marker to view its dossier, source, and report errors.</p>')
content = content.replace('<h3>Filtri</h3>', '<h3>Filters</h3>')
content = content.replace('<p>Usa settore, finestra temporale, affidabilita e categorie per ridurre il rumore informativo.</p>', '<p>Use sector, timeframe, reliability, and category filters to reduce informational noise.</p>')
content = content.replace("<p>Il caricamento iniziale mostra gli ultimi 7 giorni. L'archivio completo viene scaricato solo su richiesta.</p>", "<p>Initial load displays the last 7 days. The complete archive is downloaded only upon request.</p>")
content = content.replace('<button class="tutorial-primary" type="button" onclick="dismissImpactAtlasTutorial()">Ho capito</button>', '<button class="tutorial-primary" type="button" onclick="dismissImpactAtlasTutorial()">Understood</button>')

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(content)
