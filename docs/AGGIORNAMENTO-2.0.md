# Aggiornamento 2.0.0 — Atlante

Base del pacchetto differenziale: `risiko-aggiornamento-1.8.0.zip`. Versione del progetto: 2.0.0; build nativa: 13.

## Applicare lo ZIP con le sole modifiche

1. Conserva una copia del progetto e un backup JSON dei dati dell'app.
2. Estrai `risiko-solo-modifiche-2.0.0.zip` nella **radice del progetto esistente**, dove si trova `package.json`. Accetta la sostituzione dei file omonimi. Non creare una seconda cartella `risiko-offline` dentro la prima.
3. Leggi `FILE-MODIFICATI-2.0.txt`: elenca file nuovi, modificati ed eventuali rimozioni. Lo ZIP non contiene le dipendenze, i file invariati, la build web o un APK.
4. Esegui `npm ci`, `npm run typecheck`, `npm test`, `npm run build` e `npm run pwa:verify`.
5. Prima di distribuire, esegui anche `npm run test:ui` e `npm run test:pwa` su un ambiente che consenta l'avvio dei browser Playwright. Per installarli: `npx playwright install --with-deps chromium webkit`.
6. Android: `npx cap sync android`, poi il workflow Android già presente o `npm run apk` con JDK e SDK configurati. iPhone: PWA tramite il workflow Pages esistente; il pacchetto APK non è installabile su iOS.

Il delta è confrontato byte per byte con l'archivio 1.8.0 conservato nel workspace. Se il tuo repository contiene ulteriori modifiche personali, confronta i file prima di sovrascriverli. Non è una patch Git né un merge automatico.

## Funzionalità

- Interfaccia navy/ottone/avorio: home, preparazione, profilo, archivi e impostazioni verticali; plancia orizzontale con barre separate, sidebar obiettivo/carte/registro e comandi zoom dedicati.
- Mascotte Breton con elmo e uniforme, icona aggiornata, font locali e icone SVG. Carri isometrici, badge numerici e pulsanti di piazzamento conservati.
- Preparazione con 3–5 partecipanti, almeno un umano, umani e bot configurabili. Seed numerico o testuale nel campo richiudibile **Codice Partita / Seed**; codice condivisibile con configurazione. Il codice non contiene una partita avviata o la cronologia delle mosse.
- Pass & Play opzionale: la cortina mostra solo nome e colore del destinatario. Il Worker non invia lo stato proiettato finché non riceve il token corretto di rivelazione. Nessuna lobby online.
- Slot multipli, profili locali, migrazione del salvataggio precedente; backup v2 con importazione atomica per sostituzione o come copie. Le partite concluse duplicate non incrementano nuovamente le statistiche.
- Editor di posizioni avviate e tre scenari tattici di esempio; tutorial in cinque checkpoint; risultati, replay per fine turno e resoconto strategico. Le statistiche distinguono solo, hot-seat e personalizzate.
- Carte avorio con sagome dei territori, figure militari, Jolly a rosa dei venti e tre alloggiamenti per il tris. Obiettivo Torneo con mappa evidenziata e punti.
- Dialoghi di attacco e spostamento compatti, dadi rossi/avorio, probabilità di conquista e soglia Blitz, spostamento minimo automatico dopo conquista e selezione di eventuali carri aggiuntivi.
- Ritmo bot e filtro animazioni indipendenti, riepilogo pubblico del turno, budget di valutazione IA e personalità tattiche. Il livello facile mantiene il comportamento precedente.
- Riutilizzo del nodo SVG durante gli aggiornamenti, conservazione della camera, hitbox di almeno 44 CSS px e gestione delle etichette per limitare sovrapposizioni. Nessuna clonazione profonda ripetuta delle intere cronologie append-only a ogni comando.

## Regole conservate

Solo obiettivi Torneo da 86 punti; 35/30/25 armate iniziali; mani iniziali da tre; limite del 50% dei territori per continente nella distribuzione; fino a tre dadi in difesa; 130 carri per giocatore; massimo sette carte; protezione dall'eliminazione nei primi quattro giri. Il badge da 12 è soltanto grafico. Regular con Sdadata oppure Countdown 30/45/60 minuti, predefinito 45. Antartide facoltativa e disattivata di default: due territori, bonus +2, obiettivi invariati.

Le sortite disperate restano rare e subordinate all'insufficienza dei rinforzi: medio 3%, difficile 2%, solo con non più di otto territori e reddito effettivo inferiore a tre. Survival non usa questa concessione.

## Anteprime

L'archivio delle anteprime è separato dal delta del progetto. `index.html` contiene la tavola riepilogativa; 30 HTML numerati mostrano singole schermate, con asset e font incorporati. I dati sono dimostrativi e i comandi di gioco non sono attivi. Sono esportazioni statiche dei componenti e dei CSS di produzione, **non screenshot né test di layout eseguiti in browser**. Solo la posizione del pannello flottante è fissata per il rendering statico, mentre l'app la calcola sul DOM.

Rigenerazione: `node scripts/export-previews.mjs`. Su una macchina con Chromium Playwright funzionante: `node scripts/capture-previews.mjs artifacts/previews-2.0` produce anche PNG singoli e tavola PNG. Gli export non scrivono salvataggi e non avviano il Worker di gioco.

Vedi [verifiche effettivamente eseguite](VERIFICHE-2.0.md) e [specifica integrativa](SPECIFICA-2.0.md).
