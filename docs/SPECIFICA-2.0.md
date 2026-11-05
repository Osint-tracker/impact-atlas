# Specifica integrativa 2.0.0

Questa specifica descrive le differenze implementate rispetto al documento storico `technical specification.md` della 1.8.0. I sorgenti e i test restano il riferimento per le formule e i parametri esatti. Le funzionalità online raffigurate in precedenti riferimenti estetici non fanno parte dell'app offline.

## Architettura e flusso

`src/gui/App.ts` orchestra eventi, schermate, selezioni, camera, presentazioni e protocollo Worker. I template sono divisi in `src/gui/views/`: HomeView, SetupView, BoardView, BoardMap, CombatDock, CardsView, ProfileView, LibraryView, ReplayView. `Icons.ts` centralizza gli SVG; `art.ts` contiene carri, badge, dadi, carte e grafici. `patchElement.ts` aggiorna attributi e figli preservando l'identità del nodo mappa e i suoi listener.

Il Worker rimane l'autorità: valida attore/revisione, applica il comando, salva, proietta le informazioni autorizzate e pubblica stato/transizione. La presentazione termina con ACK; clock e bot non procedono mentre è in corso. Il riepilogo usa `SUMMARY_ACK`. Il rendering non sceglie risultati dei dadi né risolve combattimenti.

`applyCommand` clona il corpo mutabile ma condivide le righe immutabili delle cronologie già registrate, creando nuovi array per append. `project` costruisce una copia pubblica senza copiare preventivamente l'intero archivio privato. Replay e analisi non sono esposti prima della fine partita. Le mutazioni sul GameView non devono modificare il GameState originale.

## Stato e privacy

GameState v2 mantiene compatibilità in lettura con v1 e aggiunge `initialSeed`, `seedLabel`, `custom`, `tutorialStep`, `replay`, `analysis`, `recordResults`. Player può avere `profileId`. Config contiene i partecipanti: nome, umano/bot, profilo e difficoltà. Almeno un umano; 3–5 posti; profili persistenti non duplicati tra umani dello stesso tavolo.

L'ID della partita è un UUID indipendente dal seed: replicare una distribuzione non sovrascrive lo slot precedente. Il seed governa la riproducibilità del motore; budget dipendenti dal tempo, mosse dell'utente e scheduling non promettono partite interamente identiche.

`PrivacyGate` crea un token per la consegna. `PRIVACY` invia solo giocatore, nome, colore e token; la UI rimuove stato e finestre precedenti. `REVEAL` richiede destinatario/token corretti. Il fallback localStorage della partita è disabilitato per hot-seat; lo stato completo rimane in IndexedDB per il salvataggio. È una protezione del passaggio a schermo, non cifratura contro chi ispeziona lo storage o il backup.

## Persistenza

IndexedDB `risiko-offline`, versione 2: store `game`, `profile`, `results`, `slots`, `profiles`, `scenarios`. I salvataggi aggiornano partita corrente, slot e risultati nella stessa transazione. I record risultato sono idempotenti; una duplicazione di partita conclusa imposta `recordResults=false`.

Backup v2: archivio di slot, profili, risultati e scenari; massimo 64 MiB; validazione strutturale, stato di gioco, identificatori, storie, proprietà pericolose e versioni. V1 viene normalizzato. Importazione `replace` atomica; `merge` rimappa identificatori e riferimenti per aggiungere copie senza perdere l'archivio corrente. Errori o quota insufficiente interrompono la transazione.

## Seed e scenari

`MatchCode.ts`: seed testuale convertito con FNV-1a; codice `RKO2:` con JSON codificato, validato attraverso la configurazione del motore. I profili locali non vengono condivisi come identità portabili: al destinatario vengono assegnati profilo locale/ospiti. Il campo è in un pannello richiudibile in preparazione.

`Scenarios.ts`: editor di posizione già avviata, giri 1–19; proprietà/armate per territorio, giocatore attivo e configurazione. Tutti devono essere ancora in gioco, nessuno già a 86 punti. Il limite continentale iniziale non viene applicato a queste posizioni intermedie. Tre esempi curati espongono ingressi a Oceania, Europa e Antartide. Non sono puzzle con vittoria classica alternativa.

## Tutorial, replay, statistiche

Tutorial non persistente in cinque checkpoint deterministici: piazzamento, tris, attacco a tre dadi, conquista, spostamento. `tutorialComplete` controlla gli eventi prodotti dal motore.

Replay per checkpoint di fine turno, termine setup e fine partita: giro, attore, revisione, coppie proprietario/armate, punti e indice log. Non è un video di ogni animazione. A fine partita l'analisi riporta incremento punti maggiore, conquiste, confini continentali esposti e osservazioni sulle decisioni/dadi disponibili.

Profili: categorie solo/hotseat/custom; partite, vittorie, media punti, RisiKo, Sdadata, andamento e dadi. XP per partita non custom: 100 + 2×punti obiettivo +100 per vittoria +50 per vittoria RisiKo. Livello: 1 + floor(XP/500). Il grafico delle difficoltà può contare un tavolo misto in più categorie.

## Presentazione e accessibilità

Palette: navy `#0B1F2E`, slate `#1E3440`, teal `#0F6863`, ottone `#C8A45A`, avorio `#F2E7D3`. Superfici bordate, gradienti sobri, cornici arrotondate, texture cartografica, rotte curve tratteggiate. Font locali AtlasSerif/AtlasUI/AtlasHeading (Liberation Serif Bold e Open Sans, licenze incluse) e RisikoCounter.

Home: logo/crest, mascotte, nuova/continua partita, navigazione verticale. Preparazione: selettori rapidi, partecipanti, strategie, timer, modalità, Antartide, seed, avvio. Carte: avorio, sagome e figure militari; Jolly a bussola; tre alloggiamenti e bonus dettagliati. Obiettivo: esclusivamente Torneo.

Plancia: header turno/fase/giro; mappa centrale; sidebar obiettivo/carte/registro; zoom separato; footer medaglioni e comandi contestuali. Map hitbox almeno 44 CSS px, carri solidali alla scala della mappa, collisioni etichette limitate. Clic e tastiera per territori; pressione lunga per ispezione; pan/pinch cancellano la pressione. Le anteprime statiche non eseguono il calcolo DOM di collisioni e posizionamento dei pannelli.

Combattimento: pannello flottante, probabilità e superstiti attesi, dadi rossi/avorio, Blitz e soglia. Spostamento: larghezza 288 px, slider e Min/−1/+1/+5/Max; la conquista mostra il minimo già trasferito. Resta consultabile la mano durante le animazioni bot. Rispettata la preferenza reduced-motion; audio locale/procedurale e aptica opzionale.

## IA e prestazioni

Planner, simulazione e valutazione tattica esistenti conservati con personalità distinte. `Budget.ts` introduce finestre nominali di 15/40/80/140 ms: scelta sulla base dei lotti interamente valutati, fallback deterministico. Non è una deadline hard real-time. Strategie perseguono opportunità, rottura dei bonus, punti obiettivo, riserve e trasferimenti; il benchmark confronta livelli adiacenti con posti scambiati e terzo avversario indipendente.

Il facile non viene reso più aggressivo. Medio e difficile conservano le rare sortite sotto la soglia di reddito, Survival punta alla massima forza senza tale concessione. La superiorità nei bot benchmark non va equiparata a invincibilità contro persone.

## Distribuzione e limiti verificati

Vite produce `dist/`; il builder PWA prepara cache offline e revisione. Android usa Capacitor, versione 2.0.0/build 13. iPhone via PWA o progetto Xcode; icona da sorgente condivisa. Il delta di consegna esclude dist, dipendenze, cache e asset Android generati da sync. I test browser e le build native restano da eseguire in un runtime abilitato: leggere `VERIFICHE-2.0.md`.
