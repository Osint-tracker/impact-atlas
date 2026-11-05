# RisiKo Offline 2.0.0 — Android, iPhone e PWA

Gioco offline in italiano per 3–5 partecipanti: un umano contro i bot oppure più umani in Pass & Play, sempre con almeno un umano. Solo obiettivi Torneo da 86 punti, difesa fino a tre dadi, distribuzione iniziale con limite continentale del 50%. Antartide facoltativa, disattivata di default. Il badge da 12 carri è una soglia grafica, non un nuovo limite.

## Aggiornamento 2.0.0 — Atlante

Redesign navy/ottone/avorio, menu verticali, preparazione con seed condivisibile, Pass & Play privato, slot e profili multipli, scenari/editor, tutorial, replay e analisi. Restano esclusivamente gli obiettivi Torneo.

- [Installazione dello ZIP con sole modifiche](docs/AGGIORNAMENTO-2.0.md)
- [Verifiche e limiti della consegna](docs/VERIFICHE-2.0.md)
- [Specifica integrativa 2.0](docs/SPECIFICA-2.0.md)
- Anteprime: `node scripts/export-previews.mjs`, poi apri `artifacts/previews-2.0/index.html`.

Le sezioni seguenti documentano le release precedenti.

## Aggiornamento 1.8.0 — PWA gratuita su iPhone

PWA installabile dalla Home di Safari, avvio offline dopo il primo download completo, backup/importazione di partita e statistiche. Rotazione assistita della plancia iPhone, con menu verticali; Android nativo conserva il suo plugin e non carica la cornice web.

Corretto anche il pannello d'attacco che copriva il pulsante zoom e bloccava il test browser della 1.7.0.

- [Pubblicazione su GitHub Pages e installazione iPhone](docs/PWA-IPHONE.md)
- [Dettagli aggiornamento e verifica](docs/AGGIORNAMENTO-1.8.0.md)
- Build: `npm run build`; verifica pacchetto: `npm run pwa:verify`; test Chromium/WebKit: `npm run test:pwa` dopo la build e l'installazione dei browser Playwright.

Il workflow manuale **Pubblica PWA su GitHub Pages** esegue i test e pubblica il sito. Nessun account Apple Developer richiesto per la PWA. La build nativa Android segue il workflow precedente.

## Aggiornamento 1.7.0 — zoom stabile e iPhone

Attacchi, piazzamenti e animazioni mantengono lo zoom scelto dal giocatore. Nessun allargamento temporaneo della mappa per mostrare territori fuori inquadratura.

Progetto iPhone incluso in `ios/`: su Mac con Xcode 26+, esegui `npm ci`, `npm run ios:setup`, `npm run ios:open`. La compilazione/firma nativa va verificata su Mac o nel workflow iOS: non è stata eseguita in questo ambiente Linux. L’APK resta solo Android.

[Installazione iPhone e TestFlight](docs/IPHONE.md) · [Dettagli aggiornamento](docs/AGGIORNAMENTO-1.7.0.md).

## Aggiornamento 1.6.0 — menu coerenti e correzioni tattiche

Statistiche, regole, audio, strategie e risultati passano allo stile verticale azzurro/blu della home. I bot Medio/Difficile/Survival riconoscono le incursioni convenienti sui continenti avversari, riservano rinforzi ai propri accessi e gestiscono le attese entro il tempo del turno.

La verifica include scenari degli errori segnalati dal giocatore, eseguiti anche sulla 1.5.0, e una nuova arena con avversari indipendenti e cronometro attivo: `npm run benchmark:arena`. Non si considera la sola percentuale di vittorie contro altri bot una prova di forza contro umani.

Dettagli e limiti: [AGGIORNAMENTO-1.6.0.md](docs/AGGIORNAMENTO-1.6.0.md). Anteprime statiche: [statistiche](docs/profile-1.6-preview.html), [audio](docs/audio-1.6-preview.html).

## Aggiornamento 1.5.0 — home verticale e pianificatore IA

- Menu verticale con pulsanti reali; configurazione partita separata. Android passa in orizzontale soltanto per la plancia.
- Medio, Difficile e Survival concentrano i rinforzi, sfruttano le riconquiste convenienti e riportano le armate interne verso i confini. Facile conserva il comportamento 1.4.
- Difficile e Survival confrontano campagne, fallimenti e risposte avversarie con simulazioni limitate e informazioni pubbliche. Survival si può scegliere per singolo bot o per l’intero tavolo dalla home.
- Gli attacchi evidenziano soltanto origine e bersagli validi; eliminato lo strato dorato pieno che colorava gli altri territori.
- Antartide facoltativa, salvataggi, obiettivi Torneo, icona e animazioni mantenuti.

Dettagli, limiti delle misure e verifiche: [AGGIORNAMENTO-1.5.0.md](docs/AGGIORNAMENTO-1.5.0.md). Anteprima **statica**, con pulsanti non operativi: [home-preview-1.5.0.html](docs/home-preview-1.5.0.html).

Per il confronto riproducibile contro IA precedenti e un avversario opportunista: `npm run benchmark:planner`. Variabili opzionali: `BENCH_SEEDS` (numero di serie da 18 partite) e `BENCH_OFFSET` (nuovi semi di verifica).

## Aggiornamento 1.4.1 — leggibilità e fluidità

- Carri e nomi seguono lo zoom della mappa. Restano i “+” di piazzamento e le animazioni.
- Barra giocatori, plancia, comandi e velocità hanno spazi separati. Zoom orizzontale distinto da Cronologia; il menu laterale scorre sui display bassi.
- Home azzurra con griglia e triangoli, pannelli blu, profilo con emblema e pulsante GIOCA arancione, seguendo l’ultimo riferimento fornito.
- Trascinamento calcolato con la trasformazione SVG effettiva, aggiornamenti raggruppati per fotogramma, meno filtri e ridisegni.
- Timer senza clonazione continua della cronologia, messaggi compatti ogni secondo, salvataggio di ogni azione invariato e checkpoint del solo tempo ogni cinque secondi.
- Presentazioni più brevi: piazzamento 360 ms, attacco 1,8 s a ritmo Normale; a Veloce/Istantaneo anche le mosse umane sono accelerate.

[Anteprima statica home 1.4.1](docs/home-preview-1.4.1.html) · [Note e verifiche](docs/AGGIORNAMENTO-1.4.1.md)

## Aggiornamento 1.4

- Plancia blu profondo, pannelli antracite/ambra, carri isometrici e scudo numerico separato; confini continentali e rotte curve con ancoraggi costieri.
- Home con impostazioni a tutta larghezza sui telefoni, statistiche locali, ripresa con numero del giro e selettore Regular/Countdown (30/45/60 minuti, default 45).
- Barra giocatori superiore, widget obiettivo espandibile, punti sulla mappa, preview Tris e Undo dei rinforzi prima di iniziare l’attacco.
- Dadi rossi/blu, rotolamento di 350 ms, confronti ordinati, probabilità di conquista, Blitz con soglia di arresto, vettore d’attacco e marcia dei carri.
- Audio locale e procedurale, feedback aptico opzionale, tre velocità dei bot e personalità configurabili.
- Auto-save IndexedDB con copia completa in localStorage, statistiche dadi e andamento dei punti obiettivo.
- Nuova icona Android e browser: la seconda immagine fornita dall’utente, con logo RisiKo! e cane.

[Note, regole e verifiche della 1.4](docs/AGGIORNAMENTO-1.4.md) · [Anteprima statica della home](docs/home-preview-1.4.html)

L’interfaccia è una ricostruzione indipendente basata sulle specifiche e sui riferimenti forniti. Il sito ufficiale non era consultabile da questo ambiente (restrizione geografica); non è stata verificata un’identità pixel per pixel.

Le sezioni seguenti conservano la cronologia degli aggiornamenti precedenti; per grafica e funzionalità correnti fa fede la sezione 1.4.

## Correzione home 1.3.1

Sui display in orizzontale alti al massimo 600 px, le impostazioni usano tutta la larghezza disponibile. Opzioni partita e avversari sono affiancati da 760 px di larghezza; sui display più stretti si dispongono in verticale. Scorre l’intero pannello, inclusi titolo e comandi, con GIOCA e Riprendi nella stessa barra. Nessuna piccola area di scorrimento interna. I cambi di opzione conservano la posizione di scorrimento.

[Anteprima statica aggiornata](docs/home-preview-1.3.1.html) · [Istruzioni e verifiche](docs/CORREZIONE-HOME-1.3.1.md).

## Novità 1.3

- Attacchi con mirino, flash, clash e suono sincronizzato.
- Home nei colori dei carri e impostazioni dei bot personalizzabili.
- Bot più attenti a territori deboli, continenti, riserve e contrattacchi.
- Sortite rare solo con al massimo 8 territori e meno di 3 rinforzi totali.
- Antartide opzionale: due territori, tre collegamenti intercontinentali, +2 rinforzi.

## Correzione grafica 1.2.1

Silhouette ricalcata dal nuovo dettaglio: corpo più pieno, canna larga e profilo superiore compatto. Cifre dritte e spesse, con carattere incorporato offline: il numero 1 è stato ridisegnato dal riferimento; gli altri numeri derivano da Open Sans ExtraBold. Il font originale non è identificabile con certezza dalla sola immagine sfocata. Anteprima: [token-preview-v1.2.1.png](docs/token-preview-v1.2.1.png).

Verifica della correzione: 14 test di rendering/animazione superati, build web locale e import verificati. Il controllo browser del caricamento del carattere è incluso nella CI, da eseguire su GitHub. Versione Android 1.2.1, versionCode 4.

## Aggiornamento 1.2

- Carte e obiettivo rimangono aperti durante attacchi e cambi di turno dei bot, con selezione e scorrimento conservati.
- Alla conquista vengono trasferiti subito i carri obbligatori (numero di dadi dell’ultimo attacco). La finestra compatta da 280–290 px permette di scegliere soltanto gli aggiuntivi, partendo da zero.
- Nessuna freccia sulla mappa: impulsi coordinati sui territori, indicatori colorati +N/−N e transizioni dei contatori. I dadi restano quelli della versione 1.1.
- Carro armato ridisegnato seguendo la foto, con numero interno adattato fino a tre cifre.
- Ripresa dei vecchi salvataggi post-conquista con trasferimento del minimo una sola volta.

Istruzioni: [AGGIORNAMENTO-1.2.md](docs/AGGIORNAMENTO-1.2.md). Rimangono tutte le funzioni della [versione 1.1](docs/AGGIORNAMENTO-1.1.md), compresi setup corretto e tempi umani dei bot.

## Stato della consegna 1.3

Nuova home, clash con audio locale, strategie configurabili per ciascun bot e modalità **Antartide** facoltativa. Istruzioni complete: [AGGIORNAMENTO-1.3.md](docs/AGGIORNAMENTO-1.3.md). Anteprima della mappa: [antarctica-preview.png](docs/antarctica-preview.png).

TypeScript, build React/Vite, verifica degli asset offline e sincronizzazione Capacitor completati localmente. Confronto di 90 partite con la precedente IA e test specifici dell’Antartide completati. Il collaudo browser e il nuovo APK restano da eseguire nel workflow GitHub: qui l’avvio del server e di Chromium è impedito dai permessi di rete/sistema e non è installato l’SDK Android. Dettagli nel [rapporto 1.3](docs/VERIFICA-1.3.md).

Le sezioni 1.2 e 1.2.1 sopra descrivono le verifiche storiche di quelle versioni.

## Prova locale senza installare pacchetti

Richiede Node 22.18+ (verificato con Node 24). Su un computer che consenta l’apertura di una porta locale:

```bash
node scripts/serve.mjs
```

Aprire `http://localhost:4173`. Il server serve soltanto i file locali. Non aprire direttamente `index.html` dal filesystem: Web Worker e IndexedDB richiedono un’origine supportata dal browser.

```bash
npm test
npm run build
node scripts/verify-package.mjs
```

Senza dipendenze installate, `npm run build` rimuove le annotazioni TypeScript tramite Node e crea `dist/`, senza scaricare nulla. Dopo `npm install` usa automaticamente Vite e il contenitore React. Il renderer imperativo è condiviso fra le due modalità per mantenere identico il comportamento.

## Compilazione APK

Prerequisiti sulla macchina di build: Node 22.18+, JDK 21, Android SDK Command-line Tools, piattaforma Android 36, Build Tools 36.0.0, curl, unzip e sha256sum. Occorre rete sulla macchina di build per scaricare dipendenze; il gioco installato non la usa.

```bash
npm install
npm run typecheck
npm test
npm run build
npx cap sync android
cd android
./gradlew assembleDebug
```

APK previsto: `android/app/build/outputs/apk/debug/app-debug.apk`.

Impostare `ANDROID_HOME` sul proprio Android SDK. Lo script `scripts/prepare-android.sh` installa i componenti SDK richiesti tramite `sdkmanager`, sincronizza Capacitor, compila e copia l’APK e il checksum in `artifacts/`. Le licenze SDK vanno accettate con gli strumenti Android sulla macchina di build.

Il launcher `android/gradlew` è un bootstrap sorgente per Gradle 8.14.3: scarica la distribuzione e ne confronta il checksum pubblicato. Non è incluso un wrapper JAR precompilato. Su Windows usare WSL oppure Android Studio con Gradle 8.14.3 e JDK 21.

È incluso un workflow GitHub Actions per build, test e caricamento dell’APK come artifact. Usa Node 24, JDK 21 e gli strumenti SDK preinstallati nel runner `ubuntu-24.04`, quindi installa i componenti Android 36. I download richiedono rete soltanto durante la compilazione.

### Se GitHub Actions fallisce

Il workflow conserva stdout e stderr di ogni comando nell’artifact **log-compilazione**, disponibile nella pagina del run anche in caso di errore. I testi delle licenze sono salvati separatamente in `android-licenses.log`; gli errori Gradle includono lo stack trace. Le verifiche restano obbligatorie: un loro errore blocca la consegna dell’APK.

Per aggiornare un repository esistente con questa correzione, sostituire `.github/workflows/android.yml` e aggiungere `scripts/ci-run.sh`, `scripts/ci-android-sdk.sh` e `tests/ci.test.ts`, conservando questi percorsi. Poi avviare un **nuovo run** da Actions → Build e verifica Android → Run workflow. «Re-run jobs» di un vecchio run riutilizza il vecchio commit.

I log completi del tentativo esterno confermano il fallimento del vecchio workflow su `sdkmanager tools`: il pacchetto non è stato trovato. Il workflow aggiornato elimina questa richiesta e usa solo i componenti necessari. Vedi [analisi e correzioni CI](docs/CORREZIONE-CI.md). La nuova build Android completa resta da verificare su GitHub.

## Uso

1. Scegli nome, 3–5 giocatori, difficoltà di ogni bot e tempo per turno (3 minuti per impostazione iniziale).
2. Durante il setup tocca i tuoi territori per piazzare un carro; la mano passa dopo tre carri o all’esaurimento della dotazione.
3. All’inizio del turno puoi cambiare un tris; poi piazza tutti i rinforzi.
4. Seleziona un tuo territorio e un nemico adiacente per combattere. Il blitz si può interrompere e si ferma ai limiti legali.
5. Dopo una conquista scegli le armate da trasferire. Alla fine puoi effettuare un solo spostamento adiacente.
6. Il pulsante con l’occhio evidenzia l’obiettivo; il cerchio concentrico apre la carta. Il pannello destro mostra anche carte e informazioni sul territorio selezionato.
7. Zoom con rotella, pulsanti o pinch; trascina per spostare la plancia. `NOMI` attiva/disattiva le etichette. Velocità bot 1×/2×/4×/rapida.
8. L’app salva dopo ogni azione; quando va in background si mette in pausa. Alla riapertura usa **Riprendi**. La creazione di un nuovo tavolo chiede conferma prima di sostituire il salvataggio.

I livelli dei bot casuali e gli obiettivi avversari vengono rivelati soltanto nei risultati. Dopo l’eliminazione del giocatore umano, i bot proseguono: puoi osservare e accelerare la conclusione.

## Regole e fedeltà

Il MHT fornito dall’utente è il riferimento prioritario. Le integrazioni approvate (130 carri, 7 carte, guarnigioni, protezione iniziale, spareggi, ecc.) sono riportate in [REGOLE.md](docs/REGOLE.md). La formula locale dell’esperienza è distinta dai livelli ufficiali.

La plancia ha 42 territori e 164 punti. Ogni obiettivo vale 86 punti. Le appartenenze dei 16 obiettivi sono ricostruite dalle descrizioni territoriali e confrontate con 120 sovrapposizioni pubblicate; tre discrepanze della fonte secondaria sono esplicitate in [FONTI.md](docs/FONTI.md), senza occultarle nei test.

Le geometrie, le icone e la texture sono ricostruzioni locali. Il mazzo ha 14 simboli di ciascun tipo più due Jolly; l’associazione specifica simbolo–territorio è una distribuzione locale bilanciata, non una trascrizione verificata delle carte fisiche. Il MHT non specifica queste associazioni.

## Architettura

- `src/data`: topologia stabile, geometria da riferimento, continenti, mazzo e obiettivi.
- `src/engine`: FSM deterministica, validazione, dadi, timer, sdadata e Worker autorevole. Le viste pubbliche escludono carte/obiettivi altrui, stato del generatore casuale e mazzo.
- `src/ai`: strategie Facile/Medio/Difficile, distribuzioni esatte dei dadi, probabilità di conquista e inferenza basata sulle sole azioni pubbliche.
- `src/services`: IndexedDB con transazioni; risultati idempotenti per ID partita.
- `src/gui`: interfaccia e interazione touch; ingresso React opzionale per la build Vite.
- `android`: host Capacitor, landscape e fullscreen, rimozione del permesso Internet nel manifest sorgente.

Gli eventi `onGameStateUpdate`, `onDiceRolled`, `onTurnChanged`, `onSdadataRoll`, `onGameOver` sono emessi dal Worker dopo il salvataggio. I comandi includono ID univoco e revisione per impedire doppie applicazioni. Il routing locale usa hash con i percorsi `/app/gui/user/home`, `/app/gui/game/table`, `/app/gui/game/results`, `/app/gui/profile/local`.

Compatibilità prevista: Android 7+ con Android System WebView aggiornato. La UI usa funzioni moderne del browser; è consigliato WebView 111+. Il comportamento effettivo su Android e in modalità aereo richiede ancora il collaudo indicato nel rapporto.
