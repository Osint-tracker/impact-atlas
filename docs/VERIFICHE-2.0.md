# Verifiche 2.0.0 — 5 novembre 2026

## Esito effettivo

| Verifica | Esito |
| --- | --- |
| `npm test` | **152 test passati**, 0 falliti, 0 saltati; circa 79 secondi |
| `npm run typecheck` | Passato |
| `npm run build` | Passato; Vite, 62 moduli; PWA generata |
| `npm run pwa:verify` | Passato; 23 risorse offline, percorsi relativi e icone valide |
| `node scripts/verify-package.mjs` | Passato; 25 file locali, import risolti, nessun endpoint remoto |
| `npm run ios:verify` | Passato, **controllo statico** |
| `npx cap sync android` | Passato |
| Generazione anteprime | 30 schermate e tavola HTML, asset incorporati; controllo statico dei contenuti |
| `npm run test:ui` / `npm run test:pwa` | **Non verificati in questa sessione**: sandbox impedisce l'avvio di Chromium/socket |
| APK Android / build Xcode | **Non compilati in questa sessione** |

I test comprendono engine, Antartide, giocabilità, strategia, animazioni, backup, UI con DOM simulato e integrazione Worker. Quest'ultima verifica che non escano stato o transizioni durante il passaggio privato prima di `REVEAL`. La UI simulata non sostituisce prove di layout, touch o rendering nel browser.

Il tentativo di Chromium headless è fallito con `setsockopt: Operation not permitted`. Non è un test passato, né prova che i timeout precedenti siano definitivamente risolti. Gli script browser sono aggiornati al nuovo layout e devono essere eseguiti nel workflow GitHub prima della distribuzione. Non sono stati aggiunti click forzati per aggirare elementi sovrapposti.

## Benchmark IA

Esecuzione di `npm run benchmark:progression`: **60 coppie, 120 partite**, posti invertiti, mappe classica/Antartide alternate, terzo avversario indipendente. Budget di produzione. Report completo: [benchmark-progression-2.0.json](benchmark-progression-2.0.json).

| Confronto | Partite | Vittorie livello superiore | Vittorie livello inferiore | Vittorie terzo bot | Differenza media punti |
| --- | ---: | ---: | ---: | ---: | ---: |
| Medio / Facile | 40 | 19 | 11 | 10 | +11,725 |
| Difficile / Medio | 40 | 23 | 16 | 1 | +13,6 |
| Survival / Difficile | 40 | 24 | 11 | 5 | +31,05 |

Tempo decisione p95: **26,143 ms**; massimo osservato **159,119 ms**. I budget interrompono la valutazione tra lotti completi, quindi non sono un limite rigido per ogni singola chiamata. Il risultato indica progressione nel campione; non certifica la forza contro un giocatore umano, né una superiorità statistica generalizzabile. Le tattiche specifiche e gli invarianti continuano a essere verificati dai test di scenario.
