import {combatDock,moveDock} from './views/CombatDock.ts';
import {cardsView} from './views/CardsView.ts';
import {boardMap} from './views/BoardMap.ts';
import {patchElement} from './patchElement.ts';
import {resolveMapPointers} from './mapPointer.ts';
import {tutorialSteps} from '../engine/Tutorial.ts';
import {profileView} from './views/ProfileView.ts';
import {replayView} from './views/ReplayView.ts';
import {icon,decorateIcons} from './Icons.ts';
import {homeView} from './views/HomeView.ts';
import {setupView,participants} from './views/SetupView.ts';
import {boardView} from './views/BoardView.ts';
import {slotsView,profilesView,scenariosView,editorView,type SlotView} from './views/LibraryView.ts';
import {seedNumber,matchCode,readMatchCode} from '../services/MatchCode.ts';
import {scenarioFrom,curatedScenarios,scenarioGame,type Scenario} from '../services/Scenarios.ts';
import type {LocalProfile} from '../services/StorageService.ts';
import {conquestEstimate} from '../ai/Strategy.ts';
import {initWebApp,redirectForApple,webStatus,installWebApp,updateWebApp,sendWebOrientation} from '../pwa/client.ts';
import {isNativeApp,isAppleMobile,rotationAssistance} from '../pwa/platform.ts';
import {parseBackup,MAX_BACKUP_BYTES,type Backup} from '../services/Backup.ts';
import {setGameOrientation} from './orientation.ts';
import {seaRoute} from './mapGeometry.ts';
import {conquestOdds} from '../ai/BotAI.ts';
import {tournamentStats} from '../services/TournamentStats.ts';
import {readLocalSnapshot,writeLocalSnapshot,clearLocalSnapshot} from '../services/LocalSnapshot.ts';
import {GameAudio} from './AudioService.ts';
import {botPolicy,policyOptions} from '../ai/policy.ts';
import { allTerritories as territories, objectives, allCards as cards, colors, mapFor } from '../data/mapGraph.ts';
import { canAttack, moveBounds, validTris, exchangeValue } from '../engine/RisikoRulesEngine.ts';
import type { GameView, Command, GameConfig } from '../engine/types.ts';
import {tankModel,armyBadge,scoreChart,armyToken, counterFontSize, tankShape, territoryColors, cardFace, objectiveMap, dieMarkup} from './art.ts';
import {playTransition, positionPanel, bindPanelDrag} from './effects.ts';
import type {Transition} from '../engine/presentation.ts';
import type { Profile } from '../services/StorageService.ts';
const escape = (v: unknown) => String(v ?? '').replace(/[&<>"']/g, c => ({
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;'
} as Record<string, string>)[c]);
const phases = ['Rinforzi', 'Attacco', 'Spostamento', 'Fine turno'];
const phaseNames: Record<string, string> = {
  SETUP_PLACEMENT: 'Piazzamento iniziale',
  TRIS_EXCHANGE: 'Cambio tris',
  REINFORCEMENT: 'Rinforzi',
  ATTACK: 'Attacco',
  POST_CONQUEST_MOVE: 'Territorio conquistato',
  FORTIFY: 'Spostamento strategico',
  CARD_DRAW: 'Pesca carta',
  SDADATA_CHECK: 'Sdadata',
  GAME_OVER: 'Partita conclusa'
};
const levelNames: Record<string, string> = {
  facile: 'Facile',
  medio: 'Medio',
  difficile: 'Difficile',
  survival: 'Survival',
  casuale: 'Casuale · Blind',
  '?': '?'
};
const tank = '<path d="M2 10h26l-3-5h-7V2h-7v3H7zM4 12h22a4 4 0 010 8H4a4 4 0 010-8zm1 3a1.5 1.5 0 100 3 1.5 1.5 0 000-3zm7 0a1.5 1.5 0 100 3 1.5 1.5 0 000-3zm7 0a1.5 1.5 0 100 3 1.5 1.5 0 000-3zM16 3h17v3H16z"/>';
const tankIcon = `<svg viewBox="0 0 50 36" class="tank-icon" aria-hidden="true">${tankModel('currentColor')}</svg>`;
const duration = (ms: number) => `${Math.floor(ms / 60000)}:${String(Math.floor(ms / 1000) % 60).padStart(2, '0')}`;
export function mountGame(root: HTMLElement): () => void {
  if(redirectForApple())return ()=>{};
  const sounds=new GameAudio();
  const sound=(kind='tap')=>kind==='battle'?sounds.clash():sounds.tap();
  let strategyBot=0;
  let inspected:number|null=null,inspectedPlayer=0;
  let resumeRequested=false;
  let tutorialDone=false,turnSummary:string[]|null=null;
  let profileCategory='solo',replayIndex=0;
  let seedOpen=false,selectedProfile='local',animationFilter='all',mapLayer='standard';
  let localProfiles:LocalProfile[]=[],slots:SlotView[]=[],scenarios:Scenario[]=[],editingScenario:Scenario|null=null,editorTerritory=0;
  let privacy:{player:number;name:string;color:string;token:string}|null=null;
  const viewer=()=>state?.viewer??0;

  let backupURL='',backupText='',importText='',importPreview:Backup|null=null;
  const backupFilename=()=>`risiko-backup-${new Date().toISOString().slice(0,10)}.json`;
  let objectiveExpanded=false,blitzStop=3;
  let localSaveWarning=false;
  const worker = new Worker(new URL('../engine/server.worker.ts', import.meta.url), {
    type: 'module'
  });
  let state: GameView | null = null,
    profile: Profile = {
      name: 'Comandante',
      results: []
    },
    screen = 'home',
    ready = false,
    hasGame = false;
  let showNames = true;
  let placementCount = 1;
  let lastPresentedId = '';
  let dialogMarkup = '';
  let dialogExit: ReturnType<typeof setTimeout> | null = null;
  const consulting = () => ['cards','objective','game-menu','audio','rules','player-info'].includes(modal??'');
  let panelKey = '';
  let panelPosition: {x:number;y:number} | null = null;
  let presenting: Transition | null = null;
  let cancelEffects: (() => void) | null = null;
  let blitzTimer: ReturnType<typeof setTimeout> | null = null;
  function cancelPresentation() {
    sounds.stop();
    cancelEffects?.(); cancelEffects=null; presenting=null;
    if(blitzTimer){clearTimeout(blitzTimer);blitzTimer=null;}
  }
  let selected: number | null = null,
    target: number | null = null,
    modal: string | null = null,
    objectiveView = false,
    showLog = false,
    speed = 1,
    muted = sounds.muted,
    pendingCommand = false,
    blitz = false;
  let config: GameConfig = {
    antarctica: false,
    manualReinforcement:true,
    endMode:'regular',countdownMinutes:45,botSpeed:1,
    players: 4,
    name: 'Comandante',
    difficulties: ['medio', 'medio', 'medio'],
    turnSeconds: 180,
    botPolicies:['equilibrato','stratega','incursore','conquistatore','equilibrato'].map(personality=>botPolicy({personality:personality as never}))
  };
  let viewport = {
      x: 0,
      y: 0,
      w: 1240,
      h: 690
    },
    moveCount = 1,
    cardSelection: number[] = [],
    lastBattleMessage = '',
    toastTimer: ReturnType<typeof setTimeout> | null = null;
  const send = (data: unknown) => worker.postMessage(data);
  const humanTurn = () => !privacy && !!state && state.active === state.viewer && !state.paused && state.phase !== 'GAME_OVER';
  function command(c: Command) {
    if (!state || presenting || pendingCommand || !humanTurn()) return;
    pendingCommand = true;
    send({
      type: 'COMMAND',actor:state.viewer,
      command: c,
      id: crypto.randomUUID(),
      revision: state.revision
    });
    if (!muted && c.type !== 'ATTACK') sound('tap');
  }
  function toast(message: string) {
    const el = root.querySelector<HTMLElement>('#toast');
    if (el) {
      el.textContent = message;
      el.classList.add('visible');
      if (toastTimer) clearTimeout(toastTimer);
      toastTimer = setTimeout(() => el.classList.remove('visible'), 5000);
    }
  }
  function points() {
    return Math.min(86,state?.players[state.viewer].points ?? 0);
  }
  function mapMarkup(preview=false){return boardMap({state,config,viewport,selected,target,modal,inspected,mapLayer,objectiveView,presenting:!!presenting,human:humanTurn(),cardSelection},preview);}
  const header = () => `<header class="portal-header"><button class="portal-brand" data-action="home" aria-label="Menu principale">RisiKo<span>!</span></button><span>TORNEO · OFFLINE</span><button data-action="audio" aria-label="Impostazioni audio">♫</button></header>`;

  function home(){return homeView(profile.name,ready,hasGame||slots.length>0,isNativeApp(),webStatus.ready);}
  function setup(){return setupView(config,ready,localProfiles,seedOpen);}
  function playerCards() {
    return `<aside class="players-row" aria-label="Giocatori">${state!.order.map(id => {
      const p=state!.players[id];
      return `<button data-player-info="${id}" class="player-card ${id===state!.active?'turn-active':''} ${p.eliminated?'eliminated':''}" style="--player:${p.color}"><div class="player-heading"><span class="player-orb"><span></span></span><div class="player-identity"><strong>${escape(p.name)}</strong><small>${p.eliminated?'ELIMINATO':p.id===state!.viewer?'TU':p.human?'UMANO':`Bot ${levelNames[p.difficulty]} · ${p.botPolicy?.personality??'equilibrato'}`}</small></div>${id===state!.active?'<span class="turn-arrow">▶</span>':''}</div><div class="player-numbers"><span title="Carri">${tankIcon}<b>${p.totalArmies}</b></span><span title="Territori">◇ ${p.territoryCount}</span><span title="Carte">▣ ${p.cardCount}</span><span title="Rinforzi">↑${p.nextReinforcements}</span></div></button>`;
    }).join('')}</aside>`;
  }
  function instructions() {
    if (!state) return '';
    if (state.paused) return 'Partita in pausa';
    if (!humanTurn()) return `${escape(state.players[state.active].name)} sta pianificando la prossima mossa…`;
    switch (state.phase) {
      case 'SETUP_PLACEMENT':
        return `Tocca un tuo territorio per piazzare un carro. Restano ${state.setupBatch} in questa mano.`;
      case 'TRIS_EXCHANGE':
        return 'Puoi cambiare le carte prima di piazzare i rinforzi.';
      case 'REINFORCEMENT':
        return state.reinforcements?`Tocca i tuoi territori: hai ${state.reinforcements} rinforzi da piazzare.`:'Rinforzi completati. Puoi annullare un piazzamento o iniziare l’attacco.';
      case 'ATTACK':
        return selected === null ? 'Scegli un tuo territorio per attaccare.' : 'Tocca un territorio nemico confinante per iniziare la battaglia.';
      case 'FORTIFY':
        return 'Scegli origine e destinazione adiacenti, oppure termina il turno.';
      default:
        return 'Completa lo spostamento obbligatorio.';
    }
  }
  function table(){return boardView(state!,mapMarkup(),playerCards(),instructions(),phaseNames[state!.phase],placementCount,showNames,showLog,speed,animationFilter,pendingCommand,objectiveView)+(state!.tutorialStep!==undefined?`<aside class="tutorial-banner"><b>${tutorialSteps[state!.tutorialStep].title}</b><p>${tutorialSteps[state!.tutorialStep].text}</p><button class="primary" data-action="tutorial-next" ${tutorialDone?'':'disabled'}>Continua</button><button data-action="home">Esci dal tutorial</button></aside>`:'');}
  function miniObjective(id: number) { return objectiveMap(id,state?.antarctica===true); }
  function results() {
    const s = state!,
      ranking = [...s.players].sort((a, b) => (b.points ?? 0) - (a.points ?? 0) || (b.outside ?? 0) - (a.outside ?? 0) || s.order.indexOf(b.id) - s.order.indexOf(a.id));
    return `<div class="classic-home portal-route">${header()}<main class="results-page portal-column"><span class="eyebrow">${s.reason === 'risiko' ? 'OBIETTIVO COMPLETATO' : s.reason==='countdown'?'COUNTDOWN TERMINATO':'LA SDADATA HA DECISO'}</span><h1>${s.winner === s.viewer ? 'La vittoria è tua.' : `${escape(s.players[s.winner!].name)} conquista la vittoria.`}</h1><p class="muted">${s.round} giri · ${duration(s.elapsedMs)} di gioco · Tutti gli obiettivi sono ora rivelati</p><div class="results-list">${ranking.map((p, i) => `<article class="result-row" style="--player:${p.color}"><strong class="rank">${String(i + 1).padStart(2, '0')}</strong><div class="result-player"><strong>${escape(p.name)} ${p.id===s.viewer ? '· TU' : p.human?'· UMANO':''}</strong><small>${p.human ? 'Comandante' : `Bot ${levelNames[p.difficulty]}${p.blind ? ' · livello svelato' : ''}`}</small></div><div class="result-objective">${miniObjective(p.objective!)}<span>${escape(objectives[p.objective! - 1].name)}</span></div><div><strong>${Math.min(86,p.points??0)}</strong><small>${p.points === 100 ? 'RISIKO! · 100 PT TAVOLO' : 'IN OBIETTIVO'}</small></div><div><strong>${p.outside}</strong><small>FUORI OBIETTIVO</small></div><div><strong>${p.territoryCount}</strong><small>TERRITORI</small></div><div><strong>${p.cardCount}</strong><small>CARTE</small></div></article>`).join('')}</div>${scoreChart(s.scoreHistory,s.players)}${replayView(s,replayIndex)}<div class="results-actions"><button class="primary" data-action="home">Un’altra strategia. Un nuovo tavolo. →</button><button class="secondary" data-action="profile">Le tue statistiche</button></div></main></div>`;
  }
  function profilePage(){return profileView(profile,profileCategory);}
  function modalMarkup() {
    if (!modal || !state && !['rules','replace','strategy','audio','install','backup','import-confirm','slots','profiles','scenarios','editor','seed-confirm'].includes(modal)) return '';
    let content = '',
      cls = '';
    const s = state;
    if(modal==='player-info'&&s){const p=s.players[inspectedPlayer];content=`<h2>${escape(p.name)}</h2><p>${p.human?'Giocatore umano':`Bot ${levelNames[p.difficulty]}`}</p><dl><dt>Territori</dt><dd>${p.territoryCount}</dd><dt>Armate</dt><dd>${p.totalArmies}</dd><dt>Carte in mano</dt><dd>${p.cardCount}</dd><dt>Rinforzi previsti</dt><dd>+${p.nextReinforcements}</dd></dl>`;}
    if(modal==='inspect'&&s&&inspected!==null){const t=mapFor(s).territories[inspected],cell=s.board[inspected];content=`<h2>${escape(t.name)}</h2><p>${escape(s.players[cell.owner].name)} · ${cell.armies} carri</p><p>${objectives[s.players[s.viewer].objective!-1]?.territories.includes(inspected)?t.points:0} punti obiettivo · ${t.points} fuori obiettivo</p><h3>Confini</h3><p>${t.neighbors.map(id=>escape(territories[id].name)).join(' · ')}</p>`;}
    if(modal==='summary'&&turnSummary)content=`<div class="turn-summary"><h2>Riepilogo turno bot</h2>${turnSummary.map(line=>`<p>${escape(line)}</p>`).join('')}<button class="primary full" data-action="summary-continue">Inizia il mio turno</button></div>`;
    if(modal==='slots')content=slotsView(slots);
    if(modal==='profiles')content=profilesView(localProfiles);
    if(modal==='scenarios')content=scenariosView([...curatedScenarios({...config,participants:participants(config)}),...scenarios]);
    if(modal==='editor'&&editingScenario)content=editorView(editingScenario,editorTerritory);
    if(modal==='seed-confirm')content=`<h2>Applica codice partita?</h2><p>Le impostazioni della preparazione saranno sostituite con quelle incluse nel codice.</p><button class="primary" data-action="seed-confirm">Applica impostazioni</button>`;
    if(modal==='game-menu')content=`<h2>Menu partita</h2><button class="secondary full" data-action="pause">Pausa</button><button class="secondary full" data-action="home">Salva e torna alla Home</button><h3>Ritmo bot</h3><div class="button-row">${[1,2,0].map(n=>`<button data-speed="${n}" class="${speed===n?'active':''}">${n===1?'Normale':n===2?'Veloce':'Istantaneo'}</button>`).join('')}</div><label>Animazioni<select id="animation-filter">${[['all','Tutte'],['involved','Battaglie contro di me e conquiste'],['summary','Solo riepilogo']].map(([v,l])=>`<option value="${v}" ${animationFilter===v?'selected':''}>${l}</option>`).join('')}</select></label><button data-action="audio">Audio e vibrazione</button><button data-action="rules">Regole</button><button data-action="backup">Backup</button><p>Seed: ${escape(s?.seedLabel??s?.initialSeed??'non disponibile per questo salvataggio')}</p>`;
    if(modal==='install'){
      content=`<h2>Installa RisiKo</h2><p>${webStatus.ready?'✓ Gioco pronto offline. Puoi avviarlo senza connessione.':webStatus.error?escape(webStatus.error):'Preparazione offline in corso: tieni aperta questa pagina con Internet attivo.'}</p><p>${isAppleMobile()?'In Safari: Condividi → Aggiungi alla schermata Home. Se compare, lascia attivo «Apri come app web». Poi avvia RisiKo dalla nuova icona.':'Usa «Installa app» nel menu del browser, oppure il pulsante qui sotto se disponibile.'}</p><button class="primary" data-action="pwa-install">${isAppleMobile()?'Richiedi conservazione dati':'Installa app / conserva dati'}</button><p>Esporta periodicamente un backup: cancellare i dati del sito elimina partita e statistiche.</p>${isAppleMobile()?`<label class="audio-switch"><input id="web-rotation" type="checkbox" ${rotationAssistance()?'checked':''}/> Rotazione assistita della plancia su iPhone</label><p>La plancia si dispone in orizzontale anche se Safari non può ruotare lo schermo. I menu restano verticali. Se necessario, gira l’iPhone con il lato superiore verso sinistra.</p>`:''}<button class="secondary" data-action="pwa-update">${webStatus.update?'Aggiorna alla nuova versione':'Controlla aggiornamenti'}</button>`;
    }
    if(modal==='backup'){
      content=`<h2>Salvataggi e backup</h2><p>Il file conserva la partita completa e le statistiche. Salvalo in File o iCloud Drive per recuperarlo anche dopo la cancellazione dei dati del sito.</p><button class="primary" data-action="export-backup">Prepara backup</button>${backupURL?`<a class="secondary backup-download" href="${backupURL}" download="${backupFilename()}">Scarica file JSON</a><button class="secondary" data-action="share-backup">Condividi / Salva in File</button>`:''}<label class="backup-picker">Importa un backup<input id="backup-file" type="file" accept=".json,application/json"/></label><p>Prima della sostituzione vedrai un riepilogo. Il file contiene anche gli obiettivi dei bot: conservalo come salvataggio personale.</p>`;
    }
    if(modal==='import-confirm'&&importPreview){
      const b=importPreview;
      content=`<h2>Ripristinare questo backup?</h2><p>${escape(b.profile.name)} · ${escape(new Date(b.createdAt).toLocaleString('it-IT'))}<br>${b.profile.results.length} partite nelle statistiche<br>${b.game?`${b.game.players.length} giocatori · giro ${b.game.round}${b.game.antarctica?' · Antartide':''}`:'Nessuna partita salvata'}</p><p>${b.slots?.length??(b.game?1:0)} slot · ${b.profiles?.length??1} profili · ${b.scenarios?.length??0} scenari. «Aggiungi come copie» conserva i dati attuali e importa nuovi slot e profili. «Sostituisci tutto» sostituisce l’intero archivio in una sola operazione.</p><button class="primary" data-action="merge-import">Aggiungi come copie</button><button class="secondary" data-action="confirm-import">Sostituisci tutto</button><button class="secondary" data-action="backup">Annulla</button>`;
    }
    if(modal==='strategy'){
      const policy=config.botPolicies![strategyBot];cls='strategy-window';
      content=`<h2>Strategia · ${escape(participants(config)[strategyBot===4?0:strategyBot+1]?.name??'Bot')}</h2><p>Le caselle definiscono le priorità di questo bot per la prossima partita. La partita salvata mantiene le strategie scelte al suo avvio.</p><label class="grace-field">Personalità <select id="bot-personality">${['equilibrato','stratega','incursore','conquistatore','equilibrato'].map(personality=>`<option value="${personality}" ${policy.personality===personality?'selected':''}>${personality}</option>`).join('')}</select></label><div class="policy-list">${policyOptions.map(([key,label,hint])=>`<label><input type="checkbox" data-policy="${key}" ${policy[key]?'checked':''}/><span><strong>${label}</strong><small>${hint}</small></span></label>`).join('')}</div><label class="grace-field">Tregua iniziale <select id="grace-rounds">${[0,1,2,3].map(n=>`<option value="${n}" ${policy.graceRounds===n?'selected':''}>${n===0?'Nessuna':n+' giri'}</option>`).join('')}</select></label><p class="policy-note">Durante la tregua il bot difende e rinforza, ma non attacca.</p><div class="policy-actions"><button class="secondary" data-action="policy-all">Applica a tutti</button><button class="text-button" data-action="policy-reset">Ripristina consigliati</button></div>`;
    }
    if(modal==='audio'){
      cls='audio-window';content=`<h2>Audio</h2><label class="audio-switch"><input type="checkbox" id="audio-enabled" ${muted?'':'checked'}/> Effetti sonori</label><label class="audio-switch"><input type="checkbox" id="haptics-enabled" ${sounds.haptics?'checked':''}/> Vibrazione Android</label><label for="audio-volume">Volume</label><input id="audio-volume" type="range" min="0" max="100" value="${Math.round(sounds.volume*100)}"/><button class="primary full" data-action="audio-test">Prova il clash</button><p>Il volume multimediale del telefono deve essere attivo.</p>`;
    }
    if (modal === 'replace') {
      content = '<span class="eyebrow">NUOVA PARTITA</span><h2>Aprire un nuovo tavolo?</h2><p>La partita in corso verrà sostituita. Le statistiche delle partite concluse rimangono salvate.</p><button class="primary full" data-action="confirm-new">Crea il nuovo tavolo</button>';
    }
    if (modal === 'rules') {
      content = '<span class="eyebrow">COME SI GIOCA</span><h2>La strategia, in breve.</h2><div class="rules-copy"><p>Rinforza, attacca, effettua uno spostamento tra territori adiacenti e pesca una carta se hai conquistato almeno un territorio.</p><p>Si attacca e si difende con il massimo dei dadi, fino a tre. Non puoi attaccare con meno dadi del difensore. I pareggi favoriscono la difesa.</p><p>I tris valgono 8, 10 o 12 armate, più 2 per ogni carta territorio posseduta. Puoi conservare fino a 7 carte e schierare fino a 130 carri.</p><p>Conquista il tuo obiettivo per vincere subito. Dal termine del giro 20, la sdadata può chiudere la partita: soglia 4, poi 5, 6 e 7. Con tre conquiste o più non si lancia.</p><p>Gli avversari non possono essere eliminati nei primi quattro giri. Una guarnigione di almeno due carri deve rimanere sui confini nemici quando sposti volontariamente.</p><p>Alla scadenza del turno vengono completati i piazzamenti e i movimenti obbligatori; poi il turno termina. L’app in background mette in pausa la partita.</p><p><b>Modalità Antartide (facoltativa):</b> 44 territori e 46 carte. Antartide Occidentale confina con Argentina e Antartide Orientale; Antartide Orientale confina anche con Africa del Sud e Australia Orientale. Il controllo di entrambe dà +2 armate a turno. I due territori valgono 3 punti ciascuno fuori obiettivo; gli obiettivi Torneo restano da 86 punti. Dotazioni iniziali e mani da tre restano invariate.</p><p><b>Countdown:</b> scegli 30, 45 o 60 minuti (45 predefiniti). Il tempo inizia dopo il piazzamento iniziale; pausa e animazioni non consumano tempo. Alla scadenza vince la classifica a punti, senza Sdadata. RisiKo! resta una vittoria immediata.</p><p><b>Annulla piazzamento:</b> durante i rinforzi puoi annullare i tuoi ultimi piazzamenti fino a premere Inizia attacco. Le decorazioni dei badge a 5, 10 e 12 carri sono solo grafiche: resta il limite di 130 carri per giocatore.</p><p class="muted">Riferimento: il file “RisiKo! Online – Come si gioca” fornito dall’utente, integrato con le regole concordate. Ricostruzione indipendente offline, non affiliata a Spin Master.</p></div>';
    }
    if (modal === 'objective') {
      const o = objectives[s!.players[s!.viewer].objective! - 1];
      content = `<span class="eyebrow">RISERVATO AL COMANDANTE</span><h2>IL TUO OBIETTIVO</h2>${miniObjective(o.id)}<p>Punti attuali: ${points()} / 86</p><div class="continent-bonuses">${mapFor(s!).continents.map(c=>`<span>${c.name} +${c.bonus}</span>`).join('')}</div><div class="objective-score"><b>${points()}</b><span>/ 86 punti</span></div><p>Occupa tutti i territori evidenziati per fare RisiKo!</p>${s?.antarctica?'<p class="card-hint">Antartide: +2 rinforzi con entrambi i territori. Valgono 3 punti ciascuno fuori obiettivo; gli obiettivi restano da 86 punti.</p>':''}<button class="primary full" data-action="objective-map">Mostra Punti su Mappa ◎</button><details><summary>Territori dell’obiettivo</summary><p>${o.territories.map(t => `${escape(territories[t].name)} (${territories[t].points})`).join(' · ')}</p></details>`;
    }
    if(modal==='cards'&&s){content=cardsView(s,humanTurn(),cardSelection,pendingCommand);cls='cards-window';}
    if(modal==='combat'&&s&&selected!==null&&target!==null){content=combatDock(s,selected,target,pendingCommand,blitzStop,blitz);cls='combat';}
    if(modal==='move'&&s&&selected!==null&&target!==null){const dock=moveDock(s,selected,target,moveCount);content=dock.content;moveCount=dock.count;cls='move-panel';}
    if(screen!=='table')return `<div class="modal-backdrop menu-sheet"><div class="classic-home portal-route">${header()}<section class="modal ${cls} portal-column" role="dialog" aria-modal="true"><button class="portal-back" data-action="close" aria-label="Chiudi">‹ Indietro</button>${content}</section></div></div>`;
    const floating=modal==='combat'||modal==='move';
    return `<div class="${floating?'floating-wrap':consulting()?'modal-backdrop consultation':'modal-backdrop'}"><section class="modal ${cls} ${floating?'floating-panel':''}" ${floating?'data-floating':''} role="dialog" aria-modal="${!floating}">${floating?`<header data-drag-handle>${modal==='combat'?'Attacco':'Spostamento'} · trascina per spostare</header>`:''}${modal==='move'&&s?.phase === 'POST_CONQUEST_MOVE' ? '' : '<button class="close-button" data-action="close" aria-label="Chiudi">×</button>'}${content}</section></div>`;
  }
  function refreshDialogs() {
    const host=root.querySelector<HTMLElement>('#dialogs');
    if(!host)return;
    const markup=presenting&&!consulting()?'':modalMarkup();
    if(markup===dialogMarkup)return;
    if(dialogExit){clearTimeout(dialogExit);dialogExit=null;}
    if(!markup && host.firstElementChild?.classList.contains('consultation') && !state?.paused && screen==='table'){
      dialogMarkup='';host.firstElementChild.classList.add('dialog-leaving');
      dialogExit=setTimeout(()=>{if(!dialogMarkup)host.innerHTML='';dialogExit=null;},160);
      return;
    }
    const hand=host.querySelector<HTMLElement>('.hand');
    const scroll=hand?.scrollTop??0;
    const active=document.activeElement as HTMLElement | null;
    const focus=active&&host.contains(active)?active:null;
    const card=focus?.dataset.card, action=focus?.dataset.action;
    host.innerHTML=markup;dialogMarkup=markup;decorateIcons(host);
    const newHand=host.querySelector<HTMLElement>('.hand');if(newHand)newHand.scrollTop=scroll;
    if(card!==undefined)host.querySelector<HTMLElement>(`[data-card="${card}"]`)?.focus({preventScroll:true});
    else if(action)host.querySelector<HTMLElement>(`[data-action="${action}"]`)?.focus({preventScroll:true});
  }
  function previewMove() {
    if(!state || presenting || modal!=='move' || selected===null || target===null)return;
    for(const [id,delta] of [[selected,-moveCount],[target,moveCount]]) {
      const token=root.querySelector<SVGElement>(`[data-token="${id}"]`);
      const number=token?.querySelector<SVGTextElement>('.tank-number');
      if(number){number.textContent=String(state.board[id].armies+delta);number.style.fontSize=`${counterFontSize(state.board[id].armies+delta)}px`;}
      token?.classList.toggle('move-preview',delta!==0);
    }
  }
  function render(force=false) {
    if(privacy&&screen==='table'){
      root.innerHTML=`<main class="privacy-screen" role="dialog" aria-modal="true" aria-labelledby="privacy-title"><i class="privacy-color" style="background:${privacy.color}"></i><h1 id="privacy-title">Turno di ${escape(privacy.name)} — Passa il dispositivo</h1><button class="primary" data-action="reveal">Rivela il mio turno</button></main>`;
      root.querySelector<HTMLButtonElement>('button')?.focus();return;
    }

    const homeScroll=screen==='setup'?root.querySelector<HTMLElement>('.classic-settings')?.scrollTop??0:0;
    if(presenting&&!force){refreshDialogs();return;}
    const key=`${modal}:${selected}:${target}`;
    const oldPanel=root.querySelector<HTMLElement>('.floating-panel');
    if(oldPanel&&panelKey===key){const r=oldPanel.getBoundingClientRect();panelPosition={x:r.left,y:r.top};}
    else if(panelKey!==key)panelPosition=null;
    panelKey=key;
    const routes: Record<string, string> = {
      home: '/app/gui/user/home',
      setup: '/app/gui/user/prepare',
      table: '/app/gui/game/table',
      results: '/app/gui/game/results',
      profile: '/app/gui/profile/local'
    };
    history.replaceState(null, '', '#' + routes[screen]);
    setGameOrientation(screen==='table'?'landscape':'portrait');
    const sceneMarkup=screen === 'home' ? home() : screen === 'setup' ? setup() : screen === 'profile' ? profilePage() : screen === 'results' ? results() : state ? table() : home();
    const scene=root.querySelector<HTMLElement>('#scene');
    const oldMap=screen==='table'?scene?.querySelector<SVGSVGElement>('#world-map'):null;
    if(scene){scene.innerHTML=sceneMarkup;if(oldMap){const replacement=scene.querySelector<SVGSVGElement>('#world-map');if(replacement){patchElement(oldMap,replacement);oldMap.setAttribute('viewBox',`${viewport.x} ${viewport.y} ${viewport.w} ${viewport.h}`);delete oldMap.dataset.hitScale;replacement.replaceWith(oldMap);}}refreshDialogs();}
    else {
      dialogMarkup=presenting&&!consulting()?'':modalMarkup();
      root.innerHTML = `<div class="app-shell"><div id="scene">${sceneMarkup}</div><div id="dialogs">${dialogMarkup}</div><div id="effects" class="effects-layer"></div><div id="toast" role="status" class="toast"></div><div class="portrait-notice"><span>↻</span><h2>Ruota il dispositivo</h2><p>La tua strategia merita tutta la plancia.<br>Gioca in orizzontale.</p></div></div>`;
    }
    root.querySelector<HTMLElement>('.app-shell')?.classList.toggle('on-board',screen==='table');
    root.querySelector<HTMLElement>('.app-shell')?.classList.toggle('menu-sheet-open',!!modal&&screen!=='table');
    const homePanel=screen==='setup'?root.querySelector<HTMLElement>('.classic-settings'):null;
    if(homePanel)homePanel.scrollTop=homeScroll;
    decorateIcons(root);
    bindMap();
    fitMapTargets();
    previewMove();
    const panel=root.querySelector<HTMLElement>('[data-floating]');
    if(panel){
      positionPanel(root,panel,[selected,target].filter((n): n is number=>n!==null),panelPosition??undefined);
      bindPanelDrag(panel);
    }
  }
  function start() {
    cancelPresentation();
    privacy=null;config.participants=participants(config);config.name=config.participants.find(p=>p.human)?.name??config.name;
    if(config.seedLabel&&!config.seedLabel.startsWith('RKO2:'))config.seed=seedNumber(config.seedLabel);
    modal = null;
    selected = null;
    target = null;
    objectiveView = false;
    viewport = {
      x: 0,
      y: 0,
      w: 1240,
      h: mapFor(config).height
    };
    speed=config.botSpeed??1;
    screen = 'table';
    send({
      type: 'NEW',
      config
    });
  }
  function inspectTerritory(id:number){if(!state||privacy)return;inspected=id;modal='inspect';render();}
  function onTerritory(id: number) {
    if (!state || presenting || !humanTurn() || pendingCommand || modal) return;
    const cell = state.board[id];
    if (['SETUP_PLACEMENT', 'REINFORCEMENT'].includes(state.phase)) {
      if (cell.owner === state.viewer) command({
        type: 'PLACE',
        territory: id,
        count: state.phase==='SETUP_PLACEMENT'?1:Math.min(placementCount,state.reinforcements)
      });else toast('Puoi rinforzare solo i tuoi territori.');
      return;
    }
    if (state.phase === 'TRIS_EXCHANGE') {
      toast('Conferma prima il cambio tris con “Piazza rinforzi”.');
      return;
    }
    if (state.phase === 'ATTACK') {
      if (cell.owner === state.viewer) {
        selected = id;
        target = null;
        render();
      } else if (selected !== null && canAttack(state!, selected, id)) {
        target = id;
        modal = 'combat';
        blitz = false;
        render();
      } else toast('Attacco non consentito: scegli un’origine valida e confinante.');
    } else if (state.phase === 'FORTIFY') {
      if (cell.owner !== state.viewer) {
        toast('Lo spostamento richiede due tuoi territori adiacenti.');
        return;
      }
      if (selected !== null && selected !== id && moveBounds(state, selected, id).max >= 1) {
        target = id;
        modal = 'move';
        moveCount = 1;
        render();
      } else {
        selected = id;
        render();
      }
    }
  }
  function setZoom(factor: number, deferred=false) {
    const nw = Math.max(380, Math.min(1240, viewport.w * factor)),
      nh = nw * mapFor(state??{}).height / 1240;
    viewport.x += (viewport.w - nw) / 2;
    viewport.y += (viewport.h - nh) / 2;
    viewport.w = nw;
    viewport.h = nh;
    if(deferred)scheduleMapUpdate();else updateMap();
  }
  let mapFrame:number|null=null;
  function scheduleMapUpdate(){
    if(!globalThis.requestAnimationFrame){updateMap();return;}
    if(mapFrame===null)mapFrame=requestAnimationFrame(()=>{mapFrame=null;updateMap();});
  }
  function updateMap() {
    viewport.x = Math.max(-100, Math.min(1240 - viewport.w + 100, viewport.x));
    viewport.y = Math.max(-70, Math.min(mapFor(state??{}).height - viewport.h + 70, viewport.y));
    root.querySelector('#world-map')?.setAttribute('viewBox', `${viewport.x} ${viewport.y} ${viewport.w} ${viewport.h}`);
    fitMapTargets();
  }
  function fitMapTargets(){
    const svg=root.querySelector<SVGSVGElement>('.game-table #world-map'),matrix=svg?.getScreenCTM?.();
    if(!svg||!matrix)return;
    const scale=Math.max(.01,Math.hypot(matrix.a,matrix.b));
    const key=scale.toFixed(5);if(svg.dataset.hitScale===key)return;svg.dataset.hitScale=key;
    svg.querySelectorAll<SVGCircleElement>('.territory-hit,.placement-plus-hit').forEach(el=>el.setAttribute('r',String(22/scale)));
    const occupied=Array.from(svg.querySelectorAll<SVGGraphicsElement>('.army-map-icon')).map(el=>{const r=el.getBoundingClientRect();return {x:r.left,y:r.top,w:r.width,h:r.height};});
    svg.querySelectorAll<SVGTextElement>('.territory-name').forEach(el=>{const id=Number(el.closest('[data-token]')?.getAttribute('data-token'));el.textContent=viewport.w<750?territories[id].name:territories[id].short;el.style.fontSize=`${12/scale}px`;el.style.visibility='visible';const b=el.getBoundingClientRect();if(occupied.some(r=>b.left<r.x+r.w+4&&b.right>r.x-4&&b.top<r.y+r.h+2&&b.bottom>r.y-2))el.style.visibility='hidden';else occupied.push({x:b.left,y:b.top,w:b.width,h:b.height});});
    // Only invisible touch targets remain 44 CSS pixels. Artwork and names
    // stay in map coordinates, so zoom scales them with their territories.
  }
  function bindMap() {
    const svg = root.querySelector<SVGSVGElement>('.game-table #world-map');
    if (!svg || svg.dataset.bound==='true') return;svg.dataset.bound='true';
    const pointers = new Map<number, {
      x: number;
      y: number;
    }>();
    let moved = false,
      startId: number | null = null,
      lastDistance = 0,
      travel = 0;
    let press:ReturnType<typeof setTimeout>|null=null;
    const clearPress=()=>{if(press)clearTimeout(press);press=null;};
    svg.addEventListener('pointerdown', e => {
      pointers.set(e.pointerId, {
        x: e.clientX,
        y: e.clientY
      });
      startId = resolveMapPointers(svg,[{x:e.clientX,y:e.clientY}])[0];
      moved = false;
      travel = 0;
      clearPress();if(startId!==null&&pointers.size===1){const id=startId;press=setTimeout(()=>{if(!svg.isConnected||pointers.size!==1||moved)return;moved=true;inspectTerritory(id);},450);}
      svg.setPointerCapture(e.pointerId);
      if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        lastDistance = Math.hypot(a.x - b.x, a.y - b.y);
      }
    });
    svg.addEventListener('pointermove', e => {
      const old = pointers.get(e.pointerId);
      if (!old) return;
      const dx = e.clientX - old.x,
        dy = e.clientY - old.y;
      pointers.set(e.pointerId, {
        x: e.clientX,
        y: e.clientY
      });
      travel += Math.abs(dx) + Math.abs(dy);
      if (travel > 6){moved = true;clearPress();}
      if (pointers.size === 2) {
        const [a, b] = [...pointers.values()],
          dist = Math.hypot(a.x - b.x, a.y - b.y);
        if (lastDistance) setZoom(lastDistance / dist,true);
        lastDistance = dist;
        moved = true;
      } else if (moved) {
        // SVG preserveAspectRatio can add wide letterboxes; element width is
        // not the drawing scale. Convert the actual screen transform instead.
        const matrix=svg.getScreenCTM();
        if(matrix){const determinant=matrix.a*matrix.d-matrix.b*matrix.c;
          if(Math.abs(determinant)>1e-8){viewport.x-=(matrix.d*dx-matrix.c*dy)/determinant;viewport.y-=(-matrix.b*dx+matrix.a*dy)/determinant;}
        }
        scheduleMapUpdate();
      }
    });
    svg.addEventListener('pointerup', e => {
      clearPress();const clicked = !moved && pointers.size === 1;
      pointers.delete(e.pointerId);
      if (clicked && startId !== null) onTerritory(startId);
    });
    svg.addEventListener('pointercancel', e => {clearPress();pointers.delete(e.pointerId);});
    svg.addEventListener('wheel', e => {
      e.preventDefault();
      setZoom(e.deltaY > 0 ? 1.12 : .88);
    }, {
      passive: false
    });
  }
  function click(e: MouseEvent) {
    const el = (e.target as Element).closest<HTMLElement>('button');
    const editorTarget=(e.target as Element).closest('[data-editor-territory]');if(editingScenario&&editorTarget&&editorTarget.getAttribute?.('data-editor-territory')!=null){editorTerritory=Number(editorTarget.getAttribute('data-editor-territory'));render();return;}
    if (!el || el.hasAttribute('disabled')) return;
    if(privacy&&screen==='table'){if(el.dataset.action==='reveal')send({type:'REVEAL',player:privacy.player,token:privacy.token});return;}
    void sounds.unlock();
    if(presenting && el.dataset.card===undefined && el.dataset.speed===undefined && !['game-menu','cards','objective','objective-map','objective-view','objective-widget','log','sound','close','pause','home','blitz','zoom-in','zoom-out','reset-map'].includes(el.dataset.action??''))return;
    if(el.dataset.blitzStep){blitzStop=Math.max(2,Math.min(130,blitzStop+Number(el.dataset.blitzStep)));render();return;}
    if(el.dataset.placeCount){placementCount=Number(el.dataset.placeCount);render();return;}
    if (el.dataset.count) {
      config.participants=participants(config);config.players = Number(el.dataset.count);config.participants=participants(config);
      config.difficulties = Array.from({
        length: config.players - 1
      }, (_, i) => config.difficulties[i] ?? (config.difficulties.every(d=>d==='survival')?'survival':'medio'));
      render();
      return;
    }
    if (el.dataset.speed !== undefined) {
      speed = Number(el.dataset.speed);
      send({
        type: 'SPEED',
        speed
      });
      render();
      return;
    }
    if (el.dataset.tris !== undefined) {
      const combo = validTris(state!, state!.viewer, state!.players[state!.viewer].cards ?? [])[Number(el.dataset.tris)];
      command({
        type: 'EXCHANGE',
        cards: combo.cards
      });
      return;
    }
    if (el.dataset.card !== undefined) {
      const id = Number(el.dataset.card);
      cardSelection = cardSelection.includes(id) ? cardSelection.filter(x => x !== id) : [...cardSelection.slice(-2), id];
      render();
      return;
    }
    if (el.dataset.move) {
      const bounds = moveBounds(state!, selected!, target!);
      moveCount = el.dataset.move === 'Min' ? bounds.min : el.dataset.move === 'Max' ? bounds.max : moveCount + (el.dataset.move === '−1' ? -1 : el.dataset.move === '+5' ? 5 : 1);
      render();
      return;
    }
    if(el.dataset.strategy!==undefined){strategyBot=Number(el.dataset.strategy);modal='strategy';render();return;}
    if(el.dataset.slot){screen='table';modal=null;send({type:'SLOT_SELECT',id:el.dataset.slot});return;}
    if(el.dataset.slotEdit){const action=el.dataset.slotEdit;if(action==='delete'&&!confirm('Eliminare questa partita?'))return;const name=action==='rename'?prompt('Nome della partita'):'';if(action==='rename'&&name===null)return;send({type:'SLOT_EDIT',id:el.dataset.slotId,action,name});return;}
    if(el.dataset.globalLevel){config.difficulties.fill(el.dataset.globalLevel as never);config.participants=participants(config).map(p=>p.human?p:{...p,difficulty:el.dataset.globalLevel as never});render();return;}
    if(el.dataset.turnSeconds!==undefined){config.turnSeconds=Number(el.dataset.turnSeconds);config.custom=config.turnSeconds===0;render();return;}
    if(el.dataset.endMode){config.endMode=el.dataset.endMode as 'regular'|'countdown';render();return;}
    if(el.dataset.playerInfo!==undefined){inspectedPlayer=Number(el.dataset.playerInfo);modal='player-info';render();return;}
    if(el.dataset.profile){selectedProfile=el.dataset.profile;send({type:'PROFILE',profileId:selectedProfile});screen='profile';modal=null;render();return;}
    if(el.dataset.scenario||el.dataset.scenarioEdit){const found=[...curatedScenarios({...config,participants:participants(config)}),...scenarios].find(sc=>sc.id===(el.dataset.scenario??el.dataset.scenarioEdit));if(!found)return;editingScenario=structuredClone(found);if(el.dataset.scenarioEdit){editingScenario.id=crypto.randomUUID();editorTerritory=0;modal='editor';render();}else{screen='table';modal=null;send({type:'NEW',config:editingScenario.config,scenario:editingScenario});}return;}
    switch (el.dataset.action) {
      case 'summary-continue':turnSummary=null;modal=null;send({type:'SUMMARY_ACK'});render();break;
      case 'tutorial':tutorialDone=false;screen='table';modal=null;send({type:'TUTORIAL'});break;
      case 'tutorial-next':tutorialDone=false;send({type:'TUTORIAL_NEXT'});break;
      case 'slots':resumeRequested=true;send({type:'SLOTS'});modal='slots';render();break;
      case 'profiles':send({type:'PROFILES'});modal='profiles';render();break;
      case 'profile-add':{const name=root.querySelector<HTMLInputElement>('#new-profile')?.value;if(name?.trim())send({type:'PROFILE_ADD',name});break;}
      case 'scenarios':send({type:'SCENARIOS'});modal='scenarios';render();break;
      case 'editor-new':editingScenario=scenarioFrom({...config,participants:participants(config)});editorTerritory=0;modal='editor';render();break;
      case 'editor-save':case 'editor-play':case 'editor-export':if(editingScenario){try{scenarioGame(editingScenario);if(el.dataset.action==='editor-save'){send({type:'SCENARIO_SAVE',scenario:editingScenario});toast('Scenario salvato');}else if(el.dataset.action==='editor-play'){modal=null;screen='table';send({type:'NEW',config:editingScenario.config,scenario:editingScenario});}else{const url=URL.createObjectURL(new Blob([JSON.stringify(editingScenario)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download='risiko-scenario.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}}catch(error){toast(String(error));}}break;
      case 'game-menu':modal='game-menu';render();break;
      case 'map-layer':mapLayer=mapLayer==='standard'?'control':mapLayer==='control'?'objective':'standard';objectiveView=mapLayer==='objective';render();toast(mapLayer==='standard'?'Vista cartografica':mapLayer==='control'?'Controllo giocatori':'Punti obiettivo');break;
      case 'seed-apply':try{if(readMatchCode(config.seedLabel??'')){modal='seed-confirm';render();}else{config.seed=seedNumber(config.seedLabel??'');toast('Seed impostato');}}catch(error){toast(String(error));}break;
      case 'seed-confirm':try{const parsed=readMatchCode(config.seedLabel??'');if(parsed){config={...parsed,seedLabel:String(parsed.seed)};modal=null;render();}}catch(error){toast(String(error));}break;
      case 'seed-copy':case 'seed-share':{if(!config.seedLabel){config.seedLabel=String(crypto.getRandomValues(new Uint32Array(1))[0]);render();}if(!config.seedLabel.startsWith('RKO2:'))config.seed=seedNumber(config.seedLabel);const text=matchCode({...config,participants:participants(config)});if(el.dataset.action==='seed-share'&&navigator.share)void navigator.share({title:'Codice Partita RisiKo',text}).catch(()=>{});else void navigator.clipboard.writeText(text).then(()=>toast('Codice partita copiato')).catch(()=>toast('Copia il seed dal campo di testo'));break;}

      case 'install': modal='install';render();break;
      case 'pwa-install': void installWebApp().then(()=>toast('Su iPhone aggiungi l’app alla Home dal menu Condividi di Safari.')).catch(()=>toast('Usa il menu del browser per installare.'));break;
      case 'pwa-update': updateWebApp();toast('Controllo aggiornamenti: serve una connessione.');break;
      case 'backup': modal='backup';render();break;
      case 'export-backup': send({type:'EXPORT_BACKUP'});toast('Preparazione del backup…');break;
      case 'share-backup': {
        const file=new File([backupText],backupFilename(),{type:'application/json'});
        if(navigator.canShare?.({files:[file]}))void navigator.share({files:[file],title:'Backup RisiKo'}).catch(()=>{});
        else toast('Usa «Scarica file JSON» e conserva il file scaricato.');break;
      }
      case 'merge-import':case 'confirm-import': if(importPreview){send({type:'IMPORT_BACKUP',text:importText,mode:el.dataset.action==='merge-import'?'merge':'replace'});importPreview=null;importText='';modal='backup';render();}break;
      case 'audio': modal='audio';render();break;
      case 'audio-test': void sounds.unlock().then(()=>{sounds.clash();if(!sounds.ready)toast('Tocca ancora Prova il clash per attivare l’audio.');});break;
      case 'policy-all': config.botPolicies=Array.from({length:5},()=>({...config.botPolicies![strategyBot]}));toast('Strategia applicata a tutti i bot');break;
      case 'policy-reset': config.botPolicies![strategyBot]=botPolicy();render();break;
      case 'exchange-selected':
        if(state&&exchangeValue(state,state.viewer,cardSelection)>0)command({type:'EXCHANGE',cards:[...cardSelection]});
        break;
      case 'undo-place':command({type:'UNDO_PLACE'});break;
      case 'objective-widget':modal='objective';render();break;
      case 'names': showNames=!showNames;render();break;
      case 'configure':
        screen='setup';modal=null;render();break;
      case 'survival':
        config.difficulties.fill('survival');config.participants=participants(config).map(p=>p.human?p:{...p,difficulty:'survival'});screen='setup';modal=null;render();break;
      case 'new':start();break;
      case 'confirm-new':
        start();
        break;
      case 'resume':
        screen = 'table';
        modal = null;
        send({
          type: 'RESUME'
        });
        break;
      case 'home':
        cancelPresentation();
        if (state && state.phase !== 'GAME_OVER') send({
          type: 'PAUSE'
        });
        screen = 'home';
        modal = null;
        send({
          type: 'PROFILE',profileId:selectedProfile
        });
        render();
        break;
      case 'profile':
        cancelPresentation();
        if (state && state.phase !== 'GAME_OVER') send({
          type: 'PAUSE'
        });
        screen = 'profile';
        modal = null;
        send({
          type: 'PROFILE',profileId:selectedProfile
        });
        render();
        break;
      case 'blind-all':
        config.difficulties.fill('casuale');config.participants=participants(config).map(p=>p.human?p:{...p,difficulty:'casuale'});
        render();
        break;
      case 'rules':
      case 'cards':
      case 'objective':
        modal = el.dataset.action;
        render();
        break;
      case 'objective-view':
        objectiveView = !objectiveView;
        render();
        break;
      case 'objective-map':
        objectiveView = true;
        modal = null;
        render();
        break;
      case 'close':
        if(modal==='summary'){turnSummary=null;send({type:'SUMMARY_ACK'});}
        if(modal==='summary'){turnSummary=null;send({type:'SUMMARY_ACK'});}
        else if(turnSummary){modal='summary';render();break;}
        if(consulting() && (state?.phase!=='POST_CONQUEST_MOVE'||!humanTurn())){
          modal=null;blitz=false;
          if(root.querySelector('#dialogs'))refreshDialogs();else render();
          break;
        }
        if (state?.phase !== 'POST_CONQUEST_MOVE' || !humanTurn()) modal = null;
        else if(consulting()) modal = 'move';
        blitz = false;
        render();
        break;
      case 'roll':
        command({
          type: 'ATTACK',
          from: selected!,
          to: target!
        });
        break;
      case 'blitz':
        if(presenting){blitz=false;const stop=root.querySelector<HTMLElement>('.stop-blitz');if(stop)stop.textContent='Blitz fermato';break;}
        blitz = !blitz;
        if(blitz && state!.board[selected!].armies<blitzStop){blitz=false;toast('Soglia di arresto del Blitz raggiunta.');}
        if (blitz) command({
          type: 'ATTACK',
          from: selected!,
          to: target!
        });
        render();
        break;
      case 'confirm-move':
        command({
          type: 'MOVE',
          from: selected!,
          to: target!,
          count: moveCount
        });
        modal = null;
        selected = target;
        target = null;
        break;
      case 'next':
        selected = null;
        target = null;
        command({
          type: 'NEXT'
        });
        break;
      case 'pause':
        blitz = false;
        send({
          type: 'PAUSE'
        });
        break;
      case 'log':
        showLog = !showLog;
        render();
        break;
      case 'sound':
        muted = !muted;sounds.muted=muted;sounds.save();
        render();
        break;
      case 'zoom-in':
        setZoom(.8);
        break;
      case 'zoom-out':
        setZoom(1.25);
        break;
      case 'reset-map':
        viewport = {
          x: 0,
          y: 0,
          w: 1240,
          h: mapFor(state??{}).height
        };
        updateMap();
        break;
    }
  }
  function change(e: Event) {
    if(privacy&&screen==='table')return;
    const el = e.target as HTMLInputElement;
    if(el.id==='profile-category'){profileCategory=el.value;render();return;}
    if(el.id==='replay-turn'){replayIndex=Number(el.value);const panel=root.querySelector('.replay-panel');if(panel&&state)panel.outerHTML=replayView(state,replayIndex);return;}
    if(el.id==='match-seed'){config.seedLabel=el.value;return;}
    if(el.id==='animation-filter'){animationFilter=el.value;send({type:'ANIMATION_FILTER',filter:animationFilter});return;}
    if(el.id==='global-difficulty'){if(el.value){config.difficulties.fill(el.value as never);config.participants=participants(config).map(p=>({...p,difficulty:el.value as never}));render();}return;}
    if(el.dataset.seatName!==undefined){config.participants=participants(config);config.participants[Number(el.dataset.seatName)].name=el.value;return;}
    if(el.dataset.seatKind!==undefined||el.dataset.seatProfile!==undefined||el.dataset.seatLevel!==undefined){config.participants=participants(config);const i=Number(el.dataset.seatKind??el.dataset.seatProfile??el.dataset.seatLevel),p=config.participants[i];if(el.dataset.seatKind!==undefined)p.human=el.value==='human';if(el.dataset.seatProfile!==undefined){p.profileId=el.value;const profile=localProfiles.find(p=>p.id===el.value);if(profile)p.name=profile.name;}if(el.dataset.seatLevel!==undefined){p.difficulty=el.value as never;if(i>0)config.difficulties[i-1]=el.value as never;}render();return;}
    if(editingScenario){const sc=editingScenario;if(el.id==='scenario-name')sc.name=el.value;if(el.id==='scenario-description')sc.description=el.value;if(el.id==='editor-owner')sc.board[editorTerritory].owner=Number(el.value);if(el.id==='editor-armies')sc.board[editorTerritory].armies=Number(el.value);if(el.id==='editor-round')sc.round=Number(el.value);if(el.id==='editor-active')sc.active=Number(el.value);if(el.id==='editor-territory'){editorTerritory=Number(el.value);render();return;}}
    if(el.id==='scenario-file'){const file=el.files?.[0];if(file&&file.size<1024*1024)void file.text().then(text=>{const sc=JSON.parse(text);scenarioGame(sc);sc.id=crypto.randomUUID();send({type:'SCENARIO_SAVE',scenario:sc});}).catch(error=>toast(String(error)));return;}

    if(el.id==='web-rotation'){try{localStorage.setItem('risiko-web-rotation',el.checked?'on':'off');}catch{}sendWebOrientation(screen==='table'?'landscape':'portrait');return;}
    if(el.id==='backup-file'){
      const file=el.files?.[0];if(!file)return;
      if(file.size>MAX_BACKUP_BYTES){toast('File troppo grande: massimo 64 MB.');return;}
      void file.text().then(raw=>{importPreview=parseBackup(raw);importText=raw;modal='import-confirm';render();}).catch(error=>toast(error instanceof Error?error.message:'File non leggibile.'));
      return;
    }
    if(el.id==='end-mode'){config.endMode=el.value as 'regular'|'countdown';render();return;}
    if(el.id==='countdown-minutes')config.countdownMinutes=Number(el.value);
    if(el.id==='bot-speed')config.botSpeed=Number(el.value);
    if(el.id==='bot-personality')config.botPolicies![strategyBot].personality=el.value as never;
    if(el.id==='haptics-enabled'){sounds.haptics=el.checked;sounds.save();}
    if(el.id==='blitz-stop'){blitzStop=Number(el.value);const label=root.querySelector('#blitz-limit');if(label)label.textContent=el.value;}
    if(el.id==='antarctica-mode'){config.antarctica=el.checked;render();return;}
    if(el.dataset.policy){const key=el.dataset.policy as typeof policyOptions[number][0];config.botPolicies![strategyBot][key]=el.checked;}
    if(el.id==='grace-rounds')config.botPolicies![strategyBot].graceRounds=Number(el.value);
    if(el.id==='audio-enabled'){muted=!el.checked;sounds.muted=muted;sounds.save();}
    if(el.id==='audio-volume'){sounds.volume=Number(el.value)/100;sounds.save();}
    if (el.id === 'player-name') config.name = el.value;
    if (el.id === 'turn-time'){config.turnSeconds = Number(el.value);config.custom=config.turnSeconds===0;}
    if (el.dataset.bot !== undefined) {
      config.difficulties[Number(el.dataset.bot)] = el.value as never;
      render();
    }
    if (el.id === 'move-range') {
      moveCount = Number(el.value);
      const label = root.querySelector('#move-number');
      if (label) label.textContent = String(moveCount);
      previewMove();
    }
  }
  function visibility() {
    if (document.hidden) {
      blitz = false;
      send({
        type: 'PAUSE'
      });
    }
  }
  root.addEventListener('click', click);
  root.addEventListener('input', change);
  root.addEventListener('keydown', e => {
    if(privacy&&screen==='table'){if(e.key==='Tab'){e.preventDefault();root.querySelector<HTMLButtonElement>('[data-action="reveal"]')?.focus();}return;}
    if(presenting&&!consulting())return;
    if (e.key === 'Escape' && (state?.phase !== 'POST_CONQUEST_MOVE' || !humanTurn())) {
      modal = null;
      blitz = false;
      render();
    }
    if (e.key === 'Enter') {
      const id = (e.target as Element).closest('[data-territory]')?.getAttribute('data-territory');
      if (id) onTerritory(Number(id));
    }
  });
  document.addEventListener('visibilitychange', visibility);
  const onViewportResize=()=>{
    fitMapTargets();
    root.querySelectorAll<HTMLElement>('[data-floating]').forEach(panel=>positionPanel(root,panel,[selected,target].filter((n):n is number=>n!==null)));
  };
  globalThis.addEventListener?.('resize',onViewportResize);
  const nativePause = () => {
    blitz = false;
    send({
      type: 'PAUSE'
    });
  };
  document.addEventListener('risiko-pause', nativePause);
  worker.onmessage = ({
    data
  }) => {
    if(data.type==='TURN_SUMMARY'){turnSummary=data.lines;if(screen==='table'&&!consulting()&&!state?.paused){modal='summary';render();}return;}
    if(data.type==='TUTORIAL_DONE'){tutorialDone=true;return;}
    if(data.type==='TUTORIAL_FINISHED'){screen='home';state=null;modal=null;send({type:'SLOTS'});render();toast('Tutorial completato');return;}
    if(data.type==='PRIVACY'){cancelPresentation();privacy={player:data.player,name:data.name,color:data.color,token:data.token};state=null;modal=null;selected=null;target=null;cardSelection=[];dialogMarkup='';pendingCommand=false;render();return;}
    if(data.type==='SLOTS'){slots=data.slots;if(resumeRequested&&slots.length===1){resumeRequested=false;screen='table';modal=null;send({type:'SLOT_SELECT',id:slots[0].id});return;}resumeRequested=false;hasGame=slots.some(s=>s.phase!=='GAME_OVER');render();return;}
    if(data.type==='SLOTS_CHANGED'){send({type:'SLOTS'});return;}
    if(data.type==='PROFILES'){localProfiles=data.profiles;render();return;}
    if(data.type==='SCENARIOS'){scenarios=data.scenarios;render();return;}
    if(data.type==='CLOCK'){
      if(state&&state.id===data.id&&state.revision===data.revision&&!presenting){
        state={...state,remainingMs:data.remainingMs,matchRemainingMs:data.matchRemainingMs,elapsedMs:data.elapsedMs};
        const timer=root.querySelector('#timer'),match=root.querySelector('#match-timer');
        if(timer)timer.textContent=state.phase==='SETUP_PLACEMENT'||!state.turnSeconds?'—':duration(state.remainingMs);
        if(match)match.textContent=duration(state.matchRemainingMs??0);
      }
      return;
    }
    if(data.type==='LOCAL_SAVE'){if(!data.state){clearLocalSnapshot();return;}if(!writeLocalSnapshot(data.state)&&!localSaveWarning){localSaveWarning=true;toast('Copia locale non disponibile; il salvataggio principale resta in IndexedDB.');}return;}
    if(data.type==='BACKUP_FILE'){
      if(backupURL)URL.revokeObjectURL(backupURL);backupText=data.text;
      backupURL=URL.createObjectURL(new Blob([backupText],{type:'application/json'}));modal='backup';render();return;
    }
    if(data.type==='BACKUP_RESTORED'){
      cancelPresentation();state=null;profile=data.profile;config.name=profile.name;screen='home';modal=null;
      hasGame=!!data.state&&data.state.phase!=='GAME_OVER';
      if(data.state)receiveState(data.state);else render();toast('Backup ripristinato. La partita è in pausa.');return;
    }
    if (data.type === 'ERROR') {
      pendingCommand = false;
      blitz = false;
      toast(data.message);
      return;
    }
    if (data.type === 'READY') {
      ready = true;
      hasGame = data.hasGame;
      render();
    }
    if (data.type === 'PROFILE') {
      profile = data.profile;
      if (config.name === 'Comandante') config.name = profile.name;
      render();
    }
    if (data.type === 'TRANSITION') {
      const t=data.transition as Transition;
      if(presenting?.id===t.id)return;
      if(lastPresentedId===t.id){send({type:'PRESENTED',id:t.id});return;}
      cancelPresentation();
      if(screen!=='table'||document.hidden){send({type:'PRESENTED',id:t.id});receiveState(t.after);return;}
      state=t.before;presenting=t;pendingCommand=true;
      // The viewport belongs to the player. Effects must not zoom out to
      // reveal off-screen territories or restore an older camera after a gesture.
      // The map already shows transition.before. Keep its SVG mounted while
      // effects run, instead of reparsing every path twice per action.
      if(root.querySelector('#world-map')){updateMap();refreshDialogs();}
      else render(true);

      cancelEffects=playTransition(root,t,()=>{
        if(presenting?.id!==t.id)return;
        lastPresentedId=t.id;lastBattleMessage=t.after.lastBattle?.message??'';cancelPresentation();send({type:'PRESENTED',id:t.id});receiveState(t.after);
      },()=>sounds.clash(),cue=>{if(cue==='deploy')sounds.playDeploy();if(cue==='dice')sounds.playDiceRoll();if(cue==='conquest')sounds.playConquest();if(cue==='draw')sounds.playCardDraw();if(cue==='trade')sounds.playCardTrade();});
      return;
    }
    if (data.type === 'STATE') {
      privacy=null;
      if(presenting)cancelPresentation();
      receiveState(data.state);
    }
  };
  function receiveState(nextState: GameView) {
      const previous = state;
      state = nextState;
      cardSelection=cardSelection.filter(id=>state!.players[state!.viewer].cards?.includes(id));
      if(previous?.id===state.id && previous.active!==previous.viewer && state.active===state.viewer && !state.paused)sounds.playTurnStart();
      if(!previous||previous.id!==state.id)viewport={x:0,y:0,w:1240,h:mapFor(state).height};

      hasGame = state!.phase !== 'GAME_OVER';
      const changed = !previous || previous.revision !== state!.revision || previous.paused !== state!.paused || previous.id !== state!.id;
      if (changed) {
        pendingCommand = false;
        if (previous?.active !== state!.active) {
          selected = null;
          target = null;
          if(!consulting())modal = null;
          blitz = false;
        }
        if(modal==='combat' && target!==null && state!.phase==='ATTACK' && state!.board[target].owner===state!.viewer){
          selected=target;target=null;modal=null;blitz=false;
        }
        if (state!.phase === 'GAME_OVER' && screen === 'table') {
          screen = 'results';
          modal = null;
          send({
            type: 'PROFILE',profileId:selectedProfile
          });
        }
        if (state!.paused) { if(screen==='table')modal=null; blitz=false; }
        if (screen==='table' && !state!.paused && state!.phase === 'POST_CONQUEST_MOVE' && state!.active === state!.viewer) {
          selected = state!.pending!.from;
          target = state!.pending!.to;
          if(previous?.pending?.from!==state!.pending!.from || previous?.pending?.to!==state!.pending!.to || previous?.phase!=='POST_CONQUEST_MOVE' || previous?.paused)moveCount = moveBounds(state!,selected,target).min;
          if(!consulting())modal = 'move';
          blitz = false;
        }
        if (state!.lastBattle?.message !== lastBattleMessage) {
          lastBattleMessage = state!.lastBattle?.message ?? '';

        }
        render();
        if (blitz && modal === 'combat' && selected !== null && target !== null) {
          if (state!.board[selected].armies>=blitzStop && canAttack(state!, selected, target)) {
            const expected=state!.revision;
            blitzTimer=setTimeout(() => {
            if (blitz && !presenting && state?.revision===expected && modal === 'combat') command({
              type: 'ATTACK',
              from: selected!,
              to: target!
            });
          }, 120); } else {
            blitz = false;
            render();
          }
        }
      } else {
        const timer = root.querySelector('#timer');
        const matchTimer=root.querySelector('#match-timer');if(matchTimer)matchTimer.textContent=duration(state!.matchRemainingMs??0);
        if (timer) timer.textContent = state!.phase === 'SETUP_PLACEMENT' ? '—' : duration(state!.remainingMs);
      }
    }
  worker.onerror = e => toast(`Errore del motore: ${e.message}`);
  const disposeWebApp=initWebApp(()=>{if(screen==='home'||modal==='install')render();});
  render();
  send({
    type: 'INIT', presentation: true, snapshot:readLocalSnapshot()
  });
  send({type:'PROFILES'});send({type:'SLOTS'});
  root.addEventListener('toggle',e=>{if((e.target as HTMLElement).id==='seed-details')seedOpen=(e.target as HTMLDetailsElement).open;},true);
  return () => {
    disposeWebApp();if(backupURL)URL.revokeObjectURL(backupURL);
    globalThis.removeEventListener?.('resize',onViewportResize);
    if(mapFrame!==null)cancelAnimationFrame(mapFrame);
    if(dialogExit)clearTimeout(dialogExit);
    cancelPresentation();
    sounds.dispose();
    worker.terminate();
    root.removeEventListener('click', click);
    root.removeEventListener('input', change);
    document.removeEventListener('visibilitychange', visibility);
    document.removeEventListener('risiko-pause', nativePause);
  };
}
