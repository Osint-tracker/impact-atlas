import { test } from 'node:test';
import assert from 'node:assert/strict';
import { writeFileSync, mkdirSync, readFileSync } from 'node:fs';
import {makeTransition} from '../src/engine/presentation.ts';
import { mountGame } from '../src/gui/App.ts';
import { createGame, project, applyCommand, owned } from '../src/engine/RisikoRulesEngine.ts';
class MockWorker {
  static instance: MockWorker;
  onmessage: ((event: {
    data: any;
  }) => void) | null = null;
  onerror: any;
  messages: any[] = [];
  constructor() {
    MockWorker.instance = this;
  }
  postMessage(data: any) {
    this.messages.push(data);
  }
  terminate() {}
  emit(data: any) {
    this.onmessage?.({
      data
    });
  }
}
function fixture(holdEffects=false,prepare=true,trackMap=false) {
  const callbacks: Record<string, Function> = {};
  let html = '';
  const map={
    setAttribute(name:string,value:string){if(name==='viewBox')html=html.replace(/(id="world-map"[^>]*viewBox=")[^"]+/,`$1${value}`);},
    insertAdjacentHTML(){}, getScreenCTM(){return null;}
  };
  const effects={innerHTML:'',className:'',style:{setProperty(){}},classList:{add(){},remove(){}},querySelector(){return null;}};
  const root = {
    set innerHTML(v: string) {
      html = v;
    },
    get innerHTML() {
      return html;
    },
    querySelector(selector:string) {
      if(trackMap&&selector==='#world-map'&&html.includes('id="world-map"'))return map;
      return holdEffects&&selector==='#effects'?effects:null;
    },
    querySelectorAll(){return [];},
    addEventListener(type: string, f: Function) {
      callbacks[type] = f;
    },
    removeEventListener() {}
  };
  Object.assign(globalThis, {
    Worker: MockWorker,
    document: {
      hidden: false,
      addEventListener() {},
      removeEventListener() {}
    },
    history: {
      replaceState() {}
    }
  });
  const cleanup = mountGame(root as never),
    worker = MockWorker.instance;
  const click = (dataset: Record<string, string>) => callbacks.click({
    target: {
      closest: () => ({
        dataset,
        hasAttribute: () => false
      })
    }
  });
  worker.emit({
    type: 'PROFILE',
    profile: {
      name: 'Comandante',
      results: []
    }
  });
  worker.emit({
    type: 'READY',
    hasGame: false
  });
  if(prepare)click({action:'configure'});
  return {
    root,
    worker,
    click,
    cleanup,
    callbacks
  };
}
test('lobby renders all bot slots, defaults and blind selectors without real levels', () => {
  const f = fixture();
  assert.match(f.root.innerHTML, /Regular/);
  assert.equal((f.root.innerHTML.match(/data-bot=/g) || []).length, 3);
  f.click({
    action: 'blind-all'
  });
  assert.equal((f.root.innerHTML.match(/Bot \? · livello segreto/g) || []).length, 3);
  f.click({
    count: '5'
  });
  assert.equal((f.root.innerHTML.match(/data-bot=/g) || []).length, 4);
  f.cleanup();
});
test('table renders 42 territories, hidden objectives, timer and battle controls', () => {
  const f = fixture();
  f.click({
    action: 'new'
  });
  const s = createGame({
    players: 4,
    name: '<script>alert(1)</script>',
    difficulties: ['casuale', 'casuale', 'casuale'],
    turnSeconds: 180,
    seed: 5
  });
  s.phase = 'ATTACK';
  s.round = 5;
  s.active = 0;
  f.worker.emit({
    type: 'STATE',
    state: project(s)
  });
  assert.equal((f.root.innerHTML.match(/data-territory=/g) || []).length, 42);
  assert.match(f.root.innerHTML, /&lt;script&gt;/);
  assert.doesNotMatch(f.root.innerHTML, /<script>alert/);
  assert.equal((f.root.innerHTML.match(/Bot \?/g) || []).length, 3);
  assert.match(f.root.innerHTML, /3:00/);
  f.click({
    action: 'objective'
  });
  assert.match(f.root.innerHTML, /RISERVATO AL COMANDANTE/);
  assert.match(f.root.innerHTML, /objective-mini/);
  f.click({
    action: 'close'
  });
  f.click({
    action: 'cards'
  });
  assert.match(f.root.innerHTML, /Carte territorio/);
  f.click({
    action: 'close'
  });
  const svg = f.root.innerHTML.match(/<svg id="world-map"[\s\S]*?<\/svg>/)![0];
  const css = readFileSync('src/gui/styles.css', 'utf8');
  mkdirSync('docs', {
    recursive: true
  });
  writeFileSync('docs/board-preview.svg', svg.replace('class="world-map ', 'class="world-map reference-table ').replace('<svg ', '<svg xmlns="http://www.w3.org/2000/svg" ').replace('<defs>', `<style>${css}</style><defs>`));
  f.cleanup();
});
test('new game creates another slot without replacing the existing save', () => {
  const f = fixture();
  f.worker.emit({
    type: 'READY',
    hasGame: true
  });
  f.click({
    action: 'new'
  });
  assert.doesNotMatch(f.root.innerHTML, /verrà sostituita/);
  assert.ok(!f.worker.messages.some(x=>x.type==='SLOT_EDIT'&&x.action==='delete'));
  assert.ok(f.worker.messages.some(x => x.type === 'NEW'));
  f.cleanup();
});
test('commands carry actor-independent revision and unique request id', () => {
  const f = fixture();
  f.click({
    action: 'new'
  });
  const s = createGame({
    players: 3,
    name: 'Test',
    difficulties: ['medio', 'medio'],
    turnSeconds: 180,
    seed: 3
  });
  s.phase = 'ATTACK';
  s.active = 0;
  s.round = 5;
  f.worker.emit({
    type: 'STATE',
    state: project(s)
  });
  f.click({
    action: 'next'
  });
  const command = f.worker.messages.find(m => m.type === 'COMMAND');
  assert.equal(command.revision, s.revision);
  assert.equal(command.command.type, 'NEXT');
  assert.match(command.id, /^[0-9a-f-]+$/);
  f.cleanup();
});
test('results reveal blind difficulty and all objectives only after conclusion', () => {
  const f = fixture();
  f.click({
    action: 'new'
  });
  const s = createGame({
    players: 3,
    name: 'Test',
    difficulties: ['casuale', 'casuale'],
    turnSeconds: 180,
    seed: 7
  });
  s.phase = 'GAME_OVER';
  s.winner = 1;
  s.reason = 'sdadata';
  f.worker.emit({
    type: 'STATE',
    state: project(s)
  });
  assert.match(f.root.innerHTML, /livello svelato/);
  assert.match(f.root.innerHTML, /Tutti gli obiettivi sono ora rivelati/);
  assert.doesNotMatch(f.root.innerHTML, /Bot \?/);
  f.cleanup();
});
test('background pause during compulsory conquest never traps the resume controls',()=>{
 const f=fixture();f.click({action:'new'});const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});s.phase='POST_CONQUEST_MOVE';s.round=5;s.active=0;s.pending={from:0,to:1,min:3};s.board[0]={owner:0,armies:5};s.board[1]={owner:0,armies:0};f.worker.emit({type:'STATE',state:project(s)});assert.match(f.root.innerHTML,/Conferma spostamento/);s.paused=true;f.worker.emit({type:'STATE',state:project(s)});assert.match(f.root.innerHTML,/pause-card/);assert.doesNotMatch(f.root.innerHTML,/role="dialog"/);s.paused=false;f.worker.emit({type:'STATE',state:project(s)});assert.match(f.root.innerHTML,/Conferma spostamento/);f.cleanup();
});

test('setup UI distingue schierati, riserva e mano, evidenzia solo i propri territori',()=>{
 const f=fixture();f.click({action:'new'});const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});s.active=0;
 f.worker.emit({type:'STATE',state:project(s)});
 assert.match(f.root.innerHTML,/Schierati 14\/35/);assert.match(f.root.innerHTML,/Riserva 21/);assert.match(f.root.innerHTML,/Mano: 3/);
 assert.equal((f.root.innerHTML.match(/class="placement-plus"/g)??[]).length,14);
 assert.equal((f.root.innerHTML.match(/data-token="\d+"/g)??[]).length,42);
 assert.doesNotMatch(f.root.innerHTML,/class="point-badge"/);f.cleanup();
});
test('carte militari selezionabili: invia solo il tris scelto e mostra il bonus',()=>{
 const f=fixture();f.click({action:'new'});const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});s.active=0;s.phase='TRIS_EXCHANGE';s.round=5;s.players[0].cards=[0,3,6];
 f.worker.emit({type:'STATE',state:project(s)});f.click({action:'cards'});
 assert.match(f.root.innerHTML,/LE TUE CARTE/);assert.doesNotMatch(f.root.innerHTML,/[♜♟♞]/);
 for(const id of [0,3,6])f.click({card:String(id)});
 assert.equal((f.root.innerHTML.match(/aria-pressed="true"/g)??[]).length,3);
 f.click({action:'exchange-selected'});assert.deepEqual(f.worker.messages.at(-1).command,{type:'EXCHANGE',cards:[0,3,6]});f.cleanup();
});
test('spostamento obbligatorio usa finestra flottante senza oscurare la plancia',()=>{
 const f=fixture();f.click({action:'new'});const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});s.active=0;s.phase='POST_CONQUEST_MOVE';s.round=5;s.pending={from:0,to:1,min:3};s.board[0]={owner:0,armies:8};s.board[1]={owner:0,armies:0};
 f.worker.emit({type:'STATE',state:project(s)});assert.match(f.root.innerHTML,/floating-panel/);assert.match(f.root.innerHTML,/aria-modal="false"/);assert.doesNotMatch(f.root.innerHTML,/class="modal-backdrop"/);f.cleanup();
});

test('carte e selezione restano aperte tra attacchi e cambi di turno dei bot',()=>{
 const f=fixture(true);f.click({action:'new'});
 const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});
 s.phase='ATTACK';s.active=1;s.players[0].cards=[0,3,6];
 f.worker.emit({type:'STATE',state:project(s)});f.click({action:'cards'});f.click({card:'3'});
 const before=project(s),after=project({...s,revision:s.revision+1});
 f.worker.emit({type:'TRANSITION',transition:{id:'cards:1',before,after,effects:[{kind:'draw',player:1}],speed:1}});
 assert.match(f.root.innerHTML,/LE TUE CARTE/);assert.match(f.root.innerHTML,/data-card="3" aria-pressed="true"/);
 s.active=2;s.revision++;
 f.worker.emit({type:'STATE',state:project(s)});
 assert.match(f.root.innerHTML,/LE TUE CARTE/);assert.match(f.root.innerHTML,/data-card="3" aria-pressed="true"/);
 f.click({action:'close'});assert.doesNotMatch(f.root.innerHTML,/LE TUE CARTE/);
 f.click({action:'objective'});s.active=0;s.revision++;f.worker.emit({type:'STATE',state:project(s)});
 assert.match(f.root.innerHTML,/IL TUO OBIETTIVO/);f.cleanup();
});
test('finestra conquista mostra minimo trasferito e selettore aggiuntivi a zero',()=>{
 const f=fixture();f.click({action:'new'});
 const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});
 s.active=0;s.phase='POST_CONQUEST_MOVE';s.pending={from:0,to:1,min:3,transferred:3};
 s.board[0]={owner:0,armies:9};s.board[1]={owner:0,armies:3};
 f.worker.emit({type:'STATE',state:project(s)});
 assert.match(f.root.innerHTML,/3 carri già trasferiti/);assert.match(f.root.innerHTML,/Carri aggiuntivi/);
 assert.match(f.root.innerHTML,/id="move-range" min="0" max="\d+" value="0"/);
 f.click({action:'confirm-move'});assert.equal(f.worker.messages.at(-1).command.count,0);f.cleanup();
});

test('strategie per bot: caselle, tregua, applica a tutti e configurazione della nuova partita',()=>{
 const f=fixture();f.click({strategy:'0'});
 assert.match(f.root.innerHTML,/Strategia · Falco/);assert.equal((f.root.innerHTML.match(/data-policy=/g)??[]).length,8);
 f.callbacks.input({target:{id:'',dataset:{policy:'continents'},checked:false}});
 f.callbacks.input({target:{id:'grace-rounds',dataset:{},value:'2'}});
 f.click({action:'policy-all'});f.click({action:'close'});f.click({strategy:'1'});
 assert.match(f.root.innerHTML,/data-policy="continents" \/>/);
 assert.match(f.root.innerHTML,/value="2" selected/);
 f.click({action:'close'});f.click({action:'new'});
 const message=f.worker.messages.find(m=>m.type==='NEW');
 assert.equal(message.config.botPolicies[0].continents,false);assert.equal(message.config.botPolicies[1].graceRounds,2);
 f.cleanup();
});

test('Antartide facoltativa: menu, plancia completa e ripresa con inquadratura estesa',()=>{
 const f=fixture();assert.match(f.root.innerHTML,/42 TERRITORI/);
 f.callbacks.input({target:{id:'antarctica-mode',dataset:{},checked:true}});
 assert.match(f.root.innerHTML,/44 TERRITORI/);assert.match(f.root.innerHTML,/Antartide Occidentale/);
 f.click({action:'new'});assert.equal(f.worker.messages.find(m=>m.type==='NEW').config.antarctica,true);
 const s=createGame({antarctica:true,players:3,name:'Test',difficulties:['medio','difficile'],turnSeconds:180,seed:3});
 f.worker.emit({type:'STATE',state:project(s)});
 assert.equal((f.root.innerHTML.match(/data-territory=/g)??[]).length,44);
 assert.match(f.root.innerHTML,/viewBox="0 0 1240 815"/);assert.match(f.root.innerHTML,/data-token="43"/);
 mkdirSync('artifacts/previews',{recursive:true});writeFileSync('artifacts/previews/antarctica-table.html',f.root.innerHTML);
 f.click({action:'objective'});assert.match(f.root.innerHTML,/gli obiettivi restano da 86 punti/);
 f.click({action:'close'});f.click({action:'home'});f.click({action:'configure'});
 f.callbacks.input({target:{id:'antarctica-mode',dataset:{},checked:false}});
 assert.match(f.root.innerHTML,/42 TERRITORI/);assert.doesNotMatch(f.root.innerHTML,/data-territory="42"/);
 writeFileSync('artifacts/previews/home.html',f.root.innerHTML);f.cleanup();
});

test('Home Torneo: Countdown 45 minuti, Antartide off, velocità e personalità personalizzabili',()=>{
 const f=fixture();assert.match(f.root.innerHTML,/30 armate iniziali/);assert.match(f.root.innerHTML,/Preparazione partita/);
 f.callbacks.input({target:{id:'end-mode',dataset:{},value:'countdown'}});
 assert.match(f.root.innerHTML,/value="45" selected/);
 f.callbacks.input({target:{id:'bot-speed',dataset:{},value:'2'}});
 f.click({strategy:'0'});assert.match(f.root.innerHTML,/personality/);
 f.callbacks.input({target:{id:'bot-personality',dataset:{},value:'incursore'}});
 f.click({action:'close'});f.click({action:'new'});
 const config=f.worker.messages.find(m=>m.type==='NEW').config;
 assert.equal(config.countdownMinutes,45);assert.equal(config.endMode,'countdown');assert.equal(config.antarctica,false);assert.equal(config.botSpeed,2);assert.equal(config.manualReinforcement,true);
 assert.equal(config.botPolicies[0].personality,'incursore');f.cleanup();
});

test('Widget obiettivo mostra punti, mappa punti e bonus; Undo passa attraverso il protocollo dei comandi',()=>{
 const f=fixture();f.click({action:'new'});
 let s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3,manualReinforcement:true});
 s.active=0;s.phase='REINFORCEMENT';s.round=5;s.reinforcements=1;
 s=applyCommand(s,0,{type:'PLACE',territory:owned(s,0)[0],count:1});
 f.worker.emit({type:'STATE',state:project(s)});
 assert.match(f.root.innerHTML,/Inizia attacco/);f.click({action:'objective-widget'});
 assert.match(f.root.innerHTML,/Punti attuali:/);assert.match(f.root.innerHTML,/Mostra Punti su Mappa/);assert.match(f.root.innerHTML,/continent-bonuses/);
 f.click({action:'objective-view'});assert.equal((f.root.innerHTML.match(/class="objective-points"/g)??[]).length,42);
 f.click({action:'undo-place'});assert.deepEqual(f.worker.messages.at(-1).command,{type:'UNDO_PLACE'});f.cleanup();
});

test('I tick del timer non ricostruiscono la plancia né fanno perdere la finestra carte',()=>{
 const f=fixture();f.click({action:'new'});const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3,endMode:'countdown'});
 s.phase='ATTACK';s.active=0;f.worker.emit({type:'STATE',state:project(s)});f.click({action:'cards'});
 const markup=f.root.innerHTML;
 f.worker.emit({type:'CLOCK',id:s.id,revision:s.revision,remainingMs:179000,matchRemainingMs:2699000,elapsedMs:1000});
 assert.equal(f.root.innerHTML,markup,'Nessun rimpiazzo del DOM a ogni secondo');
 f.worker.emit({type:'CLOCK',id:'altra-partita',revision:0,remainingMs:0,matchRemainingMs:0,elapsedMs:1000});
 f.click({action:'close'});assert.match(f.root.innerHTML,/2:59/);assert.match(f.root.innerHTML,/44:59/);f.cleanup();
});

test('home verticale: menu reale, preparazione separata e Survival per tutti i bot',()=>{
 const f=fixture(false,false);
 mkdirSync('artifacts/previews',{recursive:true});writeFileSync('artifacts/previews/home-portrait.html',f.root.innerHTML);
 assert.match(f.root.innerHTML,/atlas-home/);assert.match(f.root.innerHTML,/Statistiche/);
 assert.doesNotMatch(f.root.innerHTML,/data-bot=/);
 f.click({action:'survival'});assert.equal((f.root.innerHTML.match(/value="survival" selected/g)??[]).length,3);
 f.click({action:'new'});assert.deepEqual(f.worker.messages.find(m=>m.type==='NEW').config.difficulties,['survival','survival','survival']);
 f.cleanup();
});
test('selezione attacco: solo bersagli legali evidenziati, nessun territorio estraneo attenuato',()=>{
 const f=fixture();f.click({action:'new'});
 const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:3});
 s.active=0;s.phase='ATTACK';s.round=5;s.board.forEach(c=>{c.owner=1;c.armies=3;});s.board[0]={owner:0,armies:8};
 f.worker.emit({type:'STATE',state:project(s)});f.callbacks.keydown({key:'Enter',target:{closest:()=>({getAttribute:()=> '0'})}});
 assert.doesNotMatch(f.root.innerHTML,/map-territory--dimmed/);
 assert.equal((f.root.innerHTML.match(/ attackable /g)??[]).length,3);
 assert.match(f.root.innerHTML,/source-ring/);assert.doesNotMatch(f.root.innerHTML,/fill="#d49b35"/);
 f.cleanup();
});

test('menu: audio, regole e strategie usano pagine verticali coerenti; ritorno alla preparazione',()=>{
 const f=fixture();
 for(const action of ['audio','rules']){
  f.click({action});assert.match(f.root.innerHTML,/modal-backdrop menu-sheet/);assert.match(f.root.innerHTML,/classic-home portal-route/);assert.match(f.root.innerHTML,/portal-back/);
  f.click({action:'close'});assert.doesNotMatch(f.root.innerHTML,/modal-backdrop menu-sheet/);assert.match(f.root.innerHTML,/Preparazione partita/);
 }
 f.click({strategy:'0'});assert.match(f.root.innerHTML,/menu-sheet/);assert.match(f.root.innerHTML,/Strategia · Falco/);f.cleanup();
});
test('statistiche: pagina a colonna con cronologia espandibile, senza tabella orizzontale',()=>{
 const f=fixture();f.worker.emit({type:'PROFILE',profile:{name:'Comandante',results:[{date:'2026-10-30',players:3,rank:1,points:86,reason:'risiko',duration:180000,name:'Comandante'}]}});
 f.click({action:'profile'});
 assert.match(f.root.innerHTML,/profile-page portal-column/);assert.match(f.root.innerHTML,/match-history/);assert.match(f.root.innerHTML,/details class="portal-section"/);assert.doesNotMatch(f.root.innerHTML,/<table/);
 mkdirSync('artifacts/previews',{recursive:true});writeFileSync('artifacts/previews/profile-1.6.html',f.root.innerHTML);
 f.click({action:'audio'});writeFileSync('artifacts/previews/audio-1.6.html',f.root.innerHTML);f.cleanup();
});

// Inspect every render, not just the final state: the old camera reset lasted
// only for the presentation and was hidden by its subsequent restore.
for(const placementPhase of ['SETUP_PLACEMENT','REINFORCEMENT'] as const)test(`zoom: ${placementPhase} e attacchi fuori inquadratura non cambiano la camera, neanche durante gli effetti`,()=>{
 const f=fixture(true,true,true),originalSet=globalThis.setTimeout,originalClear=globalThis.clearTimeout;
 let serial=0;const timers=new Map<number,()=>void>();
 globalThis.setTimeout=((fn:()=>void)=>{const id=++serial;timers.set(id,fn);return id;}) as never;
 globalThis.clearTimeout=((id:number)=>timers.delete(id)) as never;
 const camera=()=>f.root.innerHTML.match(/id="world-map"[^>]*viewBox="([^"]+)"/)![1];
 const flush=()=>{for(let n=0;timers.size&&n<1000;n++){const [id,fn]=timers.entries().next().value!;timers.delete(id);fn();}};
 try{
  f.click({action:'new'});
  let s=createGame({players:3,name:'Zoom',difficulties:['medio','medio'],turnSeconds:180,seed:31});
  s.active=0;s.phase=placementPhase;s.reinforcements=5;s.round=6;
  if(placementPhase==='REINFORCEMENT')s.players.forEach(p=>p.setupRemaining=0);
  s.board[0]={owner:0,armies:9};s.board[29]={owner:1,armies:1};
  f.worker.emit({type:'STATE',state:project(s)});
  for(let n=0;n<4;n++)f.click({action:'zoom-in'});
  for(const command of [{type:'PLACE',territory:0,count:1},{type:'ATTACK',from:0,to:29}] as const){
   if(command.type==='ATTACK'){s.phase='ATTACK';s.players.forEach(p=>p.setupRemaining=0);s.revision++;f.worker.emit({type:'STATE',state:project(s)});}
   const before=camera(),after=applyCommand(s,0,command),transition=makeTransition(s,after)!;
   assert.ok(transition);f.worker.emit({type:'TRANSITION',transition});
   assert.equal(camera(),before,'Nessun fotogramma a mappa completa');
   f.click({action:'zoom-in'});f.click({action:'names'});const changed=camera();
   assert.notEqual(changed,before,'Il giocatore può cambiare zoom durante gli effetti');
   flush();assert.equal(camera(),changed,'Il completamento non deve ripristinare uno zoom vecchio');
   assert.ok(f.worker.messages.some(m=>m.type==='PRESENTED'&&m.id===transition.id));s=after;
  }
  // A state update can cancel an animation (pause/resume/watchdog).
  s.phase='REINFORCEMENT';s.pending=null;s.reinforcements=2;s.revision++;
  f.worker.emit({type:'STATE',state:project(s)});
  const after=applyCommand(s,0,{type:'PLACE',territory:0,count:1}),t=makeTransition(s,after)!;
  f.worker.emit({type:'TRANSITION',transition:t});const before=camera();
  f.worker.emit({type:'STATE',state:project({...after,paused:true})});flush();assert.equal(camera(),before);
  f.click({action:'reset-map'});f.click({action:'names'});assert.equal(camera(),'0 0 1240 690');
 }finally{f.cleanup();globalThis.setTimeout=originalSet;globalThis.clearTimeout=originalClear;}
});

test('PWA e backup: menu web, stato offline esplicito e conferma separata dal ripristino',()=>{
 const f=fixture(false,false);
 assert.match(f.root.innerHTML,/Installa offline/);assert.match(f.root.innerHTML,/Backup/);
 f.click({action:'install'});assert.match(f.root.innerHTML,/Preparazione offline in corso/);assert.match(f.root.innerHTML,/Aggiornamenti|aggiornamenti/);
 f.click({action:'close'});f.click({action:'backup'});assert.match(f.root.innerHTML,/backup-file/);
 f.click({action:'export-backup'});assert.equal(f.worker.messages.at(-1).type,'EXPORT_BACKUP');
 assert.ok(!f.worker.messages.some(m=>m.type==='IMPORT_BACKUP'));
 f.cleanup();
});
