// Real view markup and production CSS, with isolated, deterministic demonstration data.
// No server, saved games, network, or gameplay worker is used by the exported files.
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {resolve} from 'node:path';
import {mountGame} from '../src/gui/App.ts';
import {createGame,project,owned} from '../src/engine/RisikoRulesEngine.ts';
import {tutorialGame} from '../src/engine/Tutorial.ts';
import {captureTurn} from '../src/engine/Replay.ts';
import {mapFor} from '../src/data/mapGraph.ts';
const out=resolve(process.argv[2]??'artifacts/previews-2.0');mkdirSync(out,{recursive:true});
class MockWorker{static instance;onmessage;constructor(){MockWorker.instance=this;}postMessage(){}terminate(){}emit(data){this.onmessage?.({data});}}
const callbacks={};let html='';
const root={set innerHTML(v){html=v;},get innerHTML(){return html;},querySelector(){return null;},querySelectorAll(){return [];},addEventListener(k,f){callbacks[k]=f;},removeEventListener(){}};
Object.assign(globalThis,{Worker:MockWorker,document:{hidden:false,addEventListener(){},removeEventListener(){}},history:{replaceState(){}}});
const cleanup=mountGame(root),worker=MockWorker.instance;
const click=(dataset)=>callbacks.click({target:{closest:()=>({dataset,hasAttribute:()=>false})}});
const action=a=>click({action:a});
const territory=id=>callbacks.keydown({key:'Enter',target:{closest:()=>({getAttribute:()=>String(id)})}});
const profile={name:'Comandante',results:Array.from({length:8},(_,i)=>({id:'demo-'+i,date:Date.UTC(2026,10,4-i),name:'Comandante',rank:i%3===0?2:1,points:[58,69,100,45,78,63,72,60][i],objectivePoints:[58,69,86,45,78,63,72,60][i],reason:i===2?'risiko':'sdadata',duration:2400000,players:4,category:'solo',difficulties:['medio','difficile'],dice:[20+i,24,19,26,21,28]}))};
worker.emit({type:'PROFILE',profile});worker.emit({type:'READY',hasGame:true});
const screens=[];
function snap(id,title,w=420,h=900,description=''){
 const board=html.includes('class="game-table');
 let markup=html.replace('class="app-shell"',`class="app-shell${board?' on-board':''}${!board&&html.includes('menu-sheet')?' menu-sheet-open':''}"`);
 screens.push({id,title,w,h,description,markup});
}
function home(){action('home');action('close');}
function table(s){home();action('new');worker.emit({type:'STATE',state:project(s,0)});}
const config={players:4,name:'Comandante',difficulties:['medio','difficile','survival'],turnSeconds:180,seed:42};
const base=createGame(config);base.active=0;base.round=5;base.phase='ATTACK';base.players.forEach(p=>p.setupRemaining=0);
base.board.forEach((c,i)=>c.armies=1+i%4);
base.log=[{type:'onTurnStarted',round:5,actor:0,message:'Giro 5 · Il tuo turno',action:'turn'},{type:'onReinforced',round:5,actor:0,message:'Rinforzi: +5 carri',action:'reinforce'}];
snap('01-home','Home',420,960);
action('configure');snap('02-preparazione','Preparazione partita',420,1000);
click({strategy:'0'});snap('03-strategie','Strategie e personalità bot');action('close');
for(const [id,title,a] of [['04-statistiche','Statistiche e profilo','profile'],['05-audio','Audio e vibrazione','audio'],['06-backup','Backup e ripristino','backup'],['07-profili','Profili locali','profiles'],['08-scenari','Scenari e sfide','scenarios'],['09-editor','Editor scenario','editor-new'],['10-regole','Regole Torneo','rules'],['11-installazione','Installazione offline','install']]){
 home();action(a);if(a==='profiles')worker.emit({type:'PROFILES',profiles:[{id:'local',name:'Comandante'},{id:'marco',name:'Marco'}]});snap(id,title,420,a==='profile'?1100:900);
}
home();action('slots');worker.emit({type:'SLOTS',slots:[{id:'demo1',name:'Tavolo del comandante',round:5,updatedAt:Date.UTC(2026,10,4),players:4,phase:'ATTACK',endMode:'regular',board:base.board},{id:'demo2',name:'Sfida Survival',round:3,updatedAt:Date.UTC(2026,10,3),players:3,phase:'REINFORCEMENT',endMode:'regular',board:base.board}]});snap('12-partite','Archivio partite');
table(base);snap('13-plancia','Plancia di gioco',1180,720);
const initial=createGame(config);initial.active=0;table(initial);snap('14-piazzamento','Piazzamento iniziale',1180,720);
const reinforce=structuredClone(base);reinforce.phase='REINFORCEMENT';reinforce.reinforcements=5;table(reinforce);snap('15-rinforzi','Rinforzi',1180,720);
const cards=mapFor(base).cards.filter(c=>c.symbol==='fante').slice(0,3).map(c=>c.id);
const hand=structuredClone(base);hand.phase='TRIS_EXCHANGE';hand.players[0].cards=cards;hand.deck=hand.deck.filter(id=>!cards.includes(id));table(hand);action('cards');cards.forEach(id=>click({card:String(id)}));snap('16-carte','Carte territorio e tris',1180,720);
action('close');action('objective');snap('17-obiettivo','Carta obiettivo Torneo',1180,720);
action('close');action('objective-map');snap('18-punti','Punti obiettivo sulla plancia',1180,720);
const battle=tutorialGame(2);delete battle.tutorialStep;battle.players[0].name='Comandante';const from=owned(battle,0).find(id=>battle.board[id].armies===12),to=mapFor(battle).territories[from].neighbors.find(n=>battle.board[n].owner!==0&&battle.board[n].armies===3);
battle.lastBattle={from,to,attack:[6,5,3],defense:[6,4,2],message:'Attaccante −1 · Difensore −2'};
table(battle);territory(from);territory(to);snap('19-attacco','Dialogo di combattimento',1180,720);
const move=tutorialGame(4);delete move.tutorialStep;move.players[0].name='Comandante';const mf=owned(move,0).find(id=>move.board[id].armies===8),mt=mapFor(move).territories[mf].neighbors.find(id=>move.board[id].owner===0);table(move);territory(mf);territory(mt);snap('20-spostamento','Spostamento strategico',1180,720);
table(base);action('game-menu');snap('21-menu-partita','Menu partita e ritmo bot',1180,720);action('close');action('log');snap('22-registro','Registro degli eventi',1180,720);action('log');
table({...base,paused:true});snap('23-pausa','Pausa',1180,720);
table(base);worker.emit({type:'TURN_SUMMARY',lines:['Oceania: bonus continente interrotto.','Falco ha conquistato Indonesia.','Vega ha rinforzato Ucraina di 4 carri.']});snap('24-riepilogo','Riepilogo turno bot',1180,720);
table(tutorialGame(0));snap('25-tutorial','Tutorial guidato',1180,720);
table(base);worker.emit({type:'PRIVACY',player:1,name:'Marco',color:'#d91e34',token:'preview'});snap('26-privacy','Pass & Play · Passaggio privato',420,900);
const end=structuredClone(base);end.phase='GAME_OVER';end.winner=0;end.reason='sdadata';end.round=22;end.elapsedMs=2450000;end.replay=[captureTurn(base),captureTurn(end)];end.scoreHistory=[{round:5,points:[24,20,23,19]},{round:12,points:[38,26,31,18]},{round:22,points:[54,33,28,21]}];table(end);snap('27-risultati','Risultati, replay e resoconto',420,1100);
const ant=createGame({...config,antarctica:true});ant.active=0;table(ant);snap('28-antartide','Plancia con Antartide',1180,760);
const conquest=structuredClone(move);conquest.phase='POST_CONQUEST_MOVE';conquest.pending={from:mf,to:mt,min:3,transferred:3};conquest.board[mf].armies=5;conquest.board[mt].armies=3;table(conquest);snap('29-conquista','Spostamento dopo la conquista',1180,720);
table(base);click({playerInfo:'1'});snap('30-giocatore','Scheda giocatore',1180,720);
cleanup();
let css=readFileSync('src/gui/effects.css','utf8')+'\n'+readFileSync('src/gui/styles.css','utf8').replace("@import './effects.css';",'');
css=css.replace(/url\(['"]?\/fonts\/([^'"\)]+)['"]?\)/g,(_,file)=>`url(data:font/ttf;base64,${readFileSync('public/fonts/'+file).toString('base64')})`);
// Fixing a preview window's position is the only presentation override; actual
// App runtime normally computes this using DOM measurements, unavailable here.
css+='\n.floating-panel{right:80px;top:90px;left:auto}.portrait-notice{display:none!important}';
const mascot='data:image/png;base64,'+readFileSync('public/mascot.png').toString('base64');
const head=`<!doctype html><html lang="it"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>${css}</style></head><body>`;
for(const s of screens){s.markup=s.markup.replaceAll('./mascot.png',mascot);s.file=s.id+'.html';writeFileSync(resolve(out,s.file),head+s.markup+'<script>document.querySelectorAll("button").forEach(b=>b.addEventListener("click",()=>{const t=document.querySelector("#toast");if(t){t.textContent="Anteprima statica: apri il gioco per interagire";t.classList.add("visible");setTimeout(()=>t.classList.remove("visible"),1500);}}));</script></body></html>');}
// One self-contained overview: shared CSS/assets, actual view markup per iframe.
// Keep image payload only once and inject it into previews at runtime.
const data=screens.map(s=>({...s,markup:s.markup.replaceAll(mascot,'__MASCOT__')}));
const overview=`<!doctype html><html lang="it"><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RisiKo Offline · Anteprime 2.0</title><style>*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at top,#1e4650,#0b1f2e 65%);color:#f2e7d3;font-family:Georgia,serif}header{text-align:center;padding:36px 20px;border-bottom:1px solid #c8a45a}h1{color:#e6c885;font-size:42px;margin:0}p{font:15px/1.5 system-ui,sans-serif;color:#c9d9da}nav{display:flex;flex-wrap:wrap;gap:8px;justify-content:center;padding:20px}nav a{font:13px system-ui;color:#e5c480;text-decoration:none;border:1px solid #756844;padding:8px;border-radius:6px}main{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:24px;padding:24px;max-width:1800px;margin:auto}article{min-width:0}article.wide{grid-column:span 2}h2{text-align:center;font-size:18px;text-transform:uppercase;letter-spacing:1px;border:1px solid #c8a45a;padding:10px;margin:0 0 8px;color:#e6c885}section{position:relative;overflow:hidden;border:2px solid #c8a45a;border-radius:20px;background:#0b1f2e;box-shadow:0 4px 14px #0008}iframe{display:block;border:0;transform-origin:top left}button{display:block;background:#1e3440;color:#f2e7d3;border:1px solid #c8a45a;padding:9px 16px;border-radius:8px;margin:8px auto;cursor:pointer}dialog{padding:8px;background:#0b1f2e;border:1px solid #c8a45a;max-width:98vw;max-height:98dvh;color:white}dialog iframe{width:100%;height:calc(90dvh - 52px)}footer{text-align:center;padding:20px;color:#bccccc}@media(max-width:650px){main{display:block;padding:12px}article{margin-bottom:24px}h1{font-size:30px}}</style><header><h1>RisiKo Offline</h1><p>Atlante delle schermate · versione 2.0.0 · Navy, ottone, avorio</p><p>30 anteprime statiche generate dai componenti e dai CSS del progetto.<br>Dati dimostrativi. Scorri dentro le finestre per vedere i contenuti lunghi; “Apri” mostra la schermata ingrandita.<br>Non sono screenshot di un browser verificato: il runtime di questo ambiente ne impedisce l’avvio.</p></header><nav></nav><main></main><dialog><button onclick="this.parentElement.close()">Chiudi</button><iframe title="Anteprima ingrandita"></iframe></dialog><footer>Solo obiettivi Torneo da 86 punti · Antartide facoltativa · 3–5 partecipanti</footer><script>
const screens=${JSON.stringify(data).replaceAll('<','\\u003c')},head=${JSON.stringify(head).replaceAll('<','\\u003c')},mascot=${JSON.stringify(mascot)};
const doc=s=>head+s.markup.replaceAll('__MASCOT__',mascot)+'</body></html>';
for(const s of screens){const a=document.createElement('article');a.id=s.id;a.className=s.w>600?'wide':'';const h=document.createElement('h2');h.textContent=s.title;a.append(h);const frame=document.createElement('section'),iframe=document.createElement('iframe');iframe.title=s.title;iframe.loading='lazy';iframe.srcdoc=doc(s);iframe.style.width=s.w+'px';iframe.style.height=s.h+'px';frame.append(iframe);a.append(frame);const b=document.createElement('button');b.textContent='Apri '+s.title;b.onclick=()=>{const d=document.querySelector('dialog');d.style.width=Math.min(s.w,innerWidth-24)+'px';d.querySelector('iframe').srcdoc=doc(s);d.showModal()};a.append(b);document.querySelector('main').append(a);new ResizeObserver(()=>{const scale=frame.clientWidth/s.w;iframe.style.transform='scale('+scale+')';frame.style.height=s.h*scale+'px'}).observe(frame);const link=document.createElement('a');link.href='#'+s.id;link.textContent=s.title;document.querySelector('nav').append(link);}
</script></html>`;
writeFileSync(resolve(out,'index.html'),overview);
writeFileSync(resolve(out,'manifest.json'),JSON.stringify(screens.map(({markup,...s})=>s),null,2));
writeFileSync(resolve(out,'LEGGIMI.txt'),'Apri index.html in un browser per la tavola riepilogativa. Ogni file numerato è una schermata indipendente con CSS, font e immagini incorporati: nessuna connessione necessaria.\nSono anteprime statiche dei componenti, con dati dimostrativi; non una partita giocabile né screenshot certificati su dispositivo. Il browser in sandbox non è eseguibile.\nPer esportare PNG su una macchina abilitata: node scripts/capture-previews.mjs <cartella-anteprime>.\n');
console.log(`Esportate ${screens.length} schermate e tavola: ${out}`);
