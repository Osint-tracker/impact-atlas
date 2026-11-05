import {spawn} from 'node:child_process';
import {mkdir,writeFile} from 'node:fs/promises';
import {clickTerritory,browserDiagnostics} from './browser-input.mjs';
import {readBoardLayout,assertBoardLayout,readMapScale} from './browser-layout.mjs';
import {setTimeout as wait} from 'node:timers/promises';
import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {objectives,territories} from '../src/data/mapGraph.ts';
import {createGame,applyCommand,assertState} from '../src/engine/RisikoRulesEngine.ts';
await mkdir('artifacts/browser',{recursive:true});
const server=spawn(process.execPath,['scripts/serve.mjs'],{stdio:'pipe',env:{...process.env,PORT:'4173'}});
let browser,page,logs='';server.stdout.on('data',d=>logs+=d);server.stderr.on('data',d=>logs+=d);
try{
 let connected=false;
 for(let i=0;i<100;i++){if(server.exitCode!==null)throw new Error('Server non disponibile: '+logs);try{if((await fetch('http://127.0.0.1:4173')).ok){connected=true;break;}}catch{}await wait(200);}
 assert.ok(connected,'Server non avviato');
 browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 page=await browser.newPage({viewport:{width:844,height:390},...(process.env.RECORD_UI?{recordVideo:{dir:'artifacts/browser/video',size:{width:844,height:390}}}:{})});
 const errors=[],external=[];
 await page.context().tracing.start({screenshots:true,snapshots:true});
 page.on('pageerror',e=>errors.push(e.message));
 page.on('request',r=>{if(!r.url().startsWith('http://127.0.0.1:4173')&&!r.url().startsWith('data:')&&!r.url().startsWith('blob:'))external.push(r.url());});
 await page.goto('http://127.0.0.1:4173');
 await page.locator('[data-action="configure"]:not([disabled])').first().waitFor();
 await page.setViewportSize({width:390,height:844});
 assert.equal(await page.locator('.portrait-notice').isVisible(),false);
 assert.equal(await page.locator('[data-bot]').count(),0);
 await page.screenshot({path:'artifacts/browser/portrait-home.png',fullPage:true});
 await page.locator('.portal-menu [data-action="profile"]').click();
 await page.locator('.profile-page.portal-column').waitFor();
 assert.equal(await page.locator('.portrait-notice').isVisible(),false);
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
 await page.screenshot({path:'artifacts/browser/portrait-statistics.png'});
 await page.locator('[data-action="home"]').first().click();
 await page.locator('.portal-play').click();
 await page.locator('[data-bot="0"]').selectOption('survival');
 assert.equal(await page.locator('[data-bot="0"]').inputValue(),'survival');
 await page.locator('[data-bot="0"]').selectOption('medio');
 await page.locator('[data-strategy="2"]').scrollIntoViewIfNeeded();
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
 await page.screenshot({path:'artifacts/browser/portrait-prepare.png'});
 // On shallow phone/webview screens the whole settings panel must scroll.
 for(const viewport of [{width:844,height:390},{width:932,height:260},{width:640,height:300}]){
  await page.setViewportSize(viewport);
  const layout=await page.locator('.classic-settings').evaluate(panel=>({height:panel.clientHeight,overflow:getComputedStyle(panel).overflowY,innerOverflow:getComputedStyle(panel.querySelector('.settings-scroll')).overflowY}));
  assert.ok(layout.height>=viewport.height*.65,`Impostazioni troppo basse: ${JSON.stringify(layout)}`);
  assert.equal(layout.overflow,'visible','La pagina verticale scorre senza un secondo pannello interno');assert.equal(layout.innerOverflow,'visible','Nessuna seconda striscia scorrevole');
  await page.locator('[data-strategy="2"]').scrollIntoViewIfNeeded();
  const row=await page.locator('[data-strategy="2"]').boundingBox();assert.ok(row.height>=44&&row.y>=0&&row.y+row.height<=viewport.height);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await page.locator('.classic-settings').evaluate(panel=>panel.scrollTop=0);
  await page.screenshot({path:`artifacts/browser/home-${viewport.width}x${viewport.height}.png`});
 }
 await page.setViewportSize({width:844,height:390});
 await page.screenshot({path:'artifacts/browser/lobby.png',fullPage:true});
 await page.locator('#antarctica-mode').check();assert.match(await page.locator('.section-heading').innerText(),/44 TERRITORI/);
 await page.screenshot({path:'artifacts/browser/lobby-antarctica.png'});
 await page.locator('#antarctica-mode').uncheck();assert.match(await page.locator('.section-heading').innerText(),/42 TERRITORI/);
 await page.locator('.classic-settings').evaluate(panel=>panel.scrollTop=0);
 await page.locator('[data-action="new"]').scrollIntoViewIfNeeded();
 const startRect=await page.locator('[data-action="new"]').boundingBox();assert.ok(startRect.y+startRect.height<=390);
 await page.locator('[data-strategy="0"]').click();await page.locator('.strategy-window').waitFor();
 assert.equal(await page.locator('[data-policy]').count(),8);
 await page.locator('[data-policy="continents"]').uncheck();await page.locator('#grace-rounds').selectOption('2');
 await page.locator('[data-action="policy-all"]').click();await page.screenshot({path:'artifacts/browser/strategies.png'});
 await page.locator('[data-action="close"]').click();await page.locator('[data-strategy="1"]').click();
 assert.equal(await page.locator('[data-policy="continents"]').isChecked(),false);assert.equal(await page.locator('#grace-rounds').inputValue(),'2');
 await page.locator('[data-action="policy-reset"]').click();await page.locator('[data-action="policy-all"]').click();await page.locator('[data-action="close"]').click();
 

 await page.locator('[data-global-level="casuale"]').click();assert.equal(await page.getByText('Bot ? · livello segreto',{exact:true}).count(),3);
 await page.locator('[data-count="3"]').click();await page.locator('[data-action="new"]').click();
 await page.locator('.game-table').waitFor();assert.equal(await page.locator('[data-territory]').count(),42);
 assert.equal(await page.evaluate(async()=>{await document.fonts.load('900 15px RisikoCounter','0123456789');return document.fonts.check('900 15px RisikoCounter','0123456789');}),true,'Il carattere dei carri deve essere incorporato e caricato');
 async function setPacing(speed,filter='all'){
  await page.locator('[data-action="game-menu"]').click();
  await page.locator(`[data-speed="${speed}"]`).click();
  await page.locator('#animation-filter').selectOption(filter);
  await page.locator('[data-action="close"]').click();
 }
 async function pauseGame(){
  if(await page.locator('.pause-card').isVisible())return;
  if(await page.locator('#dialogs [data-action="close"]').isVisible())await page.locator('#dialogs [data-action="close"]').click();
  await page.locator('[data-action="game-menu"]').click();
  await page.locator('[data-action="pause"]').click();
  await page.locator('.pause-card').waitFor();
 }
 async function resumeHome(){
  await page.locator('[data-action="slots"]:not([disabled])').waitFor();
  await page.locator('[data-action="slots"]').click();
  await page.locator('.pause-card').waitFor();
  await page.locator('[data-action="resume"]').click();
  await page.locator('.pause-card').waitFor({state:'detached'});
 }
 async function completeSetup(expected,territoryCount){
  await page.locator('.game-table[data-phase="SETUP_PLACEMENT"]').waitFor();
  await setPacing(0,'summary');
  console.log(`Setup: ${territoryCount} territori umani, ${expected} armate da piazzare`);
  let clicks=0;const deadline=Date.now()+60000;
  while(Date.now()<deadline){
   const phase=await page.locator('.game-table').getAttribute('data-phase');if(phase!=='SETUP_PLACEMENT')break;
   const human=await page.locator('.game-table').getAttribute('data-active')==='0';
   if(human&&await page.locator('#effects > *').count()===0){
    assert.equal(await page.locator('.placement-plus').count(),territoryCount);
    const plus=await page.locator('.placement-plus-hit').first().boundingBox();assert.ok(plus.width>=43.9&&plus.height>=43.9,'Area touch + almeno 44 px');
    const revision=Number(await page.locator('.game-table').getAttribute('data-revision'));
    const id=Number(await page.locator('.territory.placeable').first().getAttribute('data-territory'));
    const before=Number(await page.locator(`[data-token="${id}"] .tank-number`).textContent());
    await clickTerritory(page,id);
    await page.waitForFunction(({revision,id,before})=>{
     const table=document.querySelector('.game-table'),number=document.querySelector(`[data-token="${id}"] .tank-number`);
     return table&&Number(table.getAttribute('data-revision'))>revision&&Number(number?.textContent)===before+1;
    },{revision,id,before},{timeout:5000});clicks++;
   }else await wait(100);
  }
  assert.equal(clicks,expected,'Devono essere piazzate tutte le armate della riserva iniziale');
  assert.notEqual(await page.locator('.game-table').getAttribute('data-phase'),'SETUP_PLACEMENT');
 }
 await completeSetup(21,14);
 // Geometry and zoom require one stable position, not a live instant-speed bot turn.
 // The setup above remains exercised through actual clicks and the real Worker.
 const layoutFixture=createGame({players:3,name:'Comandante',difficulties:['medio','medio'],turnSeconds:600,seed:17});
 layoutFixture.active=0;layoutFixture.round=5;layoutFixture.phase='ATTACK';
 layoutFixture.players.forEach(p=>p.setupRemaining=0);
 await loadFixture(layoutFixture);
 await page.locator('.game-table[data-active="0"][data-phase="ATTACK"]').waitFor();
 for(const viewport of [{width:844,height:390},{width:932,height:430},{width:667,height:375}]){
  await page.setViewportSize(viewport);await page.waitForFunction(()=>document.querySelector('.map-stage')?.clientHeight>0);
  assertBoardLayout(await readBoardLayout(page));
  await page.screenshot({path:`artifacts/browser/atlas-board-${viewport.width}x${viewport.height}.png`});
 }
 await page.setViewportSize({width:844,height:390});
 const initialScale=await readMapScale(page);
 await page.locator('[data-action="zoom-in"]').click();
 await page.waitForFunction(before=>document.querySelector('#world-map')?.getAttribute('viewBox')!==before,initialScale.viewBox);
 const zoomedScale=await readMapScale(page);
 assert.ok(Math.abs(zoomedScale.tokenWidth/initialScale.tokenWidth-initialScale.viewWidth/zoomedScale.viewWidth)<.02,'I carri devono scalare nella stessa proporzione dei territori');
 await page.locator('[data-action="reset-map"]').click();

 await pauseGame();await page.locator('.pause-card').waitFor();
 await page.screenshot({path:'artifacts/browser/table.png',fullPage:true});
 await page.reload();await resumeHome();await page.locator('.game-table').waitFor();
 await page.locator('[data-action="objective"]').click();await page.getByText('IL TUO OBIETTIVO',{exact:true}).waitFor();
 await page.screenshot({path:'artifacts/browser/objective.png'});await page.locator('[data-action="close"]').click();
 await page.locator('.reference-eye').click();await page.locator('[data-action="objective-map"]').click();assert.ok(await page.locator('.territory.goal').count()>0);

 // Load through real IndexedDB with the previous game's Worker already gone.
 // Reloading an active game after writing the fixture can trigger visibility
 // PAUSE and overwrite the fixture with the old game's in-memory state.
 async function loadFixture(state){
  assertState(state);
  // Callers may already have paused the game (for example the turn-boundary
  // scenario). Its overlay covers Pausa: do not click through that overlay.
  const pauseCard=page.locator('.pause-card');
  if(!await pauseCard.isVisible())await pauseGame();
  await pauseCard.waitFor();
  await page.goto('http://127.0.0.1:4173/icon.svg');
  const fixture=structuredClone(state);fixture.paused=true;
  const stored=await page.evaluate(state=>new Promise((resolve,reject)=>{
   const req=indexedDB.open('risiko-offline',2);req.onerror=()=>reject(req.error);
   req.onsuccess=()=>{
    const db=req.result,tx=db.transaction(['game','slots'],'readwrite'),store=tx.objectStore('game');
    tx.objectStore('slots').clear();tx.objectStore('slots').put({id:state.id,name:'Fixture browser',updatedAt:Date.now(),state},state.id);
    store.put(state,'current');
    const check=store.get('current');let saved;
    check.onsuccess=()=>{saved=check.result;};
    tx.oncomplete=()=>{db.close();resolve({id:saved.id,phase:saved.phase,players:saved.players.length,remaining:saved.players[0].setupRemaining});};
    tx.onerror=tx.onabort=()=>{db.close();reject(tx.error??new Error('Salvataggio fixture interrotto'));};
   };
  }),fixture);
  assert.deepEqual(stored,{id:fixture.id,phase:fixture.phase,players:fixture.players.length,remaining:fixture.players[0].setupRemaining},'Fixture salvata correttamente prima di avviare il nuovo Worker');
  await page.goto('http://127.0.0.1:4173');
  await page.locator('.portal-play:not([disabled])').waitFor();
  const loaded=await page.evaluate(()=>new Promise((resolve,reject)=>{
   const req=indexedDB.open('risiko-offline',2);req.onerror=()=>reject(req.error);
   req.onsuccess=()=>{
    const db=req.result,tx=db.transaction('game','readonly'),read=tx.objectStore('game').get('current');let saved;
    read.onsuccess=()=>{saved=read.result;};
    tx.oncomplete=()=>{db.close();resolve({id:saved.id,phase:saved.phase,players:saved.players.length,remaining:saved.players[0].setupRemaining});};
    tx.onerror=tx.onabort=()=>{db.close();reject(tx.error??new Error('Lettura fixture interrotta'));};
   };
  }));
  assert.deepEqual(loaded,stored,'Il nuovo Worker deve caricare la fixture, non il salvataggio precedente');
  await resumeHome();
  await page.locator(`.game-table[data-phase="${fixture.phase}"]`).waitFor();
  await page.locator('.pause-card').waitFor({state:'detached'});
  assert.equal(await page.locator('.player-card').count(),fixture.players.length);
 }
 // Exercise new reinforcement undo through the real UI and persisted Worker.
 const undo=createGame({players:3,name:'Comandante',difficulties:['medio','medio'],turnSeconds:180,seed:17,manualReinforcement:true,endMode:'countdown'});
 undo.active=0;undo.phase='REINFORCEMENT';undo.round=5;undo.reinforcements=1;undo.players.forEach(p=>p.setupRemaining=0);
 const ownId=undo.board.findIndex(c=>c.owner===0),ownBefore=undo.board[ownId].armies;
 await loadFixture(undo);await clickTerritory(page,ownId);
 await page.locator('#effects > *').waitFor({state:'detached'});
 await page.locator('[data-action="undo-place"]:not([disabled])').waitFor();
 assert.equal(await page.locator('.game-table').getAttribute('data-phase'),'REINFORCEMENT');
 assert.equal(await page.locator(`[data-token="${ownId}"] .tank-number`).textContent(),String(ownBefore+1));
 await page.locator('[data-action="undo-place"]').click();
 await page.waitForFunction(({id,n})=>document.querySelector(`[data-token="${id}"] .tank-number`)?.textContent===String(n),{id:ownId,n:ownBefore});
 await page.locator('#effects > *').waitFor({state:'detached'});
 await clickTerritory(page,ownId);await page.locator('[data-action="next"]:not([disabled])').waitFor();
 await page.locator('[data-action="next"]').click();await page.locator('.game-table[data-phase="ATTACK"]').waitFor();
 assert.equal(await page.locator('[data-action="undo-place"]').count(),0);
 await page.locator('[data-action="objective"]').click();
 await page.locator('[data-action="objective-map"]').click();assert.equal(await page.locator('.objective-points').count(),42);
 const hit=await page.locator('.territory-hit').first().boundingBox();assert.ok(hit.width>=43.9&&hit.height>=43.9);
 await page.screenshot({path:'artifacts/browser/countdown-objective-points.png'});
 for(const n of [4,5]){
  const setup=createGame({players:n,name:'Comandante',difficulties:Array(n-1).fill('medio'),turnSeconds:180,seed:41});
  const owned=setup.board.filter(c=>c.owner===0).length;
  await loadFixture(setup);await completeSetup(setup.players[0].setupRemaining,owned);
 }
 let s=createGame({players:3,name:'Comandante',difficulties:['medio','medio'],turnSeconds:180,seed:17});
 s.round=5;s.active=0;s.phase='ATTACK';s.players.forEach(p=>p.setupRemaining=0);s.board[0]={owner:0,armies:12};s.board[1]={owner:1,armies:1};
 // Select a seed with a conquest on the first roll, without mocking dice in the UI.
 for(let seed=1;seed<1000;seed++){s.rng=seed;if(applyCommand(s,0,{type:'ATTACK',from:0,to:1}).phase==='POST_CONQUEST_MOVE')break;}
 await loadFixture(s);await setPacing(1,'all');
 await clickTerritory(page,0);await clickTerritory(page,1);
 await page.locator('.floating-panel').waitFor();assert.equal(await page.locator('.modal-backdrop').count(),0);
 const panel=await page.locator('.floating-panel').boundingBox();assert.ok(panel.height<=290&&panel.width<=360);
 const zoomReachable=await page.locator('[data-action="zoom-in"]').evaluate(button=>{
  const r=button.getBoundingClientRect();return document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)?.closest('button')===button;
 });
 assert.ok(zoomReachable,'Il pannello di combattimento non deve coprire i controlli zoom');
 // Start the attack with its territories outside the zoomed viewport. Observe
 // every camera mutation, so a temporary zoom-out cannot hide behind a restore.
 for(let i=0;i<4;i++)await page.locator('[data-action="zoom-in"]').click();
 const attackCamera=await page.locator('#world-map').getAttribute('viewBox');
 await page.evaluate(()=>{
  window.__cameraFrames=[document.querySelector('#world-map').getAttribute('viewBox')];
  window.__cameraObserver=new MutationObserver(records=>{
   for(const record of records)if(record.type==='attributes'&&record.target.id==='world-map')window.__cameraFrames.push(record.oldValue);
   const current=document.querySelector('#world-map')?.getAttribute('viewBox');if(current)window.__cameraFrames.push(current);
  });
  window.__cameraObserver.observe(document.querySelector('#scene'),{subtree:true,childList:true,attributes:true,attributeFilter:['viewBox'],attributeOldValue:true});
 });
 await page.locator('[data-action="roll"]').click();await page.locator('.effects-layer.rolling').waitFor();
 assert.equal(await page.locator('.clash-origin').count(),1);assert.equal(await page.locator('.clash-impact').count(),1);
 const timer=await page.locator('#timer').innerText();await wait(100);assert.equal(await page.locator('#timer').innerText(),timer);
 assert.equal(await page.locator('.attack-vector').count(),1);
 assert.equal(await page.locator('.fx-source').count(),1);assert.equal(await page.locator('.fx-target').count(),1);
 await page.screenshot({path:'artifacts/browser/dice-rolling.png'});
 await page.locator('#map-fx.impact-now').waitFor();await page.screenshot({path:'artifacts/browser/clash.png'});
 await page.locator('[data-action="confirm-move"]').waitFor();
 const attackFrames=await page.evaluate(()=>{window.__cameraObserver.disconnect();return [...window.__cameraFrames,document.querySelector('#world-map').getAttribute('viewBox')];});
 assert.ok(attackFrames.length>1);assert.ok(attackFrames.every(frame=>frame===attackCamera),'Nessun rimbalzo della camera durante attacco/conquista');
 await page.locator('[data-action="reset-map"]').click();
 const move=page.locator('.move-panel');
 const moveRect=await move.boundingBox();assert.ok(moveRect.width<=290&&moveRect.height<=260);
 assert.equal(await move.evaluate(el=>el.scrollHeight<=el.clientHeight+1),true,'Tutti i controlli di spostamento devono essere visibili senza scorrere');
 assert.equal(await page.locator('#move-range').inputValue(),'0');
 assert.equal(await page.locator('[data-token="1"] .tank-number').textContent(),'3');
 assert.equal(await page.locator('.attack-vector').count(),0,'Il vettore termina insieme alla presentazione');
 await page.screenshot({path:'artifacts/browser/post-conquest.png'});
 // Preview additional armies without mutating the stored board.
 await page.locator('[data-move="+1"]').click();
 assert.equal(await page.locator('[data-token="1"] .tank-number').textContent(),'4');
 await page.locator('[data-move="Min"]').click();
 assert.equal(await page.locator('[data-token="1"] .tank-number').textContent(),'3');
 await page.locator('[data-action="confirm-move"]').click();
 await page.waitForSelector('.game-table[data-phase="ATTACK"]');
 await page.locator('[data-action="next"]').click();await page.waitForSelector('.game-table[data-phase="FORTIFY"]');
 await page.locator('[data-action="next"]').click();await page.locator('.card-reward').waitFor();await wait(1100);
 await page.screenshot({path:'artifacts/browser/card-reward.png'});
 await page.waitForFunction(()=>!document.querySelector('.card-reward'));
 // Pause is always available; opening cards while paused must still render their real artwork.
 await pauseGame();await page.locator('.pause-card').waitFor();
 await page.reload();await resumeHome();await page.locator('.game-table').waitFor();
 await page.locator('[data-action="cards"]').click();await page.getByText('LE TUE CARTE',{exact:true}).waitFor();
 assert.equal(await page.locator('.territory-card').count(),1);assert.equal(await page.locator('.territory-card .military-icon').count()>=1,true);
 await page.screenshot({path:'artifacts/browser/cards.png'});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'Overflow orizzontale');
 await page.locator('[data-action="close"]').click();
 await page.setViewportSize({width:1365,height:900});await page.screenshot({path:'artifacts/browser/tablet.png'});
 // Interrupt a real roll: persistence already contains its outcome; resume must
 // open the mandatory transfer without replaying dice or losing the result.
 await loadFixture(s);await setPacing(1,'all');
 await clickTerritory(page,0);await clickTerritory(page,1);
 await page.locator('[data-action="roll"]').click();await page.locator('.effects-layer.rolling').waitFor();
 await pauseGame();await page.locator('.pause-card').waitFor();
 assert.equal(await page.locator('.battle-window').count(),0);
 await page.reload();await resumeHome();
 await page.locator('[data-action="confirm-move"]').waitFor();assert.equal(await page.locator('.battle-window').count(),0);
 // A genuine bot attack must not replace or close the consultation DOM.
 const bot=createGame({players:3,name:'Comandante',difficulties:['facile','facile'],turnSeconds:180,seed:17});
 bot.order=[0,1,2];bot.active=1;bot.round=5;bot.phase='ATTACK';bot.players.forEach(p=>p.setupRemaining=0);
 bot.board.forEach(cell=>{cell.owner=0;cell.armies=1;});
 bot.board[1]={owner:1,armies:20};bot.board[objectives[bot.players[0].objective-1].territories.find(id=>id!==1&&!territories[1].neighbors.includes(id))]={owner:2,armies:1};
 bot.players[0].cards=[0,3,6,9,12,15,18];bot.deck=bot.deck.filter(c=>!bot.players[0].cards.includes(c));
 await page.setViewportSize({width:844,height:390});await loadFixture(bot);
 await setPacing(1,'all');
 await page.locator('[data-action="cards"]').click();await page.locator('[data-card="3"]').click();
 await page.evaluate(()=>{window.__cardsNode=document.querySelector('.cards-window');document.querySelector('.hand').scrollTop=30;window.__cardsScroll=document.querySelector('.hand').scrollTop;});
 await page.locator('.battle-window').waitFor();
 assert.equal(await page.evaluate(()=>window.__cardsNode===document.querySelector('.cards-window')),true,'La finestra carte deve rimanere montata');
 assert.equal(await page.locator('[data-card="3"]').getAttribute('aria-pressed'),'true');
 assert.equal(await page.evaluate(()=>document.querySelector('.hand').scrollTop===window.__cardsScroll),true);
 await page.screenshot({path:'artifacts/browser/cards-during-bot-attack.png'});
 // Close and reopen during the same presentation: UI commands must stay available.
 await page.locator('[data-action="close"]').click();await page.locator('.cards-window').waitFor({state:'detached'});
 await page.locator('[data-action="cards"]').click();await page.locator('.cards-window').waitFor();
 await page.locator('.battle-window').waitFor({state:'detached'});
 assert.equal(await page.locator('.cards-window').count(),1);
 await page.locator('[data-action="close"]').click();await page.locator('.cards-window').waitFor({state:'detached'});
 await pauseGame();await page.locator('.pause-card').waitFor();
 // Explicit turn boundary, retaining the same game ID, using the persisted fixture.
 bot.active=2;bot.phase='FORTIFY';bot.revision++;await loadFixture(bot);
 await page.locator('[data-action="cards"]').click();
 await page.waitForSelector('.game-table[data-active="0"]');
 assert.equal(await page.locator('.cards-window').count(),1);
 // Optional map must survive IndexedDB and finish setup with its actual territory count.
 await page.locator('[data-action="close"]').click();
 for(const n of [3,4,5]){
  const polar=createGame({antarctica:true,players:n,name:'Comandante',difficulties:Array(n-1).fill('medio'),turnSeconds:180,seed:41});
  const own=polar.board.filter(c=>c.owner===0).length;
  await loadFixture(polar);assert.equal(await page.locator('[data-territory]').count(),44);
  assert.equal(await page.locator('#world-map').getAttribute('viewBox'),'0 0 1240 815');
  await page.screenshot({path:`artifacts/browser/antarctica-${n}.png`});
  await completeSetup(polar.players[0].setupRemaining,own);
 }
 const polar=createGame({antarctica:true,players:3,name:'Comandante',difficulties:['medio','medio'],turnSeconds:180,seed:7});
 polar.phase='ATTACK';polar.round=5;polar.active=0;polar.players.forEach(p=>p.setupRemaining=0);
 polar.board[42]={owner:0,armies:12};polar.board[43]={owner:1,armies:1};
 for(let seed=1;seed<1000;seed++){polar.rng=seed;if(applyCommand(polar,0,{type:'ATTACK',from:42,to:43}).phase==='POST_CONQUEST_MOVE')break;}
 await loadFixture(polar);await clickTerritory(page,42);await clickTerritory(page,43);
 await page.locator('[data-action="roll"]').click();await page.locator('#map-fx.impact-now').waitFor();
 await page.screenshot({path:'artifacts/browser/antarctica-clash.png'});
 await page.locator('[data-action="confirm-move"]').waitFor();
 assert.equal(await page.locator('[data-token="43"] .tank-number').textContent(),'3');
 await page.locator('[data-action="confirm-move"]').click();
 console.log('PASS: home, strategie personalizzate; Antartide 3/4/5, ripresa e conquista polare.');
 assert.deepEqual(errors,[]);assert.deepEqual(external,[]);
 console.log('PASS: setup 3/4/5 completo via tocchi, IndexedDB/ripresa anche durante i dadi, obiettivo, finestre compatte, dadi, timer sospeso, conquista, spostamento, pesca e carte; carte persistenti durante attacchi e cambio turno, trasferimento immediato e anteprima aggiuntivi; telefono/tablet; zero richieste esterne.');
}catch(error){
 if(page){
  const info=await browserDiagnostics(page).catch(e=>({diagnosticsError:String(e)}));
  console.error('Stato al fallimento:',JSON.stringify(info,null,2));
  await writeFile('artifacts/browser/failure-state.json',JSON.stringify(info,null,2)).catch(()=>{});
 }
 await page?.screenshot({path:'artifacts/browser/failure.png',fullPage:true}).catch(()=>{});
 throw error;
}finally{
 if(page)await page.context().tracing.stop({path:'artifacts/browser/trace.zip'}).catch(()=>{});
 await page?.context().close();await browser?.close();server.kill('SIGTERM');
}
