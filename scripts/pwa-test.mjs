import {spawn} from 'node:child_process';
import {mkdir} from 'node:fs/promises';
import {setTimeout as wait} from 'node:timers/promises';
import assert from 'node:assert/strict';
import {chromium,webkit,devices} from 'playwright';
import {createGame,assertState} from '../src/engine/RisikoRulesEngine.ts';
import {parseBackup} from '../src/services/Backup.ts';
import {clickTerritory} from './browser-input.mjs';
await mkdir('artifacts/pwa',{recursive:true});
const server=spawn(process.execPath,['scripts/serve-pwa.mjs'],{stdio:'pipe'});let logs='';server.stdout.on('data',x=>logs+=x);server.stderr.on('data',x=>logs+=x);
const base='http://127.0.0.1:4174/risiko/';
let browser;
try{
 let ready=false;for(let i=0;i<60;i++){if(server.exitCode!==null)throw Error(logs);try{if((await fetch(base)).ok){ready=true;break;}}catch{}await wait(100);}assert.ok(ready);
 for(const [name,engine,ios] of [['chromium',chromium,false],['webkit-iphone',webkit,true]]){
  browser=await engine.launch({headless:true,...(!ios&&process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
  const context=await browser.newContext(ios?{...devices['iPhone 13']}:{viewport:{width:844,height:390}});
  await context.tracing.start({screenshots:true,snapshots:true});const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  // No app Worker is running while the fixture transaction is written.
  await page.goto(base+'icon.svg');
  const state=createGame({players:3,name:'PWA Test',difficulties:['medio','survival'],turnSeconds:600,seed:18,manualReinforcement:true});
  state.active=0;state.phase='REINFORCEMENT';state.reinforcements=2;state.round=5;state.paused=true;state.players.forEach(p=>p.setupRemaining=0);state.board[22]={owner:0,armies:2};assertState(state);
  await page.evaluate(s=>new Promise((resolve,reject)=>{
   const request=indexedDB.open('risiko-offline',1);request.onupgradeneeded=()=>{for(const key of ['game','profile','results'])if(!request.result.objectStoreNames.contains(key))request.result.createObjectStore(key);};request.onerror=()=>reject(request.error);
   request.onsuccess=()=>{const db=request.result,tx=db.transaction('game','readwrite');tx.objectStore('game').put(s,'current');tx.oncomplete=()=>{db.close();resolve();};tx.onerror=()=>reject(tx.error);};
  }),state);
  await page.goto(base);
  if(ios)await page.locator('#game-frame').waitFor();
  const game=ios?await page.locator('#game-frame').elementHandle().then(el=>el.contentFrame()):page;
  // Use a real touch in the outer viewport, accounting for the rotated iframe.
  // No forced clicks or event dispatch through an obstructing panel.
  async function tap(locator,offset){
   await locator.waitFor({state:'visible'});
   if(ios&&await page.locator('#web-stage.assisted').count()){
    const point=await locator.evaluate((el,offset)=>{const r=el.getBoundingClientRect();return {x:r.x+(offset?.x??r.width/2),y:r.y+(offset?.y??r.height/2)};},offset);
    const frameBounds=await page.locator('#game-frame').evaluate(el=>{const r=el.getBoundingClientRect();return {right:r.right,top:r.top};});
    await page.touchscreen.tap(frameBounds.right-point.y,frameBounds.top+point.x);
   }else await locator.click(offset?{position:offset}:undefined);
  }
  await game.locator('.portal-resume').waitFor();
  await game.locator('[data-action="install"]').click();await game.getByText(/Gioco pronto offline/).waitFor({timeout:30000});await tap(game.locator('[data-action="close"]'));
  await game.locator('.portal-resume').click();await game.locator('.pause-card').waitFor();await tap(game.locator('[data-action="resume"]'));await game.locator('.game-table[data-phase="REINFORCEMENT"]').waitFor();
  if(ios){await page.locator('#web-stage.assisted').waitFor();assert.ok(await game.evaluate(()=>innerWidth>innerHeight));assert.equal(await game.locator('.portrait-notice').isVisible(),false);}
  await clickTerritory(game,22,tap);await game.waitForFunction(()=>document.querySelector('[data-token="22"] .tank-number')?.textContent==='3');await game.locator('#effects > *').waitFor({state:'detached'});
  const before=await game.locator('#world-map').getAttribute('viewBox');await tap(game.locator('[data-action="zoom-in"]'));assert.notEqual(await game.locator('#world-map').getAttribute('viewBox'),before);
  await tap(game.locator('[data-action="cards"]'));await game.getByText('LE TUE CARTE',{exact:true}).waitFor();await tap(game.locator('[data-action="close"]'));
  await page.screenshot({path:`artifacts/pwa/${name}-board.png`});
  await tap(game.locator('[data-action="game-menu"]'));await tap(game.locator('[data-action="pause"]'));await tap(game.locator('.pause-card [data-action="home"]'));await game.locator('.atlas-home').waitFor();
  if(ios){await page.locator('#web-stage:not(.assisted)').waitFor();assert.ok(await game.evaluate(()=>innerHeight>innerWidth));}
  await game.locator('[data-action="backup"]').click();await game.locator('[data-action="export-backup"]').click();await game.locator('.backup-download').waitFor();
  const raw=await game.locator('.backup-download').evaluate(a=>fetch(a.href).then(r=>r.text()));const backup=parseBackup(raw);assert.equal(backup.game.board[22].armies,3);
  await game.locator('#backup-file').setInputFiles({name:'invalid.json',mimeType:'application/json',buffer:Buffer.from('{}')});await game.getByText(/Backup non valido/).waitFor();assert.equal(await game.locator('[data-action="confirm-import"]').count(),0);
  await game.locator('#backup-file').setInputFiles({name:'backup.json',mimeType:'application/json',buffer:Buffer.from(raw)});await game.locator('[data-action="confirm-import"]').click();await game.locator('.atlas-home').waitFor();
  await context.setOffline(true);await page.reload();
  const offlineGame=ios?await page.locator('#game-frame').elementHandle().then(el=>el.contentFrame()):page;
  await offlineGame.locator('.portal-resume').waitFor();await offlineGame.locator('.portal-resume').click();await offlineGame.locator('.game-table').waitFor();
  assert.equal(await offlineGame.locator('[data-token="22"] .tank-number').textContent(),'3');
  assert.equal(await offlineGame.evaluate(()=>fetch('./audio/clash.wav').then(r=>r.status)),200);
  assert.deepEqual(errors,[]);await context.tracing.stop({path:`artifacts/pwa/${name}-trace.zip`});await context.close();
  if(!ios){
   const native=await browser.newContext({viewport:{width:844,height:390}});
   await native.addInitScript(()=>{window.nativeOrientationCalls=[];window.Capacitor={isNativePlatform:()=>true,nativePromise:async(plugin,method,options)=>{window.nativeOrientationCalls.push({plugin,method,options});}};});
   const p=await native.newPage();await p.goto(base);await p.locator('.atlas-home').waitFor();assert.equal(await p.locator('iframe').count(),0);assert.equal(await p.locator('[data-action="install"]').count(),0);assert.equal(await p.evaluate(()=>navigator.serviceWorker.controller),null);assert.equal(await p.evaluate(()=>window.nativeOrientationCalls[0].plugin),'GameOrientation');await native.close();
  }
  await browser.close();browser=null;console.log(`${name}: offline, Worker, audio, backup e orientamento verificati.`);
 }
}finally{await browser?.close();server.kill();}
