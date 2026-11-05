import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createGame,applyCommand,project,owned,assertState,validTris,canAttack} from '../src/engine/RisikoRulesEngine.ts';
import {PrivacyGate} from '../src/engine/Privacy.ts';
import {seedNumber,matchCode,readMatchCode} from '../src/services/MatchCode.ts';
import {curatedScenarios,scenarioGame} from '../src/services/Scenarios.ts';
import {tutorialGame,tutorialComplete} from '../src/engine/Tutorial.ts';
import {mapFor} from '../src/data/mapGraph.ts';
import {StorageService} from '../src/services/StorageService.ts';
import {makeBackup,parseBackup} from '../src/services/Backup.ts';
import {withDecisionBudget,budgetExpired} from '../src/ai/Budget.ts';
import {humanDecision} from '../src/ai/HumanBot.ts';
import type {GameConfig,GameState} from '../src/engine/types.ts';
const config:GameConfig={players:3,name:'Marco',difficulties:['medio','difficile'],turnSeconds:180,seed:517,manualReinforcement:true};
function archive(){
 let rows:Record<string,Record<string,any>>=Object.fromEntries(['game','profile','results','slots','profiles','scenarios'].map(n=>[n,{}]));let fail=false;
 const service=new StorageService();
 (service as any).open=async()=>({transaction(names:string|string[],mode='readonly'){
  const data=structuredClone(rows),tx:any={error:Error('Quota simulata'),objectStore(name:string){assert.ok((Array.isArray(names)?names:[names]).includes(name));return {
   get(key:string){const request:any={};queueMicrotask(()=>{request.result=structuredClone(data[name][key]);request.onsuccess?.();});return request;},
   getAll(){const request:any={};queueMicrotask(()=>{request.result=Object.values(data[name]);request.onsuccess?.();});return request;},
   put(value:any,key:string){data[name][key]=structuredClone(value);},delete(key:string){delete data[name][key];},clear(){data[name]={};}
  };}};
  setTimeout(()=>{if(fail&&mode==='readwrite')tx.onabort?.();else{if(mode==='readwrite')for(const name of Array.isArray(names)?names:[names])rows[name]=data[name];tx.oncomplete?.();}},0);return tx;
 }});
 return {service,rows:()=>rows,fail:(v:boolean)=>fail=v};
}
test('Codice Partita: seed alfanumerico stabile, configurazione riproducibile e convalida completa',()=>{
 assert.equal(seedNumber('  risiko-bologna-42 '),seedNumber('risiko-bologna-42'));
 assert.equal(seedNumber('42'),42);assert.equal(seedNumber('0'),1);
 const c={...config,seed:seedNumber('risiko-bologna-42'),seedLabel:'risiko-bologna-42',antarctica:true};
 const decoded=readMatchCode(matchCode(c))!;assert.deepEqual(decoded,c);
 const a=createGame(c),b=createGame(decoded);assert.deepEqual(a.board,b.board);assert.deepEqual(a.order,b.order);assert.deepEqual(a.players.map(p=>p.objective),b.players.map(p=>p.objective));assert.equal(a.rng,b.rng);
 assert.equal(readMatchCode('solo-un-seed'),null);
 const shared=matchCode({...c,participants:[{name:'Marco',human:true,profileId:'private-device-id'},{name:'Anna',human:true,profileId:'private-anna'},{name:'Bot',human:false}]});assert.ok(!shared.includes('private-device-id'));assert.deepEqual(readMatchCode(shared)!.participants!.map(p=>p.profileId),['local','','']);
 for(const changes of [{players:2},{turnSeconds:1},{seed:-1},{participants:[{human:'true'}]}])assert.throws(()=>readMatchCode(matchCode({...c,...changes} as any)));
});
test('Hot-seat: il sipario richiede identità e token, proiezione limitata al ricevente anche con umano non zero',()=>{
 const s=createGame({...config,participants:[{name:'Bot',human:false,difficulty:'survival'},{name:'Marco',human:true,profileId:'marco'},{name:'Anna',human:true,profileId:'anna'}]});
 s.active=1;const gate=new PrivacyGate();gate.reset(s);assert.equal(gate.pending?.player,1);
 assert.throws(()=>gate.reveal(2,gate.pending!.token));assert.throws(()=>gate.reveal(1,'stale'));
 gate.reveal(1,gate.pending!.token);assert.equal(gate.pending,null);
 let view=project(s,gate.viewer);assert.equal(view.players[1].objective,s.players[1].objective);assert.equal(view.players[0].objective,undefined);assert.equal(view.players[2].cards,undefined);
 s.active=2;gate.changed(s);assert.equal(gate.pending?.player,2);gate.reveal(2,gate.pending!.token);
 view=project(s,gate.viewer);assert.equal(view.players[1].objective,undefined);assert.equal(view.players[2].objective,s.players[2].objective);
 gate.lock(s);assert.ok(gate.pending);assert.throws(()=>createGame({...config,participants:s.players.map(p=>({...p,human:false}))}));
});
test('Archivio: migrazione v1, slot indipendenti, duplicazione, rinomina, eliminazione e rollback',async()=>{
 const a=archive(),s=createGame(config);s.version=1;delete s.players[0].profileId;
 await a.service.put('game','current',s);const migrated=await a.service.load();assert.equal(migrated?.version,2);assert.equal(migrated?.players[0].profileId,'local');assert.equal((await a.service.slots()).length,1);
 await a.service.editSlot(s.id,'duplicate');const copy=(await a.service.slots()).find(x=>x.id!==s.id)!;assert.ok(copy);assert.equal(copy.state.paused,true);
 await a.service.editSlot(copy.id,'rename','Prova');assert.equal((await a.service.get<any>('slots',copy.id)).name,'Prova');
 await a.service.selectSlot(copy.id);assert.equal((await a.service.load())?.id,copy.id);
 await a.service.editSlot(s.id,'delete');assert.equal((await a.service.load())?.id,copy.id);
 const backup=makeBackup(createGame({...config,seed:2}),{name:'Nuovo',results:[]}),before=structuredClone(a.rows());a.fail(true);
 await assert.rejects(a.service.restoreBackup(backup),/Quota/);assert.deepEqual(a.rows(),before);a.fail(false);
 await a.service.restoreBackup(backup,'merge');assert.equal((await a.service.slots()).length,2);assert.equal((await a.service.load())?.id,copy.id);
});
test('Statistiche: risultati distinti per umano; riapertura e copia conclusa non duplicano vittorie',async()=>{
 const a=archive(),s=createGame({...config,participants:[{name:'Marco',human:true,profileId:'local'},{name:'Anna',human:true,profileId:'anna'},{name:'Bot',human:false}]});
 s.phase='GAME_OVER';s.reason='countdown';s.winner=0;s.players.forEach(p=>p.setupRemaining=0);
 await a.service.save(s);const initial=structuredClone(a.rows().results);assert.equal(Object.keys(initial).length,2);
 await a.service.save(s);assert.deepEqual(a.rows().results,initial);
 assert.equal((await a.service.profile('anna')).results.length,1);assert.equal((await a.service.profile('local')).results.length,1);
 await a.service.editSlot(s.id,'duplicate');const copy=(await a.service.slots()).find(x=>x.id!==s.id)!;await a.service.save(copy.state);assert.deepEqual(a.rows().results,initial);
});
test('Scenari: configurazioni 3/4/5 e Antartide valide; rifiuto posizioni oltre i limiti',()=>{
 for(const n of [3,4,5])for(const polar of [false,true])for(const sc of curatedScenarios({...config,players:n,difficulties:Array(n-1).fill('medio'),antarctica:polar})){
  const s=scenarioGame(sc);assertState(s);assert.equal(s.custom,true);assert.equal(s.phase,'TRIS_EXCHANGE');assert.ok(s.reinforcements>=0);
  const broken=structuredClone(sc);broken.board[0].armies=131;assert.throws(()=>scenarioGame(broken));broken.board[0].armies=0;assert.throws(()=>scenarioGame(broken));
 }
});
test('Tutorial: i cinque checkpoint riconoscono i comandi reali e non alterano le regole',()=>{
 for(let step=0;step<5;step++){
  let s=tutorialGame(step),before=s;assertState(s);assert.equal(s.tutorialStep,step);
  if(step===0)s=applyCommand(s,0,{type:'PLACE',territory:owned(s,0)[0],count:1});
  if(step===1)s=applyCommand(s,0,{type:'EXCHANGE',cards:validTris(s,0,s.players[0].cards)[0].cards});
  if(step===2||step===3){const from=owned(s,0).find(id=>s.board[id].armies===12)!,to=mapFor(s).territories[from].neighbors.find(n=>canAttack(s,from,n)&&s.board[n].armies===(step===2?3:1))!;
   for(let i=0;i<30;i++){before=s;s=applyCommand(s,0,{type:'ATTACK',from,to});if(tutorialComplete(before,s))break;}
  }
  if(step===4){const from=owned(s,0).find(id=>s.board[id].armies===8)!,to=mapFor(s).territories[from].neighbors.find(n=>s.board[n].owner===0&&s.board[n].armies===2)!;s=applyCommand(s,0,{type:'MOVE',from,to,count:2});}
  assert.equal(tutorialComplete(before,s),true,`Checkpoint ${step}`);assertState(s);
 }
});
test('Replay e analisi: nascosti durante la partita e disponibili solo ai risultati',()=>{
 let s=createGame(config);s.phase='FORTIFY';s.round=5;s.active=0;s.players.forEach(p=>p.setupRemaining=0);s=applyCommand(s,0,{type:'NEXT'});
 assert.ok(s.replay!.length>=2);assert.equal(project(s).replay,undefined);assert.equal(project(s).analysis,undefined);
 s.phase='GAME_OVER';s.reason='countdown';s.winner=1;assert.ok(project(s).replay);assert.equal(parseBackup(JSON.stringify(makeBackup(s,{name:'Marco',results:[]}))).game?.replay?.length,s.replay!.length);
});
test('Budget IA esaurito: decisione legale e nessuna contaminazione della decisione successiva',()=>{
 const s=createGame(config);s.active=1;s.phase='REINFORCEMENT';s.round=5;s.reinforcements=5;s.players.forEach(p=>p.setupRemaining=0);
 for(const level of ['medio','difficile','survival'] as const){const decision=withDecisionBudget(level,()=>humanDecision(project(s,1),level,()=>.5),0);assert.doesNotThrow(()=>applyCommand(s,1,decision.command));assert.equal(budgetExpired(),false);}
});

test('Comandi sconosciuti e cronologia condivisa: nessuna mutazione dello stato precedente',()=>{
 const before=createGame(config),snapshot=JSON.stringify(before);
 assert.throws(()=>applyCommand(before,before.active,{type:'UNKNOWN'} as any),/Comando non riconosciuto/);
 const after=applyCommand(before,before.active,{type:'PLACE',territory:owned(before,before.active)[0],count:1});
 assert.equal(JSON.stringify(before),snapshot);assert.equal(after.log.length,before.log.length+1);
 const view=project(after);view.board[0].armies=999;view.log[0].message='modified';assert.notEqual(after.board[0].armies,999);assert.notEqual(after.log[0].message,'modified');
});
