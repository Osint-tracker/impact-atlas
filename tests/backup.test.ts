import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createGame,applyCommand,owned} from '../src/engine/RisikoRulesEngine.ts';
import {makeBackup,parseBackup} from '../src/services/Backup.ts';
import {StorageService} from '../src/services/StorageService.ts';
const game=(antarctica=false)=>createGame({players:3,name:'Backup',difficulties:['medio','survival'],turnSeconds:180,seed:18,antarctica,endMode:'countdown'});
const profile={name:'Backup',results:[{id:'old',name:'Backup',date:100,rank:1,points:100,reason:'risiko',duration:1000.625,players:3,dice:[1,2,3,4,5,6]}]};
test('backup: stato completo classico/Antartide, profilo, RNG, carte segrete e Undo sopravvivono al giro JSON',()=>{
 for(const polar of [false,true]){
  let s=game(polar);s.active=0;s.phase='REINFORCEMENT';s.round=5;s.reinforcements=4;s.players.forEach(p=>p.setupRemaining=0);
  s.elapsedMs=12.375;s.remainingMs-=12.375;
  s=applyCommand(s,0,{type:'PLACE',territory:owned(s,0)[0],count:2});
  const backup=parseBackup(JSON.stringify(makeBackup(s,profile)));assert.deepEqual(backup.game,s);assert.deepEqual(backup.profile,profile);
  assert.deepEqual(applyCommand(backup.game!,0,{type:'UNDO_PLACE'}),applyCommand(s,0,{type:'UNDO_PLACE'}));
 }
 assert.equal(parseBackup(JSON.stringify(makeBackup(null,profile))).game,null);
});
test('backup: file errati, versioni nuove, dati strutturalmente corrotti e prototipi vengono rifiutati',()=>{
 const original=makeBackup(game(),profile);
 for(const change of [
  (b:any)=>b.version=99,(b:any)=>b.game.active=9,(b:any)=>b.game.order=['0','1','2'],
  (b:any)=>b.game.players[1].human='yes',(b:any)=>b.game.board[0].armies=-1,
  (b:any)=>b.game.players[1].color='red" onload="bad', (b:any)=>b.game.phase='UNKNOWN',
  (b:any)=>b.profile.results[0].rank=10,(b:any)=>b.game.deck=[],(b:any)=>b.game.log=[{type:'onDiceRolled',player:0,message:'',attack:[8]}]
 ]){const b=structuredClone(original);change(b);assert.throws(()=>parseBackup(JSON.stringify(b)));}
 assert.throws(()=>parseBackup('{not json'));assert.throws(()=>parseBackup('{"__proto__":{}}'));assert.throws(()=>parseBackup(JSON.stringify(original).replace('"elapsedMs":0','"elapsedMs":1e999')));
 assert.throws(()=>parseBackup(' '.repeat(12*1024*1024+1)));
});
test('backup: ripristino atomico conserva gioco e statistiche precedenti se la transazione viene annullata',async()=>{
 let saved:any={game:{current:'old game'},profile:{name:'Old'},results:{old:'old result'}},fail=false;
 const storage=new StorageService(),requests:string[][]=[];
 (storage as any).open=async()=>({transaction(stores:string[],mode:string){
  assert.equal(mode,'readwrite');requests.push(stores);
  const staged=structuredClone(saved),tx:any={error:new Error('Quota simulata'),objectStore(name:string){return {put(value:any,key:string){staged[name][key]=structuredClone(value);},delete(key:string){delete staged[name][key];},clear(){staged[name]={};}};}};
  queueMicrotask(()=>{if(fail)tx.onabort();else{saved=staged;tx.oncomplete();}});return tx;
 }});
 const backup=makeBackup(game(true),profile),before=structuredClone(saved);fail=true;
 await assert.rejects(storage.restoreBackup(backup),/Quota/);assert.deepEqual(saved,before);
 fail=false;await storage.restoreBackup(backup);assert.equal(saved.game.current.paused,true);assert.equal(saved.game.current.antarctica,true);assert.equal(saved.profile.name,profile.name);assert.deepEqual(Object.keys(saved.results),['old']);assert.equal(saved.results.old.points,100);
 await storage.restoreBackup(makeBackup(null,{name:'Empty',results:[]}));assert.equal(saved.game.current,undefined);assert.deepEqual(saved.results,{});
 assert.deepEqual(requests[0],['game','profile','results','slots','profiles','scenarios']);
});
