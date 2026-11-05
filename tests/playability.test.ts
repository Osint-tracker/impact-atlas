import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createGame,applyCommand,project,total,owned,random,assertState,canAttack} from '../src/engine/RisikoRulesEngine.ts';
import {makeTransition,PresentationGate,effectDuration,sequenceScale} from '../src/engine/presentation.ts';
import {humanDecision,decisionDelay} from '../src/ai/HumanBot.ts';
import {cardFace,dieMarkup,objectiveMap} from '../src/gui/art.ts';
import {effectMarkup} from '../src/gui/effects.ts';
import type {BotContext,Difficulty,GameState} from '../src/engine/types.ts';
const game=(players=3,seed=2)=>createGame({players,name:'Test',difficulties:Array(players-1).fill('medio'),turnSeconds:180,seed});
function active() {
 const s=game();s.phase='ATTACK';s.round=5;s.active=0;s.players.forEach(p=>p.setupRemaining=0);
 s.board[0]={owner:0,armies:9};s.board[1]={owner:1,armies:3};return s;
}
function hand(s:GameState,p:number,ids:number[]){s.players[p].cards=ids;s.deck=s.deck.filter(c=>!ids.includes(c));}
test('setup completo 3/4/5: contatori pubblici, mani da tre, nessuna armata persa',()=>{
 for(const n of [3,4,5])for(let seed=1;seed<=10;seed++){
  let s=game(n,seed);const initial=s.players.map(p=>total(s,p.id)),added=Array(n).fill(0);
  while(s.phase==='SETUP_PLACEMENT'){
   const actor=s.active,batch=s.setupBatch;assert.ok(batch>=1&&batch<=3);
   assert.equal(project(s).players[actor].setupRemaining,s.players[actor].setupRemaining);
   for(let i=0;i<batch;i++){s=applyCommand(s,actor,{type:'PLACE',territory:owned(s,actor)[0],count:1});added[actor]++;if(i<batch-1)assert.equal(s.active,actor);}
  }
  const expected=({3:35,4:30,5:25} as Record<number,number>)[n];
  for(const p of s.players){assert.equal(total(s,p.id),expected);assert.equal(added[p.id],expected-initial[p.id]);assert.equal(p.setupRemaining,0);}
 }
});
test('rinforzi normali: trenta carri in un comando sono consentiti',()=>{
 const s=active();s.phase='REINFORCEMENT';s.reinforcements=30;
 const n=applyCommand(s,0,{type:'PLACE',territory:0,count:30});assert.equal(n.board[0].armies,s.board[0].armies+30);assert.equal(n.phase,'ATTACK');
});
test('transizione attacco conserva origine, dadi reali, perdite e stato precedente',()=>{
 const s=active(),n=applyCommand(s,0,{type:'ATTACK',from:0,to:1}),t=makeTransition(s,n)!;
 assert.equal(t.before.board[0].armies,9);assert.equal(t.after.board[0].armies,n.board[0].armies);
 const e=t.effects[0];assert.equal(e.kind,'battle');if(e.kind!=='battle')return;
 assert.equal(e.from,0);assert.equal(e.to,1);assert.deepEqual(e.attack,n.lastBattle!.attack);assert.deepEqual(e.losses,n.lastBattle!.losses);
 assert.match(effectMarkup(e,t),/Alaska/);assert.match(effectMarkup(e,t),/Territori del Nord Ovest/);
 for(const v of [...e.attack,...e.defense])assert.match(effectMarkup(e,t),new RegExp(`aria-label="Dado ${v}"`));
 assert.equal(effectDuration(e,1),1800);
});
test('pesca: animazione solo dopo conquista; identità carta riservata al destinatario',()=>{
 for(const actor of [0,1])for(const conquests of [0,1]){
  const s=active();s.active=actor;s.phase='FORTIFY';s.conquests=conquests;
  const n=applyCommand(s,actor,{type:'NEXT'}),t=makeTransition(s,n,1,0);
  const e=t?.effects.find(e=>e.kind==='draw');assert.equal(!!e,conquests>0);
  if(e?.kind==='draw'){assert.equal('card'in e,actor===0);if(actor===0)assert.equal(e.card,n.players[0].cards.at(-1));else assert.match(effectMarkup(e,t!),/card-back/);}
 }
 const s=active();s.phase='FORTIFY';s.conquests=1;hand(s,0,[0,1,2,3,4,5,6]);
 const n=applyCommand(s,0,{type:'NEXT'});assert.equal(makeTransition(s,n),null);assert.equal(n.players[0].cards.length,7);
});
test('scambio: solo le proprie carte vengono trasmesse nella presentazione',()=>{
 for(const actor of [0,1]){
  const s=active();s.active=actor;s.phase='TRIS_EXCHANGE';hand(s,actor,[0,3,6]);
  const n=applyCommand(s,actor,{type:'EXCHANGE',cards:[0,3,6]}),t=makeTransition(s,n)!,e=t.effects[0];
  assert.equal(e.kind,'exchange');if(e.kind==='exchange'){assert.equal('cards'in e,actor===0);assert.equal(e.count,n.reinforcements-s.reinforcements);}
 }
});
test('fortificazione, pesca e sdadata mantengono ordine e risultati',()=>{
 const s=active();s.phase='FORTIFY';s.round=20;s.active=s.order.at(-1)!;s.conquests=1;
 s.board[0]={owner:s.active,armies:9};s.board[1]={owner:s.active,armies:2};
 const n=applyCommand(s,s.active,{type:'MOVE',from:0,to:1,count:2}),t=makeTransition(s,n)!;
 assert.deepEqual(t.effects.map(e=>e.kind),['move','draw','sdadata']);
 const e=t.effects[2];if(e.kind==='sdadata')assert.deepEqual(e.dice,n.lastSdadata!.attack);
});
test('watchdog: ACK esatto, duplicati ignorati e scadenza limitata',()=>{
 const g=new PresentationGate();g.begin('a:1',50);assert.equal(g.finish('a:0'),false);assert.equal(g.expired(10049),false);assert.equal(g.expired(10050),true);assert.equal(g.finish('a:1'),true);assert.equal(g.finish('a:1'),false);assert.equal(g.pending,null);
});
test('timeout con molti piazzamenti non supera gli otto secondi di presentazione',()=>{
 const s=active();s.phase='REINFORCEMENT';s.reinforcements=75;
 const n=applyCommand(s,0,{type:'TIMEOUT'}),t=makeTransition(s,n)!;
 assert.ok(t.effects.reduce((v,e)=>v+effectDuration(e,t.speed),0)*sequenceScale(t)<=8000.001);
 assert.equal(t.effects.reduce((sum,e)=>sum+(e.kind==='place'?e.count:0),0),75);
});
test('tempi umani: intervalli contestuali, velocità e limite residuo',()=>{
 const s=active(),v=project(s);const next={type:'PLACE' as const,territory:0,count:1};v.phase='REINFORCEMENT';
 assert.equal(decisionDelay(v,next,null,1,()=>.5),3000);
 assert.equal(decisionDelay(v,next,next,1,()=>.5),650);
 assert.equal(decisionDelay(v,{...next,territory:1},next,1,()=>.5),1500);
 v.phase='POST_CONQUEST_MOVE';assert.equal(decisionDelay(v,next,null,1,()=>.5),6000);
 assert.equal(decisionDelay(v,next,null,1,()=>0),8000);
 assert.equal(decisionDelay(v,next,null,2,()=>.5),3000);
 assert.equal(decisionDelay(v,next,null,0,()=>.5),0);
 v.phase='ATTACK';v.conquests=1;v.board[0].armies=3;v.lastBattle={type:'onDiceRolled',player:0,message:'',from:0,to:1,losses:[0,1]};
 assert.equal(decisionDelay(v,{type:'NEXT'},null,1,()=>.5),6000,'Valuta se fermarsi dopo una conquista con pochi carri');
 v.remainingMs=900;assert.equal(decisionDelay(v,next,null,1,()=>.5),400);
});
test('personalità privata e compatibilità con salvataggi privi del contesto IA',()=>{
 const s=active(),rng={rng:s.aiRng};const d=humanDecision(project(s,0),'medio',()=>random(rng));
 s.botContexts={0:d.context};assertState(s);assert.equal('botContexts'in project(s,1),false);
 const old=structuredClone(s);delete old.botContexts;assert.doesNotThrow(()=>humanDecision(project(old,0),'difficile',()=>.5));
});
test('sortita disperata: pochi rinforzi, tre lanci massimi, cooldown',()=>{
 const s=active();s.board.forEach((c,i)=>{c.owner=i===0?0:i%2+1;c.armies=i===0?5:3;});s.board[1].armies=1;
 s.turnReinforcements=0;const v=project(s);assert.ok(canAttack(s,0,1));
 let context:BotContext={temperament:0,round:4,turns:4,lastEpisode:0,previousArmies:15,cautious:false,lostTerritory:1,logIndex:0,episode:null};
 let d=humanDecision(v,'difficile',()=>0,context);assert.equal(d.command.type,'ATTACK');assert.equal(d.context.episode?.rolls,1);
 context=d.context;
 for(let i=2;i<=3;i++){d=humanDecision(v,'difficile',()=>0,context);context=d.context;assert.equal(context.episode?.rolls,i);}
 d=humanDecision(v,'difficile',()=>0,context);assert.equal(d.context.episode,null);
 v.round=6;d=humanDecision(v,'difficile',()=>0,d.context);assert.equal(d.context.episode,null);
 const safe=project(active());safe.round=4;assert.equal(humanDecision(safe,'facile',()=>0).context.episode,null);
});
test('immagini locali: figure militari, sagome, obiettivi, dadi con pallini',()=>{
 for(const id of [0,1,2,42]){const html=cardFace(id);assert.doesNotMatch(html,/[♜♟♞]/);assert.match(html,id===42?/compass-rose/:/military-icon/);}
 assert.match(cardFace(0),/card-country/);assert.match(cardFace(42),/Jolly/);assert.match(objectiveMap(10),/objective-id">10/);
 assert.equal((dieMarkup(6,'#123456').match(/class="die-face/g)??[]).length,6);
});
test('nove partite con personalità: mosse legali e conclusione a tutte le difficoltà',()=>{
 for(const n of [3,4,5])for(const level of ['facile','medio','difficile'] as Difficulty[]){
  let s=game(n,417+n),steps=0;
  while(s.phase!=='GAME_OVER'&&steps<20000){
   const rng={rng:s.aiRng},actor=s.active;
   const d=humanDecision(project(s,actor),level,()=>random(rng),s.botContexts?.[actor]);
   const next=applyCommand(s,actor,d.command);next.aiRng=rng.rng;next.botContexts={...s.botContexts,[actor]:d.context};s=next;steps++;
  }
  assert.equal(s.phase,'GAME_OVER',`${n}/${level} dopo ${steps} azioni`);assertState(s);
 }
});
