import {scenarioGame} from './Scenarios.ts';
import {assertState,upgradeConquestSave} from '../engine/RisikoRulesEngine.ts';
import {colors} from '../data/mapGraph.ts';
import {botPolicy} from '../ai/policy.ts';
import type {GameState} from '../engine/types.ts';
import type {Profile,SaveSlot,LocalProfile} from './StorageService.ts';
export const MAX_BACKUP_BYTES=64*1024*1024;
export type Backup={format:'risiko-offline-backup';version:1|2;createdAt:string;game:GameState|null;profile:Profile;slots?:SaveSlot[];profiles?:LocalProfile[];scenarios?:import('./Scenarios.ts').Scenario[]};
const phases=['SETUP_PLACEMENT','TRIS_EXCHANGE','REINFORCEMENT','ATTACK','POST_CONQUEST_MOVE','FORTIFY','CARD_DRAW','SDADATA_CHECK','GAME_OVER'];
function check(ok:unknown):asserts ok{if(!ok)throw Error('Backup non valido o incompatibile. Nessun salvataggio è stato sostituito.');}
const int=(v:unknown,min=0,max=Number.MAX_SAFE_INTEGER)=>Number.isSafeInteger(v)&&Number(v)>=min&&Number(v)<=max;
const elapsed=(v:unknown)=>typeof v==='number'&&Number.isFinite(v)&&v>=0;
const text=(v:unknown,max=100)=>typeof v==='string'&&v.length<=max;
function safeTree(value:unknown,depth=0){
 check(depth<32);
 if(typeof value==='number')check(Number.isFinite(value));
 if(value&&typeof value==='object')for(const [key,child] of Object.entries(value)){check(!['__proto__','constructor','prototype'].includes(key));safeTree(child,depth+1);}
}
function validateGame(g:GameState){
 check(g&&[1,2].includes(g.version)&&Array.isArray(g.players)&&[3,4,5].includes(g.players.length));
 const player=(id:unknown)=>int(id,0,g.players.length-1),territory=(id:unknown)=>int(id,0,g.antarctica?43:41);
 check(text(g.id,150)&&g.id.length&&phases.includes(g.phase)&&player(g.active));
 for(const key of ['revision','rng','aiRng','round','reinforcements','setupBatch','conquests'] as const)check(int(g[key]));
 check(elapsed(g.remainingMs)&&elapsed(g.elapsedMs));
 check(g.rng<=0xffffffff&&g.aiRng<=0xffffffff&&(int(g.turnSeconds,1,3600)||g.custom===true&&g.turnSeconds===0)&&typeof g.paused==='boolean');
 check(g.endMode===undefined||['regular','countdown'].includes(g.endMode));
 check(g.reason===null||['risiko','sdadata','countdown'].includes(g.reason));
 check(g.winner===null||player(g.winner));check(g.phase!=='GAME_OVER'||g.winner!==null);
 check(g.manualReinforcement===undefined||typeof g.manualReinforcement==='boolean');
 check(g.recordResults===undefined||typeof g.recordResults==='boolean');
 check(g.lastCommandId===null||text(g.lastCommandId,200));
 check(Array.isArray(g.order)&&g.order.length===g.players.length&&g.order.every(player));
 check(new Set(g.players.map(p=>p.objective)).size===g.players.length);
 for(const [id,p] of g.players.entries()){
  check(p.id===id&&(g.version===1?p.human===(id===0):typeof p.human==='boolean')&&typeof p.blind==='boolean'&&text(p.name,100)&&colors.includes(p.color));
  check(['facile','medio','difficile','survival'].includes(p.difficulty)&&int(p.setupRemaining,0,35));
  check(Array.isArray(p.cards)&&p.cards.every(c=>int(c))&&typeof p.eliminated==='boolean');
  check(p.profileId===undefined||text(p.profileId,150));
  if(p.botPolicy!==undefined){check(p.botPolicy&&typeof p.botPolicy==='object');for(const [key,value] of Object.entries(botPolicy(p.botPolicy)))check((p.botPolicy as any)[key]===undefined||(p.botPolicy as any)[key]===value);}
 }
 check(Array.isArray(g.board)&&g.board.every(c=>c&&player(c.owner)&&int(c.armies,0,130))&&Array.isArray(g.deck)&&Array.isArray(g.discard));
 check(g.turnReinforcements===undefined||int(g.turnReinforcements,0,130));
 check(g.deck.every(c=>int(c))&&g.discard.every(c=>int(c)));
 check(g.beliefs&&typeof g.beliefs==='object'&&!Array.isArray(g.beliefs));
 const belief=(v:unknown)=>Array.isArray(v)&&v.length===16&&v.every(n=>typeof n==='number'&&Number.isFinite(n));
 for(const [id,value] of Object.entries(g.beliefs))check(player(Number(id))&&belief(value));
 if(g.pending!==null){check(g.pending&&territory(g.pending.from)&&territory(g.pending.to)&&int(g.pending.min,1,130));}
 if(g.sdadata!==null)check(g.sdadata&&typeof g.sdadata.started==='boolean'&&player(g.sdadata.anchor)&&int(g.sdadata.threshold,4,7));
 const event=(e:any)=>{
  check(e&&['onGameStateUpdate','onDiceRolled','onTurnChanged','onSdadataRoll','onGameOver'].includes(e.type)&&player(e.player)&&text(e.message,4000));
  for(const key of ['from','to','territory'])if(e[key]!==undefined)check(territory(e[key]));
  if(e.defender!==undefined)check(player(e.defender));
  for(const key of ['attack','defense'])if(e[key]!==undefined)check(Array.isArray(e[key])&&e[key].length<=3&&e[key].every((n:number)=>int(n,1,6)));
  if(e.losses!==undefined)check(Array.isArray(e.losses)&&e.losses.length===2&&e.losses.every((n:number)=>int(n,0,3)));
  if(e.count!==undefined)check(int(e.count,-130,130));
 };
 check(Array.isArray(g.log)&&g.log.length<=100000);g.log.forEach(event);
 if(g.lastBattle!==null)event(g.lastBattle);if(g.lastSdadata!==null)event(g.lastSdadata);
 if(g.placementHistory!==undefined){check(Array.isArray(g.placementHistory));for(const row of g.placementHistory)check(territory(row.territory)&&int(row.count,1,130)&&(row.belief===null||belief(row.belief)));}
 if(g.scoreHistory!==undefined){check(Array.isArray(g.scoreHistory));for(const row of g.scoreHistory)check(int(row.round)&&player(row.player)&&Array.isArray(row.points)&&row.points.length===g.players.length&&row.points.every(p=>int(p,0,86)));}
 if(g.botContexts!==undefined){check(g.botContexts&&typeof g.botContexts==='object');for(const [id,c] of Object.entries(g.botContexts)){
  check(player(Number(id))&&c&&typeof c.temperament==='number'&&typeof c.cautious==='boolean');
  for(const key of ['round','turns','lastEpisode','previousArmies','logIndex'] as const)check(int(c[key],-100));
  check(c.lostTerritory===null||territory(c.lostTerritory));
  if(c.episode)check(territory(c.episode.from)&&territory(c.episode.to)&&int(c.episode.initialArmies,1,130)&&int(c.episode.rolls,0,3));
  if(c.plan)check(int(c.plan.round)&&phases.includes(c.plan.phase)&&Array.isArray(c.plan.placements)&&c.plan.placements.length<=130&&c.plan.placements.every(territory));
 }}
 if(g.replay!==undefined){check(Array.isArray(g.replay)&&g.replay.length<=10000);for(const row of g.replay)check(int(row.round)&&player(row.actor)&&int(row.revision)&&int(row.logIndex,0,g.log.length)&&Array.isArray(row.board)&&row.board.length===g.board.length&&row.board.every(c=>Array.isArray(c)&&c.length===2&&player(c[0])&&int(c[1],0,130))&&Array.isArray(row.points)&&row.points.length===g.players.length&&row.points.every(n=>int(n,0,86)));}
 if(g.analysis!==undefined){check(Array.isArray(g.analysis)&&g.analysis.length<=100000);for(const row of g.analysis)check(int(row.round)&&player(row.player)&&text(row.message,4000));}
 assertState(g);
}
export function parseBackup(raw:string):Backup{
 check(typeof raw==='string'&&new TextEncoder().encode(raw).byteLength<=MAX_BACKUP_BYTES);
 let b:any;try{b=JSON.parse(raw);}catch{throw Error('Il file non è un backup JSON valido.');}
 safeTree(b);check(b?.format==='risiko-offline-backup'&&[1,2].includes(b.version)&&text(b.createdAt,40)&&Number.isFinite(Date.parse(b.createdAt)));
 check(b.profile&&text(b.profile.name,100)&&Array.isArray(b.profile.results)&&b.profile.results.length<=10000);
 const ids=new Set<string>();
 for(const r of b.profile.results){
  check(r&&text(r.id,150)&&!ids.has(r.id)&&text(r.name,100)&&int(r.date)&&elapsed(r.duration)&&[3,4,5].includes(r.players)&&int(r.rank,1,r.players)&&int(r.points,0,100)&&['risiko','sdadata','countdown'].includes(r.reason));ids.add(r.id);
  if(r.objectivePoints!==undefined)check(int(r.objectivePoints,0,86));
  if(r.dice!==undefined)check(Array.isArray(r.dice)&&r.dice.length===6&&r.dice.every((n:number)=>int(n)));
  check(r.profileId===undefined||text(r.profileId,150));check(r.category===undefined||['solo','hotseat','custom'].includes(r.category));check(r.difficulties===undefined||Array.isArray(r.difficulties)&&r.difficulties.every((d:string)=>['facile','medio','difficile','survival'].includes(d)));
  if(r.antarctica!==undefined)check(typeof r.antarctica==='boolean');
 }
 if(b.game!==null){validateGame(b.game);b.game=upgradeConquestSave(b.game);}
 if(b.version===2){
  check(Array.isArray(b.slots)&&Array.isArray(b.profiles)&&Array.isArray(b.scenarios));
  const slotIds=new Set();for(const slot of b.slots){check(slot&&text(slot.id,150)&&!slotIds.has(slot.id)&&text(slot.name,80)&&elapsed(slot.updatedAt));slotIds.add(slot.id);validateGame(slot.state);check(slot.id===slot.state.id);}
  const scenarioIds=new Set();for(const sc of b.scenarios){check(text(sc.id,150)&&!scenarioIds.has(sc.id)&&text(sc.description,300));scenarioIds.add(sc.id);scenarioGame(sc);}
  const profiles=new Set();for(const p of b.profiles){check(text(p.id,150)&&!profiles.has(p.id)&&text(p.name,24));profiles.add(p.id);}
 }
 return b as Backup;
}
export function makeBackup(game:GameState|null,profile:Profile):Backup{
 return parseBackup(JSON.stringify({format:'risiko-offline-backup',version:2,createdAt:new Date().toISOString(),game,profile,slots:game?[{id:game.id,name:'Partita importata',updatedAt:Date.now(),state:game}]:[],profiles:[{id:'local',name:profile.name.slice(0,24)}],scenarios:[]}));
}
