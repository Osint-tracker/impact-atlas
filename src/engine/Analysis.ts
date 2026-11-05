import {conquestOdds,diceDistribution} from '../ai/BotAI.ts';
import {forcingAttacks} from '../ai/Tactics.ts';
import {project} from './RisikoRulesEngine.ts';
import {mapFor} from '../data/mapGraph.ts';
import type {GameState,Command} from './types.ts';
/** Records the information available to the acting player, never another objective. */
export function recordAnalysis(before:GameState,after:GameState,command:Command){
 const entries:{round:number;player:number;message:string}[]=[],map=mapFor(before),name=before.players[before.active].name;
 if(command.type==='ATTACK'){
  const from=before.board[command.from],to=before.board[command.to],odds=conquestOdds(from.armies,to.armies);
  const battle=after.log.slice(before.log.length).find(e=>e.type==='onDiceRolled');
  if(odds<.5)entries.push({round:before.round,player:before.active,message:`${name}: attacco rischioso ${map.territories[command.from].name} → ${map.territories[command.to].name}, conquista stimata ${Math.round(odds*100)}%.`});
  if(battle?.attack&&battle.defense&&battle.losses){const probability=diceDistribution(battle.attack.length,battle.defense.length).find(o=>o.attacker===battle.losses![0]&&o.defender===battle.losses![1])?.probability??1;
   if(probability<.15&&battle.losses[0]>battle.losses[1])entries.push({round:before.round,player:before.active,message:`${name}: lancio sfavorevole a ${map.territories[command.to].name}, probabilità di queste perdite ${(probability*100).toFixed(1)}%.`});
  }
 }
 if(command.type==='NEXT'&&before.phase==='ATTACK'){
  const missed=forcingAttacks(project(before,before.active)).slice(0,2);
  for(const a of missed)entries.push({round:before.round,player:before.active,message:`${name}: occasione non sfruttata ${map.territories[a.from].name} → ${map.territories[a.to].name}, ${Math.round(a.win*100)}% di conquista; ${a.reason}.`});
 }
 if(entries.length)after.analysis=[...(before.analysis??[]),...entries];
}
