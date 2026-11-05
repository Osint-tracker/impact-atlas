import type {GameState} from './types.ts';
import {objectivePoints,mapFor} from '../data/mapGraph.ts';
export type TurnSnapshot={round:number;actor:number;revision:number;board:[number,number][];points:number[];logIndex:number};
export function captureTurn(s:GameState):TurnSnapshot{return {round:s.round,actor:s.active,revision:s.revision,board:s.board.map(c=>[c.owner,c.armies]),points:s.players.map(p=>objectivePoints(p.objective,s.board.flatMap((c,i)=>c.owner===p.id?[i]:[]))),logIndex:s.log.length};}
export function recordTurn(before:GameState,after:GameState){
 if(before.phase==='SETUP_PLACEMENT'&&after.phase==='SETUP_PLACEMENT')return;
 if(before.active!==after.active||after.phase==='GAME_OVER'&&before.phase!=='GAME_OVER'||before.phase==='SETUP_PLACEMENT'&&after.phase!=='SETUP_PLACEMENT'){
  after.replay=[...(before.replay?.length?before.replay:[captureTurn(before)]),captureTurn(after)];
 }
}
export function analyzeGame(s:Pick<GameState,'replay'|'log'|'antarctica'> & {players:{id:number}[]}){
 const history=s.replay??[];let swing={delta:0,round:0,actor:0};
 for(let i=1;i<history.length;i++)for(let p=0;p<s.players.length;p++){const delta=history[i].points[p]-history[i-1].points[p];if(delta>swing.delta)swing={delta,round:history[i].round,actor:p};}
 const losses=s.log.filter(e=>e.action==='conquer');
 return {swing,conquests:losses.length,exposed:history.flatMap(row=>mapFor(s).continents.flatMap(c=>{const ts=mapFor(s).territories.filter(t=>t.continent===c.id),owner=row.board[ts[0].id][0];if(!ts.every(t=>row.board[t.id][0]===owner))return [];const weak=ts.filter(t=>row.board[t.id][1]===1&&t.neighbors.some(n=>row.board[n][0]!==owner));return weak.length?[{round:row.round,owner,continent:c.name,territories:weak.map(t=>t.name)}]:[]}))};
}
