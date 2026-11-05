import {mapFor} from '../data/mapGraph.ts';
import type {GameState} from './types.ts';
export function summarizeBots(before:GameState,after:GameState,viewer:number):string[]{
 const events=after.log.slice(before.log.length).filter(e=>!after.players[e.player].human),map=mapFor(after),lines:string[]=[];
 for(const p of after.players.filter(p=>!p.human)){
  const captures=events.filter(e=>e.player===p.id&&e.action==='conquer');
  if(captures.length)lines.push(`${p.name}: ${captures.map(e=>map.territories[e.to??e.territory!].name).join(', ')} conquistati.`);
  if(!before.players[p.id].eliminated&&p.eliminated)lines.push(`${p.name} eliminato.`);
 }
 const lost=after.board.flatMap((c,i)=>before.board[i].owner===viewer&&c.owner!==viewer?[map.territories[i].name]:[]);if(lost.length)lines.push(`Hai perso: ${lost.join(', ')}.`);
 for(const c of map.continents){const ts=map.territories.filter(t=>t.continent===c.id),owner=(s:GameState)=>ts.every(t=>s.board[t.id].owner===s.board[ts[0].id].owner)?s.board[ts[0].id].owner:null;const a=owner(before),b=owner(after);if(a!==b){if(a!==null)lines.push(`${before.players[a].name}: bonus ${c.name} interrotto.`);if(b!==null)lines.push(`${after.players[b].name}: controllo ${c.name}, +${c.bonus} rinforzi.`);}}
 const total=(s:GameState)=>s.board.reduce((n,c)=>n+(c.owner===viewer?c.armies:0),0),delta=total(after)-total(before);if(delta)lines.push(`Le tue armate: ${delta>0?'+':''}${delta}.`);
 return lines;
}
