import {allTerritories as territories} from '../../data/mapGraph.ts';
import {canAttack,moveBounds} from '../../engine/RisikoRulesEngine.ts';
import {conquestOdds} from '../../ai/BotAI.ts';
import {conquestEstimate} from '../../ai/Strategy.ts';
import {escapeHtml as e,dieMarkup} from '../art.ts';
import {icon} from '../Icons.ts';
import type {GameView} from '../../engine/types.ts';
function silhouette(id:number){
 const t=territories[id],coords=t.path.match(/-?\d+(?:\.\d+)?/g)!.map(Number),xs=coords.filter((_,i)=>i%2===0),ys=coords.filter((_,i)=>i%2===1);
 const x=Math.min(...xs)-3,y=Math.min(...ys)-3,w=Math.max(...xs)-x+3,h=Math.max(...ys)-y+3;
 return `<svg class="combat-country" viewBox="${x} ${y} ${w} ${h}" aria-hidden="true"><path d="${t.path}"/></svg>`;
}
export function combatDock(s:GameView,selected:number,target:number,pending:boolean,stop:number,blitz:boolean){
 const from=territories[selected],to=territories[target],a=s.board[selected].armies,d=s.board[target].armies;
 const last=s.lastBattle?.from===selected&&s.lastBattle.to===target?s.lastBattle:null,legal=canAttack(s,selected,target);
 return `<h2 class="combat-header sr-only">${icon('swords')}Attacco</h2><div class="combat-route"><span>${silhouette(selected)}<span>Da: <b>${e(from.name)}</b></span></span>${icon('move')}<span>${silhouette(target)}<span>A: <b>${e(to.name)}</b></span></span></div><div class="combat-metrics"><div><label>Soglia Stop Blitz</label><div class="blitz-stepper"><button data-blitz-step="-1" aria-label="Riduci soglia">${icon('minus')}</button><strong id="blitz-limit">${stop}</strong><button data-blitz-step="1" aria-label="Aumenta soglia">${icon('plus')}</button></div><small>${a} carri · ${Math.min(3,a-1)} dadi</small></div><div class="combat-chance">${icon('shield')}<b>${d} difensori</b><span>Conquista: <b>${Math.round(conquestOdds(a,d)*100)}%</b></span><small>Superstiti attesi: ${conquestEstimate(a,d).survivors.toFixed(1)}</small></div></div><div class="combat-preview">${last?`<div aria-label="Dadi attaccante">${last.attack!.map((n,i)=>dieMarkup(n,'#d93829',i)).join('')}</div><span>vs</span><div aria-label="Dadi difensore">${last.defense!.map((n,i)=>dieMarkup(n,'#F2E7D3',i)).join('')}</div>`:'Dadi rossi contro avorio · pareggi alla difesa'}</div><div class="combat-actions"><button class="primary ${blitz?'active':''}" data-action="blitz" ${!legal?'disabled':''}>${icon('bolt')}${blitz?'Ferma Blitz':'Blitz'}</button><button class="secondary" data-action="roll" ${pending||!legal?'disabled':''}>${icon('swords')}Attacca 1 volta</button></div>`;
}
export function moveDock(s:GameView,from:number,to:number,count:number){
 const bounds=moveBounds(s,from,to);count=Math.max(bounds.min,Math.min(bounds.max,count));const conquest=s.phase==='POST_CONQUEST_MOVE';
 return {count,content:`<div class="move-heading"><h2 class="sr-only">${icon('move')}${conquest?'Territorio conquistato':'Spostamento'}</h2><p>${e(territories[from].name)} → ${e(territories[to].name)}</p></div><label class="move-label" for="move-range">${conquest?`Aggiuntivi (${s.pending!.transferred??0} carri già trasferiti)`:'Carri da spostare'} <strong id="move-number">${count}</strong></label><input type="range" id="move-range" min="${bounds.min}" max="${bounds.max}" value="${count}" aria-label="${conquest?'Carri aggiuntivi':'Armate da spostare'}"/><div class="move-shortcuts">${['Min','−1','+1','+5','Max'].map(n=>`<button class="secondary" data-move="${n}" aria-label="${n==='Min'?'Minimo':n==='Max'?'Massimo':n}">${n}</button>`).join('')}</div><button class="primary full" data-action="confirm-move">Conferma spostamento</button>`};
}
