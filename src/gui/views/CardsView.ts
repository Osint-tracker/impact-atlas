import {allTerritories as territories,allCards as cards} from '../../data/mapGraph.ts';
import {exchangeValue} from '../../engine/RisikoRulesEngine.ts';
import {escapeHtml as escape,armyToken,cardFace} from '../art.ts';
import {icon} from '../Icons.ts';
import type {GameView} from '../../engine/types.ts';
export function cardsView(s:GameView,human:boolean,cardSelection:number[],pendingCommand:boolean){

      const hand=s!.players[s!.viewer].cards??[], allowed=human&&s!.phase==='TRIS_EXCHANGE';
      cardSelection=cardSelection.filter(id=>hand.includes(id));
      const bonuses=cardSelection.flatMap(id=>{const t=cards[id].territory;return t!==null&&s.board[t].owner===s.viewer?[territories[t].name]:[];});
      const value=exchangeValue(s!,s!.viewer,cardSelection);
      return`<h2>LE TUE CARTE</h2><p class="card-instructions">Seleziona 3 CARTE compatibili<br>per ottenere ARMATE BONUS</p><span class="sr-only">Carte territorio</span><div class="hand">${hand.map(id=>`<button class="territory-card ${cardSelection.includes(id)?'chosen':''}" data-card="${id}" aria-pressed="${cardSelection.includes(id)}" aria-label="${escape(cards[id].territory===null?'Jolly':territories[cards[id].territory!].name)} · ${cards[id].symbol}">${cardFace(id)}</button>`).join('')||'<div class="empty-state">NON HAI CARTE<br><small>Conquista un territorio per pescare a fine turno.</small></div>'}</div><div class="tris-tray">${Array.from({length:3},(_,i)=>cardSelection[i]!==undefined?`<button class="tris-slot" data-card="${cardSelection[i]}" aria-label="Rimuovi carta ${i+1}">${escape(cards[cardSelection[i]].territory===null?'Jolly':territories[cards[cardSelection[i]].territory!].name)} ${icon('close')}</button>`:'<span class="tris-slot">Seleziona carta</span>').join('')}</div><div class="card-bonus"><small>DOTAZIONE AGGIUNTIVA:</small>${armyToken(value,s!.players[s!.viewer].color)}<strong>${value?'+'+value:'—'}</strong></div>${value&&bonuses.length?`<p class="card-owned-bonus">Inclusi ${bonuses.map(name=>`${escape(name)} +2`).join(' · ')}</p>`:''}<button class="primary full" data-action="exchange-selected" ${allowed&&value>0&&!pendingCommand?'':'disabled'}>Cambia tris</button><p class="card-hint">${allowed?(cardSelection.length===3&&!value?'Queste tre carte non formano un tris.':'3 uguali: 8 · 3 diversi: 10 · Jolly + 2 uguali: 12 · +2 per territorio posseduto'):'Puoi cambiare un tris soltanto all’inizio del tuo turno.'}</p>`;

    
}
