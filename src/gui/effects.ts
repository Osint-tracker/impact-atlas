import {allTerritories as territories} from '../data/mapGraph.ts';
import {effectDuration, sequenceScale} from '../engine/presentation.ts';
import type {Transition, VisualEffect} from '../engine/presentation.ts';
import {conquestOdds} from '../ai/BotAI.ts';
import {armyBadge,tankModel,armyToken, counterFontSize, tankShape, cardFace, dieMarkup, escapeHtml} from './art.ts';

export function effectMarkup(e: VisualEffect, t: Transition) {
  const player = t.before.players[e.player];
  if (e.kind === 'battle') {
    const attack=[...e.attack].sort((a,b)=>b-a),defense=[...e.defense].sort((a,b)=>b-a);
    return `<section class="battle-window fx-window" data-floating role="status" aria-label="Combattimento"><header data-drag-handle>COMBATTIMENTO ${player.human?'<button data-action="blitz" class="stop-blitz">Ferma blitz</button>':''}</header><div class="conquest-odds">Probabilità di conquista: <b>${Math.round(conquestOdds(t.before.board[e.from].armies,t.before.board[e.to].armies)*100)}%</b></div><div class="battle-sides">${[[e.from,attack,'#d93829','ATTACCO',e.losses[0]],[e.to,defense,'#F2E7D3','DIFESA',e.losses[1]]].map(([id,dice,color,label,loss],i)=>`<div class="battle-side side-${i}"><small>${label}</small><strong>${escapeHtml(territories[id as number].name)}</strong><div class="dice-bowl">${(dice as number[]).map((n,j)=>dieMarkup(n,color as string,j+i*3)).join('')}</div><span class="loss-label">−${loss} carri</span></div>`).join('')}</div><div class="battle-comparison">${attack.slice(0,defense.length).map((n,i)=>`<span class="dice-pair ${n>defense[i]?'defender-lost':'attacker-lost'}" style="--pair:${i}">${n} ${n>defense[i]?'›':'≤'} ${defense[i]} <svg class="battle-hit-tank" viewBox="0 0 50 36" aria-hidden="true">${tankModel(n>defense[i]?'#F2E7D3':'#d93829')}</svg><b>${n>defense[i]?'difesa −1':'attacco −1'}</b></span>`).join('')}</div>${e.conquered?'<p class="conquest-label">Territorio conquistato!</p>':''}</section>`;
  }
  if (e.kind === 'sdadata') return `<section class="fx-window sdadata-window" role="status"><header>SDADATA · ${escapeHtml(player.name)}</header><div class="dice-bowl">${e.dice.map((n,i)=>dieMarkup(n,player.color,i)).join('')}</div><p class="battle-comparison">${e.dice.join(' + ')} = ${e.dice.reduce((a,b)=>a+b,0)} · chiusura ≤ ${e.threshold}</p></section>`;
  if (e.kind === 'draw') return `<section class="card-reward" role="status"><h2>${e.player===t.before.viewer?'HAI RICEVUTO UNA CARTA':`${escapeHtml(player.name)} pesca una carta`}</h2><div class="reward-flight"><div class="reward-flip ${e.card===undefined?'hidden-card':''}"><div class="reward-back">${cardFace()}</div><div class="reward-front">${cardFace(e.card)}</div></div></div></section>`;
  if (e.kind === 'exchange') return `<section class="exchange-effect" role="status"><h2>CAMBIO TRIS</h2><div class="exchange-cards">${(e.cards??[undefined,undefined,undefined]).map(c=>cardFace(c)).join('')}</div><div class="exchange-bonus">${armyToken(e.count,player.color)}<strong>+${e.count} rinforzi</strong></div></section>`;
  return `<div class="map-effect-caption" role="status">${escapeHtml(player.name)} · ${e.kind==='place'?`${e.count>0?'+':''}${e.count} in ${escapeHtml(territories[e.to].name)}`:`${escapeHtml(territories[e.from].name)} → ${escapeHtml(territories[e.to].name)} · ${e.count} carri`}</div>`;
}

type Rect={left:number;top:number;right:number;bottom:number;priority?:number};
type Point={x:number;y:number};
/** Keep floating panels clear of controls, using measured rectangles instead of fixed offsets. */
export function panelPosition(bounds:Rect,width:number,height:number,points:Point[],blocked:Rect[],preferred?:Point):Point{
 const minX=bounds.left+8,minY=bounds.top+8,maxX=Math.max(minX,bounds.right-width-8),maxY=Math.max(minY,bounds.bottom-height-8);
 const clamp=(p:Point)=>({x:Math.max(minX,Math.min(maxX,p.x)),y:Math.max(minY,Math.min(maxY,p.y))});
 const xs=[minX,maxX,(minX+maxX)/2],ys=[minY,maxY,(minY+maxY)/2];
 for(const r of blocked){xs.push(r.left-width-8,r.right+8);ys.push(r.top-height-8,r.bottom+8);}
 const choices=[...(preferred?[clamp(preferred)]:[]),...xs.flatMap(x=>ys.map(y=>clamp({x,y})))];
 const score=(p:Point)=>{
  const overlap=blocked.reduce((sum,r)=>sum+Math.max(0,Math.min(p.x+width,r.right)-Math.max(p.x,r.left))*Math.max(0,Math.min(p.y+height,r.bottom)-Math.max(p.y,r.top))*(r.priority??1),0);
  const obscured=points.filter(t=>t.x>p.x-30&&t.x<p.x+width+30&&t.y>p.y-30&&t.y<p.y+height+30).length;
  return overlap*10000+obscured*1000+(preferred?Math.hypot(p.x-preferred.x,p.y-preferred.y)*.01:0);
 };
 return choices.reduce((best,p)=>score(p)<score(best)?p:best,choices[0]);
}
export function positionPanel(root:HTMLElement,panel:HTMLElement,ids:number[],preferred?:Point){
 const stage=root.querySelector<HTMLElement>('.map-stage');
 if(stage)panel.style.maxHeight=`${Math.max(180,stage.getBoundingClientRect().height-12)}px`;
 const svg=root.querySelector<SVGSVGElement>('#world-map'),width=panel.offsetWidth,height=panel.offsetHeight;
 const points=ids.flatMap(id=>{
  const matrix=svg?.getScreenCTM();if(!svg||!matrix)return [];
  const p=svg.createSVGPoint();p.x=territories[id].x;p.y=territories[id].y;const screen=p.matrixTransform(matrix);return [{x:screen.x,y:screen.y}];
 });
 const blocked=Array.from(root.querySelectorAll<HTMLElement>('.reference-zoom,.reference-sidebar,.command-deck,.board-top,.players-row')).map(el=>{const r=el.getBoundingClientRect();return {left:r.left,top:r.top,right:r.right,bottom:r.bottom,width:r.width,height:r.height,priority:el.matches('.players-row')?1:1000};}).filter(r=>r.width>0&&r.height>0);
 const c=panelPosition({left:0,top:0,right:innerWidth,bottom:innerHeight},width,height,points,blocked,preferred);
 panel.style.left=`${c.x}px`;panel.style.top=`${c.y}px`;panel.style.transform='none';
}

export function bindPanelDrag(panel: HTMLElement) {
  const handle=panel.querySelector<HTMLElement>('[data-drag-handle]');
  if(!handle)return;
  let drag: {id:number;x:number;y:number;left:number;top:number}|null=null;
  handle.onpointerdown=e=>{
    if((e.target as Element).closest('button'))return;
    const r=panel.getBoundingClientRect(); drag={id:e.pointerId,x:e.clientX,y:e.clientY,left:r.left,top:r.top};
    handle.setPointerCapture(e.pointerId); e.preventDefault();
  };
  handle.onpointermove=e=>{
    if(!drag||drag.id!==e.pointerId)return;
    panel.style.left=`${Math.max(6,Math.min(innerWidth-panel.offsetWidth-6,drag.left+e.clientX-drag.x))}px`;
    panel.style.top=`${Math.max(6,Math.min(innerHeight-panel.offsetHeight-6,drag.top+e.clientY-drag.y))}px`;
    panel.style.transform='none';
  };
  handle.onpointerup=handle.onpointercancel=()=>{
    drag=null;const root=panel.closest<HTMLElement>('.app-shell');
    if(root)positionPanel(root,panel,[],{x:parseFloat(panel.style.left)||0,y:parseFloat(panel.style.top)||0});
  };
}

/** Runs on a separate layer: no full-page render between dice frames. */
export function playTransition(root: HTMLElement, t: Transition, finished: () => void, onClash?: () => void, onCue?: (cue:'deploy'|'dice'|'conquest'|'draw'|'trade')=>void): () => void {
  const host = root.querySelector<HTMLElement>('#effects');
  if (!host) { finished(); return ()=>{}; }
  const timers: ReturnType<typeof setTimeout>[]=[];
  let stopped=false;
  const later=(f:()=>void, ms:number)=>timers.push(setTimeout(()=>{if(!stopped)f();},ms));
  const clearMap=()=>{
    root.querySelectorAll('.fx-source,.fx-target,.fx-place,.fx-conquer,.fx-clash').forEach(el=>el.classList.remove('fx-source','fx-target','fx-place','fx-conquer','fx-clash'));
    root.querySelectorAll('.count-changing').forEach(el=>el.classList.remove('count-changing'));
    root.querySelectorAll('.count-before').forEach(el=>el.remove());
    root.querySelector('#map-fx')?.remove();
  };
  const reduced = globalThis.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
  const board=t.before.board.map(cell=>({...cell}));
  function badge(id:number, delta:number, owner:number, duration:number) {
    if(!delta)return;
    const point=territories[id], color=t.before.players[owner].color;
    root.querySelector('#map-fx')?.insertAdjacentHTML('beforeend',`<g class="army-delta ${delta<0?'army-loss':''}" transform="translate(${point.x-19} ${point.y-34})" style="--counter-ms:${duration}ms"><g class="delta-rise">${tankModel(color)}<rect x="30" y="18" width="34" height="22" rx="11" fill="#111827" stroke="${color}"/><text x="47" y="34" style="font-size:${counterFontSize(delta,true)}px">${delta>0?'+':'−'}${Math.abs(delta)}</text></g></g>`);
  }
  function travel(from:number,to:number,color:string,duration:number,arrow=false){
    const a=territories[from],b=territories[to],dx=b.x-a.x,dy=b.y-a.y;
    const host=root.querySelector('#map-fx');
    if(arrow)host?.insertAdjacentHTML('beforeend',`<g class="attack-vector"><path d="M${a.x} ${a.y}L${b.x} ${b.y}" pathLength="100" stroke="${color}"/><path d="M-13-6L0 0L-13 6" transform="translate(${b.x} ${b.y}) rotate(${Math.atan2(dy,dx)*180/Math.PI})" stroke="${color}"/></g>`);
    else {
      host?.insertAdjacentHTML('beforeend',`<g class="march-origin" transform="translate(${a.x-12} ${a.y-8})"><g class="march-tank" style="--march-x:${dx}px;--march-y:${dy}px;--march-ms:${duration}ms"><g transform="scale(.6)">${tankModel(color)}</g></g></g>`);
    }
  }
  function changeArmy(id:number, delta:number, owner:number, duration:number) {
    if(!delta && board[id].owner===owner)return;
    board[id].armies+=delta;board[id].owner=owner;
    const token=root.querySelector<SVGElement>(`[data-token="${id}"]`);
    const number=token?.querySelector<SVGTextElement>('.tank-number');
    if(token && number) {
      token.querySelector('.count-before')?.remove();
      const old=number.cloneNode(true) as SVGTextElement;
      old.classList.remove('tank-number');old.classList.add('count-before');
      number.parentElement?.append(old);
      number.textContent=String(board[id].armies);
      number.style.fontSize=`${counterFontSize(board[id].armies)}px`;
      const color=t.before.players[owner].color;
      token.querySelector('.tank-token')?.setAttribute('fill',color);
      token.querySelector<SVGElement>('.tank-model')?.style.setProperty('color',color);
      const shield=token.querySelector('.army-shield');
      if(shield)shield.outerHTML=armyBadge(board[id].armies,color);
      token.style.setProperty('--counter-ms',`${duration}ms`);
      token.classList.remove('count-changing');
      // Restart just the small SVG counter; the map and open windows stay mounted.
      token.getBoundingClientRect();token.classList.add('count-changing');
      later(()=>{old.remove();token.classList.remove('count-changing');},duration);
    }
    const cell=root.querySelector(`[data-territory="${id}"]`);
    cell?.setAttribute('aria-label',`${territories[id].name}: ${board[id].armies} armate, ${t.before.players[owner].name}`);
    badge(id,delta,owner,duration);
  }
  function run(index:number) {
    if(stopped)return;
    clearMap();
    const e=t.effects[index];
    if(!e){host!.innerHTML='';finished();return;}
    const duration=(reduced?Math.min(450,effectDuration(e,t.speed)):effectDuration(e,t.speed))*sequenceScale(t);
    host!.style.setProperty('--effect-ms',`${duration}ms`);
    host!.className=`effects-layer effect-${e.kind} ${reduced?'reduced-effects':''}`;
    host!.innerHTML=effectMarkup(e,t);
    if(e.kind==='battle'||e.kind==='sdadata')onCue?.('dice');
    if(e.kind==='draw')onCue?.('draw');if(e.kind==='exchange')onCue?.('trade');
    const panel=host!.querySelector<HTMLElement>('[data-floating]');
    if(panel){positionPanel(root,panel,e.kind==='battle'?[e.from,e.to]:[]);bindPanelDrag(panel);}
    if('to' in e) {
      root.querySelector(`[data-territory="${e.to}"]`)?.classList.add(e.kind==='place'?'fx-place':'fx-target');
      if('from' in e)root.querySelector(`[data-territory="${e.from}"]`)?.classList.add('fx-source');
      root.querySelector('#world-map')?.insertAdjacentHTML('beforeend', '<g id="map-fx" pointer-events="none"></g>');
      if(e.kind==='place')later(()=>{onCue?.('deploy');changeArmy(e.to,e.count,e.player,duration*.75);},duration*.15);
      if(e.kind==='move')later(()=>{
        if(!reduced)travel(e.from,e.to,t.before.players[e.player].color,duration*.7);
        changeArmy(e.from,-e.count,e.player,duration*.75);
        changeArmy(e.to,e.count,e.player,duration*.75);
      },duration*.15);
      if(e.kind==='battle') {
        const reveal=Math.min(350/Math.max(1,t.speed),duration*.55);
        if(!reduced)travel(e.from,e.to,t.before.players[e.player].color,duration,true);
        const source=territories[e.from],target=territories[e.to];
        const matrix=root.querySelector<SVGSVGElement>('#world-map')?.getScreenCTM?.();
        const factor=matrix?Math.min(3,Math.max(.5,1/Math.hypot(matrix.a,matrix.b))):1;
        root.querySelector('#map-fx')?.insertAdjacentHTML('beforeend',`<g class="clash-origin" transform="translate(${source.x} ${source.y}) scale(${factor})"><circle r="22"/><circle r="28"/><path d="M-34 0H-24M24 0H34M0-34V-24M0 24V34"/></g><g class="clash-impact" transform="translate(${target.x} ${target.y}) scale(${factor})"><g class="clash-burst"><path d="M0-29L6-12L22-22L12-5L30 0L12 6L21 23L5 12L0 30L-6 12L-23 22L-12 5L-30 0L-12-6L-22-22L-5-12Z"/><circle class="clash-wave wave-one" r="24"/><circle class="clash-wave wave-two" r="30"/></g></g>`);
        later(()=>{
          root.querySelector(`[data-territory="${e.to}"]`)?.classList.add('fx-clash');
          root.querySelector('#map-fx')?.classList.add('impact-now');
          onClash?.();
        },reveal);
        later(()=>{
          changeArmy(e.from,-e.losses[0],e.player,duration*.28);
          // A conquered territory swaps directly to its compulsory garrison;
          // never show the intermediate empty territory to the player.
          if(!e.conquered)changeArmy(e.to,-e.losses[1],e.defender,duration*.28);
          else badge(e.to,-e.losses[1],e.defender,duration*.19);
        },reveal);
        if(e.conquered)later(()=>{
          onCue?.('conquest');if(!reduced)travel(e.from,e.to,t.before.players[e.player].color,duration*.25);
          const target=root.querySelector(`[data-territory="${e.to}"]`);
          target?.classList.add('fx-conquer');
          target?.classList.remove('fx-target');
          board[e.to].armies=0;
          changeArmy(e.from,-e.transferred,e.player,duration*.23);
          changeArmy(e.to,e.transferred,e.player,duration*.23);
        },reveal+duration*.18);
      }
    }
    later(()=>host!.classList.add('rolling'),0);
    later(()=>{host!.classList.remove('rolling');host!.classList.add('resolved');},Math.min(350/Math.max(1,t.speed),duration*.55));
    later(()=>host!.classList.add('show-losses'),duration*.78);
    later(()=>host!.classList.add('effect-leaving'),Math.max(duration*.8,duration-160));
    later(()=>run(index+1),duration);
  }
  run(0);
  return ()=>{stopped=true;timers.forEach(clearTimeout);clearMap();host.innerHTML='';};
}
