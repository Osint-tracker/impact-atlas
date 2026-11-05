import {allCards as cards, allTerritories as territories, objectives, mapFor} from '../data/mapGraph.ts';
export const escapeHtml = (value: unknown) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'} as Record<string,string>)[c]);
export const tankShape = 'M1.8 16.5Q1.7 12 4.3 10.2Q6.8 8.3 8.9 8.3L9.2 6.4Q10.6 3.6 13 3.2H17.4Q18.6 3.2 19.1 4.8L38.5 5.3Q41.3 5.4 41.4 8V10Q41.1 12 39.4 12.3L35.1 12.8L34.9 19.4Q34.5 23.7 30.2 24H7.4Q2 23.8 1.8 19.1Z';
export function counterFontSize(count: number, signed = false): number {
  const digits=String(Math.abs(count)).length+(signed?1:0);
  return digits<=2?15:digits===3?12:10;
}
export function tankModel(color:string){
 return `<g class="tank-model" style="color:${color}"><ellipse cx="24" cy="31" rx="22" ry="5" fill="#000" opacity=".35"/><path d="M3 18L15 10L42 17L43 26L31 33L3 25Z" fill="#17202c"/><path d="M5 20L30 27L30 31L5 24ZM33 26L41 21L41 25L33 30Z" fill="#66717d"/>${[9,16,23].map(x=>`<circle cx="${x}" cy="${21+(x-9)*.27}" r="2.2" fill="#111827" stroke="#8a949b" stroke-width=".7"/>`).join('')}<path class="tank-token" d="M3 16L16 7L43 14L30 24L3 18Z" fill="${color}"/><path d="M3 18L30 24L30 28L3 21ZM30 24L43 14L43 20L30 28Z" fill="currentColor"/><path d="M30 24L43 14L43 20L30 28Z" fill="#000" opacity=".28"/><path d="M12 13L21 7L33 11L24 18L12 16Z" fill="currentColor" stroke="#151b25" stroke-width=".6"/><path d="M12 13L24 18V21L12 16Z" fill="#000" opacity=".25"/><path d="M24 12L47 5L49 8L26 16Z" fill="currentColor" stroke="#1a2330" stroke-width=".8"/><path d="M16 11L22 8L29 10L23 12ZM7 16L16 10L18 11L10 17Z" fill="#fff" opacity=".36"/><path d="M28 12L47 6" stroke="#fff" opacity=".4" stroke-width="1.1"/></g>`;
}
export function armyBadge(count:number,color:string){
 const tier=count>=10?'gold':count>=5?'silver':'standard';
 return `<g class="army-shield tier-${tier}" data-tier="${tier}"><circle class="shield-rim" cx="48" cy="29" r="13" fill="#111827" stroke="${count>=10?'#e7ba50':count>=5?'#c4d0dd':color}" stroke-width="2"/><circle cx="48" cy="29" r="10.5" fill="none" stroke="${color}" stroke-width="1"/><text class="tank-number" x="48" y="34" text-anchor="middle" style="font-size:${Math.max(12,counterFontSize(count)-1)}px">${count}</text>${count>=10?'<path class="prestige-star" d="M48 12L49.4 15L53 15.3L50.2 17.5L51 21L48 19.3L45 21L45.8 17.5L43 15.3L46.6 15Z" fill="#f6d97a"/>':''}${count>=12?'<path class="army-strength" d="M37 43H59" stroke="#f8ce5b" stroke-width="2"/>':''}</g>`;
}
export function armyToken(count: number, color: string) {
 return `<svg class="army-token" viewBox="0 0 64 46" aria-label="${count} carri">${tankModel(color)}${armyBadge(count,color)}</svg>`;
}
export function scoreChart(history:{round:number;points:(number|null)[]}[]|undefined,players:{color:string;name:string}[]){
 if(!history?.length)return '<p class="muted">Andamento disponibile dalle nuove partite.</p>';
 return `<figure class="score-chart"><figcaption>Andamento punti obiettivo</figcaption><svg viewBox="0 0 600 150" role="img" aria-label="Andamento punti obiettivo da 0 a 86"><path d="M30 10V125H590" fill="none" stroke="#748297"/><text x="4" y="18">86</text><text x="12" y="128">0</text>${players.map((p,i)=>`<polyline fill="none" stroke="${p.color}" stroke-width="2.5" points="${history.flatMap((row,j)=>row.points[i]===null?[]:[`${30+j*560/Math.max(1,history.length-1)},${125-Math.min(86,row.points[i]!)*110/86}`]).join(' ')}"/>`).join('')}</svg><div>${players.map(p=>`<span style="color:${p.color}">● ${escapeHtml(p.name)}</span>`).join(' ')}</div></figure>`;
}
export const territoryColors = [
  '#a77d37','#956536','#b77519','#af994e','#b76631','#b08744','#a8391e','#a45128','#b96c24',
  '#387b66','#528679','#749989','#38775e',
  '#9a538a','#664a79','#926090','#653c71','#744780','#8b6080','#773b75',
  '#876124','#986e32','#aa903e','#8b592c','#794920','#b49245',
  '#829a4b','#27653d','#487e36','#8c9e38','#4e863b','#315f35','#57865b','#688743','#235a37','#8b9d4b','#287343','#367361',
  '#914774','#963b78','#993c7b','#a4478c','#71b7d2','#b1d5e5'
];
const military: Record<string,string> = {
  cannone: '<path d="M7 10h22l5-4h5v7H23l-4 5H7zM6 22l9-8 4 2-9 9z"/><circle cx="26" cy="22" r="6" fill="none" stroke="currentColor" stroke-width="3"/><path d="M26 17v10m-5-5h10" stroke="currentColor" stroke-width="1.5"/>',
  fante: '<circle cx="21" cy="6" r="4"/><path d="M16 12h10l3 10-4 1-2-7v10l4 10h-5l-3-8-2 8h-5l4-12V16l-5 7-3-2zM31 6h2v28h-2z"/>',
  cavaliere: '<path d="M5 19l6-8 13 2 6-7 8 3-2 9-6 1-4 9H11L8 35H4l3-11zM24 25l6 10h-5l-7-9M11 26l5 9h-4L8 29M13 11l-4-6-5 2 3 10z"/><circle cx="22" cy="5" r="4"/><path d="M19 9h6l-1 12h-8l2-4z"/>',
};
export function militaryIcon(symbol: string) {
  return `<svg viewBox="0 0 44 40" class="military-icon" aria-label="${symbol}">${military[symbol] ?? military.cannone}</svg>`;
}
function silhouette(id: number) {
  const t = territories[id], n = t.path.match(/-?\d+(?:\.\d+)?/g)!.map(Number);
  const xs = n.filter((_,i) => i % 2 === 0), ys = n.filter((_,i) => i % 2 === 1);
  const x = Math.min(...xs)-4, y = Math.min(...ys)-4, w = Math.max(...xs)-x+4, h = Math.max(...ys)-y+4;
  return `<svg class="card-country" viewBox="${x} ${y} ${w} ${h}" aria-hidden="true"><path d="${t.path}" fill="${territoryColors[id]}" stroke="#786444" stroke-width="1"/></svg>`;
}
export function cardFace(id?: number) {
  if (id === undefined) return '<div class="card-face card-back"><span>RisiKo!</span><small>OFFLINE</small></div>';
  const c = cards[id];
  if (c.symbol === 'jolly') return `<div class="card-face card-jolly"><svg class="compass-rose" viewBox="0 0 100 100" aria-hidden="true"><circle cx="50" cy="50" r="35" fill="none" stroke="currentColor"/><path d="M50 1L59 39L91 9L61 41L99 50L61 59L91 91L59 61L50 99L41 61L9 91L39 59L1 50L39 41L9 9L41 39Z"/><circle cx="50" cy="50" r="7" fill="#0b1f2e"/></svg><strong>Jolly</strong></div>`;
  return `<div class="card-face"><div class="card-symbol">${militaryIcon(c.symbol)}</div>${silhouette(c.territory!)}<strong>${escapeHtml(territories[c.territory!].name)}</strong><small>${c.symbol}</small></div>`;
}
export function objectiveMap(id: number, antarctica=false) {
  const goal = objectives[id-1], map=mapFor({antarctica}), territories=map.territories;
  return `<div class="objective-card"><svg class="objective-mini" viewBox="${antarctica?'220 0 840 815':'220 45 840 580'}" role="img" aria-label="Mappa obiettivo ${id}"><rect x="0" y="0" width="1240" height="${map.height}" fill="#e4d8b9"/>${territories.map(t => `<path d="${t.path}" fill="${goal.territories.includes(t.id)?'#48694c':'#c8bfa9'}" stroke="#8c8267" stroke-width="1"/>`).join('')}</svg><span class="objective-id">${id}</span></div>`;
}
const pipPositions: Record<number,number[][]> = {
  1:[[15,15]],2:[[8,8],[22,22]],3:[[8,8],[15,15],[22,22]],4:[[8,8],[22,8],[8,22],[22,22]],
  5:[[8,8],[22,8],[15,15],[8,22],[22,22]],6:[[8,7],[22,7],[8,15],[22,15],[8,23],[22,23]]
};
export function pips(n: number) {
  return `<svg viewBox="0 0 30 30" aria-hidden="true">${(pipPositions[n]??[]).map(([x,y])=>`<circle cx="${x}" cy="${y}" r="2.5"/>`).join('')}</svg>`;
}
export function dieMarkup(n: number, color: string, index = 0) {
  const other = [1,2,3,4,5,6].filter(x=>x!==n && x!==7-n);
  return `<span class="visual-die" role="img" aria-label="Dado ${n}" style="--die-color:${color};--i:${index};--pip-color:${(color==='#f0d846'||color==='#F2E7D3')?'#173340':'white'}"><span class="die-cube">${[n,7-n,...other].map((v,i)=>`<span class="die-face face-${i}">${pips(v)}</span>`).join('')}</span></span>`;
}

export function crest(){return `<svg class="atlas-crest" viewBox="0 0 170 64" aria-hidden="true"><g fill="currentColor">${[0,1,2,3,4,5].map(i=>`<ellipse cx="${28-i*2}" cy="${52-i*7}" rx="3" ry="8" transform="rotate(${-40+i*12} ${28-i*2} ${52-i*7})"/><ellipse cx="${142+i*2}" cy="${52-i*7}" rx="3" ry="8" transform="rotate(${40-i*12} ${142+i*2} ${52-i*7})"/>`).join('')}<path d="M28 57Q80 69 142 57" fill="none" stroke="currentColor"/><g transform="translate(59 9) scale(1.05)">${tankModel('#c8a45a')}</g></g></svg>`;}
