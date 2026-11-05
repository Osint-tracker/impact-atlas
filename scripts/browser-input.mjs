import assert from 'node:assert/strict';

import {resolveMapPointers} from '../src/gui/mapPointer.ts';

// Click the map through a real pointer, using the same resolution as gameplay.
// A transparent neighbour may be the DOM hit without being the intended target.
// Clicking the SVG (not an obscured territory group) respects that distinction.
export async function clickTerritory(page, id, performClick) {
 const territory=page.locator(`[data-territory="${id}"]`),svg=page.locator('#world-map');
 const candidates=await territory.evaluate(element=>{
  const box=element.getBoundingClientRect();
  const points=[...element.querySelectorAll('.territory-hit,.placement-plus-hit')].map(hit=>{
   const r=hit.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};
  });
  const left=Math.max(1,box.left),right=Math.min(innerWidth-1,box.right);
  const top=Math.max(1,box.top),bottom=Math.min(innerHeight-1,box.bottom);
  for(let y=top+2;y<bottom;y+=4)for(let x=left+2;x<right;x+=4)points.push({x,y});
  return points;
 });
 // Usually the tank or + centre is enough; avoid scanning the whole grid unless
 // an actual panel/viewport edge hides both. No force clicks or synthetic events.
 const centres=candidates.slice(0,2);
 let resolved=await svg.evaluate(resolveMapPointers,centres);
 let point=centres.find((_,i)=>resolved[i]===id);
 if(!point){resolved=await svg.evaluate(resolveMapPointers,candidates);point=candidates.find((_,i)=>resolved[i]===id);}
 assert.ok(point,`Territorio ${id}: nessun punto raggiungibile dal puntatore; controllare sovrapposizioni o inquadratura.`);
 // DOM rects stay in the frame's coordinate system. Playwright boundingBox()
 // is relative to the outer page, which would break the rotated iPhone iframe.
 const box=await svg.evaluate(element=>{if(!element.getClientRects().length)return null;const r=element.getBoundingClientRect();return {x:r.x,y:r.y};});
 assert.ok(box,'Mappa non visibile durante il click');
 const position={x:point.x-box.x,y:point.y-box.y};
 if(performClick)await performClick(svg,position);
 else await svg.click({position,timeout:5000});
}

export async function browserDiagnostics(page) {
 return page.evaluate(()=>({
  table:document.querySelector('.game-table')?.dataset,
  effects:document.querySelector('#effects')?.innerHTML.slice(0,500),
  toast:document.querySelector('#toast')?.textContent,
  paused:!!document.querySelector('.pause-card'),
  placeable:[...document.querySelectorAll('.territory.placeable')].map(el=>{
   const r=el.querySelector('.territory-hit').getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2;
   return {id:el.getAttribute('data-territory'),x,y,hit:document.elementFromPoint(x,y)?.outerHTML.slice(0,250),hitTerritory:document.elementFromPoint(x,y)?.closest('[data-territory]')?.getAttribute('data-territory')};
  })
 }));
}
