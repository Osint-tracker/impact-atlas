import assert from 'node:assert/strict';

// One synchronous browser evaluation: all rectangles belong to the same render.
// Separate locator.boundingBox() requests can observe nodes detached by a bot move.
export async function readBoardLayout(page) {
 return page.evaluate(()=>{
  const table=document.querySelector('.game-table');
  const selectors=['.board-top','.map-stage','.board-bottom','.players-row','.command-deck','.reference-zoom','.reference-sidebar'];
  const boxes=Object.fromEntries(selectors.map(selector=>{
   const element=table?.querySelector(selector);
   if(!element||!element.getClientRects().length)return [selector,null];
   const r=element.getBoundingClientRect();
   return [selector,{x:r.x,y:r.y,width:r.width,height:r.height}];
  }));
  return {phase:table?.getAttribute('data-phase'),active:table?.getAttribute('data-active'),revision:table?.getAttribute('data-revision'),boxes,scrollWidth:document.documentElement.scrollWidth,viewportWidth:innerWidth};
 });
}

export function assertBoardLayout(snapshot) {
 const context=`fase ${snapshot.phase}, giocatore ${snapshot.active}, revisione ${snapshot.revision}`;
 for(const selector of ['.board-top','.map-stage','.board-bottom','.players-row','.command-deck','.reference-zoom','.reference-sidebar']) {
  const box=snapshot.boxes[selector];
  assert.ok(box&&[box.x,box.y,box.width,box.height].every(Number.isFinite)&&box.width>0&&box.height>0,`HUD assente o non visibile: ${selector} (${context}). Snapshot: ${JSON.stringify(snapshot)}`);
 }
 const rows=['.board-top','.map-stage','.board-bottom'];
 for(let i=1;i<rows.length;i++) {
  const previous=snapshot.boxes[rows[i-1]],current=snapshot.boxes[rows[i]];
  assert.ok(current.y>=previous.y+previous.height-1,`HUD sovrapposto: ${rows[i-1]} / ${rows[i]} (${context})`);
 }
 const bottom=snapshot.boxes['.board-bottom'];
 for(const selector of ['.players-row','.command-deck']){const b=snapshot.boxes[selector];assert.ok(b.y>=bottom.y-1&&b.y+b.height<=bottom.y+bottom.height+1,`HUD fuori dalla barra inferiore: ${selector}`);}
 const players=snapshot.boxes['.players-row'],commands=snapshot.boxes['.command-deck'];assert.ok(players.x+players.width<=commands.x+1,'Giocatori e comandi sovrapposti');
 const zoom=snapshot.boxes['.reference-zoom'],rail=snapshot.boxes['.reference-sidebar'];
 assert.ok(zoom.x+zoom.width<=rail.x,'Zoom e Cronologia devono stare in colonne distinte');
 assert.ok(snapshot.scrollWidth<=snapshot.viewportWidth+1,'La pagina non deve scorrere orizzontalmente');
}

export async function readMapScale(page) {
 const snapshot=await page.evaluate(()=>{
  const map=document.querySelector('#world-map'),token=map?.querySelector('[data-token="0"] .army-map-icon');
  return {viewBox:map?.getAttribute('viewBox'),tokenWidth:token?.getBoundingClientRect().width};
 });
 assert.ok(snapshot.viewBox&&Number.isFinite(snapshot.tokenWidth)&&snapshot.tokenWidth>0,`Mappa o carro non misurabile: ${JSON.stringify(snapshot)}`);
 const viewWidth=Number(snapshot.viewBox.trim().split(/[\s,]+/)[2]);
 assert.ok(Number.isFinite(viewWidth)&&viewWidth>0,`viewBox non valido: ${snapshot.viewBox}`);
 return {...snapshot,viewWidth};
}
