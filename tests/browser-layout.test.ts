import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readBoardLayout,assertBoardLayout,readMapScale} from '../scripts/browser-layout.mjs';

function layout(){return {phase:'ATTACK',active:'0',revision:'70',scrollWidth:844,viewportWidth:844,boxes:{
 '.board-top':{x:3,y:3,width:838,height:46},'.map-stage':{x:3,y:52,width:838,height:250},
 '.board-bottom':{x:3,y:305,width:838,height:72},'.players-row':{x:3,y:305,width:410,height:72},
 '.command-deck':{x:417,y:305,width:424,height:72},'.reference-zoom':{x:8,y:250,width:230,height:44},
 '.reference-sidebar':{x:770,y:56,width:62,height:240}
}};}

test('HUD: snapshot coerente supera tutti i controlli geometrici',()=>{
 assert.doesNotThrow(()=>assertBoardLayout(layout()));
});
test('HUD: un elemento mancante produce un errore identificabile, non null.y né un falso successo',()=>{
 for(const selector of Object.keys(layout().boxes)){
  const snapshot:any=layout();snapshot.boxes[selector]=null;
  assert.throws(()=>assertBoardLayout(snapshot),(error:any)=>error.code==='ERR_ASSERTION'&&error.message.includes(selector)&&error.message.includes('revisione 70'));
 }
});
test('HUD: restano rilevate sovrapposizioni, elementi nascosti e overflow',()=>{
 const overlap=layout();overlap.boxes['.board-bottom'].y=290;assert.throws(()=>assertBoardLayout(overlap),/HUD sovrapposto/);
 const rail=layout();rail.boxes['.reference-zoom'].width=800;assert.throws(()=>assertBoardLayout(rail),/Zoom e Cronologia/);
 const hidden=layout();hidden.boxes['.map-stage'].height=0;assert.throws(()=>assertBoardLayout(hidden),/non visibile/);
 const overflow=layout();overflow.scrollWidth=900;assert.throws(()=>assertBoardLayout(overflow),/orizzontalmente/);
});
test('HUD: lettura atomica, senza richieste locator che possano attraversare due render',async()=>{
 const previous={document:globalThis.document,innerWidth:globalThis.innerWidth};
 const expected=layout();let reads=0,evaluations=0;
 const table={getAttribute:(key:string)=>({'data-phase':'ATTACK','data-active':'0','data-revision':'70'}[key]),querySelector:(selector:string)=>({getClientRects:()=>[{}],getBoundingClientRect:()=>{reads++;return expected.boxes[selector as keyof typeof expected.boxes];}})};
 Object.assign(globalThis,{innerWidth:844,document:{querySelector:()=>table,documentElement:{scrollWidth:844}}});
 const page={evaluate:async(fn:()=>unknown)=>{evaluations++;const snapshot=fn();Object.assign(globalThis,{document:{querySelector:()=>null}});return snapshot;},locator:()=>{throw Error('Letture separate vietate');}};
 try{
  const snapshot=await readBoardLayout(page);assert.equal(evaluations,1);assert.equal(reads,7);assert.deepEqual(snapshot,expected);assertBoardLayout(snapshot);
 }finally{Object.assign(globalThis,previous);}
});
test('zoom: misura carro e viewBox nello stesso snapshot e diagnostica una mappa assente',async()=>{
 const good={evaluate:async()=>({viewBox:'0 0 1240 690',tokenWidth:28})};
 assert.deepEqual(await readMapScale(good),{viewBox:'0 0 1240 690',tokenWidth:28,viewWidth:1240});
 await assert.rejects(readMapScale({evaluate:async()=>({})}),/non misurabile/);
});
