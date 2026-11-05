import {test} from 'node:test';
import assert from 'node:assert/strict';
import {clickTerritory} from '../scripts/browser-input.mjs';
import {resolveMapPointers} from '../src/gui/mapPointer.ts';

async function fixture(hit:(x:number,y:number)=>boolean,check:(page:any,clicks:any[])=>Promise<void>){
 const previous={document:globalThis.document,width:globalThis.innerWidth,height:globalThis.innerHeight};
 const clicks:any[]=[];
 const rect={x:100,y:40,left:100,top:40,right:180,bottom:100};
 const element:any={getBoundingClientRect:()=>rect,getAttribute:()=> '1'};
 element.closest=()=>element;
 const target={getClientRects:()=>[{}],getBoundingClientRect:()=>({x:108,y:28,width:44,height:44}),closest:()=>element};
 element.querySelectorAll=()=>[target];
 const overlay={closest:()=>null};
 const svg={getClientRects:()=>[rect],getBoundingClientRect:()=>rect,querySelectorAll:()=>[target],contains:(el:any)=>el===element,ownerDocument:{elementFromPoint:(x:number,y:number)=>hit(x,y)?element:overlay}};
 Object.assign(globalThis,{innerWidth:844,innerHeight:390});
 const page={locator:(selector:string)=>{
  assert.ok(['[data-territory="1"]','#world-map'].includes(selector));
  return {evaluate:async(fn:any,arg:any)=>fn(selector==='#world-map'?svg:element,arg),click:async(options:any)=>{assert.equal(selector,'#world-map');clicks.push(options);}};
 }};
 try{await check(page,clicks);}finally{Object.assign(globalThis,{document:previous.document,innerWidth:previous.width,innerHeight:previous.height});}
}
test('tocchi browser: centro libero, click normale senza forzare sovrapposizioni',async()=>{
 await fixture(()=>true,async(page,clicks)=>{
  await clickTerritory(page,1);assert.equal(clicks.length,1);assert.deepEqual(clicks[0].position,{x:30,y:10});assert.equal(clicks[0].force,undefined);
 });
});
test('tocchi browser: centro sotto HUD, seleziona un punto libero dello stesso territorio',async()=>{
 await fixture((_x,y)=>y>65,async(page,clicks)=>{
  await clickTerritory(page,1);assert.equal(clicks.length,1);assert.ok(clicks[0].position.y+40>65);assert.equal(clicks[0].force,undefined);
 });
});
test('tocchi browser: territorio completamente coperto, errore esplicito senza click falso',async()=>{
 await fixture(()=>false,async(page,clicks)=>{
  await assert.rejects(clickTerritory(page,1),/Territorio 1: nessun punto raggiungibile/);assert.equal(clicks.length,0);
 });
});

// Realistic zoomed-out positions: touch circles overlap, artwork centres do not.
function pointerFixture(scale=1, rotated=false) {
 const transform=(x:number,y:number)=>rotated?{x:700-y*scale,y:50+x*scale}:{x:100+x*scale,y:60+y*scale};
 const territories=new Map<number,any>(),centres:any[]=[];
 for(const [id,x,y] of [[2,100,100],[13,130,105],[14,130,145]]){
  const hits:any[]=[];
  const territory:any={getAttribute:()=>String(id),querySelectorAll:()=>hits,getBoundingClientRect:()=>({x:0,y:0,left:0,top:0,right:800,bottom:600,width:800,height:600})};
  territory.closest=()=>territory;territories.set(id,territory);
  for(const [hx,hy] of [[x,y],[x+23,y-5]]){
   const p=transform(hx,hy);
   const hit={id,point:p,closest:()=>territory,getClientRects:()=>[{}],getBoundingClientRect:()=>({x:p.x-22,y:p.y-22,width:44,height:44})};hits.push(hit);centres.push(hit);
  }
 }
 // The last-painted neighbour wins the DOM hit everywhere. The pointer policy
 // must still reach the centres of earlier territories, including their +.
 let top:any=territories.get(14);
 const svg:any={querySelectorAll:()=>centres,contains:(el:any)=>el===svg||centres.includes(el)||[...territories.values()].includes(el),closest:()=>null,ownerDocument:{elementFromPoint:(x:number,y:number)=>x<0||y<0||x>=800||y>=600?null:top}};
 return {svg,territories,centres,transform,setTop:(value:any)=>top=value};
}

test('puntatore: hitbox del + sovrapposta non sottrae il centro del territorio vicino',()=>{
 const f=pointerFixture(.36);
 const points=f.centres.map(h=>h.point);
 assert.deepEqual(resolveMapPointers(f.svg,points),f.centres.map(h=>h.id));
 assert.equal(resolveMapPointers(f.svg,[f.transform(100,100)])[0],2);
});

test('puntatore: centro del + resta raggiungibile anche se è più vicino al carro adiacente',()=>{
 const f=pointerFixture(1),point=f.transform(123,95);
 assert.ok(Math.hypot(123-130,95-105)<Math.hypot(123-100,95-100));
 assert.equal(resolveMapPointers(f.svg,[point])[0],2);
});

test('puntatore: stesso destinatario con zoom e plancia iPhone ruotata',()=>{
 for(const scale of [.36,.7,1.5])for(const rotated of [false,true]){
  const f=pointerFixture(scale,rotated);
  assert.deepEqual(resolveMapPointers(f.svg,f.centres.map(h=>h.point)),f.centres.map(h=>h.id));
 }
});

test('puntatore: raggio touch di 22px, terra fuori dal raggio e mare libero',()=>{
 const f=pointerFixture(1);f.setTop(f.svg);
 const at=f.transform(100,100);
 assert.equal(resolveMapPointers(f.svg,[{x:at.x-22,y:at.y}])[0],2);
 assert.equal(resolveMapPointers(f.svg,[{x:at.x-23,y:at.y}])[0],null);
 f.setTop(f.territories.get(13));
 assert.equal(resolveMapPointers(f.svg,[{x:400,y:400}])[0],13);
});

test('puntatore: pannelli e punti fuori viewport non vengono attraversati',()=>{
 const f=pointerFixture(.36);f.setTop({closest:()=>null});
 assert.deepEqual(resolveMapPointers(f.svg,f.centres.map(h=>h.point)),f.centres.map(()=>null));
 assert.deepEqual(resolveMapPointers(f.svg,[{x:-1,y:50},{x:900,y:50}]),[null,null]);
});

async function browserFixture(covered=false,performClick?:Function){
 const f=pointerFixture(.36);if(covered)f.setTop({closest:()=>null});
 const rect={x:10,y:20,width:790,height:580},clicks:any[]=[];
 const evaluated:Function[]=[];f.svg.getBoundingClientRect=()=>rect;f.svg.getClientRects=()=>[rect];
 const svgLocator={evaluate:async(fn:Function,arg:any)=>{evaluated.push(fn);return fn(f.svg,arg);},boundingBox:async()=>{throw Error('Le coordinate della pagina esterna non valgono nel frame iPhone');},click:async(args:any)=>{clicks.push(args);}};
 const territoryLocator={evaluate:async(fn:Function)=>fn(f.territories.get(2)),click:()=>{throw Error('Non cliccare il gruppo SVG oscurato');}};
 const page={locator:(selector:string)=>selector==='#world-map'?svgLocator:territoryLocator};
 const previous={innerWidth:globalThis.innerWidth,innerHeight:globalThis.innerHeight};Object.assign(globalThis,{innerWidth:800,innerHeight:600});
 try{await clickTerritory(page,2,performClick?async(locator:any,point:any)=>{assert.equal(locator,svgLocator);performClick(point);}:undefined);}finally{Object.assign(globalThis,previous);}
 assert.ok(evaluated.includes(resolveMapPointers),'Il test deve usare la funzione di produzione');
 return {clicks,point:f.transform(100,100),rect};
}

test('browser helper: invia un vero click alla mappa, anche con un vicino sopra nel DOM',async()=>{
 const {clicks,point,rect}=await browserFixture();
 assert.deepEqual(clicks,[{position:{x:point.x-rect.x,y:point.y-rect.y},timeout:5000}]);
 assert.equal(clicks[0].force,undefined);
});

test('browser helper: mantiene coordinate relative per il tap della PWA ruotata',async()=>{
 let actual:any;const {point,rect,clicks}=await browserFixture(false,(p:any)=>actual=p);
 assert.deepEqual(actual,{x:point.x-rect.x,y:point.y-rect.y});assert.equal(clicks.length,0);
});

test('browser helper: un pannello che copre la mappa continua a far fallire il test',async()=>{
 await assert.rejects(browserFixture(true),/Territorio 2: nessun punto raggiungibile/);
});
