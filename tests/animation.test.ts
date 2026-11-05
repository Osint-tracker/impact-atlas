import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createGame,applyCommand} from '../src/engine/RisikoRulesEngine.ts';
import {makeTransition} from '../src/engine/presentation.ts';
import type {Transition} from '../src/engine/presentation.ts';
import {playTransition} from '../src/gui/effects.ts';

// Small SVG adapter: tests actual timeline callbacks and rendered counts, not CSS layout.
function timeline(t:Transition,check:(run:(until?:number)=>void,cancel:()=>void,dom:any)=>void) {
 const original={set:globalThis.setTimeout,clear:globalThis.clearTimeout};
 let now=0,serial=0,finished=0,clashes=0;
 const timers=new Map<number,{at:number;fn:()=>void}>();
 const classes=()=>{const set=new Set<string>();return {add:(...xs:string[])=>xs.forEach(x=>set.add(x)),remove:(...xs:string[])=>xs.forEach(x=>set.delete(x)),contains:(x:string)=>set.has(x)};};
 const tokens=t.before.board.map(c=>{
  const seen=[String(c.armies)];let current=String(c.armies);
  const number={style:{fontSize:''},get textContent(){return current;},set textContent(v:string){current=v;seen.push(v);},cloneNode:()=>({classList:classes(),remove(){}}),parentElement:{append(){}}};
  const path={color:'',setAttribute(_name:string,v:string){this.color=v;}};
  return {number,path,seen,style:{setProperty(){}},classList:classes(),getBoundingClientRect(){return {};},querySelector:(q:string)=>q==='.tank-number'?number:q==='.tank-token'?path:null};
 });
 const host={innerHTML:'',className:'',classList:classes(),style:{setProperty(){}},querySelector(){return null;}};
 const marks:string[]=[];
 const fx={classList:classes(),insertAdjacentHTML(_where:string,markup:string){marks.push(markup);},remove(){}};
 const cell={classList:classes(),setAttribute(){}};
 const root={querySelector(q:string){
  if(q==='#effects')return host;
  if(q==='#world-map'||q==='#map-fx')return fx;
  const match=q.match(/^\[data-token="(\d+)"\]$/);if(match)return tokens[Number(match[1])];
  if(q.startsWith('[data-territory='))return cell;
  return null;
 },querySelectorAll(){return [];}};
 globalThis.setTimeout=((fn:()=>void,delay=0)=>{const id=++serial;timers.set(id,{at:now+delay,fn});return id;}) as never;
 globalThis.clearTimeout=((id:number)=>{timers.delete(id);}) as never;
 const run=(until=Infinity)=>{
  for(let i=0;i<1000;i++){
   const next=[...timers.entries()].sort((a,b)=>a[1].at-b[1].at)[0];
   if(!next||next[1].at>until)break;
   timers.delete(next[0]);now=next[1].at;next[1].fn();
  }
 };
 try{const cancel=playTransition(root as never,t,()=>finished++,()=>clashes++);check(run,cancel,{tokens,marks,host,get finished(){return finished;},get clashes(){return clashes;}});}
 finally{globalThis.setTimeout=original.set;globalThis.clearTimeout=original.clear;}
}
function conquest(){
 const s=createGame({players:3,name:'Test',difficulties:['medio','medio'],turnSeconds:180,seed:17});
 s.active=0;s.phase='ATTACK';s.round=5;s.board[0]={owner:0,armies:12};s.board[1]={owner:1,armies:1};
 for(let seed=1;seed<1000;seed++){
  s.rng=seed;const n=applyCommand(s,0,{type:'ATTACK',from:0,to:1});
  if(n.pending)return {s,n};
 }
 throw Error('No conquest');
}
test('animazione conquista: conteggi autentici, nessun fotogramma a zero, vettore e marcia',()=>{
 const {s,n}=conquest();
 timeline(makeTransition(s,n)!, (run,_cancel,dom)=>{
  run(300);assert.equal(dom.tokens[1].number.textContent,'1');
  run();assert.equal(dom.finished,1);
  assert.equal(dom.clashes,1);assert.match(dom.marks.join(''),/clash-origin/);assert.match(dom.marks.join(''),/clash-impact/);
  assert.equal(dom.tokens[1].number.textContent,'3');assert.equal(dom.tokens[0].number.textContent,'9');
  assert.ok(!dom.tokens[1].seen.includes('0'));
  assert.match(dom.marks.join(''),/>\+3</);assert.match(dom.marks.join(''),/>−3</);
  assert.match(dom.marks.join(''),/attack-vector/);assert.match(dom.marks.join(''),/march-tank/);
 });
});
test('animazione aggiuntivi: aggiorna entrambi i contatori una sola volta',()=>{
 const {n}=conquest();const after=applyCommand(n,0,{type:'MOVE',from:0,to:1,count:2});
 timeline(makeTransition(n,after)!, (run,_cancel,dom)=>{
  run();assert.equal(dom.finished,1);
  assert.equal(dom.tokens[0].number.textContent,'7');assert.equal(dom.tokens[1].number.textContent,'5');
  assert.deepEqual(dom.tokens[1].seen,['3','5']);
 });
});
test('interruzione animazioni cancella i callback e non invia un secondo completamento',()=>{
 const {s,n}=conquest();
 timeline(makeTransition(s,n)!, (run,cancel,dom)=>{
  run(200);cancel();run();assert.equal(dom.finished,0);assert.equal(dom.clashes,0);assert.equal(dom.host.innerHTML,'');
  assert.deepEqual(dom.tokens[1].seen,['1']);
 });
});

test('I dadi si fermano a 350 ms; l’esito appare dopo il rotolamento e una sola volta',()=>{
 const {s,n}=conquest();timeline(makeTransition(s,n)!, (run,_cancel,dom)=>{
  run(349);assert.equal(dom.host.classList.contains('rolling'),true);assert.equal(dom.host.classList.contains('resolved'),false);assert.equal(dom.clashes,0);
  run(350);assert.equal(dom.host.classList.contains('rolling'),false);assert.equal(dom.host.classList.contains('resolved'),true);assert.equal(dom.clashes,1);
  run();assert.equal(dom.finished,1);assert.equal(dom.clashes,1);
 });
});

test('Filtro effetti indipendente dalla velocità: solo riepilogo salta i bot',()=>{
 const {s,n}=conquest();assert.equal(makeTransition(s,n,0)!.speed,2);
 s.players[0].human=false;n.players[0].human=false;
 assert.equal(makeTransition(s,n,0)!.speed,4);assert.equal(makeTransition(s,n,0,0,'summary'),null);assert.equal(makeTransition(s,n,2)!.speed,2);
});
