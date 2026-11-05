import {budgetExpired} from './Budget.ts';
import {forcingAttacks,defensiveCommitments,criticalGarrison,hostileNeighbors} from './Tactics.ts';
import {mapFor,objectives} from '../data/mapGraph.ts';
import {owned,border,canAttack,moveBounds,validTris,reinforcement,total} from '../engine/RisikoRulesEngine.ts';
import {conquestOdds,diceDistribution,inferObjectiveWeights} from './BotAI.ts';
import type {BotMood} from './BotAI.ts';
import {strategicCommand as legacyCommand,conquestEstimate} from './LegacyStrategy.ts';
import {botPolicy} from './policy.ts';
import type {GameView,Command,Difficulty,BotPolicy,BotContext} from '../engine/types.ts';
export {conquestEstimate};
type Attack={from:number;to:number;score:number;win:number};
const clone=(v:GameView):GameView=>({...v,board:v.board.map(c=>({...c})),players:v.players.map(p=>({...p}))});
const goal=(v:GameView,id=v.active)=>objectives[(v.players[id].objective??0)-1]?.territories??[];
const enemies=(v:GameView,id:number)=>mapFor(v).territories[id].neighbors.filter(n=>v.board[n].owner!==v.board[id].owner);
const pressure=(v:GameView,id:number)=>Math.max(0,...enemies(v,id).map(n=>v.board[n].armies-1));
type Summary={campaign:number;counts:number[];armies:number[];income:number[];continentCounts:number[][];continentOf:number[];members:number[][]};
const summaries=new WeakMap<GameView,Summary>();
const geography=new Map<object,{continentOf:number[];members:number[][]}>();
function summary(v:GameView):Summary{
 const cached=summaries.get(v);if(cached)return cached;
 const map=mapFor(v);let geo=geography.get(map);
 if(!geo){geo={continentOf:map.territories.map(t=>map.continents.findIndex(c=>c.id===t.continent)),members:map.continents.map(c=>map.territories.filter(t=>t.continent===c.id).map(t=>t.id))};geography.set(map,geo);}
 const counts=v.players.map(()=>0),armies=counts.slice(),continentCounts=map.continents.map(()=>counts.slice());
 v.board.forEach((c,id)=>{counts[c.owner]++;armies[c.owner]+=c.armies;continentCounts[geo!.continentOf[id]][c.owner]++;});
 const income=counts.map((n,id)=>Math.floor(n/3)+map.continents.reduce((s,c,i)=>s+(continentCounts[i][id]===geo!.members[i].length?c.bonus:0),0));
 // Campaign target balances progress, border cost and recurring income.
 let campaign=-1,best=0;
 map.continents.forEach((c,i)=>{
  const held=continentCounts[i][v.active];if(!held||held===geo!.members[i].length)return;
  const missing=geo!.members[i].filter(id=>v.board[id].owner!==v.active);
  const cost=missing.reduce((n,id)=>n+v.board[id].armies+1,0);
  const score=c.bonus*(held/geo!.members[i].length)/cost;
  if(score>best){best=score;campaign=i;}
 });
 const result={...geo,campaign,counts,armies,income,continentCounts};summaries.set(v,result);return result;
}
function refresh(v:GameView){
 summaries.delete(v);const s=summary(v);
 for(const p of v.players){p.totalArmies=s.armies[p.id];p.territoryCount=s.counts[p.id];p.nextReinforcements=s.income[p.id];p.eliminated=s.counts[p.id]===0;}
}
function holding(v:GameView,id:number,p:BotPolicy):number{
 const {territories,continents}=mapFor(v),s=summary(v),ci=s.continentOf[id],c=continents[ci];
 const count=s.continentCounts[ci][v.active],length=s.members[ci].length;
 return 1+(goal(v).includes(id)?territories[id].points*(p.personality==='stratega'?.95:.65):0)+(p.continents?c.bonus*(count===length?4:count/length):0);
}
function gain(v:GameView,to:number,p:BotPolicy,mood?:BotMood):number{
 const {territories,continents}=mapFor(v),g=goal(v),enemy=v.board[to].owner;
 if(g.length&&g.every(id=>id===to||v.board[id].owner===v.active))return 10000;
 const s=summary(v),ci=s.continentOf[to],c=continents[ci],length=s.members[ci].length;
 const missing=length-s.continentCounts[ci][v.active],enemyCount=s.continentCounts[ci][enemy];
 let n=2+(g.includes(to)?territories[to].points*(p.personality==='stratega'?1.7:1.15):0);
 if(v.conquests===0&&v.players[v.active].cardCount<7)n+=p.personality==='incursore'?9:5;
 if(p.weakBorders)n+=Math.max(0,4-v.board[to].armies)*1.3;
 if(p.continents)n+=(p.personality==='conquistatore'?1.45:1)*c.bonus*(missing===1?9:missing<=3?3/missing:0)+(s.campaign===ci?c.bonus*2.5:0);
 if(p.breakBonus&&enemyCount===length)n+=c.bonus*(p.personality==='incursore'?10:7);
 if(p.disruptGrowth&&enemyCount<length&&enemyCount>=length-2)n+=c.bonus*2;
 if(p.eliminate&&s.counts[enemy]===1)n+=10+Math.min(7-v.players[v.active].cardCount,v.players[enemy].cardCount)*4;
 if(p.containLeader){const power=(id:number)=>s.armies[id]+s.income[id]*5;const max=Math.max(...v.players.filter(x=>x.id!==v.active).map(x=>power(x.id)));if(power(enemy)===max&&max>power(v.active)*1.15)n+=4;}
 if(mood?.reclaim===to)n+=4;
 return n;
}
/** Includes failure costs, the garrison left behind and the destination's exposure. */
export function rankAttacks(v:GameView,p=botPolicy(v.players[v.active].botPolicy),mood?:BotMood):Attack[]{
 const out:Attack[]=[];
 for(const from of owned(v,v.active))for(const to of mapFor(v).territories[from].neighbors){
  if(!canAttack(v,from,to))continue;
  const a=v.board[from].armies,d=v.board[to].armies,e=conquestEstimate(a,d);
  const value=gain(v,to,p,mood);
  // Normal campaigns must preserve force. Rare desperate sorties are handled separately.
  const risk={equilibrato:.65,stratega:.72,incursore:.59,conquistatore:.63}[p.personality??'equilibrato'];
  if(e.win<(value>=50?.48:risk))continue;
  const remaining=Math.max(1,Math.floor(e.survivors)-1);
  let score=e.win*value-(1-e.win)*(a-1)*1.2-(a-e.survivors)*1.1;
  if(p.reserve){
   const reserveFactor=p.personality==='stratega'?1.35:p.personality==='incursore'?.8:1;
   const other=enemies(v,from).filter(id=>id!==to);
   const risk=Math.max(0,...other.map(id=>conquestOdds(v.board[id].armies,2)));
   score-=risk*holding(v,from,p)*.35*reserveFactor;
   score-=Math.max(0,...hostileNeighbors(v,to).filter(id=>id!==from).map(id=>conquestOdds(v.board[id].armies,remaining)))*gain(v,to,p)*.22;
  }
  out.push({from,to,score,win:e.win});
 }
 return out.sort((a,b)=>b.score-a.score||a.from-b.from||a.to-b.to);
}
/** Whole reserve investments, not one-army marginal improvements. Saved until consumed. */
function greedyReinforcementPlan(v:GameView):number[]{
 summaries.delete(v);
 const p=botPolicy(v.players[v.active].botPolicy),w=clone(v),plan=defensiveCommitments(v);
 for(const id of plan)w.board[id].armies++;refresh(w);
 let left=(v.phase==='SETUP_PLACEMENT'?Math.min(v.setupBatch,v.players[v.active].setupRemaining):v.reinforcements)-plan.length;
 while(left>0){
  let best={id:owned(w,w.active)[0],count:1,score:-Infinity};
  for(const id of owned(w,w.active)){
   const army=w.board[id].armies,threat=pressure(w,id);
   // Bundle defence as well: crossing from one die to three is valuable.
   if(p.defend&&threat>0){
    const need=Math.min(left,Math.max(1,Math.ceil(threat*.65)+1-army));
    const before=conquestOdds(threat+1,army),after=conquestOdds(threat+1,army+need);
    const score=(before-after)*holding(w,id,p)*5/Math.pow(need,.6);
    if(score>best.score)best={id,count:need,score};
   }
   if(w.round>p.graceRounds||w.phase==='SETUP_PLACEMENT')for(const to of enemies(w,id)){
    if(w.round<=4&&owned(w,w.board[to].owner).length===1)continue;
    const before=conquestOdds(army,w.board[to].armies);
    // Evaluate all affordable bundles, including a currently illegal attack.
    for(let n=1;n<=left;n++){
     const e=conquestEstimate(army+n,w.board[to].armies);
     if(e.win<.6)continue;
     const score=((e.win-before)*gain(w,to,p)*2+e.win*2)/Math.pow(n,.6);
     if(score>best.score)best={id,count:n,score};
    }
   }
   if(best.score===-Infinity||(!border(w,best.id)&&border(w,id)))best={id,count:1,score:border(w,id)?1:0};
  }
  if(budgetExpired())best.count=left;
  plan.push(...Array(best.count).fill(best.id));w.board[best.id].armies+=best.count;summaries.delete(w);left-=best.count;
 }
 return plan;
}
/** Compare complete deployment plans with actual stochastic campaigns. */
export function reinforcementPlan(v:GameView,level:Difficulty):number[]{
 const base=greedyReinforcementPlan(v);
 if(level==='medio'||v.phase==='SETUP_PLACEMENT'||base.length<3||v.round<=botPolicy(v.players[v.active].botPolicy).graceRounds)return base;
 const p=botPolicy(v.players[v.active].botPolicy),front=owned(v,v.active).filter(id=>border(v,id));
 const protectedPlan=defensiveCommitments(v),free=base.length-protectedPlan.length;
 const candidates=[base];
 for(const id of front){
  candidates.push([...protectedPlan,...Array(free).fill(id)]);
  const reserve=protectedPlan.length+Math.floor(free*.3);
  candidates.push([...base.slice(0,reserve),...Array(base.length-reserve).fill(id)]);
 }
 // Discard duplicate allocations; placement ordering does not change a campaign.
 const seen=new Set<string>(),unique=candidates.filter(plan=>{const key=plan.slice().sort((a,b)=>a-b).join(',');if(seen.has(key))return false;seen.add(key);return true;});
 const deployed=unique.map(plan=>{const view=clone(v);for(const id of plan)view.board[id].armies++;refresh(view);return {plan,view};});
 const samples=level==='survival'?24:12,depth=level==='survival'?8:5;
 // First prune only by an inexpensive tactical estimate, retaining the defensive baseline.
 const shortlist=deployed.map((a,i)=>({...a,index:i,score:position(a.view,v.active,p)+Math.max(0,rankAttacks(a.view,p)[0]?.score??0)*.6})).sort((a,b)=>b.score-a.score||a.index-b.index).slice(0,level==='survival'?8:5);
 if(!shortlist.some(a=>a.index===0))shortlist.push({...deployed[0],index:0,score:0});
 let best=base,bestScore=-Infinity;
 const sums=shortlist.map(()=>0),squares=shortlist.map(()=>0);let completed=0;
 for(let sample=0;sample<samples;sample++){
  const values:number[]=[];
  for(const choice of shortlist){
   if(budgetExpired())break;
   const w=clone(choice.view),random=rngFrom(0x51f15e+v.round*7919+sample*104729);
   for(let step=0;step<depth;step++){
    if(budgetExpired())break;
    const attack=rankAttacks(w,p)[0];if(!attack||attack.score<=0)break;fight(w,attack,random);
    if(goal(w).every(id=>w.board[id].owner===v.active))break;
   }
   values.push(position(w,v.active,p)+(w.conquests>0&&v.players[v.active].cardCount<7?5:0));
  }
  if(budgetExpired()||values.length!==shortlist.length)break;
  completed++;values.forEach((value,i)=>{sums[i]+=value;squares[i]+=value*value;});
 }
 if(completed)shortlist.forEach((choice,i)=>{const mean=sums[i]/completed,score=mean-Math.sqrt(Math.max(0,squares[i]/completed-mean*mean))*.15;if(score>bestScore){bestScore=score;best=choice.plan;}});
 return best;
}
function occupation(v:GameView,from:number,to:number,min:number,max:number,p:BotPolicy):number{
 let best=min,score=-Infinity;
 const fronts=[enemies(v,from),enemies(v,to)];
 const values=fronts.map(list=>list.map(id=>gain(v,id,p)));
 const importance=[holding(v,from,p),holding(v,to,p)];
 max=Math.max(min,Math.min(max,v.board[from].armies-criticalGarrison(v,from,p)));
 for(let n=min;n<=max;n++){
  const armies=[v.board[from].armies-n,v.board[to].armies+n];
  const risk=fronts.map((list,i)=>Math.max(0,...list.map(e=>conquestOdds(v.board[e].armies,armies[i])))*importance[i]);
  const offense=fronts.map((list,i)=>Math.max(0,...list.map((e,j)=>conquestOdds(armies[i],v.board[e].armies)*values[i][j])));
  const value=Math.max(...offense)+Math.min(...offense)*.25-(p.reserve||p.defend?(risk[0]+risk[1])*1.8:0)+(fronts[1].length?n*.015:0);
  if(value>score){score=value;best=n;}
 }
 return best;
}
function rngFrom(seed:number){let n=seed>>>0||1;return()=>{n^=n<<13;n^=n>>>17;n^=n<<5;return(n>>>0)/4294967296;};}
/** Sample actual 3-dice rules, including failed campaigns. No engine RNG or private state. */
function fight(v:GameView,a:Attack,rng:()=>number):boolean{
 const source=v.board[a.from],target=v.board[a.to],defender=target.owner;
 while(canAttack(v,a.from,a.to)){
  const ad=Math.min(3,source.armies-1),dd=Math.min(3,target.armies),r=rng();let cumulative=0;
  const dist=diceDistribution(ad,dd);let result=dist[dist.length-1];
  for(const x of dist){cumulative+=x.probability;if(r<cumulative){result=x;break;}}
  source.armies-=result.attacker;target.armies-=result.defender;
  if(target.armies===0){
   target.owner=v.active;source.armies-=ad;target.armies=ad;v.conquests++;
   summaries.delete(v);
   const max=Math.max(0,source.armies-(border(v,a.from)?2:1));
   const extra=occupation(v,a.from,a.to,0,max,botPolicy(v.players[v.active].botPolicy));source.armies-=extra;target.armies+=extra;
   refresh(v);
   if(!v.players[defender].territoryCount){v.players[v.active].cardCount=Math.min(7,v.players[v.active].cardCount+v.players[defender].cardCount);v.players[defender].cardCount=0;}
   return true;
  }
  // The live planner reassesses after every roll: a simulation must not force a doomed blitz.
  const value=gain(v,a.to,botPolicy(v.players[v.active].botPolicy));
  if(conquestOdds(source.armies,target.armies)<(value>=50?.48:.65))break;
 }
 refresh(v);return false;
}
function position(v:GameView,actor:number,p:BotPolicy):number{
 const g=goal(v,actor),own=owned(v,actor),points=own.reduce((n,id)=>n+(g.includes(id)?mapFor(v).territories[id].points:0),0);
 if(g.length&&g.every(id=>v.board[id].owner===actor))return 10000;
 let value=points*(p.personality==='stratega'?1.7:1.35)+own.length*1.7+total(v,actor)*.8;
 if(p.continents){
  value+=Math.floor(own.length/3)*6;
  const map=mapFor(v);
  for(const c of map.continents){
   const members=map.territories.filter(t=>t.continent===c.id).map(t=>t.id);
   if(members.every(id=>v.board[id].owner===actor)){
    const retention=Math.min(1,...members.map(id=>1-Math.max(0,...enemies(v,id).map(n=>conquestOdds(v.board[n].armies,v.board[id].armies)))));
    value+=c.bonus*(5+retention*10);
   }
  }
 }
 if(p.breakBonus)value-=v.players.filter(x=>x.id!==actor).reduce((n,x)=>n+Math.max(0,reinforcement(v,x.id)-Math.floor(owned(v,x.id).length/3))*2,0);
 if(p.reserve||p.defend)for(const id of own){const risk=Math.max(0,...enemies(v,id).map(n=>conquestOdds(v.board[n].armies,v.board[id].armies)));value-=risk*(g.includes(id)?2:1);}
 return value;
}
/** Public-information reply model: independent opponents, sampled goals from visible moves. */
function replies(v:GameView,actor:number,rng:()=>number,weights:number[][],depth:number){
 const index=v.order.indexOf(actor),order=[...v.order.slice(index+1),...v.order.slice(0,index)];
 const assignedGoals=new Set([v.players[actor].objective]);
 const availableCards=mapFor(v).cards.map(c=>c.id).filter(id=>!v.players[actor].cards?.includes(id));
 for(const id of order){
  if(!owned(v,id).length)continue;
  v.active=id;v.conquests=0;summaries.delete(v);
  const allowed=weights[id].map((w,i)=>assignedGoals.has(i+1)?0:w),sum=allowed.reduce((a,b)=>a+b,0);
  let r=rng()*sum,objective=0;for(;objective<15;objective++){r-=allowed[objective];if(r<0)break;}
  v.players[id].objective=objective+1;assignedGoals.add(objective+1);
  // Sample unknown cards from public hand sizes; never inspect another player's hand.
  const hand:number[]=[];
  for(let i=0;i<v.players[id].cardCount&&availableCards.length;i++)hand.push(availableCards.splice(Math.floor(rng()*availableCards.length),1)[0]);
  const trade=validTris(v,id,hand).sort((a,b)=>b.value-a.value)[0]?.value??0;
  const policy=botPolicy(v.players[id].botPolicy);
  const fronts=owned(v,id).filter(t=>border(v,t));
  if(!fronts.length)continue;
  // Reinforce a promising front, respecting the public 130-army ceiling.
  const income=Math.min(130-summary(v).armies[id],summary(v).income[id]+trade);
  const target=fronts.map(from=>({from,score:Math.max(...enemies(v,from).map(to=>gain(v,to,policy)*conquestOdds(v.board[from].armies+income,v.board[to].armies)))})).sort((a,b)=>b.score-a.score||a.from-b.from)[0].from;
  v.board[target].armies+=income;refresh(v);
  if(v.round<=policy.graceRounds)continue;
  for(let step=0;step<depth;step++){const best=rankAttacks(v,policy)[0];if(!best||best.score<=1)break;fight(v,best,rng);}
 }
 v.active=actor;summaries.delete(v);
}
export type DecisionTrace={command:Command;alternatives:{from:number;to:number;score:number;win:number;simulation?:number}[];samples:number};
/** Bounded stochastic lookahead, same random streams for attack and holding comparisons. */
export function attackDecision(v:GameView,level:Difficulty,rng:()=>number,mood?:BotMood):DecisionTrace{
 summaries.delete(v);
 const p=botPolicy(v.players[v.active].botPolicy);
 if(v.round<=p.graceRounds)return {command:{type:'NEXT'},alternatives:[],samples:0};
 const forcing=forcingAttacks(v);
 if(forcing.length)return {command:{type:'ATTACK',from:forcing[0].from,to:forcing[0].to},alternatives:forcing,samples:0};
 const roots=rankAttacks(v,p,mood).slice(0,level==='survival'?6:level==='difficile'?4:3);
 // Medium uses a short chance-weighted continuation; deeper levels sample actual branches.
 if(level==='medio'){
 const baseline=roots.map(a=>{
  const w=clone(v),e=conquestEstimate(v.board[a.from].armies,v.board[a.to].armies);
  w.board[a.from].armies=border(v,a.from)?2:1;
  w.board[a.to]={owner:v.active,armies:Math.max(1,Math.floor(e.survivors)-w.board[a.from].armies)};w.conquests++;refresh(w);
  const next=rankAttacks(w,p).find(x=>x.from===a.to);
  return {...a,score:a.score+e.win*Math.max(0,next?.score??0)*.3};
 }).sort((a,b)=>b.score-a.score||a.from-b.from||a.to-b.to);
  const best=baseline[0];return {command:best?.score>0?{type:'ATTACK',from:best.from,to:best.to}:{type:'NEXT'},alternatives:baseline,samples:0};
 }
 if(!roots.length)return {command:{type:'NEXT'},alternatives:[],samples:0};
 const samples=level==='survival'?48:24,depth=level==='survival'?8:5,seed=Math.floor(rng()*0xffffffff);
 const weights=v.players.map(x=>inferObjectiveWeights(v,x.id));
 const sums=roots.map(()=>[0,0]),squares=roots.map(()=>[0,0]);let completed=0;
 for(let i=0;i<samples;i++){
  if(budgetExpired())break;
  const holdingView=clone(v);replies(holdingView,v.active,rngFrom(seed+i*7919),weights,level==='survival'?2:1);const hold=position(holdingView,v.active,p);
  const batch:number[][]=[];
  for(const a of roots){
   if(budgetExpired())break;
   const first=clone(v),random=rngFrom(seed+i*7919),firstCapture=fight(first,a,random)?1:0,values:number[]=[];
   for(const policy of [0,1]){
    const w=clone(first);let captured=firstCapture;
    if(policy===1)for(let j=1;j<depth;j++){if(budgetExpired())break;const next=rankAttacks(w,p)[0];if(!next||next.score<=1)break;if(fight(w,next,random))captured++;if(goal(w).every(id=>w.board[id].owner===v.active))break;}
    if(goal(w).every(id=>w.board[id].owner===v.active)){values.push(10000);continue;}
    const card=v.conquests===0&&captured&&w.players[v.active].cardCount<7?5:0;
    replies(w,v.active,rngFrom(seed+i*7919),weights,level==='survival'?2:1);
    values.push(position(w,v.active,p)-hold+card);
   }
   batch.push(values);
  }
  if(budgetExpired()||batch.length!==roots.length)break;
  completed++;batch.forEach((values,r)=>values.forEach((value,p)=>{sums[r][p]+=value;squares[r][p]+=value*value;}));
 }
 const alternatives=roots.map((a,r)=>{
  if(!completed)return {...a,simulation:a.score};
  const simulation=Math.max(...sums[r].map((sum,i)=>{const mean=sum/completed;return mean-Math.sqrt(Math.max(0,squares[r][i]/completed-mean*mean))*.12;}));
  return {...a,simulation,score:a.score*.45+simulation*.55};
 }).sort((a,b)=>b.score-a.score||a.from-b.from||a.to-b.to);
 const best=alternatives[0];
 return {command:best.score>0?{type:'ATTACK',from:best.from,to:best.to}:{type:'NEXT'},alternatives,samples:completed};
}
function frontDistances(v:GameView){
 const distances=v.board.map(()=>Infinity),queue=owned(v,v.active).filter(id=>border(v,id));queue.forEach(id=>distances[id]=0);
 for(let i=0;i<queue.length;i++)for(const n of mapFor(v).territories[queue[i]].neighbors)if(v.board[n].owner===v.active&&distances[n]>distances[queue[i]]+1){distances[n]=distances[queue[i]]+1;queue.push(n);}
 return distances;
}
export function strategicCommand(v:GameView,level:Difficulty,rng:()=>number,mood?:BotMood,context?:BotContext):Command{
 summaries.delete(v);
 if(level==='facile')return legacyCommand(v,level,rng,mood);
 const p=botPolicy(v.players[v.active].botPolicy);
 if(v.phase==='TRIS_EXCHANGE'){
  const combos=validTris(v,v.active,v.players[v.active].cards??[]).sort((a,b)=>b.value-a.value);
  return combos.length&&total(v,v.active)+v.reinforcements<130?{type:'EXCHANGE',cards:combos[0].cards}:{type:'NEXT'};
 }
 if(v.phase==='SETUP_PLACEMENT'||v.phase==='REINFORCEMENT'){
  const count=v.phase==='SETUP_PLACEMENT'?Math.min(v.setupBatch,v.players[v.active].setupRemaining):v.reinforcements;
  if(count<=0)return {type:'NEXT'};
  let plan=context?.plan;
  if(!plan||plan.round!==v.round||plan.phase!==v.phase||plan.placements.length!==count||plan.placements.some(id=>v.board[id]?.owner!==v.active))plan={round:v.round,phase:v.phase,placements:reinforcementPlan(v,level)};
  if(context)context.plan=plan;
  return {type:'PLACE',territory:plan.placements.shift()!,count:1};
 }
 if(v.phase==='ATTACK')return attackDecision(v,level,rng,mood).command;
 if(v.phase==='POST_CONQUEST_MOVE'){
  const {from,to}=v.pending!,bounds=moveBounds(v,from,to);
  return {type:'MOVE',from,to,count:occupation(v,from,to,bounds.min,bounds.max,p)};
 }
 if(v.phase==='FORTIFY'){
  const distances=frontDistances(v);let best:{from:number;to:number;count:number;score:number}|undefined;
  for(const from of owned(v,v.active))for(const to of mapFor(v).territories[from].neighbors){
   const bounds=moveBounds(v,from,to);if(bounds.max<bounds.min)continue;
   const safe=p.reserve&&border(v,from)?Math.min(bounds.max,Math.max(0,v.board[from].armies-Math.max(2,Math.ceil(pressure(v,from)*.65)+1))):bounds.max;
   if(safe<1)continue;
   const toward=distances[from]>distances[to];
   const attackValue=(army:number)=>Math.max(0,...enemies(v,to).map(e=>conquestOdds(army,v.board[e].armies)*gain(v,e,p)));
   const defense=p.defend?(conquestOdds(pressure(v,to)+1,v.board[to].armies)-conquestOdds(pressure(v,to)+1,v.board[to].armies+safe))*holding(v,to,p)*2:0;
   const score=attackValue(v.board[to].armies+safe)-attackValue(v.board[to].armies)+defense+(toward?safe*.65:0)-(border(v,from)?safe*.15:0);
   if(score>0&&(!best||score>best.score))best={from,to,count:safe,score};
  }
  return best?{type:'MOVE',from:best.from,to:best.to,count:best.count}:{type:'NEXT'};
 }
 return {type:'NEXT'};
}
