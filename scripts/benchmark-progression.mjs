import {writeFileSync,mkdirSync} from 'node:fs';
import {createGame,applyCommand,project,random,ranks,assertState} from '../src/engine/RisikoRulesEngine.ts';
import {humanDecision} from '../src/ai/HumanBot.ts';
import {withDecisionBudget} from '../src/ai/Budget.ts';
import {arenaCommand} from './arena-opponents.mjs';
// Same initial position, objectives and dice stream, with candidates swapping
// seats. A third independent opponent prevents a pure mirror-play benchmark.
const pairs=Number(process.env.BENCH_PAIRS??60),offset=Number(process.env.BENCH_OFFSET??0),rows=[],timings=[];
if(!Number.isInteger(pairs)||pairs<1)throw Error('BENCH_PAIRS deve essere positivo');
const levels=['facile','medio','difficile','survival'];
for(let pair=0;pair<pairs;pair++){
 const lower=levels[pair%3],upper=levels[pair%3+1],seed=508101+pair+offset,antarctica=pair%2===1;
 for(let swap=0;swap<2;swap++){
  let s=createGame({players:3,name:'Arena',difficulties:['medio','medio'],turnSeconds:180,seed,antarctica});
  const contexts={},candidate=swap,opponent=1-swap;let actions=0;
  while(s.phase!=='GAME_OVER'&&actions<18000){
   const actor=s.active,view=project(s,actor),rngState={rng:s.aiRng};let command;
   if(actor===2)command=arenaCommand(view,['builder','raider','finisher'][pair%3]);
   else{const level=actor===candidate?upper:lower,start=performance.now(),d=withDecisionBudget(level,()=>humanDecision(view,level,()=>random(rngState),contexts[actor]));timings.push(performance.now()-start);contexts[actor]=d.context;command=d.command;}
   s=applyCommand(s,actor,command);s.aiRng=rngState.rng;actions++;
  }
  assertState(s);if(s.phase!=='GAME_OVER')throw Error(`Partita incompleta ${seed}/${swap}`);
  const ranking=ranks(s),up=ranking.find(r=>r.player===candidate),down=ranking.find(r=>r.player===opponent);
  rows.push({pair,seed,antarctica,upper,lower,swap,upperWon:s.winner===candidate,lowerWon:s.winner===opponent,upperPoints:up.points,lowerPoints:down.points,actions});
 }
 console.log(`Coppia ${pair+1}/${pairs}: ${upper} contro ${lower}`);
 mkdirSync('artifacts/ai',{recursive:true});writeFileSync('artifacts/ai/progression-progress.json',JSON.stringify(rows));
}
timings.sort((a,b)=>a-b);
const groups=levels.slice(1).map(upper=>{const games=rows.filter(r=>r.upper===upper),wins=games.filter(r=>r.upperWon).length,losses=games.filter(r=>r.lowerWon).length;return {upper,games:games.length,wins,lowerWins:losses,meanPointDifference:games.reduce((n,r)=>n+r.upperPoints-r.lowerPoints,0)/Math.max(1,games.length)};});
const report={note:'Partite accoppiate, posti scambiati, avversario indipendente. Budget di calcolo di produzione. Le vittorie non dimostrano forza contro persone; campioni piccoli non certificano la progressione.',pairs,groups,decisionMs:{p95:timings[Math.floor(timings.length*.95)],max:timings.at(-1)},rows};
writeFileSync('artifacts/ai/progression.json',JSON.stringify(report,null,2));console.log(JSON.stringify({...report,rows:undefined},null,2));
