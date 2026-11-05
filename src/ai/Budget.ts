import type {Difficulty} from '../engine/types.ts';
export const decisionBudgets:Record<Difficulty,number>={facile:15,medio:40,difficile:80,survival:140};
let deadline=Infinity;
export const budgetExpired=()=>performance.now()>=deadline;
/** Limit only live decisions. Deterministic benchmarks may supply a generous budget. */
export function withDecisionBudget<T>(level:Difficulty,run:()=>T,ms=decisionBudgets[level]):T{const previous=deadline;deadline=performance.now()+ms;try{return run();}finally{deadline=previous;}}
