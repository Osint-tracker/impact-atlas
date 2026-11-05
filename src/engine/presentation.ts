import {project} from './RisikoRulesEngine.ts';
import type {GameState, GameView} from './types.ts';

export type VisualEffect =
  | {kind: 'battle'; player: number; defender: number; from: number; to: number; attack: number[]; defense: number[]; losses: number[]; conquered: boolean; transferred: number}
  | {kind: 'place'; player: number; to: number; count: number}
  | {kind: 'move'; player: number; from: number; to: number; count: number}
  | {kind: 'exchange'; player: number; count: number; cards?: number[]}
  | {kind: 'draw'; player: number; card?: number}
  | {kind: 'sdadata'; player: number; dice: number[]; threshold: number};
export type Transition = {
  id: string;
  before: GameView;
  after: GameView;
  effects: VisualEffect[];
  speed: number;
};
// Only public data or the viewer's own cards enter this transport.
export function makeTransition(before: GameState, after: GameState, speed = 1, viewer = 0, filter = 'all'): Transition | null {
  if (before.id !== after.id || before.revision === after.revision) return null;
  if(!before.players[before.active].human && filter==='summary')return null;
  if(!before.players[before.active].human && speed===0 && filter==='all')speed=4;
  if(before.players[before.active].human)speed=speed===0?2:Math.max(1,speed);
  const effects: VisualEffect[] = [];
  for (const e of after.log.slice(before.log.length)) {
    if (e.type === 'onDiceRolled' && e.from !== undefined && e.to !== undefined) {
      effects.push({kind: 'battle', player: e.player, defender: e.defender!, from: e.from, to: e.to,
        attack: e.attack!, defense: e.defense!, losses: e.losses!, conquered: after.board[e.to].owner !== before.board[e.to].owner,
        transferred: after.log.slice(before.log.length).find(x=>x.action==='conquer'&&x.to===e.to)?.count ?? 0});
    } else if ((e.action === 'reinforce'||e.action==='undo')) {
      effects.push({kind: 'place', player: e.player, to: e.territory!, count: e.count!});
    } else if (e.action === 'move') {
      effects.push({kind: 'move', player: e.player, from: e.from!, to: e.to!, count: e.count!});
    } else if (e.action === 'exchange') {
      effects.push({kind: 'exchange', player: e.player, count: e.count!, ...(e.player === viewer ? {
        cards: before.players[e.player].cards.filter(c => !after.players[e.player].cards.includes(c))
      } : {})});
    } else if (e.action === 'draw') {
      effects.push({kind: 'draw', player: e.player, ...(e.player === viewer ? {
        card: after.players[e.player].cards.find(c => !before.players[e.player].cards.includes(c))
      } : {})});
    } else if (e.type === 'onSdadataRoll' && e.attack?.length) {
      effects.push({kind: 'sdadata', player: e.player, dice: e.attack!, threshold: before.sdadata?.threshold ?? 4});
    }
  }
  // Timeout can distribute many armies. Merge its placements per territory and
  // cap the complete sequence below the watchdog, without omitting placements.
  const merged: VisualEffect[] = [];
  for (const e of effects) {
    if(!before.players[before.active].human&&filter==='involved'&&!(e.kind==='battle'&&(e.defender===viewer||e.conquered)))continue;
    const same=e.kind==='place'?merged.find(x=>x.kind==='place'&&x.player===e.player&&x.to===e.to):undefined;
    if(same?.kind==='place'&&e.kind==='place')same.count+=e.count;else merged.push(e);
  }
  return merged.length ? {id: `${after.id}:${after.revision}`, before: project(before, viewer), after: project(after, viewer), effects: merged, speed} : null;
}
export function sequenceScale(t: Transition) {
  const total=t.effects.reduce((sum,e)=>sum+effectDuration(e,t.speed),0);
  return Math.min(1,8000/Math.max(1,total));
}
export function effectDuration(e: VisualEffect, speed: number): number {
  const base = {battle: 1800, place: 360, move: 520, exchange: 850, draw: 1600, sdadata: 1500}[e.kind];
  return speed === 0 ? Math.min(base, e.kind === 'place' ? 80 : 320) : base / Math.max(1, speed);
}
/** One durable action at a time; watchdog prevents a lost UI from freezing play. */
export class PresentationGate {
  pending: {id: string; deadline: number} | null = null;
  begin(id: string, now: number) { this.pending = {id, deadline: now + 10000}; }
  finish(id: string) {
    if (this.pending?.id !== id) return false;
    this.pending = null;
    return true;
  }
  expired(now: number) { return !!this.pending && now >= this.pending.deadline; }
  clear() { this.pending = null; }
}
