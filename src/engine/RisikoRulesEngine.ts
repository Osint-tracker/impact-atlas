import {recordAnalysis} from './Analysis.ts';
import {recordTurn} from './Replay.ts';
import {botPolicy} from '../ai/policy.ts';
import { allTerritories as territories, objectives, colors, objectivePoints, mapFor } from '../data/mapGraph.ts';
import type { GameConfig, GameState, GameView, Command, GameEvent, BoardCell } from './types.ts';
export class RuleError extends Error {}
const requireRule = (condition: unknown, message: string) => {
  if (!condition) throw new RuleError(message);
};
export function random(s: {
  rng: number;
}): number {
  let x = s.rng >>> 0;
  x ^= x << 13;
  x ^= x >>> 17;
  x ^= x << 5;
  s.rng = x >>> 0;
  return s.rng / 4294967296;
}
export function shuffle<T>(s: {
  rng: number;
}, items: T[]): T[] {
  const a = [...items];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(random(s) * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
export const owned = (s: Pick<GameState, 'board'>, p: number) => s.board.flatMap((t, i) => t.owner === p ? [i] : []);
export const total = (s: Pick<GameState, 'board'>, p: number) => s.board.reduce((n, t) => n + (t.owner === p ? t.armies : 0), 0);
export function reinforcement(s: Pick<GameState, 'board'>, p: number): number {
  const {territories,continents}=mapFor(s);
  const own = owned(s, p);
  return Math.floor(own.length / 3) + continents.reduce((n, c) => n + (territories.filter(t => t.continent === c.id).every(t => s.board[t.id].owner === p) ? c.bonus : 0), 0);
}
export function border(s: Pick<GameState, 'board'>, t: number): boolean {
  return mapFor(s).territories[t].neighbors.some(n => s.board[n].owner !== s.board[t].owner);
}
export function exchangeValue(s: Pick<GameState, 'board'>, p: number, hand: number[]): number {
  const {cards}=mapFor(s);
  if (hand.length !== 3 || new Set(hand).size !== 3 || hand.some(c => !cards[c])) return 0;
  const symbols = hand.map(c => cards[c].symbol),
    j = symbols.filter(x => x === 'jolly').length;
  const regular = symbols.filter(x => x !== 'jolly');
  let base = 0;
  if (j === 1 && regular[0] === regular[1]) base = 12;
  if (j === 0) base = new Set(regular).size === 1 ? 8 : new Set(regular).size === 3 ? 10 : 0;
  return base ? base + hand.filter(c => cards[c].territory !== null && s.board[cards[c].territory!].owner === p).length * 2 : 0;
}
export function validTris(s: Pick<GameState, 'board'>, p: number, hand: number[]): {
  cards: number[];
  value: number;
}[] {
  const out = [];
  for (let a = 0; a < hand.length; a++) for (let b = a + 1; b < hand.length; b++) for (let c = b + 1; c < hand.length; c++) {
    const combo = [hand[a], hand[b], hand[c]],
      value = exchangeValue(s, p, combo);
    if (value) out.push({
      cards: combo,
      value
    });
  }
  return out;
}
function event(s: GameState, type: GameEvent['type'], message: string, extra: Partial<GameEvent> = {}) {
  const e = {
    type,
    player: s.active,
    message,
    ...extra
  };
  s.log.push(e);
  return e;
}
function observe(s: GameState, p: number, t: number, weight: number) {
  const belief = s.beliefs[p] ?? Array(16).fill(0);
  for (let i = 0; i < 16; i++) belief[i] += objectives[i].territories.includes(t) ? weight : -weight / 2;
  s.beliefs[p] = belief;
}
function die(s: GameState) {
  return 1 + Math.floor(random(s) * 6);
}
export function createGame(config: GameConfig): GameState {
  requireRule([3, 4, 5].includes(config.players), 'Il tavolo deve avere 3, 4 o 5 giocatori.');
  requireRule(Number.isInteger(config.turnSeconds) && (config.turnSeconds >= 60 && config.turnSeconds <= 600 || config.custom === true && config.turnSeconds === 0), 'Tempo turno non valido.');
  requireRule(config.difficulties.length === config.players - 1 && config.difficulties.every(d => ['facile', 'medio', 'difficile', 'survival', 'casuale'].includes(d)), 'Difficoltà non valida.');
  requireRule(config.antarctica===undefined||typeof config.antarctica==='boolean', 'Modalità non valida.');
  const {territories,continents,cards}=mapFor(config);
  requireRule(!config.endMode||['regular','countdown'].includes(config.endMode),'Fine partita non valida.');
  requireRule(config.countdownMinutes===undefined||[30,45,60].includes(config.countdownMinutes),'Durata Countdown non valida.');
  requireRule(typeof config.name==='string','Nome non valido.');
  requireRule(config.seed===undefined||Number.isInteger(config.seed)&&config.seed>=0&&config.seed<=0xffffffff,'Seed non valido.');
  requireRule(config.seedLabel===undefined||typeof config.seedLabel==='string'&&config.seedLabel.length<=20000,'Codice partita non valido.');
  if(config.participants){requireRule(Array.isArray(config.participants)&&config.participants.every(p=>p&&typeof p.human==='boolean'&&typeof p.name==='string'&&(p.profileId===undefined||typeof p.profileId==='string')&&(p.difficulty===undefined||['facile','medio','difficile','survival','casuale'].includes(p.difficulty))),'Partecipanti non validi.');requireRule(config.participants.length===config.players&&config.participants.some(p=>p.human),'Serve almeno un umano.');const ids=config.participants.filter(p=>p.human&&p.profileId).map(p=>p.profileId);requireRule(new Set(ids).size===ids.length,'Un profilo non può occupare due posti.');}
  const rng = (config.seed ?? Math.floor(Math.random() * 4294967295)) >>> 0 || 1;
  const s: GameState = {
    version: 2,
    initialSeed: rng, seedLabel: config.seedLabel??String(rng), custom: config.custom===true, replay: [],
    manualReinforcement: config.manualReinforcement===true,
    endMode: config.endMode??'regular',
    matchRemainingMs: (config.countdownMinutes??45)*60000,
    placementHistory: [],
    scoreHistory: [],
    antarctica: config.antarctica===true,
    id: crypto.randomUUID(),
    revision: 0,
    rng,
    aiRng: (rng ^ 0x9e3779b9) >>> 0 || 1,
    players: [],
    order: [],
    active: 0,
    round: 0,
    phase: 'SETUP_PLACEMENT',
    board: [],
    deck: [],
    discard: [],
    reinforcements: 0,
    setupBatch: 3,
    conquests: 0,
    pending: null,
    lastBattle: null,
    lastSdadata: null,
    sdadata: null,
    turnSeconds: config.turnSeconds,
    remainingMs: config.turnSeconds * 1000,
    elapsedMs: 0,
    paused: false,
    reason: null,
    winner: null,
    log: [],
    beliefs: {},
    lastCommandId: null
  };
  const objectiveDeck = shuffle(s, objectives.map(o => o.id));
  s.players = Array.from({
    length: config.players
  }, (_, id) => {
    const participant=config.participants?.[id];
    const requested = participant?.difficulty ?? (id === 0 ? 'medio' : config.difficulties[id - 1]);
    return {
      id,
      name: participant?.name.trim().slice(0,24) || (id === 0 ? config.name.trim().slice(0, 24) || 'Comandante' : ['', 'Falco', 'Vega', 'Atlas', 'Ombra'][id]),
      profileId:participant?.profileId ?? (id===0?'local':''),
      color: colors[id],
      human: participant?.human ?? id === 0,
      difficulty: requested === 'casuale' ? (['facile', 'medio', 'difficile'] as const)[Math.floor(random(s) * 3)] : requested,
      blind: requested === 'casuale',
      objective: objectiveDeck[id],
      cards: [],
      setupRemaining: ({
        3: 35,
        4: 30,
        5: 25
      } as Record<number, number>)[config.players],
      botPolicy: botPolicy(config.botPolicies?.[id===0?4:id-1]),
      eliminated: false
    };
  });
  s.order = shuffle(s, s.players.map(p => p.id));
  // Rejection sampling gives an unbiased balanced deal conditional on the continent cap.
  let dealt = false;
  for (let attempt = 0; attempt < 100000 && !dealt; attempt++) {
    const deck = shuffle(s, territories.map(t => t.id));
    const board: BoardCell[] = Array(territories.length);
    const counts = s.players.map(() => Object.fromEntries(continents.map(c => [c.id, 0])));
    let valid = true;
    for (let i = 0; i < territories.length; i++) {
      const p = s.order[config.players - 1 - i % config.players],
        t = territories[deck[i]];
      counts[p][t.continent]++;
      if (counts[p][t.continent] > Math.floor(territories.filter(x => x.continent === t.continent).length / 2)) {
        valid = false;
        break;
      }
      board[t.id] = {
        owner: p,
        armies: 1
      };
    }
    if (valid) {
      s.board = board;
      dealt = true;
    }
  }
  requireRule(dealt, 'Impossibile distribuire i territori con il seme indicato.');
  for (const p of s.players) p.setupRemaining -= owned(s, p.id).length;
  s.deck = shuffle(s, cards.map(c => c.id));
  s.active = s.order[0];
  event(s, 'onGameStateUpdate', 'Territori assegnati. Piazzamento iniziale: tre armate per mano.');
  return s;
}
export function canAttack(s: Pick<GameState, 'board' | 'round' | 'active'>, from: number, to: number): boolean {
  const a = s.board[from],
    d = s.board[to];
  if (!a || !d || a.owner !== s.active || d.owner === s.active || !mapFor(s).territories[from].neighbors.includes(to)) return false;
  if (s.round <= 4 && owned(s, d.owner).length === 1) return false;
  return a.armies > 1 && Math.min(3, a.armies - 1) >= Math.min(3, d.armies);
}
export function moveBounds(s: Pick<GameState, 'board' | 'pending' | 'phase' | 'active'>, from: number, to: number): {
  min: number;
  max: number;
} {
  const a = s.board[from],
    b = s.board[to];
  if (!a || !b || a.owner !== s.active || b.owner !== s.active || !mapFor(s).territories[from].neighbors.includes(to)) return {
    min: 1,
    max: 0
  };
  const min = s.phase === 'POST_CONQUEST_MOVE' && s.pending?.from === from && s.pending?.to === to ? (s.pending.transferred === undefined ? s.pending.min : 0) : 1;
  const max = a.armies - (border(s, from) ? 2 : 1);
  // Only the compulsory conquest transfer may leave one on an enemy border.
  return {
    min,
    max: s.phase === 'POST_CONQUEST_MOVE' ? Math.max(min, max) : max
  };
}
export function ranks(s: GameState) {
  return s.players.map(p => {
    const own = owned(s, p.id),
      points = s.reason === 'risiko' && s.winner === p.id ? 100 : objectivePoints(p.objective, own);
    return {
      player: p.id,
      points,
      outside: own.filter(t => !objectives[p.objective - 1].territories.includes(t)).reduce((n, t) => n + territories[t].points, 0),
      territories: own.length,
      cards: p.cards.length
    };
  }).sort((a, b) => b.points - a.points || b.outside - a.outside || s.order.indexOf(b.player) - s.order.indexOf(a.player));
}
function recordScore(s:GameState){
 const points=s.players.map(p=>objectivePoints(p.objective,owned(s,p.id))),last=s.scoreHistory?.at(-1);
 if(last?.round===s.round&&last.player===s.active&&last.points.every((n,i)=>n===points[i]))return;
 (s.scoreHistory??=[]).push({round:s.round,player:s.active,points});
}
function finish(s: GameState, reason: 'risiko' | 'sdadata' | 'countdown', winner: number | null = null) {
  recordScore(s);
  s.placementHistory=[];
  s.reason = reason;
  s.winner = winner;
  s.phase = 'GAME_OVER';
  s.pending = null;
  if (winner === null) s.winner = ranks(s)[0].player;
  event(s, 'onGameOver', `${s.players[s.winner!].name} vince ${reason === 'risiko' ? 'per RisiKo!' : reason==='countdown'?'al termine del Countdown':'alla sdadata'}.`);
}
function checkObjective(s: GameState) {
  if (s.phase !== 'SETUP_PLACEMENT' && s.phase !== 'GAME_OVER' && objectives[s.players[s.active].objective - 1].territories.every(t => s.board[t].owner === s.active)) finish(s, 'risiko', s.active);
}
function startTurn(s: GameState) {
  s.placementHistory=[];
  s.phase = 'TRIS_EXCHANGE';
  s.conquests = 0;
  s.pending = null;
  s.remainingMs = s.turnSeconds * 1000;
  s.reinforcements = Math.min(130 - total(s, s.active), reinforcement(s, s.active));
  s.turnReinforcements=s.reinforcements;
  event(s, 'onTurnChanged', `${s.players[s.active].name}: giro ${s.round}, ${s.reinforcements} rinforzi.`);
  checkObjective(s);
}
function endTurn(s: GameState) {
  recordScore(s);
  s.placementHistory=[];
  s.phase = 'CARD_DRAW';
  const p = s.players[s.active];
  if (s.conquests > 0 && p.cards.length < 7) {
    if (!s.deck.length) {
      s.deck = shuffle(s, s.discard);
      s.discard = [];
    }
    if (s.deck.length) {
      p.cards.push(s.deck.pop()!);
      event(s, 'onGameStateUpdate', `${p.name} pesca una carta (${p.cards.length}/7).`, {action: 'draw'});
    }
  }
  const index = s.order.indexOf(s.active),
    hasLater = s.order.slice(index + 1).some(id => !s.players[id].eliminated);
  if (s.endMode!=='countdown' && !s.sdadata && s.round >= 20 && !hasLater) s.sdadata = {
    started: true,
    anchor: s.active,
    threshold: 4
  };
  if (s.sdadata) {
    s.phase = 'SDADATA_CHECK';
    if (s.conquests <= 2) {
      const dice = [die(s), die(s)],
        sum = dice[0] + dice[1],
        closed = sum <= s.sdadata.threshold;
      s.lastSdadata = event(s, 'onSdadataRoll', `${p.name}: sdadata ${dice[0]} + ${dice[1]} = ${sum}, chiusura ≤ ${s.sdadata.threshold}. ${closed ? 'Partita conclusa.' : 'Si continua.'}`, {
        attack: dice
      });
      if (closed) {
        finish(s, 'sdadata');
        return;
      }
    } else event(s, 'onSdadataRoll', `${p.name}: ${s.conquests} conquiste, sdadata bloccata.`);
  }
  let nextIndex = index;
  do {
    nextIndex = (nextIndex + 1) % s.order.length;
    if (nextIndex === 0) s.round++;
  } while (s.players[s.order[nextIndex]].eliminated);
  // Threshold cycles follow the fixed initial seating anchor, even if its player is eliminated.
  if (s.sdadata) {
    let cursor = index;
    while (cursor !== nextIndex) {
      cursor = (cursor + 1) % s.order.length;
      if (cursor === s.order.indexOf(s.sdadata.anchor)) s.sdadata.threshold = Math.min(7, s.sdadata.threshold + 1);
    }
  }
  s.active = s.order[nextIndex];
  startTurn(s);
}
function place(s: GameState, t: number, count: number) {
  requireRule(Number.isInteger(t) && s.board[t]?.owner === s.active, 'Scegli un tuo territorio.');
  requireRule(Number.isInteger(count) && count > 0, 'Numero di armate non valido.');
  const p = s.players[s.active];
  requireRule(total(s, s.active) + count <= 130, 'Limite di 130 carri.');
  if (s.phase === 'SETUP_PLACEMENT') {
    requireRule(count <= Math.min(s.setupBatch, p.setupRemaining), 'Puoi piazzare solo le armate disponibili.');
    s.board[t].armies += count;
    p.setupRemaining -= count;
    s.setupBatch -= count;
    observe(s, s.active, t, count * .15);
    event(s, 'onGameStateUpdate', `${p.name} piazza ${count} in ${territories[t].name}.`, {
      territory: t,
      action: 'reinforce', count
    });
    if (s.setupBatch === 0 || p.setupRemaining === 0) {
      if (s.players.every(p => p.setupRemaining === 0)) {
        s.active = s.order[0];
        s.round = 1;
        startTurn(s);
      } else {
        let i = s.order.indexOf(s.active);
        do {
          i = (i + 1) % s.order.length;
        } while (s.players[s.order[i]].setupRemaining === 0);
        s.active = s.order[i];
        s.setupBatch = Math.min(3, s.players[s.active].setupRemaining);
      }
    }
    return;
  }
  requireRule(s.phase === 'REINFORCEMENT', 'Il piazzamento non è disponibile in questa fase.');
  requireRule(count <= s.reinforcements, 'Rinforzi insufficienti.');
  if(p.human)(s.placementHistory??=[]).push({territory:t,count,belief:s.beliefs[s.active]?[...s.beliefs[s.active]]:null});
  s.board[t].armies += count;
  s.reinforcements -= count;
  observe(s, s.active, t, count * .2);
  event(s, 'onGameStateUpdate', `${p.name} rinforza ${territories[t].name}: +${count}.`, {
    territory: t,
    action: 'reinforce', count
  });
  if (!s.reinforcements && !(s.manualReinforcement&&p.human)) {s.phase = 'ATTACK';s.placementHistory=[];}
}
export function applyCommand(input: GameState, actor: number, command: Command): GameState {
  requireRule(input.phase !== 'GAME_OVER', 'La partita è conclusa.');
  requireRule(!input.paused, 'Partita in pausa.');
  requireRule(actor === input.active, 'Non è il tuo turno.');
  const {replay,analysis,log,scoreHistory,...body}=input;
  const s:GameState = {...structuredClone(body),log:[...log],...(scoreHistory?{scoreHistory:[...scoreHistory]}:{}),...(replay?{replay}:{}),...(analysis?{analysis}:{})},
    p = s.players[s.active];
  switch (command.type) {
    case 'PLACE':
      place(s, command.territory, command.count);
      break;
    case 'UNDO_PLACE': {
      requireRule(p.human&&s.phase==='REINFORCEMENT'&&!!s.placementHistory?.length,'Non puoi annullare questo piazzamento.');
      const last=s.placementHistory!.pop()!;
      requireRule(Number.isInteger(last.count)&&last.count>0&&s.board[last.territory]?.owner===actor&&s.board[last.territory].armies>last.count,'Piazzamento non più annullabile.');
      s.board[last.territory].armies-=last.count;s.reinforcements+=last.count;
      if(last.belief)s.beliefs[actor]=last.belief;else delete s.beliefs[actor];
      event(s,'onGameStateUpdate',`${p.name} annulla ${last.count} carri in ${territories[last.territory].name}.`,{action:'undo',territory:last.territory,count:-last.count});
      break;
    }
    case 'EXCHANGE':
      {
        requireRule(s.phase === 'TRIS_EXCHANGE', 'Il tris va giocato prima di piazzare i rinforzi.');
        requireRule(command.cards.every(c => p.cards.includes(c)), 'Non possiedi queste carte.');
        const value = exchangeValue(s, s.active, command.cards);
        requireRule(value > 0, 'Tris non valido.');
        p.cards = p.cards.filter(c => !command.cards.includes(c));
        s.discard.push(...command.cards);
        const added = Math.min(value, 130 - total(s, s.active) - s.reinforcements);
        s.reinforcements += added;
        s.turnReinforcements=(s.turnReinforcements??s.reinforcements-added)+added;
        event(s, 'onGameStateUpdate', `${p.name} cambia un tris: ${added} rinforzi${added < value ? ' (limite carri)' : ''}.`, {action: 'exchange', count: added});
        break;
      }
    case 'NEXT':
      if(s.phase==='REINFORCEMENT'){requireRule(s.reinforcements===0,'Piazza tutti i rinforzi prima di attaccare.');s.placementHistory=[];s.phase='ATTACK';}
      else if (s.phase === 'TRIS_EXCHANGE') s.phase = s.reinforcements ? 'REINFORCEMENT' : 'ATTACK';else if (s.phase === 'ATTACK') s.phase = 'FORTIFY';else if (s.phase === 'FORTIFY') endTurn(s);else throw new RuleError('Completa prima la fase corrente.');
      break;
    case 'ATTACK':
      {
        s.placementHistory=[];
        requireRule(s.phase === 'ATTACK', 'Non puoi attaccare in questa fase.');
        const {
          from,
          to
        } = command;
        requireRule(canAttack(s, from, to), 'Attacco vietato: controlla confini, dadi e protezione iniziale.');
        const a = s.board[from],
          d = s.board[to],
          defender = d.owner;
        const ad = Array.from({
            length: Math.min(3, a.armies - 1)
          }, () => die(s)).sort((a, b) => b - a),
          dd = Array.from({
            length: Math.min(3, d.armies)
          }, () => die(s)).sort((a, b) => b - a);
        let al = 0,
          dl = 0;
        for (let i = 0; i < Math.min(ad.length, dd.length); i++) {
          if (ad[i] > dd[i]) dl++;else al++;
        }
        a.armies -= al;
        d.armies -= dl;
        s.lastBattle = event(s, 'onDiceRolled', `${p.name}: ${territories[from].name} → ${territories[to].name}. Dadi ${ad.join(',')} / ${dd.join(',')}: attacco −${al}, difesa −${dl}.`, {
          attack: ad,
          defense: dd,
          losses: [al, dl], from, to, defender,
          territory: to
        });
        if (d.armies === 0) {
          d.owner = s.active;
          s.conquests++;
          s.pending = {
            from,
            to,
            min: ad.length,
            transferred: ad.length
          };
          a.armies -= ad.length;
          d.armies += ad.length;
          s.phase = 'POST_CONQUEST_MOVE';
          observe(s, s.active, to, 2);
          event(s, 'onGameStateUpdate', `${p.name} conquista ${territories[to].name}.`, {
            territory: to,
            action: 'conquer', from, to, count: ad.length
          });
          if (owned(s, defender).length === 0) {
            s.players[defender].eliminated = true;
            const loot = shuffle(s, s.players[defender].cards),
              take = loot.splice(0, 7 - p.cards.length);
            p.cards.push(...take);
            s.discard.push(...loot);
            s.players[defender].cards = [];
            event(s, 'onGameStateUpdate', `${s.players[defender].name} eliminato; ${p.name} riceve ${take.length} carte.`);
          }
          if (objectives[p.objective - 1].territories.every(t => s.board[t].owner === s.active)) {
            s.pending = null;
            finish(s, 'risiko', s.active);
          } else if (moveBounds(s, from, to).max === 0) {
            s.pending = null;
            s.phase = 'ATTACK';
          }
        }
        break;
      }
    case 'MOVE':
      {
        requireRule(s.phase === 'POST_CONQUEST_MOVE' || s.phase === 'FORTIFY', 'Spostamento non disponibile.');
        const {
          from,
          to,
          count
        } = command;
        if (s.phase === 'POST_CONQUEST_MOVE') requireRule(s.pending?.from === from && s.pending?.to === to, 'Completa la conquista in corso.');
        const {
          min,
          max
        } = moveBounds(s, from, to);
        requireRule(Number.isInteger(count) && count >= min && count <= max, 'Spostamento non valido: rispetta la guarnigione minima.');
        s.board[from].armies -= count;
        s.board[to].armies += count;
        if (count > 0) event(s, 'onGameStateUpdate', `${p.name} sposta ${count} carri: ${territories[from].name} → ${territories[to].name}.`, {action: 'move', from, to, count});
        if (s.phase === 'POST_CONQUEST_MOVE') {
          s.pending = null;
          s.phase = 'ATTACK';
          checkObjective(s);
        } else endTurn(s);
        break;
      }
    case 'TIMEOUT':
      {
        requireRule(s.phase !== 'SETUP_PLACEMENT', 'Il setup non ha scadenza.');
        event(s, 'onGameStateUpdate', `${p.name}: tempo scaduto, completamento del turno.`);
        if (s.phase === 'TRIS_EXCHANGE') s.phase = s.reinforcements ? 'REINFORCEMENT' : 'ATTACK';
        while (s.phase === 'REINFORCEMENT' && s.reinforcements > 0) {
          const t = owned(s, s.active).sort((a, b) => s.board[a].armies - s.board[b].armies || a - b)[0];
          place(s, t, 1);
        }
        if (s.phase === 'POST_CONQUEST_MOVE') {
          const m = s.pending!;
          if (m.transferred === undefined) {
            s.board[m.from].armies -= m.min;
            s.board[m.to].armies += m.min;
            event(s, 'onGameStateUpdate', `${p.name}: spostamento obbligatorio di ${m.min} carri.`, {action:'move',from:m.from,to:m.to,count:m.min});
          }
          s.pending = null;
          s.phase = 'ATTACK';
          checkObjective(s);
        }
        if (s.phase !== 'GAME_OVER') endTurn(s);
        break;
      }
    default: throw new RuleError('Comando non riconosciuto.');
  }
  s.revision++;
  recordTurn(input,s);
  recordAnalysis(input,s,command);
  assertState(s);
  return s;
}
export function advanceClock(input: GameState, delta: number): GameState {
  if (input.paused || input.phase === 'GAME_OVER' || !Number.isFinite(delta) || delta <= 0) return input;
  // Ordinary ticks change only counters. Clone the board/log only when expiry
  // performs a real game action; long histories must not be copied 20 times/s.
  let s = {...input};
  s.elapsedMs += delta;
  if (s.phase !== 'SETUP_PLACEMENT') {
    if(s.endMode==='countdown'){
      s.matchRemainingMs=Math.max(0,(s.matchRemainingMs??45*60000)-delta);
      if(!s.matchRemainingMs){
        s=structuredClone(s);
        // Old pending saves may not yet have transferred the compulsory garrison.
        if(s.pending&&s.pending.transferred===undefined){s.board[s.pending.from].armies-=s.pending.min;s.board[s.pending.to].armies+=s.pending.min;}
        finish(s,'countdown');s.revision++;recordTurn(input,s);assertState(s);return s;
      }
    }
    if(s.turnSeconds===0)return s;
    s.remainingMs = Math.max(0, s.remainingMs - delta);
    if (!s.remainingMs) return applyCommand(s, s.active, {
      type: 'TIMEOUT'
    });
  }
  return s;
}
export function project(s: GameState, viewer = 0): GameView {
  const {
    rng,
    aiRng,
    deck,
    discard,
    beliefs,
    players,
    lastCommandId,
    botContexts,
    placementHistory,scoreHistory,replay,analysis,
    ...publicState
  } = s;
  return {
    ...structuredClone(publicState),
    undoAvailable: s.active===viewer && s.phase==='REINFORCEMENT' && !!placementHistory?.length,
    scoreHistory: scoreHistory?.map(row=>({...row,points:row.points.map((n,i)=>i===viewer||s.phase==='GAME_OVER'?n:null)})),
    viewer,
    ...(s.phase==='GAME_OVER'?structuredClone({replay,analysis}):{}),
    deckCount: deck.length,
    players: structuredClone(players).map(p => {
      const {
          difficulty,
          objective,
          cards,
          setupRemaining,
          ...rest
        } = p,
        expose = p.id === viewer || s.phase === 'GAME_OVER',
        own = owned(s, p.id);
      return {
        ...rest,
        setupRemaining,
        difficulty: p.blind && s.phase !== 'GAME_OVER' ? '?' : difficulty,
        ...(expose ? {
          objective,
          cards,
          points: s.reason === 'risiko' && s.winner === p.id ? 100 : objectivePoints(objective, own),
          outside: own.filter(t => !objectives[objective - 1].territories.includes(t)).reduce((n, t) => n + territories[t].points, 0)
        } : {}),
        cardCount: cards.length,
        totalArmies: total(s, p.id),
        territoryCount: own.length,
        nextReinforcements: reinforcement(s, p.id)
      };
    })
  };
}
export function upgradeConquestSave(input: GameState): GameState {
  assertState(input);
  if (input.phase !== 'POST_CONQUEST_MOVE' || !input.pending || input.pending.transferred !== undefined) return input;
  const s = structuredClone(input), m = s.pending!;
  requireRule(s.board[m.to].armies === 0 && s.board[m.from].armies > m.min &&
    s.board[m.from].owner === s.active && s.board[m.to].owner === s.active &&
    Number.isInteger(m.min) && m.min >= 1 && m.min <= 3, 'Spostamento salvato non valido.');
  s.board[m.from].armies -= m.min;
  s.board[m.to].armies += m.min;
  m.transferred = m.min;
  if (moveBounds(s, m.from, m.to).max === 0) { s.pending = null; s.phase = 'ATTACK'; }
  s.revision++;
  assertState(s);
  return s;
}
export function assertState(s: GameState) {
  requireRule(s.endMode===undefined||['regular','countdown'].includes(s.endMode),'Fine partita salvata non valida.');
  requireRule(s.matchRemainingMs===undefined||(Number.isFinite(s.matchRemainingMs)&&s.matchRemainingMs>=0),'Countdown salvato non valido.');
  requireRule(s.manualReinforcement===undefined||typeof s.manualReinforcement==='boolean','Fase rinforzi salvata non valida.');
  requireRule(s.antarctica===undefined||typeof s.antarctica==='boolean', 'Modalità salvata non valida.');
  const {territories,cards}=mapFor({antarctica:s.antarctica===true});
  requireRule([1,2].includes(s.version) && s.board.length === territories.length, 'Salvataggio incompatibile.');
  requireRule(Number.isInteger(s.active)&&!!s.players[s.active],'Giocatore attivo non valido.');
  requireRule(s.players.some(p=>p.human),'Serve almeno un giocatore umano.');
  requireRule(s.players.every((p,i)=>p.id===i&&typeof p.human==='boolean'),'Partecipanti non validi.');
  requireRule([3, 4, 5].includes(s.players.length), 'Numero giocatori non valido.');
  requireRule(new Set(s.order).size === s.players.length && s.order.every(id => s.players[id]), 'Ordine di gioco non valido.');
  for (let i = 0; i < territories.length; i++) {
    const t = s.board[i];
    requireRule(s.players[t.owner] && Number.isInteger(t.armies) && t.armies >= (s.phase === 'POST_CONQUEST_MOVE' && s.pending?.to === i && s.pending.transferred === undefined ? 0 : 1), 'Stato territorio non valido.');
  }
  if(s.pending?.transferred !== undefined) {
    const m=s.pending;
    requireRule(s.phase==='POST_CONQUEST_MOVE' && Number.isInteger(m.transferred) && m.transferred===m.min && m.min>=1 && m.min<=3 &&
      !!s.board[m.from] && !!s.board[m.to] && territories[m.from].neighbors.includes(m.to) &&
      s.board[m.from].owner===s.active && s.board[m.to].owner===s.active && s.board[m.to].armies>=m.min, 'Conquista pendente non valida.');
  }
  for (const p of s.players) {
    requireRule(total(s, p.id) <= 130 && p.cards.length <= 7 && objectives[p.objective - 1], 'Limiti giocatore non validi.');
    requireRule(p.eliminated === (owned(s, p.id).length === 0), 'Eliminazione incoerente.');
  }
  const all = [...s.deck, ...s.discard, ...s.players.flatMap(p => p.cards)];
  requireRule(all.length === cards.length && new Set(all).size === cards.length && all.every(c => cards[c]), 'Conservazione delle carte violata.');
  requireRule(s.reinforcements >= 0 && s.remainingMs >= 0, 'Contatori non validi.');
}
