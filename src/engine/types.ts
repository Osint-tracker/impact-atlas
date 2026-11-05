export type BotPersonality = 'equilibrato' | 'stratega' | 'incursore' | 'conquistatore';
export type BotPolicy = { personality?: BotPersonality; weakBorders: boolean; continents: boolean; defend: boolean; disruptGrowth: boolean; breakBonus: boolean; containLeader: boolean; eliminate: boolean; reserve: boolean; graceRounds: number };
export type Difficulty = 'facile' | 'medio' | 'difficile' | 'survival';
export type Phase = 'SETUP_PLACEMENT' | 'TRIS_EXCHANGE' | 'REINFORCEMENT' | 'ATTACK' | 'POST_CONQUEST_MOVE' | 'FORTIFY' | 'CARD_DRAW' | 'SDADATA_CHECK' | 'GAME_OVER';
export type Player = {
  id: number;
  name: string;
  color: string;
  human: boolean;
  profileId?: string;
  difficulty: Difficulty;
  blind: boolean;
  objective: number;
  cards: number[];
  setupRemaining: number;
  eliminated: boolean;
  botPolicy?: BotPolicy;
};
export type BoardCell = {
  owner: number;
  armies: number;
};
export type PendingMove = {
  from: number;
  to: number;
  min: number;
  /** Compulsory armies already moved. Absent only in legacy saves. */
  transferred?: number;
};
export type GameEvent = {
  type: 'onGameStateUpdate' | 'onDiceRolled' | 'onTurnChanged' | 'onSdadataRoll' | 'onGameOver';
  player: number;
  message: string;
  territory?: number;
  action?: 'reinforce' | 'conquer' | 'move' | 'draw' | 'exchange' | 'undo';
  from?: number;
  to?: number;
  count?: number;
  defender?: number;
  attack?: number[];
  defense?: number[];
  losses?: number[];
};
export type GameState = {
version: 1|2;
  recordResults?:boolean;
  tutorialStep?: number;
  initialSeed?: number;
  seedLabel?: string;
  custom?: boolean;
  analysis?: {round:number;player:number;message:string}[];
  replay?: import('./Replay.ts').TurnSnapshot[];
  manualReinforcement?: boolean;
  placementHistory?: {territory:number;count:number;belief:number[]|null}[];
  scoreHistory?: {round:number;player:number;points:number[]}[];
  endMode?: 'regular' | 'countdown';
  matchRemainingMs?: number;
  antarctica?: boolean;
  id: string;
  revision: number;
  rng: number;
  aiRng: number;
  players: Player[];
  order: number[];
  active: number;
  round: number;
  phase: Phase;
  board: BoardCell[];
  deck: number[];
  discard: number[];
  reinforcements: number;
  turnReinforcements?: number;
  setupBatch: number;
  conquests: number;
  pending: PendingMove | null;
  lastBattle: GameEvent | null;
  lastSdadata: GameEvent | null;
  sdadata: {
    started: boolean;
    anchor: number;
    threshold: number;
  } | null;
  turnSeconds: number;
  remainingMs: number;
  elapsedMs: number;
  paused: boolean;
  reason: 'risiko' | 'sdadata' | 'countdown' | null;
  winner: number | null;
  log: GameEvent[];
  beliefs: Record<string, number[]>;
  lastCommandId: string | null;
  botContexts?: Record<number, BotContext>;
};
export type BotContext = {
  plan?: {round:number; phase:string; placements:number[]; target?:number};
  temperament: number;
  sortieChecked?: boolean;
  sortiePending?: boolean;
  round: number;
  turns: number;
  lastEpisode: number;
  previousArmies: number;
  cautious: boolean;
  lostTerritory: number | null;
  reclaimUntil?: number;
  logIndex: number;
  episode: { from: number; to: number; initialArmies: number; rolls: number } | null;
};
export type Participant = {name:string;human:boolean;profileId?:string;difficulty?:Difficulty|'casuale'};
export type GameConfig = {
  participants?: Participant[];
  seedLabel?: string;
  custom?: boolean;
  manualReinforcement?: boolean;
  endMode?: 'regular' | 'countdown';
  countdownMinutes?: number;
  botSpeed?: number;
  antarctica?: boolean;
  players: number;
  name: string;
  difficulties: (Difficulty | 'casuale')[];
  botPolicies?: BotPolicy[];
  turnSeconds: number;
  seed?: number;
};
export type Command = {
  type: 'PLACE';
  territory: number;
  count: number;
} | {
  type: 'EXCHANGE';
  cards: number[];
} | {
  type: 'NEXT';
} | {
  type: 'UNDO_PLACE';
} | {
  type: 'ATTACK';
  from: number;
  to: number;
} | {
  type: 'MOVE';
  from: number;
  to: number;
  count: number;
} | {
  type: 'TIMEOUT';
};
export type PublicPlayer = Omit<Player, 'difficulty' | 'objective' | 'cards' | 'setupRemaining'> & {
  setupRemaining: number;
  difficulty: Difficulty | '?';
  objective?: number;
  cards?: number[];
  cardCount: number;
  totalArmies: number;
  territoryCount: number;
  nextReinforcements: number;
  points?: number;
  outside?: number;
};
export type GameView = Omit<GameState, 'rng' | 'aiRng' | 'deck' | 'discard' | 'beliefs' | 'players' | 'lastCommandId' | 'botContexts' | 'placementHistory' | 'scoreHistory' | 'replay' | 'analysis'> & {
  replay?: import('./Replay.ts').TurnSnapshot[];
  analysis?: {round:number;player:number;message:string}[];
  undoAvailable?: boolean;
  scoreHistory?: {round:number;player:number;points:(number|null)[]}[];
  players: PublicPlayer[];
  deckCount: number;
  viewer: number;
};
