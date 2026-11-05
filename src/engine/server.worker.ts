import {summarizeBots} from './TurnSummary.ts';
import {withDecisionBudget} from '../ai/Budget.ts';
import {tutorialGame,tutorialComplete,tutorialSteps} from './Tutorial.ts';
import {PrivacyGate} from './Privacy.ts';
import {scenarioGame} from '../services/Scenarios.ts';
import {makeBackup,parseBackup} from '../services/Backup.ts';
import {createGame, applyCommand, advanceClock, project, random, assertState} from './RisikoRulesEngine.ts';
import {humanDecision, decisionDelay} from '../ai/HumanBot.ts';
import {makeTransition, PresentationGate} from './presentation.ts';
import {StorageService} from '../services/StorageService.ts';
import type {GameConfig, GameState, Command, BotContext} from './types.ts';
const ctx = self as unknown as DedicatedWorkerGlobalScope;
const storage = new StorageService();
const gate = new PresentationGate();
const privacy=new PrivacyGate();
let tutorialDone=false;
let botBoundary:GameState|null=null,turnSummary:string[]|null=null;
let animationFilter='all';
let state: GameState | null = null, speed = 1, lastTick = performance.now(), dirtyClock = 0, clockUiMs = 0;
let busy = Promise.resolve(), ticking = false, presentationEnabled = false;
let publishedGameId = '', publishedCount = 0;
let previousBot: {actor: number; command: Command} | null = null;
let planned: {revision: number; actor: number; due: number; command: Command; context: BotContext; aiRng: number} | null = null;
const post = (message: unknown) => ctx.postMessage(message);
const publish = () => { if (!state)return;if(privacy.pending){const p=state.players[privacy.pending.player];post({type:'PRIVACY',player:p.id,name:p.name,color:p.color,token:privacy.pending.token});return;}post({type:'STATE',state:project(state,privacy.viewer)});if(turnSummary?.length)post({type:'TURN_SUMMARY',lines:turnSummary}); };
const localSave=()=>{if(state&&state.tutorialStep===undefined&&!privacy.hotseat(state))post({type:'LOCAL_SAVE',state});else post({type:'LOCAL_SAVE',state:null});};
function clearPresentation() { gate.clear(); planned = null; lastTick = performance.now(); }
async function commit(next: GameState, before?: GameState) {
  if(next.tutorialStep===undefined)await storage.save(next);
  if(before&&tutorialComplete(before,next)){tutorialDone=true;post({type:'TUTORIAL_DONE',step:before.tutorialStep});}
  state = next;
  if(before&&before.active!==next.active){
    if(before.players[before.active].human&&!next.players[next.active].human)botBoundary=next;
    if(!before.players[before.active].human&&next.players[next.active].human&&botBoundary){turnSummary=summarizeBots(botBoundary,next,next.active);if(!turnSummary.length)turnSummary=null;botBoundary=null;}
  }
  dirtyClock=0;clockUiMs=0;
  privacy.changed(next);localSave();
  if(privacy.pending){clearPresentation();publishedGameId=next.id;publishedCount=next.log.length;publish();return;}
  const start = publishedGameId === next.id ? publishedCount : 0;
  for (const event of next.log.slice(start)) post({type: event.type, event});
  publishedGameId = next.id;
  publishedCount = next.log.length;
  planned = null;
  const transition = presentationEnabled && before ? makeTransition(before, next, speed, privacy.viewer, animationFilter) : null;
  if (transition) {
    gate.begin(transition.id, performance.now());
    lastTick = performance.now();
    post({type: 'TRANSITION', transition});
  } else publish();
}
function enqueue(f: () => Promise<void>) {
  busy = busy.then(f).catch(e => {
    clearPresentation();
    if (state) { state.paused = true; publish(); }
    post({type: 'ERROR', message: e instanceof Error ? e.message : String(e)});
  });
}
ctx.onmessage = ({data}) => enqueue(async () => {
  switch (data.type) {
    case 'INIT': {
      presentationEnabled = data.presentation === true;
      post({type: 'PROFILE', profile: await storage.profile()});
      try {
        state = await storage.load();
        if(!state&&data.snapshot){assertState(data.snapshot);await storage.save(data.snapshot);state=await storage.load();}
        if (state) {
          state.paused = true;privacy.reset(state);
          publishedGameId = state.id;
          publishedCount = state.log.length;
          publish();
        }
        post({type: 'READY', hasGame: !!state && state.phase !== 'GAME_OVER'});
      } catch {
        state = null;
        post({type: 'READY', hasGame: true});
        post({type: 'ERROR', message: 'Il salvataggio non è compatibile o è danneggiato. Puoi sostituirlo creando una nuova partita.'});
      }
      break;
    }
    case 'NEW': {
      clearPresentation(); previousBot = null;turnSummary=null;botBoundary=null;
      const next = data.scenario?scenarioGame(data.scenario):createGame(data.config as GameConfig);privacy.reset(next);
      speed=[0,1,2,4].includes(data.config.botSpeed)?data.config.botSpeed:1;
      await commit(next);
      const local=next.players.find(p=>p.human&&p.profileId==='local');
      if(local)await storage.setName(local.name);
      lastTick = performance.now();
      break;
    }
    case 'SUMMARY_ACK':turnSummary=null;lastTick=performance.now();break;
    case 'TUTORIAL':clearPresentation();tutorialDone=false;turnSummary=null;botBoundary=null;state=tutorialGame(0);privacy.reset(state);await commit(state);break;
    case 'TUTORIAL_NEXT':if(state?.tutorialStep!==undefined&&tutorialDone){const step=state.tutorialStep+1;tutorialDone=false;if(step<tutorialSteps.length){clearPresentation();state=tutorialGame(step);await commit(state);}else{state=null;post({type:'TUTORIAL_FINISHED'});}}break;
    case 'REVEAL':
      if(!state)break;privacy.reveal(data.player,data.token);lastTick=performance.now();await commit({...state,paused:false});break;
    case 'ANIMATION_FILTER':animationFilter=['all','involved','summary'].includes(data.filter)?data.filter:'all';break;
    case 'SLOTS':post({type:'SLOTS',slots:(await storage.slots()).map(({state:s,...slot})=>({...slot,round:s.round,players:s.players.length,phase:s.phase,board:s.board,antarctica:s.antarctica,endMode:s.endMode}))});break;
    case 'SLOT_SELECT':clearPresentation();turnSummary=null;botBoundary=null;state=await storage.selectSlot(data.id);if(state){privacy.reset(state);await commit(state);}break;
    case 'SLOT_EDIT':await storage.editSlot(data.id,data.action,data.name);if(data.action==='delete'&&state?.id===data.id)state=null;post({type:'SLOTS_CHANGED'});break;
    case 'PROFILES':post({type:'PROFILES',profiles:await storage.localProfiles()});break;
    case 'PROFILE_ADD':await storage.addProfile(String(data.name));post({type:'PROFILES',profiles:await storage.localProfiles()});break;
    case 'SCENARIOS':post({type:'SCENARIOS',scenarios:await storage.list('scenarios')});break;
    case 'SCENARIO_SAVE':scenarioGame(data.scenario);await storage.put('scenarios',data.scenario.id,data.scenario);post({type:'SCENARIOS',scenarios:await storage.list('scenarios')});break;
    case 'PRESENTED':
      if (gate.finish(data.id)) { lastTick = performance.now(); dirtyClock = 0; publish(); }
      break;
    case 'RESUME':
      if (state) {
        clearPresentation(); assertState(state);privacy.lock(state);
        await commit({...state, paused: false});
      }
      break;
    case 'PAUSE':
      if (state) {
        const next = gate.pending ? state : advanceClock(state, Math.max(0, performance.now() - lastTick));
        clearPresentation();privacy.lock(next);
        await commit({...next, paused: true});
      }
      break;
    case 'SPEED':
      speed = [1, 2, 4, 0].includes(data.speed) ? data.speed : 1;
      planned = null;
      break;
    case 'EXPORT_BACKUP': {
      if(state){clearPresentation();await commit({...state,paused:true});}
      const profile=await storage.profile();profile.results=await storage.list('results');
      const backup={...makeBackup(state?.tutorialStep===undefined?state:await storage.load(),profile),version:2,slots:await storage.slots(),profiles:await storage.localProfiles(),scenarios:await storage.list('scenarios')};
      post({type:'BACKUP_FILE',text:JSON.stringify(backup)});break;
    }
    case 'IMPORT_BACKUP': {
      const backup=parseBackup(data.text);
      await storage.restoreBackup(backup,data.mode==='merge'?'merge':'replace');
      clearPresentation();previousBot=null;turnSummary=null;botBoundary=null;state=await storage.load();if(state)privacy.reset(state);
      publishedGameId=state?.id??'';publishedCount=state?.log.length??0;
      post({type:'BACKUP_RESTORED',profile:await storage.profile(),state:state&&!privacy.pending?project(state,privacy.viewer):null});
      localSave();publish();break;
    }
    case 'PROFILE': post({type: 'PROFILE', profile: await storage.profile(data.profileId??'local')}); break;
    case 'COMMAND': {
      if (!state) throw new Error('Nessuna partita attiva.');
      if (state.lastCommandId === data.id) return;
      if (privacy.pending)throw Error('Rivela il tuo turno prima di giocare.');
      if (data.actor!==undefined&&data.actor!==privacy.viewer)throw Error('Osservatore non autorizzato.');
      if (state.active!==privacy.viewer)throw Error('Attendi il tuo turno.');
      if (gate.pending) return; // UI will unlock only after PRESENTED.
      if (data.revision !== state.revision) throw new Error('Il turno è cambiato. Riprova.');
      if (!state.players[state.active].human) throw new Error('Attendi il tuo turno.');
      const now = performance.now(), before = state;
      const timed = advanceClock(state, now - lastTick);
      lastTick = now;
      if (timed.revision !== state.revision) { await commit(timed, before); break; }
      const next = applyCommand(timed, privacy.viewer, data.command as Command);
      next.lastCommandId = data.id;
      await commit(next, timed);
      break;
    }
  }
});
setInterval(() => {
  if (ticking) return;
  ticking = true;
  enqueue(async () => {
    try {
      const now = performance.now(), delta = Math.max(0, now - lastTick);
      lastTick = now;
      if (gate.pending) {
        if (gate.expired(now)) { clearPresentation(); publish(); }
        return;
      }
      if (!state || turnSummary?.length || state.tutorialStep!==undefined && tutorialDone || privacy.pending || state.paused || state.phase === 'GAME_OVER') return;
      const before = state;
      const timed = advanceClock(state, delta);
      dirtyClock += delta;clockUiMs += delta;
      if (timed.revision !== before.revision) { await commit(timed, before); dirtyClock = 0; return; }
      state = timed;
      const p = state.players[state.active];
      if (!p.human && state.tutorialStep===undefined) {
        if (!planned || planned.revision !== state.revision || planned.actor !== p.id) {
          const rngState = {rng: state.aiRng};
          const view = project(state, p.id);
          const decision = withDecisionBudget(p.difficulty,()=>humanDecision(view, p.difficulty, () => random(rngState), state!.botContexts?.[p.id]));
          const delay = decisionDelay(view, decision.command, previousBot?.actor === p.id ? previousBot.command : null, speed, Math.random, p.difficulty);
          planned = {...decision, revision: state.revision, actor: p.id, aiRng: rngState.rng, due: now + delay};
        }
        if (now >= planned.due) {
          const decision = planned;
          const next = applyCommand(state, p.id, decision.command);
          next.aiRng = decision.aiRng;
          next.botContexts = {...state.botContexts, [p.id]: decision.context};
          await commit(next, state);
          previousBot = {actor: p.id, command: decision.command};
          dirtyClock = 0;
          return;
        }
      }
      if(clockUiMs>=1000){
        post({type:'CLOCK',id:state.id,revision:state.revision,remainingMs:state.remainingMs,matchRemainingMs:state.matchRemainingMs,elapsedMs:state.elapsedMs});
        clockUiMs=0;
      }
      if (dirtyClock >= 5000) {
        // Clock saves must not restart a bot's pending thinking interval.
        if(state.tutorialStep===undefined)await storage.save(state); localSave(); dirtyClock = 0;
      }
    } finally { ticking = false; }
  });
}, 50);
