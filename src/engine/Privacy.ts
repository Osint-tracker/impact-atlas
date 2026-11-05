import type {GameState} from './types.ts';
/** No secrets cross the worker boundary while a handoff is pending. */
export class PrivacyGate {
 viewer=0; pending:{player:number;token:string}|null=null;
 hotseat(s:GameState){return s.players.filter(p=>p.human).length>1;}
 reset(s:GameState){this.viewer=s.players.find(p=>p.human)?.id??0;this.pending=null;if(this.hotseat(s))this.lock(s);}
 lock(s:GameState){if(!this.hotseat(s)||s.phase==='GAME_OVER')return;const player=s.players[s.active].human?s.active:this.viewer;this.pending={player,token:crypto.randomUUID()};}
 changed(s:GameState){if(this.hotseat(s)&&s.players[s.active].human&&s.active!==this.viewer&&!this.pending&&s.phase!=='GAME_OVER')this.lock(s);}
 reveal(player:number,token:string){if(!this.pending||this.pending.player!==player||this.pending.token!==token)throw Error('Passaggio del dispositivo non valido.');this.viewer=player;this.pending=null;}
}
