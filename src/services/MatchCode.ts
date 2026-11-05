import {createGame} from '../engine/RisikoRulesEngine.ts';
import type {GameConfig,Participant} from '../engine/types.ts';
/** Versioned FNV-1a; numeric seeds keep their original meaning. */
export function seedNumber(text:string):number {
 const value=text.normalize('NFKC').trim();
 if(!value)throw Error('Inserisci un codice partita.');
 if(/^\d+$/.test(value)&&Number(value)<=0xffffffff)return Number(value)>>>0||1;
 let hash=2166136261;for(const byte of new TextEncoder().encode(value)){hash^=byte;hash=Math.imul(hash,16777619);}return hash>>>0||1;
}
export function matchCode(config:GameConfig):string {
 const shared={...config,seed:config.seed??1};
 if(shared.seedLabel?.startsWith('RKO2:'))shared.seedLabel=String(shared.seed);
 if(shared.participants)shared.participants=shared.participants.map(({profileId,...p})=>p);
 return 'RKO2:'+encodeURIComponent(JSON.stringify(shared));
}
export function readMatchCode(text:string):GameConfig|null {
 if(!text.startsWith('RKO2:'))return null;
 if(text.length>20000)throw Error('Codice troppo lungo.');
 const c=JSON.parse(decodeURIComponent(text.slice(5)));
 if(!c||![3,4,5].includes(c.players)||!Array.isArray(c.difficulties)||!Number.isInteger(c.seed))throw Error('Codice partita non valido.');
 createGame(c); // Validate all engine rules before replacing the preparation form.
 if(c.participants){let assigned=false;c.participants=c.participants.map((p:Participant)=>{const profileId=p.human&&!assigned?'local':'';if(p.human)assigned=true;return {...p,profileId};});}
 return c;
}
