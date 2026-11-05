import {parseBackup,type Backup} from './Backup.ts';
import {diceHistogram} from './TournamentStats.ts';
import {objectivePoints} from '../data/mapGraph.ts';
import {owned} from '../engine/RisikoRulesEngine.ts';
import {botPolicy} from '../ai/policy.ts';
import type { GameState } from '../engine/types.ts';
import { assertState, ranks, upgradeConquestSave } from '../engine/RisikoRulesEngine.ts';
export type ResultRecord = {
  profileId?: string;
  category?: 'solo'|'hotseat'|'custom';
  difficulties?: string[];
  antarctica?: boolean;
  objectivePoints?: number;
  dice?: number[];
  id: string;
  date: number;
  name: string;
  rank: number;
  points: number;
  reason: string;
  duration: number;
  players: number;
};
export type Profile = {
  name: string;
  results: ResultRecord[];
};
export class StorageService {
  private promise: Promise<IDBDatabase> | null = null;
  private open() {
    return this.promise ??= new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open('risiko-offline', 2);
      req.onupgradeneeded = () => {
        for (const name of ['game', 'profile', 'results','slots','profiles','scenarios']) if (!req.result.objectStoreNames.contains(name)) req.result.createObjectStore(name);
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
      req.onblocked=()=>reject(Error('Chiudi le altre schede di RisiKo per aggiornare i salvataggi.'));
    });
  }
  async get<T>(store: string, key: string): Promise<T | null> {
    const db = await this.open();
    return new Promise((resolve, reject) => {
      const t = db.transaction(store, 'readonly'),
        r = t.objectStore(store).get(key);
      r.onsuccess = () => resolve(r.result ?? null);
      r.onerror = () => reject(r.error);
    });
  }
  async put(store:string,key:string,value:unknown){
    const db=await this.open();return new Promise<void>((resolve,reject)=>{const tx=db.transaction(store,'readwrite');tx.objectStore(store).put(value,key);tx.oncomplete=()=>resolve();tx.onerror=tx.onabort=()=>reject(tx.error);});
  }
  async list<T>(store:string):Promise<T[]>{const db=await this.open();return new Promise((resolve,reject)=>{const r=db.transaction(store).objectStore(store).getAll();r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});}
  async save(state:GameState):Promise<void>{
    assertState(state);const db=await this.open();
    return new Promise((resolve,reject)=>{
      const tx=db.transaction(['game','slots','results'],'readwrite');
      tx.objectStore('game').put(state,'current');
      const slots=tx.objectStore('slots'),read=slots.get(state.id);
      read.onsuccess=()=>slots.put({id:state.id,name:read.result?.name??`Partita · ${state.players.filter(p=>p.human).map(p=>p.name).join(', ')}`,updatedAt:Date.now(),state},state.id);
      if(state.phase==='GAME_OVER'&&state.recordResults!==false)for(const player of state.players.filter(p=>p.human&&p.profileId!=='')){
        const ranking=ranks(state),rank=ranking.findIndex(r=>r.player===player.id),id=player.id===0?state.id:`${state.id}:${player.id}`;
        const result:ResultRecord={id,profileId:player.profileId??'local',category:state.custom?'custom':state.players.filter(p=>p.human).length>1?'hotseat':'solo',difficulties:state.players.filter(p=>!p.human).map(p=>p.difficulty),antarctica:state.antarctica===true,date:Date.now(),name:player.name,rank:rank+1,points:ranking[rank].points,objectivePoints:objectivePoints(player.objective,owned(state,player.id)),dice:diceHistogram(state,player.id),reason:state.reason!,duration:state.elapsedMs,players:state.players.length};
        const results=tx.objectStore('results'),existing=results.get(id);
        existing.onsuccess=()=>{if(!existing.result)results.put(result,id);};
      }
      tx.oncomplete=()=>resolve();tx.onerror=tx.onabort=()=>reject(tx.error??Error('Salvataggio interrotto'));
    });
  }
  async slots():Promise<SaveSlot[]>{return (await this.list<SaveSlot>('slots')).sort((a,b)=>b.updatedAt-a.updatedAt);}
  async selectSlot(id:string){const slot=await this.get<SaveSlot>('slots',id);if(!slot)throw Error('Partita non trovata');await this.put('game','current',{...slot.state,paused:true});return this.load();}
  async editSlot(id:string,action:'rename'|'duplicate'|'delete',name=''){
    const slot=await this.get<SaveSlot>('slots',id);if(!slot)throw Error('Partita non trovata');
    if(action==='rename'){await this.put('slots',id,{...slot,name:name.trim().slice(0,80)||slot.name});return;}
    if(action==='duplicate'){const copy=structuredClone(slot);copy.id=crypto.randomUUID();copy.name+=' · copia';copy.state.id=copy.id;if(copy.state.phase==='GAME_OVER')copy.state.recordResults=false;copy.updatedAt=Date.now();copy.state.paused=true;await this.put('slots',copy.id,copy);return;}
    const db=await this.open();await new Promise<void>((resolve,reject)=>{const tx=db.transaction(['slots','game'],'readwrite');tx.objectStore('slots').delete(id);const r=tx.objectStore('game').get('current');r.onsuccess=()=>{if(r.result?.id===id)tx.objectStore('game').delete('current');};tx.oncomplete=()=>resolve();tx.onerror=tx.onabort=()=>reject(tx.error);});
  }
  async localProfiles():Promise<LocalProfile[]>{let rows=await this.list<LocalProfile>('profiles');if(!rows.length){const p={id:'local',name:(await this.get<string>('profile','name'))??'Comandante'};await this.put('profiles',p.id,p);rows=[p];}return rows;}
  async addProfile(name:string){const p={id:crypto.randomUUID(),name:name.trim().slice(0,24)||'Comandante'};await this.put('profiles',p.id,p);return p;}
  async load(): Promise<GameState | null> {
    const s = await this.get<GameState>('game', 'current');
    if (!s) return null;
    const upgraded = upgradeConquestSave(s);
    if (upgraded !== s) await this.save(upgraded);
    let changed=upgraded.version===1;upgraded.version=2;
    for(const p of upgraded.players)if(p.profileId===undefined){p.profileId=p.id===0?'local':'';changed=true;}
    if(!await this.get('slots',upgraded.id))changed=true;
    if(!upgraded.manualReinforcement){upgraded.manualReinforcement=true;changed=true;}
    for(const player of upgraded.players)if(!player.botPolicy){player.botPolicy=botPolicy();changed=true;}
    if(upgraded.turnReinforcements===undefined){
      let start=upgraded.log.length-1;while(start>=0&&upgraded.log[start].type!=='onTurnChanged')start--;
      upgraded.turnReinforcements=upgraded.reinforcements+upgraded.log.slice(start+1).filter(e=>e.player===upgraded.active&&e.action==='reinforce').reduce((n,e)=>n+(e.count??0),0);
      changed=true;
    }
    if(changed)await this.save(upgraded);
    return upgraded;
  }
  async profile(profileId='local'): Promise<Profile> {
    const db = await this.open();
    const name = (await this.get<LocalProfile>('profiles',profileId))?.name ?? (await this.get<string>('profile', 'name')) ?? 'Comandante';
    const results = await new Promise<ResultRecord[]>((resolve, reject) => {
      const req = db.transaction('results', 'readonly').objectStore('results').getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return {
      name,
      results: results.filter(r=>(r.profileId??'local')===profileId).sort((a, b) => b.date - a.date)
    };
  }
  async restoreBackup(input:Backup,mode:'replace'|'merge'='replace'):Promise<void>{
    const backup=parseBackup(JSON.stringify(input)),db=await this.open();
    const slots:SaveSlot[]=backup.slots??(backup.game?[{id:backup.game.id,name:'Partita importata',updatedAt:Date.now(),state:backup.game}]:[]);
    const profiles:LocalProfile[]=backup.profiles?.length?backup.profiles:[{id:'local',name:backup.profile.name.slice(0,24)}];
    const remap=new Map(profiles.map(p=>[p.id,mode==='merge'?crypto.randomUUID():p.id]));
    const gameIds=new Map(slots.map(s=>[s.id,mode==='merge'?crypto.randomUUID():s.id]));
    const remapGame=(g:GameState,id:string)=>({...g,id,paused:true,players:g.players.map(p=>({...p,profileId:p.profileId===''?'':remap.get(p.profileId??'local')??p.profileId}))});
    return new Promise((resolve,reject)=>{
      const tx=db.transaction(['game','profile','results','slots','profiles','scenarios'],'readwrite');
      if(mode==='replace'){
        if(backup.game)tx.objectStore('game').put(remapGame(backup.game,backup.game.id),'current');else tx.objectStore('game').delete('current');
        tx.objectStore('profile').put(backup.profile.name,'name');
        for(const name of ['slots','profiles','scenarios','results'])tx.objectStore(name).clear();
      }
      for(const slot of slots){const id=gameIds.get(slot.id)!;tx.objectStore('slots').put({...slot,id,state:remapGame(slot.state,id)},id);}
      for(const p of profiles){const id=remap.get(p.id)!;tx.objectStore('profiles').put({...p,id},id);}
      for(const sc of backup.scenarios??[]){const id=mode==='merge'?crypto.randomUUID():sc.id;tx.objectStore('scenarios').put({...sc,id,config:{...sc.config,participants:sc.config.participants?.map(p=>({...p,profileId:p.profileId?remap.get(p.profileId)??'':''}))}},id);}
      for(const result of backup.profile.results){const sourceId=[...gameIds.keys()].sort((a,b)=>b.length-a.length).find(id=>result.id===id||result.id.startsWith(id+':'));const id=mode==='merge'?(sourceId?gameIds.get(sourceId)!+result.id.slice(sourceId.length):crypto.randomUUID()):result.id;tx.objectStore('results').put({...result,id,profileId:remap.get(result.profileId??'local')??'local'},id);}
      tx.oncomplete=()=>resolve();tx.onerror=tx.onabort=()=>reject(tx.error??new Error('Importazione interrotta: backup precedente conservato.'));
    });
  }
  async setName(name: string) {
    const db = await this.open();
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction(['profile','profiles'], 'readwrite');
      tx.objectStore('profiles').put({id:'local',name:name.trim().slice(0,24)||'Comandante'},'local');
      tx.objectStore('profile').put(name, 'name');
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
}

export type SaveSlot={id:string;name:string;updatedAt:number;state:GameState};
export type LocalProfile={id:string;name:string};
