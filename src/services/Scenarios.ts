import {createGame,assertState,reinforcement,owned} from '../engine/RisikoRulesEngine.ts';
import {objectivePoints,mapFor} from '../data/mapGraph.ts';
import type {GameConfig,GameState,BoardCell} from '../engine/types.ts';
export type Scenario={id:string;name:string;description:string;config:GameConfig;board:BoardCell[];round:number;active:number};
export function scenarioGame(sc:Scenario):GameState {
 if(!sc||typeof sc.name!=='string'||sc.name.length>80||!Number.isInteger(sc.round)||sc.round<1||sc.round>19)throw Error('Scenario non valido');
 const s=createGame({...sc.config,custom:true});if(!Number.isInteger(sc.active)||!s.players[sc.active])throw Error('Giocatore iniziale non valido');
 s.board=structuredClone(sc.board);s.round=sc.round;s.active=sc.active;s.phase='TRIS_EXCHANGE';s.players.forEach(p=>p.setupRemaining=0);
 assertState(s);for(const p of s.players)if(!owned(s,p.id).length||objectivePoints(p.objective,owned(s,p.id))>=86)throw Error('Ogni partecipante deve avere territori e nessun obiettivo deve essere già completato.');
 s.reinforcements=reinforcement(s,s.active);s.turnReinforcements=s.reinforcements;return s;
}
export function scenarioFrom(config:GameConfig):Scenario {const s=createGame(config);return {id:crypto.randomUUID(),name:'Nuovo scenario',description:'Posizione tattica personalizzata',config,board:s.board,round:5,active:0};}
export function curatedScenarios(config:GameConfig):Scenario[]{
 const definitions=[
  {name:'Oceania contesa',continent:'oc',target:38,from:37,description:'Spezza il bonus Oceania entrando in Indonesia dal Siam. Proteggi il corridoio dopo la conquista.'},
  {name:'Frontiere europee',continent:'eu',target:19,from:26,description:'L’Europa è unita ma lascia un varco in Ucraina. Valuta il rischio prima di avanzare dagli Urali.'},
  {name:'Rotta antartica',continent:'an',target:43,from:24,description:'L’Antartide controlla le rotte australi. Interrompi il bonus dall’Africa del Sud e difendi il nuovo confine.'}
 ];
 return definitions.map((d,i)=>{
  const sc=scenarioFrom({...config,seed:817+i,antarctica:i===2||config.antarctica});sc.name=d.name;sc.id='curated-'+i;sc.description=d.description;
  for(const t of mapFor(sc.config).territories){sc.board[t.id].armies=2;if(t.continent===d.continent)sc.board[t.id].owner=1;}
  sc.board[d.target]={owner:1,armies:1};sc.board[d.from]={owner:0,armies:10};
  scenarioGame(sc);return sc;
 });
}
