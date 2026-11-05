import {createGame,canAttack,owned,validTris,assertState} from './RisikoRulesEngine.ts';
import {mapFor} from '../data/mapGraph.ts';
import type {GameState} from './types.ts';
export const tutorialSteps=[
 {title:'Piazza un carro',text:'Tocca il + accanto a un tuo carro. Ogni mano iniziale permette fino a tre piazzamenti.'},
 {title:'Cambia un tris',text:'Apri Carte, seleziona le tre carte e premi Cambia tris per ricevere rinforzi.'},
 {title:'Lancia tre dadi',text:'Seleziona il tuo territorio con 12 carri, poi il vicino nemico con 3. Premi Attacca.'},
 {title:'Conquista',text:'Attacca il territorio con un solo difensore. Puoi usare Blitz; quando lo conquisti il minimo viene trasferito subito.'},
 {title:'Spostamento strategico',text:'Seleziona il territorio con 8 carri, poi quello adiacente da 2 e conferma lo spostamento.'}
];
export function tutorialGame(step:number):GameState{
 const s=createGame({players:3,name:'Allievo',difficulties:['facile','facile'],turnSeconds:0,custom:true,seed:173,manualReinforcement:true});
 s.id='tutorial-session';s.tutorialStep=step;s.active=0;s.players.forEach(p=>p.profileId='');if(step===0)return s;
 s.round=5;s.players.forEach(p=>p.setupRemaining=0);s.phase='ATTACK';const from=owned(s,0).find(id=>mapFor(s).territories[id].neighbors.some(n=>s.board[n].owner!==0))!,to=mapFor(s).territories[from].neighbors.find(n=>s.board[n].owner!==0)!;
 s.board[from].armies=12;s.board[to].armies=step===2?3:1;
 if(step===1){s.phase='TRIS_EXCHANGE';const cards=mapFor(s).cards.filter(c=>c.symbol==='fante').slice(0,3).map(c=>c.id);s.players[0].cards=cards;s.deck=s.deck.filter(c=>!cards.includes(c));s.reinforcements=4;if(!validTris(s,0,cards).length)throw Error('Tris tutorial non valido');}
 if(step===4){s.phase='FORTIFY';s.board[from].armies=8;s.board[to]={owner:0,armies:2};}
 if(step===2&&!canAttack(s,from,to))throw Error('Attacco tutorial non valido');assertState(s);return s;
}
export function tutorialComplete(before:GameState,after:GameState){
 switch(before.tutorialStep){case 0:return after.log.length>before.log.length;case 1:return after.log.slice(before.log.length).some(e=>e.action==='exchange');case 2:return after.log.slice(before.log.length).some(e=>e.type==='onDiceRolled');case 3:return after.log.slice(before.log.length).some(e=>e.action==='conquer');case 4:return after.log.slice(before.log.length).some(e=>e.action==='move');default:return false;}
}
