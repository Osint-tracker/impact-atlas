const paths:Record<string,string>={
 home:'M3 11L12 3L21 11M5 10V21H10V15H14V21H19V10',
 play:'M7 4L21 12L7 20Z',chevron:'M9 5L16 12L9 19',back:'M15 5L8 12L15 19',
 shield:'M12 3L21 6V12Q20 19 12 22Q4 19 3 12V6Z',
 swords:'M4 3L20 19M20 3L4 19M3 3L4 8L8 4M21 3L20 8L16 4M2 16L8 22M16 22L22 16',
 cards:'M7 4H20V21H7ZM3 18L1 5L5 4',
 folder:'M2 7V4H10L13 7H22V21H2Z',
 sound:'M3 9H7L12 5V19L7 15H3ZM16 8Q20 12 16 16M19 4Q26 12 19 20',
 chart:'M3 21V14H7V21M10 21V9H14V21M17 21V3H21V21',
 trophy:'M7 3H17V9Q17 15 12 15Q7 15 7 9ZM7 5H3V8Q3 12 8 12M17 5H21V8Q21 12 16 12M12 15V20M7 21H17',
 target:'M12 3A9 9 0 1 0 12 21A9 9 0 1 0 12 3M12 7A5 5 0 1 0 12 17A5 5 0 1 0 12 7M12 10V14M10 12H14',
 gear:'M9 3H15L16 7L20 8L22 13L18 16L17 21H11L9 18L5 18L2 13L5 9L5 5ZM12 8A4 4 0 1 0 12 16A4 4 0 1 0 12 8',
 clock:'M12 3A9 9 0 1 0 12 21A9 9 0 1 0 12 3M12 6V12L16 14',
 robot:'M4 7H20V21H4ZM12 7V3M9 13H10M15 13H16M8 17H16',
 edit:'M4 16L16 4L20 8L8 20L3 21ZM14 6L18 10',
 close:'M5 5L19 19M19 5L5 19',plus:'M12 4V20M4 12H20',minus:'M4 12H20',
 menu:'M4 6H20M4 12H20M4 18H20',move:'M3 5L10 12L3 19M12 5L19 12L12 19',
 mountain:'M2 21L9 5L14 13L17 8L23 21ZM6 12L9 15L12 12',
 flag:'M5 22V3M5 3Q10 0 15 3Q18 5 22 3V13Q18 15 15 13Q10 10 5 13',
 copy:'M8 7H21V22H8ZM4 17H2V2H15V4',share:'M12 16V2M7 7L12 2L17 7M5 11H3V22H21V11H19',
 undo:'M8 4L2 10L8 16M2 10H14Q23 10 21 20',
 center:'M3 9V3H9M15 3H21V9M21 15V21H15M9 21H3V15M8 12H16M12 8V16',
 info:'M12 3A9 9 0 1 0 12 21A9 9 0 1 0 12 3M12 10V17M12 6V7',
 bolt:'M13 2L4 14H11L10 22L21 9H13Z',
};
export function icon(name:string){return `<svg class="ui-icon" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round" stroke-linecap="round"><path d="${paths[name]??paths.info}"/></svg>`;}
/** Replace legacy decorative glyphs without changing labels, SVG text or inputs. */
export function decorateIcons(root:HTMLElement){
 if(typeof document.createTreeWalker!=='function')return;
 const glyphs:Record<string,string>={'◉':'target','▰':'cards','◎':'target','ⓘ':'info','▤':'chart','⌖':'center','♫':'sound','⌛':'clock','★':'trophy','⚑':'flag','›':'chevron','‹':'back','▶':'play','→':'move','↶':'undo','×':'close','☰':'menu','▣':'cards','◇':'flag','↻':'undo'};
 const walk=document.createTreeWalker(root,NodeFilter.SHOW_TEXT);const nodes:Text[]=[];while(walk.nextNode())nodes.push(walk.currentNode as Text);
 for(const node of nodes){if(node.parentElement?.closest('svg,script,style,input,textarea'))continue;const text=node.data;if(![...text].some(c=>glyphs[c]))continue;const fragment=document.createDocumentFragment();for(const c of text){if(glyphs[c]){const span=document.createElement('span');span.innerHTML=icon(glyphs[c]);fragment.append(span.firstChild!);}else fragment.append(document.createTextNode(c));}node.replaceWith(fragment);}
}
