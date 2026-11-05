import {writeFile,mkdir} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {mapFor} from '../src/data/mapGraph.ts';
const map=mapFor();
const background=`<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512"><defs><radialGradient id="g"><stop stop-color="#24505b"/><stop offset="1" stop-color="#0b1f2e"/></radialGradient></defs><rect width="512" height="512" fill="url(#g)"/><g transform="translate(-150 15) scale(.7)" fill="#c8a45a" opacity=".28">${map.territories.map(t=>`<path d="${t.path}"/>`).join('')}</g><rect x="12" y="12" width="488" height="488" rx="52" fill="none" stroke="#c8a45a" stroke-width="4"/></svg>`;
await mkdir('artifacts/atlas',{recursive:true});await writeFile('artifacts/atlas/icon-background.svg',background);
function convert(args){const r=spawnSync('convert',args,{stdio:'inherit'});if(r.status!==0)throw Error('ImageMagick icon build failed');}
const render=spawnSync('inkscape',['artifacts/atlas/icon-background.svg','--export-type=png','--export-filename=artifacts/atlas/icon-background.png'],{stdio:'inherit'});if(render.status!==0)throw Error('SVG rasterization failed');
convert(['artifacts/atlas/icon-background.png','(','public/mascot.png','-resize','440x460',')','-gravity','south','-composite','public/icons/icon-512.png']);
for(const [size,file] of [[192,'icon-192.png'],[180,'apple-touch-icon.png']])convert(['public/icons/icon-512.png','-resize',`${size}x${size}`,`public/icons/${file}`]);
convert(['public/icons/icon-512.png','public/app-icon.jpg']);
convert(['public/app-icon.jpg','android/app/src/main/res/drawable-nodpi/app_icon_art.jpg']);
await writeFile('public/icon.svg',background.replace('</svg>','<path d="M150 290H350V335H150ZM180 260H300L320 285H160ZM230 240H280V265H230ZM270 240H355V252H270Z" fill="#f2e7d3"/></svg>'));
