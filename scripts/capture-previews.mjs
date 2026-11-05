// Run on a machine where Playwright/Chromium can launch. Does not use a server.
import {chromium} from 'playwright';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
const dir=resolve(process.argv[2]??'artifacts/previews-2.0');
const screens=JSON.parse(await readFile(resolve(dir,'manifest.json'),'utf8'));
const browser=await chromium.launch(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{});
await mkdir(resolve(dir,'png'),{recursive:true});
try{
 for(const s of screens){const page=await browser.newPage({viewport:{width:s.w,height:s.h},deviceScaleFactor:1});await page.goto(pathToFileURL(resolve(dir,s.file)).href);await page.evaluate(()=>document.fonts.ready);await page.emulateMedia({reducedMotion:'reduce'});await page.screenshot({path:resolve(dir,'png',s.id+'.png'),fullPage:true,animations:'disabled'});await page.close();console.log(s.title);}
 const p=await browser.newPage({viewport:{width:1800,height:1200}});await p.goto(pathToFileURL(resolve(dir,'index.html')).href);await p.locator('article').last().waitFor();await p.evaluate(async()=>{document.querySelectorAll('iframe').forEach(f=>f.loading='eager');await document.fonts.ready;});await p.waitForTimeout(1500);await p.screenshot({path:resolve(dir,'png','tavola.png'),fullPage:true,animations:'disabled'});
 await writeFile(resolve(dir,'png','render-info.txt'),'PNG acquisiti con Playwright '+browser.version()+'. I dati delle anteprime sono dimostrativi.\n');
}finally{await browser.close();}
