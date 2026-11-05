"""Create and verify a byte-exact source delta against a supplied full archive."""
from pathlib import Path
import hashlib,json,sys,zipfile
root=Path(__file__).resolve().parents[1]
baseline=Path(sys.argv[1] if len(sys.argv)>1 else '/workspace/risiko-aggiornamento-1.8.0.zip')
out=Path(sys.argv[2] if len(sys.argv)>2 else '/workspace/risiko-solo-modifiche-2.0.0.zip')
ignored_dirs={'node_modules','dist','artifacts','.git','.gradle','build','DerivedData','Pods','__pycache__'}
ignored_prefixes=('android/app/src/main/assets/','android/capacitor-cordova-android-plugins/','ios/App/App/public/')
ignored_files={'android/local.properties','android/capacitor.settings.gradle','android/app/capacitor.build.gradle'}
def include(n):
 return not any(p in ignored_dirs for p in Path(n).parts) and not n.startswith(ignored_prefixes) and n not in ignored_files and not n.endswith(('.pyc','.DS_Store'))
with zipfile.ZipFile(baseline) as z:old={i.filename:z.read(i) for i in z.infolist() if not i.is_dir() and include(i.filename)}
current={p.relative_to(root).as_posix():p.read_bytes() for p in root.rglob('*') if p.is_file() and include(p.relative_to(root).as_posix())}
changed={n:b for n,b in current.items() if n not in old or b!=old[n]}
deleted=sorted(old.keys()-current.keys())
manifest={'version':'2.0.0','baseline':baseline.name,'baseline_sha256':hashlib.sha256(baseline.read_bytes()).hexdigest(),'files':[{'path':n,'status':'modified' if n in old else 'new','sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)} for n,b in sorted(changed.items())],'deleted':deleted,'excluded':'Dependencies, dist, artifacts, native generated assets and build caches'}
notice='RisiKo Offline 2.0.0 — SOLO FILE MODIFICATI / NUOVI\n\nBase: '+baseline.name+'\nEstrai nella radice del progetto esistente (dove si trova package.json), mantenendo i percorsi e sovrascrivendo gli omonimi. NON eliminare gli altri file e NON caricare una seconda cartella di progetto.\nPrima conserva un backup del progetto e dei dati dell’app.\nLeggi docs/AGGIORNAMENTO-2.0.md e docs/VERIFICHE-2.0.md.\nLe anteprime sono in un archivio separato. Nessun APK incluso.\n\n'+''.join(('MODIFICATO ' if r['status']=='modified' else 'NUOVO     ')+r['path']+'\n' for r in manifest['files'])+'\nRIMOZIONI: '+(', '.join(deleted) if deleted else 'nessuna')+'\n'
with zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
 for n in sorted(changed):z.write(root/n,n)
 z.writestr('FILE-MODIFICATI-2.0.txt',notice)
 z.writestr('MANIFEST-MODIFICHE-2.0.json',json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
with zipfile.ZipFile(out) as z:
 assert z.testzip() is None
 reconstructed={**old,**{n:z.read(n) for n in changed}}
 for n in deleted:reconstructed.pop(n)
 assert reconstructed==current,'Reconstruction differs from current sources'
 assert all(n not in old or z.read(n)!=old[n] for n in changed),'Unchanged file in delta'
 for r in manifest['files']:assert hashlib.sha256(z.read(r['path'])).hexdigest()==r['sha256']
checksum=hashlib.sha256(out.read_bytes()).hexdigest()
Path(str(out)+'.sha256').write_text(checksum+'  '+out.name+'\n')
(root/'artifacts'/'delta-verification.json').write_text(json.dumps({'archive':str(out),'changed':len(changed),'modified':sum(n in old for n in changed),'new':sum(n not in old for n in changed),'deleted':deleted,'source_files_reconstructed':len(current),'reconstruction':'byte-exact','sha256':checksum},indent=2))
print(f'{out}: {len(changed)} file, {sum(n in old for n in changed)} modificati, {sum(n not in old for n in changed)} nuovi; ricostruzione di {len(current)} file verificata byte per byte.')
