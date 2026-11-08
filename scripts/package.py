"""Create a portable submission bundle, excluding runtimes and regenerable state."""
from pathlib import Path
import zipfile
R=Path(__file__).resolve().parents[1]; dest=R/'output/GridPulse_Submission.zip'
paths=[]
for folder in ['src','scripts','tests','docs']:
    paths += [p for p in (R/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name not in ['EVALUATION_AUDIT.txt','AUDIT_REPRODUCTION.json']]
paths += [R/n for n in ['README.md','requirements.txt','requirements.lock.txt','.gitignore','submission.json']]
paths += [R/'data/raw/manifest.json']
paths += list((R/'output/pdf').glob('*.pdf'))
for p in (R/'outputs').rglob('*'):
    rel=p.relative_to(R/'outputs')
    if not p.is_file() or any(x in rel.parts for x in ['checkpoint','snapshots','inbox','event_logs','clean','hourly']): continue
    if p.suffix in ['.csv','.json','.png','.txt','.sql','.log','.html'] or rel.parts[0]=='evidence_event_logs': paths.append(p)
with zipfile.ZipFile(dest,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(set(paths)):
        if p.exists():z.write(p,Path('GridPulse')/p.relative_to(R))
print(dest)
