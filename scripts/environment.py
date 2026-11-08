from pathlib import Path
import json,platform,subprocess,os
r=Path(__file__).resolve().parents[1]
env={'platform':platform.platform(),'python':platform.python_version(),'logical_cpus':os.cpu_count()}
if platform.system()=='Darwin':
    for key,out in [('cpu_model','machdep.cpu.brand_string'),('ram_bytes','hw.memsize')]:
        env[key]=subprocess.check_output(['sysctl','-n',out],text=True).strip()
(r/'outputs/environment.json').write_text(json.dumps(env,indent=2))
print(env)
