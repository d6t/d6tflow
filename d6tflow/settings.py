isinit = False # is initialized?
isinitpipe = False # is pipe initialized?
cached = False # cache files in memory
save_with_param = True # save files for each parameter setting

from pathlib import Path
dir = 'data'
dirpath = Path(dir)

db=dirpath/'.d6tflow.json'

check_dependencies = True
check_crc = False

uri = None
