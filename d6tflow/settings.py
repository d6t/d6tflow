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
log_level = 'WARNING'
execution_summary = True

import luigi.task
def set_parameter_len(nparams=20, len=64):
    luigi.task.TASK_ID_INCLUDE_PARAMS=nparams
    luigi.task.TASK_ID_TRUNCATE_PARAMS=len
set_parameter_len()

uri = None
