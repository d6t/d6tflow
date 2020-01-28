#**************************************************
# setup
#**************************************************
import d6tflow
import luigi

import pandas as pd
df = pd.DataFrame({'a': range(10)})

class Task1A0(d6tflow.tasks.TaskCache):
    pass

class Task1A(Task1A0):
    persist=['df','df2']
    idx=luigi.IntParameter(default=1)
    idx2=luigi.Parameter(default='test')
    def run(self):
        self.save({'df':df,'df2':df})

class Task1B(d6tflow.tasks.TaskCache):
    persist=['df2','df']
    idx3=luigi.Parameter(default='test3')
    def run(self):
        self.save({'df':df,'df2':df})

class Task1C(d6tflow.tasks.TaskCache):
    persist=['df2','df']
    idx3=luigi.Parameter(default='test3')
    export = False
    def run(self):
        self.save({'df':df,'df2':df})

@d6tflow.requires(Task1A,Task1B,Task1C)
class Task1All(d6tflow.tasks.TaskCache):

    def run(self):
        self.save(df)

d6tflow.run(Task1All())
d6tflow.invalidate_upstream(Task1All(), confirm=False)
d6tflow.preview(Task1All())

task = Task1All()

#**************************************************
# tests
#**************************************************

import pytest

import d6tflow.pipes

def readfile(file_dir):
    with open(file_dir, 'r') as f:
        file = f.read()
        file = file.replace("\n    \n","\n\n") # in order to match string format
    return file

import pathlib
cfg_write_dir=pathlib.Path('tests')
cfg_write_filename_tasks='tasks_d6tpipe.py'
cfg_write_filename_run='run_d6tpipe.py'

@pytest.fixture
def cleanup():
    yield True
    (cfg_write_dir/cfg_write_filename_tasks).unlink()
    (cfg_write_dir/cfg_write_filename_run).unlink()

class TestExport:

    def test_export(self,cleanup):

        e = d6tflow.pipes.FlowExport(Task1All(),'utest-flowexport',write_dir=cfg_write_dir)
        e.generate()

        code = readfile(e.write_dir/e.write_filename_tasks)
        assert code == '''
import d6tflow
import luigi

class Task1A(d6tflow.tasks.TaskCache):
    external=True
    persist=['df', 'df2']
    idx=luigi.parameter.IntParameter(default=1)
    idx2=luigi.parameter.Parameter(default='test')

class Task1B(d6tflow.tasks.TaskCache):
    external=True
    persist=['df2', 'df']
    idx3=luigi.parameter.Parameter(default='test3')

class Task1All(d6tflow.tasks.TaskCache):
    external=True
    persist=['data']
    idx=luigi.parameter.IntParameter(default=1)
    idx2=luigi.parameter.Parameter(default='test')
    idx3=luigi.parameter.Parameter(default='test3')

'''

        code = readfile(e.write_dir/e.write_filename_run)
        assert code == '''
# shared d6tflow workflow, see https://d6tflow.readthedocs.io/en/latest/collaborate.html
import d6tflow.pipes
import tasks_d6tpipe

d6tflow.pipes.init('utest-flowexport',profile='default') # to customize see https://d6tflow.readthedocs.io/en/latest/d6tflow.html#d6tflow.pipes.init
d6tflow.pipes.get_pipe('utest-flowexport').pull()

# task output is loaded below, for more details see https://d6tflow.readthedocs.io/en/latest/tasks.html#load-output-data
df_task1a = tasks_d6tpipe.Task1A(idx=1, idx2='test', ).outputLoad()
df_task1b = tasks_d6tpipe.Task1B(idx3='test3', ).outputLoad()
df_task1all = tasks_d6tpipe.Task1All(idx=1, idx2='test', idx3='test3', ).outputLoad()
'''
