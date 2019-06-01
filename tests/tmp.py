import d6tflow
import pandas as pd

class Task1(d6tflow.tasks.TaskPqPandas):
    def run(self):
        df = pd.DataFrame({'a': range(10)})
        self.save(df)

class TaskMultiInput(d6tflow.tasks.TaskCache):
    def requires(self):
        return {1: Task1(), 2: Task1()}

    def run(self):
        dft1, dft2 = self.loadInputs()
        assert dft1.equals(dft2)

TaskMultiInput().run()
quit()

import importlib
import d6tflow
d6tflow.tasks.TaskPqPandas

importlib.reload(d6tflow)

import pandas as pd
df = pd.DataFrame({'a': range(10)})

class Task1(d6tflow.tasks.TaskCache):
    persist=['df']
    def run(self):
        self.save({'df':df})

class Task2(d6tflow.tasks.TaskCache):
    persist=['df']
    def requires(self):
        return Task1()
    def run(self):
        self.save({'df':df})

class Task1a(d6tflow.tasks.TaskCache):
    persist=['df']
    def run(self):
        self.save({'df':df})

class Task3(d6tflow.tasks.TaskCache):
    persist=['df']
    def requires(self):
        return {'2':Task2(),'1a':Task1a()}
    def run(self):
        self.save({'df':df})

Task3().input()

importlib.reload(d6tflow)
d6tflow.show_flow(Task3())
d6tflow.invalidate_downstream(Task3())
d6tflow.invalidate_upstream(Task1(), Task3())

importlib.reload(d6tflow)
d6tflow.run_local([Task3()],forced=[Task1()])
d6tflow.run_local([Task3()])

import luigi.tools.deps
import luigi.tools.deps_tree

import yaml
cfg = yaml.load(open('tests/.creds.yml'))
print(cfg)
print(cfg['d6tpipe_profile'])

from pathlib import Path

pfile = Path('test1/test2/file.ext')
pfile.relative_to(Path('test1'))