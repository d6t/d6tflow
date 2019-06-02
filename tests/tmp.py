import importlib
import d6tflow

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

import luigi
class TaskCollector(luigi.Task):
        def run(self):
            yield Task1()
            yield Task2()

        def invalidate(self, confirm=True):
            [t.invalidate(confirm) for t in self.run()]

        def complete(self):
            return all([t.complete() for t in self.run()])

        def outputLoad(self):
            return [t.outputLoad() for t in self.run()]

d6tflow.run(TaskCollector())
assert TaskCollector().complete()
TaskCollector().invalidate()
assert not TaskCollector().complete()
quit()

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