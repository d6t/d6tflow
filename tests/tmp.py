import importlib
import d6tflow
import luigi

import pandas as pd
df = pd.DataFrame({'a': range(10)})

class Task1(d6tflow.tasks.TaskCache):
    persist=['df']
    idx=luigi.Parameter(default='test')
    idx2=luigi.Parameter(default='test')
    def run(self):
        self.save({'df':df})

@d6tflow.inherits(Task1)
@d6tflow.clone_parent
class Task2(d6tflow.tasks.TaskCache):
    def run(self):
        self.save({'df':df})

d6tflow.preview(Task2(), clip_params=True)
