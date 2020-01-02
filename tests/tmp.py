import importlib
import d6tflow
import luigi

import pandas as pd
df = pd.DataFrame({'a': range(10)})


class Task1A(d6tflow.tasks.TaskCache):
    persist=['df','df2']
    idx=luigi.Parameter(default='test')
    idx2=luigi.Parameter(default='test')
    def run(self):
        self.save({'df':df,'df2':df})

class Task1B(d6tflow.tasks.TaskCache):
    persist=['df2','df']
    idx3=luigi.Parameter(default='test3')
    def run(self):
        self.save({'df':df,'df2':df})

@d6tflow.inherits(Task1A,Task1B)
class Task1All(d6tflow.tasks.TaskCache):

    def requires(self):
        return dict([('1',Task1A()),('2',Task1B())])

    def run(self):
        self.save(df)

d6tflow.run(Task1All())
d6tflow.invalidate_upstream(Task1All(), confirm=False)
d6tflow.preview(Task1All())


quit()

@d6tflow.requires(Task1A)
class Task2A(d6tflow.tasks.TaskCache):
    def run(self):
        dft,dft2 = self.inputLoad()
        self.save({'df':df})


@d6tflow.inherits(Task1A,Task1B)
class Task2B(d6tflow.tasks.TaskCache):
    def requires(self):
        return {'input1': Task1A(), 'input2': Task1B()}

    def run(self):
        dft,dft2 = self.inputLoad(task='input1')
        self.save({'df':df})

d6tflow.preview(Task2B())
# d6tflow.run(Task2B())

