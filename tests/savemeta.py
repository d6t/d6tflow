import d6tflow
import pandas as pd

# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskCache):
    
    def run(self):
        df = pd.DataFrame({'a':range(3)})
        self.save(df) # quickly save dataframe
        self.metaSave({'columns':df.columns.tolist()})


class Task2(Task1):
    pass

# define another task that depends on data from task1 and task2
@d6tflow.requires({'upstream1':Task1,'upstream2':Task2})
class Task3(d6tflow.tasks.TaskCache):
    multiplier = d6tflow.IntParameter(default=2)
    
    def run(self):
        meta = self.metaLoad()['upstream1']
        print(meta)
        print(meta['columns'])
        self.metaSave({'columns':100})

d6tflow.run(Task3())
print(Task3().outputLoadAllMeta())