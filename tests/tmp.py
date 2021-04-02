import d6tflow
import pandas as pd


# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe
        self.metaSave({'a': 1})


@d6tflow.requires(Task1)
class Task2(Task1):
    pass


# define another task that depends on data from task1 and task2
@d6tflow.requires(Task2)
class Task3(d6tflow.tasks.TaskCache):
    multiplier = d6tflow.IntParameter(default=2)

    def run(self):
        df1 = self.input()['upstream1'].load()  # quickly load input data
        df2 = self.input()['upstream2'].load()  # quickly load input data
        df = df1.join(df2, lsuffix='1', rsuffix='2')
        df['b'] = df['a1'] * self.multiplier  # use task parameter
        self.save(df)
        print(self.metaLoad()['a'])
        self.saveMeta({'b': 2})

d6tflow.run(Task3())
