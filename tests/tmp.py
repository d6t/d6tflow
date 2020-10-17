import d6tflow, luigi
import pandas as pd


# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskPqPandas):
    persist = ['a','b']

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save({'a':df,'b':df})  # quickly save dataframe


class Task2(Task1):
    pass


# define another task that depends on data from task1 and task2
@d6tflow.requires(Task1, Task2)
class Task3(d6tflow.tasks.TaskPqPandas):
    multiplier = luigi.IntParameter(default=2)

    def run(self):
        self.inputLoad()
        df1a,df1b = self.input()[0].load()  # quickly load input data
        df1=df1a
        df2 = self.input()[1].load()  # quickly load input data
        df = df1.join(df2, lsuffix='1', rsuffix='2')
        df['b'] = df['a1'] * self.multiplier  # use task parameter
        self.save(df)


# Execute task including all its dependencies
d6tflow.run(Task3(),forced_all_upstream=True,confirm=False)
