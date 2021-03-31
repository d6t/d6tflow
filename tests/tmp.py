import pandas as pd
import d6tflow
import logging

import sys
log = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe


class Task2(Task1):
    pass


# @d6tflow.requires({0: Task1, 1: Task2})
@d6tflow.requires(Task1, Task2)
class Task3(d6tflow.tasks.TaskCache):
    multiplier = d6tflow.IntParameter(default=2)

    def run(self):
        df1 = self.input()[0].load()  # quickly load input data
        df2 = self.input()[1].load()  # quickly load input data
        df = df1.join(df2, lsuffix='1', rsuffix='2')
        df['b'] = df['a1'] * self.multiplier  # use task parameter
        self.save(df)


# Execute task including all its dependencies
d6tflow.run(Task3())