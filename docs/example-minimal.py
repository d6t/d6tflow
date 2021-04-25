import d6tflow
import pandas as pd


# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe


class Task2(Task1):
    pass


# define another task that depends on data from task1 and task2
@d6tflow.requires({'input1': Task1, 'input2': Task2})
class Task3(d6tflow.tasks.TaskCache):
    multiplier = d6tflow.IntParameter(default=2)

    def run(self):
        df1 = self.input()['input1'].load()  # quickly load input data
        df2 = self.input()['input2'].load()  # quickly load input data
        df = df1.join(df2, lsuffix='1', rsuffix='2')
        df['b'] = df['a1'] * self.multiplier  # use task parameter
        self.save(df)


# Execute task including all its dependencies
flow = d6tflow.Workflow(Task3)
flow.run()
'''
* 3 ran successfully:
    - 1 Task1()
    - 1 Task2()
    - 1 Task3(multiplier=2)
'''

# quickly load output data. Task1().outputLoad() also works
flow.outputLoad()
'''
   a1  a2  b
0   0   0  0
1   1   1  2
2   2   2  4
'''

# Intelligently rerun workflow after changing parameters
flow2 = d6tflow.Workflow(Task3, {'multiplier': 3})
flow2.preview()
'''
└─--[Task3-{'multiplier': '3'} (PENDING)] => this changed and needs to run
   |--[Task1-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
   └─--[Task2-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
'''
