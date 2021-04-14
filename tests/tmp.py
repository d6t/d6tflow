import d6tflow, luigi
import pandas as pd

# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskPickle):

    def run(self):
        df = pd.DataFrame({'a':range(3)})
        self.save(df) # quickly save dataframe
        self.saveMeta({1:1})

Task1(path='data/data2').run()
Task1(path='data/data2').outputLoad()
Task1(path='data/data2').metaLoad()
