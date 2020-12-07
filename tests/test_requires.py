import unittest
import pandas as pd
import d6tflow

class Task1(d6tflow.tasks.TaskCache):  
    def run(self):
        df = pd.DataFrame({'a':range(3)})
        self.save(df) # quickly save dataframe

class Task2(Task1):
    pass

# define another task that depends on data from task1 and task2
@d6tflow.requires({'a':Task1,'b':Task2})
class Task3(d6tflow.tasks.TaskCache):
    
    def run(self):
        df1 = self.input()['a'].load() # quickly load input data
        df2 = self.input()['b'].load() # quickly load input data

        self.test.assertTrue(df1.equals(pd.DataFrame({'a':range(3)})))




class TestRequires(unittest.TestCase):
    def test_requires(self):
        task3 = Task3()
        task3.test = self
        
        d6tflow.run(task3)

