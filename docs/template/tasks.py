
import d6tflow
import luigi
import sklearn, sklearn.datasets, sklearn.svm
import pandas as pd

import cfg

# define workflow
class TaskGetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

    def run(self):
        iris = sklearn.datasets.load_iris()
        df_train = pd.DataFrame(iris.data,columns=['feature{}'.format(i) for i in range(4)])
        df_train['y'] = iris.target
        self.save(df_train) # quickly save dataframe

class TaskPreprocess(d6tflow.tasks.TaskCachePandas):  # save data in memory
    do_preprocess = luigi.BoolParameter(default=True) # parameter for preprocessing yes/no
    persist = ['data1','data2']

    def requires(self):
        return TaskGetData() # define dependency

    def run(self):
        df_train = self.input().load() # quickly load required data
        if self.do_preprocess:
            df_train.iloc[:,:-1] = sklearn.preprocessing.scale(df_train.iloc[:,:-1])
        self.save({'a':df_train})

class TaskTrain(d6tflow.tasks.TaskPickle): # save output as pickle
    do_preprocess = luigi.BoolParameter(default=cfg.do_preprocess)

    def requires(self):
        return TaskPreprocess(do_preprocess=self.do_preprocess)

    def run(self):
        df_train = self.input().load()
        model = sklearn.svm.SVC()
        model.fit(df_train.iloc[:,:-1], df_train['y'])
        self.save(model)

