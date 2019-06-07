
import d6tflow
import luigi
from luigi.util import inherits
import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model

import pandas as pd

# define workflow
class TaskGetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

    def run(self):
        iris = sklearn.datasets.load_iris()
        df_train = pd.DataFrame(iris.data,columns=['feature{}'.format(i) for i in range(4)])
        df_train['y'] = iris.target
        self.save(df_train) # quickly save dataframe

class TaskPreprocess(d6tflow.tasks.TaskPqPandas):
    do_preprocess = luigi.BoolParameter(default=False) # parameter for preprocessing yes/no

    def requires(self):
        return TaskGetData() # define dependency

    def run(self):
        df_train = self.input().load() # quickly load required data
        if self.do_preprocess:
            df_train.iloc[:,:-1] = sklearn.preprocessing.scale(df_train.iloc[:,:-1])
        self.save(df_train)

@inherits(TaskPreprocess)
class TaskTrain(d6tflow.tasks.TaskPickle): # save output as pickle
    model = luigi.Parameter(default='ols') # parameter for model selection

    def requires(self):
        return self.clone_parent() # automatically pass parameters upstream

    def run(self):
        df_train = self.input().load()
        if self.model=='ols':
            model = sklearn.linear_model.LogisticRegression()
        elif self.model=='svm':
            model = sklearn.svm.SVC()
        else:
            raise ValueError('invalid model selection')
        model.fit(df_train.iloc[:,:-1], df_train['y'])
        self.save(model)

# Check task dependencies and their execution status
d6tflow.preview(TaskTrain())

'''
└─--[TaskTrain-{'do_preprocess': 'False', 'model': 'ols'} (PENDING)]
   └─--[TaskPreprocess-{'do_preprocess': 'False'} (PENDING)]
      └─--[TaskGetData-{} (PENDING)]
'''

# Execute the model training task including dependencies
d6tflow.run(TaskTrain())

'''
===== Luigi Execution Summary =====

Scheduled 3 tasks of which:
* 3 ran successfully:
    - 1 TaskGetData()
    - 1 TaskPreprocess(do_preprocess=False)
    - 1 TaskTrain(do_preprocess=False, model=ols)
'''

# Load task output to pandas dataframe and model object for model evaluation
model = TaskTrain().output().load()
df_train = TaskPreprocess().output().load()
print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))
# 0.96

# Intelligently rerun workflow after changing parameters
taskModel2 = TaskTrain(do_preprocess=True, model='svm')
d6tflow.preview(taskModel2)

'''
└─--[TaskTrain-{'do_preprocess': 'False'} (PENDING)]
   └─--[TaskPreprocess-{'do_preprocess': 'False'} (PENDING)]
      └─--[TaskGetData-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
'''

# compare results from new model
d6tflow.run(taskModel2)
model = taskModel2.output().load()
df_train = TaskPreprocess(do_preprocess=True).output().load()
print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))
# 0.9733333333333334
