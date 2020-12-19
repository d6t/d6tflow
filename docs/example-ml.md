# Example Usage For a Machine Learning Workflow

Below is an example of a typical machine learning workflow: you retreive data, preprocess it, train a model and evaluate the model output.

In this example you will:
* Build a machine learning workflow made up of individual tasks
* Check task dependencies and their execution status
* Execute the model training task including dependencies
* Save intermediary task output to Parquet, pickle and in-memory
* Load task output to pandas dataframe and model object for model evaluation
* Intelligently rerun workflow after changing a preprocessing parameter

Code is below, interactive example available on **[mybinder](http://tiny.cc/d6tflow-start-interactive)**.

## Workflow with d6tpipe

```python


import d6tflow
import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model
import pandas as pd

# define workflow
class TaskGetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

    def run(self):
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_train['y'] = ds.target
        self.save(df_train) # quickly save dataframe


@d6tflow.requires(TaskGetData) # define dependency
class TaskPreprocess(d6tflow.tasks.TaskPqPandas):
    do_preprocess = d6tflow.BoolParameter(default=True) # parameter for preprocessing yes/no

    def run(self):
        df_train = self.input().load() # quickly load required data
        if self.do_preprocess:
            df_train.iloc[:,:-1] = sklearn.preprocessing.scale(df_train.iloc[:,:-1])
        self.save(df_train)

@d6tflow.requires(TaskPreprocess) # automatically pass parameters upstream
class TaskTrain(d6tflow.tasks.TaskPickle): # save output as pickle
    model = d6tflow.Parameter(default='ols') # parameter for model selection

    def run(self):
        df_train = self.input().load()
        if self.model=='ols':
            model = sklearn.linear_model.LogisticRegression()
        elif self.model=='svm':
            model = sklearn.svm.SVC()
        else:
            raise ValueError('invalid model selection')
        model.fit(df_train.drop('y',1), df_train['y'])
        self.save(model)

# goal: compare performance of two models
params_model1 = {'do_preprocess':True, 'model':'ols'}
params_model2 = {'do_preprocess':False, 'model':'svm'}

# run workflow for model 1
d6tflow.run(TaskTrain(**params_model1)) 

'''
===== Luigi Execution Summary =====

Scheduled 3 tasks of which:
* 3 ran successfully:
    - 1 TaskGetData()
    - 1 TaskPreprocess(do_preprocess=False)
    - 1 TaskTrain(do_preprocess=False, model=ols)
'''

# Intelligently rerun workflow after changing parameters
d6tflow.preview(TaskTrain(**params_model2))

'''
└─--[TaskTrain-{'do_preprocess': 'False'} (PENDING)]
   └─--[TaskPreprocess-{'do_preprocess': 'False'} (PENDING)]
      └─--[TaskGetData-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
'''

# run workflow for model 2
d6tflow.run(TaskTrain(**params_model2))

# compare results from new model
# Load task output to pandas dataframe and model object for model evaluation

model1 = TaskTrain(**params_model1).output().load()
df_train = TaskPreprocess(**params_model1).output().load()
print(model1.score(df_train.drop('y',1), df_train['y']))
# 0.987

model2 = TaskTrain(**params_model2).output().load()
df_train = TaskPreprocess(**params_model2).output().load()
print(model2.score(df_train.drop('y',1), df_train['y']))
# 0.922

```

## Next steps

See https://d6tflow.readthedocs.io/en/latest/transition.html
