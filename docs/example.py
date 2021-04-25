import d6tflow
import sklearn, sklearn.datasets, sklearn.ensemble, sklearn.linear_model
import pandas as pd

# define workflow
class GetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_train['y'] = ds.target
        self.save(df_train) # quickly save dataframe


@d6tflow.requires(GetData) # define dependency
class ModelData(d6tflow.tasks.TaskPqPandas):
    do_preprocess = d6tflow.BoolParameter(default=True) # parameter for preprocessing yes/no

    def run(self):
        df_train = self.input().load() # quickly load required data
        if self.do_preprocess:
            df_train.iloc[:,:-1] = sklearn.preprocessing.scale(df_train.iloc[:,:-1])
        self.save(df_train)

@d6tflow.requires(ModelData) # automatically pass parameters upstream
class ModelTrain(d6tflow.tasks.TaskPickle): # save output as pickle
    model = d6tflow.Parameter(default='ols') # parameter for model selection

    def run(self):
        df_train = self.input().load()
        if self.model=='ols':
            model = sklearn.linear_model.LinearRegression()
        elif self.model=='gbm':
            model = sklearn.ensemble.GradientBoostingRegressor()
        else:
            raise ValueError('invalid model selection')
        model.fit(df_train.drop('y',1), df_train['y'])
        self.save(model)
        self.saveMeta({'score':model.score(df_train.drop('y',1), df_train['y'])})

# goal: compare performance of two models
params_model1 = {'do_preprocess':True, 'model':'ols'}
params_model2 = {'do_preprocess':False, 'model':'gbm'}

# run workflow
flow = d6tflow.WorkflowMulti(ModelTrain, {'ols':params_model1, 'gbm':params_model2})
flow.reset_upstream(confirm=False) # force re-run
print(flow.preview('ols'))

flow.run()
'''
Scheduled 3 tasks of which:
* 3 ran successfully:
    - 1 GetData()
    - 1 ModelData(do_preprocess=True)
    - 1 ModelTrain(do_preprocess=True, model=ols)

# To run 2nd model, don't need to re-run all tasks, only the ones that changed
Scheduled 3 tasks of which:
* 1 complete ones were encountered:
    - 1 GetData()
* 2 ran successfully:
    - 1 ModelData(do_preprocess=False)
    - 1 ModelTrain(do_preprocess=False, model=gbm)

'''

data = flow.outputLoadAll()

scores = flow.outputLoadMeta()
print(scores)
# {'ols': {'score': 0.7406426641094095}, 'gbm': {'score': 0.9761405838418584}}

# get training data and models
data_train = flow.outputLoad(task=ModelData)
models = flow.outputLoad(task=ModelTrain)

print(models['ols'].score(data_train['ols'].drop('y',1), data_train['ols']['y']))
# 0.7406426641094095
print(models['gbm'].score(data_train['gbm'].drop('y',1), data_train['gbm']['y']))
# 0.9761405838418584
