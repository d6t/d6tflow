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
flow.run()
flow.reset_upstream(confirm=False)
assert flow.get_task('gbm').output().path.exists()==False
assert flow.get_task('gbm').complete()==False
quit()
flow.preview('ols')
flow.run('ols')

flow.run(forced_all_upstream=True, confirm=False)

data = flow.outputLoadAll()

scores = flow.outputLoadMeta()
print(scores)

models = flow.outputLoad(task=ModelTrain)
data_train = flow.outputLoad(task=ModelData)

print(models['ols'].score(data_train['ols'].drop('y',1), data_train['ols']['y']))
print(models['gbm'].score(data_train['gbm'].drop('y',1), data_train['gbm']['y']))
quit()

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

quit()

# model comparison
df_train = ModelEval(**params).outputLoad()
print('insample errors')
print('naive mean',mean_squared_error(df_train[cfg_col_Y],df_train['target_naive1']))
print('ols',mean_squared_error(df_train[cfg_col_Y],df_train['target_ols']))
print('gbm',mean_squared_error(df_train[cfg_col_Y],df_train['target_lgbm']))

print('cv errors')
model_ols = ModelTrainOLS(**params).outputLoad()
mod_lgbm = ModelTrainLGBM(**params).outputLoad()
df_trainX, df_trainY = DataTrain(**params).outputLoad()
print('ols',-cross_validate(model_ols, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())
print('gbm',-cross_validate(mod_lgbm, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())
quit()
# single model experiments
experiments = {'max_depth':1,'max_depth':2}
for experiment in experiments:
    print('cv errors')
    mod_lgbm = ModelTrainLGBM(**params).outputLoad()
    df_trainX, df_trainY = DataTrain(**params).outputLoad()
    print('gbm',-cross_validate(mod_lgbm, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())

# multi model experiments
experiments = {} # ?????? add both lgbm and ols parameters?
for experiment in experiments:
    print('cv errors')
    mod_lgbm = ModelTrainLGBM(**params).outputLoad()
    df_trainX, df_trainY = DataTrain(**params).outputLoad()
    print('gbm',-cross_validate(mod_lgbm, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())
