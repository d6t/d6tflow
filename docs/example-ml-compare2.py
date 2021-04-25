# processing
import d6tflow
import pandas as pd

# modeling
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import cross_validate
import lightgbm

# cfg
cfg_col_X = ['TV', 'radio', 'newspaper']
cfg_col_Y = 'target'
cfg_run_tests = True

# tasks

class GetData(d6tflow.tasks.TaskPqPandas):

    def run(self):
        df = pd.read_csv('https://www.statlearning.com/s/Advertising.csv', usecols=range(1,5))
        self.save(df)

@d6tflow.requires(GetData)
class ModelFeatures(d6tflow.tasks.TaskPqPandas):
    interaction = d6tflow.BoolParameter(default=False)

    def run(self):
        df = self.inputLoad()
        df['target']=df['sales']
        if self.interaction:
            df['tv_radio']=df['TV']*df['radio']
        self.save(df)

@d6tflow.requires(ModelFeatures)
class DataTrain(d6tflow.tasks.TaskPqPandas):
    persist = ['x','y']

    def run(self):
        df_train = self.inputLoad()
        df_trainX, df_trainY = df_train[cfg_col_X], df_train[[cfg_col_Y]]
        self.save({'x':df_trainX,'y':df_trainY})

@d6tflow.requires(DataTrain)
class ModelTrainOLS(d6tflow.tasks.TaskPickle):

    def run(self):
        df_trainX, df_trainY = self.inputLoad()

        model = LinearRegression()
        model.fit(df_trainX,df_trainY[cfg_col_Y])

        self.save(model)

@d6tflow.requires(DataTrain)
class ModelTrainLGBM(d6tflow.tasks.TaskPickle):
    max_depth = d6tflow.IntParameter(default=2)
    learning_rate = d6tflow.FloatParameter(default=0.1)

    def run(self):
        df_trainX, df_trainY = self.inputLoad()

        model = lightgbm.LGBMRegressor(max_depth=self.max_depth,learning_rate=self.learning_rate)
        model.fit(df_trainX,df_trainY[cfg_col_Y])

        self.save(model)

@d6tflow.requires({'data':ModelFeatures, 'data-train':DataTrain, 'ols':ModelTrainOLS, 'lgbm':ModelTrainLGBM})
class ModelEvalAll(d6tflow.tasks.TaskPqPandas):

    def run(self):
        data = self.inputLoad()
        df_train = data['data']
        df_trainX = data['data-train']['x']

        if cfg_run_tests:
            assert df_train.equals(self.input()['data'].load())
            assert df_train.equals(self.inputLoad(task='data'))
            assert df_trainX.equals(self.input()['data-drain']['x'].load())
            assert df_trainX.equals(self.inputLoad(task='data-train')[0])
            assert df_trainX.equals(self.inputLoad(task='data-train', as_dict=True)['x'])

        df_train['target_naive1'] = df_train['target'].mean()
        df_train['target_ols'] = data['ols'].predict(df_trainX)
        df_train['target_lgbm'] = data['lgbm'].predict(df_trainX)

        self.save(df_train)

params = dict()
d6tflow.preview(ModelEval(**params))
d6tflow.run(ModelEval(**params))#,forced_all=True,confirm=False, forced_all_upstream=True)

# multi model comparison
df_train = ModelEval(**params).outputLoad()
print('insample errors')
print('naive mean',mean_squared_error(df_train[cfg_col_Y],df_train['target_naive1']))
print('ols',mean_squared_error(df_train[cfg_col_Y],df_train['target_ols']))
print('gbm',mean_squared_error(df_train[cfg_col_Y],df_train['target_lgbm']))

print('cv errors')
model_ols = ModelTrainOLS(**params)
mod_lgbm = ModelTrainLGBM(**params)
df_trainX, df_trainY = DataTrain(**params).outputLoad()
print('ols',-cross_validate(model_ols, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())
print('gbm',-cross_validate(mod_lgbm, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())

# single model experiments
experiments = {f'experiment{i}': {'max_depth':d} for i, d in enumerate(range(3))}
for experiment in experiments:
    print('cv errors')
    mod_lgbm = ModelTrainLGBM(**params)
    df_trainX, df_trainY = DataTrain(**params).outputLoad()
    print('gbm',-cross_validate(mod_lgbm, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())

# multi model experiments
experiments = {} # ?????? add both lgbm and ols parameters?
#!!! if changing lgbm or ols params independently it will recalc ModelEval even if no difference between experiments
for experiment in experiments:
    print('cv errors')
    mod_lgbm = ModelTrainLGBM(**params)
    df_trainX, df_trainY = DataTrain(**params).outputLoad()
    print('gbm',-cross_validate(mod_lgbm, df_trainX, df_trainY, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean())
