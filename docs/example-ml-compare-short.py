import d6tflow
import sklearn.datasets, sklearn.ensemble, sklearn.linear_model
import pandas as pd


# get training data and save it
class GetData(d6tflow.tasks.TaskPqPandas):
    persist = ['x','y']

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_trainX = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_trainY = pd.DataFrame(ds.target, columns=['target'])
        self.save({'x': df_trainX, 'y': df_trainY}) # persist/cache training data


# train different models to compare
@d6tflow.requires(GetData)  # define dependency
class ModelTrain(d6tflow.tasks.TaskPickle):
    model = d6tflow.Parameter()  # parameter for model selection

    def run(self):
        df_trainX, df_trainY = self.inputLoad()  # quickly load input data

        if self.model=='ols':  # select model based on parameter
            model = sklearn.linear_model.LinearRegression()
        elif self.model=='gbm':
            model = sklearn.ensemble.GradientBoostingRegressor()

        # fit and save model with training score
        model.fit(df_trainX, df_trainY)
        self.save(model)
        self.saveMeta({'score': model.score(df_trainX, df_trainY)})

# goal: compare performance of two models
# define workflow manager
flow = d6tflow.WorkflowMulti(ModelTrain, {'model1':{'model':'ols'}, 'model2':{'model':'gbm'}})
flow.reset_upstream(confirm=False) # DEMO ONLY: force re-run
flow.run()  # execute model training including all dependencies

'''
===== Execution Summary =====
Scheduled 2 tasks of which:
* 2 ran successfully:
    - 1 GetData()
    - 1 ModelTrain(model=ols)
This progress looks :) because there were no failed tasks or missing dependencies
'''

scores = flow.outputLoadMeta()  # load model scores
print(scores)
# {'model1': {'score': 0.7406426641094095}, 'gbm': {'model2': 0.9761405838418584}}
