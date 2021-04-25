import d6tflow
import sklearn, sklearn.datasets, sklearn.ensemble, sklearn.linear_model
import pandas as pd
import lightgbm

# from d6tflow.tasks.onnx import ONNXModel

# define workflow
class GetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_train['y'] = ds.target
        self.save(df_train) # quickly save dataframe

@d6tflow.requires(GetData) # automatically pass parameters upstream
# class TaskTrainOLS(ONNXModel): # save output as onnx
class TrainOLS(d6tflow.tasks.TaskPickle): # save output as pickle

    def run(self):
        df_trainX = self.inputLoad()
        df_trainY = df_trainX.pop('y')

        model = sklearn.linear_model.LinearRegression()
        model.fit(df_trainX,df_trainY)

        self.save(model)

@d6tflow.requires(GetData) # automatically pass parameters upstream
# class TaskTrainOLS(ONNXModel): # save output as onnx
class TrainGBM(d6tflow.tasks.TaskPickle): # save output as pickle

    def run(self):
        df_trainX = self.inputLoad()
        df_trainY = df_trainX.pop('y')

        model = lightgbm.LGBMRegressor()
        model.fit(df_trainX,df_trainY)

        self.save(model)

@d6tflow.requires({'ols':TrainOLS,'gbm':TrainGBM})
class TrainAllModels(d6tflow.tasks.TaskAggregator):

    def run(self):
        yield self.clone(TrainOLS)
        yield self.clone(TrainGBM)

flow = d6tflow.Workflow({}, default=TrainAllModels)
flow.run(forced_all_upstream=True, confirm=False)

model_ols = TrainOLS().outputLoad()
model_gbm = TrainGBM().outputLoad()
df_trainX = GetData().outputLoad()
df_trainY = df_trainX.pop('y')
print(model_ols.score(df_trainX, df_trainY))
print(model_gbm.score(df_trainX, df_trainY))

