import sklearn
import tasks


def accuracy():
    model = tasks.TaskTrain().output().load()
    df_train = tasks.TaskPreprocess().output().load()
    print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))
