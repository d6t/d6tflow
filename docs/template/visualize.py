def accuracy():
    model = TaskTrain().output().load()
    df_train = TaskPreprocess().output().load()
    print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))
