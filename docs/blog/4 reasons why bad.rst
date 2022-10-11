4 Reasons Why Your Machine Learning Code is Probably Bad
============================================================

Your current workflow probably chains several functions together like in the example below. While quick, it likely has many problems:  

* it doesn't scale well as you add complexity
* you have to manually keep track of which functions were run with which parameter as you iterate through your workflow
* you have to manually keep track of where data is saved
* it's difficult for others to read

.. code-block:: python

    import pandas as pd
    import sklearn.svm, sklearn.metrics

    def get_data():
        data = download_data()
        data.to_pickle('data.pkl')

    def preprocess(data):
        data = clean_data(data)
        return data

    # flow parameters
    do_preprocess = True

    # run workflow
    get_data()

    df_train = pd.read_pickle('data.pkl')
    if do_preprocess:
        df_train = preprocess(df_train)
    model = sklearn.svm.SVC()
    model.fit(df_train.iloc[:,:-1], df_train['y'])
    print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))

What to do about it?
------------------------------------------------------------

Instead of linearly chaining functions, data science code is better written as a set of tasks with dependencies between them. That is your data science workflow should be a DAG.

`d6tflow <https://github.com/d6t/d6tflow>`_ is a free open-source library which makes it easy for you to build highly effective data science workflows.

Instead of writing a function that does:

.. code-block:: python

    def process_data(df, parameter):
        df = do_stuff(df)
        data.to_pickle('data.pkl')
        return df

    dataraw = download_data()
    data = process_data(dataraw)

You can write tasks that you can chain together as a DAG:

.. code-block:: python

    class TaskGetData(d6tflow.tasks.TaskPqPandas):

        def run():
            data = download_data()
            self.save(data) # save output data

    @d6tflow.requires(TaskGetData) # define dependency
    class TaskProcess(d6tflow.tasks.TaskPqPandas):

        def run(self):
            data = self.input().load() # load input data
            data = do_stuff(data) # process data
            self.save(data) # save output data

    flow = d6tflow.Workflow(task=TaskProcess)
    flow.run() # execute task including dependencies
    data = flow.outputLoad(TaskProcess) # load output data

The benefits of doings this are:

* All tasks follow the same pattern no matter how complex your workflow gets
* You have a scalable input ``requires()`` and processing function ``run()``
* You can quickly load and save data without having to hardcode filenames
* If the input task is not complete it will automatically run
* If input data or parameters change, the function will automatically rerun
* It’s much easier for others to read and understand the workflow

An example machine learning DAG
------------------------------------------------------------

Below is a stylized example of a machine learning flow which is expressed as a DAG. In the end you just need to run `TaskTrain()` and it will automatically know which dependencies to run. For a full example see https://github.com/d6t/d6tflow/blob/master/docs/example-ml.md

.. code-block:: python

    import pandas as pd
    import sklearn, sklearn.svm
    import d6tflow
    import luigi

    # define workflow
    class TaskGetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

        def run(self):        
            data = download_data()
            data = clean_data(data)
            self.save(data) # quickly save dataframe

    @d6tflow.requires(TaskGetData) # define dependency
    class TaskPreprocess(d6tflow.tasks.TaskCachePandas):  # save data in memory
        do_preprocess = luigi.BoolParameter(default=True) # parameter for preprocessing yes/no

        def run(self):
            df_train = self.input().load() # quickly load required data
            if self.do_preprocess:
                df_train = preprocess(df_train)
            self.save({'df_train': df_train})

    @d6tflow.requires(TaskPreprocess) # define dependency
    class TaskTrain(d6tflow.tasks.TaskPickle): # save output as pickle
        model = luigi.Parameter(default='ols')

        def run(self):
            df_train = self.inputLoad(task='TaskPreprocess', key='df_train')
            if self.model=='ols':
                model = sklearn.linear_model.LogisticRegression()
            elif self.model=='svm':
                model = sklearn.svm.SVC()
            else:
                raise ValueError('invalid model selection')
            model.fit(df_train.drop('y',1), df_train['y'])
            self.save(model)

    # Check task dependencies and their execution status
    flow = d6tflow.Workflow(task=TaskTrain)
    flow.preview(TaskTrain)

    '''
    └─--[TaskTrain-{'do_preprocess': 'True'} (PENDING)]
       └─--[TaskPreprocess-{'do_preprocess': 'True'} (PENDING)]
          └─--[TaskGetData-{} (PENDING)]
    '''

    # Execute the ols model training task including dependencies
    flow.run()

    '''
    ===== Luigi Execution Summary =====

    Scheduled 3 tasks of which:
    * 3 ran successfully:
        - 1 TaskGetData()
        - 1 TaskPreprocess(do_preprocess=True)
        - 1 TaskTrain(do_preprocess=True)
    '''

    # Execute the svm model training task including dependencies
    params = dict(model='svm')
    flow = d6tflow.Workflow(task=TaskTrain, params=params)
    flow.run()

    '''
    ===== Luigi Execution Summary =====

    Scheduled 3 tasks of which:
    * 2 complete ones were encountered:
        - 1 TaskGetData()
        - 1 TaskPreprocess(do_preprocess=True)
    * 1 ran successfully:
        - 1 TaskTrain(do_preprocess=True, model='svm')
    '''

    # Load task output to pandas dataframe and model object for model evaluation
    model = flow.outputLoad(TaskTrain)
    df_train = flow.outputLoad(TaskPreprocess, key='df_train')
    print(model.score(df_train.drop('y',1), df_train['y']))
    # 0.9733333333333334

Conclusion
------------------------------------------------------------

Writing machine learning code as a linear series of functions likely creates many workflow problems. Because of the complex dependencies between different ML tasks it is better to write them as a DAG. https://github.com/d6t/d6tflow makes this very easy. Alternatively you can use `luigi 
<https://github.com/spotify/luigi>`_ and `airflow 
<https://airflow.apache.org/>`_  but they are more optimized for ETL than data science.
