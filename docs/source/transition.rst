Transition to d6tflow
==============================================

Problems with your current workflow
------------------------------------------------------------

Your current workflow probably chains several functions together like in the example below. While quick, it likely has many problems:  

* it doesn't scale well as you add complexity
* you have to manually keep track of which functions were run with which parameter
* you have to manually keep track of where data is saved
* pickle files are neither compressed nor portable
* data files get mixed with code files

.. code-block:: python

    import pandas as pd

    def get_data():
        data = download_data()
        data = clean_data(data)
        data.to_pickle('data.pkl')

    def preprocess(data):
        data = apply_function(data)
        return data

    # flow parameters
    reload_source = True
    do_preprocess = True

    # run workflow
    if reload_source:
        get_data()

    df_train = pd.read_pickle('data.pkl')
    if do_preprocess:
        df_train = preprocess(df_train)
    model = sklearn.svm.SVC()
    model.fit(df_train.iloc[:,:-1], df_train['y'])
    print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))


Benefits of d6tflow
------------------------------------------------------------

Instead of chaining functions that look like: 

.. code-block:: python

    def process_data(data, parameter):

        if parameter:
            data = do_stuff(data)
        else:
            data = do_other_stuff(data)

        data.to_pickle('data.pkl')
        return data

With d6tflow you chain tasks which look like this: 

.. code-block:: python

    class TaskProcess(d6tflow.tasks.TaskPqPandas): # define output format
        parameter = luigi.BoolParameter(default=True) # define parameter

        def requires(self):
            return TaskGetData() # define dependency

        def run(self):
            df_train = self.input().load() # load input data
            if self.parameter: # process data
                data = do_stuff(data)
            else:
                data = do_other_stuff(data)
            self.save(data) # save output data

The benefits of doings this are:

* All tasks follow the same pattern no matter how complex your workflow gets
* You have a scalable input `requires()` and processing function `run()`
* If the input task is not complete it will automatically run before this task
* You can quickly load input data without having to remember where it is saved
* You can quickly save output data without having to remember where it is saved
* If input data or parameters change, the function will automatically rerun

The next section explains how these tasks work in more detail.