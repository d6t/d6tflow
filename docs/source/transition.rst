Transition to d6tflow
==============================================

Current Workflow with Functions
------------------------------------------------------------

Your code currently probably looks like the example below. How do you turn it into a d6tflow workflow?

.. code-block:: python

    import pandas as pd

    def get_data():
        data = pd.read_csv('rawdata.csv')
        data = clean(data)
        data.to_pickle('data.pkl')

    def preprocess(data):
        data = scale(data)
        return data

    # execute workflow
    get_data()
    df_train = pd.read_pickle('data.pkl')
    do_preprocess = True
    if do_preprocess:
        df_train = preprocess(df_train)


Target Workflow with d6tflow Tasks
------------------------------------------------------------

In a d6tflow workflow, you define your own task classes and then execute the workflow by running the final downstream task which will automatically run required upstream depencies. 

The function-based workflow example will transform to this:

.. code-block:: python

    import d6tflow
    import pandas as pd

    class TaskGetData(d6tflow.tasks.TaskPqPandas):

        # no dependency

        def run(): # from `def get_data()`
            data = pd.read_csv('rawdata.csv')
            data = clean(data)
            self.save(data) # save output data

    class TaskProcess(d6tflow.tasks.TaskPqPandas):
        do_preprocess = luigi.BoolParameter(default=True) # optional parameter

        def requires(self):
            return TaskGetData() # define dependency

        def run(self): 
            data = self.input().load() # load input data
            if self.do_preprocess:
                data = do_stuff(data) # # from `def preprocess(data)`
            self.save(data) # save output data

    d6tflow.run(TaskProcess()) # execute task with dependencies
    data = TaskProcess().output().load() # load output data

Learn more about :doc:`Tasks <../tasks>` and :doc:`Execution <../run>`.

Also see code template for a larger real-life project at https://github.com/d6t/d6tflow/tree/master/docs/template