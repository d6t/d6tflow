Transition to d6tflow
==============================================

Current Workflow Using Functions
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


Workflow Using d6tflow Tasks
------------------------------------------------------------

In a d6tflow workflow, you define your own task classes and then execute the workflow by running the final downstream task which will automatically run required upstream dependencies. 

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
                data = scale(data) # # from `def preprocess(data)`
            self.save(data) # save output data

    flow = d6tflow.Workflow(TaskProcess)
    flow.run() # execute task with dependencies
    data = flow.outputLoad() # load output data

Learn more about :doc:`Writing and Managing Tasks <../tasks>` and :doc:`Running Workflows <../run>`.

Interactive Notebook
------------------------------------------------------------

Live mybinder example http://tiny.cc/d6tflow-start-interactive

Design Pattern Templates for Machine Learning Workflows
------------------------------------------------------------

See code templates for a larger real-life project at https://github.com/d6t/d6tflow-template. Clone & code!
