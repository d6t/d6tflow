Writing and Managing Tasks
==============================================

What are tasks?
------------------------------------------------------------

Tasks are the main object you will be interacting with. They allow your to:

* define input dependency tasks
* process data
    * load input data from upstream tasks
    * save output data for downstream tasks
* run tasks
* load output data

You write a your own tasks by inheriting from one of the predefined d6tflow task formats, for example pandas dataframes saved to parquet. 

.. code-block:: python

    class YourTask(d6tflow.tasks.TaskPqPandas):


Define Upstream Dependency Tasks
------------------------------------------------------------

You define input dependencies by writing a ``requires()`` function which takes input tasks. You can have no, one or multiple input tasks. 

.. code-block:: python

    # no dependency
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):
        # leave blank

    # single dependency
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return TaskGetData()

    # multiple dependencies
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return {'data1':TaskGetData1(), 'data2':TaskGetData2()}

**Make sure you add `()` when you define a dependency, so `TaskGetData()` NOT `TaskGetData`.**

Process Data
------------------------------------------------------------

You process data by writing a ``run()`` function. This function will take input data, process it and save output data.

.. code-block:: python

    class YourTask(d6tflow.tasks.TaskPqPandas):

        def run(self):
            # load input data
            # process data
            # save data


Load Input Data
------------------------------------------------------------

Input data from upstream dependency tasks can be easily loaded in ``run()``

.. code-block:: python

    # no dependency
    class TaskNoInput(d6tflow.tasks.TaskPqPandas):

        def run(self):
            data = pd.read_csv(d6tflow.settings.dirpath/'file.csv') # data/file.csv

    # single dependency, single output
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return TaskGetData()

        def run(self):
            data = self.input().load()

    # single dependency, multiple outputs
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return TaskGetData()

        def run(self):
            data = self.input()['output1'].load()
            data = self.input()['output2'].load()

    # multiple dependencies, single output
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return {'data1':TaskGetData1(), 'data2':TaskGetData2()}

        def run(self):
            data1 = self.input()['data1'].load()
            data2 = self.input()['data2'].load()

    # multiple dependencies, multiple outputs
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return {'data1':TaskMultipleOutput1(), 'data2':TaskMultipleOutput1()}

        def run(self):
            data1 = self.input()['data1']['output1'].load()
            data2 = self.input()['data2']['output1'].load()

Load External Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You probably want to load external data which is not the output of a task. There are a few options.

.. code-block:: python

    class TaskExternalData(d6tflow.tasks.TaskPqPandas):

        def run(self):

            import pandas as pd
            # read from d6tflow data folder
            data = pd.read_parquet(d6tflow.settings.dirpath/'file.pq')

            # totally manual
            data = pd.read_parquet('/some/folder/file.pq')

For more advanced options see :doc:`Sharing Workflows and Outputs <../collaborate>`

Save Output Data
------------------------------------------------------------

Saving output data is quick and convenient. You can save a single or multiple outputs.

.. code-block:: python

    # quick save one output
    class TaskSingleOutput(d6tflow.tasks.TaskPqPandas):

        def run(self):
            self.save(data_output)

    # save more than one output
    class TaskMultipleOutput(d6tflow.tasks.TaskPqPandas):
        persist=['output1','output2'] # declare what you will save

        def run(self):
            self.save({'output1':data1, 'output2':data2}) # needs to match self.persist

When you have multiple outputs and don't include ``persist`` you will get ``raise ValueError('Save dictionary needs to consistent with Task.persist')``


Where Is Output Data Saved?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Output data by default is saved in ``data/``, you can check with

.. code-block:: python

    d6tflow.settings.dirpath # folder where workflow output is saved
    TaskTrain().output().path # file where task output is saved

You can change where data is saved using ``d6tflow.set_dir('data/')``. See advanced options for :doc:`Sharing Workflows and Outputs <../collaborate>`

Running tasks
------------------------------------------------------------

See :doc:`Running Workflows <../run>`

Load Output Data
------------------------------------------------------------

Once a workflow is run and the task is complete, you can easily load its output data by referencing the task.

.. code-block:: python

    df = TaskSingleOutput().output().load()
    data1 = TaskMultipleOutput().output()['data1'].load()
    data2 = TaskMultipleOutput().output()['data2'].load()

**Before you load output data you need to run the workflow**. See :doc:`run the workflow <../run>`. If a task has not been run, it will show

::

    raise RuntimeError('Target does not exist, make sure task is complete')
    RuntimeError: Target does not exist, make sure task is complete

Changing Task Output Formats
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See :doc:`Targets <../targets>`


Putting it all together
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See https://github.com/d6t/d6tflow/blob/master/docs/example-ml.md

