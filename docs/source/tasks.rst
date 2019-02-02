Managing Tasks
==============================================

What are tasks?
------------------------------------------------------------

Tasks are the main object you will be interacting with. They allow your to:

* define input data
* process data
    * load input data
    * save output data
* load output data

Define Input Data
------------------------------------------------------------

You define inputs by writing a `requires` function. You can have one or multiple inputs for a task.

.. code-block:: python

    # single dependency
    class TaskSingleInput(d6tflow.tasks.TaskCachePandas):

        def requires(self):
            return TaskGetData()

    # multiple dependencies
    class TaskMultipleInput(d6tflow.tasks.TaskCachePandas):

        def requires(self):
            return {'data1':TaskGetData1(), 'data2':TaskGetData2()}

Process Data
------------------------------------------------------------

You process data by writing a `run` function. 

.. code-block:: python

    class Task(d6tflow.tasks.TaskCachePandas):

        def run(self):
            # process data


Load Input Data
------------------------------------------------------------

After you have defined inputs, you can load them in the function that processes input data.

.. code-block:: python

    # single dependency
    class TaskSingleInput(d6tflow.tasks.TaskCachePandas):

        def requires(self):
            return TaskGetData()

        def run(self):
            data = self.input().load()

    # multiple dependencies
    class TaskMultipleInput(d6tflow.tasks.TaskCachePandas):

        def requires(self):
            return {'data1':TaskGetData1(), 'data2':TaskGetData2()}

        def run(self):
            data1 = self.input()['data1'].load()
            data2 = self.input()['data2'].load()

Load External Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You probably want to load external data which is not the output of a task. There are a few options.

.. code-block:: python

    class TaskExternalData(d6tflow.tasks.TaskCachePandas):

        def run(self):

            import pandas as pd
            # read from d6tflow data folder
            data = pd.read_parquet(d6tflow.settings.dirpath/'file.pq')

            # totally manual
            data = pd.read_parquet('/some/folder/file.pq')


Save Output Data
------------------------------------------------------------

Saving output data is quick and convenient. You can save a single or multiple outputs.

.. code-block:: python

    # quick save one output
    class TaskSingleOutput(d6tflow.tasks.TaskCachePandas):

        def run(self):
            self.save(data_output)

    # save more than one output
    class TaskMultipleOutput(d6tflow.tasks.TaskCachePandas):
        persist=['data1','data2']

        def run(self):
            self.save({'data1':data1, 'data2':data2})

Load Output Data
------------------------------------------------------------

Once a task is complete, you can quickly load output data.

.. code-block:: python

    df = TaskSingleOutput().output().load()
    data1 = TaskMultipleOutput().output()['data1'].load()
    data2 = TaskMultipleOutput().output()['data2'].load()


Matching Task and Targets Output Formats
------------------------------------------------------------

To correctly load data, make sure your match the target and task output formats. **Note each task can only have ONE output format, that is all outputs have to have the same format!**

There are many tasks and targets, here are a few examples:

* Load to pandas, save as parquet
* Load to dictionary, save as pickle
* Load to dictionary, save as JSON

See :doc:`Targets <../targets>`