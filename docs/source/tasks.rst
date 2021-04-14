Writing and Managing Tasks
==============================================

What are tasks?
------------------------------------------------------------

Tasks are the main object you will be interacting with. They allow you to:

* define input dependency tasks
* process data  
    * load input data from upstream tasks
    * save output data for downstream tasks
* run tasks
* load output data

You write your own tasks by inheriting from one of the predefined d6tflow task formats, for example pandas dataframes saved to parquet. 

.. code-block:: python

    class YourTask(d6tflow.tasks.TaskPqPandas):

Additional details on how to write tasks is below. To run tasks see :doc:`Running Workflows <../run>`.

Define Upstream Dependency Tasks
------------------------------------------------------------

You can define input dependencies by using a `@d6tflow.requires` decorator which takes input tasks. You can have no, one or multiple input tasks. This may be required when the decorator shortcut does not work.

.. code-block:: python

    # no dependency
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):
        #[...]

    # single dependency
    @d6tflow.requires(TaskSingleOutput)
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):
        #[...]

    # multiple dependencies
    @d6tflow.requires({'input1':TaskSingleOutput1, 'input2':TaskSingleOutput2})
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):
        #[...]



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
    @d6tflow.requires(TaskSingleOutput)
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data = self.inputLoad()

    # single dependency, multiple outputs
    @d6tflow.requires(TaskMultipleOutput)
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data1, data2 = self.inputLoad()

    # multiple dependencies, single output
    @d6tflow.requires({'input1':TaskSingleOutput1, 'input2':TaskSingleOutput2})
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data1 = self.inputLoad()['input1']
            data2 = self.inputLoad()['input2']
            # or
            data1 = self.inputLoad(task='input1')
            data2 = self.inputLoad(task='input2')

    # multiple dependencies, multiple outputs
    @d6tflow.requires({'input1':TaskMultipleOutput1, 'input2':TaskMultipleOutput2})
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data = self.inputLoad(as_dict=True)
            data1a = data['input1']['output1']
            data1b = data['input1']['output2']
            data2a = data['input2']['output1']
            data2b = data['input2']['output2']
            # or
            data1a, data1b = self.inputLoad()["input1"]
            data2a, data2b = self.inputLoad()["input2"]
            # or
            data1a, data1b = self.inputLoad(task='input1')
            data2a, data2b = self.inputLoad(task='input2')

    # multiple dependencies (without using dictionary), multiple outputs 
    @d6tflow.requires(TaskMultipleOutput1, TaskMultipleOutput2)
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data = self.inputLoad(as_dict=True)
            data1a = data[0]['output1']
            data1b = data[0]['output2']
            data2a = data[1]['output1']
            data2b = data[1]['output2']
            # or
            data1a, data1b = self.inputLoad()[0]
            data2a, data2b = self.inputLoad()[1]
            # or
            data1a, data1b = self.inputLoad(task=0)
            data2a, data2b = self.inputLoad(task=1)

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

            # multiple files
            from d6tstack.combine_csv import CombinerCSV
            def do_stuff(df):
                return df
            df = CombinerCSV(glob.glob('*.csv'), apply_after_read=do_stuff).to_pandas)


For more advanced options see :doc:`Sharing Workflows and Outputs <../collaborate>`

Dynamic Inputs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See :doc:`Dynamic Tasks <../advtasksdyn>`

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

Changing Task Output Formats
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See :doc:`Targets <../targets>`

Running tasks
------------------------------------------------------------

See :doc:`Running Workflows <../run>`

Load Output Data
------------------------------------------------------------

Once a workflow is run and the task is complete, you can easily load its output data by referencing the task.

.. code-block:: python

    data = flow.outputLoad() # load default task output
    data = flow.outputLoad(as_dict=True) # useful for multi output
    data2 = flow.outputLoad(TaskMultipleOutput, as_dict=True) # load another task output
    data2['data1']
    data2['data2']

**Before you load output data you need to run the workflow**. See :doc:`run the workflow <../run>`. If a task has not been run, it will show

::

    raise RuntimeError('Target does not exist, make sure task is complete')
    RuntimeError: Target does not exist, make sure task is complete


Loading Output Data with Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are :doc:`using parameters <../advparam>` this is how you load outputs. Make sure you run the task with that parameter first.

.. code-block:: python

    params = {'default_params':{}, 'use_params':{'preprocess':True}}
    flow = d6tflow.WorkflowMulti(TaskSingleOutput, params)
    data = flow.outputLoad() # load default task output
    data['default_params']
    data['use_params']

    # multi output
    data2 = flow.outputLoad(TaskMultipleOutput, as_dict=True) # load another task output
    data2['default_params']['data1']
    data2['default_params']['data2']
    data2['use_params']['data1']
    data2['use_params']['data2']


Putting it all together
------------------------------------------------------------

See full example https://github.com/d6t/d6tflow/blob/master/docs/example-ml.md

See real-life project template https://github.com/d6t/d6tflow-template

Advanced: task attribute overrides
------------------------------------------------------------

`persist`: data items to save, see above  
`external`: do check dependencies, good for sharing tasks without providing code
`target_dir`: specify directory  
`target_ext`: specify extension  
`save_attrib`: include taskid in filename  
`pipename`: d6tpipe to save/load to/from  
