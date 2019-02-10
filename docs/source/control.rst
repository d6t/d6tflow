Controlling Workflows
==============================================

Previewing Execution Status
------------------------------------------------------------

Before running a workflow, you can check which tasks will be run.

.. code-block:: python

    d6tflow.preview(TaskTrain()) # single task
    d6tflow.preview([TaskPreprocess(),TaskTrain()]) # multiple tasks

Running Workflows
------------------------------------------------------------

Now you can execute the workflow which creates the targets.

.. code-block:: python

    d6tflow.run(TaskTrain()) # single task
    d6tflow.run([TaskPreprocess(),TaskTrain()]) # multiple tasks


Showing Execution Status
------------------------------------------------------------

If you change anything, you can check the latest execution status.

.. code-block:: python

    d6tflow.show(TaskTrain()) # single task
    d6tflow.show([TaskPreprocess(),TaskTrain()]) # multiple tasks


Manually Running a Single Task
------------------------------------------------------------

You can always run single tasks by calling the `run()` function. This is useful during debugging. However, this will only run this one task and not take care of any downstream dependencies.

.. code-block:: python

    # forcing execution
    TaskTrain().run()


Manually Forcing Task Reset and Rerun
------------------------------------------------------------

You have several options to force tasks to reset and rerun.

.. code-block:: python

    # force execution including downstream tasks
    d6tflow.run([TaskTrain()],force=[TaskGetData()])

    # reset single task
    TaskGetData().invalidate()

    # reset all downstream task output
    d6tflow.invalidate_downstream(TaskGetData(), TaskTrain())

    # reset all upstream task input
    d6tflow.invalidate_upstream(TaskTrain())
    

When to reset and rerun tasks?
------------------------------------------------------------

Typically you want to reset and rerun tasks when:

* parameters changed
* data changed
* code changed

Handling Parameter Change
------------------------------------------------------------

As long as the parameter is defined in the task, d6tflow will automatically rerun tasks with different parameters. 

.. code-block:: python

    d6tflow.run([TaskTrain(do_preprocess=True)]) # first experiment
    d6tflow.run([TaskTrain(do_preprocess=False)]) # another experiment

For d6tflow to intelligently figure out which tasks to rerun, the parameter has to be defined in the task. The downstream task (`TaskTrain`) has to pass on the parameter to the upstream task (`TaskPreprocess`).

.. code-block:: python

    class TaskGetData(d6tflow.tasks.TaskPqPandas):
    # no parameter dependence

    class TaskPreprocess(d6tflow.tasks.TaskCachePandas):  # save data in memory
        do_preprocess = luigi.BoolParameter(default=True) # parameter for preprocessing yes/no

    class TaskTrain(d6tflow.tasks.TaskPickle):
        # pass parameter upstream
        do_preprocess = luigi.BoolParameter(default=True)

        def requires(self):
            # pass parameter upstream
            return TaskPreprocess(do_preprocess=self.do_preprocess)

See [luigi docs for handling parameter inheritance](https://luigi.readthedocs.io/en/stable/api/luigi.util.html#using-inherits-and-requires-to-ease-parameter-pain)

Default Parameter Values in Config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative to inheriting parameters, you can define defaults in a config files. When you change the config it will automatically rerun tasks. **The DOWNSIDE is that previously saved data will be overwritten!**

.. code-block:: python

    class TaskPreprocess(d6tflow.tasks.TaskCachePandas):  
        do_preprocess = luigi.BoolParameter(default=cfg.do_preprocess) # store default in config


Handling Data Change
------------------------------------------------------------

In future releases, d6tflow will automatically detect data changes. For now you have to manually reset tasks.

Handling Code Change
------------------------------------------------------------

Code changes likely lead to data changes. Code changes are difficult to detect and it is best if you manually force tasks to rerun. 

