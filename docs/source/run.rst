Running Tasks and Managing Workflows
==============================================

A workflow object is used to orchestrate tasks and define a task pipeline.

NB: the workflow object is new preferred way of interacting with workflow. Alternatively, :doc:`legacy workflow <../run_legacy>` describes the old way which might help understand better how everything works.

Define a workflow object
------------------------------------------------------------

Workflow object can be defined by passing the default task and the parameters for the pipeline. Both the arguments are optional.

.. code-block:: python

    flow = d6tflow.Workflow(Task1, params)
    flow = d6tflow.Workflow(Task1) # use default params

Note you want to pass the task definition, not an instantiated task.

.. code-block:: python

    import tasks
    flow = d6tflow.Workflow(tasks.Task1) # yes
    flow = d6tflow.Workflow(tasks.Task1()) # no

Previewing Task Execution Status
------------------------------------------------------------

Running a task will automatically run all the upstream dependencies. Before running a workflow, you can preview which tasks will be run.

.. code-block:: python

    flow.preview() # default task
    flow.preview(TaskTrain) # single task
    flow.preview([TaskPreprocess,TaskTrain]) # multiple tasks

Running Multiple Tasks as Workflows
------------------------------------------------------------

To run all tasks in a workflow, run the downstream task you want to complete. It will check if all the upstream dependencies are complete and if not it will run them intelligently for you. 

.. code-block:: python

    flow.run() # default task
    flow.run(TaskTrain) # single task
    flow.run([TaskPreprocess,TaskTrain]) # multiple tasks

If your tasks are already complete, they will not rerun. To force rerunning of all tasks but there are better alternatives, see below.

.. code-block:: python

    flow.run(forced_all_upstream=True, confirm=False) # use flow.reset() instead


Run and Load in One Call
------------------------------------------------------------

For quick scripts and notebooks, ``d6tflow.runLoad`` builds a workflow, runs the task (with all upstream dependencies) and returns its loaded output in a single call - saving you from creating a ``Workflow`` object just to fetch one result.

.. code-block:: python

    # equivalent to: d6tflow.Workflow(TaskTrain, params).run() then outputLoad()
    model = d6tflow.runLoad(TaskTrain, params={'do_preprocess': True})

    # reset=True forces a rerun first (use after changing the task's code)
    df = d6tflow.runLoad(TaskPreprocess, params={'do_preprocess': True}, reset=True)

    # runIt runs without loading the output (same as runLoad(..., load=False))
    d6tflow.runIt(TaskTrain)


How is a task marked complete?
------------------------------------------------------------

Tasks are complete when task output exists. This is typically the existance of a file, database table or cache. See :doc:`Task I/O Formats <../targets>` how task output is stored to understand what needs to exist for a task to be complete. 

.. code-block:: python

    flow.get_task().complete() # status
    flow.get_task().output().path # where is output saved?
    flow.get_task().output()['output1'].path # multiple outputs

Task Completion with Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a task has parameters, it needs to be run separately for each parameter to be complete when using different parameter settings. The `d6tflow.WorkflowMulti` helps you do that

.. code-block:: python

    flow = d6tflow.WorkflowMulti(Task1, {'flow1':{'preprocess':False},'flow2':{'preprocess':True}})
    flow.run() # will run all flow with all parameters

Disable Dependency Checks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, for a task to be complete, it checks if all dependencies are complete also, not just the task itself. To check if just the task is complete without checking dependencies, set ``d6tflow.settings.check_dependencies=False``

.. code-block:: python

    flow.reset(TaskGetData, confirm=False)
    d6tflow.settings.check_dependencies=True # default
    flow.preview() # TaskGetData is pending so all tasks are pending
    '''
    +--[TaskTrain-{'do_preprocess': 'True'} (PENDING)]
       +--[TaskPreprocess-{'do_preprocess': 'True'} (PENDING)]
          +--[TaskGetData-{} (PENDING)]
    '''
    d6tflow.settings.check_dependencies=False # deactivate dependency checks
    flow.preview()
    +--[TaskTrain-{'do_preprocess': 'True'} (COMPLETE)]
       +--[TaskPreprocess-{'do_preprocess': 'True'} (COMPLETE)]
          +--[TaskGetData-{} (PENDING)]
    d6tflow.settings.check_dependencies=True # set to default


Debugging Failures
------------------------------------------------------------

If a task fails, d6tflow raises a ``RuntimeError`` chained to the original error that caused
the failure (``... the direct cause of the following exception ...``). Read the FIRST
traceback -- the line in your task's ``run()`` is the real cause. Example::

    File "tasks.py", line 37, in run     <== the real error is here
        1/0
    ZeroDivisionError: division by zero

    The above exception was the direct cause of the following exception:
    ...
    RuntimeError: Exception found running flow, check trace

Tips:

* The first traceback (the ``ZeroDivisionError`` above) points at your bug; the trailing
  ``RuntimeError`` is just d6tflow reporting that the flow aborted.
* Set a breakpoint in the task's ``run()`` and step through it.
* Run a single task in isolation to debug it directly: ``TaskTrain().run()``
  (note: this skips dependency resolution -- make sure upstream outputs already exist).
* Turn on engine logging to see which task failed and timing:
  ``d6tflow.enable_logging()`` (see "Logging" below).


Rerun Tasks When You Make Changes
------------------------------------------------------------

You have several options to force tasks to reset and rerun. See sections below on how to handle parameter, data and code changes.

.. code-block:: python

    # preferred way: reset single task, this will automatically run all upstream dependencies
    flow.reset(TaskGetData, confirm=False) # remove confirm=False to avoid accidentally deleting data

    # force execution including upstream tasks
    flow.run([TaskTrain],forced_all=True, confirm=False)

    # force run everything
    flow.run(forced_all_upstream=True, confirm=False)


When to reset and rerun tasks?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Typically you want to reset and rerun tasks when:

* parameters changed
* data changed
* code changed

Handling Parameter Change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As long as the parameter is defined in the task, d6tflow will automatically rerun tasks with different parameters. 

.. code-block:: python

    flow = d6tflow.WorkflowMulti(Task1, {'flow1':{'preprocess':False},'flow2':{'preprocess':True}})
    flow.run() # executes 2 flows, one for each task

For d6tflow to intelligently figure out which tasks to rerun, the parameter has to be defined in the task. The downstream task (`TaskTrain`) has to pass on the parameter to the upstream task (`TaskPreprocess`).

.. code-block:: python

    class TaskGetData(d6tflow.tasks.TaskPqPandas):
    # no parameter dependence

    class TaskPreprocess(d6tflow.tasks.TaskCachePandas):  # save data in memory
        do_preprocess = d6tflow.BoolParameter(default=True) # parameter for preprocessing yes/no

    @d6tflow.requires(TaskPreprocess)
    class TaskTrain(d6tflow.tasks.TaskPickle):
        # pass parameter upstream
        # no need for to define it again: do_preprocess = d6tflow.BoolParameter(default=True)


See [d6tflow docs for handling parameter inheritance](https://d6tflow.readthedocs.io/en/stable/api/d6tflow.util.html#using-inherits-and-requires-to-ease-parameter-pain)

Default Parameter Values in Config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As an alternative to inheriting parameters, you can define defaults in a config files. When you change the config it will automatically rerun tasks.

.. code-block:: python

    class TaskPreprocess(d6tflow.tasks.TaskCachePandas):  
        do_preprocess = d6tflow.BoolParameter(default=cfg.do_preprocess) # store default in config


Handling Data Change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Premium feature, request access at https://pipe.databolt.tech/gui/request-premium/. You can manually reset tasks if you know your data has changed.

Handling Code Change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Premium feature, request access at https://pipe.databolt.tech/gui/request-premium/. You can manually reset tasks if you know your code has changed.

Forcing a Single Task to Run
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can always run single tasks by calling the `run()` function. This is useful during debugging. However, this will only run this one task and not take care of any downstream dependencies.

.. code-block:: python

    # forcing execution
    flow.get_task().run()
    # or
    TaskTrain().run()

Hiding Execution Output
------------------------------------------------------------

By default, the workflow execution summary is shown, because it shows important information which tasks were run and if any failed. At times, eg during deployment, it can be desirable to not show the execution output.

.. code-block:: python

    d6tflow.settings.execution_summary = False # global
    # or
    flow.run(execution_summary=False) # at each run

Logging
------------------------------------------------------------

d6tflow can log engine activity (task start/complete, timing, failures) and gives each task a
contextual ``self.logger`` for logging from inside your own ``run()``. Logging is disabled by
default. Quick start:

.. code-block:: python

    import d6tflow
    d6tflow.enable_logging()                 # INFO+ to stderr
    d6tflow.enable_logging(level="DEBUG")    # also I/O, cached-skips, dependency detail
    d6tflow.disable_logging()                # silence again

See :doc:`logging` for the full guide, including ``self.logger``, log levels, and routing
d6tflow records into your application's own loguru sinks.

Cloud Storage
------------------------------------------------------------

By default task output is written under the local data directory (``d6tflow.set_dir()``). You can instead store output in cloud storage (S3, GCS, etc.) - d6tflow uses `fsspec <https://github.com/fsspec>`_ / `universal-pathlib <https://pypi.org/project/universal-pathlib/>`_ under the hood, so task code does not change.

Install the relevant extra first, e.g. ``pip install d6tflow[gcs]`` or ``pip install d6tflow[s3]`` (``cloud-base`` for other fsspec protocols), then enable it once before running:

.. code-block:: python

    import d6tflow

    # Google Cloud Storage shortcut
    d6tflow.enable_gcs(bucket='my-bucket', prefix='myproject')

    # any fsspec protocol (s3, gcs, dropbox, ...)
    d6tflow.enable_cloud_storage(protocol='s3', bucket='my-bucket', prefix='myproject')

    flow = d6tflow.Workflow(TaskTrain)
    flow.run()   # task output now reads/writes under s3://my-bucket/myproject/

``prefix`` is optional and behaves like a top-level folder within the bucket.
