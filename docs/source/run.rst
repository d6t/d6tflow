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

    flow = d6tflow.WorkflowMult(Task1, {'flow1':{'preprocess':False},'flow2':{'preprocess':True}})
    flow.run() # will run all flow with all parameters

Disable Dependency Checks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, for a task to be complete, it checks if all dependencies are complete also, not just the task itself. To check if just the task is complete without checking dependencies, set ``d6tflow.settings.check_dependencies=False``

.. code-block:: python

    flow.reset(TaskGetData, confirm=False)
    d6tflow.settings.check_dependencies=True # default
    flow.preview() # TaskGetData is pending so all tasks are pending
    '''
    └─--[TaskTrain-{'do_preprocess': 'True'} (PENDING)]
       └─--[TaskPreprocess-{'do_preprocess': 'True'} (PENDING)]
          └─--[TaskGetData-{} (PENDING)]
    '''
    d6tflow.settings.check_dependencies=False # deactivate dependency checks
    flow.preview()
    └─--[TaskTrain-{'do_preprocess': 'True'} (COMPLETE)]
       └─--[TaskPreprocess-{'do_preprocess': 'True'} (COMPLETE)]
          └─--[TaskGetData-{} (PENDING)]
    d6tflow.settings.check_dependencies=True # set to default


Debugging Failures
------------------------------------------------------------

If a task fails, it will show the stack trace. You need to look further up in the stack trace to find the line that caused the error. You can also set breakpoints in the task obviously.

::

    File "tasks.py", line 37, in run => error is here
        1/0
    ZeroDivisionError: division by zero

    [...] => look further up to find error

    ===== d6tflow Execution Summary =====
    Scheduled 2 tasks of which:
    * 1 complete ones were encountered:
        - 1 TaskPreprocess(do_preprocess=True)
    * 1 failed:
        - 1 TaskTrain(do_preprocess=True)
    This progress looks :( because there were failed tasks
    ===== d6tflow Execution Summary =====

     File 
         raise RuntimeError('Exception found running flow, check trace')
    RuntimeError: Exception found running flow, check trace

    => look further up to find error


Rerun Tasks When You Make Changes
------------------------------------------------------------

You have several options to force tasks to reset and rerun. See sections below on how to handle parameter, data and code changes.

.. code-block:: python

    # preferred way: reset single task, this will automatically run all upstream dependencies
    flow.reset(TaskGetData, confirm=False) # remove confirm=False to avoid accidentally deleting data

    # force execution including upstream tasks
    flow.run([TaskTrain()],forced_all=True, confirm=False)

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

    flow = d6tflow.WorkflowMult(Task1, {'flow1':{'preprocess':False},'flow2':{'preprocess':True}})
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

While typically not necessary, you can control change the log level to see additional log data. Default is ``WARNING``. It is a global setting, modify before you execute ``d6tflow.run()``.

.. code-block:: python

    d6tflow.settings.log_level = 'WARNING' # 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
