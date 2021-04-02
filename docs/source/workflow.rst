Workflow
==============================================

Workflow object is used to orchestrate tasks and define a task pipeline

Define a workflow object
------------------------------------------------------------

Workflow object can be defined by passing the parameters and the default task for the pipeline. Both the arguments are optional.

.. code-block:: python

    flow = Workflow(params, task=Task1)


Previewing the flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The pipeline can be previewed for the defined flow and passing the task. If nothing is passed, default task used during the initiation of the flow object is used

.. code-block:: python

    flow.preview(Task1)


Runinng the flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A list of tasks can run for the defined parameters of the flow. Other arguments that can be passed during the flow are:
`forced`, `forced_all`,`forced_all_upstream`, `confirm`, `workers`, `abort`, `execution_summary`. Any additional named arguments can also be passed for the task objects.

.. code-block:: python

    flow.run(Task1)


Getting the output load for the flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To get the outputload for a specific task, after running it:

.. code-block:: python

    flow.run(Task1)
    flow.outputLoad(Task1)


Getting the output load for the flow including upstream tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To get the outputload for a specific task along with its upstream tasks, after running it:

.. code-block:: python

    flow.run(Task1)
    flow.outputLoadAll(Task1)


Reset task for the flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To reset the task for the flow:

.. code-block:: python

    flow.reset(Task1)


Reset downstream tasks for the flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To reset the task for the flow:

.. code-block:: python

    flow.reset_downstream(Task1)

Setting the default task for the flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To set the default task for the flow:

.. code-block:: python

    flow.set_default(Task1)

Getting the task the for flow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A task object can be retrieved by calling the get_task method

.. code-block:: python

    flow.get_task(Task1)


Define a multi experiment workflow object
------------------------------------------------------------

A multi experiment workflow can be defined with multiple flows and separate parameters for each flow and a default task. It is mandatory to define the flows and parameters for each of the flows.

.. code-block:: python

        flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}}, task=Task1)


Operations on multi experiment workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All the operations like `run`, `preview`, `outputLoad`, `outputLoadAll`, `reset`, `reset_downstream` , `get_task` can be called for the multi flow object.
Each of this functions have an extra argument called flow which can be used to define the flow parameters to be used foe the corresponding fucntions.
If the flow parameter is not passed

.. code-block:: python

    flow2.run(Task1, flow = "experiment1")
    flow2.preview(Task1, flow = "experiment2")
    flow2.get_task(Task1, flow = "experiment1")
    flow2.outputLoad(Task1, flow = "experiment1")
    flow2.outputLoadAll(Task1, flow = "experiment1")
    flow2.reset(Task1, flow = "experiment1")
    flow2.reset_downstream(Task1, flow = "experiment1")
