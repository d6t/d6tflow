Functional Tasks
==============================================

What are functional tasks?
------------------------------------------------------------

Functional tasks are meant to provide a nice decorator based way of defining tasks.

How to create a functional task?
------------------------------------------------------------

For defining our tasks we will need to first define a `Flow()` object.

.. code-block:: python

    from d6tflow.functional import Flow
    flow = Flow()

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^




Each function is decorated with a `flow.step` decorator - that
takes a `d6tflow.tasks.TaskName` as parameter


.. code-block:: python

    @flow.step(d6tflow.tasks.TaskPqPandas)
        def your_functional_task(task):
            print("Running a complicated task!!")



You might have noticed we provide a `task` parameter to the function above.

This is deliberate. 

If you have worked with d6tflow.task before you would remember having a `self` parameter passed to `run()` method.

Here `task` is exactly that. It contains all methods available in `d6tflow.task.Task` 


^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Running a functional task
------------------------------------------------------------

All functional tasks are run as `d6tflow.task` under the hood.

So we require to run them as you would run any `d6tflow.task`

`Flow()` object comes with a run method which does exactly that.

.. code-block:: python
    
    flow.run(your_functional_task)

    
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Below is a minimal example of functional task that encompasses everything mentioned above.

.. code-block:: python

    import d6tflow
    from d6tflow.functional import Flow
    import pandas as pd

    flow = Flow()

    @flow.step(d6tflow.tasks.TaskCache)
    def sample_functional_task(task):
        df = pd.DataFrame({'a':range(3)})
        print("Functional task running!")
        task.save(df)

    flow.run(sample_functional_task)

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Additional decorators
------------------------------------------------------------

These decorators are to be decorated after @flow.step

* `@flow.persists`
    *  Takes in a list of variables that need to be persisted for the flow step.

    *   .. code-block:: python

            @flow.persists(['a1', 'a2'])


* `@flow.requires`
    * Defines dependencies between flow steps. 

    *    .. code-block:: python

            @flow.requires({"foo": func1, "bar": func2})
            @flow.requires(func1)

Example - 

.. code-block:: python
    
    ...
    @flow.step(d6tflow.tasks.TaskCache)
    @flow.requires({"a":get_data1, "b":get_data2})
    @flow.persists(['aa'])
    def example_function(task):
        df = task.inputLoad()
        a = df["a"]
        b = df["b"]
        print(a,b)
        output = pd.DataFrame({'a':range(4)})
        task.save({'aa':output})
    ...
    
Additional methods
------------------------------------------------------------

Some of the functions that are in d6tflow are available in the `Flow()` object too!

Here's a list of them -

* preview(function)
* outputLoad(function)
* run(functions_as_list)



