Sharing Workflows and Outputs
==============================================

Introduction
------------------------------------------------------------

With d6tflow you can Export and Import Tasks from other projects and files.
This makes sharing flow output and handing projects off to others very seamless. Some cases when you want to do this include:

* data engineers share data with data scientists
* vendors sharing data with clients
* teachers sharing data with students

Exporting Tasks
------------------------------------------------------------

You can Export your tasks into a new File or print the tasks in the console.
All parameters, paths, task_group will be exported.

.. code-block:: python

    class Task1():
        def run(self):
            #Save

    @d6tflow.requires(Task1)
    class Task2():
        def run(self):
            #Save

    flow = d6tflow.Workflow(Task2())

    # This will only export Task 2 to console
    e = d6tflow.FlowExport(tasks=Task2())
    e.generate() 

    # This will export All the flow (Task1, Task2) to a file
    e = d6tflow.FlowExport(flows=flow, save=True, path_export='tasks_export.py')
    e.generate()

Attaching Flows
------------------------------------------------------------

In more complex projects, users need to import data from many sources.
Flows can be attached together in order to access the data generated in one flow inside the other.

.. code-block:: python

    class Task1():
        def run(self):
            #Save

    @d6tflow.requires(Task1)
    class Task2():
        def run(self):
            #Save

    class Task3():
        def run():
            temp_flow_df = self.flows['flow'].outputLoad()
            self.save(temp_flow_df)

    # Define Both flows and run the first
    flow = d6tflow.workflow(Task1)
    flow2 = d6tflow.workflow(Task3)
    flow.run()

    # Attach the First Flow to the Second
    flow2.attach(flow, 'flow')
    flow2.run()
