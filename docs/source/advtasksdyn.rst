Advanced: Dynamic Tasks
==============================================

Sometimes you might not know exactly what other tasks to depend on until runtime. There are several cases of dynamic dependencies.

Fixed Dynamic
------------------------------------------------------------

If you have a fixed set parameters, you can make `requires()` "dynamic". 

.. code-block:: python

    class TaskInput(d6tflow.tasks.TaskPqPandas):
        param = luigi.Parameter()
        ...

    class TaskYieldFixed(d6tflow.tasks.TaskPqPandas):

        def requires(self):
            return dict([(s,TaskInput(param=s)) for s in ['a','b','c']])

        def run(self):
            df = [k.load() for k in self.input().values()]
            df = pd.concat(df)

You could also use this to load an unknown number of files as a starting point for the workflow.

.. code-block:: python

        def requires(self):
            return dict([(s,TaskInput(param=s)) for s in glob.glob('*.csv')])

Collector Task
------------------------------------------------------------

If you want to run the workflow with multiple parameters at the same time, you can use `luigi.Task` to yield multiple tasks. The fixed dynamic approach is probably better though.

.. code-block:: python

    class TaskCollector(luigi.Task): # note luigi.Task

        def run(self):
            yield TaskTrain(do_preprocess=False)
            yield TaskTrain(do_preprocess=True)

See https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies

Fully Dynamic
------------------------------------------------------------

This doesn't work yet... It's actually quite rare though that you need that though. Parameters normally fall in a fixed range which can be solved with the approaches above. Another typical reason you would want to do this is to load an unknown number of input files which you can do manually, see "Load External Files" in :doc:`tasks <../tasks>`.

.. code-block:: python

    class TaskA(d6tflow.tasks.TaskCache):
        param = luigi.IntParameter()
        def run(self):
            self.save(self.param)

    class TaskB(d6tflow.tasks.TaskCache):
        param = luigi.IntParameter()

        def requires(self):
            return TaskA()

        def run(self):
            value = 1
            df_train = self.input(param=value).load()
