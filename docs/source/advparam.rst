Advanced: Parameters
==============================================

Specifying parameters
------------------------------------------------------------

Tasks can take any number of parameters.

.. code-block:: python

    import datetime

    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = luigi.BoolParameter(default=True)
        model = luigi.Parameter(default='xgboost')


Running tasks with parameters
------------------------------------------------------------

Just pass the parameters values, everything else is the same.

.. code-block:: python

    d6tflow.run(TaskTrain(do_preprocess=True, model='nnet'))
    d6tflow.run(TaskTrain(do_preprocess=True)) # use default model='xgboost'


Loading Output Data with Parameters
------------------------------------------------------------

If you are :doc:`using parameters <../advparam>` this is how you load outputs. Make sure you run the task with that parameter first.

.. code-block:: python

    df = TaskSingleOutput(param=value).output().load()


Parameter types
------------------------------------------------------------

Parameters can be typed.

.. code-block:: python

    import datetime

    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = luigi.BoolParameter(default=True)
        dt_start = luigi.DateParameter(default=datetime.date(2010,1,1))
        dt_end = luigi.DateParameter(default=datetime.date(2020,1,1))

        def run(self):
            if self.do_preprocess:
                if self.dt_start>datetime.date(2010,1,1):
                    pass

Overview https://luigi.readthedocs.io/en/stable/parameters.html#parameter-types

Full reference https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html

Avoid repeating parameters in every class
------------------------------------------------------------

You often need to pass parameters between classes. You can avoid having to repeat parameters in every class.

.. code-block:: python


    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = luigi.BoolParameter(default=True)
        dt_start = luigi.DateParameter(default=datetime.date(2010,1,1))
        dt_end = luigi.DateParameter(default=datetime.date(2020,1,1))
        # ...

    @d6tflow.inherits(TaskTrain) # inherit all params from TaskTrain
    @d6tflow.clone_parent
    class TaskEvaluate(d6tflow.tasks.TaskPickle):

        # requires() is automatic

        def run(self):
            print(self.do_preprocess) # inherited
            print(self.dt_start) # inherited


For more details see https://luigi.readthedocs.io/en/stable/api/luigi.util.html

The project template also implements task parameter inheritance https://github.com/d6t/d6tflow-template
