Advanced: Parameters
==============================================

Multiple parameters
------------------------------------------------------------

Tasks can take any number of parameters.

.. code-block:: python

    import datetime

    class TaskTrain(d6tflow.tasks.TaskPickle):
        do_preprocess = luigi.BoolParameter(default=True)
        model = luigi.Parameter(default='xgboost')


Parameter types
------------------------------------------------------------

Parameters can be typed.

.. code-block:: python

    import datetime

    class TaskTrain(d6tflow.tasks.TaskPickle):
        do_preprocess = luigi.BoolParameter(default=True)
        dt_start = luigi.DateParameter(default=datetime.date(2010,1,1))
        dt_end = luigi.DateParameter(default=datetime.date(2020,1,1))

        def run(self):
            if do_preprocess:
                if dt_start>datetime.date(2010,1,1):
                    pass

Overview https://luigi.readthedocs.io/en/stable/parameters.html#parameter-types

Full reference https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html

Avoid repeating parameters in every class
------------------------------------------------------------

You often need to pass parameters between classes. You can avoid having to repeat parameters in every class.

See https://luigi.readthedocs.io/en/stable/api/luigi.util.html

The project template also implements task parameter inheritance https://github.com/d6t/d6tflow-template
