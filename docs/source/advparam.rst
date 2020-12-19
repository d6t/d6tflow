Advanced: Parameters
==============================================

Intelligent parameter management is one of the most powerful features of d6tflow. New users often have questions on parameter management, this is an important section to read.

Specifying parameters
------------------------------------------------------------

Tasks can take any number of parameters.

.. code-block:: python

    import datetime

    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = d6tflow.BoolParameter(default=True)
        model = d6tflow.Parameter(default='xgboost')


Running tasks with parameters
------------------------------------------------------------

Just pass the parameters values, everything else is the same.

.. code-block:: python

    d6tflow.run(TaskTrain() # use default do_preprocess=True, model='xgboost'
    d6tflow.run(TaskTrain(do_preprocess=False, model='nnet')) # specify non-default parameters
    # or
    params = dict(do_preprocess=False, model='nnet')
    d6tflow.run(TaskTrain(**params)) # specify non-default parameters

Note that you can pass parameters for upstream tasks directly to the terminal task, they will be automatically passed to upstream tasks. See below for details.

Loading Output Data with Parameters
------------------------------------------------------------

If you are :doc:`using parameters <../advparam>` this is how you load outputs. Make sure you run the task with that parameter first.

.. code-block:: python

    df = TaskTrain().output().load() # load data with default parameters
    params = dict(do_preprocess=False, model='nnet')
    df = TaskTrain(**params).output().load() # specify non-default parameters


Parameter types
------------------------------------------------------------

Parameters can be typed.

.. code-block:: python

    import datetime

    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = d6tflow.BoolParameter(default=True)
        dt_start = d6tflow.DateParameter(default=datetime.date(2010,1,1))
        dt_end = d6tflow.DateParameter(default=datetime.date(2020,1,1))

        def run(self):
            if self.do_preprocess:
                if self.dt_start>datetime.date(2010,1,1):
                    pass

Overview https://d6tflow.readthedocs.io/en/stable/parameters.html#parameter-types

Full reference https://d6tflow.readthedocs.io/en/stable/api/d6tflow.parameter.html

Avoid repeating parameters in every class
------------------------------------------------------------

You often need to pass parameters between classes. With d6tflow, you do not need to repeat parameters in every class, they are automatically managed, that is they are automatically passed to upstream tasks from downstream tasks.

.. code-block:: python


    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = d6tflow.BoolParameter(default=True)
        dt_start = d6tflow.DateParameter(default=datetime.date(2010,1,1))
        dt_end = d6tflow.DateParameter(default=datetime.date(2020,1,1))
        # ...

    @d6tflow.requires(TaskTrain) # automatically inherits parameters
    class TaskEvaluate(d6tflow.tasks.TaskPickle):

        # requires() is automatic
        # do_preprocess => inherited from TaskTrain
        # dt_start => inherited from TaskTrain
        # dt_end => inherited from TaskTrain

        def run(self):
            print(self.do_preprocess) # inherited
            print(self.dt_start) # inherited

    d6tflow.preview(TaskEvaluate(do_preprocess=False))  # specify non-default parameters
    '''
    └─--[TaskEvaluate-{'do_preprocess': 'False', 'dt_start': '2010-01-01', 'dt_end': '2020-01-01'} (PENDING)]
    └─--[TaskTrain-{'do_preprocess': 'False', 'dt_start': '2010-01-01', 'dt_end': '2020-01-01'} (PENDING)] => automatically passed upstream
    '''

Note that you can pass parameters for upstream tasks directly to the terminal task, they will be automatically passed to upstream tasks. `do_preprocess=False` will be passed down from `TaskEvaluate` to `TaskTrain`.

If you require multiple tasks, you can inherit parameters from those tasks. `TaskEvaluate` depeonds on both `TaskTrain` and `TaskPredict`.

.. code-block:: python

    class TaskTrain(d6tflow.tasks.TaskPqPandas):
        do_preprocess = d6tflow.BoolParameter(default=True)

    class TaskPredict(d6tflow.tasks.TaskPqPandas):
        dt_start = d6tflow.DateParameter(default=datetime.date(2010,1,1))
        dt_end = d6tflow.DateParameter(default=datetime.date(2020,1,1))

    @d6tflow.requires(TaskTrain,TaskPredict) # inherit all params from input tasks
    class TaskEvaluate(d6tflow.tasks.TaskPickle):
        # do_preprocess => inherited from TaskTrain
        # dt_start => inherited from TaskPredict
        # dt_end => inherited from TaskPredict

        def run(self):
            print(self.do_preprocess) # inherited from TaskTrain
            print(self.dt_start) # inherited from TaskPredict

    d6tflow.preview(TaskEvaluate(do_preprocess=False))  # specify non-default parameters
    '''
    └─--[TaskEvaluate-{'do_preprocess': 'False', 'dt_start': '2010-01-01', 'dt_end': '2020-01-01'} (PENDING)]
       |--[TaskTrain-{'do_preprocess': 'False'} (PENDING)] => automatically passed upstream
       └─--[TaskPredict-{'dt_start': '2010-01-01', 'dt_end': '2020-01-01'} (PENDING)] => automatically passed upstream
    '''

`@d6tflow.requires` also works with aggregator tasks.

.. code-block:: python

    @d6tflow.requires(TaskTrain,TaskPredict) # inherit all params from input tasks
    class TaskEvaluate(d6tflow.tasks.TaskAggregator):

        def run(self):
            yield self.clone(TaskTrain)
            yield self.clone(TaskPredict)

For another ML example see https://github.com/d6t/d6tflow/blob/master/docs/example-ml.md

For more details see https://d6tflow.readthedocs.io/en/stable/api/d6tflow.util.html

The project template also implements task parameter inheritance https://github.com/d6t/d6tflow-template

Avoid repeating parameters when referring to tasks
------------------------------------------------------------

To run tasks and load their output for different parameters, you have to pass them to the task. Instead of hardcoding them each time, it is best to keep them in a dictionary and pass that to the task.

.. code-block:: python

    # avoid this
    d6tflow.run(TaskTrain(do_preprocess=False, model='nnet'))
    TaskTrain(do_preprocess=False, model='nnet').outputLoad()

    # better
    params = dict(do_preprocess=False, model='nnet')
    d6tflow.run(TaskTrain(**params))
    TaskTrain(**params).outputLoad()

