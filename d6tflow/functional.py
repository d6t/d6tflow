import d6tflow.tasks as tasks
import d6tflow
from functools import wraps


class Flow:
    """
        Functional Flow class that acts as a manager of all flow steps.
        Defines all the decorators that can be used on flow functions.
    """

    def __init__(self):
        self.funcs_to_run = []
        self.params = {}
        self.steps = {}
        self.instantiated_tasks = {}
        self.object_count = 0

        def common_decorator(func):
            """
                Common decorator that all decorators use.
            """
            # Checking if the function that is decorated is the function that we want to run.
            # If so then we set the function as the run function for the current task class.
            # Also we are changing the name of the task class to the function name.
            if not '__wrapped__' in func.__dict__:
                self.steps[func.__name__] = self.steps[self.current_step]
                del self.steps[self.current_step]
                self.steps[func.__name__].__name__ = func.__name__
                setattr(self.steps[func.__name__], 'run', func)

            # Thanks to wraps, wrapper has all the metadata of func.
            @wraps(func)
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            return wrapper
        self.common_decorator = common_decorator

    def step(self, task_type: d6tflow.tasks.TaskData):
        """
            Flow step decorator.
            Converts the decorated function into a flow step.
            Should be defined at the top of the decorator stack.

            Parameters
            ----------
            task_type : d6ftflow.tasks.TaskData
                task_type should be a class which inherits from d6ftflow.tasks.TaskData

            Example
            -------
            @flow.step(d6tflow.tasks.TaskCache)
        """
        assert task_type.__base__ == tasks.TaskData, "Invalid parameter. Parameter should be type defined in d6tflow.tasks"
        self.task_type = task_type
        step_name = str(self.object_count)
        self.steps[step_name] = type(step_name, (task_type,), {})
        self.current_step = step_name
        self.object_count += 1
        return self.common_decorator

    def requires(self, *args, **kwargs):
        """
            Flow requires decorator.
            Defines dependencies between flow steps.
            Internally calls d6tflow.requires which inturn calls luigi.requires.

            Parameters
            ----------
            func : dict or function or mutiple functions

            Examples
            --------
            @flow.requires({"foo": func1, "bar": func2})
            @flow.requires(func1)
        """
        if isinstance(args[0], dict):
            tasks_to_require = {}
            for key in args[0]:
                tasks_to_require[key] = self.steps[args[0][key].__name__]

            tasks_to_require = [tasks_to_require]
        else:
            tasks_to_require = [
                self.steps[func.__name__]
                for func in args
            ]

        self.steps[self.current_step] = d6tflow.requires(
            *tasks_to_require, **kwargs)(self.steps[self.current_step])

        return self.common_decorator

    def persists(self, to_persist: list):
        """
            Flow persists decorator.
            Takes in a list of variables that need to be persisted for the flow step.
            Parameters
            ----------
            to_persist : list

            Example
            -------
            @flow.persists(['a1', 'a2'])
        """
        self.steps[self.current_step].persist = to_persist
        return self.common_decorator

    def preview(self, func_to_preview, params=None):
        self._instantiate([func_to_preview], params=params)
        return d6tflow.preview(self.instantiated_tasks[func_to_preview.__name__])

    def run(self, funcs_to_run, params: dict = None, *args, **kwargs):
        """
            Runs flow steps locally. See luigi.build for additional details
            Parameters
            ----------
            funcs_to_run : function or list of functions

            params : dict
                dictionary of paramaters. Keys are param names and values are the values of params.

            Examples
            --------

            flow.run(func, params={'multiplier':2})

            flow.run([func1, func2], params={'multiplier':42})

            flow.run(func)
        """
        funcs_to_run = funcs_to_run if isinstance(
            funcs_to_run, list) else [funcs_to_run]

        self._instantiate(funcs_to_run, params=params)

        d6tflow.run(
            list(self.instantiated_tasks.values()),
            *args,
            **kwargs)

    def _instantiate(self, funcs_to_run: list, params=None):
        params = params if params else {}
        instantiated_tasks = {
            func_to_run.__name__: self.steps[func_to_run.__name__](**params)
            for func_to_run in funcs_to_run
        }
        self.instantiated_tasks.update(instantiated_tasks)

    def add_params(self, params):
        """
            Adds params to flow functions.
            More like declares the params for further use.
            Parameters
            ----------
            params : dict
                dictionary of param name and param type

            Example
            -------
            flow.add_params({'multiplier': d6tflow.IntParameter(default=0)})
        """
        for step in self.steps:
            for param in params:
                setattr(self.steps[step], param, params[param])

    def outputLoad(self, func_to_run, params: dict = None, *args, **kwargs):
        """
            Loads all or several outputs from flow step.

            Args:
                func_to_run: flow step function
                keys (list): list of data to load
                as_dict (bool): cache data in memory
                cached (bool): cache data in memory

            Returns: list or dict of all task output
        """
        name = func_to_run.__name__
        assert name in self.steps, "The function either does not qualify as a task or has not been run yet.\n Did you forget to decorate your function with one of the classes in d6tflow.tasks?"

        self._instantiate([func_to_run], params=params)

        return self.instantiated_tasks[name].outputLoad(*args, **kwargs)
