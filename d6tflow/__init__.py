import warnings
import luigi
from luigi.task import flatten
import luigi.tools.deps
from luigi.util import inherits, requires
from luigi.parameter import (
    Parameter,
    DateParameter, MonthParameter, YearParameter, DateHourParameter, DateMinuteParameter, DateSecondParameter,
    DateIntervalParameter, TimeDeltaParameter,
    IntParameter, FloatParameter, BoolParameter,
    TaskParameter, EnumParameter, DictParameter, ListParameter, TupleParameter,
    NumericalParameter, ChoiceParameter, OptionalParameter
)

import d6tcollect
d6tcollect.submit = False

import d6tflow.targets, d6tflow.tasks, d6tflow.settings
import d6tflow.utils
from d6tflow.cache import data as data
import d6tflow.cache

from d6tflow.settings import dir, dirpath

print('Welcome to d6tflow!')# We hope you find it useful. If you run into any problems please raise an issue on github at https://github.com/d6t/d6tflow')

def set_dir(dir=None):
    """
    Initialize d6tflow

    Args:
        dir (str): data output directory

    """
    if d6tflow.settings.isinitpipe:
        raise RuntimeError('Using d6tpipe, set dir via d6tflow.pipes.set_default()')
    if dir is None:
        dirpath = d6tflow.settings.dirpath
        dirpath.mkdir(exist_ok=True)
    else:
        from pathlib import Path
        dirpath = Path(dir)
        d6tflow.settings.dir = dir
        d6tflow.settings.dirpath = dirpath

    d6tflow.settings.isinit = True
    return dirpath


def preview(tasks, indent='', last=True, show_params=True, clip_params=False):
    """
    Preview task flows

    Args:
        tasks (obj, list): task or list of tasks
    """
    print('\n ===== Luigi Execution Preview ===== \n')
    if not isinstance(tasks, (list,)):
        tasks = [tasks]
    for t in tasks:
        print(d6tflow.utils.print_tree(t, indent=indent, last=last, show_params= show_params, clip_params=clip_params))
    print('\n ===== Luigi Execution Preview ===== \n')

def run(tasks, forced=None, forced_all=False, forced_all_upstream=False, confirm=True, workers=1, abort=True, execution_summary=None, **kwargs):
    """
    Run tasks locally. See luigi.build for additional details

    Args:
        tasks (obj, list): task or list of tasks
        forced (list): list of forced tasks
        forced_all (bool): force all tasks
        forced_all_upstream (bool): force all tasks including upstream
        confirm (list): confirm invalidating tasks
        workers (int): number of workers
        abort (bool): on errors raise exception
        execution_summary (bool): print execution summary
        kwargs: keywords to pass to luigi.build

    """
    if not isinstance(tasks, (list,)):
        tasks = [tasks]

    if forced_all:
        forced = tasks
    if forced_all_upstream:
        for t in tasks:
            invalidate_upstream(t,confirm=confirm)
    if forced is not None:
        if not isinstance(forced, (list,)):
            forced = [forced]
        invalidate = []
        for tf in forced:
            for tup in tasks:
                invalidate.append(d6tflow.taskflow_downstream(tf,tup))
        invalidate = set().union(*invalidate)
        invalidate = {t for t in invalidate if t.complete()}
        if len(invalidate)>0:
            if confirm:
                print('Forced tasks', invalidate)
                c = input('Confirm invalidating forced tasks (y/n)')
            else:
                c = 'y'
            if c == 'y':
                [t.invalidate(confirm=False) for t in invalidate]
            else:
                return None

    execution_summary = execution_summary if execution_summary is not None else d6tflow.settings.execution_summary
    opts = {**{'workers':workers, 'local_scheduler':True, 'log_level':d6tflow.settings.log_level},**kwargs}
    if execution_summary:
        opts['detailed_summary']=True
    result = luigi.build(tasks, **opts)
    if abort and not result.scheduling_succeeded:
        raise RuntimeError('Exception found running flow, check trace. For more details see https://d6tflow.readthedocs.io/en/latest/run.html#debugging-failures')

    if execution_summary:
        print(result.summary_text)
    return result

def taskflow_upstream(task, only_complete=False):
    """
    Get all upstream inputs for a task

    Args:
        task (obj): task

    """

    tasks = d6tflow.utils.traverse(task)
    if only_complete:
        tasks = [t for t in tasks if t.complete()]
    return tasks

def taskflow_downstream(task, task_downstream, only_complete=False):
    """
    Get all downstream outputs for a task

    Args:
        task (obj): task
        task_downstream (obj): downstream target task

    """
    tasks = luigi.tools.deps.find_deps(task_downstream, task.task_family)
    if only_complete:
        tasks = {t for t in tasks if t.complete()}
    return tasks

def invalidate_all(confirm=True):
    """
    Invalidate all tasks by deleting all files in data directory

    Args:
        confirm (bool): confirm operation

    """
    # record all tasks that run and their output vs files present
    raise NotImplementedError()

def invalidate_orphans(confirm=True):
    """
    Invalidate all unused task outputs

    Args:
        confirm (bool): confirm operation

    """
    # record all tasks that run and their output vs files present
    raise NotImplementedError()

def show(task):
    """
    Show task execution status

    Args:
        tasks (obj, list): task or list of tasks
    """
    preview(task)

def invalidate_upstream(task, confirm=True):
    """
    Invalidate all tasks upstream tasks in a flow.

    For example, you have 3 dependant tasks. Normally you run Task3 but you've changed parameters for Task1. By invalidating Task3 it will check the full DAG and realize Task1 needs to be invalidated and therefore Task2 and Task3 also.

    Args:
        task (obj): task to invalidate. This should be an upstream task for which you want to check upstream dependencies for invalidation conditions
        confirm (bool): confirm operation

    """
    tasks = taskflow_upstream(task, only_complete=True)
    if len(tasks)==0:
        print('no tasks to invalidate')
        return True
    if confirm:
        print('Compeleted tasks to invalidate:')
        print(tasks)
        c = input('Confirm invalidating tasks (y/n)')
    else:
        c = 'y'
    if c=='y':
        [t.invalidate(confirm=False) for t in tasks]

def invalidate_downstream(task, task_downstream, confirm=True):
    """
    Invalidate all downstream tasks in a flow.

    For example, you have 3 dependant tasks. Normally you run Task3 but you've changed parameters for Task1. By invalidating Task3 it will check the full DAG and realize Task1 needs to be invalidated and therefore Task2 and Task3 also.

    Args:
        task (obj): task to invalidate. This should be an downstream task for which you want to check downstream dependencies for invalidation conditions
        task_downstream (obj): downstream task target
        confirm (bool): confirm operation

    """
    tasks = taskflow_downstream(task, task_downstream, only_complete=True)
    if len(tasks)==0:
        print('no tasks to invalidate')
        return True
    if confirm:
        print('Compeleted tasks to invalidate:')
        print(tasks)
        c = input('Confirm invalidating tasks (y/n)')
    else:
        c = 'y'
    if c=='y':
        [t.invalidate(confirm=False) for t in tasks]
        return True
    else:
        return False

def clone_parent(cls):
    warnings.warn("This is replaced with `@d6tflow.requires()`", DeprecationWarning, stacklevel=2)
    def requires(self):
        return self.clone_parent()

    setattr(cls, 'requires', requires)
    return cls

class inherits:
    def __init__(self, *tasks_to_inherit):
        super(inherits, self).__init__()
        if not tasks_to_inherit:
            raise TypeError("tasks_to_inherit cannot be empty")

        #if the tuple contains a dict, we should parse it as one.
        if isinstance(tasks_to_inherit[0], dict):
            self.tasks_to_inherit = tasks_to_inherit[0]
        else:
            self.tasks_to_inherit = tasks_to_inherit
        

    def __call__(self, task_that_inherits):
        # Get all parameter objects from each of the underlying tasks

        #if task_to_inherit is a dict than process it differently
        if isinstance(self.tasks_to_inherit, dict):
            for task_to_inherit in self.tasks_to_inherit:                   
                for param_name, param_obj in self.tasks_to_inherit[task_to_inherit].get_params():
                    # Check if the parameter exists in the inheriting task                    
                    if not hasattr(task_that_inherits, param_name):
                        # If not, add it to the inheriting task
                        setattr(task_that_inherits, param_name, param_obj)
        else:
            for task_to_inherit in self.tasks_to_inherit:
                for param_name, param_obj in task_to_inherit.get_params():
                    # Check if the parameter exists in the inheriting task    
                    if not hasattr(task_that_inherits, param_name):
                        # If not, add it to the inheriting task
                        setattr(task_that_inherits, param_name, param_obj)

        # Modify task_that_inherits by adding methods
        def clone_parent(_self, **kwargs):
            return _self.clone(cls=self.tasks_to_inherit[0], **kwargs)
        task_that_inherits.clone_parent = clone_parent

        def clone_parents(_self, **kwargs):
            return [
                _self.clone(cls=task_to_inherit, **kwargs)
                for task_to_inherit in self.tasks_to_inherit
            ]
        task_that_inherits.clone_parents = clone_parents

        #adding dictionary functionality
        def clone_parents_dict(_self, **kwargs):
            return {
                task_to_inherit: _self.clone(cls=self.tasks_to_inherit[task_to_inherit], **kwargs)
                for task_to_inherit in self.tasks_to_inherit
            }
        task_that_inherits.clone_parents_dict = clone_parents_dict
        return task_that_inherits


#Like luigi.utils.requires but with extra functionality
class requires:
    def __init__(self, *tasks_to_require):
        super(requires, self).__init__()
        if not tasks_to_require:
            raise TypeError("tasks_to_require cannot be empty")
        
        #if the tuple contains a dict, we should parse it as one.
        if isinstance(tasks_to_require[0], dict):
            self.tasks_to_require = tasks_to_require[0]
        else:
            self.tasks_to_require = tasks_to_require

    def __call__(self, task_that_requires):
        
        #only unpack if self.tasks_to_require is a tuple
        if isinstance(self.tasks_to_require, dict):
            task_that_requires = inherits(self.tasks_to_require)(task_that_requires)
        else:
            task_that_requires = inherits(*self.tasks_to_require)(task_that_requires)

        # Modify task_that_requires by adding requires method.
        # If only one task is required, this single task is returned.
        # If a dictionary of tasks is required than a dictionary of tasks is returned
        # Otherwise, list of tasks is returned
        def requires(_self):
            if isinstance(self.tasks_to_require, dict):
                return _self.clone_parents_dict()
            return _self.clone_parent() if len(self.tasks_to_require) == 1 else _self.clone_parents()
        task_that_requires.requires = requires

        return task_that_requires