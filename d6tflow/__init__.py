import warnings
import luigi
from luigi.task import flatten
import luigi.tools.deps
from luigi.util import inherits as luigi_inherits, requires as luigi_requires
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

    #if forced_all_upstream is true we are going to force run tasks anyway
    # in the second if condition.
    # So in this case we are going to skip running forced tasks.
    if forced_all and not forced_all_upstream:
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
    if execution_summary and luigi.__version__>='3.0.0':
        opts['detailed_summary']=True
    result = luigi.build(tasks, **opts)
    if isinstance(result, bool):
        success = result
    else:
        success = result.scheduling_succeeded
    if abort and not success:
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

#Like luigi.utils.inherits but for handling dictionaries
class dict_inherits:
    def __init__(self, *tasks_to_inherit):
        super(dict_inherits, self).__init__()
        if not tasks_to_inherit:
            raise TypeError("tasks_to_inherit cannot be empty")
        #We know the first arg is a dict.
        self.tasks_to_inherit = tasks_to_inherit[0]

    def __call__(self, task_that_inherits):
        for task_to_inherit in self.tasks_to_inherit:
            for param_name, param_obj in self.tasks_to_inherit[task_to_inherit].get_params():
                # Check if the parameter exists in the inheriting task
                if not hasattr(task_that_inherits, param_name):
                    # If not, add it to the inheriting task
                    setattr(task_that_inherits, param_name, param_obj)

        #adding dictionary functionality
        def clone_parents_dict(_self, **kwargs):
            return {
                task_to_inherit: _self.clone(cls=self.tasks_to_inherit[task_to_inherit], **kwargs)
                for task_to_inherit in self.tasks_to_inherit
            }
        task_that_inherits.clone_parents_dict = clone_parents_dict
        return task_that_inherits


#Like luigi.utils.requires but for handling dictionaries
class dict_requires:
    def __init__(self, *tasks_to_require):
        super(dict_requires, self).__init__()
        if not tasks_to_require:
            raise TypeError("tasks_to_require cannot be empty")

        self.tasks_to_require = tasks_to_require[0] #Assign the dictionary

    def __call__(self, task_that_requires):
        task_that_requires = dict_inherits(self.tasks_to_require)(task_that_requires)
        def requires(_self):
            return _self.clone_parents_dict()

        task_that_requires.requires = requires

        return task_that_requires

def inherits(*tasks_to_inherit):
    if isinstance(tasks_to_inherit[0], dict):
        return dict_inherits(*tasks_to_inherit)
    return luigi_inherits(*tasks_to_inherit)

def requires(*tasks_to_require):
    #Check the type; if a dictionary call our custom requires decorator
    is_dict = isinstance(tasks_to_require[0], dict)
    if len(tasks_to_require) > 1 and not is_dict:
        if isinstance(tasks_to_require, tuple):
            tasks_to_require = (dict(zip(range(len(tasks_to_require)),tasks_to_require)),)
            return dict_requires(*tasks_to_require)
        else:
            raise TypeError("Please use a dictionary if you are passing multiple tasks to d6tflow.requires() \n \
        Example: @d6tflow.requires({'task1': Task1, 'task2': Task2})")
    if is_dict:
        return dict_requires(*tasks_to_require)
    return luigi_requires(*tasks_to_require)


class Workflow(object):


    def __init__(self, params={}, task = None):
        self.params = params
        self.default_task = task


    def preview(self, tasks=None, indent='', last=True, show_params=True, clip_params=False):
        if not isinstance(tasks, (list,)):
            tasks = [tasks]
        tasks_inst = [self.get_task(x) for x in tasks]
        return preview(tasks = tasks_inst, indent = indent, last = last, show_params = show_params, clip_params = clip_params)


    def run(self,tasks=None, forced=None, forced_all=False, forced_all_upstream=False, confirm=True, workers=1, abort=True, execution_summary=None, **kwargs):
        if not isinstance(tasks, (list,)):
            tasks = [tasks]
        tasks_inst = [self.get_task(x) for x in tasks]
        return run(tasks_inst, forced=forced, forced_all=forced_all, forced_all_upstream=forced_all_upstream, confirm=confirm, workers=workers, abort=abort, execution_summary=execution_summary, **kwargs)


    def outputLoad(self, task_cls=None, keys=None, as_dict=False, cached=False):
        task_cls_inst = self.get_task(task_cls)

        return task_cls_inst.outputLoad(keys=keys, as_dict=as_dict, cached=cached)


    def outputLoadAll(self, task_cls=None, keys=None, as_dict=False, cached=False):
        task_cls_inst = self.get_task(task_cls)
        data_dict = {}
        tasks = taskflow_upstream(task_cls_inst)
        for task in tasks:
            data_dict[type(task).__name__] = task.outputLoad(keys=keys, as_dict=as_dict, cached=cached)
        return data_dict


    def reset(self, task_cls, confirm=True):
        task_cls_inst = self.get_task(task_cls)
        return task_cls_inst.reset(confirm)


    def reset_downstream(self, task_cls, task_downstream_cls, confirm=True):
        task_inst = self.get_task(task_cls)
        task_downstream_inst = self.get_task(task_downstream_cls)
        return taskflow_downstream(task_inst, task_downstream_inst, confirm)


    def set_default(self, task):
        self.default_task = task


    def get_task(self, task_cls = None):
        if task_cls is None:
            if self.default_task is None:
                raise RuntimeError('no default tasks set')
            else:
                task_cls = self.default_task
        return task_cls(**self.params)


class WorkflowMulti(object):

    def __init__(self, exp_params, task = None):
        self.exp_params = exp_params
        if exp_params is None or len(exp_params.keys())==0:
            raise Exception("Experments not defined")
        self.default_task = task
        if exp_params is not None:
            self.workflow_objs = {k: Workflow(task=task, params=v) for k, v in self.exp_params.items()}


    def run(self,tasks=None, forced=None, forced_all=False, forced_all_upstream=False, confirm=True, workers=1, abort=True, execution_summary=None, flow = None, **kwargs):
        if flow is not None:
            return self.workflow_objs[flow].run(tasks=tasks, forced=forced, forced_all=forced_all,
                                           forced_all_upstream=forced_all_upstream, confirm=confirm, workers=workers,
                                           abort=abort,
                                           execution_summary=execution_summary, **kwargs)
        result = {}
        for exp_name in self.exp_params.keys():
            result[exp_name] = self.workflow_objs[exp_name].run(tasks, forced, forced_all, forced_all_upstream,
                                                                  confirm, workers, abort,
                                                                  execution_summary, **kwargs)
        return result


    def outputLoad(self, task_cls=None, keys=None, as_dict=False, cached=False, flow = None):
        if flow is not None:
            return self.workflow_objs[flow].outputLoad(task_cls, keys, as_dict, cached)
        data = {}
        for exp_name in self.exp_params.keys():
            data[exp_name] = self.workflow_objs[exp_name].outputLoad(task_cls, keys, as_dict, cached)
        return data


    def outputLoadAll(self, task_cls=None, keys=None, as_dict=False, cached=False, flow = None):
        if flow is not None:
            return self.workflow_objs[flow].outputLoadAll(task_cls, keys, as_dict, cached)
        data = {}
        for exp_name in self.exp_params.keys():
            data[exp_name] = self.workflow_objs[exp_name].outputLoadAll(task_cls, keys, as_dict, cached)
        return data


    def reset(self, task_cls, confirm=True, flow = None):
        if flow is not None:
            return self.workflow_objs[flow].reset(task_cls, confirm)
        return {self.workflow_objs[exp_name].reset(task_cls, confirm) for exp_name in self.exp_params.keys()}



    def preview(self, tasks = None, indent='', last=True, show_params=True, clip_params=False, flow = None):
        if not isinstance(tasks, (list,)):
            tasks = [tasks]
        if flow is not None:
            return self.workflow_objs[flow].preview(tasks)
        data = {}
        for exp_name in self.exp_params.keys():
            data[exp_name] = self.workflow_objs[exp_name].preview(tasks = tasks, indent = indent, last = last, show_params=show_params, clip_params=clip_params)
        return data


    def set_default(self, task):
        self.default_task = task
        for exp_name in self.exp_params.keys():
            self.workflow_objs[exp_name].set_default(task)


    def get_task(self, task_cls = None, flow = None):
        if task_cls is None:
            if self.default_task is None:
                raise RuntimeError('no default tasks set')
            else:
                task_cls = self.default_task
        if flow is None:
            return {exp_name: task_cls(**self.exp_params[exp_name]) for exp_name in self.exp_params.keys()}
        return task_cls(**self.exp_params[flow])
