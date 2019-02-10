import luigi
from luigi.task import flatten
import luigi.tools.deps
import luigi.tools.deps_tree

import d6tflow.targets
import d6tflow.tasks
from d6tflow.cache import data as data
import d6tflow.cache

import d6tflow.settings
from d6tflow.settings import dir, dirpath

print('Welcome to d6tflow! We hope you find it useful. If you run into any problems please raise an issue on github at https://github.com/d6t/d6tflow')

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


import luigi
import luigi.tools.deps_tree
import luigi.tools.deps

def preview(tasks):
    """
    Preview task flows

    Args:
        tasks (obj, list): task or list of tasks
    """
    if not isinstance(tasks, (list,)):
        tasks = [tasks]
    for t in tasks:
        print(luigi.tools.deps_tree.print_tree(t))

def run(tasks, forced=None, confirm=True, workers=1, abort=True, **kwargs):
    """
    Run tasks locally. See luigi.build for additional details

    Args:
        tasks (obj, list): task or list of tasks
        forced (list): list of forced tasks
        confirm (list): confirm invalidating tasks
        workers (int): number of workers
        abort (bool): on errors raise exception
        kwargs: keywords to pass to luigi.build

    """
    if not isinstance(tasks, (list,)):
        tasks = [tasks]

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
                [t.invalidate() for t in invalidate]
            else:
                return None

    opts = {**{'workers':workers, 'local_scheduler':True, 'log_level':d6tflow.settings.log_level},**kwargs}
    result = luigi.build(tasks, **opts)
    if abort and not result:
        raise RuntimeError('Exception found running flow, check trace')

    return result

def taskflow_upstream(task, only_complete=False):
    """
    Get all upstream inputs for a task

    Args:
        task (obj): task

    """

    def traverse(t, path=None):
        if path is None: path = []
        path = path + [t]
        for node in flatten(t.requires()):
            if not node in path:
                path = traverse(node, path)
        return path

    tasks = traverse(task)
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
        [t.invalidate() for t in tasks]

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
        [t.invalidate() for t in tasks]
        return True
    else:
        return False
