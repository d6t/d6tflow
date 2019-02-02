import luigi
import luigi.tools.deps

import d6tcollect

import d6tflow.targets
import d6tflow.settings as settings
from d6tflow.cache import data as cache
import d6tflow.cache

def _taskpipeoperation(task, fun, funargs=None):
    pipe = task.get_pipe()
    fun = getattr(pipe, fun)
    funargs = {} if funargs is None else funargs
    return fun(**funargs)

class TaskData(luigi.Task):
    """
    Task which has data as input and output

    Args:
        target_class (obj): target data format
        target_ext (str): file extension
        persist (list): list of string to identify data
        data (dict): data container for all outputs

    """
    target_class = d6tflow.targets.DataTarget
    target_ext = 'ext'
    persist = ['data']

    def invalidate(self):
        """
        Invalidate a task, eg by deleting output file
        """
        if self.complete():
            if self.persist == ['data']:  # 1 data shortcut
                self.output().invalidate()
            else:
                [t.invalidate() for t in self.output().values()]
        return True

    @d6tcollect._collectClass
    def complete(self):
        """
        Invalidate a task, eg by deleting output file
        """
        complete = super().complete()
        if d6tflow.settings.check_crc:
            # todo: add CRC complete check
            pass
        if d6tflow.settings.check_dependencies and not getattr(self, 'external', False):
            complete = complete and all([t.complete() for t in luigi.task.flatten(self.requires())])
        return complete

    def output(self):
        """
        Similar to luigi task output
        """

        # output dir, check if using d6tpipe
        if hasattr(self, 'pipename'):
            import d6tflow.pipes
            dirpath = d6tflow.pipes.get_dirpath(self.pipename)
        else:
            dirpath = settings.dirpath

        def getpath(k):
            tidroot = self.task_id.split('_')[0]
            fname = '{}-{}'.format(self.task_id, k) if settings.save_with_param else '{}'.format(k)
            fname += '.{}'.format(self.target_ext)
            path = dirpath / tidroot / fname
            return path

        save_ = getattr(self, 'persist', [])
        output = dict([(k, self.target_class(getpath(k))) for k in save_])
        if self.persist==['data']: # 1 data shortcut
            output = output['data']
        return output

    def loadall(self, keys=None, cached=False):
        """
        Load data from task to memory

        Args:
            keys (list): list of data to load
            cached (bool): cache data in memory
            check_complete (bool): check if task is complete before loading
        """
        if not self.complete():
            return RuntimeError('Cannot load, task not complete, run task first')
        keys = self.persist if keys is None else keys
        if self.persist==['data']: # 1 data shortcut
            data = {'data':self.output().load()}
        else:
            data = {k: v.load(cached) for k, v in self.output().items() if k in keys}
        return data

    def save(self, data):
        """
        Persist data to target

        Args:
            data (dict): data to save. keys are the self.persist keys and values is data

        """
        if self.persist==['data']: # 1 data shortcut
            self.output().save(data)
        else:
            targets = self.output()
            if not set(data.keys())==set(targets.keys()):
                raise ValueError('Save dictionary needs to consistent with Task.persist')
            for k, v in data.items():
                targets[k].save(v)

    @d6tcollect._collectClass
    def get_pipename(self):
        """
        Get associated pipe name
        """
        return getattr(self, 'pipename', d6tflow.cache.pipe_default_name)
    def get_pipe(self):
        """
        Get associated pipe object
        """
        import d6tflow.pipes
        return d6tflow.pipes.get_pipe(self.get_pipename())
    def pull(self, **kwargs):
        """
        Pull files from data repo
        """
        return _taskpipeoperation(self,'pull', **kwargs)
    def pull_preview(self, **kwargs):
        """
        Preview pull files from data repo
        """
        return _taskpipeoperation(self,'pull_preview', **kwargs)
    def push(self, **kwargs):
        """
        Push files to data repo
        """
        return _taskpipeoperation(self,'push', **kwargs)
    def push_preview(self, **kwargs):
        """
        Preview push files to data repo
        """
        return _taskpipeoperation(self,'push_preview', **kwargs)


class TaskCache(TaskData):
    """
    Task which saves to cache
    """
    target_class = d6tflow.targets.CacheTarget
    target_ext = 'cache'

class TaskCachePandas(TaskData):
    """
    Task which saves to cache pandas dataframes
    """
    target_class = d6tflow.targets.PdCacheTarget
    target_ext = 'cache'

class TaskJson(TaskData):
    """
    Task which saves to json
    """
    target_class = d6tflow.targets.JsonTarget
    target_ext = 'json'

class TaskPickle(TaskData):
    """
    Task which saves to pickle
    """
    target_class = d6tflow.targets.PickleTarget
    target_ext = 'pkl'

class TaskCSVPandas(TaskData):
    """
    Task which saves to CSV
    """
    target_class = d6tflow.targets.CSVPandasTarget
    target_ext = 'csv'

class TaskPqPandas(TaskData):
    """
    Task which saves to parquet
    """
    target_class = d6tflow.targets.PqPandasTarget
    target_ext = 'pq'

