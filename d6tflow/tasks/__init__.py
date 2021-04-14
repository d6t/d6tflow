import pickle, pathlib

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
    metadata = None

    def __init__(self, *args, path=None, **kwargs):
        kwargs_ = {k: v for k, v in kwargs.items(
        ) if k in self.get_param_names(include_significant=True)}
        super().__init__(*args, **kwargs_)
        self.path = path

    @classmethod
    def get_param_values(cls, params, args, kwargs):
        kwargs_ = {k: v for k, v in kwargs.items(
        ) if k in cls.get_param_names(include_significant=True)}
        return super(TaskData, cls).get_param_values(params, args, kwargs_)

    def reset(self, confirm=True):
        return self.invalidate(confirm)

    def invalidate(self, confirm=True):
        """
        Invalidate a task, eg by deleting output file
        """
        if confirm:
            c = input(
                'Confirm invalidating task: {} (y/n)'.format(self.__class__.__qualname__))
        else:
            c = 'y'
        if c == 'y':# and self.complete():
            if self.persist == ['data']:  # 1 data shortcut
                self.output().invalidate()
            else:
                [t.invalidate() for t in self.output().values()]
        return True

    @d6tcollect._collectClass
    def complete(self, cascade=True):
        """
        Invalidate a task, eg by deleting output file
        """
        complete = super().complete()
        if d6tflow.settings.check_dependencies and cascade and not getattr(self, 'external', False):
            complete = complete and all(
                [t.complete() for t in luigi.task.flatten(self.requires())])
        return complete

    def _getpath(self, dirpath, k, subdir=True):
        tidroot = getattr(self, 'target_dir', self.task_id.split('_')[0])
        fname = '{}-{}'.format(self.task_id, k) if (settings.save_with_param and getattr(
            self, 'save_attrib', True)) else '{}'.format(k)
        fname += '.{}'.format(self.target_ext)
        if subdir:
            path = dirpath / tidroot / fname
        else:
            path = dirpath / fname
        return path

    def output(self):
        """
        Similar to luigi task output
        """
        # output dir, check if using d6tpipe
        if hasattr(self, 'pipename'):
            import d6tflow.pipes
            dirpath = d6tflow.pipes.get_dirpath(self.pipename)
        elif self.path is not None:
            dirpath = pathlib.Path(self.path)
        else:
            dirpath = settings.dirpath

        save_ = getattr(self, 'persist', [])
        output = dict([(k, self.target_class(self._getpath(dirpath, k)))
                       for k in save_])
        if self.persist == ['data']:  # 1 data shortcut
            output = output['data']
        return output

    def inputLoad(self, keys=None, task=None, cached=False, as_dict=False):
        """
        Load all or several outputs from task

        Args:
            keys (list): list of data to load
            task (str): if requires multiple tasks load that task 'input1' for eg `def requires: {'input1':Task1(), 'input2':Task2()}`
            cached (bool): cache data in memory
            as_dict (bool): if the inputs were saved as a dictionary. use this to return them as dictionary. 
        Returns: list or dict of all task output
        """

        if isinstance(self.requires(), dict) and task is not None:
            input = self.input()[task]
        else:
            input = self.input()

        requires = self.requires()
        type_of_requires = type(requires)

        if isinstance(input, dict):
            keys = input.keys() if keys is None else keys
            data = {}
            for k, v in input.items():
                if k in keys:
                    if type(v) == dict:
                        if as_dict:
                            data[k] = {k: v.load(cached) for k, v in v.items()}
                        else:
                            data[k] = [v.load(cached) for k, v in v.items()]
                    else:
                        data[k] = v.load(cached)
            # Convert to list if dependency is single and as_dict is False
            if (type_of_requires != dict or task is not None) and not as_dict:
                data = list(data.values())
        else:
            data = input.load()

        return data

    def outputLoad(self, keys=None, as_dict=False, cached=False):
        """
        Load all or several outputs from task

        Args:
            keys (list): list of data to load
            as_dict (bool): cache data in memory
            cached (bool): cache data in memory

        Returns: list or dict of all task output
        """
        if not self.complete():
            raise RuntimeError(
                'Cannot load, task not complete, run flow first')
        keys = self.persist if keys is None else keys
        if self.persist == ['data']:  # 1 data shortcut
            return self.output().load()

        data = {k: v.load(cached)
                for k, v in self.output().items() if k in keys}
        if not as_dict:
            data = list(data.values())
        return data

    def save(self, data, **kwargs):
        """
        Persist data to target

        Args:
            data (dict): data to save. keys are the self.persist keys and values is data

        """

        if self.persist == ['data']:  # 1 data shortcut
            self.output().save(data, **kwargs)
        else:
            targets = self.output()
            if not set(data.keys()) == set(targets.keys()):
                raise ValueError(
                    'Save dictionary needs to consistent with Task.persist')
            for k, v in data.items():
                targets[k].save(v, **kwargs)

    def metaSave(self, data):
        self.metadata = data
        path = self._get_meta_path(self)
        with open(path, "wb") as fh:
            pickle.dump(data, fh)

    def saveMeta(self, data):
        self.metaSave(data)

    def metaLoad(self):
        if isinstance(self.requires(), dict):
            output = {}
            inputs = self.requires()
            for _input in inputs:
                path = self._get_meta_path(inputs[_input])
                output[_input] = pickle.load(open(path, "rb"))
            return output
        else:
            _input = self.requires()
            path = self._get_meta_path(_input)
            return pickle.load(open(path, "rb"))

    def outputLoadMeta(self):
        if not self.complete(cascade=False):
            raise RuntimeError(
                'Cannot load, task not complete, run flow first')
        path = self._get_meta_path(self)
        try:
            data = pickle.load(open(path, "rb"))
        except FileNotFoundError:
            raise RuntimeError(
                f"No metadata to load for task {self.task_family}")
        return data

    def outputLoadAllMeta(self):
        if not self.complete(cascade=False):
            raise RuntimeError(
                'Cannot load, task not complete, run flow first')
        tasks = d6tflow.taskflow_upstream(self, only_complete=True)
        meta = []
        for task in tasks:
            try:
                meta.append(task.outputLoadMeta())
            except:
                tasks.remove(task)
        tasks = [task.task_family for task in tasks]
        return dict(zip(tasks, meta))

    def _get_meta_path(self, task):
        # meta_file = f"meta-{task_id}.pickle"
        meta_path = task._getpath(
            d6tflow.settings.dirpath, 'meta'
        ).with_suffix('.pickle')
        meta_path.parent.mkdir(exist_ok=True, parents=True)
        return meta_path

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
        return _taskpipeoperation(self, 'pull', **kwargs)

    def pull_preview(self, **kwargs):
        """
        Preview pull files from data repo
        """
        return _taskpipeoperation(self, 'pull_preview', **kwargs)

    def push(self, **kwargs):
        """
        Push files to data repo
        """
        return _taskpipeoperation(self, 'push', **kwargs)

    def push_preview(self, **kwargs):
        """
        Preview push files to data repo
        """
        return _taskpipeoperation(self, 'push_preview', **kwargs)


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


class TaskCSVGZPandas(TaskData):
    """
    Task which saves to CSV
    """
    target_class = d6tflow.targets.CSVGZPandasTarget
    target_ext = 'csv.gz'


class TaskExcelPandas(TaskData):
    """
    Task which saves to CSV
    """
    target_class = d6tflow.targets.ExcelPandasTarget
    target_ext = 'xlsx'


class TaskPqPandas(TaskData):
    """
    Task which saves to parquet
    """
    target_class = d6tflow.targets.PqPandasTarget
    target_ext = 'parquet'


class TaskAggregator(luigi.Task):
    """
    Task which yields other tasks

    NB: Use this function by implementing `run()` which should do nothing but yield other tasks

    example::

        class TaskCollector(d6tflow.tasks.TaskAggregator):
            def run(self):
                yield Task1()
                yield Task2()

    """

    def reset(self, confirm=True):
        return self.invalidate(confirm=confirm)

    def invalidate(self, confirm=True):
        [t.invalidate(confirm) for t in self.run()]

    def complete(self, cascade=True):
        return all([t.complete(cascade) for t in self.run()])

    def output(self):
        return [t.output() for t in self.run()]

    def outputLoad(self, keys=None, as_dict=False, cached=False):
        return [t.outputLoad(keys, as_dict, cached) for t in self.run()]
