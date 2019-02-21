import d6tflow
import d6tcollect

try:
    import d6tpipe
except:
    ModuleNotFoundError("Optional requirement d6tpipe not found, install with `pip install d6tpipe`")


@d6tcollect.collect
def init(default_pipe_name, profile=None, local_pipe=False, local_api=False, reset=False, api=None, set_dir=True, **kwargs):
    """
    Initialize d6tpipe

    Args:
        default_pipe_name (str): name of pipe to store results. Override by setting Task.pipe attribute
        profile (str): name of d6tpipe profile to get api if api not provided
        local_pipe (bool): use `PipeLocal()`
        local_api (bool): use `APILocal()`
        reset (bool): reset api and pipe connection
        api (obj): d6tpipe api object. if not provided will be loaded
        set_dir (bool): if True, set d6tflow directory to default pipe directory
    """
    if not d6tflow.settings.isinitpipe or reset:
        d6tflow.cache.pipe_default_name = default_pipe_name
        if local_pipe:
            pipe_ = d6tpipe.PipeLocal(default_pipe_name, profile=profile)
        else:
            if api is None:
                if local_api:
                    d6tflow.cache.api = d6tpipe.APILocal(profile=profile, **kwargs)
                else:
                    d6tflow.cache.api = d6tpipe.APIClient(profile=profile, **kwargs)
            else:
                d6tflow.cache.api = api

            pipe_ = d6tpipe.Pipe(d6tflow.cache.api, default_pipe_name)
        d6tflow.cache.pipes[default_pipe_name] = pipe_
        if set_dir:
            d6tflow.settings.isinitpipe = False
            d6tflow.set_dir(pipe_.dir)

        d6tflow.settings.isinitpipe = True

def _assertinit():
    if not d6tflow.settings.isinitpipe:
        raise RuntimeError('d6tpipe not initialized, run d6tflow.pipes.init()')


@d6tcollect.collect
def init_pipe(name=None, **kwargs):
    _assertinit()
    name = d6tflow.cache.pipe_default_name if name is None else name
    pipe_ = d6tpipe.Pipe(d6tflow.cache.api,name, **kwargs)
    d6tflow.cache.pipes[name] = pipe_


def get_pipe(name=None):
    """
    Get a pipe

    Args:
        name (str): name of pipe

    Returns:
        obj: pipe object

    """
    _assertinit()
    name = d6tflow.cache.pipe_default_name if name is None else name
    if name not in d6tflow.cache.pipes:
        init_pipe(name)

    return d6tflow.cache.pipes[name]

def get_dirpath(name=None):
    """
    Get a pipe directory as Pathlib.Path

    Args:
        name (str): name of pipe

    """
    return get_pipe(name).dirpath

def set_default(name):
    """
    Set default pipe. Will also change d6tflow directory

    Args:
        name (str): name of pipe

    """
    pipe_ = get_pipe(name)
    d6tflow.cache.pipe_default_name = name
    d6tflow.settings.isinitpipe = False
    d6tflow.set_dir(pipe_.dir)
    d6tflow.settings.isinitpipe = True

def _all_pipes(task, push=False):
    """
    Get all pipes for all downstream tasks in a flow

    Args:
        task (obj): task object

    """
    tasks = d6tflow.taskflow_upstream(task, only_complete=False)
    if push:
        pipenames = set([t.get_pipename() for t in tasks if not getattr(t, 'external', False)])
    else:
        pipenames = set([t.get_pipename() for t in tasks])
    return {name:get_pipe(name) for name in pipenames}

@d6tcollect.collect
def _all_runfun(task, fun, push=False, **kwargs):
    pipes = _all_pipes(task,push)
    def runfun(p):
        f = getattr(p, fun)
        return f(**kwargs)
    return {pipe.pipe_name: runfun(pipe) for pipe in pipes.values()}

def all_pull(task, **kwargs):
    """
    Pull for all upstream tasks in a flow
    """
    return _all_runfun(task, 'pull', **kwargs)
def all_pull_preview(task, **kwargs):
    """
    Pull preview for all upstream tasks in a flow
    """
    return _all_runfun(task, 'pull_preview', **kwargs)
def all_push(task, **kwargs):
    """
    Push for all upstream tasks in a flow
    """
    return _all_runfun(task, 'push', push=True, **kwargs)
def all_push_preview(task, **kwargs):
    """
    Push preview for all upstream tasks in a flow
    """
    return _all_runfun(task, 'push_preview', push=True, **kwargs)
