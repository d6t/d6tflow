import d6tflow
import d6tcollect

try:
    import d6tpipe
except:
    ModuleNotFoundError("Optional requirement d6tpipe not found, install with `pip install d6tpipe`")


#@d6tcollect.collect
def init(default_pipe_name, profile=None, local_pipe=False, local_api=False, reset=False, api=None, set_dir=True, api_args=None, pipe_args=None):
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
        api_args (dir): arguments to pass to api
        pipe_args (dir): arguments to pass to pipe
    """
    if not d6tflow.settings.isinitpipe or reset:
        d6tflow.cache.pipe_default_name = default_pipe_name
        api_args = {} if api_args is None else api_args
        pipe_args = {} if pipe_args is None else pipe_args
        if local_pipe:
            pipe_ = d6tpipe.PipeLocal(default_pipe_name, profile=profile)
        else:
            if api is None:
                if local_api:
                    d6tflow.cache.api = d6tpipe.APILocal(profile=profile, **api_args)
                else:
                    d6tflow.cache.api = d6tpipe.APIClient(profile=profile, **api_args)
            else:
                d6tflow.cache.api = api

            pipe_ = d6tpipe.Pipe(d6tflow.cache.api, default_pipe_name, **pipe_args)
        d6tflow.cache.pipes[default_pipe_name] = pipe_
        if set_dir:
            d6tflow.settings.isinitpipe = False
            d6tflow.set_dir(pipe_.dir)

        d6tflow.settings.isinitpipe = True

def _assertinit():
    if not d6tflow.settings.isinitpipe:
        raise RuntimeError('d6tpipe not initialized, run d6tflow.pipes.init()')


#@d6tcollect.collect
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
    return {pipe.name: runfun(pipe) for pipe in pipes.values()}

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


#**********************************************
# export tasks
#**********************************************
import pathlib

from jinja2 import Template

class FlowExport(object):

    """
    Auto generate run and task files to quickly share workflows with others using d6tpipe.

    Args:
        tasks (obj): task or list of tasks to share
        flows (obj): task or list of tasks to share, including upstream tasks
        pipename (str): name of d6tpipe pipe
        write_dir (str): directory to export files into
        write_filename_tasks (str): filename for tasks
        write_filename_run (str): filename for run
        run_load_values (bool): include param values in run file, eg `Task1(param=1)` vs just `Task1(param=1)` if false
        run (bool): auto-run tasks
        run_params (dict): parameters to pass to d6tflow.run(tasks)
    """
    def __init__(self, pipename, tasks=None, flows=None, write_dir='.', write_filename_tasks='tasks_d6tpipe.py', write_filename_run='run_d6tpipe.py', run_load_values=True, run=False, run_params=None):
        # todo NN: copy = False # copy task output to pipe
        tasks = [] if tasks is None else tasks
        flows = [] if flows is None else flows
        if not isinstance(tasks, (list,)):
            tasks = [tasks]
        if not isinstance(flows, (list,)):
            flows = [flows]
        for flow in flows:
            for task in d6tflow.utils.traverse(flow):
                tasks.append(task)

        if run:
            run_params = {} if run_params is None else run_params
            d6tflow.run(tasks,**run_params)

        self.tasks = tasks
        self.pipename = pipename
        self.write_dir = pathlib.Path(write_dir)
        self.write_filename_tasks = write_filename_tasks
        self.write_filename_run = write_filename_run
        self.run_load_values = run_load_values

        # file templates
        self.tmpl_tasks = '''
import d6tflow
import luigi
import datetime

{% for task in tasks -%}

class {{task.name}}({{task.class}}):
    external=True
    persist={{task.obj.persist}}
    {% for param in task.params -%}
    {{param.name}}={{param.class}}(default={{param.value}})
    {% endfor %}
{% endfor %}
'''

        self.tmpl_run = '''
# shared d6tflow workflow, see https://d6tflow.readthedocs.io/en/latest/collaborate.html
import d6tflow.pipes
import {{self_.write_filename_tasks[:-3]}}
import datetime

d6tflow.pipes.init('{{self_.pipename}}',profile='default') # to customize see https://d6tflow.readthedocs.io/en/latest/d6tflow.html#d6tflow.pipes.init
d6tflow.pipes.get_pipe('{{self_.pipename}}').pull()

# task output is loaded below, for more details see https://d6tflow.readthedocs.io/en/latest/tasks.html#load-output-data
{% for task in tasks -%}
{% if self_.run_load_values -%}
{% if task.persist|length > 1 -%}
{% for df in task.persist -%}df_{{task.name|lower}}_{{df}}, {% endfor -%} = {{self_.write_filename_tasks[:-3]}}.{{task.name}}({% for param in task.params %}{{param.name}}={{param.value}}, {% endfor %}).outputLoad()
{% else -%}
df_{{task.name|lower}} = {{self_.write_filename_tasks[:-3]}}.{{task.name}}({% for param in task.params %}{{param.name}}={{param.value}}, {% endfor %}).outputLoad()
{% endif -%}
{% else -%}
df_{{task.name|lower}} = {{self_.write_filename_tasks[:-3]}}.{{task.name}}().outputLoad()
{% endif -%}
{% endfor %}
'''


    def generate(self):
        """
        Generate output files
        """
        tasksPrint = []
        for task in self.tasks:
                if getattr(task,'export',True):
                    class_ = next(c for c in type(task).__mro__ if 'd6tflow.tasks.' in str(c)) # type(task).__mro__[1]
                    taskPrint = {'name': task.__class__.__name__, 'class': class_.__module__ + "." + class_.__name__,
                                 'obj': task, 'persist': task.persist, 'params': [{'name': param[0],
                                                          'class': f'{param[1].__class__.__module__}.{param[1].__class__.__name__}',
                                                          'value': repr(getattr(task,param[0]))} for param in task.get_params()]} # param[1]._default
                    tasksPrint.append(taskPrint)

        tasksPrint[-1], tasksPrint[0:-1] = tasksPrint[0], tasksPrint[1:]

        # write files
        with open(self.write_dir/self.write_filename_tasks, 'w') as fh:
            fh.write(Template(self.tmpl_tasks).render(tasks=tasksPrint))

        with open(self.write_dir/self.write_filename_run, 'w') as fh:
            fh.write(Template(self.tmpl_run).render(self_=self,tasks=tasksPrint))

    def push(self): # pipename.push()
        raise NotImplementedError()

    def test(self): # test if target user can run all
        raise NotImplementedError()

    def complete(self): # check if all tasks complete
        raise NotImplementedError()
