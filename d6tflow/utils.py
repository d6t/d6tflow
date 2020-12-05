# luigi.tools.deps_tree

from luigi.task import flatten
import warnings

from luigi.tools.deps_tree import bcolors

def print_tree(task, indent='', last=True, show_params=True, clip_params=False):
    '''
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format
    '''
    # dont bother printing out warnings about tasks with no output
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='Task .* without outputs has no custom complete\\(\\) method')
        is_task_complete = task.complete()
    is_complete = (bcolors.OKGREEN + 'COMPLETE' if is_task_complete else bcolors.OKBLUE + 'PENDING') + bcolors.ENDC
    name = task.__class__.__name__
    if show_params:
        params = task.to_str_params(only_significant=True)
        if len(params)>1 and clip_params:
            params = next(iter(params.items()), None)  # keep only one param
            params = str(dict([params]))+'[more]'
    else:
        params = ''
    result = '\n' + indent
    if(last):
        result += '└─--'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    result += '[{0}-{1} ({2})]'.format(name, params, is_complete)
    children = flatten(task.requires())
    for index, child in enumerate(children):
        result += print_tree(child, indent, (index+1) == len(children), clip_params)
    return result


def traverse(t, path=None):
    '''
    Get upstream dependencies
    '''
    if path is None: path = []
    path = path + [t]
    for node in flatten(t.requires()):
        if not node in path:
            path = traverse(node, path)
    return path

def to_parquet(df, path, **kwargs):
    opts = {**{'compression': 'gzip', 'engine': 'pyarrow'}, **kwargs}
    df.to_parquet(path, **opts)