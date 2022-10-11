# luigi.tools.deps_tree

from luigi.task import flatten
import os
import warnings
import pathlib

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
    pathlib.Path(path).parent.mkdir(exist_ok=True)
    df.to_parquet(path, **opts)


def generate_exps_for_multi_param(params_dict, current_key = 0, multi_exp_dict = {}):
    current_multi_exp_dict = {}
    permutation_keys_list = list(params_dict.keys())
    permutation_keys_list.sort()
    input_key = permutation_keys_list[current_key]
    for current_key_val in params_dict[input_key]:
        if current_key == 0:
            current_key_val_multi_exp_dict = {f'{input_key}_{current_key_val}': {f'{input_key}' : current_key_val}}
            current_key_val_multi_exp_dict_results = generate_exps_for_multi_param(params_dict, current_key = current_key + 1, multi_exp_dict = current_key_val_multi_exp_dict)
            current_multi_exp_dict = {**current_multi_exp_dict, **current_key_val_multi_exp_dict_results}
        if current_key == len(permutation_keys_list) - 1:
            current_multi_exp_dict = {**current_multi_exp_dict, **{f'{k}_{input_key}_{current_key_val}': {**v, **{f'{input_key}' : current_key_val}} for k,v in multi_exp_dict.items()}}
        else:
            current_key_val_multi_exp_dict = {f'{k}_{input_key}_{current_key_val}': {**v, **{f'{input_key}' : current_key_val}} for k,v in multi_exp_dict.items()}
            current_key_val_multi_exp_dict_results = generate_exps_for_multi_param(params_dict, current_key = current_key + 1, multi_exp_dict = current_key_val_multi_exp_dict)
            current_multi_exp_dict = {**current_multi_exp_dict, **current_key_val_multi_exp_dict_results}
    return current_multi_exp_dict

params_generator_multiple = generate_exps_for_multi_param

def params_generator_single(dict_,params_base=None):
    # example input: {'a':[1,2,3]}
    key,list_=list(dict_.items())[0]

    params = {}
    for i, v in enumerate(list_):
        params[i] = {**{key: v}, **params_base} if params_base is not None else {key: v}

    return params
