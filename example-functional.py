import d6tflow
import pandas as pd

from d6tflow.functional import Workflow
flow = Workflow()

@flow.task(d6tflow.tasks.TaskPqPandas)
def get_data1(task):
    df = pd.DataFrame({'a':range(3)})
    task.save(df)

@flow.task(d6tflow.tasks.TaskCache)
@flow.persists(['b'])
def get_data2(task):
    df = pd.DataFrame({'b':range(3)})
    task.save({'b':df})

@flow.task(d6tflow.tasks.TaskPqPandas)
@flow.requires({"input1":get_data1, "input2":get_data2})
@flow.params(multiplier=d6tflow.IntParameter(default=0))
def usedata(task):
    data = task.inputLoad()
    df1 = data['input1']
    df2 = data['input2']
    df3 = df1.join(df2, lsuffix='1', rsuffix='2')
    df3['b']=df3['a']*task.multiplier # use task parameter
    task.save(df3)


flow.add_global_params(multiplier=d6tflow.IntParameter(default=0))
use_params = {'exp1':{'multiplier':40}, 'exp2': {'multiplier':42}}


flow.preview(usedata, params=use_params)
flow.run(usedata, multi_params=use_params, forced_all_upstream=True, confirm=False)
print(flow.outputLoad(usedata))
print(flow.outputLoadAll(usedata))

use_params = {'multiplier':42}
flow.preview(usedata, params=use_params)
flow.run(usedata, params=use_params, forced_all_upstream=True, confirm=False)

print(flow.outputLoad(usedata))
print(flow.outputLoadAll(usedata, as_dict=True))
flow.resetAll(confirm=False)
