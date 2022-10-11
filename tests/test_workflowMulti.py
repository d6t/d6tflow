import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model
from pathlib import PosixPath
import pandas as pd
import pytest
import d6tflow
from tests.test_workflow import Task2

# Test Workflow
# Save dataframe as parquet
class TaskGetData(d6tflow.tasks.TaskPqPandas):

    def run(self):
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_train['y'] = ds.target
        self.save(df_train) # quickly save dataframe

@d6tflow.requires(TaskGetData) # define dependency
class TaskPreprocess(d6tflow.tasks.TaskPqPandas):
    do_preprocess = d6tflow.BoolParameter(default=True) # parameter for preprocessing yes/no

    def run(self):
        df_train = self.input().load() # quickly load required data
        if self.do_preprocess:
            df_train.iloc[:,:-1] = sklearn.preprocessing.scale(df_train.iloc[:,:-1])
        self.save(df_train)

@d6tflow.requires(TaskPreprocess) # automatically pass parameters upstream
class TaskTrain(d6tflow.tasks.TaskPickle): # save output as pickle
    model = d6tflow.Parameter(default='ols') # parameter for model selection
    scale = d6tflow.BoolParameter(default=False)

    def run(self):
        df_train = self.input().load()
        if self.model=='ols':
            model = sklearn.linear_model.LogisticRegression()
        elif self.model=='svm':
            model = sklearn.svm.SVC()
        else:
            raise ValueError('invalid model selection')
        model.fit(df_train.drop('y',axis=1), df_train['y'])
        self.save(model)

class Task1(d6tflow.tasks.TaskCache):
    param1 = d6tflow.IntParameter(default=0)

    def run(self):
        self.save({'hello':self.param1})

class TestWorkflowMulti:

    def test_preview_single_flow(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)
        # Reset
        flow2.reset(TaskGetData)
        flow2.reset(TaskPreprocess)
        flow2.reset(TaskTrain)

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow2.preview(tasks = TaskTrain, flow = 'experiment1')
            output = buf.getvalue()
            assert output.count('PENDING') == 3
            assert output.count('COMPLETE') == 0
            assert output.count("'do_preprocess': 'False'") == 1


    def test_preview_all_flow(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)
        # Reset
        flow2.reset(TaskGetData)
        flow2.reset(TaskPreprocess)
        flow2.reset(TaskTrain)

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow2.preview(flow = None, tasks = TaskTrain)
            output = buf.getvalue()
            assert output.count('PENDING') == 6
            assert output.count('COMPLETE') == 0
            assert output.count("'do_preprocess': 'False'") == 1
            assert output.count("'do_preprocess': 'True'") == 1


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}})
            flow2.run()


    def test_no_exp_params_passed(self):
        with pytest.raises(Exception) as e:
            flow2 = d6tflow.WorkflowMulti()
            assert str(e.value) == "Experments not defined"


    def test_default_task(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}})
        flow2.set_default(TaskTrain)
        out_class = flow2.get_task()
        type(out_class).__name__ == "TaskTrain"


    def test_multi_exp_single_flows_single_outputload(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)
        flow2.run(flow = "experiment1")

        out = flow2.outputLoad(flow="experiment1", task=TaskTrain)
        type(out).__name__ == "LogisticRegression"


    def test_multi_exp_all_flows_outputloadall(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)
        flow2.run()

        data = flow2.outputLoadAll()
        type(data['experiment1']['TaskTrain']).__name__ == "LogisticRegression"
        type(data['experiment2']['TaskTrain']).__name__ == "LogisticRegression"


    def test_multi_get_task(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = {1: params, 2: params2}, task=Task1)
        assert flow2.get_task(flow=1).param_kwargs['param1'] == 1
        assert flow2.get_task(flow=2).param_kwargs['param1'] == 2


    def test_multi_params_generator(self):
        params = {'model': ['ols', 'gbm'], 'scale': [False, True]}
        flow2 = d6tflow.WorkflowMulti(params = params, task=TaskTrain)
        assert flow2.get_task(flow="model_ols_scale_False").param_kwargs["model"] == "ols"
        assert flow2.get_task(flow="model_ols_scale_False").param_kwargs["scale"] == False
        assert flow2.get_task(flow="model_ols_scale_True").param_kwargs["model"] == "ols"
        assert flow2.get_task(flow="model_ols_scale_True").param_kwargs["scale"] == True
        assert flow2.get_task(flow="model_gbm_scale_False").param_kwargs["model"] == "gbm"
        assert flow2.get_task(flow="model_gbm_scale_False").param_kwargs["scale"] == False
        assert flow2.get_task(flow="model_gbm_scale_True").param_kwargs["model"] == "gbm"
        assert flow2.get_task(flow="model_gbm_scale_True").param_kwargs["scale"] == True


    def test_multi_get_task_with_list_params(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = [params, params2], task=Task1)
        assert flow2.get_task(flow=0).param_kwargs['param1'] == 1
        assert flow2.get_task(flow=1).param_kwargs['param1'] == 2


    def test_output_load_comparing_with_task_run(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = {1: params, 2: params2}, task=Task1)

        flow2.run()
        assert flow2.outputLoad(flow = 1) == Task1(**params).outputLoad()
        assert flow2.outputLoad(flow = 2) == Task1(**params2).outputLoad()


    def test_output_load_all_comparing_with_task_run(self):
        params2 = {'param1': 2}
        params = {'param1': 1}
        flow2 = d6tflow.WorkflowMulti(params = {1: params, 2: params2}, task=Task1)

        flow2.run()
        outputs = flow2.outputLoadAll()
        assert outputs[1]['Task1'] == Task1(**params).outputLoad()
        assert outputs[2]['Task1'] == Task1(**params2).outputLoad()

    def test_flow_task_list_raise_error(self):
        with pytest.raises(Exception) as e_info:
            flow = d6tflow.Workflow(task=[Task1, Task2])
            flow.run()

# Test Output Reset
class PathTask1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe

@d6tflow.requires(PathTask1)
class PathTask2(d6tflow.tasks.TaskPqPandas):
    path = 'data/data2_changed'

    def run(self):
        df1 = self.inputLoad() * 2
        self.save(df1)

@d6tflow.requires(PathTask2)
class PathTask3(d6tflow.tasks.TaskPqPandas):

    def run(self):
        df2 = self.inputLoad() * 3
        self.save(df2)

class TestWorkflowReset:

    def test_reset_downstream_multi(self):
        # Execute task including all its dependencies
        flow_multi = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': True}, 'experiment2': {'do_preprocess': True}},
                                  task=PathTask3)
        flow_multi.run()
        # Reset
        flow_multi.reset_downstream(PathTask1)
        # assert not flow_multi.complete()


# Test Output Path
class PathTask1(d6tflow.tasks.TaskCache):

    def run(self):
        df = pd.DataFrame({'a': range(3)})
        self.save(df)  # quickly save dataframe

@d6tflow.requires(PathTask1)
class PathTask2(d6tflow.tasks.TaskPqPandas):
    path = 'data/data2_changed'

    def run(self):
        df1 = self.inputLoad() * 2
        self.save(df1)

@d6tflow.requires(PathTask2)
class PathTask3(d6tflow.tasks.TaskPqPandas):

    def run(self):
        df2 = self.inputLoad() * 3
        self.save(df2)

class PathTaskMultiOutput(d6tflow.tasks.TaskPqPandas):
    persist = ['xx','yy']

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_trainX = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_trainY = pd.DataFrame(ds.target, columns=['target'])
        self.save({'xx': df_trainX, 'yy': df_trainY}) # persist/cache training data

class TestWorkflowOutput:

    def setup_module(self):
        # Reset Dir For Tests
        d6tflow.set_dir('data/')

    def test_workflow_multi_outputPath(self):
        flow_multi = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=PathTask3)
        flow_multi.run()

        path_1 = flow_multi.outputPath(PathTask1)
        path_2 = flow_multi.outputPath(PathTask2)
        assert path_1 == {
            'experiment1': 'data/PathTask1/PathTask1__99914b932b-data.cache', 
            'experiment2': 'data/PathTask1/PathTask1__99914b932b-data.cache'
        }
        assert path_2 == {
            'experiment1': PosixPath('data/data2_changed/PathTask2/PathTask2__99914b932b-data.parquet'), 
            'experiment2': PosixPath('data/data2_changed/PathTask2/PathTask2__99914b932b-data.parquet')
        }

        # Test Multi with Multi Output
        flow_multi = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=PathTaskMultiOutput)
        flow_multi.run()

        path_1 = flow_multi.outputPath(PathTaskMultiOutput)
        assert path_1 == {
            'experiment1': 
                {
                    'xx': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-xx.parquet'),
                    'yy': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-yy.parquet')
                }, 
            'experiment2': 
                {
                    'xx': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-xx.parquet'),
                    'yy': PosixPath('data/PathTaskMultiOutput/PathTaskMultiOutput__99914b932b-yy.parquet')
                }
            }

    def test_workflow_set_path_and_env(self):
        # Execute task including all its dependencies
        flow = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                        task=PathTask3, path='data/data2/wfm_change', env='prod')
        flow.run()

        path_2 = flow.outputPath(PathTask2)
        path_3 = flow.outputPath(PathTask3)
        assert path_2 == {
            'experiment1': PosixPath('data/data2/wfm_change/env=prod/PathTask2/PathTask2__99914b932b-data.parquet'),
            'experiment2': PosixPath('data/data2/wfm_change/env=prod/PathTask2/PathTask2__99914b932b-data.parquet')
        }
        assert path_3 == {
            'experiment1': PosixPath('data/data2/wfm_change/env=prod/PathTask3/PathTask3__99914b932b-data.parquet'),
            'experiment2': PosixPath('data/data2/wfm_change/env=prod/PathTask3/PathTask3__99914b932b-data.parquet')
        }
        flow.reset()
