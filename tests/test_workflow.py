import pytest

import d6tflow
import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model
import pandas as pd
import numpy as np

# define workflow
class TaskGetData(d6tflow.tasks.TaskPqPandas):  # save dataframe as parquet

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

    def run(self):
        df_train = self.input().load()
        if self.model=='ols':
            model = sklearn.linear_model.LogisticRegression()
        elif self.model=='svm':
            model = sklearn.svm.SVC()
        else:
            raise ValueError('invalid model selection')
        model.fit(df_train.drop('y',1), df_train['y'])
        self.save(model)


class Task1(d6tflow.tasks.TaskCache):
    param1 = d6tflow.IntParameter(default=0)

    def run(self):
        self.save({'hello':self.param1})


class TestWorkflow:


    def test_single_flow_preview(self):
        flow = d6tflow.Workflow(params = {'do_preprocess': False})

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count('PENDING')==0
            assert output.count('COMPLETE')==3
            assert output.count("'do_preprocess': 'False'")==1


    def test_single_workflow_no_params_passes(self):
        flow = d6tflow.Workflow()
        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count("'do_preprocess': 'True'")==1


    def test_single_flow_parameters_passes_properly(self):
        flow1 = d6tflow.Workflow(params = {'do_preprocess': False})
        flow2 = d6tflow.Workflow(params = {'do_preprocess': True})

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow1.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count("'do_preprocess': 'False'")==1

            flow2.preview(TaskTrain)
            output2 = buf.getvalue()
            assert output2.count("'do_preprocess': 'True'")==1


    def test_task_runs_properly_and_gives_outputload(self):
        flow = d6tflow.Workflow(params = {'do_preprocess': False})
        flow.preview(TaskTrain)
        flow.run(TaskTrain)
        out1 = flow.outputLoad(TaskTrain)
        out2 = flow.outputLoad(TaskPreprocess)
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        assert type(out1).__name__ == "LogisticRegression"
        assert np.all(out2.drop(['y'], axis=1) == df_train)


    def test_output_load_comparing_with_task_run(self):
        params = {'param1': 1}
        flow = d6tflow.Workflow(params = params, task=Task1)

        flow.run()
        assert Task1(**params).complete() == True
        assert Task1(**params).outputLoad()['hello'] == params['param1']
        assert flow.outputLoad() == Task1(**params).outputLoad()


    def test_output_load_all_comparing_with_task_run(self):
        params = {'param1': 1}
        flow = d6tflow.Workflow(params = params, task=Task1)

        flow.run()
        outputs = flow.outputLoadAll()
        assert outputs['Task1'] == Task1(**params).outputLoad()


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow = d6tflow.Workflow(params = {'do_preprocess': False})
            flow.run()


    def test_default_task(self):
        flow = d6tflow.Workflow({'do_preprocess': False})
        flow.set_default(TaskTrain)
        out_class = flow.get_task()
        type(out_class).__name__ == "TaskTrain"


    def test_tasks_with_dependencies_outputloadall(self):
        flow = d6tflow.Workflow(params = {'do_preprocess': False}, task=TaskTrain)
        flow.preview()
        flow.run()
        flow.run(TaskPreprocess)
        flow.outputLoad()
        flow.outputLoad(TaskPreprocess)

        # load data for all dependencies
        data = flow.outputLoadAll()
        assert 'TaskTrain' in data.keys()
        assert 'TaskPreprocess' in data.keys()
        assert 'TaskGetData' in data.keys()
        assert type(data['TaskTrain']).__name__ == "LogisticRegression"
        ds = sklearn.datasets.load_breast_cancer()
        df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
        assert np.all(data['TaskPreprocess'].drop(['y'], axis=1) == df_train)
        assert np.all(data['TaskGetData'].drop(['y'], axis=1) == df_train)


    def test_get_task(self):
        flow0 = d6tflow.Workflow(task=Task1)
        assert flow0.get_task().param_kwargs['param1'] == 0
        params = {'param1': 1}
        flow = d6tflow.Workflow(params = params, task=Task1)
        assert flow.get_task().param_kwargs['param1'] == params['param1']


class TestWorkflowMulti:

    def test_preview_single_flow(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow2.preview(TaskTrain, flow = 'experiment1')
            output = buf.getvalue()
            assert output.count('PENDING')==0
            assert output.count('COMPLETE')==3
            assert output.count("'do_preprocess': 'False'")==1


    def test_preview_all_flow(self):
        flow2 = d6tflow.WorkflowMulti(params = {'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                  task=TaskTrain)

        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            flow2.preview(TaskTrain)
            output = buf.getvalue()
            assert output.count('PENDING')==0
            assert output.count('COMPLETE')==6
            assert output.count("'do_preprocess': 'False'")==1
            assert output.count("'do_preprocess': 'True'")==1


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

        out = flow2.outputLoad(TaskTrain)
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
