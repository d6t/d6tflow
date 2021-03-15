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


class TestWorkflow:


    def test_single_flow_preview(self):
        try:
            flow = d6tflow.Workflow({'do_preprocess': False})

            import io
            from contextlib import redirect_stdout

            with io.StringIO() as buf, redirect_stdout(buf):
                flow.preview(TaskTrain)
                output = buf.getvalue()
                assert output.count('PENDING')==0
                assert output.count('COMPLETE')==3
                assert output.count("'do_preprocess': 'False'")==1
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_single_flow_parameters_passes_properly(self):
        try:
            flow1 = d6tflow.Workflow({'do_preprocess': False})
            flow2 = d6tflow.Workflow({'do_preprocess': True})

            import io
            from contextlib import redirect_stdout

            with io.StringIO() as buf, redirect_stdout(buf):
                flow1.preview(TaskTrain)
                output = buf.getvalue()
                assert output.count("'do_preprocess': 'False'")==1

                flow2.preview(TaskTrain)
                output2 = buf.getvalue()
                assert output2.count("'do_preprocess': 'True'")==1
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_task_runs_properly_and_gives_outputload(self):
        try:
            flow = d6tflow.Workflow({'do_preprocess': False})
            flow.preview(TaskTrain)
            flow.run(TaskTrain)
            out1 = flow.outputLoad(TaskTrain)
            out2 = flow.outputLoad(TaskPreprocess)
            ds = sklearn.datasets.load_breast_cancer()
            df_train = pd.DataFrame(ds.data, columns=ds.feature_names)
            assert type(out1).__name__ == "LogisticRegression"
            assert np.all(out2.drop(['y'], axis=1) == df_train)
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow = d6tflow.Workflow({'do_preprocess': False})
            flow.run()


    def test_default_task(self):
        try:
            flow = d6tflow.Workflow({'do_preprocess': False})
            flow.set_default(TaskTrain)
            out_class = flow.get_task()
            type(out_class).__name__ == "TaskTrain"
        except Exception as e:
            pytest.fail("Unexpected error " + e.__str__())


    def test_tasks_with_dependencies_outputloadall(self):
        try:
            flow = d6tflow.Workflow({'do_preprocess': False}, default=TaskTrain)
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
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


class TestWorkflowMulti:

    def test_preview_single_flow(self):
        try:
            flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                      default=TaskTrain)

            import io
            from contextlib import redirect_stdout

            with io.StringIO() as buf, redirect_stdout(buf):
                flow2.preview(TaskTrain, flow = 'experiment1')
                output = buf.getvalue()
                assert output.count('PENDING')==0
                assert output.count('COMPLETE')==3
                assert output.count("'do_preprocess': 'False'")==1
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_preview_all_flow(self):
        try:
            flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                      default=TaskTrain)

            import io
            from contextlib import redirect_stdout

            with io.StringIO() as buf, redirect_stdout(buf):
                flow2.preview(TaskTrain)
                output = buf.getvalue()
                assert output.count('PENDING')==0
                assert output.count('COMPLETE')==6
                assert output.count("'do_preprocess': 'False'")==1
                assert output.count("'do_preprocess': 'True'")==1
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}})
            flow2.run()


    def test_default_task(self):
        try:
            flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}})
            flow2.set_default(TaskTrain)
            out_class = flow2.get_task()
            type(out_class).__name__ == "TaskTrain"
        except Exception as e:
            pytest.fail("Unexpected error " + e.__str__())


    def test_multi_exp_single_flows_single_outputload(self):
        try:
            flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                      default=TaskTrain)
            flow2.run(flow = "experiment1")

            out = flow2.outputLoad(TaskTrain)
            type(out).__name__ == "LogisticRegression"
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_multi_exp_all_flows_outputloadall(self):
        try:
            flow2 = d6tflow.WorkflowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                      default=TaskTrain)
            flow2.run()

            data = flow2.outputLoadAll()
            type(data['experiment1']['TaskTrain']).__name__ == "LogisticRegression"
            type(data['experiment2']['TaskTrain']).__name__ == "LogisticRegression"
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())
