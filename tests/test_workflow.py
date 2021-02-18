import pytest

import d6tflow
import sklearn, sklearn.datasets, sklearn.svm, sklearn.linear_model
import pandas as pd

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

    def test_single_flow(self):
        try:
            flow = d6tflow.flow({'do_preprocess': False})
            flow.preview(TaskTrain)
            flow.run(TaskTrain)
            flow.outputLoad(TaskTrain)
            flow.outputLoad(TaskPreprocess)
            flow.reset(TaskPreprocess, confirm=False)
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_no_default_expect_error(self):
        with pytest.raises(RuntimeError):
            flow = d6tflow.flow({'do_preprocess': False})
            flow.run()


    def test_default_set_no_error(self):
        try:
            flow = d6tflow.flow({'do_preprocess': False})
            flow.set_default(TaskTrain)
            flow.run()
        except Exception as e:
            pytest.fail("Unexpected error " + e.__str__())


    def test_tasks_with_dependencies(self):
        try:
            flow = d6tflow.flow({'do_preprocess': False}, default=TaskTrain)
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
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())


    def test_multi_exp(self):
        try:
            flow2 = d6tflow.flowMulti({'experiment1': {'do_preprocess': False}, 'experiment2': {'do_preprocess': True}},
                                      default=TaskTrain)
            flow2.run()
            flow2.run(flow='experiment1', confirm=False)
            data = flow2.outputLoad(TaskTrain)

            data = flow2.outputLoadAll()
            data['experiment1']['TaskTrain']
            data['experiment2']['TaskTrain']
        except Exception as e:
            pytest.fail("Unexpected error" + e.__str__())
