import pytest
import os
import glob
import shutil
from pathlib import Path, PurePosixPath
import pandas as pd
import fuckit
import warnings

import luigi
import d6tcollect
d6tcollect.submit=False

import d6tflow
pathdata = d6tflow.set_dir('tests-data/')
d6tflow.settings.log_level = 'WARNING'

# test data
df = pd.DataFrame({'a': range(10)})
dfc2 = df.copy()
dfc2['a']=dfc2['a']*2
dfc4 = df.copy()
dfc4['a']=dfc4['a']*2*2


@pytest.fixture
def cleanup(scope="module"):
    with fuckit:
        shutil.rmtree(pathdata)
    pathdata.mkdir(exist_ok=True)
    yield True
    shutil.rmtree(pathdata)

def test_cleanup(cleanup):
    pass

#********************************************
# targets
#********************************************

def dfhelper(obj,df_,file):
    fname = pathdata/file
    t = obj(fname)
    assert t.save(df_)==fname
    df_c = t.load()
    assert df_c.equals(df_)
    if d6tflow.settings.cached:
        assert d6tflow.data[fname].equals(df_)

@pytest.fixture
def load_targets(scope="module"):
    d6tflow.settings.cached=True
    dfhelper(d6tflow.targets.CSVPandasTarget,df,'test.csv')
    d6tflow.settings.cached=False
    dfhelper(d6tflow.targets.PqPandasTarget,df,'test.parquet')

def test_targets(cleanup, load_targets):
    pass

def test_cache(cleanup, load_targets):
    assert d6tflow.data[pathdata/'test.csv'].equals(df)
    assert pathdata/'test.parquet' not in d6tflow.data

    d6tflow.settings.cached=False
    dfhelper(d6tflow.targets.PqPandasTarget,df,'test2.parquet')
    assert pathdata/'test2.parquet' not in d6tflow.data
    d6tflow.settings.cached=True

#********************************************
# tasks
#********************************************

class Task1(d6tflow.tasks.TaskPqPandas):
    def run(self):
        self.save(df)

def df2fun(task):
    df2 = task.input().load()
    df2['a'] = df2['a'] * 2
    df4 = task.input().load()
    df4['a'] = df4['a'] * 2 * 2
    task.save({'df2': df2, 'df4': df4})


class Task2(d6tflow.tasks.TaskPqPandas):
    persist = ['df2','df4']
    def requires(self):
        return Task1()
    def run(self):
        df2fun(self)

class Task3(d6tflow.tasks.TaskPqPandas):
    do_preprocess = luigi.BoolParameter(default=True)
    def requires(self):
        return Task2()
    def run(self):
        if self.do_preprocess:
            pass
        df2, df4 = self.inputLoad()
        self.save(self.input()['df2'].load())

def test_tasks(cleanup):
    t1=Task1(); t2=Task2();
    assert not t1.complete(); assert not t2.complete();

    t1.run()
    assert t1.complete()
    assert t1.invalidate(confirm=False)
    assert not t1.complete()

    assert d6tflow.run([Task2()])
    assert t1.complete(); assert t2.complete();
    assert (pathdata/'Task1'/'Task1__99914b932b-data.parquet').exists()
    assert (pathdata/'Task2'/'Task2__99914b932b-df2.parquet').exists()

    # load outputs
    t1.output().load().equals(df)
    t1.outputLoad(as_dict=True).equals(df)
    t1.outputLoad().equals(df)

    t2.output()['df2'].load().equals(dfc2)
    t2.outputLoad(as_dict=True)['df2'].equals(dfc2)
    df2, df4 = t2.outputLoad()
    df2.equals(dfc2)
    df2, = t2.outputLoad(keys=['df2'])
    df2.equals(dfc2)

    # test inputs
    class TaskMultiInput(d6tflow.tasks.TaskCache):
        def requires(self):
            return Task1()
        def run(self):
            dft1 = self.inputLoad()
            assert dft1.equals(df)
    TaskMultiInput().run()

    class TaskMultiInput(d6tflow.tasks.TaskCache):
        def requires(self):
            return Task1(), Task1()
        def run(self):
            dft1, dft2 = self.inputLoad()
            assert dft1.equals(dft2)
    TaskMultiInput().run()

    class TaskMultiInput(d6tflow.tasks.TaskCache):
        def requires(self):
            return {1:Task1(), 2:Task1()}
        def run(self):
            input = self.inputLoad()
            assert input[1].equals(input[2])
    TaskMultiInput().run()

    class TaskMultiInput2(d6tflow.tasks.TaskCache):
        def requires(self):
            return {'in1':Task2(), 'in2':Task2()}
        def run(self):
            input = self.inputLoad(task='in1')
            assert input['df2'].equals(dfc2) and input['df4'].equals(dfc4)
    TaskMultiInput2().run()

    # check downstream incomplete
    t1.invalidate(confirm=False)
    assert not t2.complete()
    d6tflow.settings.check_dependencies=False
    assert t2.complete()
    d6tflow.settings.check_dependencies=True

def test_task_overrides(cleanup):
    t1=Task1()
    t1.target_dir = 'test'
    t1.target_ext = 'pq'

    assert not t1.complete()
    t1.run()
    assert (pathdata/t1.target_dir/f'Task1__99914b932b-data.{t1.target_ext}').exists()

    t1.save_attrib = False
    t1.run()
    assert (pathdata/t1.target_dir/f'data.{t1.target_ext}').exists()

def test_formats(cleanup):
    def helper(data,TaskClass,format=None):
        class TestTask(TaskClass):
            def run(self):
                self.save(data)

        TestTask().run()
        if format=='pd':
            assert TestTask().output().load().equals(data)
        else:
            assert TestTask().output().load()==data

    helper(df, d6tflow.tasks.TaskCachePandas, 'pd')
    helper({'test': 1}, d6tflow.tasks.TaskJson)
    helper({'test': 1}, d6tflow.tasks.TaskPickle)

    from d6tflow.tasks.h5 import TaskH5Pandas
    helper(df, TaskH5Pandas, 'pd')

    try:
        from d6tflow.tasks.dt import TaskDatatable
        import datatable as dt
        dt = dt.Frame(df)
        helper(dt, TaskH5Pandas)
    except:
        warnings.warn('datatable failed')

    if 0==1: # todo:
        import dask.dataframe as dd
        t1 = Task1();
        t1.run();
        ddf = dd.read_parquet(t1.output().path)
        from d6tflow.tasks.dask import TaskPqDask
        helper(ddf, TaskPqDask, 'pd')
        t1.invalidate(confirm=False)

def test_requires():
    class Task1(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
    class Task2(Task1):
        pass
    # define another task that depends on data from task1 and task2
    @d6tflow.requires({'a': Task1, 'b': Task2})
    class Task3(d6tflow.tasks.TaskCache):
        def run(self):
            df1 = self.input()['a'].load()  # quickly load input data
            df2 = self.input()['b'].load()  # quickly load input data
            
            assert(df1.equals(pd.DataFrame({'a': range(3)})))
    task3 = Task3()
    d6tflow.run(task3)


def test_multiple_deps_on_input_load():
    # define 2 tasks that load raw data
    class Task1(d6tflow.tasks.TaskCache):
        persist = ['a1','a2']
        def run(self):
            df = pd.DataFrame({'a':range(3)})
            self.save({'a1':df,'a2':df}) # quickly save dataframe

    class Task2(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a':range(3)})
            self.save(df) # quickly save dataframe

    # define another task that depends on data from task1 and task2
    @d6tflow.requires(Task1,Task2)
    class Task3(d6tflow.tasks.TaskCache):
        def run(self):
            data = self.inputLoad() # ===> implement this
            df1 = data[0]['a1']
            assert df1.equals(data[0]['a2'])
            df2 = data[1]
            assert df2.equals(df1)
            df = df1.join(df2, lsuffix='1', rsuffix='2')
            self.save(df)

# Execute task including all its dependencies
d6tflow.run(Task3(),forced_all_upstream=True,confirm=False)

def test_params(cleanup):
    class TaskParam(d6tflow.tasks.TaskCache):
        nrows = luigi.IntParameter(default=10)
        def run(self):
            self.save(pd.DataFrame({'a':range(self.nrows)}))

    t1 = TaskParam(); t2 = TaskParam(nrows=20);
    assert not t1.complete(); assert not t2.complete();

    t1.run()
    assert t1.complete(); assert not t2.complete();

def test_external(cleanup):
    class Task2(d6tflow.tasks.TaskPqPandas):
        external = True

    # t2=Task2()
    # assert len(t2.requires())==0
    # assert t2.output()['df2'].load().equals(dfc2)


#********************************************
# execution
#********************************************
def test_execute(cleanup):
    # execute
    t1=Task1(); t2=Task2();t3=Task3();
    [t.invalidate(confirm=False) for t in [t1,t2,t3]]
    d6tflow.run(t3)
    assert all(t.complete() for t in [t1,t2,t3])
    t1.invalidate(confirm=False); t2.invalidate(confirm=False);
    assert not t3.complete() # cascade upstream
    d6tflow.settings.check_dependencies=False
    assert t3.complete() # no cascade upstream
    d6tflow.run([t3])
    assert t3.complete() and not t1.complete()
    d6tflow.settings.check_dependencies=True
    d6tflow.run([t3])
    assert all(t.complete() for t in [t1,t2,t3])

    # forced single
    class TaskTest(d6tflow.tasks.TaskCachePandas):
        def run(self):
            self.save(df)

    d6tflow.run(TaskTest())
    assert TaskTest().output().load().equals(df)
    class TaskTest(d6tflow.tasks.TaskCachePandas):
        def run(self):
            self.save(df*2)

    d6tflow.run(TaskTest())
    assert TaskTest().output().load().equals(df)
    d6tflow.run(TaskTest(),forced=TaskTest(),confirm=False)
    assert TaskTest().output().load().equals(df*2)
    d6tflow.run([TaskTest()],forced=[TaskTest()],confirm=False)

    # forced flow
    mtimes = [t1.output().path.stat().st_mtime,t2.output()['df2'].path.stat().st_mtime]
    d6tflow.run(t3,forced=t1,confirm=False)
    assert t1.output().path.stat().st_mtime>mtimes[0]
    assert t2.output()['df2'].path.stat().st_mtime>mtimes[1]

    # forced_all => run task3 only
    mtimes = [t1.output().path.stat().st_mtime,t2.output()['df2'].path.stat().st_mtime,t3.output().path.stat().st_mtime]
    d6tflow.run(t3,forced_all=True,confirm=False)
    assert t1.output().path.stat().st_mtime==mtimes[0]
    assert t2.output()['df2'].path.stat().st_mtime==mtimes[1]
    assert t3.output().path.stat().st_mtime>mtimes[2]

    # forced_all_upstream => run all tasks
    mtimes = [t1.output().path.stat().st_mtime,t2.output()['df2'].path.stat().st_mtime,t3.output().path.stat().st_mtime]
    d6tflow.run(t3,forced_all_upstream=True,confirm=False)
    assert t1.output().path.stat().st_mtime>mtimes[0]
    assert t2.output()['df2'].path.stat().st_mtime>mtimes[1]
    assert t3.output().path.stat().st_mtime>mtimes[2]

    # downstream
    assert d6tflow.run(t3)
    d6tflow.invalidate_downstream(t2, t3, confirm=False)
    assert not (t2.complete() and t3.complete()) and t1.complete()

    # upstream
    assert d6tflow.run(t3)
    d6tflow.invalidate_upstream(t3, confirm=False)
    assert not all(t.complete() for t in [t1,t2,t3])

def test_preview():
    t1=Task1(); t2=Task2();t3=Task3();
    d6tflow.invalidate_upstream(t3, confirm=False)

    import io
    from contextlib import redirect_stdout

    with io.StringIO() as buf, redirect_stdout(buf):
        d6tflow.preview(t3)
        output = buf.getvalue()
        assert output.count('PENDING')==3
        assert output.count('COMPLETE')==0

    with io.StringIO() as buf, redirect_stdout(buf):
        d6tflow.run(t3)
        d6tflow.preview(t3)
        output = buf.getvalue()
        assert output.count('PENDING')==0
        assert output.count('COMPLETE')==3

    with io.StringIO() as buf, redirect_stdout(buf):
        d6tflow.preview(Task3(do_preprocess=False))
        output = buf.getvalue()
        assert output.count('PENDING')==1
        assert output.count('COMPLETE')==2

def test_dynamic():

    class TaskCollector(d6tflow.tasks.TaskAggregator):
        def run(self):
            yield Task1()
            yield Task2()

    d6tflow.run(TaskCollector())
    assert Task1().complete() and Task2().complete() and TaskCollector().complete()
    assert TaskCollector().outputLoad()[0].equals(Task1().outputLoad())
    assert TaskCollector().outputLoad()[1][0].equals(Task2().outputLoad()[0])
    TaskCollector().invalidate(confirm=False)
    assert not (Task1().complete() and Task2().complete() and TaskCollector().complete())

