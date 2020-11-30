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
            dft1, dft2 = self.inputLoad()
            assert dft1.equals(dft2)
    TaskMultiInput().run()

    class TaskMultiInput2(d6tflow.tasks.TaskCache):
        def requires(self):
            return {'in1':Task2(), 'in1':Task2()}
        def run(self):
            df2, df4 = self.inputLoad(task='in1')
            assert df2.equals(dfc2) and df4.equals(dfc4)
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

#********************************************
# pipe
#********************************************
import yaml
import d6tpipe

cfg = yaml.safe_load(open('tests/.creds.yml'))


@pytest.fixture
def cleanup_pipe():
    d6tpipe.api.ConfigManager(profile=cfg['d6tpipe_profile']).init({'filerepo': 'tests/d6tpipe-files1/'}, reset=True)
    api1 = d6tpipe.APIClient(profile=cfg['d6tpipe_profile'])
    api1.login(cfg['d6tpipe_username'],cfg['d6tpipe_password'])
    # d6tpipe.api.create_pipe_with_remote(api, {'name': cfg['d6tpipe_pipe1'], 'protocol': 'd6tfree'})
    # settings = {"user":cfg['d6tpipe_username2'],"role":"read"}
    # d6tpipe.create_or_update_permissions(api1, cfg['d6tpipe_pipe1'], settings)

    pipe1 = d6tpipe.Pipe(api1, cfg['d6tpipe_pipe1'])
    pipe1.delete_files_local(confirm=False,delete_all=True)

    d6tpipe.api.ConfigManager(profile=cfg['d6tpipe_profile2']).init({'filerepo': 'tests/d6tpipe-files2/'}, reset=True)
    api2 = d6tpipe.APIClient(profile=cfg['d6tpipe_profile2'])
    api2.login(cfg['d6tpipe_username2'],cfg['d6tpipe_password2'])
    # d6tpipe.api.create_pipe_with_remote(api2, {'name': cfg['d6tpipe_pipe2'], 'protocol': 'd6tfree'})
    pipe12 = d6tpipe.Pipe(api2, cfg['d6tpipe_pipe1'])
    pipe2 = d6tpipe.Pipe(api2, cfg['d6tpipe_pipe2'])
    pipe12.delete_files_local(confirm=False,delete_all=True)
    pipe2.delete_files_local(confirm=False,delete_all=True)

    yield api1, api2

    @fuckit
    def delhelper():
        pipe1.delete_files_local(confirm=False, delete_all=True)
        pipe12.delete_files_local(confirm=False, delete_all=True)
        pipe2.delete_files_local(confirm=False, delete_all=True)

    delhelper()

def test_pipes_base(cleanup_pipe):
    import d6tflow.pipes
    d6tflow.pipes.init(cfg['d6tpipe_pipe1'],profile=cfg['d6tpipe_profile'])

    t1 = Task1()
    pipe1 = d6tflow.pipes.get_pipe()
    pipedir = pipe1.dirpath
    t1filepath = t1.output().path
    t1file = str(PurePosixPath(t1filepath.relative_to(pipedir)))

    assert d6tflow.run(t1)
    assert t1.complete()
    with fuckit:
        pipe1._pullpush_luigi([t1file], op='remove')
    assert pipe1.push_preview()==[t1file]
    assert pipe1.push()==[t1file]
    assert pipe1.scan_remote(cached=False) == [t1file]
    # cleanup
    pipe1.delete_files(confirm=False, all_local=True)
    assert pipe1.scan_remote(cached=False) == []

def test_pipes_advanced(cleanup_pipe):
    import d6tflow.pipes
    d6tflow.pipes.init(cfg['d6tpipe_pipe1'],profile=cfg['d6tpipe_profile'], local_pipe=True, reset=True)
    assert 'Local' in d6tflow.pipes.get_pipe().__class__.__name__
    d6tflow.pipes.init(cfg['d6tpipe_pipe1'],profile=cfg['d6tpipe_profile'], reset=True)


    class Task1(d6tflow.tasks.TaskPqPandas):
        def run(self):
            self.save(df)

    t1 = Task1()
    pipe1 = t1.get_pipe()
    pipedir = pipe1.dirpath
    t1filepath = t1.output().path
    t1file = str(PurePosixPath(t1filepath.relative_to(pipedir)))

    d6tflow.preview(t1)
    assert d6tflow.run(t1)
    assert t1.complete()

    with fuckit:
        pipe1._pullpush_luigi([t1file], op='remove')

    assert pipe1.scan_remote(cached=False) == []
    assert t1.pull_preview()==[]
    assert t1.push_preview()==[t1file]
    assert d6tflow.pipes.all_push_preview(t1) == {cfg['d6tpipe_pipe1']:[t1file]}
    assert d6tflow.pipes.all_push(t1) == {cfg['d6tpipe_pipe1']:[t1file]}

    class Task1(d6tflow.tasks.TaskPqPandas):
        external = True
        pipename = cfg['d6tpipe_pipe1']

    class Task2(d6tflow.tasks.TaskPqPandas):
        persist = ['df2', 'df4']
        def requires(self):
            return Task1()
        def run(self):
            df2fun(self)

    import importlib
    importlib.reload(d6tflow)
    importlib.reload(d6tflow.pipes)
    d6tflow.cache.pipes={}
    d6tflow.pipes.init(cfg['d6tpipe_pipe2'],profile=cfg['d6tpipe_profile2'],reset=True)
    t1 = Task1()
    assert t1.get_pipename()==cfg['d6tpipe_pipe1']
    assert not t1.complete()
    assert t1.pull_preview()==[str(t1file)]
    assert d6tflow.pipes.all_pull_preview(t1) == {cfg['d6tpipe_pipe1']:[t1file]}
    assert t1.pull()==[str(t1file)]
    assert t1.complete()
    assert t1.output().load().equals(df)

    t2 = Task2()
    d6tflow.show([t2])
    assert d6tflow.run([t2]) # run as list

    pipe2 = t2.get_pipe()
    pipedir = t2.get_pipe().dirpath
    # assert False
    t2files = [str(PurePosixPath(p.path.relative_to(pipedir))) for p in t2.output().values()]

    assert d6tflow.pipes.all_push_preview(t2) == {cfg['d6tpipe_pipe2']:t2files}

    # cleanup
    pipe1._pullpush_luigi([t1file], op='remove')
    assert pipe1.scan_remote(cached=False) == []

