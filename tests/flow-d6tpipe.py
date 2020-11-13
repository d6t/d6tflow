
#********************************************
# pipe
#********************************************
import yaml
import d6tpipe
import pytest
import fuckit
from main import *
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
