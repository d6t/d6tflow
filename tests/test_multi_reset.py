import pytest

import d6tflow


@pytest.mark.parametrize('direction', ['upstream', 'downstream'])
def test_multi_reset_selected_flow(tmp_path, direction):
    class Source(d6tflow.tasks.TaskJson):
        value = d6tflow.IntParameter()

        def run(self):
            self.save(self.value)

    @d6tflow.requires(Source)
    class Destination(d6tflow.tasks.TaskJson):
        def run(self):
            self.save(self.inputLoad() * 2)

    flow = d6tflow.WorkflowMulti(Destination, params={'one': {'value': 1}, 'two': {'value': 2}}, path=tmp_path)
    flow.run()
    if direction == 'upstream':
        flow.reset_upstream(Destination, flow='one', confirm=False)
        assert not flow.get_task(Source, flow='one').complete()
        assert not flow.get_task(Destination, flow='one').complete()
    else:
        flow.reset_downstream(Source, flow='one', confirm=False)
        assert not flow.get_task(Source, flow='one').complete()
        assert not flow.get_task(Destination, flow='one').complete(cascade=False)
    assert flow.get_task(flow='two').complete()
