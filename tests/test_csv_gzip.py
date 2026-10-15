import gzip

import pandas as pd

import d6tflow


def test_csv_gzip_task_roundtrip(tmp_path):
    expected = pd.DataFrame({'value': [1, 2]})

    class Compressed(d6tflow.tasks.TaskCSVGZPandas):
        def run(self):
            self.save(expected)

    flow = d6tflow.Workflow(Compressed, path=tmp_path)
    assert flow.run(execution_summary=False)
    pd.testing.assert_frame_equal(flow.outputLoad(), expected)
    with gzip.open(flow.outputPath(), 'rt') as fh:
        assert fh.read() == 'value\n1\n2\n'
