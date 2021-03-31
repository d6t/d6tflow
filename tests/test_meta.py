import d6tflow
import pandas as pd
import pytest
import pickle
import time


def test_metaSave_persists_after_run():
    class MetaSave(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
            self.metaSave({'metadata': df})

    ms = MetaSave()
    d6tflow.run(ms)

    metafile = f"meta-{ms.task_id.split('_')[-1]}.pickle"
    metadata = pickle.load(open("data/"+metafile, "rb"))
    assert metadata['metadata'].equals(
        pd.DataFrame({'a': range(3)}))
