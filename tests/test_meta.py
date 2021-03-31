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

    # From disk
    metafile = f"meta-{ms.task_id.split('_')[-1]}.pickle"
    metadata = pickle.load(open("data/"+metafile, "rb"))
    assert metadata['metadata'].equals(
        pd.DataFrame({'a': range(3)})
    )

    # From object
    assert ms.metadata['metadata'].equals(
        pd.DataFrame({'a': range(3)})
    )

def test_metaLoad_single_input():
    class MetaSave(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
            self.metaSave({'metadata': df})
    
    @d6tflow.requires(MetaSave)
    class MetaLoad(d6tflow.tasks.TaskCache):      
        def run(self):
            meta = self.metaLoad()
            assert meta['metadata'].equals(
                pd.DataFrame({'a': range(3)})
            )
    
    d6tflow.run(MetaLoad())

def test_metaLoad_multiple_input():
    class MetaSave(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
            self.metaSave({'metadata': df})
    
    class MetaSave2(MetaSave):
        pass

    @d6tflow.requires({'upstream1': MetaSave, 'upstream2': MetaSave2})
    class MetaLoad(d6tflow.tasks.TaskCache):      
        def run(self):
            meta = self.metaLoad()
            assert meta['upstream1']['metadata'].equals(
                pd.DataFrame({'a': range(3)})
            )
            assert meta['upstream2']['metadata'].equals(
                pd.DataFrame({'a': range(3)})
            )
    
    d6tflow.run(MetaLoad())

def test_metaLoad_multiple_input_tuple():
    class MetaSave(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
            self.metaSave({'metadata': df})
    
    class MetaSave2(MetaSave):
        pass

    @d6tflow.requires(MetaSave, MetaSave2)
    class MetaLoad(d6tflow.tasks.TaskCache):      
        def run(self):
            meta = self.metaLoad()
            assert meta[0]['metadata'].equals(
                pd.DataFrame({'a': range(3)})
            )
            assert meta[1]['metadata'].equals(
                pd.DataFrame({'a': range(3)})
            )
    
    d6tflow.run(MetaLoad())