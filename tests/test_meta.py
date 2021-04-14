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
    metadata = pickle.load(open(ms._get_meta_path(ms), "rb"))
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


def test_outputLoadMeta():
    class MetaSave(d6tflow.tasks.TaskCache):
        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
            self.metaSave({'metadata': df})

    d6tflow.run(MetaSave())
    df = pd.DataFrame({'a': range(3)})
    assert MetaSave().outputLoadMeta()["metadata"].equals(df)


def test_outputLoadAllMeta():
    class Task1(d6tflow.tasks.TaskCache):

        def run(self):
            df = pd.DataFrame({'a': range(3)})
            self.save(df)  # quickly save dataframe
            self.metaSave({'columns': 42})

    class Task2(Task1):
        pass

    @d6tflow.requires({'upstream1': Task1, 'upstream2': Task2})
    class Task3(d6tflow.tasks.TaskCache):
        multiplier = d6tflow.IntParameter(default=2)

        def run(self):
            meta = self.metaLoad()['upstream1']
            print(meta)
            print(meta['columns'])

            df1 = self.input()['upstream1'].load()  # quickly load input data
            df2 = self.input()['upstream2'].load()  # quickly load input data
            df = df1.join(df2, lsuffix='1', rsuffix='2')
            df['b'] = df['a1']*self.multiplier  # use task parameter
            self.save(df)
            self.metaSave({'columns': 100})

    d6tflow.run(Task3())
    meta_all = Task3().outputLoadAllMeta()
    assert meta_all["Task1"]["columns"] == 42
    assert meta_all["Task2"]["columns"] == 42
    assert meta_all["Task3"]["columns"] == 100
