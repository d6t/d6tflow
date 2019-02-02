import pandas as pd

from d6tflow.targets import DataTarget

class H5PandasTarget(DataTarget):
    def load(self, cached=False, **kwargs):
        opts = {**{'key':'data'},**kwargs}
        return super().load(pd.read_hdf, cached, **opts)

    def save(self, df, **kwargs):
        opts = {**{'key':'data'},**kwargs}
        return super().save(df, 'to_hdf', **opts)

