import datatable as dt

from d6tflow.targets import DataTarget

class DatatableTarget(DataTarget):
    def load(self, cached=False, **kwargs):
        opts = {**{},**kwargs}
        return super().load(dt.open, cached, **opts)

    def save(self, df, **kwargs):
        opts = {**{},**kwargs}
        return super().save(df, 'save', **opts)

