import pandas as pd
from tensorflow.keras.models import load_model
from d6tflow.targets import DataTarget
import d6tcollect

class H5KerasTarget(DataTarget):
    def load(self, cached=False, **kwargs):
        return super().load(load_model, cached, **kwargs)

    @d6tcollect._collectClass
    def save(self, df, **kwargs):
        return super().save(df, 'save', **kwargs)
