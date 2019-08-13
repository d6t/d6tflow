import luigi
import pandas as pd
import json
import pickle
import pathlib

#import d6tcollect

from d6tflow.cache import data as cache
import d6tflow.settings as settings
import d6tflow.utils

class CacheTarget(luigi.LocalTarget):
    """
    Saves to in-memory cache, loads to python object

    """
    def exists(self):
        return self.path in cache

    def invalidate(self):
        if self.path in cache:
            cache.pop(self.path)

    def load(self, cached=True):
        """
        Load from in-memory cache

        Returns: python object

        """
        if self.exists():
            return cache.get(self.path)
        else:
            raise RuntimeError('Target does not exist, make sure task is complete')

    def save(self, df):
        """
        Save dataframe to in-memory cache

        Args:
            df (obj): pandas dataframe

        Returns: filename

        """
        cache[self.path] = df
        return self.path

class PdCacheTarget(CacheTarget):
    pass

class _LocalPathTarget(luigi.LocalTarget):
    """
    Local target with `self.path` as `pathlib.Path()`

    """

    def __init__(self, path=None):
        super().__init__(path)
        self.path = pathlib.Path(path)

    def exists(self):
        return self.path.exists()

    def invalidate(self):
        if self.exists():
            self.path.unlink()
        return not self.exists()

class DataTarget(_LocalPathTarget):
    """
    Local target which saves in-memory data (eg dataframes) to persistent storage (eg files) and loads from storage to memory

    This is an abstract class that you should extend.

    """
    def load(self, fun, cached=False, **kwargs):
        """
        Runs a function to load data from storage into memory

        Args:
            fun (function): loading function
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to `fun`

        Returns: data object

        """
        if self.exists():
            if not cached or not settings.cached or self.path not in cache:
                opts = {**{},**kwargs}
                df = fun(self.path, **opts)
                if cached or settings.cached:
                    cache[self.path] = df
                return df
            else:
                return cache.get(self.path)
        else:
            raise RuntimeError('Target does not exist, make sure task is complete')

    def save(self, df, fun, **kwargs):
        """
        Runs a function to save data from memory into storage

        Args:
            df (obj): data to save
            fun (function): saving function
            **kwargs: arguments to pass to `fun`

        Returns: filename

        """
        fun = getattr(df, fun)
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        fun(self.path, **kwargs)
        return self.path

class CSVPandasTarget(DataTarget):
    """
    Saves to CSV, loads to pandas dataframe

    """
    def load(self, cached=False, **kwargs):
        """
        Load from csv to pandas dataframe

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_csv

        Returns: pandas dataframe

        """
        return super().load(pd.read_csv, cached, **kwargs)

    def save(self, df, **kwargs):
        """
        Save dataframe to csv

        Args:
            df (obj): pandas dataframe
            kwargs : additional arguments to pass to df.to_csv

        Returns: filename

        """
        opts = {**{'index':False},**kwargs}
        return super().save(df, 'to_csv', **opts)

class ExcelPandasTarget(DataTarget):
    """
    Saves to Excel, loads to pandas dataframe

    """
    def load(self, cached=False, **kwargs):
        """
        Load from Excel to pandas dataframe

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_csv

        Returns: pandas dataframe

        """
        return super().load(pd.read_excel, cached, **kwargs)

    def save(self, df, **kwargs):
        """
        Save dataframe to Excel

        Args:
            df (obj): pandas dataframe
            kwargs : additional arguments to pass to df.to_csv

        Returns: filename

        """
        opts = {**{'index':False},**kwargs}
        return super().save(df, 'to_excel', **opts)

class PqPandasTarget(DataTarget):
    """
    Saves to parquet, loads to pandas dataframe

    """
    def load(self, cached=False, **kwargs):
        """
        Load from parquet to pandas dataframe

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_parquet

        Returns: pandas dataframe

        """
        return super().load(pd.read_parquet, cached, **kwargs)

    def save(self, df, **kwargs):
        """
        Save dataframe to parquet

        Args:
            df (obj): pandas dataframe
            kwargs : additional arguments to pass to df.to_parquet

        Returns: filename

        """
        opts = {**{'compression':'gzip','engine':'pyarrow'},**kwargs}
        return super().save(df, 'to_parquet', **opts)


class JsonTarget(DataTarget):
    """
    Saves to json, loads to dict

    """
    def load(self, cached=False, **kwargs):
        """
        Load from json to dict

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to json.load

        Returns: dict

        """
        def read_json(path, **opts):
            with open(path, 'r') as fhandle:
                df = json.load(fhandle)
            return df['data']
        return super().load(read_json, cached, **kwargs)

    def save(self, dict_, **kwargs):
        """
        Save dict to json

        Args:
            dict_ (dict): python dict
            kwargs : additional arguments to pass to json.dump

        Returns: filename

        """
        def write_json(path, _dict_, **opts):
            with open(path, 'w') as fhandle:
                json.dump(_dict_, fhandle, **opts)
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        opts = {**{'indent':4},**kwargs}
        write_json(self.path, {'data':dict_}, **opts)
        return self.path


class PickleTarget(DataTarget):
    """
    Saves to pickle, loads to python obj

    """
    def load(self, cached=False, **kwargs):
        """
        Load from pickle to obj

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pickle.load

        Returns: dict

        """
        return super().load(lambda x: pickle.load(open(x,"rb" )), cached, **kwargs)

    def save(self, obj, **kwargs):
        """
        Save obj to pickle

        Args:
            obj (obj): python object
            kwargs : additional arguments to pass to pickle.dump

        Returns: filename

        """
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        pickle.dump(obj, open(self.path, "wb"), **kwargs)
        return self.path

class MatplotlibTarget(_LocalPathTarget):
    """
    Saves to png. Does not load
    """
    def load(self):
        raise RuntimeError('Images can only be saved not loaded')

    def save(self, obj, **kwargs):
        """
        Save obj to pickle and png

        Args:
            obj (obj): python object
            plotkwargs (dict): additional arguments to plt.savefig()
            kwargs : additional arguments to pass to pickle.dump

        Returns: filename

        """
        fig = obj.get_figure()
        fig.savefig(self.path, **kwargs)
        return self.path

