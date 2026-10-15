import pandas as pd
import json
import pickle
import pathlib
import markdown

#

from d6tflow import core
from d6tflow.core import Target
from d6tflow.cache import data as cache
import d6tflow.settings as settings
import d6tflow.utils


class CacheTarget(core.LocalTarget):
    """
    Saves to in-memory cache, loads to python object

    """
    def __init__(self, path=None):
        super().__init__(path)
        # store as pathlib.Path so cache keys / outputPath match file-based targets
        self.path = pathlib.Path(path)

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

class _LocalPathTarget(core.LocalTarget):
    """
    Local target with `self.path` as `pathlib.Path()`

    """

    def __init__(self, path=None):
        super().__init__(path)
        if settings.cloud_fs_enabled:
            import upath
            self.path = upath.UPath(path)
        else:
            self.path = pathlib.Path(path)
        (self.path).parent.mkdir(parents=True, exist_ok=True)

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

class CSVGZPandasTarget(CSVPandasTarget):
    """
    Saves to CSV gzip, loads to pandas dataframe

    """

    
    def save(self, df, **kwargs):
        """
        Save dataframe to csv gzip

        Args:
            df (obj): pandas dataframe
            kwargs : additional arguments to pass to df.to_csv

        Returns: filename

        """
        opts = {**{'index':False, 'compression':'gzip'},**kwargs}
        return super().save(df, **opts)

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

class ExcelPandasSheetsTarget(_LocalPathTarget):
    """
    Saves dict of dataframes as sheets in a single Excel file, loads selectively by sheet

    """
    def load(self, keys=None, cached=False, **kwargs):
        """
        Load sheets from Excel file

        Args:
            keys (str/list): sheet name(s) to load. None loads all sheets
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_excel

        Returns: dict of dataframes, single dataframe, or filtered dict

        """
        if self.exists():
            if not cached or not settings.cached or self.path not in cache:
                sheet_name = keys if keys is not None else None
                data = pd.read_excel(self.path, sheet_name=sheet_name, **kwargs)
                if cached or settings.cached:
                    cache[self.path] = data
                return data
            else:
                data = cache.get(self.path)
                if keys is None:
                    return data
                if isinstance(keys, str):
                    return data[keys]
                return {k: v for k, v in data.items() if k in keys}
        else:
            raise RuntimeError('Target does not exist, make sure task is complete')

    def save(self, data, **kwargs):
        """
        Save dict of dataframes as sheets in a single Excel file

        Args:
            data (dict): {sheet_name: dataframe}
            kwargs: additional arguments to pass to df.to_excel

        Returns: filename

        """
        opts = {**{'index': False}, **kwargs}
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(self.path, engine='openpyxl') as writer:
            for sheet_name, df in data.items():
                df.to_excel(writer, sheet_name=sheet_name, **opts)
        return self.path

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
            with path.open('r') as fhandle:
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
            with path.open('w') as fhandle:
                json.dump(_dict_, fhandle, **opts)
        opts = {**{'indent':4},**kwargs}
        write_json(self.path, {'data':dict_}, **opts)
        return self.path


class MarkdownTarget(DataTarget):
    """
    Saves to markdown (.md) and HTML (.html), loads markdown string

    """
    def load(self, cached=False, **kwargs):
        """
        Load from markdown file to string

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to read function

        Returns: markdown string

        """
        def read_md(path, **opts):
            with path.open('r', encoding='utf-8') as fhandle:
                return fhandle.read()
        return super().load(read_md, cached, **kwargs)

    def save(self, md_string, **kwargs):
        """
        Save markdown string to .md and .html files

        Args:
            md_string (str): markdown string
            kwargs : additional arguments to pass to markdown.markdown

        Returns: filename

        """
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('w', encoding='utf-8') as fhandle:
            fhandle.write(md_string)
        html_path = self.path.with_suffix('.html')
        html_body = markdown.markdown(md_string, extensions=['tables'], **kwargs)
        html_string = (
            '<!DOCTYPE html>\n'
            '<html>\n'
            '<head>\n'
            '<meta charset="utf-8">\n'
            '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/github-markdown-css/5.8.1/github-markdown.css">\n'
            '</head>\n'
            '<body>\n'
            '<article class="markdown-body">\n'
            f'{html_body}\n'
            '</article>\n'
            '</body>\n'
            '</html>\n'
        )
        with html_path.open('w', encoding='utf-8') as fhandle:
            fhandle.write(html_string)
        return self.path

    def invalidate(self):
        html_path = self.path.with_suffix('.html')
        if html_path.exists():
            html_path.unlink()
        if self.exists():
            self.path.unlink()
        return not self.exists()


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
        def funload(x):
            with x.open("rb" ) as fhandle:
                data = pickle.load(fhandle)
            return data
        return super().load(funload, cached, **kwargs)

    
    def save(self, obj, **kwargs):
        """
        Save obj to pickle

        Args:
            obj (obj): python object
            kwargs : additional arguments to pass to pickle.dump

        Returns: filename

        """
        with self.path.open("wb") as fhandle:
            pickle.dump(obj, fhandle, **kwargs)
        return self.path

