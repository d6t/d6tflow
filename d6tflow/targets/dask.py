import dask.dataframe as dd

from d6tflow.targets import DataTarget

class CSVDaskTarget(DataTarget):
    """
    Saves to CSV, loads to dask dataframe

    """
    def load(self, cached=False, **kwargs):
        """
        Load from csv to dask dataframe

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_csv

        Returns: dask dataframe

        """
        return super().load(dd.read_csv, cached, **kwargs)

    def save(self, df, **kwargs):
        """
        Save dataframe to csv

        Args:
            df (obj): dask dataframe
            kwargs : additional arguments to pass to df.to_csv

        Returns: filename

        """
        opts = {**{'index':False},**kwargs}
        return super().save(df, 'to_csv', **opts)

class PqDaskTarget(DataTarget):
    """
    Saves to parquet, loads to dask dataframe

    """
    def load(self, cached=False, **kwargs):
        """
        Load from parquet to dask dataframe

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_parquet

        Returns: dask dataframe

        """
        return super().load(dd.read_parquet, cached, **kwargs)

    def save(self, df, **kwargs):
        """
        Save dataframe to parquet

        Args:
            df (obj): dask dataframe
            kwargs : additional arguments to pass to df.to_parquet

        Returns: filename

        """
        opts = {**{'compression':'gzip'},**kwargs}
        return super().save(df, 'to_parquet', **opts)


