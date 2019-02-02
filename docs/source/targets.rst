Managing Tasks
==============================================

What are targets?
------------------------------------------------------------

Targets are objects which load and save the actual data. They come in many formats:

* save to parquet, load as pandas
* save to JSON, load as python dict
* save to pickle, load as python list
* many more

Dask Targets
------------------------------------------------------------

If you have dask installed you can define Dask Targets.

.. code-block:: python

    from d6tflow.tasks.dask import PqDaskTarget


Writing Your Own Targets
------------------------------------------------------------

This is often relatively simple since you mostly need to implement `load()` and `save()` functions. For more advanced cases you also have to implement `exist()` and `invalidate()` functions. Check the source code for details or raise an issue.
