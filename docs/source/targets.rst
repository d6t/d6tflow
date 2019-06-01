Task I/O Targets
==============================================

How is task data saved and loaded?
------------------------------------------------------------

Task data is saved in a file, database table or memory (cache). You can control how task output data is saved by . In the example below, data is saved as parquet and loaded as a pandas dataframe. The python object you want to save limits how you can save the data.

.. code-block:: python

    class YourTask(d6tflow.tasks.TaskPqPandas):


Core task targets
------------------------------------------------------------

What kind of object you want to save determines which Task class you need to use.

* pandas  
    * ``d6tflow.tasks.TaskPqPandas``: save to parquet, load as pandas 
    * ``d6tflow.tasks.TaskCachePandas``: save to memory, load as pandas 
    * ``d6tflow.tasks.TaskCSVPandas``: save to CSV, load as pandas 
    * ``d6tflow.tasks.TaskExcelPandas``: save to CSV, load as pandas
    * ``d6tflow.tasks.TaskSQLPandas``: save to SQL, load as pandas (premium, see below)
* dicts
    * ``d6tflow.tasks.TaskJson``: save to JSON, load as python dict
    * ``d6tflow.tasks.TaskPickle``: save to pickle, load as python list
    * **NB**: don't save a dict of pandas dataframes as pickle, instead save as multiple outputs, see "save more than one output" in :doc:`Tasks <../tasks>`
* any python object (eg trained models)
    * ``d6tflow.tasks.TaskPickle``: save to pickle, load as python list
    * ``d6tflow.tasks.TaskCache``: save to memory, load as python object
* dask: premium, see below


Additional Targets
------------------------------------------------------------

Database Targets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

d6tflow premium has database targets, request access at https://pipe.databolt.tech/gui/request-premium/

Dask Targets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

d6tflow premium has dask targets, request access at https://pipe.databolt.tech/gui/request-premium/

Keras Model Targets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For saving Keras model targets

.. code-block:: python

    from d6tflow.tasks.h5 import TaskH5Keras


Writing Your Own Targets
------------------------------------------------------------

This is often relatively simple since you mostly need to implement `load()` and `save()` functions. For more advanced cases you also have to implement `exist()` and `invalidate()` functions. Check the source code for details or raise an issue.
