from d6tflow.tasks import TaskData
from d6tflow.targets.dask import PqDaskTarget, CSVDaskTarget

class TaskPqDask(TaskData):
    """
    Task which saves to parquet
    """
    target_class = PqDaskTarget
    target_ext = 'pq'

class TaskCSVDask(TaskData):
    """
    Task which saves to CSV
    """
    target_class = CSVDaskTarget
    target_ext = 'csv'
