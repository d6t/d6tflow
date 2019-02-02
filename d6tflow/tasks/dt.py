from d6tflow.tasks import TaskData
from d6tflow.targets.dt import DatatableTarget

class TaskDatatable(TaskData):
    """
    Task which saves to H2O data.table
    """
    target_class = DatatableTarget
    target_ext = 'nff'

