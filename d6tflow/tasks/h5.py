from d6tflow.tasks import TaskData
from d6tflow.targets.h5 import H5PandasTarget

class TaskH5Pandas(TaskData):
    """
    Task which saves to HDF5
    """
    target_class = H5PandasTarget
    target_ext = 'hdf5'
