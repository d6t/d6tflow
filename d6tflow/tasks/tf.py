from d6tflow.tasks import TaskData
from d6tflow.targets.h5 import H5KerasTarget

class TaskH5Keras(TaskData):
    """
    Task which saves to HDF5
    """
    target_class = H5KerasTarget
    target_ext = 'hdf5'
