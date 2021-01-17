from d6tflow.tasks import TaskData
from d6tflow.targets import PyTorchModel

class PyTorch(TaskData):
    """
    Task which saves to .pt models
    """
    target_class = PyTorchModel
    target_ext = '.pt'

