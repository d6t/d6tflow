from d6tflow.targets import DataTarget
import torch


class PyTorchModel(DataTarget):


    def load(self, cached=False, **kwargs):
        """
        Load saved model

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_parquet

        Returns: pandas dataframe

        """
        return super().load(torch.load, cached, **kwargs)



    def save(self, model, **kwargs):
        """
        Save torch model

        Args:
            model (obj): python object
            kwargs : additional arguments to pass to torch.save

        Returns: filename

        """

        (self.path).parent.mkdir(parents=True, exist_ok=True)
        torch.save(model, self.path, **kwargs)
        return self.path