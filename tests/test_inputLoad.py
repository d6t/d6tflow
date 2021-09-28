import d6tflow
import pandas as pd
import pytest

# These classes are the dependencies to our test cases

class TaskSingleOutput(d6tflow.tasks.TaskPqPandas):
    def run(self):
        self.save(pd.DataFrame({'a': range(3)}))


class TaskSingleOutput1(d6tflow.tasks.TaskPqPandas):
    def run(self):
        self.save(pd.DataFrame({'b': range(3)}))


class TaskSingleOutput2(d6tflow.tasks.TaskPqPandas):
    def run(self):
        self.save(pd.DataFrame({'c': range(3)}))


class TaskMultipleOutput(d6tflow.tasks.TaskPqPandas):
    persist = ["output1", "output2"]

    def run(self):
        x = pd.DataFrame({'ax': range(3)})
        y = pd.DataFrame({'ay': range(3)})
        self.save({"output1": x, "output2": y})

class TaskMultipleOutput1(d6tflow.tasks.TaskPqPandas):
    persist = ["output1", "output2"]

    def run(self):
        x = pd.DataFrame({'ax': range(3)})
        y = pd.DataFrame({'ay': range(3)})
        self.save({"output1": x, "output2": y})

class TaskMultipleOutput2(d6tflow.tasks.TaskPqPandas):
    persist = ["output1", "output2"]

    def run(self):
        x = pd.DataFrame({'ax': range(3)})
        y = pd.DataFrame({'ay': range(3)})
        self.save({"output1": x, "output2": y})


# single dependency, single output
def test_task_inputLoad_single_single():
    @d6tflow.requires(TaskSingleOutput)
    class TaskSingleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data = self.inputLoad()
            assert data.equals(pd.DataFrame({'a': range(3)}))

    d6tflow.run(TaskSingleInput(), forced_all=True,
                forced_all_upstream=True, confirm=False)


# single dependency, multiple outputs
def test_task_inputLoad_single_multiple():
    @d6tflow.requires(TaskMultipleOutput)
    class TaskSingleInput2(d6tflow.tasks.TaskPqPandas):
        def run(self):
            output1, output2 = self.inputLoad()
            assert output1.equals(pd.DataFrame({'ax': range(3)}))
            assert output2.equals(pd.DataFrame({'ay': range(3)}))

            outputDict = self.inputLoad(as_dict=True)
            assert outputDict['output1'].equals(pd.DataFrame({'ax': range(3)}))
            assert outputDict['output2'].equals(pd.DataFrame({'ay': range(3)}))

    d6tflow.run(TaskSingleInput2(), forced_all=True,
                forced_all_upstream=True, confirm=False)


# multiple dependencies, single output
def test_task_inputLoad_multiple_single():
    @d6tflow.requires({'input1': TaskSingleOutput1, 'input2': TaskSingleOutput2})
    class TaskMultipleInput(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data1 = self.inputLoad()['input1']
            assert(data1.equals(pd.DataFrame({'b': range(3)})))
            data2 = self.inputLoad()['input2']
            assert(data2.equals(pd.DataFrame({'c': range(3)})))

    d6tflow.run(TaskMultipleInput(), forced_all=True,
                forced_all_upstream=True, confirm=False)


# multiple dependencies, multiple outputs
def test_task_inputLoad_multiple_multiple():
    @d6tflow.requires({'input1': TaskMultipleOutput1, 'input2': TaskMultipleOutput2})
    class TaskMultipleInput2(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data = self.inputLoad(as_dict=True)
            assert data["input1"]["output1"].equals(data["input2"]["output1"])
            assert data["input1"]["output2"].equals(data["input2"]["output2"])

            data1a, data1b = self.inputLoad()["input1"]
            data2a, data2b = self.inputLoad()["input2"]

            assert data1a.equals(data2a)
            assert data1b.equals(data2b)

            data1a, data1b = self.inputLoad(task='input1')
            data2a, data2b = self.inputLoad(task='input2')
            assert data1a.equals(data2a)
            assert data1b.equals(data2b)

            data1 = self.inputLoad(task='input1', as_dict=True)
            data2 = self.inputLoad(task='input2', as_dict=True)
            assert data1["output1"].equals(data2["output1"])
            assert data1["output2"].equals(data2["output2"])

    d6tflow.run(TaskMultipleInput2(), forced_all=True,
                forced_all_upstream=True, confirm=False)

# multiple dependencies, multiple outputs (Tuples)
def test_task_inputLoad_multiple_multiple_tuple():
    @d6tflow.requires(TaskMultipleOutput1, TaskMultipleOutput2)
    class TaskMultipleInput2(d6tflow.tasks.TaskPqPandas):
        def run(self):
            data = self.inputLoad(as_dict=True)
            assert data[0]["output1"].equals(data[1]["output1"])
            assert data[0]["output2"].equals(data[1]["output2"])

            assert isinstance(self.inputLoad(), list)
            data1a, data1b = self.inputLoad()[0]
            data2a, data2b = self.inputLoad()[1]

            assert data1a.equals(data2a)
            assert data1b.equals(data2b)

            data1a, data1b = self.inputLoad(task=0)
            data2a, data2b = self.inputLoad(task=1)
            assert data1a.equals(data2a)
            assert data1b.equals(data2b)

            data1 = self.inputLoad(task=0, as_dict=True)
            data2 = self.inputLoad(task=1, as_dict=True)
            assert data1["output1"].equals(data2["output1"])
            assert data1["output2"].equals(data2["output2"])

    d6tflow.run(TaskMultipleInput2(), forced_all=True,
                forced_all_upstream=True, confirm=False)