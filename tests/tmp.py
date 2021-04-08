import d6tflow

class Task1(d6tflow.tasks.TaskPickle):
    def run(self):
        self.save({1:1})
class Task2(d6tflow.tasks.TaskPickle):
    def run(self):
        self.save({1:1})

path = 'data/data2/'
# assert 'data2' in str(Task1(path=path).output().path)
flow = d6tflow.Workflow(Task1, path=path)
flow.run()
assert 'data2' in str(flow.get_task().output().path)
flow2 = d6tflow.WorkflowMulti(Task1, params={0: {}}, path=path)
print(flow2.get_task()[0].output().path)
# assert 'data2' in str(flow.get_task()[0].output().path)
