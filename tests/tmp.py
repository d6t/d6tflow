import d6tflow

class Task1(d6tflow.tasks.TaskCache):

    def run(self):
        print(self.task_id,self.task_id.split('_')[-1])
        self.save({self.task_id:self.task_family[-1]})
        self.metaSave({'a': 1})


@d6tflow.requires(Task1)
class Task2(d6tflow.tasks.TaskCache):
    multiplier2 = d6tflow.IntParameter(default=2)

    def run(self):
        print(self.task_id,self.task_id.split('_')[-1])
        self.save({self.task_id:self.task_family[-1]})
        self.metaSave({'b': 2})


# define another task that depends on data from task1 and task2
@d6tflow.requires(Task2)
class Task3(d6tflow.tasks.TaskCache):
    multiplier3 = d6tflow.IntParameter(default=2)

    def run(self):
        print(self.task_id,self.task_id.split('_')[-1])
        print(self.metaLoad())
        # print(self.metaLoad()['a'])
        self.metaSave({'c': 3})
        self.save({self.task_id:self.task_family[-1]})

class Task4(d6tflow.tasks.TaskCache):
    persist=['a','b']

d6tflow.run(Task3())

assert Task2().outputLoad()=={'Task2':'2'}
assert Task2().outputLoadMeta()=={'b': 2} # if self.complete()
assert Task3().outputLoad()=={'Task3':'3'}
assert Task3().outputLoadMeta()=={'c': 3}

assert Task2().output().pathmeta == Task4()._getpath(d6tflow.settings.dirpath,'meta').with_suffix('.pickle') # 'data/Task4/Task4__99914b932b-meta.pickle'
