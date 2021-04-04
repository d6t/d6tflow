import d6tflow

class Task1(d6tflow.tasks.TaskCache):

    def run(self):
        print(self.param_kwargs)
        self.save({1:1})
        self.saveMeta({1:1})

d6tflow.set_dir('data/utest')
print(Task1().output().path)
print(d6tflow.settings.dir)