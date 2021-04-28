
import d6tflow

# define workflow
class GetData(d6tflow.tasks.TaskPqPandas):

    def run(self):
        data = get_data()
        self.save(data) # quickly save output data


@d6tflow.requires(GetData)  # define dependency
class ModelTrain(d6tflow.tasks.TaskPickle):
    model = d6tflow.Parameter(default='ols')  # parameter for model selection

    def run(self):
        data = self.inputLoad() # quickly load training data

        if self.model=='regression':
            # [...]
        elif self.model=='deep learning':
            # [...]

        model.train(data)
        self.save(model)
        self.saveMeta({'score':model.score})

# goal: compare performance of two models
# define workflow manager
flow = d6tflow.WorkflowMulti(ModelTrain, \
        {'model1':{'model':'regression'}, 'model2':{'model':'deep learning'}})

# intelligently figures out which part of the workflow need to run for each model
# for example when training model 2, GetData() does not need to run again
flow.run()
'''
Scheduled 3 tasks of which:
* 3 ran successfully:
    - 1 GetData()
    - 1 ModelData(do_preprocess=True)
    - 1 ModelTrain(do_preprocess=True, model=ols)

# To run 2nd model, don't need to re-run all tasks, only the ones that changed
Scheduled 3 tasks of which:
* 1 complete ones were encountered:
    - 1 GetData()
* 2 ran successfully:
    - 1 ModelData(do_preprocess=False)
    - 1 ModelTrain(do_preprocess=False, model=gbm)

'''

data = flow.outputLoadAll()

scores = flow.outputLoadMeta()
print(scores)
# {'ols': {'score': 0.7406426641094095}, 'gbm': {'score': 0.9761405838418584}}
