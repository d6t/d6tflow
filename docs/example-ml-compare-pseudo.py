
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

        if self.model=='regression': # choose model to train
            # [...]
        elif self.model=='deep learning':
            # [...]

        model.train(data)
        self.save(model)  # save trained model
        self.saveMeta({'score':model.score})  # save additional experiment data

# goal: compare performance of two models
# define workflow manager
flow = d6tflow.WorkflowMulti(ModelTrain, 
        {'model1':{'model':'regression'}, 'model2':{'model':'deep learning'}})

# intelligently figures out which part of the workflow need to run for each model
# for example when training model 2, GetData() does not need to run again
flow.run()
'''
Scheduled 2 tasks of which:
* 2 ran successfully:
    - 1 GetData()
    - 1 ModelTrain(model=regression)

# To run 2nd model, don't need to re-run all tasks, only the ones that changed
Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 GetData()
* 1 ran successfully:
    - 1 ModelTrain(model=deep learning)

'''

scores = flow.outputLoadMeta()
# {'ols': {'regression': 0.7406426641094095}, 'deep learning': {'score': 0.9761405838418584}}

data = flow.outputLoad(task=GetData)  # quickly load training data
models = flow.outputLoad(task=ModelTrain)  # quickly load trained models
