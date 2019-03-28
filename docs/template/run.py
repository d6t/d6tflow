import d6tflow
import cfg, tasks, visualize

# Check task dependencies and their execution status
d6tflow.show(tasks.TaskTrain())

# Execute the model training task including dependencies
d6tflow.run(tasks.TaskTrain())

# use output
visualize.accuracy()

# change parameter and rerun
d6tflow.run(tasks.TaskTrain(do_preprocess=False))
visualize.accuracy()

# rerun flow after code changes
import importlib
importlib.reload(cfg)
importlib.reload(tasks)

# say you changed TaskGetData, reset all tasks depending on TaskGetData
d6tflow.invalidate_downstream(TaskGetData(), TaskTrain())

d6tflow.show(tasks.TaskTrain())
d6tflow.run(tasks.TaskTrain())
