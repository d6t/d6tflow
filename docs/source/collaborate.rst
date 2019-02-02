Sharing Workflows and Outputs
==============================================

d6tpipe Integration
------------------------------------------------------------

d6tflow integrates with [d6tpipe](https://github.com/d6t/d6tpipe) which makes sharing flow output and handing projects off to others very seamless. Some cases when you want to do this include:

* data engineers share data with data scientists
* vendors sharing data with clients
* teachers sharing data with students

Sharing Flow output
------------------------------------------------------------


.. code-block:: python

    import d6tflow.pipes
    d6tflow.pipes.init('your-task-output') # save flow output to pipe

    # upload task output
    TaskGenerateData().push_preview() # preview
    d6tflow.pipes.all_push(TaskGenerateData()) # push data from all downstream tasks

    class TaskOthers(d6tflow.tasks.TaskPqPandas):
        external = True
        pipename = 'your-task-output'

    TaskOthers().pull_preview() # get task data from external
    d6tflow.pipes.all_pull(TaskOthers()) # pull data for all downstream tasks

    pipe = d6tflow.pipes.get_pipe() # default pipe
    pipe = d6tflow.pipes.get_pipe(TaskOthers().pipename) # task-specific pipe 
    pipe.push()

