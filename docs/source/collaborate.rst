Sharing Workflows and Outputs
==============================================

d6tpipe Integration
------------------------------------------------------------

d6tflow integrates with `d6tpipe <https://github.com/d6t/d6tpipe>`_ which allows you to sync your flow output with remote data repos on S3 and ftp. This makes sharing flow output and handing projects off to others very seamless. Some cases when you want to do this include:

* data engineers share data with data scientists
* vendors sharing data with clients
* teachers sharing data with students

Sharing Flow output
------------------------------------------------------------

.. code-block:: python

    import d6tflow.pipes

    # work in local mode first
    d6tflow.pipes.init('your-task-output', local_pipe=True) # save flow output to local pipe directory

    d6tflow.run(SomeTask()) # output automatically saved in pipe directory

    # when you are ready to push output, connect to remote pipe
    do_push = True 
    if do_push:
        d6tflow.pipes.init('your-task-output', reset=True) # connect to remote pipe
        pipe = d6tflow.pipes.get_pipe()
        pipe.push_preview()
        pipe.push() # upload data to remote data repo

Make sure you have configured d6tpipe correctly before you push. See https://d6tpipe.readthedocs.io/en/latest/quickstart.html#first-time-setup

To customize `init()` see :doc:`d6tflow.pipes module in Reference<../d6tflow>`

You might also need to set up the remote pipe for access, for example:

.. code-block:: python

    import d6tpipe
    api = d6tpipe.api.APIClient()
    # create pipe
    d6tpipe.upsert_pipe(api, {'name': 'your-task-output'})

    # optionally set permissions, in this case make data public
    settings = {"username":"public","role":"read"}
    d6tpipe.upsert_permissions(api, 'your-task-output', settings)

For additional details on how to use d6tpipe see https://d6tpipe.readthedocs.io/en/latest/quickstart.html

Push/Pull Individual Task Output
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


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

