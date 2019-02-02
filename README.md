# Databolt Flow

For data scientists and data engineers, `d6tflow` is a python library which makes building complex data science workflows easy, fast and intuitive. It is built on top of workflow manager luigi but unlike luigi it is optimized for data science workflows.

## Why use d6tflow?

Data science workflows typically look like this.

![Sample Data Workflow](docs/d6tflow-docs-graph.png?raw=true "Sample Data Workflow")

The workflow involves chaining together parameterized tasks which pass multiple inputs and outputs between each other. The output data gets stored in multiple dataframes, files and databases but you have to manually keep track of where everything is. And often you want to rerun tasks with different parameters without inadvertently rerunning long-running tasks. The worksflows get complex and your code gets messy, difficult to audit and doesn't scale well.

`d6tflow` to the rescue! **With d6tflow you can easily chain together complex data flows and execute them. You can quickly load input and output data for each task.** It makes your workflow very clear and intuitive.

## What can d6tflow do for you?

* Build a data workflow made up of tasks with dependencies and parameters
* Check task dependencies and their execution status
* Execute tasks including dependencies
* Intelligently continue workflows after failed tasks
* Intelligently rerun workflow after changing parameters, code or data
* Save task output to Parquet, CSV, JSON, pickle and in-memory
* Load task output to pandas dataframe and python objects
* Quickly share and hand off output data to others

## Example Output

Below is sample output for a machine learning workflow. `TaskTrain` depends on `TaskPreprocess` which in turn depends on `TaskGetData`. In the end you want to train and evaluate a model but that requires running multiple dependencies. 

**[See the full example here](docs/example-ml.md)**

```python

# Check task dependencies and their execution status
d6tflow.show(TaskTrain())

'''
└─--[TaskTrain-{'do_preprocess': 'True'} (PENDING)]
   └─--[TaskPreprocess-{'do_preprocess': 'True'} (PENDING)]
      └─--[TaskGetData-{} (PENDING)]
'''

# Execute the model training task including dependencies
d6tflow.run([TaskTrain()])

'''
===== Execution Summary =====

Scheduled 3 tasks of which:
* 3 ran successfully:
    - 1 TaskGetData()
    - 1 TaskPreprocess(do_preprocess=True)
    - 1 TaskTrain(do_preprocess=True)
'''

# Load task output to pandas dataframe and model object for model evaluation
model = TaskTrain().output().load()
df_train = TaskPreprocess().output().load()
print(sklearn.metrics.accuracy_score(df_train['y'],model.predict(df_train.iloc[:,:-1])))
# 0.9733333333333334

# Intelligently rerun workflow after changing a preprocessing parameter
d6tflow.show([TaskTrain(do_preprocess=False)])

'''
└─--[TaskTrain-{'do_preprocess': 'False'} (PENDING)]
   └─--[TaskPreprocess-{'do_preprocess': 'False'} (PENDING)]
      └─--[TaskGetData-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
'''


```

## Installation


Install with `pip install d6tflow`. To update, run `pip install d6tflow -U --no-deps`.

You can also clone the repo and run `pip install .`

For dask support `pip install d6tflow[dask]`

## Documentation

https://d6tflow.readthedocs.io

## d6tpipe Integration

To quickly share workflow outputs, we recommend you make use of [d6tpipe](https://github.com/d6t/d6tpipe). See [Sharing Workflows and Outputs](https://d6tflow.readthedocs.io/en/latest/collaborate.html).

## Faster Data Engineering

Check out other d6t libraries to solve common data engineering problems, including  
* data ingest: quickly ingest raw data
* fuzzy joins: quickly join data
* data pipes: quickly share and distribute data

https://github.com/d6t/d6t-python

## Get notified

`d6tpipe` is in active development. Join the [databolt blog](http://blog.databolt.tech) for the latest announcements and tips+tricks.

## Collecting Errors Messages and Usage statistics

We have put a lot of effort into making this library useful to you. To help us make this library even better, it collects ANONYMOUS error messages and usage statistics. See [d6tcollect](https://github.com/d6t/d6tcollect) for details including how to disable collection. Collection is asynchronous and doesn't impact your code in any way.

It may not catch all errors so if you run into any problems or have any questions, please raise an issue on github.
