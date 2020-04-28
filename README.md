# Databolt Flow

For data scientists and data engineers, `d6tflow` is a python library which makes building complex data science workflows easy, fast and intuitive. It is built on top of workflow manager luigi but unlike luigi it is optimized for data science workflows.

## Why use d6tflow?

Data science workflows typically look like this.

![Sample Data Workflow](docs/d6tflow-docs-graph.png?raw=true "Sample Data Workflow")

The workflow involves chaining together parameterized tasks which pass multiple inputs and outputs between each other. The output data gets stored in multiple dataframes, files and databases but you have to manually keep track of where everything is. And often you want to rerun tasks with different parameters without inadvertently rerunning long-running tasks. The workflows get complex and your code gets messy, difficult to audit and doesn't scale well.

`d6tflow` to the rescue! **With d6tflow you can easily chain together complex data flows and execute them. You can quickly load input and output data for each task.** It makes your workflow very clear and intuitive.

Read more at:  
[4 Reasons Why Your Machine Learning Code is Probably Bad](https://github.com/d6t/d6t-python/blob/master/blogs/reasons-why-bad-ml-code.rst)  
[How d6tflow is different from airflow/luigi](https://github.com/d6t/d6t-python/blob/master/blogs/datasci-dags-airflow-meetup.md)

![Badge](https://www.kdnuggets.com/images/tkb-1904-p.png "Badge")
![Badge](https://www.kdnuggets.com/images/tkb-1902-g.png "Badge")

## What can d6tflow do for you?

* Build a data workflow made up of tasks with dependencies and parameters
* Check task dependencies and their execution status
* Execute tasks including dependencies
* Intelligently continue workflows after failed tasks
* Intelligently rerun workflow after changing parameters, code or data
* Save task output to Parquet, CSV, JSON, pickle and in-memory
* Load task output to pandas dataframe and python objects
* Quickly share and hand off output data to others


## Installation

Install with `pip install d6tflow`. To update, run `pip install d6tflow -U --no-deps`.

You can also clone the repo and run `pip install .`

**Python3 only** You might need to call `pip3 install d6tflow` if you have not set python 3 as default.

To install latest DEV `pip install git+git://github.com/d6t/d6tflow.git` or upgrade `pip install git+git://github.com/d6t/d6tflow.git -U --no-deps`

## Example: Introduction

This is a minial example. Be sure to check out the ML workflow example below.

```python

import d6tflow, luigi
import pandas as pd

# define 2 tasks that load raw data
class Task1(d6tflow.tasks.TaskPqPandas):
    
    def run(self):
        df = pd.DataFrame({'a':range(3)})
        self.save(df) # quickly save dataframe

class Task2(Task1):
    pass

# define another task that depends on data from task1 and task2
@d6tflow.requires(Task1,Task2)
class Task3(d6tflow.tasks.TaskPqPandas):
    multiplier = luigi.IntParameter(default=2)
    
    def run(self):
        df1 = self.input()[0].load() # quickly load input data
        df2 = self.input()[1].load() # quickly load input data
        df = df1.join(df2, lsuffix='1', rsuffix='2')
        df['b']=df['a1']*self.multiplier # use task parameter
        self.save(df)

# Execute task including all its dependencies
d6tflow.run(Task3())
'''
* 3 ran successfully:
    - 1 Task1()
    - 1 Task2()
    - 1 Task3(multiplier=2)
'''

Task3().outputLoad() # quickly load output data. Task1().outputLoad() also works
'''
   a1  a2  b
0   0   0  0
1   1   1  2
2   2   2  4
'''

# Intelligently rerun workflow after changing parameters
d6tflow.preview(Task3(multiplier=3))
'''
└─--[Task3-{'multiplier': '3'} (PENDING)] => this changed and needs to run
   |--[Task1-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
   └─--[Task2-{} (COMPLETE)] => this doesn't change and doesn't need to rerun
'''

```


## Example: ML Workflow

Below is sample output for a machine learning workflow. `TaskTrain()` depends on `TaskPreprocess()` which in turn depends on `TaskGetData()`. In the end you want to train and evaluate a model but that requires running multiple dependencies. 

**[See the full example here](http://tiny.cc/d6tflow-start-example)**  
**[Interactive mybinder example](http://tiny.cc/d6tflow-start-interactive)**


## Documentation

Library usage and reference https://d6tflow.readthedocs.io

Real-life project template https://github.com/d6t/d6tflow-template

Transition to d6tflow from typical scripts [5 Step Guide to Scalable Deep Learning Pipelines with d6tflow](https://htmlpreview.github.io/?https://github.com/d6t/d6t-python/blob/master/blogs/blog-20190813-d6tflow-pytorch.html)


## d6tpipe Integration

To quickly share workflow outputs, we recommend you make use of [d6tpipe](https://github.com/d6t/d6tpipe). See [Sharing Workflows and Outputs](https://d6tflow.readthedocs.io/en/latest/collaborate.html).

## Pro version

Additional features:  
* SQL target storage
* Dask and pyspark integration
* Automatically detect data changes
* Advanced machine learning features

[Request demo](https://pipe.databolt.tech/gui/request-premium/)

## Accelerate Data Science

Check out other d6t libraries, including  
* push/pull data: quickly get and share data files like code
* import data: quickly ingest messy raw CSV and XLS files to pandas, SQL and more
* join data: quickly combine multiple datasets using fuzzy joins

https://github.com/d6t/d6t-python

## Get notified

`d6tflow` is in active development. Join the [databolt blog](http://blog.databolt.tech) for the latest announcements and tips+tricks.

## Collecting Errors Messages and Usage statistics

We have put a lot of effort into making this library useful to you. To help us make this library even better, it collects ANONYMOUS error messages and usage statistics. See [d6tcollect](https://github.com/d6t/d6tcollect) for details including how to disable collection. Collection is asynchronous and doesn't impact your code in any way.

It may not catch all errors so if you run into any problems or have any questions, please raise an issue on github.
