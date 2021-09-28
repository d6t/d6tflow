# Databolt Flow

For data scientists and data engineers, `d6tflow` is a python library which makes building complex data science workflows easy, fast and intuitive. It is **primarily designed for data scientists to build better models faster**. For data engineers, it can also be a lightweight alternative and help productionize data science models faster. Unlike other data pipeline/workflow solutions, `d6tflow` focuses on managing data science research workflows instead of managing production data pipelines. 

## Why use d6tflow?

Data science workflows typically look like this.

![Sample Data Workflow](docs/d6tflow-docs-graph.png?raw=true "Sample Data Workflow")

The workflow involves chaining together parameterized tasks which pass multiple inputs and outputs between each other. The output data gets stored in multiple dataframes, files and databases but you have to manually keep track of where everything is. And often you want to rerun tasks with different parameters without inadvertently rerunning long-running tasks. The workflows get complex and your code gets messy, difficult to audit and doesn't scale well.

`d6tflow` to the rescue! **With d6tflow you can easily chain together complex data flows and execute them. You can quickly load input and output data for each task.** It makes your workflow very clear and intuitive.

#### Read more at:  
[4 Reasons Why Your Machine Learning Code is Probably Bad](https://github.com/d6t/d6t-python/blob/master/blogs/reasons-why-bad-ml-code.rst)  
[How d6tflow is different from airflow/luigi](https://github.com/d6t/d6t-python/blob/master/blogs/datasci-dags-airflow-meetup.md)

![Badge](https://www.kdnuggets.com/images/tkb-1904-p.png "Badge")
![Badge](https://www.kdnuggets.com/images/tkb-1902-g.png "Badge")

## When to use d6tflow?

* Data science: you want to build better models faster. Your workflow is EDA, feature engineering, model training and evaluation. d6tflow works with ANY ML library including sklearn, pytorch, keras
* Data engineering: you want to build robust data pipelines using a lightweight yet powerful library. You workflow is load, filter, transform, join data in pandas, dask, pyspark, sql, athena

## What can d6tflow do for you?

* Data science  
	* Experiment management: easily manage workflows that compare different models to find the best one
	* Scalable workflows: build an efficient data workflow that support rapid prototyping and iterations
	* Cache data: easily save/load intermediary calculations to reduce model training time
	* Model deployment: d6tflow workflows are easier to deploy to production
* Data engineering  
	* Build a data workflow made up of tasks with dependencies and parameters
	* Visualize task dependencies and their execution status
	* Execute tasks including dependencies
	* Intelligently continue workflows after failed tasks
	* Intelligently rerun workflow after changing parameters, code or data
	* Quickly share and hand off output data to others


## Installation

Install with `pip install d6tflow`. To update, run `pip install d6tflow -U`.

If you are behind an enterprise firewall, you can also clone/download the repo and run `pip install .`

**Python3 only** You might need to call `pip3 install d6tflow` if you have not set python 3 as default.

To install latest DEV `pip install git+git://github.com/d6t/d6tflow.git` or upgrade `pip install git+git://github.com/d6t/d6tflow.git -U --no-deps`

## Example: Model Comparison

Below is an introductory example that gets training data, trains two models and compares their performance.  

**[See the full ML workflow example here](http://tiny.cc/d6tflow-start-example)**  
**[Interactive mybinder jupyter notebook](http://tiny.cc/d6tflow-start-interactive)**

```python

import d6tflow
import sklearn.datasets, sklearn.ensemble, sklearn.linear_model
import pandas as pd


# get training data and save it
class GetData(d6tflow.tasks.TaskPqPandas):
    persist = ['x','y']

    def run(self):
        ds = sklearn.datasets.load_boston()
        df_trainX = pd.DataFrame(ds.data, columns=ds.feature_names)
        df_trainY = pd.DataFrame(ds.target, columns=['target'])
        self.save({'x': df_trainX, 'y': df_trainY}) # persist/cache training data


# train different models to compare
@d6tflow.requires(GetData)  # define dependency
class ModelTrain(d6tflow.tasks.TaskPickle):
    model = d6tflow.Parameter()  # parameter for model selection

    def run(self):
        df_trainX, df_trainY = self.inputLoad()  # quickly load input data

        if self.model=='ols':  # select model based on parameter
            model = sklearn.linear_model.LinearRegression()
        elif self.model=='gbm':
            model = sklearn.ensemble.GradientBoostingRegressor()

        # fit and save model with training score
        model.fit(df_trainX, df_trainY)
        self.save(model)  # persist/cache model
        self.saveMeta({'score': model.score(df_trainX, df_trainY)})  # save model score

# goal: compare performance of two models
# define workflow manager
flow = d6tflow.WorkflowMulti(ModelTrain, {'model1':{'model':'ols'}, 'model2':{'model':'gbm'}})
flow.reset_upstream(confirm=False) # DEMO ONLY: force re-run
flow.run()  # execute model training including all dependencies

'''
===== Execution Summary =====
Scheduled 2 tasks of which:
* 2 ran successfully:
    - 1 GetData()
    - 1 ModelTrain(model=ols)
This progress looks :) because there were no failed tasks or missing dependencies
'''

scores = flow.outputLoadMeta()  # load model scores
print(scores)
# {'model1': {'score': 0.7406426641094095}, 'gbm': {'model2': 0.9761405838418584}}


```


## Example Library

* [Minimal example](https://github.com/d6t/d6tflow/blob/master/docs/example-minimal.py)
* [Rapid Prototyping for Quantitative Investing with d6tflow](https://github.com/d6tdev/d6tflow-binder-interactive/blob/master/example-trading.ipynb) 
* d6tflow with functions only: get the power of d6tflow with little change in code. **[Jupyter notebook example](https://github.com/d6t/d6tflow/blob/master/docs/example-functional.ipynb)**

## Documentation

Library usage and reference https://d6tflow.readthedocs.io

## Getting started resources

[Transition to d6tflow from typical scripts](https://d6tflow.readthedocs.io/en/latest/transition.html)

[5 Step Guide to Scalable Deep Learning Pipelines with d6tflow](https://htmlpreview.github.io/?https://github.com/d6t/d6t-python/blob/master/blogs/blog-20190813-d6tflow-pytorch.html)

[Data science project starter templates](https://github.com/d6t/d6tflow-template)

## Pro version

Additional features:  
* Team sharing of workflows and data
* Integrations for datbase and cloud storage (SQL, S3)
* Integrations for distributed compute (dask, pyspark)
* Integrations for cloud execution (athena)
* Workflow deployment and scheduling

[Schedule demo](https://calendly.com/databolt/30min)

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

## How To Contribute

Thank you for considering to contribute to the project. First, fork the code repository and then pick an issue that is open. Afterwards follow these steps
* Create a branch called \[issue_no\]\_yyyymmdd\_\[feature\]
* Implement the feature
* Write unit tests for the desired behaviour
* Create a pull request to merge branch with master

A similar workflow applies to bug-fixes as well. In the case of a fix, just change the feature name with the bug-fix name. And make sure the code passes already written unit tests.
