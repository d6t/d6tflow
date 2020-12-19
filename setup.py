from setuptools import setup

setup(
    name='d6tflow',
    version='0.2.2',
    packages=['d6tflow','d6tflow.targets','d6tflow.tasks'],
    url='https://github.com/d6t/d6tflow',
    license='MIT',
    author='DataBolt Team',
    author_email='support@databolt.tech',
    description='d6tflow is a python library which makes it easier to build data workflows',
    long_description='d6tflow is a python library which makes it easier to build data workflows'
        'See https://github.com/d6t/d6tflow for details',
    install_requires=['luigi', 'pandas', 'pyarrow','d6tcollect'
    ],
    extras_require={
        'dask': ['toolz','dask[dataframe]'],
        'pipe': ['d6tpipe', 'jinja2']},
include_package_data=True,
    python_requires='>=3.5',
    keywords=['d6tflow', 'data workflow', 'data pipelines', 'luigi'],
    classifiers=[]
)

'''
# publish
# pip install setuptools wheel twine
python setup.py sdist bdist_wheel
twine upload dist/*  --skip-existing
'''