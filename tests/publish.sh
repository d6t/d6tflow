# pull latest github master
git fetch github master:master-prod
git checkout master-prod
git pull github master

git checkout master
git pull gitlab master
git merge master-prod

git commit -am "publish"
git push gitlab master
git push github master

# publish
# pip install setuptools wheel twine
python setup.py sdist bdist_wheel
twine upload dist/*  --skip-existing

# admin
git checkout -b master-prod
git branch -d master-prod

