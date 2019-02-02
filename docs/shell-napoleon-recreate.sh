#rm ./source/*
#cp ./source-bak/* ./source/
sphinx-apidoc -f -o ./source ..
make clean
make html
