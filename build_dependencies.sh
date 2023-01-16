#!/usr/bin/env bash

# Install package to ./packages folder
echo 'Installing packages for project...'
pip3 install -r requirements.txt --target ./packages

# check to see if there are any external dependencies
# if not then create an empty file to seed zip with
if [ -z "$(ls -A packages)" ]
then
    touch packages/empty.txt
fi

# zip dependencies
if [ ! -d packages ]
then 
    echo 'ERROR - pip failed to import dependencies'
    exit 1
fi

cd packages
zip -9mrv packages.zip .
mv packages.zip ..
cd ..

# remove temporary directory and requirements.txt
rm -rf packages

exit 0
