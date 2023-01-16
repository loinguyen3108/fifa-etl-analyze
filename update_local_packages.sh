#!/usr/bin/env bash

# Install package to ./packages folder
echo 'Update local packages for project...'

# add local modules
echo '... adding all modules from local utils package'
zip -ru9 packages.zip etl -x etl/__pycache__/\*

exit 0
