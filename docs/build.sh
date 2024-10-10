#!/bin/bash

# write the GA_TRACKING_ID (secret already set on github) to the myst.yml file
CONFIG_PATH="docs/myst.yml"
if [[ "$OSTYPE" == "darwin"* ]]; then  # sed in macos is different
  sed -i '' "s/{GA_TRACKING_ID_PLACEHOLDER}/$GA_TRACKING_ID/" $CONFIG_PATH
else
  sed -i "s/{GA_TRACKING_ID_PLACEHOLDER}/$GA_TRACKING_ID/" $CONFIG_PATH
fi

# install python dependencies so that jupyter notebooks can be executed
pip install jupyter-book "pfeed[all]"

# Clear Cache, Build HTML Assets and Execute Notebooks
cd docs/
myst clean --all
myst build --html --execute
