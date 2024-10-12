## Installation
```bash
git clone git@github.com:PFund-Software-Ltd/pfeed.git
cd pfeed
git submodule update --init --recursive
poetry install --with dev,test,doc --all-extras
```

## Pull updates
```bash
# --recurse-submodules also updates each submodule to the commit specified by the main repository,
git pull --recurse-submodules  # = git pull + git submodule update --recursive
```
