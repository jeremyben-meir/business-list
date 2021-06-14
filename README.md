# NYC Historical Storefront Analysis

## Introduction
TBD

## Prerequisites
### global_vars.py
In order to import `./global_vars.py` and classes in `./classes` from various scripts, the local repository must be [added to the PYTHONPATH](https://stackoverflow.com/questions/3387695/add-to-python-path-mac-os-x).
### requirements.txt
In order to successfully run the necessary scripts, make sure to have installed the requirements as listed in `./requirements.txt`. If needed run `pip install -r requirements.txt` from the repository path.

## Instructions
Given that the data pulls from multiple different sources of different formats, we have divided the code into segments that may be run separately. Instructions for each dataset are as follows:
### Pull from sources
* #### DCA
  * In `./scripts/dca` run all the files labeled `..._src.py`
* #### DOA
  * In `./scripts/doa` run all the files labeled `..._src.py`
* #### DOH
  * In `./scripts/doh` run all the files labeled `..._src.py`
* #### DOS
  * In `./scripts/dos` run all the files labeled `..._src.py`
### Merge
* Run `./scripts/merge.py`.
