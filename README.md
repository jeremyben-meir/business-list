# NYC Historical Storefront Analysis

## Introduction
TBD

## Prerequisites
### Classes
In order to import the classes in `./classes` from various scripts, the local repository must be [added to the PYTHONPATH](https://stackoverflow.com/questions/3387695/add-to-python-path-mac-os-x).
<!-- source ~/.bash_profile -->
### requirements.txt
In order to successfully run the necessary scripts, make sure to have installed the requirements as listed in `./requirements.txt`. If needed run `pip install -r requirements.txt` from the repository path.
## Instructions
Given that the data pulls from multiple different sources of different formats, we have divided the code into segments that may be run separately. Instructions for each dataset are as follows:
### Pull from sources
* #### DCA
  * In `./scripts/dca` run all the files labeled `..._src.py`
* #### DOA
  * In `./scripts/doa` run all the files labeled `..._src.py`
* #### DOE
  * In `./scripts/doe` run all the files labeled `..._src.py`
* #### DOH
  * In `./scripts/doh` run all the files labeled `..._src.py`
* #### DOS
  * In `./scripts/dos` run all the files labeled `..._src.py`
* #### DOT
  * In `./scripts/dot` run all the files labeled `..._src.py`
* #### LIQ
  * In `./scripts/liq` run all the files labeled `..._src.py`
### Merge
* Run `./scripts/record_linkage.py`.
### Assign industries
* Run `./scripts/industry_assign.py`.
### Prepare timelines
* Run `./scripts/prepare_timelines.py`.
### Prepare observation lists for predictor
* Run `./scripts/business_observations.py` and `./scripts/date_loc_observations.py`
### Merge business observations with date-location observations
* Run `./scripts/merge_observations.py`.
### Prepare geojson file for visualizer OR model
* Run `./scripts/prepare_geojson.py` or `./scripts/survival_model.py`.

