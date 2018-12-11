# Analysis of New York City Taxi Dataset

This repository contains an analysis of the New York City Taxi dataset (241 GB) 
with a total number of 1.878 billion rides.

The conducted analyses are:
* Filter taxi rides from Manhattan to JFK International Airport
* Correlation analysis between number of rides per day and weather in Central Park
* Visualisation of filtered rides from Manhattan to JFK International Airport

Taxi dataset and additional files in `resources/` were downloaded on 12 Nov, 2018.

## Getting Started

##### 1. Install [Miniconda](https://conda.io/miniconda.html)

##### 2. Clone repository and install Conda environment

* `git clone https://github.com/rjuer/nyc-taxi.git` (currently private)
* `conda env create -f environment.yml`

##### 3. Download raw taxi data (note: 241 GB in total, takes a few hours)

* Please change download target directory in `download_raw_data.sh` first.
* Then run `./download_raw_data.sh`.

##### 4. Run tests

* Run `./test.sh`.

## Running the code

##### 1. Configuration

* Please edit configuration in `src/config/config.py` as required (esp. directories and number of cores).

##### 2. Count taxi rides per day

* Run `./count_rides_per_day.sh`.

##### 3. Filter taxi rides from Manhattan to JFK International Airport

* Run `./filter_manhattan_to_jfk.sh`.

##### 4. Correlation analysis between number of trips per day and weather in Central Park

* `source activate nyc-taxi`
* Exploratory data analysis of weather data: `jupyter notebook notebooks/EDA_weather_data.ipynb`
* Correlation analysis: `jupyter notebook notebooks/correlation_analysis.ipynb`

##### 5. Visualisations

* `source activate nyc-taxi`
* `jupyter notebook notebooks/visualisation_of_rides_from_manhattan_to_jfk.ipynb`
