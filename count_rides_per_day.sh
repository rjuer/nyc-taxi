#!/usr/bin/env bash

export PYTHONPATH=~/repos/nyc-taxi:$PYTHONPATH

source activate nyc-taxi

python src/taxi/count_rides_per_day.py
