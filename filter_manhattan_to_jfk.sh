#!/usr/bin/env bash

export PYTHONPATH=~/repos/nyc-taxi:$PYTHONPATH

source activate nyc-taxi

python src/taxi/filter_manhattan_to_jfk.py
