#!/usr/bin/env bash

cat resources/raw_data_urls.txt | xargs -n 1 -P 6 wget -c -P ~/data/nyc-taxi/taxi_raw/
