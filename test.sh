#!/usr/bin/env bash

source activate nyc-taxi

pytest tests -v --cache-clear
