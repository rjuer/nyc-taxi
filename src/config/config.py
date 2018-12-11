from pathlib import Path

import pandas as pd


class Config:
    PATH_DIR_ROOT_DATA = Path().home() / 'data' / 'nyc-taxi'
    PATH_DIR_TAXI = PATH_DIR_ROOT_DATA / 'taxi_raw'
    PATH_DIR_RESULTS = PATH_DIR_ROOT_DATA / 'results'

    PATH_DIR_ROOT_REPO = Path().home() / 'repos' / 'nyc-taxi'
    PATH_DIR_CONFIG = PATH_DIR_ROOT_REPO / 'src' / 'config'
    PATH_DIR_TAXI_INFO = PATH_DIR_ROOT_REPO / 'resources' / 'taxi_info'

    PATH_DIR_FILTERED_RIDES = PATH_DIR_ROOT_DATA / 'results' / 'filtered_rides'

    PATH_FILE_NAMES = PATH_DIR_CONFIG / 'file_names.csv'
    PATH_COLUMN_NAMES = PATH_DIR_CONFIG / 'column_names.csv'
    PATH_SHAPE_FILE_NYC = PATH_DIR_TAXI_INFO / 'taxi_zones' / 'taxi_zones.shp'

    TAXI_ZONES = pd.read_csv(PATH_DIR_TAXI_INFO / 'taxi_zone_lookup.csv')
    LOCATION_NAME_MAPPING = pd.read_csv(PATH_DIR_CONFIG / 'location_name_mapping.csv')

    MANHATTAN_LOCATION_IDS = list(TAXI_ZONES['LocationID'][TAXI_ZONES['Borough'] == 'Manhattan'].values)
    JFK_LOCATION_ID = TAXI_ZONES['LocationID'][TAXI_ZONES['Zone'] == 'JFK Airport'].values[0]  # only one value

    FROM_IDX = 0  # Can be used to load only a subset of the files
    TO_IDX = None  # Can be used to load only a subset of the files. If set to None, all files are loaded.

    N_CORES = 16
