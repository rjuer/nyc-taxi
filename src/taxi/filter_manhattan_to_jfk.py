"""
Script filters taxi rides from Manhattan to JFK International Airport.
"""
import gc
import logging

import dask.dataframe as dd
import pandas as pd
from tqdm import tqdm

from src.config.config import Config
from src.util import data_loader
from src.util.geo_handler import GeoHandler


def cleanup(*dfs):
    """Deletes unused objects and frees up memory.

    :param dfs: Data frames to be deleted
    """
    for df in dfs:
        del df

    gc.collect()


def is_unknown_location(dropoff_location_id_colname, dropoff_latitude_colname):
    """Checks whether the location is unknown. Latitude is sufficient since longitude alone would not help.

    :param str dropoff_location_id_colname: Location ID
    :param str dropoff_latitude_colname: Latitude
    :return: Flag whether location is unknown
    """
    return dropoff_location_id_colname == 'nan' and dropoff_latitude_colname == 'nan'


def is_known_location_ids(pickup_location_id_colname, dropoff_location_id_colname):
    """Checks whether column names for pickup and dropoff location IDs are available in the file.

    :param str pickup_location_id_colname: Pickup location ID
    :param str dropoff_location_id_colname: Dropoff location ID
    :return: Flag whether both location IDs are known
    """
    return pickup_location_id_colname != 'nan' and dropoff_location_id_colname != 'nan'


def filter_by_location_id(taxi_data, geo_handler, location_datetime_colnames, file_name, n_cores):
    """Filters taxi data from Manhattan to JFK International Airport by location ID

    :param taxi_data: Taxi data to be filtered
    :param geo_handler: GeoHandler
    :param location_datetime_colnames: Location
    :param file_name: File name for saving filtered data
    :param n_cores: Number of cores to use for parallelisation
    """
    taxi_data_dask = dd.from_pandas(taxi_data, npartitions=n_cores)
    taxi_data_dask['is_dropoff_jfk'] = taxi_data_dask.map_partitions(
        lambda partition: partition.apply(
            lambda row: geo_handler.is_jfk_location(row[location_datetime_colnames.dropoff_location_id]), axis=1
        )
    )
    taxi_data_filtered = taxi_data_dask[taxi_data_dask['is_dropoff_jfk'] == True].compute()

    logging.info(f'Shape of taxi data with JFK Airport as dropoff location: {taxi_data_filtered.shape}')

    if taxi_data_filtered.shape[0] > 0:
        taxi_data_dask = dd.from_pandas(taxi_data_filtered, npartitions=n_cores)

        taxi_data_dask['is_pickup_manhattan'] = taxi_data_dask.map_partitions(
            lambda partition: partition.apply(
                lambda row: geo_handler.is_manhattan_location(row[location_datetime_colnames.pickup_location_id]), axis=1
            )
        )
        taxi_data_filtered = taxi_data_dask[taxi_data_dask['is_pickup_manhattan'] == True].compute()

        logging.info(f'Shape of taxi data from Manhattan to JFK Airport: {taxi_data_filtered.shape}')

    taxi_data_filtered.to_csv(Config.PATH_DIR_FILTERED_RIDES / file_name)
    cleanup(taxi_data, taxi_data_filtered)


def filter_by_coordinates(taxi_data, geo_handler, location_datetime_colnames, file_name, n_cores):
    """Filters taxi data from Manhattan to JFK International Airport by latitude/longitude coordinates.

    :param taxi_data: Taxi data to be filtered
    :param geo_handler: GeoHandler
    :param location_datetime_colnames: Location
    :param file_name: File name for saving filtered data
    :param n_cores: Number of cores to use for parallelisation
    """
    taxi_data_dask = dd.from_pandas(taxi_data, npartitions=n_cores)

    taxi_data_dask['is_dropoff_jfk'] = taxi_data_dask.map_partitions(
        lambda partition: partition.apply(
            lambda row: geo_handler.is_jfk_lat_lon(lat=row[location_datetime_colnames.dropoff_lat_colname],
                                                   lon=row[location_datetime_colnames.dropoff_lon_colname]), axis=1
        )
    )
    taxi_data_filtered = taxi_data_dask[taxi_data_dask['is_dropoff_jfk'] == True].compute()

    logging.info(f'Shape of taxi data with JFK Airport as dropoff location: {taxi_data_filtered.shape}')

    if taxi_data_filtered.shape[0] > 0:
        taxi_data_dask = dd.from_pandas(taxi_data_filtered, npartitions=n_cores)

        taxi_data_dask['is_pickup_manhattan'] = taxi_data_dask.map_partitions(
            lambda partition: partition.apply(
                lambda row: geo_handler.is_manhattan_lat_lon(lat=row[location_datetime_colnames.pickup_lat_colname],
                                                             lon=row[location_datetime_colnames.pickup_lon_colname]), axis=1
            )
        )
        taxi_data_filtered = taxi_data_dask[taxi_data_dask['is_pickup_manhattan'] == True].compute()

        logging.info(f'Shape of taxi data from Manhattan to JFK Airport: {taxi_data_filtered.shape}')

    taxi_data_filtered.to_csv(Config.PATH_DIR_FILTERED_RIDES / file_name)
    cleanup(taxi_data, taxi_data_filtered)


def filter_manhattan_to_jfk(file_name, columns, geo_handler, n_cores):
    """Loads given file and filters for taxi rides from Manhattan to JFK International Airport

    :param str file_name: File to be loaded and filtered
    :param list columns: Column names to be loaded from file
    :param GeoHandler geo_handler: Object to handle geo calculations
    :param int n_cores: Number of cores to be used for parallel computation
    """
    logging.info(f'Filtering file: {file_name}')

    location_datetime_colnames = data_loader.get_location_datetime_columns(file_name=file_name)

    taxi_data = pd.read_csv(Config.PATH_DIR_TAXI / file_name,
                            skiprows=1,
                            skip_blank_lines=True,
                            low_memory=False,
                            names=columns)

    logging.info(f'Shape of taxi data: {taxi_data.shape}')

    if not Config.PATH_DIR_FILTERED_RIDES.exists():
        Config.PATH_DIR_FILTERED_RIDES.mkdir(parents=True)

    if is_unknown_location(dropoff_location_id_colname=location_datetime_colnames.dropoff_location_id_colname,
                           dropoff_latitude_colname=location_datetime_colnames.dropoff_lat_colname):
        logging.info('Unknown dropoff location...')

        taxi_data['is_pickup_manhattan'] = False
        taxi_data['is_dropoff_jfk'] = False

        subset = taxi_data.loc[(taxi_data['is_pickup_manhattan'] == True) & (taxi_data['is_dropoff_jfk'] == True)]
        subset.to_csv(Config.PATH_DIR_FILTERED_RIDES / file_name)
        cleanup(taxi_data, subset)

        logging.info('Filtered rides written to disk.')
    elif is_known_location_ids(pickup_location_id_colname=location_datetime_colnames.pickup_location_id,
                               dropoff_location_id_colname=location_datetime_colnames.dropoff_location_id):
        logging.info('Filtering by location ID...')

        filter_by_location_id(taxi_data=taxi_data,
                              geo_handler=geo_handler,
                              location_datetime_colnames=location_datetime_colnames,
                              file_name=file_name,
                              n_cores=n_cores)

        logging.info('Filtered rides written to disk.')
    else:
        logging.info('Filtering by latitude/longitude coordinates...')

        filter_by_coordinates(taxi_data=taxi_data,
                              geo_handler=geo_handler,
                              location_datetime_colnames=location_datetime_colnames,
                              file_name=file_name,
                              n_cores=n_cores)

        logging.info('Filtered rides written to disk.')


def main():
    logging.info('Filtering taxi rides from Manhattan to JFK International Airport...')

    schemas = data_loader.load_schema(from_idx=Config.FROM_IDX, to_idx=Config.TO_IDX)
    file_names = schemas.keys()
    available_file_names = [file_name for file_name in file_names if (Config.PATH_DIR_TAXI / file_name).exists()]

    geo_handler = GeoHandler(manhattan_polygon=data_loader.load_manhattan_polygon(),
                             manhattan_location_ids=Config.MANHATTAN_LOCATION_IDS,
                             jfk_polygon=data_loader.load_jfk_polygon(),
                             jfk_location_id=Config.JFK_LOCATION_ID)

    for file_name in tqdm(available_file_names):
        filter_manhattan_to_jfk(file_name=file_name,
                                columns=schemas[file_name],
                                geo_handler=geo_handler,
                                n_cores=Config.N_CORES)

    logging.info('Filtering completed.')


if __name__ == '__main__':
    import sys

    message_format = '%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=message_format)

    main()
