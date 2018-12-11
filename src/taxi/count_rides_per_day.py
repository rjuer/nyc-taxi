"""
Script counts taxi rides per day.
"""
import gc
import logging
from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm

from src.config.config import Config
from src.util import data_loader


def get_pickup_date_column_name(column_names):
    """Finds the column name for the column containing the pickup date

    :param list column_names: All column names in file
    :return: Column name for pickup date
    """
    for col in column_names:
        if 'pickup_date' in col.lower():
            return col


def get_date(datetime):
    """Takes the year out of a datetime string

    :param str datetime: Datetime in format 'YYYY-mm-dd H:M:S'
    :return: Datetime as string in format 'YYYY-mm-dd'
    """
    return str(datetime)[:10]


def count_rides_per_day(file_name):
    """Counts taxi rides per day

    :param str file_name: File name to be loaded
    :return: Data frame with taxi ride counts per day
    """
    logging.info(f'Counting taxi rides per day for file: {file_name}')

    schemas = data_loader.load_schema(from_idx=Config.FROM_IDX, to_idx=Config.TO_IDX)
    column_names = schemas[file_name]

    pickup_date_column_name = get_pickup_date_column_name(column_names=column_names)
    taxi_rides = pd.read_csv(Config.PATH_DIR_TAXI / file_name,
                             skiprows=1,
                             skip_blank_lines=True,
                             low_memory=False,
                             names=column_names,
                             usecols=[pickup_date_column_name])

    taxi_rides['pickup_date_as_day'] = pd.to_datetime(taxi_rides[pickup_date_column_name],
                                                      format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')

    daily_counts = taxi_rides.groupby(by='pickup_date_as_day', as_index=True).count()
    daily_counts.drop(pickup_date_column_name, axis=1)
    daily_counts.columns = ['num_rides']

    del taxi_rides
    gc.collect()

    return daily_counts


def main():
    logging.info('Counting taxi rides per day...')

    schemas = data_loader.load_schema(from_idx=Config.FROM_IDX, to_idx=Config.TO_IDX)
    file_names = schemas.keys()
    available_file_names = [file_name for file_name in file_names if (Config.PATH_DIR_TAXI / file_name).exists()]

    logging.info(f'Number of files to be parsed: {len(available_file_names)}')

    results = []
    pool = Pool(processes=Config.N_CORES)

    for counts_by_day in tqdm(pool.imap_unordered(count_rides_per_day, available_file_names),
                              total=len(available_file_names)):
        results.append(counts_by_day)

    results_concat = pd.concat(results)
    total_counts_by_day = results_concat.groupby(results_concat.index).sum()

    if not Config.PATH_DIR_RESULTS.exists():
        Config.PATH_DIR_RESULTS.mkdir(parents=True)

    total_counts_by_day.to_csv(Config.PATH_DIR_RESULTS / 'num_rides_by_day.csv')

    logging.info(f'Counting completed. Total rides: {total_counts_by_day["num_rides"].sum()}')


if __name__ == '__main__':
    import sys

    message_format = '%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=message_format)

    main()
