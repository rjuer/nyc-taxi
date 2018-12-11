import geopandas as gpd
from shapely.ops import cascaded_union

from src.config.config import Config


class LocationTimeColNames:
    """Stores column names for location and datetime information.

    :param str pickup_location_id_colname: Pickup location ID
    :param str pickup_lon_colname: Pickup longitude
    :param str pickup_lat_colname: Pickup latitude
    :param str pickup_datetime: Pickup datetime
    :param str dropoff_location_id_colname: Dropoff location ID
    :param str dropoff_lon_colname: Dropoff longitude
    :param str dropoff_lat_colname: Dropoff latitude
    :param str dropoff_datetime: Dropoff datetime
    """
    def __init__(self, pickup_location_id_colname, pickup_lon_colname, pickup_lat_colname, pickup_datetime_colname,
                 dropoff_location_id_colname, dropoff_lon_colname, dropoff_lat_colname, dropoff_datetime_colname):
        self.pickup_location_id_colname = str(pickup_location_id_colname)
        self.pickup_lon_colname = str(pickup_lon_colname)
        self.pickup_lat_colname = str(pickup_lat_colname)
        self.pickup_datetime_colname = str(pickup_datetime_colname)
        self.dropoff_location_id_colname = str(dropoff_location_id_colname)
        self.dropoff_lon_colname = str(dropoff_lon_colname)
        self.dropoff_lat_colname = str(dropoff_lat_colname)
        self.dropoff_datetime_colname = str(dropoff_datetime_colname)


def get_location_datetime_columns(file_name):
    """Reads location and datetime columns from mapping file.

    :param str file_name: Name of the file for which location column names shall be read from mapping file.
    :return: LocationTimeColNames object with pickup and dropoff locations (location ID, latitude, longitude) as well
    as datetimes. Fields can be empty.
    """
    file_name_split = file_name.split('_')
    file_split_2 = file_name_split[2].split('-')

    taxi_type = file_name_split[0]
    year = int(file_split_2[0])
    month = int(file_split_2[1].split('.')[0])

    mapping = Config.LOCATION_NAME_MAPPING[(Config.LOCATION_NAME_MAPPING['year'] == year)
                                           & (Config.LOCATION_NAME_MAPPING['month'] == month)]

    return LocationTimeColNames(pickup_location_id_colname=mapping[f'{taxi_type}_pickup_location_id'].values[0],
                                pickup_lon_colname=mapping[f'{taxi_type}_pickup_lon'].values[0],
                                pickup_lat_colname=mapping[f'{taxi_type}_pickup_lat'].values[0],
                                pickup_datetime_colname=mapping[f'{taxi_type}_pickup_datetime'].values[0],
                                dropoff_location_id_colname=mapping[f'{taxi_type}_dropoff_location_id'].values[0],
                                dropoff_lon_colname=mapping[f'{taxi_type}_dropoff_lon'].values[0],
                                dropoff_lat_colname=mapping[f'{taxi_type}_dropoff_lat'].values[0],
                                dropoff_datetime_colname=mapping[f'{taxi_type}_dropoff_datetime'].values[0])


def load_schema(from_idx=0, to_idx=None):
    """Loads the data schemas for all taxi data files.

    :param int from_idx: Index from which file schemas shall be read.
    :param int to_idx: Index up to which file schemas shall be read.
    :return: Data schema as dictionary (key: file name, value: list of column names)
    """
    with open(str(Config.PATH_FILE_NAMES), 'r') as f:
        file_names = f.read().splitlines()

    with open(str(Config.PATH_COLUMN_NAMES), 'r') as f:
        col_names = f.read().splitlines()

    if to_idx:
        file_names = file_names[from_idx:to_idx]
        col_names = col_names[from_idx:to_idx]

    return dict(zip(file_names, [cols.split(',') for cols in col_names]))


def load_manhattan_polygon():
    """Loads all polygons for zones in Manhattan and merged them into one single polygon.

    :return: Polygon for Manhattan
    """
    taxi_zones = gpd.read_file(str(Config.PATH_SHAPE_FILE_NYC)).to_crs({'init': 'epsg:4326'})
    manhattan_polygons = list(taxi_zones[taxi_zones['borough'] == 'Manhattan']['geometry'].values)

    return gpd.GeoSeries(cascaded_union(manhattan_polygons))[0]


def load_jfk_polygon():
    """Loads polygon for JFK International Airport.

    :return: Polygon for JFK International Airport
    """
    taxi_zones = gpd.read_file(str(Config.PATH_SHAPE_FILE_NYC)).to_crs({'init': 'epsg:4326'})

    return taxi_zones[taxi_zones['LocationID'] == 132]['geometry'].values[0, ]
