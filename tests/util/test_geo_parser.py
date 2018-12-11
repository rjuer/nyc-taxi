import unittest

from src.config.config import Config
from src.util import data_loader
from src.util.geo_handler import GeoHandler


class GeoParserTest(unittest.TestCase):
    def setUp(self):
        self.geo_handler = GeoHandler(manhattan_polygon=data_loader.load_manhattan_polygon(),
                                      manhattan_location_ids=Config.MANHATTAN_LOCATION_IDS,
                                      jfk_polygon=data_loader.load_jfk_polygon(),
                                      jfk_location_id=Config.JFK_LOCATION_ID)

    def test_is_manhattan_by_location_id(self):
        self.assertTrue(self.geo_handler.is_manhattan_location(location_id=100))

        self.assertFalse(self.geo_handler.is_manhattan_location(location_id=86))  # neither Manhattan nor JFK
        self.assertFalse(self.geo_handler.is_manhattan_location(location_id=132))  # JFK

    def test_is_jfk_by_location_id(self):
        self.assertTrue(self.geo_handler.is_jfk_location(location_id=132))

        self.assertFalse(self.geo_handler.is_jfk_location(location_id=100))  # Manhattan
        self.assertFalse(self.geo_handler.is_jfk_location(location_id=86))  # neither Manhattan nor JFK

    def test_is_manhattan_by_coord(self):
        self.assertTrue(self.geo_handler.is_manhattan_lat_lon(lat=40.763939, lon=-73.977064))  # near Central Park
        self.assertTrue(self.geo_handler.is_manhattan_lat_lon(lat=40.866421, lon=-73.921658))  # Northern end
        self.assertTrue(self.geo_handler.is_manhattan_lat_lon(lat=40.702380, lon=-74.011933))  # Southern end

        self.assertFalse(self.geo_handler.is_manhattan_lat_lon(lat=40.642483, lon=-73.779158))  # JFK
        self.assertFalse(self.geo_handler.is_manhattan_lat_lon(lat=40.879426, lon=-73.914727))  # just outside Manhattan
        self.assertFalse(self.geo_handler.is_manhattan_lat_lon(lat=40.815164, lon=-73.982125))  # West of Manhattan

    def test_is_jfk_by_cooord(self):
        self.assertTrue(self.geo_handler.is_jfk_lat_lon(lat=40.642483, lon=-73.779158))

        self.assertFalse(self.geo_handler.is_jfk_lat_lon(lat=40.673344, lon=-73.717298))  # neither JFK nor Manhattan
        self.assertFalse(self.geo_handler.is_jfk_lat_lon(lat=40.763939, lon=-73.977064))  # Manhattan

    def tearDown(self):
        pass
