from shapely.geometry import Point


class GeoHandler:
    """Class to handle geographic information and check whether points are specific areas

    :param Shapely.geometry.Polygon manhattan_polygon: Polygon of Manhattan
    :param Shapely.geometry.Polygon jfk_polygon: Polygon of JFK International Airport
    :param list manhattan_location_ids: List of location IDs for all taxi zones in Manhattan
    :param int jfk_location_id: Location ID for JFK International Airport
    """
    def __init__(self, manhattan_polygon, jfk_polygon, manhattan_location_ids, jfk_location_id):
        self.manhattan_polygon = manhattan_polygon
        self.jfk_polygon = jfk_polygon
        self.manhattan_location_ids = manhattan_location_ids
        self.jfk_location_id = jfk_location_id

    def is_manhattan_lat_lon(self, lat, lon):
        """Calculates whether a point with given coordinates lies in Manhattan

        :param float lat: Latitude
        :param float lon: Longitude
        :return: Flag whether given point lies in Manhattan
        """
        return Point(lon, lat).within(self.manhattan_polygon)

    def is_manhattan_location(self, location_id):
        """Checks whether given location ID matches any of the Manhattan location ID (multiple)

        :param int location_id: Location ID
        :return: Flag whether given location ID matches any of the Manhattan location IDs
        """
        return location_id in self.manhattan_location_ids

    def is_jfk_lat_lon(self, lat, lon):
        """Calculates whether a point with given coordinates lies in the JFK International Airport area

        :param float lat: Latitude
        :param float lon: Longitude
        :return: Flag whether given point lies in JFK International Airport area
        """
        return Point(lon, lat).within(self.jfk_polygon)

    def is_jfk_location(self, location_id):
        """Checks whether given location ID matches JFK International Airport (only one ID)

        :param int location_id: Location ID
        :return: Flag whether given location ID matches JFK International Airport
        """
        return location_id == self.jfk_location_id
