import numpy as np
import pandas as pd
from geopy.distance import geodesic
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class DistanceFeature(BaseEstimator, TransformerMixin):
    """
    Calculates shortest distance (streight line) between rental and return stations.

    Args:
        rental_station_lat_col
        rental_station_long_col
        return_station_lat_col
        return_station_long_col
    """
    def __init__(self, rental_station_lat_col: str, rental_station_long_col: str,
                 return_station_lat_col: str, return_station_long_col: str, output_col: str):
        self.rental_station_lat_col = rental_station_lat_col
        self.rental_station_long_col = rental_station_long_col
        self.return_station_lat_col = return_station_lat_col
        self.return_station_long_col = return_station_long_col
        self.output_col = output_col

    def fit(self, X, y=None):
        return self

    @log_transformation(stage='DistanceFeature', indent_level=1)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        cache = {}

        def calculate_distance(row):
            """
            Calculate shortest distance between two points (streight line).
            Calculated distances are cached so there's no need to recalculate
            distance between same points.
            """
            rental_station_gps = (row[self.rental_station_lat_col], row[self.rental_station_long_col])
            return_station_gps = (row[self.return_station_lat_col], row[self.return_station_long_col])

            key1 = str(rental_station_gps + return_station_gps)
            if key1 in cache.keys():
                return cache[key1]

            key2 = str(return_station_gps + rental_station_gps)
            if key2 in cache.keys():
                return cache[key2]

            # calculate distance and cache
            distance = round(geodesic(rental_station_gps, return_station_gps).km, 1)
            cache[key1] = distance
            cache[key2] = distance
            return cache[key1]

        # determine what rows contain any NaN value in columns used for calculations
        nans = X[[self.rental_station_lat_col, self.rental_station_long_col,
                  self.return_station_lat_col, self.return_station_long_col]].isnull().any(axis=1)

        # records with any NaN value cannot get distance calculated
        X.loc[nans, self.output_col] = np.nan

        # calculate distance for every non-NaN row
        X.loc[~nans, self.output_col] = X[~nans].apply(lambda x: calculate_distance(x), axis=1)

        return X
