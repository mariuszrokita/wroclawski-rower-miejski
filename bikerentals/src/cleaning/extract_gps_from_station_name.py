import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class GpsFromStationNameExtractor(BaseEstimator, TransformerMixin):
    """
    Extract GPS coordinates from the bike station name column.
    """
    def __init__(self, station_col: str, latitude_col: str, longitude_col: str):
        self.station_col = station_col
        self.latitude_col = latitude_col
        self.longitude_col = longitude_col

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    @log_transformation(stage='GpsFromStationNameExtractor', indent_level=1)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        # find rows that have gps coordinates in the 'station name' column
        idx = X[X[self.station_col].str.contains(r'\d{2}.\d+, \d{2}.\d+', regex=True)].index
        if idx.any():
            gps_coordinates = X.loc[idx, self.station_col].str.split(', ', n=1, expand=True)
            X.loc[idx, self.latitude_col] = gps_coordinates[0].astype('float')
            X.loc[idx, self.longitude_col] = gps_coordinates[1].astype('float')

        return X
