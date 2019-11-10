import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import logger


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

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        logger.info(f"* GpsFromStationNameExtractor *")
        logger.info(f"--> input data shape: {X.shape}")

        # find rows that have gps coordinates in the 'station name' column
        idx = X[X[self.station_col].str.contains(r'\d{2}.\d+, \d{2}.\d+', regex=True)].index
        if idx.any():
            gps_coordinates = X.loc[idx, self.station_col].str.split(', ', n=1, expand=True)
            X.loc[idx, self.latitude_col] = gps_coordinates[0].astype('float')
            X.loc[idx, self.longitude_col] = gps_coordinates[1].astype('float')

        logger.info(f"--> output data shape: {X.shape}")
        return X
