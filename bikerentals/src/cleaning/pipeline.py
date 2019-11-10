import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.extract_gps_from_station_name import GpsFromStationNameExtractor
from bikerentals.src.cleaning.records_remover import RecordsRemover
from bikerentals.src.cleaning.remove_missing_gps import MissingGpsLocationRemover
from bikerentals.src.cleaning.remove_same_location import SameLocationRemover
from bikerentals.src.utils.logging import logger


class DataCleaning(BaseEstimator, TransformerMixin):

    def __init__(self, hard_delete: bool):
        """
        Execute data cleaning pipeline.

        Parameters:
        * hard_delete - delete permanently records, otherwise soft delete will be applied
        """
        self.hard_delete = hard_delete
        self.delete_flag_colname = 'IsDeleted'

        gps_location_cols = ['Rental station latitude', 'Rental station longitude',
                             'Return station latitude', 'Return station longitude']

        self.data_cleaning_pipeline = make_pipeline(
            GpsFromStationNameExtractor('Rental station', 'Rental station latitude', 'Rental station longitude'),
            GpsFromStationNameExtractor('Return station', 'Return station latitude', 'Return station longitude'),
            SameLocationRemover('Rental station', 'Return station', self.delete_flag_colname),
            MissingGpsLocationRemover(gps_location_cols, self.delete_flag_colname),
            RecordsRemover(self.hard_delete, self.delete_flag_colname)
        )

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        logger.info("****** DataCleaning stage ******")
        logger.info(f"DataCleaning - input data shape: {X.shape}")

        # execute pipeline
        X = self.data_cleaning_pipeline.transform(X)

        logger.info(f"DataCleaning - output data shape: {X.shape}")
        return X
