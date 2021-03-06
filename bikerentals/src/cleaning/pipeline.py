import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.extract_gps_from_station_name import GpsFromStationNameExtractor
from bikerentals.src.cleaning.records_remover import RecordsRemover
from bikerentals.src.cleaning.remove_missing_gps import MissingGpsLocationRemover
from bikerentals.src.cleaning.remove_same_location import SameLocationRemover
from bikerentals.src.utils.logging import log_transformation


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

    @log_transformation(stage='DataCleaning', indent_level=1)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        # execute pipeline and return
        return self.data_cleaning_pipeline.transform(X)
