import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.extract_gps_from_station_name import GpsFromStationNameExtractor
from bikerentals.src.cleaning.remove_missing_gps import MissingGpsLocationRemover
from bikerentals.src.cleaning.remove_same_location import SameLocationRemover


class DataCleaning(BaseEstimator, TransformerMixin):

    def __init__(self, hard_delete: bool):
        """
        Execute data cleaning pipeline.

        Parameters:
        * hard_delete - delete permanently records, otherwise soft delete will be applied
        """
        self.hard_delete = hard_delete
        self.flag_col = 'IsDeleted'

        gps_location_cols = ['Rental station latitude', 'Rental station longitude',
                             'Return station latitude', 'Return station longitude']

        self.data_cleaning_pipeline = make_pipeline(
            GpsFromStationNameExtractor('Rental station', 'Rental station latitude', 'Rental station longitude'),
            GpsFromStationNameExtractor('Return station', 'Return station latitude', 'Return station longitude'),
            SameLocationRemover('Rental station', 'Return station', self.flag_col),
            MissingGpsLocationRemover(gps_location_cols, self.flag_col)
        )

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        # execute pipeline
        processed_df = self.data_cleaning_pipeline.transform(X)

        # delete permanently marked records if hard delete was chosen
        if self.hard_delete:
            # remove marked rows
            idx = processed_df[processed_df[self.flag_col] == True].index  # noqa: E731
            processed_df = processed_df.drop(index=idx, axis=0)
            # remove flag column
            processed_df = processed_df.drop([self.flag_col], axis=1)

        return processed_df
