import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class MissingGpsLocationRemover(BaseEstimator, TransformerMixin):
    """
    Mark for deletion those records that have missing GPS location.
    """

    def __init__(self, gps_location_cols: [str], flag_col: str):
        """
        Parameters:
        * gps_location_cols - list of column names that contain information about gps location.
        * flag_col - a name of a flag column.
        """
        self.gps_location_cols = gps_location_cols
        self.flag_col = flag_col

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    @log_transformation(stage='MissingGpsLocationRemover', indent_level=1)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        # create 'flag' column if it's not there yet
        if self.flag_col not in X.columns:
            X[self.flag_col] = False

        # flag records for deletion
        for gps_location_column in self.gps_location_cols:
            X[self.flag_col] = (X[self.flag_col] | (X[gps_location_column].isnull()))

        return X
