import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class SameLocationRemover(BaseEstimator, TransformerMixin):
    """
    Mark for deletion those records where bikes were rented from and returned to the same bike station.
    """

    def __init__(self, rental_station_col: str, return_station_col: str, flag_col: str):
        self.rental_station_col = rental_station_col
        self.return_station_col = return_station_col
        self.flag_col = flag_col

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        print("* SameLocationRemover *")
        print("--> input data shape: ", X.shape)

        # create 'flag' column if it's not there yet
        if self.flag_col not in X.columns:
            X[self.flag_col] = False

        # flag records for deletion
        X[self.flag_col] = (X[self.flag_col] | (X[self.rental_station_col] == X[self.return_station_col]))

        print("--> output data shape: ", X.shape)
        return X
