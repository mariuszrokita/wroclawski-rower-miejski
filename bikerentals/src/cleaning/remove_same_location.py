import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class SameLocationRemover(BaseEstimator, TransformerMixin):
    """
    Removes records that were rented from and returned to the same bike station.
    """

    def __init__(self, rental_station_col: str, return_station_col: str):
        self.rental_station_col = rental_station_col
        self.return_station_col = return_station_col

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        print("* SameLocationRemover *")
        print("--> input data shape: ", X.shape)

        # Delete all records for which bike is returned to the same bike station,
        # as such situation is not considered as a regular commute.
        idx = X[X[self.rental_station_col] == X[self.return_station_col]].index
        X = X.drop(index=idx, axis=0)

        print("--> output data shape: ", X.shape)
        return X
