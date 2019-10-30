import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin


class SameLocationRemover(BaseEstimator, TransformerMixin):
    """
    Removes records that were rented from and returned to the same bike station.
    """

    def __init__(self, rental_station_col, return_station_col):
        self.rental_station_col = rental_station_col
        self.return_station_col = return_station_col

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        
        print("* SameLocationRemover *")
        print("--> input data shape: ", X.shape)

        # TODO: write logic

        print("--> output data shape: ", X.shape)
        return X
  