import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class SeasonFeature(BaseEstimator, TransformerMixin):
    """
    Extract information about season and append it to the input dataframe.
    The season is encoded as:
        1 = winter, 2 = spring, 3 = summer, 4 = fall

    Args:
        input_col - the name of the pandas.DataFrame column with values to analyze
        output_col - the name of a new column that should be appended to pandas.DataFrame
    """
    def __init__(self, input_col: str, output_col: str):
        self.input_col = input_col
        self.output_col = output_col

    def fit(self, X, y=None):
        return self

    @log_transformation(stage='SeasonFeature', indent_level=2)
    def transform(self, X):
        assert isinstance(X, pd.DataFrame)

        X[self.output_col] = (X[self.input_col].dt.month % 12 + 3) // 3
        return X
