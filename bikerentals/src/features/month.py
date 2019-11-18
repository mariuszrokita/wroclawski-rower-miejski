import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class MonthFeature(BaseEstimator, TransformerMixin):
    """
    Extract information about month and append it to the input dataframe.
    The month as January=1, December=12

    Args:
        input_col: str - the name of the pandas.DataFrame column with values to analyze
        output_col: str - the name of a new column that should be appended to pandas.DataFrame
    """
    def __init__(self, input_col: str, output_col: str):
        self.input_col = input_col
        self.output_col = output_col

    def fit(self, X, y=None):
        return self

    @log_transformation(stage='MonthFeature', indent_level=2)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        X[self.output_col] = X[self.input_col].dt.month
        return X
