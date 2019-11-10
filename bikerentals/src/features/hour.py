import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import logger


class HourFeature(BaseEstimator, TransformerMixin):
    """
    Extract information about hour and append it to the input dataframe.

    Args:
        input_col: str - the name of the pandas.DataFrame column with values to analyze
        output_col: str - the name of a new column that should be appended to pandas.DataFrame
    """
    def __init__(self, input_col: str, output_col: str):
        self.input_col = input_col
        self.output_col = output_col

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        logger.info("* HourFeature *")
        logger.info(f"--> input data shape: {X.shape}")

        X[self.output_col] = X[self.input_col].dt.hour

        logger.info(f"--> output data shape: {X.shape}")
        return X
