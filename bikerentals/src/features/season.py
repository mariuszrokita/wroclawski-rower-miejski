import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin


class SeasonFeature(BaseEstimator, TransformerMixin):
    """
    Extracts information about season and append it to the input dataframe.

    Input:
    * input_col - the name of the pandas.DataFrame column with values to analyze.
    * output_col - the name of a new column that should be appended to pandas.DataFrame

    Returns:
    * a dataframe with a new column with following values:
        1 = winter, 2 = spring, 3 = summer, 4 = fall
    """
    def __init__(self, input_col, output_col):
        self.input_col = input_col
        self.output_col = output_col

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)

        print("* SeasonFeature *")
        print("--> input data shape: ", X.shape)

        X[self.output_col] = (X[self.input_col].dt.month % 12 + 3) // 3

        print("--> output data shape: ", X.shape)
        return X
