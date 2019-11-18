import pandas as pd

from dateutil.parser import parse
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class HolidaysFeature(BaseEstimator, TransformerMixin):
    """
    Extracts information about holidays and append it to the input dataframe.
    The new column contains boolean values.

    Args:
        input_col - the name of the pandas.DataFrame column with values to analyze.
        output_col - the name of a new column that should be appended to pandas.DataFrame
        holidays_dates - list of holidays
        mark_sundays_as_holidays - flag to treat Sundays as holidays or not
    """
    def __init__(self, input_col: str, output_col: str,
                 holidays_dates: [str], mark_sundays_as_holidays: bool = True):
        self.input_col = input_col
        self.output_col = output_col
        self.holidays_dates = holidays_dates
        self.mark_sundays_as_holidays = mark_sundays_as_holidays

    def fit(self, X, y=None):
        return self

    @log_transformation(stage='HolidaysFeature', indent_level=2)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        # calculate based on provided list of holidays
        dates = [parse(date) for date in self.holidays_dates]
        X[self.output_col] = X[self.input_col].isin(dates)

        if self.mark_sundays_as_holidays:
            # The day numbers: Monday=0, Sunday=6
            X[self.output_col] = (X[self.output_col] | (X[self.input_col].dt.dayofweek == 6))

        return X
