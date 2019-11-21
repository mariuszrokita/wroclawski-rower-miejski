import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import log_transformation


class RentalFeeFeature(BaseEstimator, TransformerMixin):
    """Calculate rental fee

    Arguments:
        rental_datetime_col {str} -- A name of a column with datetime - beginning of bike rental.
        return_datetime_col {str} -- A name of column with datetime - end of bike rental.
        output_col {str} -- A name of output column containing calculated fee.
    """

    def __init__(self, rental_datetime_col: str, return_datetime_col: str, output_col: str):
        self.rental_datetime_col = rental_datetime_col
        self.return_datetime_col = return_datetime_col
        self.output_col = output_col

    def fit(self, X, y=None):
        return self

    def __calculate_fee(self, row):
        diff_seconds = (row[self.return_datetime_col] - row[self.rental_datetime_col]) / np.timedelta64(1, 's')
        if diff_seconds <= 20 * 60:  # below or equal to 20 minutes
            return 0
        elif diff_seconds <= 60 * 60:  # below or equal to 60 minutes
            return 2
        elif diff_seconds <= 12 * 60 * 60:  # below or equal to 12 hours
            hours = (diff_seconds - 1) // (60 * 60)
            return 2 + (hours * 4)
        else:  # over 12 hours
            hours = (diff_seconds - 1) // (60 * 60)
            return 300 + 2 + (hours * 4)

    @log_transformation(stage='RentalFeeFeature', indent_level=2)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        X[self.output_col] = X.apply(self.__calculate_fee, axis=1)
        return X
