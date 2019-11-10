import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.utils.logging import logger


class RecordsRemover(BaseEstimator, TransformerMixin):
    """
    Removes all records marked for removal.
    """

    def __init__(self, hard_delete: bool, delete_flag_colname: str):
        self.hard_delete = hard_delete
        self.delete_flag_colname = delete_flag_colname

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        logger.info("* RecordsRemover *")
        logger.info(f"--> input data shape: {X.shape}")

        # delete permanently marked records if hard delete was chosen
        if self.hard_delete:
            # remove marked rows
            idx = X[X[self.delete_flag_colname] == True].index  # noqa: E731
            X = X.drop(index=idx, axis=0)
            # remove flag column
            X = X.drop([self.delete_flag_colname], axis=1)

        logger.info(f"--> output data shape: {X.shape}")
        return X
