import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikerentals.src.features.day_of_week import DayOfWeekFeature
from bikerentals.src.features.distance import DistanceFeature
from bikerentals.src.features.holidays import HolidaysFeature
from bikerentals.src.features.hour import HourFeature
from bikerentals.src.features.month import MonthFeature
from bikerentals.src.features.season import SeasonFeature
from bikerentals.src.utils.logging import logger

holiday_dates = [
    '2019-01-01', '2019-01-06', '2019-04-21', '2019-04-22', '2019-05-01',
    '2019-05-03', '2019-06-09', '2019-06-20', '2019-08-15', '2019-11-01',
    '2019-11-11', '2019-12-25', '2019-12-26'
]


class DataFeaturization(BaseEstimator, TransformerMixin):

    def __init__(self):
        # TODO: Add new features:
        # - average speed
        # - weather, forecasted weather
        # - distance to nearest university, cinema etc.
        self.data_processing_pipeline = make_pipeline(
            SeasonFeature('Rental datetime', 'Season'),
            HolidaysFeature('Rental datetime', 'Holidays', holiday_dates),
            DayOfWeekFeature('Rental datetime', 'Rental day of week'),
            HourFeature('Rental datetime', 'Rental hour'),
            MonthFeature('Rental datetime', 'Rental month'),
            DistanceFeature('Rental station latitude', 'Rental station longitude',
                            'Return station latitude', 'Return station longitude', 'Distance')
        )

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(X, pd.DataFrame)

        logger.info("****** DataFeaturization stage ******")
        logger.info(f"DataFeaturization - input data shape: {X.shape}")

        # execute pipeline
        X = self.data_processing_pipeline.transform(X)

        logger.info(f"DataFeaturization - output data shape: {X.shape}")
        return X
