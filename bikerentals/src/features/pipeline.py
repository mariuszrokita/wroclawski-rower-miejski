import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikerentals.src.features.day_of_week import DayOfWeekFeature
from bikerentals.src.features.distance import DistanceFeature
from bikerentals.src.features.holidays import HolidaysFeature
from bikerentals.src.features.hour import HourFeature
from bikerentals.src.features.month import MonthFeature
from bikerentals.src.features.season import SeasonFeature

holiday_dates = [
    '2019-01-01', '2019-01-06', '2019-04-21', '2019-04-22', '2019-05-01',
    '2019-05-03', '2019-06-09', '2019-06-20', '2019-08-15', '2019-11-01',
    '2019-11-11', '2019-12-25', '2019-12-26'
]


class DataFeaturization(BaseEstimator, TransformerMixin):

    def __init__(self):
        pass

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:

        # TODO: Add new features:
        # - average speed
        # - weather, forecasted weather
        # - distance to nearest university, cinema etc.
        data_processing_pipeline = make_pipeline(
            SeasonFeature('Rental datetime', 'Season'),
            HolidaysFeature('Rental datetime', 'Holidays', holiday_dates),
            DayOfWeekFeature('Rental datetime', 'Rental day of week'),
            HourFeature('Rental datetime', 'Rental hour'),
            MonthFeature('Rental datetime', 'Rental month'),
            DistanceFeature('Rental station latitude', 'Rental station longitude',
                            'Return station latitude', 'Return station longitude', 'Distance')
        )

        # execute pipeline and return result
        processed_df = data_processing_pipeline.transform(X)
        return processed_df
