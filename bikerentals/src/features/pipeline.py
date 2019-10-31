from sklearn.pipeline import make_pipeline

from bikerentals.src.features.holidays import HolidaysFeature
from bikerentals.src.features.season import SeasonFeature

holiday_dates = [
    '2019-01-01', '2019-01-06', '2019-04-21', '2019-04-22', '2019-05-01',
    '2019-05-03', '2019-06-09', '2019-06-20', '2019-08-15', '2019-11-01',
    '2019-11-11', '2019-12-25', '2019-12-26'
]


def execute(df):

    data_processing_pipeline = make_pipeline(
        SeasonFeature('Rental datetime', 'Season'),
        HolidaysFeature('Rental datetime', 'Holidays', holiday_dates)
    )

    # execute pipeline and return result
    processed_df = data_processing_pipeline.transform(df)
    return processed_df
