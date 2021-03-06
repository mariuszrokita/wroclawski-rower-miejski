import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from bikerentals.src.features.holidays import HolidaysFeature


class TestHolidays:

    def test_should_determine_holidays_based_on_rental_datetime(self):
        # arrange
        data = {
            'Rental datetime': ['2019-02-01', '2019-05-01', '2019-05-02']
        }

        expected = {
            'Rental datetime': ['2019-02-01', '2019-05-01', '2019-05-02'],
            'Holidays': [False, True, False]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])

        holidays_dates = ['2019-05-01']

        # act
        sut = HolidaysFeature('Rental datetime', 'Holidays', holidays_dates, False)
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_treat_sundays_as_holidays(self):
        # arrange
        data = {
            'Rental datetime': ['2019-02-01', '2019-02-02', '2019-02-03', '2019-05-01']
        }

        expected = {
            'Rental datetime': ['2019-02-01', '2019-02-02', '2019-02-03', '2019-05-01'],
            'Holidays': [False, False, True, True]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])

        holidays_dates = ['2019-05-01']

        sut = HolidaysFeature('Rental datetime', 'Holidays', holidays_dates, True)

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_throw_when_missing_input_column(self):
        # arrange
        data = {
            'col_name': []
        }

        df = pd.DataFrame(data)
        df['col_name'] = pd.to_datetime(df['col_name'])

        holidays_dates = []

        # act
        sut = HolidaysFeature('Rental datetime', 'output col name', holidays_dates)

        with pytest.raises(KeyError, match='Rental datetime'):
            sut.transform(df)
