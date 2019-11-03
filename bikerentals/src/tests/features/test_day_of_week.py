import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from bikerentals.src.features.day_of_week import DayOfWeekFeature


class TestDayOfWeek:

    def test_should_determine_day_of_week_based_on_rental_datetime(self):
        # arrange
        data = {
            'Rental datetime': ['2019-05-01', '2019-02-01', '2019-10-31', '2019-08-04']
        }

        expected = {
            'Rental datetime': ['2019-05-01', '2019-02-01', '2019-10-31', '2019-08-04'],
            'Rental day': [3, 5, 4, 7]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])

        sut = DayOfWeekFeature('Rental datetime', 'Rental day')

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

        # act
        sut = DayOfWeekFeature('Rental datetime', 'Rental day')

        with pytest.raises(KeyError, match='Rental datetime'):
            sut.transform(df)
